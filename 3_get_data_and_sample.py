"""
从mysql获取数据并resample为所需time_interval
"""

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)
from sqlalchemy import create_engine
from Function import *


# 该函数同 5_check_data.py
# 从mysql中获取数据
# 需要传入symbol
def get_data_from_mysql(engine, table_name, symbol='BTCUSDT', time_interval='1m', begin_time=None, limit=100, sql=''):
    sql_query = f'select * from {table_name} where time_interval=\'{time_interval}\''

    if symbol:  # 如果没传入symbol，则返回所有交易对数据
        sql_query += f' and symbol = \'{symbol}\''

    if begin_time:  # 如果传入了begina_time
        sql_query += f' and candle_begin_time >= \'{begin_time}\''
    else:  # 如果没有传入begin_time，返回最新的n条数据
        sql_query += f' order by candle_begin_time desc limit {limit}'

    sql_query += ';'

    if sql:
        # 如果传入了sql语句，直接执行传入的sql代码
        sql_query = sql

    # print(sql_query) # debug
    df = pd.read_sql_query(sql_query, engine)

    k_columns = ['symbol', 'candle_begin_time', 'time_interval',
                 'open', 'low', 'high', 'close', 'volume', ]

    if set(k_columns) <= set(df.columns):
        # 如果df返回的列中包含了k_columns的所有字段
        df = df[k_columns]
    else:
        df = df

    return df.drop_duplicates()  # 返回去重后的数据


# 获取resample后的数据
#
def get_k_data(symbol='BTCUSDT', target_time_interval='15m', limit=100,
               engine=None, table_name='k', time_interval='1m'):
    # target_time_interval 和 limit计算出begin_time
    # 计算目标时间周期，最近一根k线开始时间
    # 如：目标时间周期15m，当前10:15:01，则返回now_run_time=10:15:00
    now_run_time = this_run_time(target_time_interval)

    if target_time_interval.endswith('m'):
        # 15m 100根k线 -> 1500，即需要当前时间 - 1500 mins
        _t = int(target_time_interval.split('m')[0])
    elif target_time_interval.endswith('h'):
        # 1h 100根k线 -> 60 * 100，即需要当前时间 - 6000 mins
        _t = int(target_time_interval.split('h')[0]) * 60

    __t = _t * (limit + 5)  # 多取5根k线

    begin_time = now_run_time - timedelta(minutes=__t)

    df = get_data_from_mysql(engine=engine, table_name=table_name,
                             symbol=symbol, time_interval=time_interval,
                             begin_time=begin_time)

    if target_time_interval.endswith('m'):
        # 15m -> 15T
        rule_type = target_time_interval.replace('m', 'T')
    elif target_time_interval.endswith('h'):
        # 1h -> 1H
        rule_type = target_time_interval.replace('h', 'H')

    # =====转换为其他分钟数据
    period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
        {'open': 'first',
         'high': 'max',
         'low': 'min',
         'close': 'last',
         'volume': 'sum',
         # 'quote_volume': 'sum',
         # 'trade_num': 'sum',
         # 'taker_buy_base_asset_volume': 'sum',
         # 'taker_buy_quote_asset_volume': 'sum',
         })

    return period_df.dropna()


# 获取某个标的的最新一条k线的candle_begin_time
def get_newest_candle_begin_time(engine=None, table_name='k', symbol='BTCUSDT'):
    sql = f'''
        select max(candle_begin_time) from {table_name} 
        where symbol=\'{symbol}\'
           '''
    # print(sql)

    # 获取某个标的的最新一条k线的candle_begin_time
    df = get_data_from_mysql(engine=engine, table_name=table_name, sql=sql)

    return df.iloc[0, 0]


# 判断数据库中某个币种的最新k线数据是否已经存在
# 比如数据库抓取的是1m数据，当下时间11:00，数据库中应有10:59的k线存在
def if_data_available(engine=None, table_name='k', symbol='BTCUSDT'):
    newest_candle = get_newest_candle_begin_time(engine=engine, table_name=table_name, symbol=symbol)
    newest_candle = str(newest_candle)
    last_candle = (datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M") + ':00'

    print('当前时间：', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(f'{symbol}最新k线candle_begin_time：', newest_candle)

    data_available = newest_candle == last_candle

    # if data_available:
    #     print('数据库中已存在最新数据', symbol, newest_candle)

    return data_available


if __name__ == '__main__':

    # 初始化
    engine = create_engine('mysql+pymysql://root:xxxx@localhost:3306/binance')
    table_name = 'k'
    # symbol = 'ETHUSDT'
    symbol_list = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT']
    target_time_interval = '5m'
    limit = 200

    while True:

        # 每target_time_interval运行一次
        run_time = sleep_until_run_time(target_time_interval, if_sleep=True)

        for symbol in symbol_list:

            # 确保数据库已存在最新k线
            data_available = if_data_available(engine=engine, table_name=table_name, symbol=symbol)

            # 检查是否存在最新数据，如不存在，sleep后重试，重试5次
            for i in range(5):
                if data_available:
                    print('已获取到最新数据', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    break  # 跳出循环
                else:
                    print('没有获取到最新数据，1秒后重新检查。', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    time.sleep(1)
                    # 再判断一遍
                    data_available = if_data_available(engine=engine, table_name=table_name, symbol=symbol)

            if not data_available:
                # 如果5s后都没有在数据库中发现最新数据
                print('没有获取到最新数据，请检查！')

            # 无论有没有取到最新数据，都输出现有数据
            df = get_k_data(symbol=symbol, target_time_interval=target_time_interval, limit=limit,
                            engine=engine, table_name=table_name, time_interval='1m')

            print(df.tail(2))
