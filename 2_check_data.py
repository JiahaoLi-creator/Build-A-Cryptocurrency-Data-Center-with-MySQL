"""
1.从mysql中读取数据
2.并检查取出的k线有无缺失
3.若有缺失，通过http获取最新k线
"""

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)
from sqlalchemy import create_engine
from datetime import datetime
import time
import requests

from Function import *


# 从mysql中获取数据
# 需要传入symbol和time_interval
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

    df = df[['symbol', 'candle_begin_time', 'time_interval',
             'open', 'low', 'high', 'close', 'volume',
             # 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
             # 'taker_buy_quote_asset_volume'
             ]]

    return df.drop_duplicates()  # 返回去重后的数据


# 判断df中的时间序列数据有没有缺失
def check_missing_data(df):
    if df.empty:
        return

    # 检查k线有无缺失，这里只检查中间k线是否有缺失，并不保证最新一根k线是当下最新

    # 需确保数据列中有['symbol', 'candle_begin_time', 'time_interval']
    df = df[['symbol', 'candle_begin_time', 'time_interval']].copy()
    # 倒序，最新一根k线在前面
    df.sort_values('candle_begin_time', ascending=True, inplace=True)

    # 看看传入的df数据是什么time_interval的
    time_interval = df['time_interval'].dropna().iloc[0]
    # 生成一列时间序列
    temp = pd.DataFrame()
    if time_interval.endswith('m'):
        _t = time_interval.split('m')[0]  # 5m -> 5
        temp['candle_begin_time_all'] = pd.date_range(df['candle_begin_time'].iloc[0],
                                                      df['candle_begin_time'].iloc[-1],
                                                      freq=f'{_t}T')
    elif time_interval.endswith('h'):
        _t = time_interval.split('m')[0]  # 2h -> 2
        temp['candle_begin_time_all'] = pd.date_range(df['candle_begin_time'].iloc[0],
                                                      df['candle_begin_time'].iloc[-1],
                                                      freq=f'{_t}T')
    # 相当于sql中的left join
    _df = pd.merge(temp, df, how='left',
                   left_on='candle_begin_time_all',
                   right_on='candle_begin_time')

    # left join后，如果candle_begin_time有空值，证明有缺失数据
    missing_time_list = (_df[_df['candle_begin_time'].isna()]['candle_begin_time_all'].astype(int) // 10 ** 6).to_list()

    # 如果有缺失数据
    if missing_time_list:
        params_list = []

        # 将缺失时间整理为binance api要求的param格式，后续用param获取数据
        for missing_time in missing_time_list:
            param = {
                'symbol': _df.dropna()['symbol'].iloc[0],
                'interval': _df.dropna()['time_interval'].iloc[0],
                'startTime': missing_time - 3600 * 8 * 1000,
                'limit': 1
            }
            # print(param)
            params_list.append(param)

        return params_list

    else:
        # 如果missing_time_list为空
        return []


# 通过币安http接口，获取缺失数据，仅获取一条k显示数据（limit=1）
# 传入的是一个list，保存需要获取的参数
# [{'symbol': 'ETHUSDT', 'interval': '1m', 'startTime': 1624433760000, 'limit': 1},
#  {'symbol': 'ETHUSDT', 'interval': '1m', 'startTime': 1624433880000, 'limit': 1}, ...]
def get_data_via_http(params_list):
    if not params_list:
        return pd.DataFrame()

    # 默认为现货数据，如需合约数据需修改url
    base_url = 'https://api.binance.com'
    path = '/api/v3/klines'
    url = base_url + path

    df_list = []
    for params in params_list:

        for i in range(3):
            try:
                # 此处暂时没有加容错
                response_data = requests.get(url, params=params, timeout=5).json()

                df = pd.DataFrame(response_data, dtype=float)
                df[11] = params['symbol']
                df[12] = params['interval']
                # 合并数据
                df_list.append(df)

                # sleep
                time.sleep(1)
            except Exception as e:
                print('通过http获取数据失败：', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not df_list:
        print('没有从交易所获取到数据，请检查。', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return pd.DataFrame()

    # =====合并整理数据
    df = pd.concat(df_list, ignore_index=True)

    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume',
                       6: 'end_MTS',
                       7: 'quote_volume', 8: 'trade_num',
                       9: 'taker_buy_base_asset_volume',
                       10: 'taker_buy_quote_asset_volume',
                       11: 'symbol', 12: 'time_interval'}, inplace=True)

    # websocket获取到的k线数据，保存到mysql时用的是本地时间，所以这里需要+8 hours
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms') + pd.Timedelta(value=8, unit='h')  # 整理时间
    df = df[['symbol', 'candle_begin_time', 'time_interval',
             'open', 'high', 'low', 'close',
             'volume', 'quote_volume', 'trade_num',
             'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']]

    return df


def save_data_to_mysql(engine, table_name, df):
    """
    将传入的df插入到mysql数据库中
    """

    if not df.empty:
        # Columns: [symbol, candle_begin_time, open, low, high, close, volume]
        df.to_sql(table_name, engine, index=False, if_exists='append')
        print(f'成功保存数据至mysql数据库: {table_name}', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        print('df为空，不插入数据。', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return


if __name__ == '__main__':

    # 初始化engine
    engine = create_engine('mysql+pymysql://root:xxxx@localhost:3306/binance')

    # 无限循环
    while True:

        # 每30m运行一次
        run_time = sleep_until_run_time('30m')

        # sleep n分钟再运行，避免在整点运行
        time.sleep(3 * 60)

        symbol_list = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT']
        # symbol_list = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT', 'DOGEUSDT',
        #                'LINKUSDT', 'SUSHIUSDT', 'UNIUSDT', 'FILUSDT']

        for symbol in symbol_list:
            # 逐个symbol检查数据

            # 从mysql数据库获取k线数据
            df = get_data_from_mysql(engine=engine, table_name='k', symbol=symbol,
                                     # begin_time='2021-06-23 16:00:00'
                                     limit=100
                                     )

            # 检查有无缺失数据
            # 注意只检查第一根k线与最后一根k线之间有无缺失
            # 不确保最后一根k线是当前时间下的最新k线
            params_list = check_missing_data(df)

            # 若有缺失数据，通过http获取
            if params_list:
                # print(params_list)
                missing_df = get_data_via_http(params_list)
                print('缺失数据：')
                print(missing_df)

                save_data_to_mysql(engine, 'k', missing_df)
            else:
                print(symbol, '无缺失数据', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
