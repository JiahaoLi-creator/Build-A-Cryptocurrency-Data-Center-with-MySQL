"""
websocket从binance获取数据
并存储到mysql
"""

import websocket
import json
import time
from datetime import datetime
import pandas as pd
import os
import requests

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)

from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:xxxx@localhost:3306/binance')


def on_open(ws):
    print('调用on_open()函数，与交易所建立websocket连接，订阅需要的数据')
    data = {'method': 'SUBSCRIBE',
            'params': [
                'btcusdt@kline_1m',
                'ethusdt@kline_1m',
                'bnbusdt@kline_1m',
                'dogeusdt@kline_1m',
                'sushiusdt@kline_1m',
                'filusdt@kline_1m',
                'uniusdt@kline_1m',
                'linkusdt@kline_1m',
                'ltcusdt@kline_1m',
                'dotusdt@kline_1m',
            ],
            'id': 123
            }
    ws.send(json.dumps(data))


def on_message(ws, msg):
    msg = json.loads(msg)
    # print(msg)
    #
    # 根据返回的不同数据，作不同的处理
    # k线数据 2000ms推送一次
    if msg.get('e') == 'kline' and msg.get('k').get('x'):  # 仅抓取k线完结时的数据即msg['k']['x']
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg['E'] / 1000))
        symbol = msg['s']
        candle_begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg['k']['t'] / 1000))
        time_interval = msg['k']['i']
        o, c, h, l = msg['k']['o'], msg['k']['c'], msg['k']['h'], msg['k']['l']
        v, n, q, V, Q = msg['k']['v'], msg['k']['n'], msg['k']['q'], msg['k']['V'], msg['k']['Q']
        # print(now, symbol, time_interval)
        # print(candle_begin_time, o, c, h, l)

        df = pd.DataFrame(columns=['candle_begin_time', 'symbol', 'time_interval',
                                   'open', 'high', 'low', 'close',
                                   'volume', 'quote_volume', 'trade_num',
                                   'taker_buy_base_asset_volume',
                                   'taker_buy_quote_asset_volume'
                                   ])
        df.loc[0, 'candle_begin_time'] = candle_begin_time
        df.loc[0, 'symbol'] = symbol
        df.loc[0, 'time_interval'] = time_interval
        df.loc[0, 'open'] = o
        df.loc[0, 'high'] = h
        df.loc[0, 'low'] = l
        df.loc[0, 'close'] = c
        df.loc[0, 'volume'] = v
        df.loc[0, 'quote_volume'] = q
        df.loc[0, 'trade_num'] = n
        df.loc[0, 'taker_buy_base_asset_volume'] = V
        df.loc[0, 'taker_buy_quote_asset_volume'] = Q

        print(df)
        # print(msg['k'])
        save_data_to_csv(df)  # k线数据保存到本地
        save_data_to_mysql(engine, 'k', df)

    # 逐笔交易数据
    if msg.get('e') == 'trade':
        # print(msg)
        pass


def on_close(ws):
    print('调用on_close()函数，关闭与交易所的ws通信')


def on_error(ws, error):
    print(f'error: {error}')


def get_listenKey(key):
    # POST /api/v3/userDataStream
    url = 'https://api.binance.com/api/v3/userDataStream'
    headers = {"X-MBX-APIKEY": key}
    response_data = requests.post(url, headers=headers, timeout=5).json()
    # print(response_data)
    return response_data['listenKey']


def run():
    ws_url = 'wss://stream.binance.com:9443/ws/'

    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close,
                                on_error=on_error,
                                )

    ws.run_forever(ping_interval=60)


def save_data_to_csv(df, data_file='test_data.csv'):
    if os.path.exists(data_file):
        # 若文件存在，则用追加模式mode='a'，且不写入列名header=False
        df.to_csv(data_file, mode='a', index=False, header=False)
    else:
        # 若文件本身不存在，则用写入模式mode='w'，且需要写入列名header=True
        df.to_csv(data_file, mode='w', index=False, header=True)


def save_data_to_mysql(engine, table_name, df):
    '''
    将传入的df插入到mysql数据库中
    '''

    if not df.empty:
        # Columns: [symbol, candle_begin_time, open, low, high, close, volume]
        df.to_sql(table_name, engine, index=False, if_exists='append')
        print(f'成功保存数据至mysql数据库: {table_name}', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        print('df为空，不插入数据。', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return


if __name__ == '__main__':

    # run()

    while True:
        # 放进while循环里，如果与交易所断开了，自动重连
        run()

        time.sleep(2)
