"""MySQL access shared by real market data and the offline replay."""

import os
import re

import pandas as pd
from sqlalchemy import URL, create_engine, inspect, text
from sqlalchemy.engine import Connection

KEY = ["symbol", "time_interval", "candle_begin_time"]
OHLCV = ["open", "high", "low", "close", "volume"]
FIELDS = KEY + OHLCV + [
    "quote_volume", "trade_num", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
]
DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,DOGEUSDT,SUSHIUSDT,FILUSDT,UNIUSDT,LINKUSDT,LTCUSDT,DOTUSDT"


def normalize_symbol(value):
    symbol = value.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,32}", symbol):
        raise ValueError(f"Expected a Spot symbol such as BTCUSDT, got {value!r}")
    return symbol


def symbols():
    return list(dict.fromkeys(normalize_symbol(x) for x in os.getenv("SYMBOLS", DEFAULT_SYMBOLS).split(",")))


def open_engine():
    socket = os.getenv("MYSQL_UNIX_PORT")
    url = URL.create("mysql+pymysql", username=os.getenv("MYSQL_USER", "kline"),
                     password=os.getenv("MYSQL_PASSWORD", ""), host=os.getenv("MYSQL_HOST", "127.0.0.1"),
                     port=int(os.getenv("MYSQL_PORT", "3306")), database=os.getenv("MYSQL_DATABASE", "binance_utc"),
                     query={"unix_socket": socket} if socket else {})
    engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 8})
    try:
        indexes = inspect(engine).get_indexes("k")
        if not any(i["unique"] and i["column_names"] == KEY for i in indexes):
            raise ValueError("k needs the UTC schema and unique candle key in schema.sql")
    except Exception:
        engine.dispose()
        raise
    return engine


def table_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid table name {name!r}")
    return name


def get_data_from_mysql(engine, table_name, symbol="BTCUSDT", time_interval="1m",
                        begin_time=None, limit=100, sql="", end_time=None, *, all_fields=False):
    """Read a half-open UTC range, or the latest limit rows in descending order."""
    if sql:
        return pd.read_sql_query(text(sql), engine, coerce_float=False)
    conditions = ["time_interval = :interval"]
    params = {"interval": time_interval}
    if symbol:
        conditions.append("symbol = :symbol")
        params["symbol"] = symbol
    if begin_time is not None:
        conditions.append("candle_begin_time >= :begin")
        params["begin"] = begin_time
    if end_time is not None:
        conditions.append("candle_begin_time < :end")
        params["end"] = end_time
    columns = FIELDS if all_fields else ["symbol", "candle_begin_time", "time_interval", *OHLCV]
    query = f"SELECT {', '.join(columns)} FROM {table_identifier(table_name)} WHERE " + " AND ".join(conditions)
    if begin_time is None:
        if limit < 1:
            raise ValueError("limit must be positive")
        query += " ORDER BY candle_begin_time DESC LIMIT :limit"
        params["limit"] = int(limit)
    else:
        query += " ORDER BY candle_begin_time"
    return pd.read_sql_query(text(query), engine, params=params, coerce_float=False)


def save_data_to_mysql(engine, table_name, df):
    """Insert closed candles. The first stored row for a candle key wins."""
    if df.empty:
        return
    rows = df[FIELDS].to_dict("records")
    values = ", ".join(":" + k for k in FIELDS)
    stmt = text(f"INSERT INTO {table_identifier(table_name)} ({', '.join(FIELDS)}) VALUES ({values}) "
                "ON DUPLICATE KEY UPDATE id = id")
    if isinstance(engine, Connection):
        engine.execute(stmt, rows)
    else:
        with engine.begin() as conn:
            conn.execute(stmt, rows)
