"""UTC half-open repair and reporting windows for closed one-minute candles."""

from decimal import localcontext

import pandas as pd
from sqlalchemy import text

from Function import MINUTE, floor_time, interval_delta, utc_now
from feeds import epoch_ms, get_data_via_http
from storage import OHLCV, get_data_from_mysql, save_data_to_mysql, table_identifier


def missing_minutes(frame, start, end):
    if start >= end or floor_time(start, MINUTE) != start or floor_time(end, MINUTE) != end:
        raise ValueError("Expected a nonempty half-open range aligned to UTC minutes")
    expected = pd.date_range(start, end, freq="min", inclusive="left")
    return expected.difference(pd.DatetimeIndex(frame["candle_begin_time"]))


def gap_requests(missing, symbol):
    """Coalesce consecutive missing opens into bounded REST requests."""
    batches = []
    for minute in missing:
        t = epoch_ms(minute)
        if batches and batches[-1]["endTime"] + 1 == t and batches[-1]["limit"] < 1000:
            batches[-1]["endTime"] = t + 59_999
            batches[-1]["limit"] += 1
        else:
            batches.append({"symbol": symbol, "interval": "1m", "startTime": t, "endTime": t + 59_999, "limit": 1})
    return batches


def check_missing_data(df):
    """Original helper for gaps between the first and last stored minute of one symbol."""
    if df.empty:
        return []
    if df["symbol"].nunique() != 1 or set(df["time_interval"]) != {"1m"}:
        raise ValueError("Expected one symbol and 1m source candles")
    times = df["candle_begin_time"]
    return gap_requests(missing_minutes(df, times.min(), times.max() + MINUTE), df["symbol"].iloc[0])


def repair_window(engine, table_name, symbol, start, end, fetch=None, now=None):
    cutoff = utc_now() if now is None else now
    if end > floor_time(cutoff, MINUTE):
        raise ValueError("The repair range includes an open or future candle")
    bounds = dict(symbol=symbol, begin_time=start, end_time=end)
    stored = get_data_from_mysql(engine, table_name, **bounds)
    gaps = missing_minutes(stored, start, end)
    if len(gaps):
        recovered = get_data_via_http(gap_requests(gaps, symbol), fetch=fetch, now=cutoff)
        save_data_to_mysql(engine, table_name, recovered)
        stored = get_data_from_mysql(engine, table_name, **bounds)
    return missing_minutes(stored, start, end)


def get_newest_candle_begin_time(engine=None, table_name="k", symbol="BTCUSDT"):
    stmt = text(f"SELECT MAX(candle_begin_time) FROM {table_identifier(table_name)} "
                "WHERE symbol=:symbol AND time_interval='1m'")
    return pd.read_sql_query(stmt, engine, params={"symbol": symbol}).iloc[0, 0]


def if_data_available(engine=None, table_name="k", symbol="BTCUSDT", as_of=None):
    end = floor_time(utc_now() if as_of is None else as_of, MINUTE)
    row = get_data_from_mysql(engine, table_name, symbol=symbol, begin_time=end - MINUTE, end_time=end)
    return not row.empty


def get_k_data(symbol="BTCUSDT", target_time_interval="15m", limit=100,
               engine=None, table_name="k", time_interval="1m", as_of=None):
    """Return complete bars within the last limit closed target windows, indexed by UTC open."""
    step = interval_delta(target_time_interval)
    if time_interval != "1m" or limit < 1:
        raise ValueError("Expected 1m inputs and a positive number of target bars")
    end = floor_time(utc_now() if as_of is None else as_of, step)
    frame = get_data_from_mysql(engine, table_name, symbol=symbol, time_interval=time_interval,
                                begin_time=end - limit * step, end_time=end)
    frame = frame.set_index("candle_begin_time")
    frame.index = pd.DatetimeIndex(frame.index)
    frame = frame.sort_index()
    # A full bucket has every distinct minute on the grid exactly once.
    on_grid = frame.index == frame.index.floor("min")
    frame = frame.loc[on_grid & ~frame.index.duplicated(keep="first")]
    groups = frame.resample(step, origin="epoch", closed="left", label="left")
    with localcontext() as precision:
        precision.prec = 65  # Preserve DECIMAL prices and summed quantities from MySQL.
        bars = groups.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    return bars.loc[groups.size() == step // MINUTE, OHLCV]
