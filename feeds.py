"""Binance Spot public 1m WebSocket and REST formats, normalized to UTC."""

import sys
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests

from Function import EPOCH, utc_now
from storage import FIELDS, KEY, save_data_to_mysql
from trading_calendar import SuspendedCandleError, load_calendar, periods

REST_URL = "https://data-api.binance.vision/api/v3/klines"
WS_URL = "wss://data-stream.binance.vision/ws"
MILLISECOND = timedelta(milliseconds=1)


def epoch_ms(utc_time):
    return (utc_time - EPOCH) // MILLISECOND


def candle_row(symbol, open_ms, o, h, l, c, volume, quote, trades, buy_base, buy_quote):
    if int(open_ms) % 60_000:
        raise ValueError("Expected a minute-aligned Binance open time")
    return dict(zip(FIELDS, [symbol, "1m", EPOCH + int(open_ms) * MILLISECOND,
                             o, h, l, c, volume, quote, int(trades), buy_base, buy_quote], strict=True))


def save_data_to_csv(df, data_file="candles_utc.csv"):
    path = Path(data_file)
    columns = ["candle_begin_time", "symbol", "time_interval", *FIELDS[3:]]
    df.to_csv(path, columns=columns, mode="a", index=False, header=not path.exists() or path.stat().st_size == 0)


def ingest_message(engine, message, table_name="k", csv_file=None):
    if message.get("e") != "kline":
        return None
    k = message["k"]
    if k["x"] is not True or k["i"] != "1m":
        return None
    row = candle_row(message["s"], k["t"], k["o"], k["h"], k["l"], k["c"],
                     k["v"], k["q"], k["n"], k["V"], k["Q"])
    frame = pd.DataFrame([row])
    try:
        save_data_to_mysql(engine, table_name, frame)
    except SuspendedCandleError as exc:
        print(f"Ignoring WebSocket event: {exc}", file=sys.stderr, flush=True)
        return None
    if csv_file:
        save_data_to_csv(frame, csv_file)
    return row


def request_klines(params):
    for attempt in range(3):
        try:
            response = requests.get(REST_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
        except requests.HTTPError as exc:
            if exc.response.status_code < 500 or attempt == 2:
                raise
        time.sleep(2 ** attempt)


def trading_requests(params_list):
    """Keep direct backfill calls inside trading periods, including calls with an unaligned endpoint."""
    for params in params_list:
        if params["interval"] != "1m":
            raise ValueError("Only Binance Spot 1m input is supported")
        if params["symbol"].upper() not in load_calendar():
            yield params
            continue
        first = params["startTime"]
        stop = first + params.get("limit", 1) * 60_000
        stop = min(stop, params.get("endTime", stop - 1) + 1)
        # Only UTC minute opens at or above first and strictly below stop can be returned.
        first_open = ((first + 59_999) // 60_000) * 60_000
        if first_open >= stop:
            continue
        grid_end = ((stop + 59_999) // 60_000) * 60_000
        for left, right, halt in periods(params["symbol"], EPOCH + first_open * MILLISECOND, EPOCH + grid_end * MILLISECOND):
            if halt is None:
                exclusive = min(stop, epoch_ms(right))
                for begin in range(epoch_ms(left), exclusive, 1000 * 60_000):
                    end = min(begin + 1000 * 60_000, exclusive)
                    yield {**params, "startTime": begin, "endTime": end - 1, "limit": (end - begin + 59_999) // 60_000}


def get_data_via_http(params_list, fetch=None, now=None):
    """Accept only requested, closed 1m rows. Ignore a later open time returned for a gap."""
    fetch = request_klines if fetch is None else fetch
    cutoff = epoch_ms(now if now is not None else utc_now())
    rows = []
    for params in trading_requests(params_list):
        first = params["startTime"]
        stop = min(first + params.get("limit", 1) * 60_000, params.get("endTime", cutoff) + 1)
        for record in fetch(params):
            t, o, h, l, c, v, close_ms, q, n, buy_base, buy_quote, _ = record
            if first <= t < stop and close_ms == t + 59_999 and close_ms < cutoff:
                rows.append(candle_row(params["symbol"], t, o, h, l, c, v, q, n, buy_base, buy_quote))
    return pd.DataFrame(rows, columns=FIELDS).drop_duplicates(subset=KEY)
