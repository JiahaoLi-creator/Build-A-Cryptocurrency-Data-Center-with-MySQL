"""Replay 10 synthetic candles through the production pipeline in a temporary MySQL table."""

import csv
from decimal import Decimal
from pathlib import Path

from Function import MINUTE, parse_utc
from feeds import epoch_ms, ingest_message
from storage import OHLCV, get_data_from_mysql, open_engine
from windows import get_k_data, if_data_available, repair_window

EXAMPLES = Path(__file__).resolve().parent / "examples"
SYMBOL = "BTCUSDT"
TABLE = "demo_k"


def read_csv(name):
    with (EXAMPLES / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def websocket_event(row, closed=True):
    return {"e": "kline", "s": SYMBOL, "k": {
        "i": "1m", "t": epoch_ms(parse_utc(row["utc"])), "x": closed,
        "o": row["open"], "h": row["high"], "l": row["low"], "c": row["close"],
        "v": row["volume"], "q": row["quote_volume"], "n": int(row["trade_num"]),
        "V": row["taker_buy_base_asset_volume"], "Q": row["taker_buy_quote_asset_volume"],
    }}


def rest_row(row):
    t = epoch_ms(parse_utc(row["utc"]))
    return [t, row["open"], row["high"], row["low"], row["close"], row["volume"], t + 59_999,
            row["quote_volume"], int(row["trade_num"]), row["taker_buy_base_asset_volume"],
            row["taker_buy_quote_asset_volume"], "0"]


def check(condition, description):
    if not condition:
        raise AssertionError(description)


def values(frame):
    return [(dt.to_pydatetime(), *row) for dt, row in zip(frame.index, frame[OHLCV].itertuples(index=False, name=None))]


def replay(conn):
    source = read_csv("minutes.csv")
    expected = [(parse_utc(r["utc"]), *(Decimal(r[k]) for k in OHLCV)) for r in read_csv("expected_5m.csv")]
    start, end = parse_utc(source[0]["utc"]), parse_utc(source[-1]["utc"]) + MINUTE
    gap = epoch_ms(parse_utc(source[3]["utc"]))
    calls = []

    def fixture_rest(params):
        calls.append(params.copy())
        check(params == {"symbol": SYMBOL, "interval": "1m", "startTime": gap, "endTime": gap + 59_999, "limit": 1},
              "repair requested an unexpected market or time range")
        return [rest_row(source[3])]

    for run in (1, 2):
        for i in (0, 1, 2, 4, 5, 5, 6, 7, 8):
            ingest_message(conn, websocket_event(source[i]), TABLE)
        ingest_message(conn, websocket_event(source[9], closed=False), TABLE)
        if run == 1:
            check(not if_data_available(conn, TABLE, SYMBOL, end), "an open candle counted as closed")
            check(get_k_data(SYMBOL, "5m", 2, conn, TABLE, as_of=end).empty, "published a partial 5m bar")
        ingest_message(conn, websocket_event(source[9]), TABLE)
        if run == 1:
            rows = get_data_from_mysql(conn, TABLE, SYMBOL, begin_time=start, end_time=end)
            check(len(rows) == 9, "duplicate closed event was stored twice")
            missing = repair_window(conn, TABLE, SYMBOL, start, end, fetch=lambda _: [], now=end)
            check(list(missing) == [parse_utc(source[3]["utc"])], "unavailable gap was hidden")
            partial = get_k_data(SYMBOL, "5m", 2, conn, TABLE, as_of=end)
            check(values(partial) == expected[1:], "returned incomplete first 5m bar")
            print("Before repair: 9 unique minutes; 00:03 missing; only the 00:05 bar is complete.")

        remaining = repair_window(conn, TABLE, SYMBOL, start, end, fetch=fixture_rest, now=end)
        check(remaining.empty and len(calls) == 1, "gap repair must succeed once and skip it on replay")
        rows = get_data_from_mysql(conn, TABLE, SYMBOL, begin_time=start, end_time=end)
        actual_minutes = [(r.candle_begin_time, *(getattr(r, col) for col in OHLCV)) for r in rows.itertuples()]
        expected_minutes = [(parse_utc(r["utc"]), *(Decimal(r[col]) for col in OHLCV)) for r in source]
        check(actual_minutes == expected_minutes, "stored minute values differ from the independent fixture")
        bars = get_k_data(SYMBOL, "5m", 2, conn, TABLE, as_of=end)
        check(values(bars) == expected, "5m OHLCV differs from examples/expected_5m.csv")
        check(values(get_k_data(SYMBOL, "5m", 1, conn, TABLE, as_of=end)) == expected[-1:], "limit=1 failed")
        check(if_data_available(conn, TABLE, SYMBOL, end), "latest closed minute missing")
        check(not if_data_available(conn, TABLE, SYMBOL, end + MINUTE), "stale data reported as current")
        print(f"Pass {run}: {len(rows)} unique minutes, {len(bars)} complete 5m bars, {len(calls)} total successful REST requests.")
    print("UTC open             open  high  low  close  volume")
    for stamp, *numbers in values(bars):
        print(stamp, *(format(n.normalize(), "f") for n in numbers), sep="  ")


def main():
    engine = open_engine()
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE TEMPORARY TABLE demo_k LIKE k")
            try:
                replay(conn)
            finally:
                conn.exec_driver_sql("DROP TEMPORARY TABLE demo_k")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
