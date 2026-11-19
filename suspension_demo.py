"""Replay the VIDTUSDT delisting and relisting with public Spot archives, REST and MySQL."""

import os
import sys
from datetime import datetime
from importlib import import_module
from unittest.mock import patch

import pandas as pd
import requests
from sqlalchemy import text

import recovery
from archive import differences, read_day
from demo import check, recorded_socket, websocket_event
from feeds import REST_URL, epoch_ms
from Function import MINUTE
from storage import get_data_from_mysql, open_engine, save_data_to_mysql
from windows import get_k_data

SYMBOL = "VIDTUSDT"
HALT = datetime(2022, 10, 31, 3)
RESUME = datetime(2022, 11, 9, 8)
START, END = HALT - 5 * MINUTE, RESUME + 5 * MINUTE
OMITTED = RESUME + 2 * MINUTE


def boundary_candles():
    before, before_hash = read_day(SYMBOL, HALT.date())
    after, after_hash = read_day(SYMBOL, RESUME.date())
    check(before.candle_begin_time.max() == HALT - MINUTE, "unexpected last pre-suspension open")
    check(after.candle_begin_time.min() == RESUME, "unexpected first relisted open")
    print(f"Final pre-suspension open: {HALT - MINUTE} UTC, archive SHA-256 {before_hash}")
    print(f"First relisted open: {RESUME} UTC, archive SHA-256 {after_hash}", flush=True)
    sample = pd.concat([before[before.candle_begin_time >= START], after[after.candle_begin_time < END]])
    expected = list(pd.date_range(START, periods=5, freq="min")) + list(pd.date_range(RESUME, periods=5, freq="min"))
    check(list(sample.candle_begin_time) == expected, "the archived boundary minutes do not match the replay")
    return sample


def verify(engine, reference, label):
    with engine.connect() as conn:
        rows, unique, suspended = conn.execute(text(
            "SELECT COUNT(*), COUNT(DISTINCT candle_begin_time), "
            "COUNT(CASE WHEN candle_begin_time >= :halt AND candle_begin_time < :resume THEN 1 END) "
            "FROM k WHERE symbol=:symbol AND time_interval='1m'"
        ), {"symbol": SYMBOL, "halt": HALT, "resume": RESUME}).one()
    check((rows, unique, suspended) == (len(reference), len(reference), 0),
          f"{label}: unexpected raw counts {(rows, unique, suspended)}")
    stored = get_data_from_mysql(engine, "k", symbol=SYMBOL, begin_time=START, end_time=END, all_fields=True)
    check(not list(differences(SYMBOL, reference, stored)), f"{label}: stored fields differ from the archive")
    bars = get_k_data(SYMBOL, "5m", (END - START) // (5 * MINUTE), engine, as_of=END)
    suspended_bars = ((bars.index >= HALT) & (bars.index < RESUME)).sum()
    expected_opens = [START, RESUME] if OMITTED in set(reference.candle_begin_time) else [START]
    check(list(bars.index) == expected_opens, f"{label}: unexpected five-minute bar opens {list(bars.index)}")
    print(f"{label}: unique minutes={unique}, raw rows during suspension={suspended}, "
          f"five-minute bars={len(bars)}, bars during suspension={suspended_bars}, archive field differences=0", flush=True)


def replay():
    reference = boundary_candles()
    seeded = reference[reference.candle_begin_time != OMITTED]
    collector = import_module("1_binance_data_to_mysql")
    audit = import_module("2_check_data")
    last_row = reference.iloc[-1].to_dict()
    last_row["utc"] = last_row["candle_begin_time"].isoformat()
    socket = recorded_socket(SYMBOL, websocket_event(last_row, symbol=SYMBOL))
    expected = {"symbol": SYMBOL, "interval": "1m", "startTime": epoch_ms(OMITTED),
                "endTime": epoch_ms(OMITTED + MINUTE) - 1, "limit": 1}
    real_get, calls = requests.get, []

    def traced_get(url, *, params, **options):
        calls.append(params.copy())
        check(params["endTime"] < epoch_ms(HALT) or params["startTime"] >= epoch_ms(RESUME),
              f"REST request intersects the suspension: {params}")
        check(url == REST_URL and params == expected, f"unexpected backfill request: {url} {params}")
        print(f"Public Spot REST request: {SYMBOL} {OMITTED} UTC, limit=1", flush=True)
        return real_get(url, params=params, **options)

    engine = open_engine()
    try:
        with engine.connect() as conn:
            check(conn.scalar(text("SELECT COUNT(*) FROM k")) == 0 and
                  conn.scalar(text("SELECT COUNT(*) FROM collector_checkpoint")) == 0, "expected an empty replay database")
        save_data_to_mysql(engine, "k", seeded)
        verify(engine, seeded, "Before recovery")
        with (patch.dict(os.environ, {"SYMBOLS": SYMBOL, "CSV_FILE": ""}),
              patch("feeds.requests.get", traced_get)):
            with patch.object(sys, "argv", ["2_check_data.py", "--once", "--minutes", "13260",
                                            "--end", RESUME.isoformat() + "Z"]):
                check(audit.main() == 0 and not calls, "a suspension-only audit queried REST or reported a gap")
            with (patch.object(sys, "argv", ["1_binance_data_to_mysql.py", "--once"]),
                  patch.object(collector, "utc_now", return_value=END), patch.object(recovery, "utc_now", return_value=END),
                  patch.object(collector.websocket, "WebSocketApp", socket)):
                for label in ("Automatic recovery", "Repeat startup"):
                    previous = len(calls)
                    check(collector.main() == 0 and calls == [expected], f"{label}: unexpected REST history {calls}")
                    verify(engine, reference, label)
                    stage_calls = calls[previous:]
                    overlapping = sum(p["startTime"] < epoch_ms(RESUME) and p["endTime"] >= epoch_ms(HALT)
                                      for p in stage_calls)
                    print(f"{label}: REST requests={len(stage_calls)}, requests overlapping suspension={overlapping}", flush=True)
        with engine.connect() as conn:
            position = conn.scalar(text("SELECT next_open_time FROM collector_checkpoint WHERE symbol=:symbol"),
                                   {"symbol": SYMBOL})
            check(position == END, f"checkpoint did not cross the suspension: {position}")
        print(f"Recovered {OMITTED} UTC. Next unverified open: {position} UTC.", flush=True)
    finally:
        engine.dispose()


if __name__ == "__main__":
    if os.getenv("MYSQL_DATABASE") != "suspension_replay":
        sys.exit("Run bash scripts/reproduce.sh --suspension to start the disposable replay database.")
    replay()
