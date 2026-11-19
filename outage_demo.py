"""Replay BTCUSDT WebSocket boundaries and recover the missing candles from public REST into MySQL."""

import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
from datetime import datetime
from importlib import import_module
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text

import recovery
from archive import differences, read_day
from demo import check, websocket_event
from feeds import epoch_ms, request_klines
from Function import MINUTE
from storage import get_data_from_mysql, open_engine

SYMBOL = "BTCUSDT"
GAP_START = datetime(2025, 1, 1, 1)
GAP_END = datetime(2025, 1, 1, 4)
ANCHOR = GAP_START - MINUTE


def collector_process(stage, path):
    collector = import_module("1_binance_data_to_mysql")
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    live_row = rows[0] if stage == "seed" else rows[-1]
    started = {"seed": GAP_START, "restart": GAP_END, "repeat": GAP_END + MINUTE}[stage]
    rest_started, live_saved = threading.Event(), threading.Event()
    calls = []
    expected_request = {"symbol": SYMBOL, "interval": "1m", "startTime": epoch_ms(GAP_START),
                        "endTime": epoch_ms(GAP_END) - 1, "limit": 180}

    def clock():
        return GAP_END + MINUTE if stage == "restart" and live_saved.is_set() else started

    def market_rest(params):
        calls.append(params.copy())
        check(stage == "restart" and params == expected_request, f"{stage}: unexpected REST request {params}")
        rest_started.set()
        response = request_klines(params)
        print(f"Public Binance Spot REST returned {len(response)} candles.", flush=True)
        check(live_saved.wait(10), "the live collector blocked while REST recovery was in flight")
        db = open_engine()
        try:
            with db.connect() as conn:
                latest = conn.scalar(text("SELECT MAX(candle_begin_time) FROM k WHERE symbol='BTCUSDT'"))
            check(latest == GAP_END and recovery.checkpoint(db, SYMBOL, started) == GAP_START,
                  "a later live close skipped the outstanding outage")
        finally:
            db.dispose()
        return response

    class RecordedSocket:
        def __init__(self, url, *, on_open, on_message, on_error, on_close):
            self.on_open, self.on_message = on_open, on_message

        def send(self, message):
            request = json.loads(message)
            check(request["method"] == "SUBSCRIBE" and request["params"] == ["btcusdt@kline_1m"],
                  "unexpected WebSocket subscription")

        def run_forever(self, **options):
            self.on_open(self)
            if stage == "restart":
                check(rest_started.wait(10), "startup never requested the missing candles")
            self.on_message(self, json.dumps(websocket_event(live_row)))
            live_saved.set()

        def close(self):
            pass

    with (patch.object(sys, "argv", ["1_binance_data_to_mysql.py", "--once"]),
          patch.object(collector, "utc_now", clock), patch.object(recovery, "utc_now", clock),
          patch.object(collector.websocket, "WebSocketApp", RecordedSocket),
          patch("feeds.request_klines", market_rest)):
        check(collector.main() == 0, f"{stage} collector failed")
    expected = [expected_request] if stage == "restart" else []
    check(calls == expected, f"{stage} made unexpected REST requests: {calls}")
    print(f"{stage}: REST requests={len(calls)}, requested minutes={sum(call['limit'] for call in calls)}", flush=True)


def verify_stage(engine, stage, reference):
    with engine.connect() as conn:
        total, unique = conn.execute(text(
            "SELECT COUNT(*), COUNT(DISTINCT symbol, time_interval, candle_begin_time) FROM k"
        )).one()
        gap_rows, gap_keys = conn.execute(text(
            "SELECT COUNT(*), COUNT(DISTINCT candle_begin_time) FROM k WHERE symbol=:symbol AND time_interval='1m' "
            "AND candle_begin_time >= :start AND candle_begin_time < :end"
        ), {"symbol": SYMBOL, "start": GAP_START, "end": GAP_END}).one()
        position = conn.scalar(text("SELECT next_open_time FROM collector_checkpoint WHERE symbol=:symbol"),
                               {"symbol": SYMBOL})
    wanted_gap = 0 if stage == "seed" else 180
    check((total, unique, gap_rows, gap_keys) == (len(reference), len(reference), wanted_gap, wanted_gap),
          f"{stage} database counts: {(total, unique, gap_rows, gap_keys)}")
    check(position == (GAP_START if stage == "seed" else GAP_END + MINUTE), f"{stage} checkpoint: {position}")
    stored = get_data_from_mysql(engine, "k", symbol=SYMBOL, begin_time=ANCHOR, end_time=GAP_END + MINUTE,
                                 all_fields=True)
    check(not list(differences(SYMBOL, reference, stored)), f"{stage} values differ from the public archive")
    print(f"{stage}: outage rows={gap_rows}, unique outage minutes={gap_keys}, total rows={total}, "
          f"unique candle keys={unique}, archive differences=0, next={position} UTC", flush=True)


def replay():
    day, digest = read_day(SYMBOL, GAP_START.date())
    reference = day[day.candle_begin_time.between(ANCHOR, GAP_END)].sort_values("candle_begin_time")
    check(len(reference) == 182, "expected both boundary candles and all 180 outage minutes")
    print(f"BTCUSDT 2025-01-01 Spot archive SHA-256 {digest}", flush=True)
    engine = open_engine()
    try:
        with engine.connect() as conn:
            check(conn.scalar(text("SELECT COUNT(*) FROM k")) == 0 and
                  conn.scalar(text("SELECT COUNT(*) FROM collector_checkpoint")) == 0,
                  "the replay requires empty candle and checkpoint tables")
        with tempfile.TemporaryDirectory() as work:
            fixture = Path(work) / "websocket_closes.csv"
            reference.iloc[[0, -1]].rename(columns={"candle_begin_time": "utc"}).to_csv(fixture, index=False)
            for stage in ("seed", "restart", "repeat"):
                subprocess.run([sys.executable, __file__, stage, str(fixture)], check=True, timeout=60,
                               env={**os.environ, "SYMBOLS": SYMBOL, "CSV_FILE": ""})
                verify_stage(engine, stage, reference.iloc[:1] if stage == "seed" else reference)
    finally:
        engine.dispose()


if __name__ == "__main__":
    if os.getenv("MYSQL_DATABASE") != "outage_replay":
        sys.exit("Run bash scripts/reproduce.sh --outage to start the disposable replay database.")
    if len(sys.argv) == 1:
        replay()
    else:
        collector_process(sys.argv[1], Path(sys.argv[2]))
