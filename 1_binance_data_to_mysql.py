"""Collect closed Binance Spot minute candles via the public WebSocket."""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import websocket
from sqlalchemy.exc import SQLAlchemyError

from Function import MINUTE, floor_time, utc_now
from feeds import WS_URL, ingest_message, save_data_to_csv
from recovery import advance_checkpoint, catch_up, checkpoint, maintain_history
from storage import open_engine, save_data_to_mysql, symbols


def on_open(ws):
    streams = [symbol.lower() + "@kline_1m" for symbol in symbols()]
    ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))


def on_close(ws, status=None, message=None):
    print(f"WebSocket closed ({status}) {message or ''}", flush=True)


def run(engine=None, once=False, *, connected=None):
    db = engine if engine is not None else open_engine()
    saved = False
    failures = []

    def subscribe(ws):
        on_open(ws)
        if connected is not None:
            connected.set()

    def on_message(ws, raw):
        nonlocal saved
        try:
            message = json.loads(raw)
            if "code" in message:
                raise RuntimeError(str(message))
            row = ingest_message(db, message, csv_file=os.getenv("CSV_FILE", "candles_utc.csv"))
            if row is not None:
                opened = row["candle_begin_time"]
                advance_checkpoint(db, row["symbol"], opened, opened + MINUTE)
                print(f"{row['symbol']} {row['candle_begin_time']} UTC close={row['close']}", flush=True)
                saved = True
                if once:
                    ws.close()
        except Exception as exc:
            failures.append(exc)
            ws.close()

    def on_error(ws, error):
        failures.append(error)

    app = websocket.WebSocketApp(WS_URL, on_open=subscribe, on_message=on_message,
                                 on_error=on_error, on_close=on_close)
    deadline = threading.Timer(90, app.close) if once else None
    if deadline:
        deadline.daemon = True
        deadline.start()
    try:
        app.run_forever(ping_interval=45, ping_timeout=15)
    finally:
        if deadline:
            deadline.cancel()
        if engine is None:
            db.dispose()
    if failures:
        raise RuntimeError(str(failures[0]))
    if once and not saved:
        raise RuntimeError("No closed candle received within the WebSocket session")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Finish one startup recovery pass and save one live close")
    args = parser.parse_args()
    engine = open_engine()
    try:
        selected = symbols()
        cutoff = floor_time(utc_now(), MINUTE)
        # Establish the tracked range before any new live candle can arrive.
        for symbol in selected:
            checkpoint(engine, symbol, cutoff)
        wake, stopping = threading.Event(), threading.Event()
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="recovery") as pool:
            history = pool.submit(maintain_history, engine, selected, wake, stopping, args.once)
            try:
                while True:
                    live_ok = True
                    try:
                        run(engine, once=args.once, connected=wake)
                    except RuntimeError as exc:
                        print(exc, file=sys.stderr, flush=True)
                        live_ok = False
                    if args.once:
                        if not history.result() or not live_ok:
                            return 1
                        # The socket may have received a newer close while the startup REST request was in flight.
                        return 0 if catch_up(engine, selected, floor_time(utc_now(), MINUTE)) else 1
                    time.sleep(2)
            finally:
                stopping.set()
                wake.set()
    except SQLAlchemyError as exc:
        print(f"Collector database error. Check the connection and apply schema.sql: {exc}", file=sys.stderr)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
