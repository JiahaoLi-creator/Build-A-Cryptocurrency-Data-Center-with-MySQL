"""Collect closed Binance Spot minute candles via the public WebSocket."""

import argparse
import json
import os
import sys
import threading
import time

import websocket

from feeds import WS_URL, ingest_message, save_data_to_csv
from storage import open_engine, save_data_to_mysql, symbols


def on_open(ws):
    streams = [symbol.lower() + "@kline_1m" for symbol in symbols()]
    ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))


def on_close(ws, status=None, message=None):
    print(f"WebSocket closed ({status}) {message or ''}", flush=True)


def run(engine=None, once=False):
    db = engine if engine is not None else open_engine()
    saved = False
    failures = []

    def on_message(ws, raw):
        nonlocal saved
        try:
            message = json.loads(raw)
            if "code" in message:
                raise RuntimeError(str(message))
            row = ingest_message(db, message, csv_file=os.getenv("CSV_FILE", "candles_utc.csv"))
            if row is not None:
                print(f"{row['symbol']} {row['candle_begin_time']} UTC close={row['close']}", flush=True)
                saved = True
                if once:
                    ws.close()
        except Exception as exc:
            failures.append(exc)
            ws.close()

    def on_error(ws, error):
        failures.append(error)

    app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message,
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
    parser.add_argument("--once", action="store_true", help="Exit after saving one closed minute")
    args = parser.parse_args()
    engine = open_engine()
    try:
        while True:
            try:
                run(engine, once=args.once)
            except RuntimeError as exc:
                print(exc, file=sys.stderr, flush=True)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(2)
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
