"""Repair missing Binance Spot candles in a bounded window of closed UTC minutes."""

import argparse
import sys

import requests

from Function import MINUTE, floor_time, parse_utc, sleep_until_run_time, utc_now
from feeds import get_data_via_http
from storage import get_data_from_mysql, open_engine, save_data_to_mysql, symbols
from windows import check_missing_data, repair_window


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Check once and exit")
    parser.add_argument("--minutes", type=int, default=100, help="Length of the repair window")
    parser.add_argument("--end", type=parse_utc, help="Exclusive UTC end, e.g. 2024-01-01T00:10:00Z")
    args = parser.parse_args()
    if args.minutes < 1 or (args.end is not None and not args.once):
        parser.error("--minutes must be positive and --end requires --once")
    engine = open_engine()
    try:
        while True:
            end = args.end or floor_time(utc_now(), MINUTE)
            start = end - args.minutes * MINUTE
            status = 0
            for symbol in symbols():
                try:
                    remaining = repair_window(engine, "k", symbol, start, end)
                except requests.RequestException as exc:
                    print(f"{symbol} REST repair failed: {exc}", file=sys.stderr, flush=True)
                    status = 1
                    break
                print(f"{symbol} [{start}, {end}) UTC | missing={len(remaining)}", flush=True)
                if len(remaining):
                    status = 1
            if args.once:
                return status
            sleep_until_run_time("30m")
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
