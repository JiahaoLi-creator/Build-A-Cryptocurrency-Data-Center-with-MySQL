"""Read up-to-date, complete Spot OHLCV bars from MySQL."""

import argparse
import time

from Function import MINUTE, floor_time, interval_delta, minute_offset, parse_utc, sleep_until_run_time, utc_now
from storage import get_data_from_mysql, open_engine, symbols
from trading_calendar import eligible_bar_count, suspension_at
from windows import get_k_data, get_newest_candle_begin_time, if_data_available


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--offset-minutes", type=int, default=0,
                        help="Shift UTC bar opens by this many whole minutes (default: 0)")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--as-of", type=parse_utc, help="Fixed UTC cutoff, e.g. 2024-01-01T00:10:00Z")
    args = parser.parse_args()
    try:
        step = interval_delta(args.interval)
        minute_offset(step, args.offset_minutes)
    except ValueError as exc:
        parser.error(str(exc))
    if args.limit < 1 or (args.as_of is not None and not args.once):
        parser.error("--limit must be positive and --as-of requires --once")
    engine = open_engine()
    try:
        while True:
            cutoff = args.as_of or utc_now()
            status = 0
            for symbol in symbols():
                latest = floor_time(cutoff, MINUTE) - MINUTE
                halt = suspension_at(symbol, latest)
                if halt:
                    print(f"{symbol}: trading suspended at {latest} UTC | {halt.reason}", flush=True)
                else:
                    attempts = 1 if args.as_of is not None else 6
                    for attempt in range(attempts):
                        if if_data_available(engine, "k", symbol, cutoff):
                            break
                        if attempt + 1 < attempts:
                            time.sleep(1)
                    else:
                        print(f"{symbol}: latest closed minute missing at {cutoff} UTC, skipping", flush=True)
                        status = 1
                        continue
                eligible = eligible_bar_count(symbol, cutoff, step, args.limit, offset_minutes=args.offset_minutes)
                bars = get_k_data(symbol, args.interval, args.limit, engine, as_of=cutoff,
                                  offset_minutes=args.offset_minutes)
                print(f"{symbol} | complete={len(bars)}/{eligible} | suspended windows={args.limit - eligible}"
                      f" | offset minutes={args.offset_minutes}\n{bars.to_string()}", flush=True)
                if len(bars) < eligible:
                    status = 1
            if args.once:
                return status
            sleep_until_run_time(args.interval, offset_minutes=args.offset_minutes)
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
