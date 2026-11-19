"""Read up-to-date, complete Spot OHLCV bars from MySQL."""

import argparse
import time

from Function import interval_delta, parse_utc, sleep_until_run_time, utc_now
from storage import get_data_from_mysql, open_engine, symbols
from windows import get_k_data, get_newest_candle_begin_time, if_data_available


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--as-of", type=parse_utc, help="Fixed UTC cutoff, e.g. 2024-01-01T00:10:00Z")
    args = parser.parse_args()
    interval_delta(args.interval)
    if args.limit < 1 or (args.as_of is not None and not args.once):
        parser.error("--limit must be positive and --as-of requires --once")
    engine = open_engine()
    try:
        while True:
            cutoff = args.as_of or utc_now()
            status = 0
            for symbol in symbols():
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
                bars = get_k_data(symbol, args.interval, args.limit, engine, as_of=cutoff)
                print(f"{symbol} | complete={len(bars)}/{args.limit}\n{bars.to_string()}", flush=True)
                if len(bars) < args.limit:
                    status = 1
            if args.once:
                return status
            sleep_until_run_time(args.interval)
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
