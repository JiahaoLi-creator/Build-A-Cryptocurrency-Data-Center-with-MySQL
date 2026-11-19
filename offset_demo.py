"""Verify default and shifted five-minute bars with public BTCUSDT Spot data and MySQL."""

import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal

from archive import differences, number_text, read_day
from demo import check, values
from feeds import epoch_ms, request_klines
from Function import MINUTE
from storage import get_data_from_mysql, open_engine, save_data_to_mysql
from windows import get_k_data

SYMBOL = "BTCUSDT"
START = datetime(2025, 1, 1)
STEP = 5 * MINUTE
END = START + 2 * STEP


def published_bars():
    rows = request_klines({"symbol": SYMBOL, "interval": "5m", "startTime": epoch_ms(START),
                           "endTime": epoch_ms(END) - 1, "limit": 2})
    opens = [START, START + STEP]
    check([r[0] for r in rows] == [epoch_ms(t) for t in opens], "unexpected public five-minute open times")
    check([r[6] for r in rows] == [epoch_ms(t + STEP) - 1 for t in opens], "unexpected public close times")
    return [(stamp, *(Decimal(value) for value in row[1:6])) for stamp, row in zip(opens, rows, strict=True)]


def replay(engine):
    archive, digest = read_day(SYMBOL, START.date())
    sample = archive[archive.candle_begin_time < END].sort_values("candle_begin_time")
    check(list(sample.candle_begin_time) == [START + i * MINUTE for i in range(10)],
          "expected the first ten consecutive UTC minutes of the archive")
    print(f"{SYMBOL} {START.date()} 1m Spot archive SHA-256 {digest}", flush=True)
    save_data_to_mysql(engine, "k", sample)

    standard = values(get_k_data(SYMBOL, "5m", 2, engine, as_of=END))
    check(standard == published_bars(), "default OHLCV differs from the two published five-minute candles")
    shifted = values(get_k_data(SYMBOL, "5m", 2, engine, as_of=END, offset_minutes=2))
    # Independent reference from the five archive rows with UTC opens 00:02 through 00:06.
    five = sample.iloc[2:7]
    expected = (START + 2 * MINUTE, five.open.iloc[0], max(five.high), min(five.low),
                five.close.iloc[-1], sum(five.volume))
    check(shifted == [expected], "shifted opens or OHLCV differ from the five source minutes")
    closed_at = START + 7 * MINUTE
    before = get_k_data(SYMBOL, "5m", 1, engine, as_of=closed_at - timedelta(microseconds=1), offset_minutes=2)
    on_close = get_k_data(SYMBOL, "5m", 1, engine, as_of=closed_at, offset_minutes=2)
    check(before.empty and values(on_close) == shifted, "the shifted bar must first appear at 00:07:00 UTC")
    stored = get_data_from_mysql(engine, "k", symbol=SYMBOL, begin_time=START, end_time=END, all_fields=True)
    check(len(stored) == 10 and not list(differences(SYMBOL, sample, stored)), "source minutes changed in MySQL")

    print("Offset minutes | UTC open | Open | High | Low | Close | Volume (BTC)")
    for offset, bars in ((0, standard), (2, shifted)):
        for stamp, *fields in bars:
            print(offset, stamp.isoformat() + "Z", *(number_text(n) for n in fields), sep=" | ")
    print("Both default bars match public five-minute candles. All five shifted OHLCV values match the archive calculation.")
    print(f"Shifted bars before 00:07 UTC: {len(before)}. At 00:07 UTC: {len(on_close)}. "
          f"Verified source minutes in MySQL: {len(stored)}.", flush=True)


if __name__ == "__main__":
    if os.getenv("MYSQL_DATABASE") != "offset_replay":
        sys.exit("Run bash scripts/reproduce.sh --offset to start the disposable replay database.")
    engine = open_engine()
    try:
        replay(engine)
    finally:
        engine.dispose()
