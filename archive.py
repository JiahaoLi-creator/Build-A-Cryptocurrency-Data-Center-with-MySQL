"""Import and reconcile checksum-verified Binance Spot one-minute daily archives."""

import argparse
import csv
import hashlib
import io
import re
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests
from sqlalchemy.exc import SQLAlchemyError

from Function import utc_now
from feeds import candle_row, epoch_ms
from storage import FIELDS, KEY, get_data_from_mysql, normalize_symbol, open_engine, save_data_to_mysql
from trading_calendar import check_trading_rows, periods

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"
MICROSECOND_START = date(2025, 1, 1)
DAY = timedelta(days=1)
VALUE_FIELDS = FIELDS[len(KEY):]
REPORT_FIELDS = ("issue", "symbol", "interval", "open_time_utc", "field", "archive_value", "mysql_value", "delta")


def decimal_value(raw):
    value = Decimal(raw)
    if not value.is_finite() or value < 0 or value >= 10**20:
        raise ValueError(f"Invalid nonnegative DECIMAL(38,18) value: {raw}")
    with localcontext() as ctx:
        ctx.prec = 38
        if value != value.quantize(Decimal("1e-18")):
            raise ValueError(f"Value exceeds 18 fractional digits: {raw}")
    return value


def parse_day(data, filename, symbol, day):
    # Spot archives switched both open and close timestamps to microseconds on this UTC date.
    units_per_ms = 1000 if day >= MICROSECOND_START else 1
    first = epoch_ms(datetime.combine(day, time.min)) * units_per_ms
    step = 60_000 * units_per_ms
    member = filename.removesuffix(".zip") + ".csv"
    rows = []
    seen = set()
    with ZipFile(io.BytesIO(data)) as zipped:
        if zipped.namelist() != [member]:
            raise ValueError(f"{filename}: expected one member named {member}")
        with zipped.open(member) as raw, io.TextIOWrapper(raw, encoding="utf-8", newline="") as stream:
            for line, record in enumerate(csv.reader(stream, strict=True), start=1):
                try:
                    t, o, h, low, c, v, close_time, q, trades, buy_base, buy_quote, _ = record
                    t, close_time, trades = int(t), int(close_time), int(trades)
                    if not first <= t < first + 1440 * step or t % step or close_time != t + step - 1:
                        raise ValueError("Expected a closed UTC minute within the archive day")
                    if t in seen:
                        raise ValueError("Duplicate minute in source archive")
                    if not 0 <= trades < 2**64:
                        raise ValueError("Trade count exceeds the unsigned BIGINT range")
                    row = candle_row(symbol, t // units_per_ms, decimal_value(o), decimal_value(h),
                                     decimal_value(low), decimal_value(c), decimal_value(v), decimal_value(q),
                                     trades, decimal_value(buy_base), decimal_value(buy_quote))
                except (ValueError, InvalidOperation) as exc:
                    raise ValueError(f"{member}, line {line}: {exc}") from exc
                seen.add(t)
                rows.append(row)
    if not rows:
        raise ValueError(f"{member}: empty archive")
    return pd.DataFrame(rows, columns=FIELDS)


def read_day(symbol, day, archive_dir=None):
    filename = f"{symbol}-1m-{day.isoformat()}.zip"

    def read(name):
        if archive_dir is not None:
            return (archive_dir / name).read_bytes()
        url = f"{BASE_URL}/{symbol}/1m/{name}"
        with requests.get(url, timeout=(10, 45)) as response:
            response.raise_for_status()
            return response.content

    checksum = read(filename + ".CHECKSUM").decode("ascii").split()
    if (len(checksum) != 2 or not re.fullmatch("[a-fA-F0-9]{64}", checksum[0])
            or checksum[1] not in (filename, "*" + filename)):
        raise ValueError(f"{filename}: invalid SHA-256 manifest or filename")
    data = read(filename)
    digest = hashlib.sha256(data).hexdigest()
    if digest != checksum[0].lower():
        raise ValueError(f"{filename}: SHA-256 mismatch")
    return parse_day(data, filename, symbol, day), digest


def number_text(value):
    if not isinstance(value, Decimal):
        return str(value)
    digits = format(value, "f")
    return digits.rstrip("0").rstrip(".") if "." in digits else digits


def differences(symbol, archive, stored):
    expected = archive.set_index("candle_begin_time")[VALUE_FIELDS].to_dict("index")
    actual = stored.set_index("candle_begin_time")[VALUE_FIELDS].to_dict("index")
    for stamp in sorted(expected.keys() | actual.keys()):
        key = (symbol, "1m", stamp.isoformat(timespec="seconds") + "Z")
        if stamp not in actual:
            yield ("missing_in_mysql", *key, "", "", "", "")
        elif stamp not in expected:
            yield ("unexpected_in_mysql", *key, "", "", "", "")
        else:
            for field in VALUE_FIELDS:
                source, database = expected[stamp][field], actual[stamp][field]
                if source != database:
                    with localcontext() as ctx:
                        ctx.prec = 40  # A difference of two stored DECIMAL(38,18) values is exact.
                        delta = database - source
                    yield ("field_mismatch", *key, field, number_text(source), number_text(database), number_text(delta))


def process(engine, args):
    day = args.start
    count = mismatches = 0
    if args.command == "reconcile":
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow(REPORT_FIELDS)
    while day < args.end:
        start = datetime.combine(day, time.min)
        if any(halt is None for _, _, halt in periods(args.symbol, start, start + DAY)):
            archive, digest = read_day(args.symbol, day, args.archive_dir)
            print(f"{args.symbol} {day}: {len(archive)} archive rows, SHA-256 {digest}", file=sys.stderr)
        else:
            # A fully suspended day expects zero candles. Reconciliation still checks MySQL for stray rows.
            archive = pd.DataFrame(columns=FIELDS)
            print(f"{args.symbol} {day}: trading suspended for the full UTC day, archive skipped.", file=sys.stderr)
        if args.command == "import":
            # One transaction per validated day. Existing WebSocket, REST and archive keys retain their values.
            save_data_to_mysql(engine, "k", archive)
        else:
            check_trading_rows(archive)
            stored = get_data_from_mysql(engine, "k", symbol=args.symbol, begin_time=start, end_time=start + DAY,
                                         all_fields=True)
            for row in differences(args.symbol, archive, stored):
                writer.writerow(row)
                mismatches += 1
        count += len(archive)
        day += DAY
    if args.command == "reconcile":
        label = "difference" if mismatches == 1 else "differences"
        print(f"Reconciled {count} archive rows across {len(VALUE_FIELDS)} fields: {mismatches} {label}.", file=sys.stderr)
    else:
        print(f"Processed {count} source rows for import.", file=sys.stderr)
    return int(bool(mismatches))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("import", "reconcile"))
    parser.add_argument("--symbol", required=True, type=normalize_symbol, help="Spot symbol, e.g. BTCUSDT")
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="First UTC date, inclusive")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="UTC end date, exclusive")
    parser.add_argument("--archive-dir", type=Path, help="Read local ZIP and .CHECKSUM pairs instead of downloading")
    args = parser.parse_args()
    if not args.start < args.end <= utc_now().date():
        parser.error("Choose a range of completed UTC days with an exclusive --end date")
    try:
        engine = open_engine()
        try:
            return process(engine, args)
        finally:
            engine.dispose()
    except (OSError, ValueError, BadZipFile, csv.Error, SQLAlchemyError) as exc:
        print(f"Archive operation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
