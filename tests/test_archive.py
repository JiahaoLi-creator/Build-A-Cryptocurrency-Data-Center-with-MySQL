import csv
import hashlib
import io
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from zipfile import ZipFile

from archive import VALUE_FIELDS, differences, parse_day, read_day
from feeds import epoch_ms

SYMBOL = "BTCUSDT"
OLD_DAY = date(2024, 12, 31)
NEW_DAY = date(2025, 1, 1)


def record(day, minute=0, scale=1):
    t = (epoch_ms(datetime(day.year, day.month, day.day)) + minute * 60_000) * scale
    return [t, "12", "14", "11", "13", "8.123456789012345678", t + 60_000 * scale - 1,
            "100.5", 17, "4", "50.2", "0"]


def daily_zip(day, records):
    name = f"{SYMBOL}-1m-{day}.zip"
    text = io.StringIO()
    csv.writer(text).writerows(records)
    data = io.BytesIO()
    with ZipFile(data, "w") as zipped:
        zipped.writestr(name[:-4] + ".csv", text.getvalue())
    return data.getvalue(), name


class ArchiveTests(unittest.TestCase):
    def test_units_on_both_sides_of_cutover_and_decimal_precision(self):
        for day, scale in [(OLD_DAY, 1), (NEW_DAY, 1000)]:
            rows = [record(day, minute, scale) for minute in (0, 1439)]
            parsed = parse_day(*daily_zip(day, rows), SYMBOL, day)
            self.assertEqual(list(parsed.candle_begin_time.dt.strftime("%Y-%m-%d %H:%M")),
                             [f"{day} 00:00", f"{day} 23:59"])
            self.assertEqual(parsed[VALUE_FIELDS].iloc[0].to_dict(), {
                "open": 12, "high": 14, "low": 11, "close": 13, "volume": Decimal("8.123456789012345678"),
                "quote_volume": Decimal("100.5"), "trade_num": 17, "taker_buy_base_asset_volume": 4,
                "taker_buy_quote_asset_volume": Decimal("50.2"),
            })

    def test_checksum_and_manifest_are_verified_before_parsing(self):
        data, name = daily_zip(OLD_DAY, [record(OLD_DAY)])
        digest = hashlib.sha256(data).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path / name).write_bytes(data)
            manifest = path / (name + ".CHECKSUM")
            manifest.write_text(f"{digest}  {name}\n")
            parsed, actual_digest = read_day(SYMBOL, OLD_DAY, path)
            self.assertEqual((len(parsed), actual_digest), (1, digest))
            manifest.write_text(f"{digest}  wrong.zip\n")
            with self.assertRaisesRegex(ValueError, "manifest"):
                read_day(SYMBOL, OLD_DAY, path)
            manifest.write_text(f"{digest}  {name}\n")
            (path / name).write_bytes(b"damaged archive")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                read_day(SYMBOL, OLD_DAY, path)

    def test_invalid_source_rows_fail_instead_of_being_rounded_or_skipped(self):
        base = record(NEW_DAY, scale=1000)
        bad_close = base.copy()
        bad_close[6] -= 1
        bad_volume = base.copy()
        bad_volume[5] = "1.0000000000000000001"
        for rows in ([base, base], [record(NEW_DAY, scale=1)], [record(OLD_DAY)], [bad_close], [bad_volume]):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "line [12]"):
                parse_day(*daily_zip(NEW_DAY, rows), SYMBOL, NEW_DAY)

    def test_exact_comparison_covers_all_fields_and_both_sides_of_the_join(self):
        rows = [record(OLD_DAY, minute) for minute in range(3)]
        data = parse_day(*daily_zip(OLD_DAY, rows), SYMBOL, OLD_DAY)
        expected, stored = data.iloc[:2].copy(), data.iloc[1:].copy()
        for field in VALUE_FIELDS:
            stored.loc[1, field] += 1
        found = list(differences(SYMBOL, expected, stored))
        self.assertEqual(found[0], ("missing_in_mysql", SYMBOL, "1m", "2024-12-31T00:00:00Z", "", "", "", ""))
        self.assertEqual(found[-1], ("unexpected_in_mysql", SYMBOL, "1m", "2024-12-31T00:02:00Z", "", "", "", ""))
        self.assertEqual([r[4] for r in found[1:-1]], VALUE_FIELDS)
        self.assertTrue(all(r[0] == "field_mismatch" and r[3] == "2024-12-31T00:01:00Z" and r[-1] == "1"
                            for r in found[1:-1]))
        self.assertIn(("field_mismatch", SYMBOL, "1m", "2024-12-31T00:01:00Z", "volume",
                       "8.123456789012345678", "9.123456789012345678", "1"), found)
        self.assertEqual(list(differences(SYMBOL, data, data.iloc[::-1])), [])


if __name__ == "__main__":
    unittest.main()
