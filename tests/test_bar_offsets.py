import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from importlib import import_module
from io import StringIO
from unittest.mock import patch

import pandas as pd

from Function import MINUTE, floor_time, sleep_until_run_time
from storage import OHLCV
from trading_calendar import eligible_bar_count
from windows import get_k_data


class BarOffsetTests(unittest.TestCase):
    def test_hourly_grid_and_live_wakeup(self):
        local = datetime.fromisoformat("2025-01-01T11:16:59+11:00")
        self.assertEqual(floor_time(local, 60 * MINUTE, offset_minutes=17), datetime(2024, 12, 31, 23, 17))
        with patch("Function.utc_now", return_value=datetime(2025, 1, 1, 0, 6)):
            self.assertEqual(sleep_until_run_time("5m", if_sleep=False, offset_minutes=2),
                             datetime(2025, 1, 1, 0, 7))

    def test_shifted_query_across_midnight_and_exclusive_end(self):
        midnight = datetime(2025, 1, 1)
        start, end = midnight - 3 * MINUTE, midnight + 2 * MINUTE
        raw = pd.DataFrame({"candle_begin_time": pd.date_range(start, periods=6, freq="min"),
                            **{name: range(1, 7) for name in OHLCV}})

        def read(engine, table_name, *, begin_time, end_time, **filters):
            return raw[(raw.candle_begin_time >= begin_time) & (raw.candle_begin_time < end_time)]

        with patch("windows.get_data_from_mysql", side_effect=read) as query:
            early = get_k_data("BTCUSDT", "5m", 1, as_of=end - timedelta(microseconds=1), offset_minutes=2)
            self.assertTrue(early.empty)
            bars = get_k_data("BTCUSDT", "5m", 1, as_of=end, offset_minutes=2)
            query.assert_called_with(None, "k", symbol="BTCUSDT", time_interval="1m", begin_time=start, end_time=end)
        self.assertEqual(list(bars.index), [start])
        self.assertEqual(list(bars.iloc[0]), [1, 5, 1, 5, 15])

    def test_shifted_reader_marks_a_window_crossing_relisting_as_suspended(self):
        reader = import_module("3_get_data_and_sample")
        resume = datetime(2022, 11, 9, 8)
        cutoff = resume + 5 * MINUTE
        self.assertEqual(eligible_bar_count("VIDTUSDT", cutoff, 5 * MINUTE, 1), 1)
        # The shifted window [07:57, 08:02) crosses the 08:00 relisting. Include old, conflicting raw rows.
        raw = pd.DataFrame({"candle_begin_time": pd.date_range(resume - 3 * MINUTE, periods=5, freq="min"),
                            **{name: [1] * 5 for name in OHLCV}})
        output = StringIO()
        with (patch("sys.argv", ["reader", "--once", "--interval", "5m", "--offset-minutes", "2", "--limit", "1",
                                "--as-of", "2022-11-09T08:05:00Z"]),
              patch.object(reader, "open_engine"), patch.object(reader, "symbols", return_value=["VIDTUSDT"]),
              patch.object(reader, "if_data_available", return_value=True),
              patch("windows.get_data_from_mysql", return_value=raw), redirect_stdout(output)):
            self.assertEqual(reader.main(), 0)
        self.assertIn("complete=0/0 | suspended windows=1 | offset minutes=2", output.getvalue())

    def test_invalid_offsets_fail_before_reading_mysql(self):
        with patch("windows.get_data_from_mysql") as read:
            for offset in (-1, 5, 1.5, True):
                with self.subTest(offset=offset), self.assertRaises(ValueError):
                    get_k_data(target_time_interval="5m", offset_minutes=offset)
            read.assert_not_called()
        reader = import_module("3_get_data_and_sample")
        with (patch("sys.argv", ["reader", "--interval", "1m", "--offset-minutes", "1"]),
              patch.object(reader, "open_engine") as connect, redirect_stderr(StringIO()),
              self.assertRaises(SystemExit) as error):
            reader.main()
        self.assertEqual(error.exception.code, 2)
        connect.assert_not_called()
