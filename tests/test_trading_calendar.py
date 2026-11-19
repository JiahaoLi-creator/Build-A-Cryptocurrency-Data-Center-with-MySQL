import csv
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

import archive
import recovery
from feeds import epoch_ms, get_data_via_http, ingest_message
from Function import MINUTE
from storage import FIELDS, save_data_to_mysql
from trading_calendar import Suspension, SuspendedCandleError, check_trading_rows, eligible_bar_count, suspension_at
from windows import expected_minutes, get_k_data, repair_window

HALT = datetime(2022, 10, 31, 3)
RESUME = datetime(2022, 11, 9, 8)
SYMBOL = "VIDTUSDT"


def zero_volume_rows(opens):
    return pd.DataFrame([[SYMBOL, "1m", stamp, 1, 1, 1, 1, 0, 0, 0, 0, 0] for stamp in opens], columns=FIELDS)


class TradingCalendarTests(unittest.TestCase):
    def test_both_edges_and_an_unlisted_calendar_symbol(self):
        self.assertIsNone(suspension_at(SYMBOL, HALT - MINUTE))
        self.assertIsNotNone(suspension_at(SYMBOL, HALT))
        self.assertIsNotNone(suspension_at(SYMBOL, RESUME - MINUTE))
        self.assertIsNone(suspension_at(SYMBOL, RESUME))
        self.assertEqual(len(expected_minutes("BTCUSDT", HALT, RESUME)), 13260)
        self.assertEqual(len(expected_minutes(SYMBOL, HALT, RESUME)), 0)

    def test_empty_response_leaves_only_active_edges_missing_and_splits_requests(self):
        fetch = Mock(return_value=[])
        with patch("windows.get_data_from_mysql", return_value=zero_volume_rows([])):
            missing = repair_window(None, "k", SYMBOL, HALT - MINUTE, RESUME + MINUTE, fetch=fetch, now=RESUME + MINUTE)
        self.assertEqual(list(missing), [HALT - MINUTE, RESUME])
        self.assertEqual([c.args[0] for c in fetch.call_args_list], [
            {"symbol": SYMBOL, "interval": "1m", "startTime": epoch_ms(t), "endTime": epoch_ms(t + MINUTE) - 1,
             "limit": 1} for t in (HALT - MINUTE, RESUME)])
        fetch.reset_mock()
        self.assertTrue(repair_window(None, "k", SYMBOL, HALT, RESUME, fetch=fetch, now=RESUME).empty)
        fetch.assert_not_called()

    def test_zero_volume_is_valid_during_trading_and_conflicting_inputs_are_refused(self):
        check_trading_rows(zero_volume_rows([HALT - MINUTE, RESUME]))
        with self.assertRaises(SuspendedCandleError):
            save_data_to_mysql(None, "k", zero_volume_rows([HALT - MINUTE, HALT]))
        event = {"e": "kline", "s": SYMBOL, "k": {
            "x": True, "i": "1m", "t": epoch_ms(HALT), "o": "1", "h": "1", "l": "1", "c": "1",
            "v": "0", "q": "0", "n": 0, "V": "0", "Q": "0"}}
        with patch("feeds.save_data_to_csv") as log:
            self.assertIsNone(ingest_message(None, event))
            log.assert_not_called()

    def test_direct_rest_backfill_cannot_span_the_suspension(self):
        fetch = Mock(return_value=[])
        left, right = epoch_ms(HALT - MINUTE), epoch_ms(RESUME + MINUTE)
        # The unaligned left endpoint excludes the last pre-suspension minute.
        get_data_via_http([{"symbol": SYMBOL, "interval": "1m", "startTime": left + 1, "endTime": right - 1,
                            "limit": 13262}], fetch=fetch, now=RESUME + MINUTE)
        fetch.assert_called_once_with({"symbol": SYMBOL, "interval": "1m", "startTime": epoch_ms(RESUME),
                                       "endTime": right - 1, "limit": 1})

    def test_reader_excludes_legacy_rows_and_buckets_that_straddle_a_halt(self):
        # The five-minute grid is deliberately offset from both ends of this suspension.
        start = RESUME
        event = Suspension(start + 2 * MINUTE, start + 7 * MINUTE, "Maintenance", "https://example.org/notice")
        raw = zero_volume_rows(pd.date_range(start, periods=15, freq="min"))
        with (patch("trading_calendar.load_calendar", return_value={SYMBOL: [event]}),
              patch("windows.get_data_from_mysql", return_value=raw)):
            bars = get_k_data(SYMBOL, "5m", 3, None, as_of=start + 15 * MINUTE)
            self.assertEqual(list(bars.index), [start + 10 * MINUTE])
            self.assertEqual(eligible_bar_count(SYMBOL, start + 15 * MINUTE, 5 * MINUTE, 3), 1)

    def test_ongoing_halt_waits_for_a_confirmed_reopening(self):
        event = Suspension(HALT, None, "Maintenance", "https://example.org/notice")
        with (patch("trading_calendar.load_calendar", return_value={SYMBOL: [event]}),
              patch("recovery.checkpoint", return_value=HALT), patch("recovery.advance_checkpoint") as advance,
              patch("recovery.repair_window") as repair):
            self.assertTrue(recovery.catch_up(None, [SYMBOL], RESUME))
            advance.assert_not_called()
            repair.assert_not_called()

    def test_suspended_archive_day_is_audited_without_downloading_a_zip(self):
        stamp = HALT + 2 * archive.DAY
        args = SimpleNamespace(symbol=SYMBOL, start=stamp.date(), end=(stamp + archive.DAY).date(), command="reconcile")
        output = StringIO()
        with (patch("archive.read_day") as download, redirect_stdout(output),
              patch("archive.get_data_from_mysql", return_value=zero_volume_rows([stamp]))):
            self.assertEqual(archive.process(None, args), 1)
            download.assert_not_called()
        report = list(csv.DictReader(StringIO(output.getvalue())))
        self.assertEqual([(r["issue"], r["open_time_utc"]) for r in report],
                         [("unexpected_in_mysql", stamp.isoformat() + "Z")])
