import unittest
from contextlib import redirect_stdout
from datetime import datetime
from importlib import import_module
from io import StringIO
from unittest.mock import Mock, patch

import pandas as pd
import requests

from Function import MINUTE, floor_time, interval_delta, parse_utc
from feeds import epoch_ms, get_data_via_http, request_klines
from windows import gap_requests


class MarketDataTests(unittest.TestCase):
    def test_timezone_and_buckets(self):
        midnight = datetime(2024, 1, 1)
        self.assertEqual(parse_utc("2024-01-01T11:00:00+11:00"), midnight)
        self.assertEqual(epoch_ms(midnight), 1704067200000)
        self.assertEqual(floor_time(midnight + 59 * MINUTE, interval_delta("1h")), midnight)

    def test_bounded_rest_batches(self):
        opens = pd.date_range("2024-01-01", periods=1003, freq="min").delete(1001)
        batches = gap_requests(opens, "BTCUSDT")
        self.assertEqual([b["limit"] for b in batches], [1000, 1, 1])
        self.assertEqual(batches[0]["endTime"] + 1, batches[1]["startTime"])
        self.assertEqual(batches[1]["endTime"] + 60_001, batches[2]["startTime"])

    def test_retry_stops_at_first_success(self):
        response = Mock()
        response.json.return_value = []
        with patch("feeds.requests.get", side_effect=[requests.Timeout(), response]) as get, patch("feeds.time.sleep"):
            self.assertEqual(request_klines({}), [])
            self.assertEqual(get.call_count, 2)

    def test_reader_skips_stale_symbol(self):
        reader = import_module("3_get_data_and_sample")
        with (patch("sys.argv", ["reader", "--once", "--as-of", "2024-01-01T00:10:00Z"]),
              patch.object(reader, "open_engine"), patch.object(reader, "symbols", return_value=["BTCUSDT"]),
              patch.object(reader, "if_data_available", return_value=False),
              patch.object(reader, "get_k_data") as aggregate, redirect_stdout(StringIO())):
            self.assertEqual(reader.main(), 1)
            aggregate.assert_not_called()

    def test_rest_rejects_open_or_unrequested_candles(self):
        stamp = datetime(2024, 1, 1)
        t = epoch_ms(stamp)
        def record(at):
            return [at, "100", "102", "99", "101", "1", at + 59_999, "101", 1, ".4", "40.4", "0"]
        params = [{"symbol": "BTCUSDT", "interval": "1m", "startTime": t, "limit": 1}]
        fetch = lambda _: [record(t), record(t), record(t + 60_000)]
        self.assertTrue(get_data_via_http(params, fetch=fetch, now=stamp).empty)
        self.assertEqual(len(get_data_via_http(params, fetch=fetch, now=stamp + MINUTE)), 1)


if __name__ == "__main__":
    unittest.main()
