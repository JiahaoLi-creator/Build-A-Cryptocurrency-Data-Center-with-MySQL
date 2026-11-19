import threading
import unittest
from datetime import datetime
from unittest.mock import Mock, call, patch

import pandas as pd
import requests

import recovery
from Function import MINUTE

START = datetime(2025, 1, 1)
NO_GAPS = pd.DatetimeIndex([])


class RecoveryTests(unittest.TestCase):
    def test_long_ranges_are_bounded_and_symbols_take_turns(self):
        end = START + 1002 * MINUTE
        recent = end - MINUTE
        with (patch.object(recovery, "checkpoint", side_effect=[START, recent]),
              patch.object(recovery, "advance_checkpoint"),
              patch.object(recovery, "repair_window", return_value=NO_GAPS) as repair):
            self.assertTrue(recovery.catch_up(None, ["BTCUSDT", "ETHUSDT"], end))
        self.assertEqual([(c.args[2], c.args[3], c.args[4]) for c in repair.call_args_list],
                         [("BTCUSDT", START, START + 1000 * MINUTE), ("ETHUSDT", recent, end),
                          ("BTCUSDT", START + 1000 * MINUTE, end)])

    def test_partial_response_keeps_the_first_gap_for_the_next_pass(self):
        end = START + 180 * MINUTE
        hole = START + 25 * MINUTE
        with (patch.object(recovery, "checkpoint", side_effect=[START, hole]),
              patch.object(recovery, "repair_window", side_effect=[pd.DatetimeIndex([hole]), NO_GAPS]) as repair,
              patch.object(recovery, "advance_checkpoint") as advance):
            self.assertFalse(recovery.catch_up(None, ["BTCUSDT"], end))
            self.assertTrue(recovery.catch_up(None, ["BTCUSDT"], end))
        self.assertEqual([c.args[3] for c in repair.call_args_list], [START, hole])
        self.assertEqual(advance.call_args_list, [call(None, "BTCUSDT", START, hole), call(None, "BTCUSDT", hole, end)])

    def test_rest_failure_leaves_position_intact_and_other_symbols_progress(self):
        end = START + 3 * MINUTE
        with (patch.object(recovery, "checkpoint", return_value=START),
              patch.object(recovery, "repair_window", side_effect=[requests.Timeout(), NO_GAPS]),
              patch.object(recovery, "advance_checkpoint") as advance):
            self.assertFalse(recovery.catch_up(None, ["BTCUSDT", "ETHUSDT"], end))
        advance.assert_called_once_with(None, "ETHUSDT", START, end)

    def test_reconnection_wakes_the_background_worker(self):
        wake, stopping = threading.Event(), threading.Event()
        ready, resumed = threading.Event(), threading.Event()
        verified = Mock(side_effect=[False, True])

        def verify(*args, **kwargs):
            ok = verified()
            (resumed if ok else ready).set()
            return ok

        with patch.object(recovery, "catch_up", side_effect=verify):
            worker = threading.Thread(target=recovery.maintain_history, args=(None, ["BTCUSDT"], wake, stopping))
            worker.start()
            try:
                self.assertTrue(ready.wait(3))
                wake.set()
                self.assertTrue(resumed.wait(3))
            finally:
                stopping.set()
                wake.set()
                worker.join(3)
            self.assertFalse(worker.is_alive())
