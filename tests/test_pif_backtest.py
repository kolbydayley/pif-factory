"""Tests for the Signal Desk golden-dataset backtest harness.

Pure scoring functions only — network fetch is isolated behind fetch_truth
and never exercised here.
"""
import importlib.util
import unittest
from pathlib import Path

_BT = Path.home() / "pif-factory" / "scripts" / "pif_backtest.py"
spec = importlib.util.spec_from_file_location("pif_backtest", _BT)
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

FLAT = [100.0] * 30
STEP = [100.0] * 15 + [300.0] * 15  # clean rise at index 15


class ChangepointTest(unittest.TestCase):
    def test_step_series_changepoint_found_near_step(self):
        cps = bt.cusum_changepoints(STEP)
        self.assertTrue(cps)
        self.assertTrue(any(13 <= c <= 17 for c in cps), cps)

    def test_flat_series_has_no_changepoints(self):
        self.assertEqual(bt.cusum_changepoints(FLAT), [])

    def test_short_series_is_safe(self):
        self.assertEqual(bt.cusum_changepoints([5.0, 6.0]), [])


class ExternalRiseTest(unittest.TestCase):
    def test_rise_confirmed_when_window_beats_baseline(self):
        self.assertTrue(bt.external_rise(STEP, 15))

    def test_flat_series_never_confirms(self):
        self.assertFalse(bt.external_rise(FLAT, 15))

    def test_firing_long_before_rise_is_not_confirmed(self):
        self.assertFalse(bt.external_rise(STEP, 5))

    def test_left_edge_without_baseline_is_safe(self):
        self.assertFalse(bt.external_rise(STEP, 0))

    def test_right_edge_uses_truncated_window(self):
        # A firing near the end of the timeline still scores against the
        # post-window data that exists (>= 2 weeks), instead of being
        # silently discarded.
        late_rise = [100.0] * 26 + [300.0] * 4
        self.assertTrue(bt.external_rise(late_rise, 27))
        self.assertFalse(bt.external_rise(FLAT, 27))


class CorrelationTest(unittest.TestCase):
    def test_identical_series_correlate_at_lag_zero(self):
        lag, r = bt.best_lag_correlation(STEP, STEP, max_lag=4)
        self.assertEqual(lag, 0)
        self.assertAlmostEqual(r, 1.0, places=6)

    def test_lagged_copy_recovers_the_lag(self):
        # ours leads truth by 3 weeks -> best lag +3 (we fire early).
        ours = STEP
        truth = [100.0] * 3 + STEP[:-3]
        lag, r = bt.best_lag_correlation(ours, truth, max_lag=5)
        self.assertEqual(lag, 3)
        self.assertGreater(r, 0.95)

    def test_constant_series_returns_zero_r(self):
        _, r = bt.best_lag_correlation(FLAT, STEP, max_lag=4)
        self.assertEqual(r, 0.0)


class ScoreFiringsTest(unittest.TestCase):
    def test_precision_and_lead_time(self):
        truths = {"topic a": STEP, "topic b": FLAT}
        firings = [
            {"topic": "topic a", "week_idx": 14, "tier": "strong"},
            {"topic": "topic b", "week_idx": 14, "tier": "strong"},
            {"topic": "topic a", "week_idx": 5, "tier": "weak"},
        ]
        scores = bt.score_firings(firings, truths)
        strong = scores["by_tier"]["strong"]
        self.assertEqual(strong["n"], 2)
        self.assertAlmostEqual(strong["precision"], 0.5)
        # firing at 14, external changepoint ~15 -> we led by ~1 week
        self.assertLessEqual(strong["median_lead_weeks"], 2)
        weak = scores["by_tier"]["weak"]
        self.assertEqual(weak["precision"], 0.0)

    def test_unmapped_topic_is_skipped_not_counted(self):
        scores = bt.score_firings(
            [{"topic": "unmapped", "week_idx": 10, "tier": "strong"}], {})
        self.assertEqual(scores["by_tier"], {})
        self.assertEqual(scores["skipped_unmapped"], 1)


class EventRecallTest(unittest.TestCase):
    def test_event_recalled_within_tolerance(self):
        events = [{"week_idx": 15, "topics": ["topic a"], "label": "launch"}]
        firings = [{"topic": "topic a", "week_idx": 13, "tier": "moderate"}]
        recall = bt.event_recall(events, firings, tolerance=3)
        self.assertEqual(recall["hit"], 1)
        self.assertEqual(recall["total"], 1)

    def test_event_missed_outside_tolerance(self):
        events = [{"week_idx": 15, "topics": ["topic a"], "label": "launch"}]
        firings = [{"topic": "topic a", "week_idx": 5, "tier": "strong"}]
        recall = bt.event_recall(events, firings, tolerance=3)
        self.assertEqual(recall["hit"], 0)


if __name__ == "__main__":
    unittest.main()
