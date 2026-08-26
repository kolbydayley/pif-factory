"""Deadline-pacing math: each lane lands at ~95% of its window by reset."""
import json
import os
import time
import unittest
from unittest import mock

from research_factory import pif_pacing


def _write_receipt(root, lane, calls, age_days, now):
    stamp = time.strftime("%Y%m%dT%H%M%S", time.localtime(now - age_days * 86400))
    p = root / f"receipt-bulk-{lane}-{stamp}.json"
    p.write_text(json.dumps({"calls_made": calls}))
    t = now - age_days * 86400
    os.utime(p, (t, t))
    return p


class GlmZaiPacingTest(unittest.TestCase):
    def test_behind_schedule_speeds_up(self):
        now = 1_787_000_000.0
        # 29% used, 3 days to reset, 5200 calls bought those 29 points.
        quota = {"percentage": 29, "nextResetTime": (now + 3 * 86400) * 1000}
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=5200):
            pace = pif_pacing.pace_glm_zai(now=now, quota=quota)
        # remaining 66 pct over 3 days at ~179 calls/pct => ~3900/day, clamped.
        self.assertEqual(pace["count"], pif_pacing.HARD_DAY_MAX["glm-zai"])
        self.assertEqual(pace["reason"], "deadline_pacing")

    def test_ahead_of_schedule_slows_down(self):
        now = 1_787_000_000.0
        quota = {"percentage": 90, "nextResetTime": (now + 5 * 86400) * 1000}
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=9000):
            pace = pif_pacing.pace_glm_zai(now=now, quota=quota)
        # only 5 pct headroom left over 5 days => ~100 calls/day.
        self.assertLess(pace["count"], 200)
        self.assertGreater(pace["count"], 0)

    def test_no_history_uses_safe_default(self):
        quota = {"percentage": 0, "nextResetTime": None}
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=0):
            pace = pif_pacing.pace_glm_zai(now=1.0, quota=quota)
        self.assertEqual(pace["count"], 1600)
        self.assertEqual(pace["reason"], "uncalibrated_default")

    def test_stale_reset_falls_back_to_uniform_burn(self):
        now = 1_787_000_000.0
        quota = {"percentage": 50, "nextResetTime": (now - 100) * 1000}
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=5000):
            pace = pif_pacing.pace_glm_zai(now=now, quota=quota)
        self.assertEqual(pace["deadline_source"], "uniform_burn_estimate")
        self.assertGreater(pace["count"], 0)

    def test_missing_quota_fails_closed(self):
        with mock.patch.object(pif_pacing, "_zai_weekly_window", return_value=None):
            self.assertIsNone(pif_pacing.pace_glm_zai(now=1.0))


class GrokPacingTest(unittest.TestCase):
    def test_budget_spent_pauses_lane(self):
        now = time.time()
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=700):
            pace = pif_pacing.pace_grok(now=now)
        self.assertEqual(pace["count"], 0)

    def test_fresh_window_paces_toward_target(self):
        # Anchor just after a Wednesday-12:48-UTC reset: full budget, ~7 days.
        start = pif_pacing._grok_window_start(time.time())
        now = start + 3600.0
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=0):
            pace = pif_pacing.pace_grok(now=now)
        # 617 target calls over ~7 days, 1.25 front-load => ~110/day.
        self.assertGreater(pace["count"], 80)
        self.assertLess(pace["count"], 160)

    def test_never_exceeds_remaining_budget(self):
        start = pif_pacing._grok_window_start(time.time())
        now = start + 6.9 * 86400  # nearly at reset
        with mock.patch.object(pif_pacing, "_receipt_calls_since", return_value=100):
            pace = pif_pacing.pace_grok(now=now)
        self.assertLessEqual(pace["count"],
                             int(0.95 * pif_pacing.GROK_WEEKLY_BUDGET) - 100 + 1)


class CodexPacingTest(unittest.TestCase):
    def test_paces_to_artificial_cap(self):
        now = 1_787_000_000
        snapshot = {"used_percent": 10.0, "resets_at": now + 4 * 86400}
        ledger = mock.Mock()
        ledger.points_used.return_value = 8.0  # of the 20-point cap
        with mock.patch.object(pif_pacing, "WeeklyLedger", return_value=ledger):
            pace = pif_pacing.pace_codex(now=now, snapshot=snapshot)
        # 12 points left / 4 days * borrow 2 = 6 points => 330 calls.
        self.assertEqual(pace["count"], 330)
        self.assertEqual(pace["cap_points"], 20.0)

    def test_cap_reached_pauses_lane(self):
        now = 1_787_000_000
        snapshot = {"used_percent": 50.0, "resets_at": now + 86400}
        ledger = mock.Mock()
        ledger.points_used.return_value = 20.0
        with mock.patch.object(pif_pacing, "WeeklyLedger", return_value=ledger):
            pace = pif_pacing.pace_codex(now=now, snapshot=snapshot)
        self.assertEqual(pace["count"], 0)

    def test_no_snapshot_fails_closed(self):
        with mock.patch.object(pif_pacing, "read_weekly_snapshot", return_value=None):
            self.assertIsNone(pif_pacing.pace_codex(now=1.0))


class LaneRoutingTest(unittest.TestCase):
    def test_glm_opencode_has_no_pacing(self):
        self.assertIsNone(pif_pacing.paced_count("glm"))


if __name__ == "__main__":
    unittest.main()
