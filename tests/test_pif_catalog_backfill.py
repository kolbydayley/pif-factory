"""Tests for the truncated-feed catalog backfiller's planning logic."""
import importlib.util
import unittest
from pathlib import Path

_P = Path.home() / "pif-factory" / "scripts" / "pif_catalog_backfill.py"
spec = importlib.util.spec_from_file_location("pif_catalog_backfill", _P)
cb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cb)


class PlanYoutubeInsertsTest(unittest.TestCase):
    def test_skips_titles_already_in_db(self):
        existing = {cb.norm_title("Meta Shifts the Blame + Do Data Center Bets Pay Off")}
        candidates = [
            {"id": "abc12345678", "title":
                "Meta Shifts the Blame + Do Data Center Bets Pay Off",
             "upload_date": "20260820", "duration": 3600},
            {"id": "def12345678", "title": "A Brand New Episode",
             "upload_date": "20260810", "duration": 3000},
        ]
        plan = cb.plan_youtube_inserts("hard-fork", existing, candidates)
        self.assertEqual(len(plan), 1)
        ep = plan[0]
        self.assertEqual(ep["guid"], "yt:def12345678")
        self.assertEqual(ep["published_at"], "2026-08-10")
        self.assertEqual(ep["verified_transcript_source_kind"],
                         "youtube_captions")
        self.assertIn("def12345678", ep["verified_transcript_url"])
        self.assertTrue(ep["id"].startswith("ep_"))

    def test_skips_shorts_and_undated(self):
        plan = cb.plan_youtube_inserts("x", set(), [
            {"id": "a" * 11, "title": "Clip", "upload_date": "20260101",
             "duration": 90},
            {"id": "b" * 11, "title": "No date", "upload_date": None,
             "duration": 3600},
        ])
        self.assertEqual(plan, [])


if __name__ == "__main__":
    unittest.main()
