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


class PlanPodcastindexInsertsTest(unittest.TestCase):
    def test_guid_dedupe_then_title_fallback(self):
        existing_guids = {"guid-a"}
        existing_titles = {cb.norm_title("An Old Episode")}
        eps = [
            {"guid": "guid-a", "title": "Different Title Same Guid",
             "published_at": "2020-01-01", "audio_url": "a.mp3",
             "duration_seconds": 3600},
            {"guid": "guid-b", "title": "An Old Episode",
             "published_at": "2020-02-01", "audio_url": "b.mp3",
             "duration_seconds": 3600},
            {"guid": "guid-c", "title": "Genuinely New",
             "published_at": "2020-03-01", "audio_url": "c.mp3",
             "duration_seconds": 3600},
        ]
        plan = cb.plan_podcastindex_inserts(
            "some-show", existing_guids, existing_titles, eps)
        self.assertEqual([e["guid"] for e in plan], ["guid-c"])
        self.assertEqual(plan[0]["source_id"], "some-show")
        self.assertTrue(plan[0]["id"].startswith("ep_"))
        self.assertEqual(plan[0]["published_at"], "2020-03-01")

    def test_missing_guid_title_or_date_skipped(self):
        plan = cb.plan_podcastindex_inserts("s", set(), set(), [
            {"guid": "", "title": "No Guid", "published_at": "2020-01-01"},
            {"guid": "g", "title": "", "published_at": "2020-01-01"},
            {"guid": "g2", "title": "No Date", "published_at": None},
        ])
        self.assertEqual(plan, [])
