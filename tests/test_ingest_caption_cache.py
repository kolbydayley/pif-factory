"""Browser-harvested caption cache short-circuits the YouTube API fetch.

When work/pif-ops/youtube-captions/<video_id>.txt exists (harvested via the
authenticated browser session), fetch_transcript_source must serve it and
never touch the youtube-transcript-api (which may be IP-blocked).
"""
import tempfile
import unittest
from pathlib import Path

from research_factory import ingest


class CaptionCacheTest(unittest.TestCase):
    def test_cache_file_short_circuits_api(self):
        with tempfile.TemporaryDirectory() as td:
            old = ingest.CAPTION_CACHE_DIR
            ingest.CAPTION_CACHE_DIR = Path(td)
            try:
                (Path(td) / "dQw4w9WgXcQ.txt").write_text(
                    "cached transcript text from the browser session")
                text, ctype = ingest.fetch_transcript_source(
                    "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                    source_kind="youtube_captions", transcript_type=None)
                self.assertIn("cached transcript text", text)
                self.assertEqual(ctype, "text/plain")
            finally:
                ingest.CAPTION_CACHE_DIR = old

    def test_no_cache_still_uses_api_path(self):
        with tempfile.TemporaryDirectory() as td:
            old = ingest.CAPTION_CACHE_DIR
            ingest.CAPTION_CACHE_DIR = Path(td)
            try:
                with self.assertRaises(Exception):
                    # no cache file and (in tests) no network success —
                    # must raise via the API path, not return silently
                    ingest.fetch_transcript_source(
                        "https://www.youtube.com/watch?v=zzzzzzzzzzz",
                        source_kind="youtube_captions",
                        transcript_type=None)
            finally:
                ingest.CAPTION_CACHE_DIR = old


if __name__ == "__main__":
    unittest.main()
