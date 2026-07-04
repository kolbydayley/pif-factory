from __future__ import annotations

import fcntl
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


class FactoryCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        shutil.copytree(PROJECT / "label_packs", self.root / "label_packs")
        self.env = os.environ.copy()
        self.env["PYTHONPATH"] = str(PROJECT)
        self.env["RESEARCH_FACTORY_ROOT"] = str(self.root)
        self.env["RESEARCH_FACTORY_DB"] = str(self.root / "data" / "factory.sqlite")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_cli(self, *args: str) -> dict:
        result = self.run_cli_process(*args, check=True)
        return json.loads(result.stdout)

    def run_cli_process(self, *args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "research_factory", *args],
            cwd=PROJECT,
            env=self.env,
            capture_output=True,
            text=True,
            check=check,
        )

    def db_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.env["RESEARCH_FACTORY_DB"])
        conn.row_factory = sqlite3.Row
        return conn

    def write_fixture_source(self) -> Path:
        transcript = self.root / "episode.vtt"
        transcript.write_text(
            """WEBVTT

00:00:00.000 --> 00:00:05.000
The guest says AGI timelines are changing because agents can use tools.

00:00:05.000 --> 00:00:10.000
They compare coding agents, inference scaling, and enterprise AI workflows.

00:00:10.000 --> 00:00:15.000
The host asks whether the word singularity is replacing AGI inside frontier labs.
""",
            encoding="utf-8",
        )
        feed = self.root / "feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Fixture Tech Podcast</title>
    <item>
      <guid>fixture-1</guid>
      <title>Agents and AGI</title>
      <link>https://example.test/fixture-1</link>
      <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
      <description>Fixture episode.</description>
      <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
    </item>
  </channel>
</rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Fixture Tech Podcast
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        return sources

    def write_audio_only_source(self) -> Path:
        feed = self.root / "audio-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Audio Only Fixture</title><item>
  <guid>audio-1</guid>
  <title>Audio Only Transcript Candidate</title>
  <link>https://example.test/audio-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <enclosure url="https://audio.example.test/audio-1.mp3" type="audio/mpeg" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "audio-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Audio Only Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        return sources

    def test_ingest_label_export_snapshot(self) -> None:
        sources = self.write_fixture_source()
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["episodes"], 1)
        first_status = self.run_cli("status")
        second = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        second_status = self.run_cli("status")
        self.assertEqual(first_status["counts"]["jobs"], second_status["counts"]["jobs"])
        self.assertEqual(second["episodes"], 1)

        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertGreaterEqual(run["completed"], 2)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["episodes"], 1)
        self.assertGreaterEqual(status["counts"]["segments"], 1)
        self.assertGreaterEqual(status["counts"]["labels"], 1)

        trend = self.run_cli("export", "trend-report", "--topic", "agi", "--window", "month")
        self.assertTrue(Path(trend["path"]).exists())
        graph = self.run_cli("export", "graph", "--type", "concept_network")
        self.assertTrue(Path(graph["path"]).exists())
        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["privacy"], "sanitized_operational_snapshot_no_raw_transcripts")
        self.assertIn("job_counts", payload)

    def test_preflight_fails_closed(self) -> None:
        result = self.run_cli("preflight", "--model", "gpt-5.4")
        self.assertIn("codex_cli_available", result)

    def test_bad_feed_is_queued_as_source_error(self) -> None:
        sources = self.root / "bad-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Broken Feed
    rss_url: {self.root / 'missing-feed.xml'}
    homepage_url: https://example.test/broken
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["source_errors"], 1)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["sources"], 1)
        self.assertEqual(status["counts"]["jobs"], 1)
        self.assertEqual(status["jobs"][0]["job_type"], "source_fetch_failed")
        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertEqual(run["processed"], 0)
        after = self.run_cli("status")
        self.assertEqual(after["jobs"][0]["status"], "pending")
        (self.root / "missing-feed.xml").write_text(
            """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Recovered Feed</title></channel></rss>""",
            encoding="utf-8",
        )
        healed = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(healed["source_errors"], 0)
        final_status = self.run_cli("status")
        self.assertEqual(final_status["jobs"][0]["status"], "completed")

    def test_attach_transcript_turns_manual_candidate_into_fetch_job(self) -> None:
        transcript = self.root / "official-transcript.vtt"
        transcript.write_text(
            """WEBVTT

00:00:00.000 --> 00:00:05.000
AGI discourse is shifting toward agents and inference scaling.

00:00:05.000 --> 00:00:10.000
The guest says enterprise AI releases will change the deployment story.
""",
            encoding="utf-8",
        )
        feed = self.root / "manual-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Manual Fixture</title><item>
  <guid>manual-1</guid>
  <title>Manual Transcript Candidate</title>
  <link>https://example.test/manual-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "manual-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Manual Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["missing_transcripts"], 1)
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5", "--claim", "--worker-id", "test-discovery")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.assertEqual(candidates["candidates"][0]["job_status"], "claimed")
        self.assertEqual(candidates["candidates"][0]["lease_owner"], "test-discovery")
        empty = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5", "--claim", "--worker-id", "other-discovery")
        self.assertEqual(empty["candidates"], [])
        attached = self.run_cli(
            "attach-transcript",
            "--episode-id",
            episode_id,
            "--transcript-url",
            transcript.as_uri(),
            "--transcript-type",
            "text/vtt",
            "--source-kind",
            "official_show_transcript",
        )
        self.assertEqual(attached["episode_id"], episode_id)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["jobs"], 2)
        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertGreaterEqual(run["completed"], 2)
        final_status = self.run_cli("status")
        self.assertEqual(final_status["counts"]["transcripts"], 1)
        self.assertGreaterEqual(final_status["counts"]["labels"], 1)

    def test_transcript_candidate_claim_caps_and_release(self) -> None:
        feed = self.root / "manual-cap-feed.xml"
        items = "\n".join(
            f"""<item>
  <guid>manual-cap-{index}</guid>
  <title>Manual Candidate {index}</title>
  <link>https://example.test/manual-cap-{index}</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item>"""
            for index in range(3)
        )
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Manual Cap Fixture</title>{items}</channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "manual-cap-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Manual Cap Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))

        first = self.run_cli(
            "transcript-candidates",
            "--lane",
            "podcast",
            "--limit",
            "5",
            "--claim",
            "--worker-id",
            "cap-worker",
            "--max-claimed",
            "10",
            "--per-source-limit",
            "1",
        )
        self.assertEqual(len(first["candidates"]), 1)
        second = self.run_cli(
            "transcript-candidates",
            "--lane",
            "podcast",
            "--limit",
            "5",
            "--claim",
            "--worker-id",
            "cap-worker-2",
            "--max-claimed",
            "1",
            "--per-source-limit",
            "1",
        )
        self.assertEqual(second["candidates"], [])

        released = self.run_cli("release-transcript-candidates", "--lane", "podcast", "--worker-id", "cap-worker", "--limit", "5")
        self.assertEqual(released["released"], 1)
        with self.db_conn() as conn:
            attempts = conn.execute("SELECT attempts FROM jobs WHERE lease_owner IS NULL AND status = 'pending' LIMIT 1").fetchone()
        self.assertEqual(attempts["attempts"], 0)

    def test_feed_refresh_preserves_verified_transcript_url(self) -> None:
        official = self.root / "official.vtt"
        official.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:10.000\nAGI agents and enterprise AI are discussed here.\n", encoding="utf-8")
        rss_transcript = self.root / "rss.vtt"
        rss_transcript.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:10.000\nRSS transcript text is different.\n", encoding="utf-8")
        feed = self.root / "refresh-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Refresh Fixture</title><item>
  <guid>refresh-1</guid>
  <title>Refresh Candidate</title>
  <link>https://example.test/refresh-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "refresh-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Refresh Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        candidate = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "1")["candidates"][0]
        self.run_cli(
            "attach-transcript",
            "--episode-id",
            candidate["episode_id"],
            "--transcript-url",
            official.as_uri(),
            "--transcript-type",
            "text/vtt",
            "--source-kind",
            "official_show_transcript",
        )
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Refresh Fixture</title><item>
  <guid>refresh-1</guid>
  <title>Refresh Candidate</title>
  <link>https://example.test/refresh-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <podcast:transcript url="{rss_transcript.as_uri()}" type="text/vtt" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        with self.db_conn() as conn:
            episode = conn.execute("SELECT transcript_url, feed_transcript_url, verified_transcript_url FROM episodes WHERE id = ?", (candidate["episode_id"],)).fetchone()
        self.assertEqual(episode["transcript_url"], official.as_uri())
        self.assertEqual(episode["verified_transcript_url"], official.as_uri())
        self.assertEqual(episode["feed_transcript_url"], rss_transcript.as_uri())

    def test_prompt_submit_is_fenced_by_worker_and_segment_identity(self) -> None:
        sources = self.write_fixture_source()
        self.assertTrue(self.run_cli("init")["ok"])
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--worker-id", "fetcher")

        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        output_path = Path(claim["output_path"])
        with self.db_conn() as conn:
            row = conn.execute(
                """
                SELECT segments.id AS segment_id, segments.episode_id
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE jobs.id = ?
                """,
                (claim["job_id"],),
            ).fetchone()
        valid_output = {
            "schema_version": "ai_discourse_v1",
            "segment_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "summary": "The segment discusses AGI, agents, coding agents, and enterprise AI workflows.",
            "topics": [{"topic": "agents", "stance": "bullish", "intensity": 0.7, "evidence": "agents can use tools"}],
            "terminology_shifts": [],
            "claims": [{"claim_text": "Agents are framed as useful for software and enterprise workflows.", "claim_type": "descriptive", "confidence": 0.7, "evidence": "coding agents, inference scaling, and enterprise AI workflows"}],
            "entities": {"people": [], "organizations": [], "products": []},
            "overall_confidence": 0.7,
            "needs_review": False,
            "review_reason": None,
        }
        wrong_segment = dict(valid_output, segment_id="seg_wrong")
        output_path.write_text(json.dumps(wrong_segment), encoding="utf-8")
        bad_owner = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-b")
        self.assertNotEqual(bad_owner.returncode, 0)
        bad_segment = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_segment.returncode, 0)

        output_path.write_text(json.dumps(valid_output), encoding="utf-8")
        wrong_path = output_path.with_name("wrong-output.json")
        wrong_path.write_text(json.dumps(valid_output), encoding="utf-8")
        bad_path = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(wrong_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_path.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE label_runs SET output_path = ? WHERE id = ?", (str(wrong_path), claim["label_run_id"]))
            conn.commit()
        bad_run_path = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_run_path.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE label_runs SET output_path = ? WHERE id = ?", (str(output_path), claim["label_run_id"]))
            conn.execute("UPDATE jobs SET leased_until = '2000-01-01T00:00:00+00:00' WHERE id = ?", (claim["job_id"],))
            conn.commit()
        stale = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(stale.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE jobs SET leased_until = '2999-01-01T00:00:00+00:00' WHERE id = ?", (claim["job_id"],))
            conn.commit()
        submitted = self.run_cli("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertIn("label_id", submitted)
        self.assertEqual(self.run_cli("status")["counts"]["labels"], 1)

    def test_recover_label_runs_releases_missing_output_handoff(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        self.assertFalse(Path(claim["output_path"]).exists())
        with self.db_conn() as conn:
            conn.execute("UPDATE jobs SET attempts = max_attempts WHERE id = ?", (claim["job_id"],))
            conn.commit()

        dry = self.run_cli("recover-label-runs")
        self.assertEqual(dry["missing_outputs"], 1)
        self.assertEqual(dry["released_jobs"], 0)
        recovered = self.run_cli("recover-label-runs", "--release-missing-outputs")
        self.assertEqual(recovered["failed_runs"], 1)
        self.assertEqual(recovered["released_jobs"], 1)
        with self.db_conn() as conn:
            run = conn.execute("SELECT status FROM label_runs WHERE id = ?", (claim["label_run_id"],)).fetchone()
            job = conn.execute("SELECT status, lease_owner, attempts, max_attempts FROM jobs WHERE id = ?", (claim["job_id"],)).fetchone()
        self.assertEqual(run["status"], "failed")
        self.assertEqual(job["status"], "pending")
        self.assertIsNone(job["lease_owner"])
        self.assertLess(job["attempts"], job["max_attempts"])

    def test_corpus_verify_detects_segment_hash_drift(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        self.assertTrue(self.run_cli("corpus-verify")["ok"])
        with self.db_conn() as conn:
            segment = conn.execute("SELECT text_path FROM segments LIMIT 1").fetchone()
        (self.root / segment["text_path"]).write_text("mutated text", encoding="utf-8")
        result = self.run_cli("corpus-verify")
        self.assertFalse(result["ok"])
        self.assertEqual(result["issues"][0]["issue"], "hash_mismatch")
        repaired = self.run_cli("corpus-verify", "--repair-hashes")
        self.assertEqual(repaired["repaired"], 1)
        self.assertTrue(self.run_cli("corpus-verify")["ok"])

    def test_refetch_removes_stale_unlabeled_segments(self) -> None:
        transcript = self.root / "changing-transcript.txt"
        transcript.write_text(" ".join(f"word{i}" for i in range(1700)), encoding="utf-8")
        feed = self.root / "changing-feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel><title>Changing Fixture</title><item>
    <guid>changing-1</guid>
    <title>Changing Transcript</title>
    <link>https://example.test/changing-1</link>
    <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
    <podcast:transcript url="{transcript.as_uri()}" type="text/plain" />
  </item></channel>
</rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "changing-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Changing Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        with self.db_conn() as conn:
            transcript_row = conn.execute("SELECT id, episode_id FROM transcripts LIMIT 1").fetchone()
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE transcript_id = ?", (transcript_row["id"],)).fetchone()[0], 3)

        transcript.write_text(" ".join(f"short{i}" for i in range(100)), encoding="utf-8")
        from research_factory.ingest import fetch_and_segment_transcript

        with self.db_conn() as conn:
            result = fetch_and_segment_transcript(conn, transcript_row["episode_id"], label_pack="ai_discourse_v1", lane="podcast")
            conn.commit()
            self.assertEqual(result["segments"], 1)
            self.assertEqual(result["removed_stale_segments"], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE transcript_id = ?", (result["transcript_id"],)).fetchone()[0], 1)
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM jobs
                    JOIN segments ON segments.id = jobs.target_id
                    WHERE jobs.job_type = 'label_segment'
                      AND segments.transcript_id = ?
                    """,
                    (result["transcript_id"],),
                ).fetchone()[0],
                1,
            )

    def test_privacy_scan_flags_absolute_paths(self) -> None:
        exports = self.root / "exports"
        exports.mkdir()
        (exports / "leak.json").write_text('{"path": "/Users/kolbydayley/private/file.txt"}', encoding="utf-8")
        result = self.run_cli("privacy-scan", "--path", str(exports))
        self.assertFalse(result["ok"])

    def test_transcript_parser_body_sniffs_json_and_plain_text_captions(self) -> None:
        from research_factory.text import transcript_to_text

        json_body = json.dumps(
            [
                {"start": 0.1, "end": 1.0, "text": "Hello from Substack JSON.", "words": [{"word": "Hello"}]},
                {"start": 1.0, "end": 2.0, "text": "Agents use tools."},
            ]
        )
        parsed_json = transcript_to_text(
            json_body,
            "binary/octet-stream",
            "https://substackcdn.com/video_upload/example/transcription.json?Expires=999",
        )
        self.assertEqual(parsed_json, "Hello from Substack JSON.\nAgents use tools.")
        self.assertNotIn('"start"', parsed_json)

        caption_body = """WEBVTT

00:00:00.000 --> 00:00:02.000
AGI timelines are changing.

00:00:02.000 --> 00:00:04.000
Agents can use tools.
"""
        parsed_caption = transcript_to_text(caption_body, "text/plain", "https://example.test/captions")
        self.assertEqual(parsed_caption, "AGI timelines are changing.\nAgents can use tools.")
        self.assertNotIn("-->", parsed_caption)

    def test_prepare_transcript_text_classifies_and_strips_mixed_page(self) -> None:
        from research_factory.prep import prepare_transcript_text

        text = """Subscribe Sign in
Latent Space: The AI Engineer Podcast
Audio playback is not supported on your browser.
Transcript
HOST: Today we are talking about OpenAI agents and enterprise AI workflows.
GUEST: The important shift is that coding agents will move from demos to production workflows.
HOST: Does that mean companies are changing budgets, procurement, and release planning around these systems?
GUEST: Yes, the adoption pattern is moving toward production deployments where evals, safety checks, and workflow integration matter.
Full show notes always on https://example.test/show
"""
        prepared = prepare_transcript_text(
            text,
            content_type="text/html",
            source_kind="official_show_transcript",
            source_name="Latent Space",
            episode_title="Agents Episode",
        )
        self.assertEqual(prepared.artifact_type, "mixed_page")
        self.assertEqual(prepared.status, "prepared")
        self.assertIn("coding agents will move", prepared.cleaned_text)
        self.assertNotIn("Subscribe Sign in", prepared.cleaned_text)
        self.assertGreater(prepared.boilerplate_ratio, 0)

    def test_v2_prepare_enqueue_local_draft_normalizes_dense_observations(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v2",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "12",
            "--pilot-id",
            "pilot-v2-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)
        self.assertTrue(prepared["artifact_counts"])

        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v2",
            "--local-draft",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            label = conn.execute("SELECT id, output_json FROM labels WHERE label_pack = 'ai_discourse_v2'").fetchone()
            self.assertIsNotNone(label)
            output = json.loads(label["output_json"])
            self.assertEqual(output["schema_version"], "ai_discourse_v2")
            self.assertGreaterEqual(len(output["observations"]), 3)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM coded_observations").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM topic_mentions").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0], 0)

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertGreater(payload["derived_table_counts"]["coded_observations"], 0)
        self.assertGreater(payload["coding_metrics"]["v2_observations_per_1000_segment_words"], 0)
        trend = self.run_cli("export", "trend-report", "--topic", "agents", "--window", "month")
        self.assertIn("Trend Report", Path(trend["path"]).read_text(encoding="utf-8"))

    def test_v3_dynamic_discourse_local_draft_discovers_signals(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
            "--pilot-id",
            "pilot-v3-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)

        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v3",
            "--local-draft",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            label = conn.execute("SELECT id, output_json FROM labels WHERE label_pack = 'ai_discourse_v3'").fetchone()
            self.assertIsNotNone(label)
            output = json.loads(label["output_json"])
            self.assertEqual(output["schema_version"], "ai_discourse_v3")
            self.assertGreaterEqual(len(output["discourse_events"]), 4)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM term_usages").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM actor_positions").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM concepts").fetchone()[0], 0)

        discovered = self.run_cli("discover-concepts", "--window", "month", "--min-evidence", "1", "--min-source-diversity", "1")
        self.assertGreaterEqual(discovered["concepts_seen"], 1)
        shifts = self.run_cli("detect-shifts", "--window", "month", "--min-support", "1")
        self.assertGreaterEqual(shifts["signals"], 1)

        signal_report = self.run_cli("export", "signal-report", "--window", "month", "--limit", "10")
        self.assertIn("Discourse Signal Report", Path(signal_report["path"]).read_text(encoding="utf-8"))
        stance_report = self.run_cli("export", "actor-stance-report", "--window", "month")
        self.assertIn("Actor Stance Report", Path(stance_report["path"]).read_text(encoding="utf-8"))
        drift_report = self.run_cli("export", "term-drift-report", "--window", "month")
        self.assertIn("Term Drift Report", Path(drift_report["path"]).read_text(encoding="utf-8"))
        narrative = self.run_cli("export", "narrative-map", "--window", "month")
        self.assertTrue(Path(narrative["path"]).exists())

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertGreater(payload["coding_metrics"]["v3_discourse_events"], 0)
        self.assertGreater(payload["derived_table_counts"]["discourse_events"], 0)
        self.assertIn("useful_signal_metrics", payload)

    def test_v31_requires_gpt55_episode_context_before_segment_prompt_and_persists_identity_mentions(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")
        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
            "--pilot-id",
            "pilot-v31-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)

        local_draft = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v3_1",
            "--local-draft",
        )
        self.assertEqual(local_draft["completed"], 0)
        self.assertEqual(local_draft["failed"], 1)
        self.assertIn("GPT-5.5 full-episode extraction", local_draft["details"][0]["error"])

        waiting = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        self.assertEqual(waiting["status"], "waiting_for_episode_context")
        self.assertIn("episode_context_job_id", waiting)

        context_claim = self.run_cli("claim-context", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-context-test")
        self.assertIn("job_id", context_claim)
        context_prompt_text = Path(context_claim["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("Full Prepared Episode Transcript", context_prompt_text)
        self.assertIn("===== SEGMENT", context_prompt_text)

        with self.db_conn() as conn:
            context_job = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (context_claim["job_id"],)).fetchone()
            context_output = {
                "schema_version": "ai_discourse_v3_1_episode_context",
                "episode_id": context_job["target_id"],
                "context_summary": "A fixture AI podcast episode about AGI timelines, agents, coding agents, inference scaling, enterprise AI, and singularity terminology.",
                "speaker_map": [
                    {
                        "surface": "Alex Rivera",
                        "role": "guest",
                        "affiliation": None,
                        "aliases": ["The guest"],
                        "confidence": 0.72,
                        "rationale": "The transcript says the guest makes the AGI timeline claim.",
                    },
                    {
                        "surface": "host",
                        "role": "host",
                        "affiliation": None,
                        "aliases": ["The host"],
                        "confidence": 0.72,
                        "rationale": "The transcript says the host asks about terminology replacement.",
                    },
                ],
                "section_map": [
                    {
                        "section": "agentic AI and AGI timeline framing",
                        "segment_ids": [],
                        "summary": "The dialogue links agents and tool use to AGI timeline shifts.",
                    }
                ],
                "entity_seed": {
                    "people": [],
                    "organizations": [],
                    "products": [],
                    "models": [],
                    "terms": ["AGI", "agents", "coding agents", "inference scaling", "enterprise AI", "singularity"],
                },
                "concept_seed": [
                    {
                        "candidate_concept": "agentic_ai_timeline_mechanisms",
                        "surface_terms": ["AGI", "agents", "tools"],
                        "why_useful": "Tracks whether agentic tool use is being used to reframe AGI timelines.",
                    }
                ],
                "extraction_guidance": "Prioritize causal mechanisms, terminology drift between AGI and singularity, and actor-specific stance around agentic systems.",
                "quality_flags": [],
                "overall_confidence": 0.84,
                "needs_review": False,
                "review_reason": None,
            }
        Path(context_claim["output_path"]).write_text(json.dumps(context_output), encoding="utf-8")
        submitted_context = self.run_cli(
            "submit-context",
            "--job-id",
            str(context_claim["job_id"]),
            "--output-json",
            context_claim["output_path"],
            "--worker-id",
            "codex-context-test",
        )
        self.assertIn("episode_context_run_id", submitted_context)

        claim = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        self.assertIn("job_id", claim)
        prompt_text = Path(claim["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("episode_context_artifact", prompt_text)
        self.assertIn("speaker_map", prompt_text)
        self.assertNotIn("full_segmented_episode_text", prompt_text)
        self.assertNotIn("Full Prepared Episode Transcript", prompt_text)

        with self.db_conn() as conn:
            job = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (claim["job_id"],)).fetchone()
            segment = conn.execute("SELECT episode_id, text_path FROM segments WHERE id = ?", (job["target_id"],)).fetchone()
            segment_text = (self.root / segment["text_path"]).read_text(encoding="utf-8")
            evidence = "AGI timelines are changing because agents can use tools"
            start = segment_text.find(evidence)
            self.assertGreaterEqual(start, 0)
            end = start + len(evidence)
            output = {
                "schema_version": "ai_discourse_v3_1",
                "segment_id": job["target_id"],
                "episode_id": segment["episode_id"],
                "extraction_status": "coded",
                "segment_quality": {
                    "artifact_type": "dialogue_transcript",
                    "boilerplate_risk": "low",
                    "substantive_word_count": len(segment_text.split()),
                    "transcript_preparation_id": None,
                },
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "The segment contains substantive discussion from the transcript.",
                },
                "discourse_events": [
                    {
                        "event_type": "technical_mechanism",
                        "event_subtype": "agentic_capability_timeline_mechanism",
                        "actor": {"name": "Alex Rivera", "actor_type": "guest", "affiliation": None, "role": "speaker"},
                        "speaker_context": {"name": "Alex Rivera", "role": "guest", "affiliation": None, "confidence": 0.7},
                        "reported_actor": {"name": "agents", "actor_type": "model", "affiliation": None, "confidence": 0.7},
                        "source_context": {"kind": "substantive_dialogue", "confidence": 0.9, "rationale": "The evidence is from substantive transcript content."},
                        "target": {"raw_target": "AGI timelines and tool-using agents", "candidate_concept": "agentic_ai_timeline_mechanisms", "canonical_concept": None, "concept_confidence": 0.8},
                        "surface_terms": ["AGI", "agents", "tools"],
                        "frames": ["capability_timeline", "tool_use_mechanism"],
                        "model_names": [],
                        "product_names": [],
                        "organizations": [],
                        "people": ["Sam Altman"],
                        "stance": "neutral",
                        "claim_text": "The guest links changing AGI timelines to agents gaining tool-use capability.",
                        "claim_type": "causal",
                        "certainty": "medium",
                        "temporal_horizon": "present",
                        "causal_mechanism": "Agents can use tools, which is presented as a reason AGI timelines are changing.",
                        "counterclaim": "",
                        "metric": {"value": None, "unit": None, "comparator": None, "direction": "not_applicable", "raw_text": None},
                        "signal_reason": "This supports analysis of how agentic tool use changes AGI timeline framing over time.",
                        "exclusion_flags": [],
                        "quality_flags": [],
                        "evidence": evidence,
                        "evidence_start": start,
                        "evidence_end": end,
                        "confidence": 0.82,
                        "audit_notes": "GPT-style fixture output for v3.1 persistence.",
                    }
                ],
                "concept_candidates": [
                    {
                        "candidate": "agentic_ai_timeline_mechanisms",
                        "surface_terms": ["AGI", "agents", "tools"],
                        "rationale": "Tracks claims that agentic tool use changes AGI timeline expectations.",
                        "usefulness_score": 0.82,
                        "evidence": evidence,
                        "evidence_start": start,
                        "evidence_end": end,
                        "confidence": 0.82,
                    }
                ],
                "rejected_candidates": [],
                "no_signal_reason": None,
                "overall_confidence": 0.82,
                "needs_review": True,
                "review_reason": "High-impact AGI timeline mechanism.",
            }
        Path(claim["output_path"]).write_text(json.dumps(output), encoding="utf-8")
        submitted = self.run_cli("submit", "--job-id", str(claim["job_id"]), "--output-json", claim["output_path"], "--worker-id", "codex-test")
        self.assertIn("label_id", submitted)
        report = self.run_cli("scale-gate-report", "--pilot-id", "pilot-v31-test")
        self.assertEqual(report["labels"]["labels"], 1)
        self.assertEqual(report["labels"]["labels_without_completed_context"], 0)
        self.assertEqual(report["labels"]["non_gpt55_labels"], 0)
        self.assertEqual(report["evidence_offsets"]["offset_failures"], 0)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM labels WHERE label_pack = 'ai_discourse_v3_1'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM episode_context_runs WHERE status = 'completed'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM discourse_event_contexts").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_speaker_mentions").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM raw_actor_mentions").fetchone()[0], 2)
            context = conn.execute("SELECT source_context_kind, speaker_name, reported_actor_name FROM discourse_event_contexts").fetchone()
            self.assertEqual(context["source_context_kind"], "substantive_dialogue")
            self.assertEqual(context["speaker_name"], "Alex Rivera")
            self.assertEqual(context["reported_actor_name"], "agents")
            event = conn.execute("SELECT event_type FROM discourse_events").fetchone()
            self.assertEqual(event["event_type"], "causal_mechanism")

        queued_audit = self.run_cli("audit", "--sample", "1.0", "--label-pack", "ai_discourse_v3_1")
        self.assertEqual(queued_audit["queued"], 1)
        audit_run = self.run_cli("run", "--lane", "quality", "--limit", "5", "--job-types", "audit_label", "--no-claim-prompts", "--max-label-prompts", "0")
        self.assertEqual(audit_run["completed"], 1)
        with self.db_conn() as conn:
            audit = conn.execute("SELECT status, score, disagreement_json FROM quality_audits WHERE label_pack = 'ai_discourse_v3_1'").fetchone()
            self.assertEqual(audit["status"], "passed")
            self.assertGreaterEqual(audit["score"], 0.85)
            self.assertNotIn("entity is not explicit", audit["disagreement_json"])

        identity = self.run_cli("groom-identities", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5")
        self.assertEqual(identity["ok"], True)
        self.assertEqual(identity["review_status"], "deterministic_candidate_bootstrap_pending_gpt55_judge")
        with self.db_conn() as conn:
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM canonical_people").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM identity_resolution_candidates").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM podcast_guest_edges").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM person_concept_edges").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM expert_authority_scores").fetchone()[0], 1)
            segment_id = conn.execute("SELECT id FROM segments LIMIT 1").fetchone()["id"]
            conn.execute(
                """
                INSERT INTO jobs
                  (lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts,
                   dedupe_key, created_at, updated_at, completed_at)
                VALUES ('podcast', 'label_segment', ?, ?, 'completed', 100, 1, 2, ?, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z')
                """,
                (
                    segment_id,
                    json.dumps({"pilot_id": "pilot-v31-test", "label_pack": "ai_discourse_v3_1"}),
                    f"historical-relabel:{segment_id}",
                ),
            )
            conn.commit()

        reviewer = self.run_cli("reviewer-audit", "--pilot-id", "pilot-v31-test", "--episodes", "1", "--model", "gpt-5.5")
        self.assertEqual(reviewer["created"], 1)
        reviewer_handoff = reviewer["audits"][0]
        reviewer_prompt = Path(reviewer_handoff["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("Full direct episode text for private review", reviewer_prompt)
        event_packet = reviewer_prompt.split("Extracted discourse events:\n", 1)[1].split("\n\nIdentity mentions and graph evidence:", 1)[0]
        self.assertEqual(len(json.loads(event_packet)), 1)
        reviewer_output = {
            "schema_version": "reviewer_audit_v1",
            "pilot_id": "pilot-v31-test",
            "episode_id": reviewer_handoff["episode_id"],
            "scores": {
                "overall": 96,
                "coverage": 94,
                "precision": 97,
                "grounding": 98,
                "identity_graph_usefulness": 92,
                "product_market_signal_usefulness": 90,
            },
            "missed_signals": [],
            "false_or_weak_events": [],
            "p0_issues": [],
            "summary": "Fixture reviewer found the single extracted event well grounded.",
            "recommendation": "scale",
        }
        Path(reviewer_handoff["output_path"]).write_text(json.dumps(reviewer_output), encoding="utf-8")
        submitted_review = self.run_cli(
            "submit-reviewer-audit",
            "--audit-id",
            reviewer_handoff["audit_id"],
            "--output-json",
            reviewer_handoff["output_path"],
        )
        self.assertEqual(submitted_review["summary"]["completed"], 1)

        refreshed_review = self.run_cli("reviewer-audit", "--pilot-id", "pilot-v31-test", "--episodes", "1", "--model", "gpt-5.5", "--fresh")
        self.assertEqual(refreshed_review["refreshed"], 1)
        with self.db_conn() as conn:
            event_row = conn.execute("SELECT id, segment_id FROM discourse_events LIMIT 1").fetchone()
        reviewer_remediation_output = {
            "schema_version": "reviewer_audit_v1",
            "pilot_id": "pilot-v31-test",
            "episode_id": reviewer_handoff["episode_id"],
            "scores": {
                "overall": 80,
                "coverage": 78,
                "precision": 87,
                "grounding": 94,
                "identity_graph_usefulness": 74,
                "product_market_signal_usefulness": 82,
            },
            "missed_signals": [
                {
                    "severity": "P1",
                    "description": "Missed identity graph cue for the guest and who mentioned whom.",
                    "evidence": "short fixture excerpt",
                    "segment_id": event_row["segment_id"],
                }
            ],
            "false_or_weak_events": [
                {
                    "severity": "P0",
                    "discourse_event_id": event_row["id"],
                    "reason": "A numeric footnote marker was treated as a quantitative product-quality signal.",
                }
            ],
            "p0_issues": ["A numeric footnote marker created a materially false research signal."],
            "summary": "Fixture reviewer found targeted remediation issues.",
            "recommendation": "do_not_scale",
        }
        Path(reviewer_handoff["output_path"]).write_text(json.dumps(reviewer_remediation_output), encoding="utf-8")
        remediation_review = self.run_cli(
            "submit-reviewer-audit",
            "--audit-id",
            reviewer_handoff["audit_id"],
            "--output-json",
            reviewer_handoff["output_path"],
        )
        self.assertEqual(remediation_review["summary"]["p0_issues"], 1)
        findings = self.run_cli("reviewer-findings", "--pilot-id", "pilot-v31-test", "--severity", "P0,P1")
        self.assertEqual(findings["privacy"], "sanitized_no_transcript_text")
        self.assertGreaterEqual(len(findings["findings"]), 3)
        self.assertIn(event_row["segment_id"], findings["impacted_segment_ids"])
        self.assertNotIn("evidence", findings["findings"][0])
        failure_bank = self.run_cli("failure-bank", "--pilot-id", "pilot-v31-test", "--patch-tag", "remediation-fixture-v1")
        self.assertEqual(failure_bank["status"], "passed")
        self.assertEqual(failure_bank["synthetic_passed"], failure_bank["synthetic_total"])
        self.assertIn("numeric_artifact", failure_bank["failure_class_counts"])
        self.assertEqual(failure_bank["privacy"], "sanitized_no_transcript_text")
        self.assertTrue(Path(failure_bank["path"]).exists())
        delta = self.run_cli("audit-delta", "--pilot-id", "pilot-v31-test", "--label-pack", "ai_discourse_v3_1", "--patch-tag", "remediation-fixture-v1", "--sentinel", "1")
        self.assertGreaterEqual(delta["selected"], 1)
        self.assertGreaterEqual(delta["queued"], 1)
        self.assertTrue(delta["failure_class_counts"])
        targeted = self.run_cli(
            "reviewer-audit",
            "--pilot-id",
            "pilot-v31-test",
            "--episodes",
            "5",
            "--model",
            "gpt-5.5",
            "--mode",
            "targeted",
            "--patch-tag",
            "remediation-fixture-v1",
        )
        self.assertEqual(targeted["mode"], "targeted")
        self.assertEqual(targeted["patch_tag"], "remediation-fixture-v1")
        self.assertEqual(targeted["created"], 1)
        self.assertEqual(targeted["summary"]["review_mode"], "targeted")
        dry_requeue = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only", "--dry-run")
        self.assertGreaterEqual(dry_requeue["selected_segments"], 1)
        requeue = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only")
        self.assertGreaterEqual(requeue["enqueued"], 1)
        requeue_again = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only")
        self.assertGreaterEqual(requeue_again["skipped"], 1)

        clusters = self.run_cli("cluster-claims", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5")
        self.assertGreaterEqual(clusters["clusters_upserted"], 1)
        claim_edges = self.run_cli("judge-claim-edges", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5", "--limit", "10")
        self.assertEqual(claim_edges["status"], "candidate_edges_pending_gpt55_semantic_judge")

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["quality_velocity_metrics"]["latest_patch_tag"], "remediation-fixture-v1")
        self.assertIn(payload["quality_velocity_metrics"]["state"], {"targeted-review", "full-gate"})
        self.assertEqual(payload["coding_metrics"]["v3_1_full_episode_context_required"], True)
        self.assertEqual(payload["coding_metrics"]["v3_1_local_draft_disabled"], True)
        self.assertEqual(payload["useful_signal_metrics"]["episode_context_runs"]["completed"], 1)
        self.assertEqual(payload["useful_signal_metrics"]["reviewer_audits"]["completed"], 1)
        self.assertGreaterEqual(payload["useful_signal_metrics"]["claim_network"]["claim_clusters"], 1)
        self.assertGreaterEqual(payload["derived_table_counts"]["identity_resolution_candidates"], 1)
        self.assertGreater(payload["derived_table_counts"]["discourse_event_contexts"], 0)

    def test_transcript_exhaustion_gates_transcription_and_is_disabled_by_default(self) -> None:
        sources = self.write_audio_only_source()
        self.run_cli("init")
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["missing_transcripts"], 1)
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.assertEqual(candidates["candidates"][0]["acquisition_status"], "browser_discovery_pending")

        blocked = self.run_cli(
            "record-transcript-attempt",
            "--episode-id",
            episode_id,
            "--method",
            "youtube_caption",
            "--status",
            "youtube_caption_blocked",
            "--source-kind",
            "youtube_captions",
            "--error-class",
            "youtube_caption_blocked",
            "--notes",
            "Fixture YouTube caption request was blocked.",
        )
        self.assertEqual(blocked["status"], "youtube_caption_blocked")
        exhausted = self.run_cli("mark-transcript-exhausted", "--episode-id", episode_id, "--worker-id", "test-browser")
        self.assertEqual(exhausted["status"], "transcription_eligible")

        dry_run = self.run_cli(
            "enqueue-transcription",
            "--lane",
            "podcast",
            "--provider",
            "voyager",
            "--label-pack",
            "ai_discourse_v3_1",
            "--limit",
            "5",
            "--dry-run",
        )
        self.assertEqual(dry_run["selected"], 1)
        self.assertEqual(dry_run["enqueued"], 0)
        queued = self.run_cli(
            "enqueue-transcription",
            "--lane",
            "podcast",
            "--provider",
            "voyager",
            "--label-pack",
            "ai_discourse_v3_1",
            "--limit",
            "5",
        )
        self.assertEqual(queued["enqueued"], 1)

        failed = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "transcribe_audio",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(failed["failed"], 1)
        self.assertIn("Paid transcription fallback is disabled", failed["details"][0]["error"])

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["transcript_acquisition_metrics"]["status_counts"]["transcription_eligible"], 1)
        self.assertTrue(any(item["status"] == "failed" for item in payload["transcript_acquisition_metrics"]["transcription_runs"]))
        self.assertIn("transcript_acquisition_attempts", payload["counts"])
        funnel = self.run_cli("acquisition-funnel", "--source", "Audio Only Fixture", "--limit", "5")
        self.assertEqual(funnel["sources"][0]["acquisition_status"]["transcription_eligible"], 1)

    def test_scale_gate_report_blocks_until_v31_gate_criteria_pass(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        fetched = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "fetch_transcript",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(fetched["completed"], 1)
        gate = self.run_cli(
            "scale-gate-enqueue",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--pilot-id",
            "scale-gate-test",
            "--priority",
            "10",
            "--source",
            "Fixture Tech Podcast",
            "--include-low-signal",
        )
        self.assertEqual(gate["selected_episodes"], 1)
        self.assertGreaterEqual(gate["label_jobs"], 1)
        self.assertEqual(gate["episode_context_jobs"], 1)

        report = self.run_cli("scale-gate-report", "--pilot-id", "scale-gate-test")
        self.assertEqual(report["gate_state"], "blocked")
        self.assertEqual(report["episodes"]["selected"], 1)
        self.assertIn("selected_25_episodes", report["failed_checks"])
        self.assertIn("all_selected_episodes_have_gpt55_context", report["failed_checks"])
        self.assertIn("all_selected_segments_labeled_v31_gpt55", report["failed_checks"])
        self.assertIn("semantic_reviewer_audit_passes", report["failed_checks"])
        self.assertTrue(any(item["job_type"] == "label_segment" and item["status"] == "pending" for item in report["jobs"]))
        self.assertEqual(report["privacy"], "sanitized_operational_report_no_raw_transcripts")
        self.assertTrue(Path(report["path"]).exists())

    def test_enabled_voyager_fixture_transcription_uses_standard_ingest_path(self) -> None:
        sources = self.write_audio_only_source()
        transcript = self.root / "voyager-fixture.txt"
        transcript.write_text(
            "Host: Today we discuss AGI, AI agents, coding systems, and inference scaling. "
            "Guest: Frontier labs are shifting from chatbot demos toward agents that take actions across tools. "
            "Guest: Enterprise AI adoption depends on reliability, product integration, and evaluation discipline. "
            "Host: The terminology around AGI and singularity may change as model capabilities improve.",
            encoding="utf-8",
        )
        self.env["ALLOW_PAID_TRANSCRIPTION"] = "true"
        self.env["VOYAGER_API_KEY"] = "fixture-key"
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.run_cli("mark-transcript-exhausted", "--episode-id", episode_id, "--worker-id", "test-browser")
        queued = self.run_cli("enqueue-transcription", "--lane", "podcast", "--provider", "voyager", "--label-pack", "ai_discourse_v3_1", "--limit", "5")
        self.assertEqual(queued["enqueued"], 1)
        with self.db_conn() as conn:
            job = conn.execute("SELECT id, payload_json FROM jobs WHERE job_type = 'transcribe_audio'").fetchone()
            payload = json.loads(job["payload_json"])
            payload["fixture_text_path"] = str(transcript)
            conn.execute(
                "UPDATE jobs SET payload_json = ? WHERE id = ?",
                (json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")), job["id"]),
            )
            conn.commit()
        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "transcribe_audio",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM transcripts WHERE source_kind = 'voyager_transcription' AND status = 'ready'").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'label_segment'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM transcription_runs WHERE status = 'completed'").fetchone()[0], 1)

    def test_observer_density_uses_unique_segment_words_not_distinct_word_counts(self) -> None:
        self.run_cli("init")
        ts = "2026-07-01T00:00:00+00:00"
        with self.db_conn() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_density', 'Density Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_transcripts_only', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at)
                VALUES ('ep_density', 'src_density', 'density-1', 'Density Episode', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO transcripts (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_density', 'ep_density', 'official_show_transcript', 'https://example.test/transcript', 'text/plain', 'corpus/transcripts/density.txt', 'sha', 'ready', ?, '{}', 20, ?, ?)
                """,
                (ts, ts, ts),
            )
            for idx in range(2):
                segment_id = f"seg_density_{idx}"
                label_id = f"lab_density_{idx}"
                event_id = f"de_density_{idx}"
                conn.execute(
                    """
                    INSERT INTO segments (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                    VALUES (?, 'tr_density', 'ep_density', 'src_density', ?, 0, 10, ?, 'segsha', 10, ?)
                    """,
                    (segment_id, idx, f"corpus/segments/{segment_id}.txt", ts),
                )
                conn.execute(
                    """
                    INSERT INTO labels (id, segment_id, label_pack, label_pack_version, model, status, output_json, confidence, created_at)
                    VALUES (?, ?, 'ai_discourse_v3_1', 'v3.1', 'gpt-5.5', 'ready', '{}', 0.9, ?)
                    """,
                    (label_id, segment_id, ts),
                )
                conn.execute(
                    """
                    INSERT INTO discourse_events
                      (id, label_id, segment_id, event_index, event_type, claim_text, confidence, evidence_text, evidence_start, evidence_end, created_at)
                    VALUES (?, ?, ?, 0, 'term_usage', 'Fixture claim', 0.9, 'evidence', 0, 8, ?)
                    """,
                    (event_id, label_id, segment_id, ts),
                )
            conn.commit()
        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["coding_metrics"]["v3_1_discourse_events"], 2)
        self.assertEqual(payload["coding_metrics"]["v3_1_events_per_1000_segment_words"], 100.0)

    def test_quarantine_contaminated_removes_pending_label_jobs(self) -> None:
        from research_factory import db
        from research_factory.util import dumps_json, now_iso, sha256_text

        self.run_cli("init")
        with self.db_conn() as conn:
            ts = now_iso()
            conn.execute(
                "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('src', 'Source', ?, ?)",
                (ts, ts),
            )
            conn.execute(
                "INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at) VALUES ('ep', 'src', 'guid', 'Episode', ?, ?)",
                (ts, ts),
            )
            transcript_text = '[{"start":0,"end":1,"text":"AGI agents."},{"start":1,"end":2,"text":"More agents."}]'
            transcript_path = self.root / "corpus" / "transcripts" / "tr_bad.txt"
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_text(transcript_text, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_bad', 'ep', 'creator_provided_rss_transcript', 'https://example.test/transcription.json', 'binary/octet-stream', ?, ?, 'ready', ?, '{}', ?, ?, ?)
                """,
                ("corpus/transcripts/tr_bad.txt", sha256_text(transcript_text), ts, len(transcript_text.split()), ts, ts),
            )
            segment_path = self.root / "corpus" / "segments" / "seg_bad.txt"
            segment_path.parent.mkdir(parents=True, exist_ok=True)
            segment_path.write_text(transcript_text, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES ('seg_bad', 'tr_bad', 'ep', 'src', 0, 0, ?, 'corpus/segments/seg_bad.txt', ?, ?, ?)
                """,
                (len(transcript_text), sha256_text(transcript_text), len(transcript_text.split()), ts),
            )
            db.enqueue_job(conn, lane="podcast", job_type="label_segment", target_id="seg_bad", payload={"label_pack": "ai_discourse_v1"})
            conn.commit()
        result = self.run_cli("quarantine-contaminated", "--apply")
        self.assertEqual(result["found"], 1)
        with self.db_conn() as conn:
            status = conn.execute("SELECT status, policy_json FROM transcripts WHERE id = 'tr_bad'").fetchone()
            self.assertEqual(status["status"], "quarantined")
            self.assertIn("raw_json_timing_payload", dumps_json(json.loads(status["policy_json"])))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE id = 'seg_bad'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE target_id = 'seg_bad'").fetchone()[0], 0)

    def test_submit_rejects_non_exact_evidence(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        output_path = Path(claim["output_path"])
        with self.db_conn() as conn:
            row = conn.execute(
                """
                SELECT segments.id AS segment_id, segments.episode_id
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE jobs.id = ?
                """,
                (claim["job_id"],),
            ).fetchone()
        invalid_output = {
            "schema_version": "ai_discourse_v1",
            "segment_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "summary": "The segment discusses agents.",
            "topics": [{"topic": "agents", "stance": "bullish", "intensity": 0.7, "evidence": "agents ... tools"}],
            "terminology_shifts": [],
            "claims": [],
            "entities": {"people": [], "organizations": [], "products": []},
            "overall_confidence": 0.7,
            "needs_review": False,
            "review_reason": None,
        }
        output_path.write_text(json.dumps(invalid_output), encoding="utf-8")
        submitted = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(submitted.returncode, 0)
        self.assertIn("Evidence must be exact", submitted.stderr)

    def test_audit_job_writes_grounding_quality_audit(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        queued = self.run_cli("audit", "--sample", "1.0", "--label-pack", "ai_discourse_v1")
        self.assertGreaterEqual(queued["queued"], 1)
        audit_run = self.run_cli("run", "--lane", "quality", "--limit", "5", "--job-types", "audit_label", "--no-claim-prompts", "--max-label-prompts", "0")
        self.assertGreaterEqual(audit_run["completed"], 1)
        with self.db_conn() as conn:
            audit = conn.execute("SELECT status, score, disagreement_json FROM quality_audits LIMIT 1").fetchone()
        self.assertIn(audit["status"], {"passed", "needs_adjudication"})
        self.assertIsNotNone(audit["score"])
        self.assertIn("checks", audit["disagreement_json"])

    def test_run_respects_label_prompt_cap(self) -> None:
        transcript = self.root / "long.vtt"
        transcript.write_text(
            "WEBVTT\n\n00:00:00.000 --> 00:00:10.000\n" + " ".join("agents" for _ in range(1700)),
            encoding="utf-8",
        )
        feed = self.root / "long-feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Long Fixture</title><item>
  <guid>long-1</guid>
  <title>Long Episode</title>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "long-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Long Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        capped = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--job-types", "label_segment", "--max-label-prompts", "1", "--worker-id", "cap-labeler")
        self.assertEqual(capped["claimed_prompts"], 1)
        with self.db_conn() as conn:
            claimed = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'label_segment' AND status = 'claimed'").fetchone()[0]
        self.assertEqual(claimed, 1)

    def test_prioritize_labels_tags_bounded_pending_jobs(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        dry = self.run_cli(
            "prioritize-labels",
            "--category",
            "tests",
            "--keyword",
            "agents",
            "--limit",
            "1",
            "--priority",
            "15",
            "--pilot-id",
            "pilot-test",
            "--dry-run",
        )
        self.assertEqual(dry["selected"], 1)
        self.assertEqual(dry["updated"], 0)
        with self.db_conn() as conn:
            pending = conn.execute("SELECT priority, payload_json FROM jobs WHERE job_type = 'label_segment'").fetchone()
        self.assertEqual(pending["priority"], 100)

        applied = self.run_cli(
            "prioritize-labels",
            "--category",
            "tests",
            "--keyword",
            "agents",
            "--limit",
            "1",
            "--priority",
            "15",
            "--pilot-id",
            "pilot-test",
        )
        self.assertEqual(applied["selected"], 1)
        self.assertEqual(applied["updated"], 1)
        with self.db_conn() as conn:
            pending = conn.execute("SELECT priority, payload_json FROM jobs WHERE job_type = 'label_segment'").fetchone()
        payload = json.loads(pending["payload_json"])
        self.assertEqual(pending["priority"], 15)
        self.assertEqual(payload["pilot_id"], "pilot-test")
        self.assertEqual(payload["priority_reason"], "bounded_source_aware_label_pilot")

    def test_observer_redacts_public_state(self) -> None:
        from research_factory.ui_server import _sanitize_snapshot

        payload = {
            "generated_at": "2026-07-01T00:00:00+00:00",
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {"segments": 1},
            "job_counts": {"failed": 1},
            "active_jobs": [
                {
                    "id": 123,
                    "target_id": "seg_abcdef1234567890",
                    "job_type": "fetch_transcript",
                    "error": "failed https://www.youtube.com/watch?v=abc at /Users/kolbydayley/private",
                }
            ],
            "recent_runs": [{"id": "run_abcdef1234567890", "segment_id": "seg_abcdef1234567890", "status": "failed"}],
            "unknown": {"secret": "keep out"},
        }
        sanitized = _sanitize_snapshot(payload)
        rendered = json.dumps(sanitized)
        self.assertNotIn("target_id", rendered)
        self.assertNotIn("youtube.com", rendered)
        self.assertNotIn("/Users/", rendered)
        self.assertNotIn("run_abcdef", rendered)
        self.assertNotIn("unknown", rendered)

    def test_public_error_text_redacts_full_local_paths(self) -> None:
        from research_factory.observer import _public_text

        redacted = _public_text("failed at /Users/kolbydayley/Desktop/private/file.txt")
        self.assertEqual(redacted, "failed at <local-path>")

    def test_label_pack_examples_are_valid_outputs(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output
            from research_factory.labels import ValidationError

            for pack_dir in (self.root / "label_packs").iterdir():
                examples = json.loads((pack_dir / "examples.json").read_text(encoding="utf-8"))
                for example in examples:
                    if "output" in example:
                        validate_label_output(pack_dir.name, example["output"])
                    else:
                        self.fail(f"{pack_dir.name} example is missing a complete output object")
            invalid = json.loads(((self.root / "label_packs" / "ai_discourse_v1" / "examples.json").read_text(encoding="utf-8")))[0]["output"]
            invalid["segment_id"] = ""
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v1", invalid)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_numeric_page_artifact_as_metric(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = example["input"]["text"]
            marker_start = segment_text.rfind("4")
            self.assertGreaterEqual(marker_start, 0)
            event = output["discourse_events"][0]
            event["event_type"] = "product_signal"
            event["event_subtype"] = "quality_multiplier_artifact"
            event["claim_text"] = "The segment claims Gemini quality improved 4x."
            event["claim_type"] = "product_market"
            event["metric"] = {"value": "4x", "unit": "quality", "comparator": "improved", "direction": "increase", "raw_text": "4x"}
            event["evidence"] = "4"
            event["evidence_start"] = marker_start
            event["evidence_end"] = marker_start + 1
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_bare_digit_metric_artifact(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "Once I have proved something in Lean, the quality of the output is basically 4 as high as if it came from a human."
            output["segment_id"] = "seg_numeric_fixture"
            output["episode_id"] = "ep_numeric_fixture"
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            event["event_type"] = "market_signal"
            event["event_subtype"] = "quality_multiplier_artifact"
            event["claim_text"] = "RJ claims Lean-proven outputs are four times as high quality as human outputs."
            event["claim_type"] = "product_market"
            event["metric"] = {"value": "4", "unit": "quality multiplier", "comparator": "if it came from a human", "direction": "increase", "raw_text": "quality of the output is basically 4 as high"}
            event["evidence"] = segment_text
            event["evidence_start"] = 0
            event["evidence_end"] = len(segment_text)
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_sponsor_events_from_durable_extraction(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, render_prompt, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "AI agent security is one of the most important and most overlooked issues in technology right now."
            output["segment_id"] = "seg_sponsor_fixture"
            output["episode_id"] = "ep_sponsor_fixture"
            output["concept_candidates"] = []
            output["segment_source_context"] = {"kind": "sponsor_ad_read", "confidence": 0.95, "rationale": "Host-read sponsor copy."}
            event = output["discourse_events"][0]
            event["event_type"] = "risk_signal"
            event["event_subtype"] = "sponsor_security_claim"
            event["claim_text"] = "Sponsor copy claims AI agent security is overlooked."
            event["source_context"] = {"kind": "sponsor_ad_read", "confidence": 0.95, "rationale": "Host-read sponsor copy."}
            event["evidence"] = segment_text
            event["evidence_start"] = 0
            event["evidence_end"] = len(segment_text)
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)

            prompt = render_prompt("ai_discourse_v3_1", {"text": segment_text}, {"segment_id": "seg_sponsor_fixture"})
            self.assertIn("Sponsor/ad-read copy is excluded", prompt)
            self.assertNotIn("Sponsor/ad-read claims may be coded", prompt)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_allows_qualitative_comparison_metric_fields(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = example["input"]["text"]
            evidence = output["discourse_events"][0]["evidence"]
            event = output["discourse_events"][0]
            event["claim_text"] = "Burger says the model captured his learning style better than a human could."
            event["metric"] = {
                "value": None,
                "unit": None,
                "comparator": "a human",
                "direction": "increase",
                "raw_text": "better than a human",
            }
            event["evidence"] = evidence
            event["evidence_start"] = segment_text.index(evidence)
            event["evidence_end"] = event["evidence_start"] + len(evidence)
            validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_speaker_label_only_identity_event(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, repair_label_output_for_submission, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "Grant:\nI think the proof system bottleneck is still important."
            output["segment_id"] = "seg_identity_fixture"
            output["episode_id"] = "ep_identity_fixture"
            event = output["discourse_events"][0]
            event["event_type"] = "actor_mention"
            event["event_subtype"] = "speaker_label_only"
            event["actor"] = {"name": "Grant", "actor_type": "person", "affiliation": None, "role": "speaker"}
            event["speaker_context"] = {"name": "Grant", "role": "guest", "affiliation": None, "confidence": 0.6}
            event["reported_actor"] = {"name": None, "actor_type": "unknown", "affiliation": None, "confidence": 0.0}
            event["claim_text"] = "Grant appears as a transcript speaker label."
            event["signal_reason"] = "This only records a transcript speaker label without graph-useful context."
            event["evidence"] = "Grant:"
            event["evidence_start"] = 0
            event["evidence_end"] = len("Grant:")
            output["discourse_events"] = [event]
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
            repairs = repair_label_output_for_submission("ai_discourse_v3_1", output, segment_text=segment_text)
            self.assertEqual(repairs, 1)
            self.assertFalse(output["discourse_events"])
            self.assertEqual(output["rejected_candidates"][-1]["reason"], "unsupported_actor")
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_identity_graph_normalizes_common_asr_aliases(self) -> None:
        from research_factory.identity_graph import normalize_identity_name

        self.assertEqual(normalize_identity_name("Ron Roy"), "ranjan_roy")
        self.assertEqual(normalize_identity_name("Ranjan Roy of Margins"), "ranjan_roy")
        self.assertEqual(normalize_identity_name("Brandon Anderson / Latent Space"), "brandon_anderson")
        self.assertEqual(normalize_identity_name("Alex Kantrowitz Big Technology"), "alex_kantrowitz")

    def test_reviewer_event_dedupe_catches_overlap_restatements(self) -> None:
        from research_factory.scale_ops import _review_events_are_near_duplicates

        left = {
            "event_type": "forecast",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times says OpenAI is leaning toward holding off its initial public offering until next year.",
            "evidence_text": "OpenAI is leaning towards holding off its initial public offering until next year.",
        }
        right = {
            "event_type": "forecast",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times reports that OpenAI is leaning toward waiting until next year for its IPO.",
            "evidence_text": "OpenAI leans toward waiting next until next year for its IPO.",
        }
        unrelated = {
            "event_type": "market_signal",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times reports a different valuation signal about SpaceX.",
            "evidence_text": "SpaceX valuation talk is a different market thread.",
        }
        self.assertTrue(_review_events_are_near_duplicates(left, right))
        self.assertFalse(_review_events_are_near_duplicates(left, unrelated))

    def test_research_queue_backfills_content_and_envelopes(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        synced = self.run_cli("queue", "sync-envelopes")
        self.assertTrue(synced["ok"])
        status = self.run_cli("queue", "status", "--by", "lane,content_type,role,status")
        self.assertTrue(status["ok"])
        self.assertFalse(status["future_mcp"]["enabled"])
        queue_rows = status["queues"]
        self.assertTrue(any(row["content_type"] == "podcast_episode" for row in queue_rows))
        self.assertTrue(any(row["role"] in {"acquisition", "extractor"} for row in queue_rows))
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_sources").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM content_spans").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM queue_envelopes").fetchone()[0], 1)

    def test_worker_run_is_role_scoped_and_records_worker_run(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        run = self.run_cli(
            "worker",
            "run",
            "--role",
            "acquisition",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--worker-id",
            "test-headless-acquisition",
            "--no-claim-prompts",
        )
        self.assertTrue(run["ok"])
        self.assertEqual(run["role"], "acquisition")
        self.assertEqual(run["processed"], 1)
        with self.db_conn() as conn:
            worker = conn.execute("SELECT worker_role, status, claimed_jobs FROM worker_runs WHERE id = ?", (run["worker_run_id"],)).fetchone()
        self.assertEqual(worker["worker_role"], "acquisition")
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["claimed_jobs"], 1)

    def test_worker_run_requires_burst_for_more_than_four_jobs(self) -> None:
        self.run_cli("init")
        result = self.run_cli_process("worker", "run", "--role", "acquisition", "--limit", "5")
        self.assertNotEqual(result.returncode, 0)

    def test_future_remote_queue_respects_privacy_tier(self) -> None:
        from research_factory import db

        self.run_cli("init")
        with self.db_conn() as conn:
            local_job = db.enqueue_job(
                conn,
                lane="research",
                job_type="label_segment",
                target_id="local-only-target",
                payload={"privacy_tier": "local_only", "content_type": "blog_post", "model": "gpt-5.5"},
            )
            remote_job = db.enqueue_job(
                conn,
                lane="research",
                job_type="label_segment",
                target_id="public-link-target",
                payload={"privacy_tier": "public_link_only", "content_type": "blog_post", "model": "gpt-5.5"},
            )
            conn.commit()
        self.run_cli("queue", "sync-envelopes")
        claimed = self.run_cli("remote-queue", "claim-job", "--worker-id", "future-chatgpt-task", "--capability", "extractor", "--max-items", "2")
        self.assertFalse(claimed["enabled"])
        self.assertEqual([item["job_id"] for item in claimed["claimed"]], [remote_job])
        self.assertNotIn(local_job, [item["job_id"] for item in claimed["claimed"]])

    def test_production_cycle_observer_only_is_local_first(self) -> None:
        self.run_cli("init")
        result = self.run_cli("production-cycle", "--worker-mode", "none")
        self.assertTrue(result["ok"])
        self.assertFalse(result["published"])
        self.assertFalse(result["railway_compute_enabled"])
        self.assertFalse(result["mcp_remote_worker_enabled"])
        self.assertTrue(any(step["step"] == "sync_queue_envelopes" for step in result["steps"]))
        self.assertTrue(any(step["step"] == "privacy_scan" and step["ok"] for step in result["steps"]))
        snapshot_step = next(step for step in result["steps"] if step["step"] == "snapshot")
        self.assertTrue(Path(snapshot_step["path"]).exists())

    def test_railway_cost_guard_allows_only_observer_footprint(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"id": "svc", "name": "observer-ui"}}]},
            "environments": {
                "edges": [
                    {
                        "node": {
                            "serviceInstances": {
                                "edges": [
                                    {
                                        "node": {
                                            "serviceName": "observer-ui",
                                            "nextCronRunAt": None,
                                            "volumeInstances": {"edges": []},
                                            "latestDeployment": {
                                                "meta": {
                                                    "volumeMounts": [],
                                                    "serviceManifest": {
                                                        "deploy": {
                                                            "cronSchedule": None,
                                                            "numReplicas": 1,
                                                            "multiRegionConfig": {},
                                                            "startCommand": "python3 -m research_factory.ui_server",
                                                            "preDeployCommand": None,
                                                            "sleepApplication": False,
                                                        }
                                                    },
                                                }
                                            },
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
        }
        path = self.root / "railway-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path))
        self.assertTrue(result["ok"])
        self.assertEqual(result["services"], ["observer-ui"])
        self.assertTrue(result["warnings"])

    def test_railway_cost_guard_blocks_compute_like_service(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"name": "observer-ui"}}, {"node": {"name": "extractor-worker"}}]},
            "environments": {"edges": []},
        }
        path = self.root / "bad-railway-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path))
        self.assertFalse(result["ok"])
        self.assertTrue(any("Unexpected Railway services" in issue["message"] for issue in result["issues"]))

    def test_tech_discourse_v1_schema_validates_example(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output

            example = json.loads((self.root / "label_packs" / "tech_discourse_v1" / "examples.json").read_text(encoding="utf-8"))[0]
            validate_label_output("tech_discourse_v1", example["output"], segment_text=example["input"]["text"])
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_queue_claim_respects_lease_and_attempt_budget_across_connections(self) -> None:
        from research_factory import db
        from research_factory.worker import claim_next_job

        Path(self.env["RESEARCH_FACTORY_DB"]).parent.mkdir(parents=True, exist_ok=True)
        conn_a = db.connect(Path(self.env["RESEARCH_FACTORY_DB"]))
        conn_b = db.connect(Path(self.env["RESEARCH_FACTORY_DB"]))
        try:
            db.init_db(conn_a)
            job_id = db.enqueue_job(conn_a, lane="podcast", job_type="fetch_transcript", target_id="episode-1", payload={}, max_attempts=2)
            conn_a.commit()
            first = claim_next_job(conn_a, lane="podcast", worker_id="worker-a", job_types=("fetch_transcript",))
            self.assertEqual(first["id"], job_id)
            self.assertIsNone(claim_next_job(conn_b, lane="podcast", worker_id="worker-b", job_types=("fetch_transcript",)))

            conn_a.execute("UPDATE jobs SET leased_until = '2000-01-01T00:00:00+00:00' WHERE id = ?", (job_id,))
            conn_a.commit()
            second = claim_next_job(conn_b, lane="podcast", worker_id="worker-b", job_types=("fetch_transcript",))
            self.assertEqual(second["id"], job_id)
            self.assertEqual(second["lease_owner"], "worker-b")

            conn_b.execute("UPDATE jobs SET status = 'pending', lease_owner = NULL, leased_until = NULL, attempts = max_attempts WHERE id = ?", (job_id,))
            conn_b.commit()
            self.assertIsNone(claim_next_job(conn_a, lane="podcast", worker_id="worker-a", job_types=("fetch_transcript",)))
        finally:
            conn_a.close()
            conn_b.close()

    def test_init_db_reports_clear_concurrent_initialization_conflict(self) -> None:
        db_path = Path(self.env["RESEARCH_FACTORY_DB"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = db_path.with_suffix(db_path.suffix + ".init.lock")
        self.env["RESEARCH_FACTORY_INIT_LOCK_TIMEOUT_SECONDS"] = "0"
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                result = self.run_cli_process("init")
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("another research_factory process is initializing the SQLite schema", result.stderr)
        self.assertNotIn("database is locked", result.stderr)


if __name__ == "__main__":
    unittest.main()
