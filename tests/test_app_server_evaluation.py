from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_evaluation import (
    APP_SERVER_DEVELOPMENT_MANIFEST_VERSION,
    APP_SERVER_DEVELOPMENT_MANIFEST_V2,
    APP_SERVER_CORE_ARM_VERSION,
    aggregate_app_server_development_matrix,
    build_episode_base_instructions,
    build_episode_batch_prompt,
    episode_batch_core_schema,
    export_app_server_development_manifest,
    export_app_server_development_manifest_v2,
    _load_prepared_episodes,
    normalize_episode_batch_output,
    plan_episode_batches,
    run_app_server_core_arm,
)
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.efficient_backtest import build_windowed_segment_packet
from research_factory.labels import ValidationError
from research_factory.util import sha256_text


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "fake_codex_app_server.py"


def core_event(text: str, *, metric_raw_text: str = "next year") -> dict:
    return {
        "window_id": 0,
        "event_type": "forecast",
        "event_subtype": "workflow_substitution_timeline",
        "claim_type": "prediction",
        "actor_name": "Guest",
        "actor_type": "person",
        "speaker_name": "Guest",
        "speaker_role": "guest",
        "reported_actor_name": "",
        "reported_actor_type": "none",
        "source_context_kind": "substantive_dialogue",
        "target_concept": "routine email triage",
        "claim_text": "The guest predicts replacement of routine email triage within a year.",
        "stance": "warning",
        "certainty": "high",
        "temporal_horizon": "near_future",
        "causal_mechanism": "Agent automation takes over repetitive message triage.",
        "counterclaim": "",
        "metric_value": "",
        "metric_unit": "year",
        "metric_comparator": "",
        "metric_direction": "not_applicable",
        "metric_raw_text": metric_raw_text,
        "signal_reason": "This is a concrete near-term workflow forecast.",
        "evidence": text,
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Guest"],
        "confidence": 0.9,
    }


def coded_segment(segment_id: str, text: str, *, metric_raw_text: str = "next year") -> dict:
    return {
        "segment_id": segment_id,
        "status": "coded",
        "segment_source_context": {
            "kind": "substantive_dialogue",
            "confidence": 0.9,
            "rationale": "The segment contains a direct forecast.",
        },
        "no_signal_reason": "",
        "events": [core_event(text, metric_raw_text=metric_raw_text)],
    }


def no_signal_segment(segment_id: str) -> dict:
    return {
        "segment_id": segment_id,
        "status": "no_signal",
        "segment_source_context": {
            "kind": "show_setup",
            "confidence": 0.9,
            "rationale": "The segment contains setup without a durable event.",
        },
        "no_signal_reason": "No durable event is present.",
        "events": [],
    }


def prepared_episode(segment_count: int = 4) -> list[dict]:
    segments = []
    for index in range(segment_count):
        text = f"Synthetic segment {index}."
        windows, boundaries = build_windowed_segment_packet(
            text,
            window_count=1,
            context_chars=0,
        )
        segments.append(
            {
                "segment_id": f"seg_private_{index}",
                "segment_index": index,
                "segment_text": text,
                "text_sha256": sha256_text(text),
                "golden_event_count": 0 if index == 0 else 16,
                "density_stratum": "no_signal" if index == 0 else "dense",
                "windows": windows,
                "boundaries": boundaries,
            }
        )
    return [
        {
            "episode_id": "ep_private",
            "source_name": "Private source",
            "episode_title": "Private title",
            "episode_context": {
                "context_summary": "Private synthetic context",
                "speaker_map": [],
                "section_map": [],
                "entity_seed": {},
                "concept_seed": [],
                "extraction_guidance": "Read semantically.",
                "quality_flags": [],
            },
            "segments": segments,
        }
    ]


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _ManifestConnection:
    def __init__(self, label_rows):
        self.label_rows = label_rows

    def execute(self, sql, _params):
        if "FROM episodes e" in sql:
            return _Rows(
                [
                    {
                        "id": "ep_dev",
                        "title": "Development episode",
                        "source_id": "src_dev",
                        "source_name": "Development source",
                    }
                ]
            )
        if "FROM segments seg" in sql:
            return _Rows(self.label_rows)
        raise AssertionError(sql)


class AppServerEvaluationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_batch_schema_has_one_hard_capped_global_event_array_per_segment(self) -> None:
        schema = episode_batch_core_schema(
            episode_id="ep_1",
            segment_ids=["seg_1", "seg_2"],
            max_events_per_segment=25,
        )
        segments = schema["properties"]["segments"]
        self.assertEqual(segments["minItems"], 2)
        self.assertEqual(segments["maxItems"], 2)
        segment_schema = segments["items"]
        self.assertEqual(segment_schema["properties"]["events"]["maxItems"], 25)
        self.assertNotIn("allOf", segment_schema)
        self.assertNotIn("windows", segment_schema["properties"])
        self.assertEqual(
            segment_schema["properties"]["segment_id"]["enum"],
            ["seg_1", "seg_2"],
        )

    def test_episode_bootstrap_owns_context_and_turn_prompt_contains_only_segments(self) -> None:
        base = build_episode_base_instructions(
            core_instructions="Immutable semantic extraction rules.",
            episode={"episode_id": "ep_1", "source_name": "Source", "episode_title": "Title"},
            episode_context={
                "context_summary": "Shared context",
                "speaker_map": [],
                "section_map": [],
                "entity_seed": {},
                "concept_seed": [],
                "extraction_guidance": "Read semantically.",
                "quality_flags": [],
            },
        )
        prompt = build_episode_batch_prompt(
            segments=[
                {
                    "segment_id": "seg_1",
                    "segment_index": 4,
                    "windows": [{"window_id": 0, "extract_text": "Exact window text"}],
                }
            ],
        )
        self.assertLess(base.index("Immutable semantic extraction rules"), base.index("Shared episode"))
        self.assertIn("Shared context", base)
        self.assertNotIn("seg_1", base)
        self.assertNotIn("Shared episode", prompt)
        self.assertNotIn("Shared context", prompt)
        self.assertIn("Exact window text", prompt)

    def test_batch_planning_preserves_order_without_cross_episode_logic(self) -> None:
        segments = [{"segment_id": f"seg_{index}"} for index in range(8)]
        batches = plan_episode_batches(segments, batch_size=3)
        self.assertEqual([len(batch) for batch in batches], [3, 3, 2])
        self.assertEqual(
            [row["segment_id"] for batch in batches for row in batch],
            [row["segment_id"] for row in segments],
        )

    def test_normalization_requires_exact_order_and_reports_metric_grounding(self) -> None:
        text = "The guest says AI agents will replace routine email triage over the next year."
        _windows, boundaries = build_windowed_segment_packet(text, window_count=1, context_chars=0)
        prepared = [
            {"segment_id": "seg_1", "segment_text": text, "boundaries": boundaries},
            {"segment_id": "seg_2", "segment_text": "Brief setup.", "boundaries": build_windowed_segment_packet("Brief setup.", window_count=1, context_chars=0)[1]},
        ]
        reversed_payload = {
            "episode_id": "ep_1",
            "segments": [no_signal_segment("seg_2"), coded_segment("seg_1", text)],
        }
        with self.assertRaises(ValidationError):
            normalize_episode_batch_output(
                reversed_payload,
                episode_id="ep_1",
                prepared_segments=prepared,
            )

        payload = {
            "episode_id": "ep_1",
            "segments": [coded_segment("seg_1", text), no_signal_segment("seg_2")],
        }
        normalized, diagnostics = normalize_episode_batch_output(
            payload,
            episode_id="ep_1",
            prepared_segments=prepared,
        )
        self.assertEqual(len(normalized["segments"]), 2)
        self.assertEqual(diagnostics[0]["exactness_pruned_events"], 0)
        self.assertEqual(diagnostics[0]["metric_grounding_error_events"], 0)
        self.assertTrue(diagnostics[0]["status_ok"])

        payload["segments"][0] = coded_segment("seg_1", text, metric_raw_text="two years")
        _normalized, diagnostics = normalize_episode_batch_output(
            payload,
            episode_id="ep_1",
            prepared_segments=prepared,
        )
        self.assertEqual(diagnostics[0]["metric_grounding_error_events"], 1)
        self.assertFalse(diagnostics[0]["status_ok"])

    def test_development_manifest_selection_uses_only_golden_event_counts(self) -> None:
        context_path = self.root / "context.json"
        context_path.write_text('{"context_summary":"fixture"}\n', encoding="utf-8")
        texts = {f"seg_{index}": f"Synthetic segment {index}." for index in range(6)}
        event_counts = [0, 2, 16, 18, 20, 22]
        rows = []
        for index, event_count in enumerate(event_counts):
            output_json = json.dumps({"discourse_events": [{} for _ in range(event_count)]})
            rows.append(
                {
                    "segment_id": f"seg_{index}",
                    "segment_index": index,
                    "text_sha256": sha256_text(texts[f"seg_{index}"]),
                    "output_json": output_json,
                    "created_at": "2026-07-11T00:00:00+00:00",
                }
            )
        connection = _ManifestConnection(rows)
        with patch(
            "research_factory.app_server_evaluation._safe_segment_text",
            side_effect=lambda _conn, segment_id: (texts[segment_id], None),
        ), patch(
            "research_factory.app_server_evaluation.completed_episode_context_for_episode",
            return_value={"id": "ctx_1", "context_artifact_path": str(context_path)},
        ):
            report = export_app_server_development_manifest(
                connection,
                output_path=self.root / "manifest.json",
                episode_ids=["ep_dev"],
                segments_per_episode=5,
                minimum_no_signal_per_episode=1,
                minimum_dense_per_episode=3,
                dense_event_min=16,
            )
        manifest = json.loads((self.root / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], APP_SERVER_DEVELOPMENT_MANIFEST_VERSION)
        self.assertIn("no_transcript_keyword_or_regex", manifest["selection_policy"])
        self.assertEqual(report["density_counts"], {"dense": 4, "no_signal": 1})
        self.assertEqual(len(manifest["episodes"][0]["segments"]), 5)

    def test_matrix_aggregation_requires_exactly_symmetric_arms(self) -> None:
        paths = []
        for batch_size in (3, 5, 8):
            for thread_mode in ("new_thread", "same_thread"):
                payload = {
                    "schema_version": APP_SERVER_CORE_ARM_VERSION,
                    "manifest_sha256": "manifest",
                    "core_instructions_sha256": "instructions",
                    "episode_base_instructions_set_sha256": "episode-instructions",
                    "guideline_artifact_sha256": "guidelines",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "low",
                    "concurrency": 1,
                    "retry_count": 0,
                    "window_count": 4,
                    "context_chars": 900,
                    "max_events_per_segment": 32,
                    "requested_segments": 16,
                    "batch_size_ceiling": batch_size,
                    "thread_mode": thread_mode,
                }
                path = self.root / f"{batch_size}-{thread_mode}.json"
                path.write_text(json.dumps(payload), encoding="utf-8")
                paths.append(path)
        matrix = aggregate_app_server_development_matrix(
            arm_report_paths=paths,
            output_path=self.root / "matrix.json",
        )
        self.assertEqual(matrix["arm_count"], 6)
        self.assertFalse(matrix["selection_eligible"])
        with self.assertRaises(ValueError):
            aggregate_app_server_development_matrix(
                arm_report_paths=paths[:-1],
                output_path=self.root / "missing.json",
            )


class AppServerCoreArmTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    async def test_fake_arm_aggregates_usage_and_keeps_report_sanitized(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                    "event_cap": 32,
                    "episodes": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        prepared = prepared_episode()
        log_path = self.root / "methods.jsonl"

        def client_factory():
            return CodexAppServerClient(
                command=[
                    sys.executable,
                    str(FIXTURE),
                    "--scenario",
                    "episode_batch",
                    "--log",
                    str(log_path),
                ],
                verify_cli=False,
                request_timeout_seconds=1,
                usage_grace_seconds=0.05,
            )

        with patch(
            "research_factory.app_server_evaluation._load_prepared_episodes",
            return_value=prepared,
        ):
            report = await run_app_server_core_arm(
                None,
                manifest_path=manifest_path,
                output_dir=self.root / "arm",
                batch_size=3,
                thread_mode="new_thread",
                model="gpt-5.6-sol",
                concurrency=1,
                client_factory=client_factory,
            )

        self.assertEqual(report["requested_calls"], 2)
        self.assertEqual(report["attempted_calls"], 2)
        self.assertEqual(report["validated_segments"], 4)
        self.assertEqual(report["schema_status_success_rate"], 1.0)
        self.assertEqual(report["usage"]["input_tokens"], 203)
        self.assertEqual(report["usage_status"], "complete")
        self.assertTrue(report["accounting_complete"])
        self.assertEqual(report["observed_thread_count"], 2)
        self.assertEqual(len(report["instruction_source_sets"]), 1)
        self.assertIsNone(report["no_signal_false_positive_rate"])
        report_text = (self.root / "arm" / "report.json").read_text(encoding="utf-8")
        self.assertNotIn("seg_private", report_text)
        self.assertNotIn("Private synthetic context", report_text)
        private_text = (self.root / "arm" / "private-mapping.json").read_text(encoding="utf-8")
        self.assertIn("seg_private_0", private_text)
        protocol_log = [
            json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()
        ]
        thread_starts = [row for row in protocol_log if row["method"] == "thread/start"]
        turn_starts = [row for row in protocol_log if row["method"] == "turn/start"]
        self.assertEqual(len(thread_starts), 2)
        self.assertTrue(all(row["base_instructions"]["contains_shared_episode_context"] for row in thread_starts))
        self.assertTrue(all(not row["base_instructions"]["contains_segment_packets"] for row in thread_starts))
        self.assertTrue(all(row["prompt"]["contains_segment_packets"] for row in turn_starts))
        self.assertTrue(all(not row["prompt"]["contains_shared_episode_context"] for row in turn_starts))

    async def test_same_thread_bootstraps_episode_once_and_never_resends_context(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                    "event_cap": 32,
                    "episodes": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        log_path = self.root / "same-thread-methods.jsonl"

        def client_factory():
            return CodexAppServerClient(
                command=[
                    sys.executable,
                    str(FIXTURE),
                    "--scenario",
                    "episode_batch",
                    "--log",
                    str(log_path),
                ],
                verify_cli=False,
                request_timeout_seconds=1,
                usage_grace_seconds=0.05,
            )

        with patch(
            "research_factory.app_server_evaluation._load_prepared_episodes",
            return_value=prepared_episode(),
        ):
            report = await run_app_server_core_arm(
                None,
                manifest_path=manifest_path,
                output_dir=self.root / "same-thread-arm",
                batch_size=3,
                thread_mode="same_thread",
                model="gpt-5.6-sol",
                concurrency=1,
                client_factory=client_factory,
            )

        protocol_log = [
            json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()
        ]
        thread_starts = [row for row in protocol_log if row["method"] == "thread/start"]
        turn_starts = [row for row in protocol_log if row["method"] == "turn/start"]
        self.assertEqual(report["observed_thread_count"], 1)
        self.assertEqual(len(thread_starts), 1)
        self.assertEqual(len(turn_starts), 2)
        self.assertTrue(thread_starts[0]["base_instructions"]["contains_shared_episode_context"])
        self.assertTrue(all(not row["prompt"]["contains_shared_episode_context"] for row in turn_starts))
        self.assertEqual(
            report["request_overhead_bytes"]["planned_thread_start_base_instructions_total"],
            report["request_overhead_bytes"]["observed_thread_start_base_instructions_total"],
        )

    async def test_incomplete_accounting_nulls_rates_and_preserves_unknown_usage(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                    "event_cap": 32,
                    "episodes": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )

        def client_factory():
            return CodexAppServerClient(
                command=[sys.executable, str(FIXTURE), "--scenario", "timeout"],
                verify_cli=False,
                request_timeout_seconds=1,
                interrupt_timeout_seconds=0.05,
                usage_grace_seconds=0.01,
            )

        with patch(
            "research_factory.app_server_evaluation._load_prepared_episodes",
            return_value=prepared_episode(segment_count=1),
        ):
            report = await run_app_server_core_arm(
                None,
                manifest_path=manifest_path,
                output_dir=self.root / "incomplete-arm",
                batch_size=3,
                thread_mode="new_thread",
                model="gpt-5.6-sol",
                concurrency=1,
                timeout_seconds=0.02,
                client_factory=client_factory,
            )

        self.assertEqual(report["validated_segments"], 0)
        self.assertFalse(report["accounting_complete"])
        self.assertEqual(report["usage_status"], "unknown")
        self.assertIsNone(report["usage"])
        self.assertIsNone(report["measured_partial_usage"])
        self.assertIsNone(report["segments_per_second"])
        self.assertIsNone(report["raw_input_tokens_per_segment"])
        self.assertIsNone(report["raw_exact_evidence_rate"])
        self.assertIsNone(report["normalized_exact_evidence_rate"])

    async def test_core_arm_refuses_event_cap_drift(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                    "event_cap": 32,
                    "episodes": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "must equal the frozen manifest event cap"):
            await run_app_server_core_arm(
                None,
                manifest_path=manifest_path,
                output_dir=self.root / "cap-drift-arm",
                batch_size=3,
                thread_mode="new_thread",
                max_events_per_segment=25,
            )


class DevelopmentManifestV2Test(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.env = patch.dict(os.environ, {"RESEARCH_FACTORY_ROOT": str(self.root)})
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE sources (id TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE episodes (id TEXT PRIMARY KEY, title TEXT, source_id TEXT);
            CREATE TABLE transcripts (
              id TEXT PRIMARY KEY, episode_id TEXT, source_kind TEXT, source_url TEXT,
              raw_text_path TEXT, raw_text_sha256 TEXT, status TEXT, word_count INTEGER,
              updated_at TEXT
            );
            CREATE TABLE transcript_preparations (
              id TEXT PRIMARY KEY, transcript_id TEXT, status TEXT, cleaned_text_path TEXT,
              cleaned_text_sha256 TEXT, substantive_word_count INTEGER, quality_score REAL,
              updated_at TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, transcript_id TEXT, segment_index INTEGER,
              text_sha256 TEXT, text_path TEXT
            );
            CREATE TABLE labels (
              id TEXT PRIMARY KEY, segment_id TEXT, label_pack TEXT, model TEXT, status TEXT,
              output_json TEXT, output_path TEXT, created_at TEXT
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, segment_id TEXT, label_pack TEXT, model TEXT, status TEXT,
              output_path TEXT, completed_at TEXT, updated_at TEXT
            );
            CREATE TABLE episode_context_runs (
              id TEXT PRIMARY KEY, episode_id TEXT, transcript_id TEXT, label_pack TEXT,
              model TEXT, status TEXT, context_artifact_path TEXT
            );
            """
        )
        self.exclusion_path = self.root / "exclusions.json"
        self.exclusion_path.write_text('{"episodes":[]}\n', encoding="utf-8")

    def tearDown(self) -> None:
        self.conn.close()
        self.env.stop()
        self.tempdir.cleanup()

    def seed_episode(self, episode_number: int, *, alternate_disagreement: bool = False) -> list[str]:
        source_id = f"src_{episode_number}"
        episode_id = f"ep_{episode_number}"
        transcript_id = f"tr_{episode_number}"
        self.conn.execute("INSERT INTO sources VALUES (?, ?)", (source_id, f"Source {episode_number}"))
        self.conn.execute(
            "INSERT INTO episodes VALUES (?, ?, ?)",
            (episode_id, f"Episode {episode_number}", source_id),
        )
        raw_text = f"Canonical transcript {episode_number}."
        raw_path = self.root / "corpus" / "transcripts" / f"{transcript_id}.txt"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(raw_text, encoding="utf-8")
        self.conn.execute(
            "INSERT INTO transcripts VALUES (?, ?, 'fixture', NULL, ?, ?, 'ready', 10, '2026-07-11')",
            (
                transcript_id,
                episode_id,
                str(raw_path.relative_to(self.root)),
                sha256_text(raw_text),
            ),
        )
        prepared_path = self.root / "corpus" / "prepared" / f"{transcript_id}.txt"
        prepared_path.parent.mkdir(parents=True, exist_ok=True)
        prepared_path.write_text(raw_text, encoding="utf-8")
        self.conn.execute(
            "INSERT INTO transcript_preparations VALUES (?, ?, 'prepared', ?, ?, 10, 1.0, '2026-07-11')",
            (
                f"prep_{episode_number}",
                transcript_id,
                str(prepared_path.relative_to(self.root)),
                sha256_text(raw_text),
            ),
        )
        context_path = self.root / "runs" / "contexts" / f"ctx_{episode_number}.json"
        context_path.parent.mkdir(parents=True, exist_ok=True)
        context_path.write_text('{"context_summary":"fixture"}\n', encoding="utf-8")
        self.conn.execute(
            "INSERT INTO episode_context_runs VALUES (?, ?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?)",
            (f"ctx_{episode_number}", episode_id, transcript_id, str(context_path)),
        )
        hashes = []
        for index in range(8):
            segment_id = f"seg_{episode_number}_{index}"
            text = f"Unique episode {episode_number} segment {index}."
            text_hash = sha256_text(text)
            hashes.append(text_hash)
            text_path = self.root / "corpus" / "segments" / f"{segment_id}.txt"
            text_path.parent.mkdir(parents=True, exist_ok=True)
            text_path.write_text(text, encoding="utf-8")
            self.conn.execute(
                "INSERT INTO segments VALUES (?, ?, ?, ?, ?, ?)",
                (
                    segment_id,
                    episode_id,
                    transcript_id,
                    index,
                    text_hash,
                    str(text_path.relative_to(self.root)),
                ),
            )
            event_count = 0 if index == 0 else 16 + index
            output = json.dumps({"discourse_events": [{} for _ in range(event_count)]})
            output_path = self.root / "runs" / "outputs" / f"run_{episode_number}_{index}.json"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(output, encoding="utf-8")
            self.conn.execute(
                "INSERT INTO labels VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, '2026-07-11')",
                (f"label_{episode_number}_{index}", segment_id, output, str(output_path)),
            )
            self.conn.execute(
                "INSERT INTO label_runs VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, '2026-07-11', '2026-07-11')",
                (f"run_{episode_number}_{index}", segment_id, str(output_path)),
            )
        if alternate_disagreement:
            alt_transcript = f"tr_{episode_number}_alternate"
            self.conn.execute(
                "INSERT INTO transcripts VALUES (?, ?, 'fixture', NULL, ?, ?, 'ready', 10, '2026-07-11')",
                (
                    alt_transcript,
                    episode_id,
                    str(raw_path.relative_to(self.root)),
                    sha256_text(raw_text),
                ),
            )
            alt_segment = f"seg_{episode_number}_alternate_1"
            self.conn.execute(
                "INSERT INTO segments VALUES (?, ?, ?, 1, ?, ?)",
                (
                    alt_segment,
                    episode_id,
                    alt_transcript,
                    hashes[1],
                    str((self.root / "corpus" / "segments" / f"seg_{episode_number}_1.txt").relative_to(self.root)),
                ),
            )
            alternate_output = json.dumps({"discourse_events": [{} for _ in range(3)]})
            alternate_path = self.root / "runs" / "outputs" / f"run_{episode_number}_alternate.json"
            alternate_path.write_text(alternate_output, encoding="utf-8")
            self.conn.execute(
                "INSERT INTO labels VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, '2026-07-11')",
                (f"label_{episode_number}_alternate", alt_segment, alternate_output, str(alternate_path)),
            )
            self.conn.execute(
                "INSERT INTO label_runs VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, '2026-07-11', '2026-07-11')",
                (f"run_{episode_number}_alternate", alt_segment, str(alternate_path)),
            )
        self.conn.commit()
        return hashes

    def export(self, output_name: str = "manifest-v2.json"):
        return export_app_server_development_manifest_v2(
            self.conn,
            output_path=self.root / output_name,
            episode_ids=["ep_1", "ep_2"],
            exclusion_manifest_paths=[self.exclusion_path],
            segments_per_episode=8,
            minimum_no_signal_per_episode=1,
            maximum_no_signal_per_episode=1,
            minimum_dense_per_episode=7,
            dense_event_min=16,
            event_cap=32,
        )

    def test_v2_binds_context_transcript_and_records_reference_noise_without_duplicate_cases(self) -> None:
        self.seed_episode(1, alternate_disagreement=True)
        self.seed_episode(2)
        report = self.export()
        manifest = json.loads((self.root / "manifest-v2.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], APP_SERVER_DEVELOPMENT_MANIFEST_V2)
        self.assertEqual(report["segment_count"], 16)
        self.assertEqual(report["unique_text_sha256_count"], 16)
        self.assertEqual(report["source_count"], 2)
        self.assertEqual(manifest["episodes"][0]["canonical_transcript"]["transcript_id"], "tr_1")
        self.assertGreaterEqual(report["reference_noise_disagreements"], 1)
        noise = json.loads((self.root / "reference-noise-v1.json").read_text(encoding="utf-8"))
        self.assertEqual(noise["policy"], "duplicate_text_labels_are_reference_noise_not_additional_cases")

    def test_v2_uses_file_bytes_for_crlf_segment_hashes(self) -> None:
        self.seed_episode(1)
        self.seed_episode(2)
        path = self.root / "corpus" / "segments" / "seg_1_1.txt"
        payload = b"Unique episode 1 segment 1.\r\nSecond line.\r\n"
        path.write_bytes(payload)
        byte_hash = hashlib.sha256(payload).hexdigest()
        self.conn.execute(
            "UPDATE segments SET text_sha256 = ? WHERE id = 'seg_1_1'",
            (byte_hash,),
        )
        self.conn.commit()
        self.export()
        manifest = json.loads((self.root / "manifest-v2.json").read_text())
        segment = next(
            row
            for episode in manifest["episodes"]
            for row in episode["segments"]
            if row["segment_id"] == "seg_1_1"
        )
        self.assertEqual(segment["text_sha256"], byte_hash)
        prepared = _load_prepared_episodes(
            self.conn,
            manifest=manifest,
            window_count=4,
            context_chars=900,
        )
        self.assertEqual(len(prepared), 2)

    def test_v2_rejects_duplicate_hashes_inside_canonical_transcript(self) -> None:
        hashes = self.seed_episode(1)
        self.seed_episode(2)
        self.conn.execute(
            "UPDATE segments SET text_sha256 = ? WHERE id = 'seg_1_2'",
            (hashes[1],),
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "duplicate segment text hashes"):
            self.export()

    def test_v2_excludes_by_text_hash_and_refuses_cap_below_observed_max(self) -> None:
        hashes = self.seed_episode(1)
        self.seed_episode(2)
        self.exclusion_path.write_text(
            json.dumps({"segments": [{"text_sha256": hashes[1]}]}) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "lacks required dense cases"):
            self.export()
        self.exclusion_path.write_text('{"episodes":[]}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "below observed development maximum"):
            export_app_server_development_manifest_v2(
                self.conn,
                output_path=self.root / "cap.json",
                episode_ids=["ep_1", "ep_2"],
                exclusion_manifest_paths=[self.exclusion_path],
                segments_per_episode=8,
                minimum_no_signal_per_episode=1,
                maximum_no_signal_per_episode=1,
                minimum_dense_per_episode=7,
                dense_event_min=16,
                event_cap=20,
            )


if __name__ == "__main__":
    unittest.main()
