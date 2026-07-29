from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research_factory import db
from research_factory import true_north


TS = "2026-07-27T12:00:00+00:00"


class TrueNorthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source_path = self.root / "source.sqlite"
        self.segment_path = self.root / "segment.txt"
        self.segment_text = "Host: The system is dangerous without independent evaluation."
        self.segment_path.write_text(self.segment_text, encoding="utf-8")
        self.conn = db.connect(self.source_path)
        db.init_db(self.conn)
        self._seed()
        self.conn.close()
        transcript_sha = "a" * 64
        self.spec = true_north.EpisodeSpec(
            "episode_1",
            "transcript_1",
            transcript_sha,
            "development",
            "Fixture Safety Show",
            "Who evaluates the evaluators?",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_opencode_runner_inlines_complete_packet_instead_of_file_attachment(self) -> None:
        packet = {
            "output_schema": {"type": "object"},
            "input": {
                "candidates": [],
                "long_value": "visible-packet-sentinel-" + ("x" * 5_000),
            },
        }
        packet_path = self.root / "packet.private.json"
        packet_payload = json.dumps(packet, separators=(",", ":")) + "\n"
        packet_path.write_text(packet_payload, encoding="utf-8")
        fake_home = self.root / "home"
        auth_path = fake_home / ".local" / "share" / "opencode" / "auth.json"
        auth_path.parent.mkdir(parents=True)
        auth_path.write_text("{}", encoding="utf-8")
        captured: dict[str, object] = {}

        def fake_run(command, **kwargs):
            captured["command"] = command
            stdout = "\n".join(
                [
                    json.dumps({"type": "text", "part": {"text": "{}"}}),
                    json.dumps(
                        {
                            "type": "step_finish",
                            "part": {
                                "reason": "stop",
                                "tokens": {
                                    "input": 1,
                                    "output": 1,
                                    "reasoning": 0,
                                    "total": 2,
                                    "cache": {"read": 0, "write": 0},
                                },
                            },
                        }
                    ),
                ]
            )
            return SimpleNamespace(
                stdout=stdout, stderr="", returncode=0
            )

        with (
            patch.object(true_north.Path, "home", return_value=fake_home),
            patch.object(true_north.subprocess, "run", side_effect=fake_run),
        ):
            output, _, _ = true_north._run_opencode_packet(
                packet_path=packet_path,
                output_dir=self.root / "output",
                models=("provider/model",),
                stage="inline-packet-test",
                timeout_seconds=10,
                opencode_binary="/fake/opencode",
                validator=lambda output, packet: None,
                _semantic_retry_remaining=0,
            )

        self.assertEqual(output, {})
        command = captured["command"]
        self.assertNotIn("--file", command)
        message = command[-1]
        self.assertIn("BEGIN_FROZEN_PACKET", message)
        self.assertIn("visible-packet-sentinel-", message)
        self.assertIn(packet_payload, message)

    def test_opencode_runner_retries_an_undecodable_checkpoint(self) -> None:
        packet_path = self.root / "packet.private.json"
        packet_path.write_text(
            json.dumps({"output_schema": {"type": "object"}, "input": {}}),
            encoding="utf-8",
        )
        fake_home = self.root / "home"
        auth_path = fake_home / ".local" / "share" / "opencode" / "auth.json"
        auth_path.parent.mkdir(parents=True)
        auth_path.write_text("{}", encoding="utf-8")
        output_dir = self.root / "retry-output"
        empty_stream = "\n".join(
            [
                json.dumps({"type": "step_start", "part": {}}),
                json.dumps(
                    {
                        "type": "step_finish",
                        "part": {
                            "reason": "length",
                            "tokens": {
                                "input": 1,
                                "output": 0,
                                "reasoning": 2,
                                "total": 3,
                                "cache": {"read": 0, "write": 0},
                            },
                        },
                    }
                ),
            ]
        )
        valid_stream = "\n".join(
            [
                json.dumps({"type": "text", "part": {"text": "{}"}}),
                json.dumps(
                    {
                        "type": "step_finish",
                        "part": {
                            "reason": "stop",
                            "tokens": {
                                "input": 1,
                                "output": 1,
                                "reasoning": 0,
                                "total": 2,
                                "cache": {"read": 0, "write": 0},
                            },
                        },
                    }
                ),
            ]
        )
        with (
            patch.object(true_north.Path, "home", return_value=fake_home),
            patch.object(
                true_north.subprocess,
                "run",
                return_value=SimpleNamespace(
                    stdout=empty_stream, stderr="", returncode=0
                ),
            ),
        ):
            with self.assertRaises(json.JSONDecodeError):
                true_north._run_opencode_packet(
                    packet_path=packet_path,
                    output_dir=output_dir,
                    models=("provider/model",),
                    stage="retry-checkpoint-test",
                    timeout_seconds=10,
                    opencode_binary="/fake/opencode",
                    validator=lambda output, packet: None,
                    _semantic_retry_remaining=0,
                )
        with (
            patch.object(true_north.Path, "home", return_value=fake_home),
            patch.object(
                true_north.subprocess,
                "run",
                return_value=SimpleNamespace(
                    stdout=valid_stream, stderr="", returncode=0
                ),
            ) as rerun,
        ):
            output, receipts, _ = true_north._run_opencode_packet(
                packet_path=packet_path,
                output_dir=output_dir,
                models=("provider/model",),
                stage="retry-checkpoint-test",
                timeout_seconds=10,
                opencode_binary="/fake/opencode",
                validator=lambda output, packet: None,
                _semantic_retry_remaining=0,
            )
        self.assertEqual(output, {})
        self.assertEqual(rerun.call_count, 1)
        self.assertFalse(any(r["usage"].get("checkpoint_reuse") for r in receipts))

    def _seed(self) -> None:
        evidence = "The system is dangerous without independent evaluation."
        start = self.segment_text.index(evidence)
        end = start + len(evidence)
        segment_sha = hashlib.sha256(self.segment_text.encode("utf-8")).hexdigest()
        self.conn.execute(
            """
            INSERT INTO sources
              (id, name, homepage_url, created_at, updated_at)
            VALUES ('source_1', 'Fixture Safety Show', 'https://example.test', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, published_at, created_at, updated_at)
            VALUES ('episode_1', 'source_1', 'guid-1',
                    'Who evaluates the evaluators?', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
               status, word_count, created_at, updated_at)
            VALUES ('transcript_1', 'episode_1', 'official',
                    '/private/transcript.txt', ?, 'ready', 8, ?, ?)
            """,
            ("a" * 64, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index,
               start_char, end_char, text_path, text_sha256, word_count, created_at)
            VALUES ('segment_1', 'transcript_1', 'episode_1', 'source_1', 0,
                    0, ?, ?, ?, 8, ?)
            """,
            (len(self.segment_text), str(self.segment_path), segment_sha, TS),
        )
        self.conn.execute(
            """
            INSERT INTO labels
              (id, segment_id, label_pack, label_pack_version, model, status,
               output_json, confidence, needs_review, created_at)
            VALUES ('label_1', 'segment_1', 'ai_discourse_v3_1', '3.1',
                    'gpt-5.5', 'ready', ?, 0.95, 0, ?)
            """,
            (json.dumps({"schema_version": "ai_discourse_v3_1"}), TS),
        )
        self.conn.execute(
            """
            INSERT INTO label_runs
              (id, segment_id, label_pack, model, status, completed_at,
               created_at, updated_at)
            VALUES ('label_run_1', 'segment_1', 'ai_discourse_v3_1',
                    'gpt-5.5', 'completed', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, actor_name,
               stance, claim_text, claim_type, certainty, temporal_horizon,
               confidence, evidence_text, evidence_start, evidence_end, created_at)
            VALUES ('event_1', 'label_1', 'segment_1', 0, 'risk_signal', 'Host',
                    'warning', 'Independent evaluation is required.', 'claim',
                    'high', 'present', 0.95, ?, ?, ?, ?)
            """,
            (evidence, start, end, TS),
        )
        self.conn.execute(
            """
            INSERT INTO discourse_event_contexts
              (discourse_event_id, label_id, segment_id, label_pack,
               source_context_kind, speaker_name, speaker_role,
               speaker_confidence, metric_json, exclusion_flags_json,
               quality_flags_json, created_at)
            VALUES ('event_1', 'label_1', 'segment_1', 'ai_discourse_v3_1',
                    'direct_speech', 'Host', 'host', 0.99, '{}', '[]', '[]', ?)
            """,
            (TS,),
        )
        self.conn.execute(
            """
            INSERT INTO episode_context_runs
              (id, episode_id, transcript_id, label_pack, model, status,
               speaker_map_json, section_map_json, entity_seed_json,
               concept_seed_json, created_at, updated_at, completed_at)
            VALUES ('context_1', 'episode_1', 'transcript_1',
                    'ai_discourse_v3_1', 'gpt-5.5', 'completed',
                    ?, '[]', '{}', '[]', ?, ?, ?)
            """,
            (json.dumps([{"name": "Host", "role": "host"}]), TS, TS, TS),
        )
        self.conn.commit()

    def test_build_is_shadow_only_hash_bound_and_idempotent(self) -> None:
        output_root = self.root / "private"
        with patch.object(true_north, "EPISODES", (self.spec,)):
            first = true_north.build_suite(
                source_db=self.source_path,
                output_root=output_root,
            )
            second = true_north.build_suite(
                source_db=self.source_path,
                output_root=output_root,
            )
            verified = true_north.verify_suite(output_root=output_root)

        self.assertTrue(first["ok"])
        self.assertEqual(first["candidate_count"], 1)
        self.assertTrue(first["production_source_unchanged"])
        self.assertTrue(second["idempotent_replay"])
        self.assertTrue(verified["ok"], verified["errors"])
        source = db.connect(self.source_path)
        try:
            self.assertEqual(
                source.execute("SELECT COUNT(*) FROM corpus_releases").fetchone()[0],
                0,
            )
            self.assertEqual(
                source.execute("SELECT COUNT(*) FROM atomic_claims").fetchone()[0],
                0,
            )
        finally:
            source.close()
        shadow = db.connect(Path(first["shadow_database"]))
        try:
            self.assertEqual(
                shadow.execute("SELECT COUNT(*) FROM corpus_releases").fetchone()[0],
                1,
            )
            self.assertEqual(
                shadow.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0],
                1,
            )
        finally:
            shadow.close()

    def test_exact_evidence_drift_fails_closed(self) -> None:
        conn = db.connect(self.source_path)
        conn.execute(
            "UPDATE discourse_events SET evidence_start = evidence_start + 1 WHERE id = 'event_1'"
        )
        conn.commit()
        conn.close()
        with patch.object(true_north, "EPISODES", (self.spec,)):
            with self.assertRaisesRegex(true_north.TrueNorthError, "exact-evidence"):
                true_north.build_suite(
                    source_db=self.source_path,
                    output_root=self.root / "bad-private",
                    include_source_db_hash=False,
                )

    def test_missing_episode_context_uses_candidate_only_shadow_fallback(self) -> None:
        conn = db.connect(self.source_path)
        conn.execute("DELETE FROM episode_context_runs")
        conn.commit()
        conn.close()
        output_root = self.root / "context-fallback"
        with patch.object(true_north, "EPISODES", (self.spec,)):
            result = true_north.build_suite(
                source_db=self.source_path,
                output_root=output_root,
                include_source_db_hash=False,
            )
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(
            manifest["copied_rows"]["synthetic_episode_context_runs"], 1
        )
        bundle = json.loads(Path(manifest["bundles"][0]["bundle_path"]).read_text())
        self.assertIn(
            "Benchmark-only context",
            bundle["episode_context"]["extraction_guidance"],
        )
        self.assertEqual(bundle["episode_context"]["model"], "gpt-5.5")

    def test_atomic_output_requires_complete_scope_and_frozen_evidence(self) -> None:
        candidate = {
            "candidate_id": "event_1",
            "evidence_text": "Exact evidence",
            "evidence_start": 4,
            "evidence_end": 18,
        }
        job = {
            "output_schema": true_north.atomic_output_schema(["event_1"]),
            "input": {"candidates": [candidate]},
        }
        output = {
            "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "event_1",
                    "disposition": "retain",
                    "reason_code": "supported_atomic_claim",
                    "atomic_claims": [
                        {
                            "claim_text": "A claim.",
                            "claim_type": "claim",
                            "raw_speaker": "Host",
                            "reported_actor": None,
                            "stance": "supports",
                            "certainty": "high",
                            "time_horizon": "present",
                            "evidence_text": "Exact evidence",
                            "evidence_start": 4,
                            "evidence_end": 18,
                            "confidence": 0.9,
                            "subject_text": "Evaluation",
                            "subject_type": "safety_issue",
                            "domain": "ai_safety",
                            "proposition_text": "Independent evaluation is required.",
                            "polarity": "positive",
                            "position": "supports",
                        }
                    ],
                }
            ],
        }
        true_north._validate_atomic_output(output, job)
        self.assertEqual(
            output["items"][0]["atomic_claims"][0]["evidence_text"],
            "Exact evidence",
        )
        output["items"][0]["atomic_claims"][0]["evidence_text"] = "Changed"
        true_north._validate_atomic_output(output, job)
        self.assertEqual(
            output["items"][0]["atomic_claims"][0]["evidence_text"],
            "Exact evidence",
        )

    def test_atomic_audit_jobs_are_small_complete_and_treat_prior_as_input(self) -> None:
        candidates = [
            {
                "candidate_id": f"event_{index}",
                "segment_index": index // 2,
                "event_index": index,
            }
            for index in range(7)
        ]
        bundle = {
            "bundle_sha256": "b" * 64,
            "episode": {"episode_id": "episode_1"},
            "episode_context": {"summary": "Fixture"},
            "candidates": candidates,
        }
        initial = {
            "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": row["candidate_id"],
                    "disposition": "reject",
                    "reason_code": "initial",
                    "atomic_claims": [],
                }
                for row in candidates
            ],
        }
        jobs = true_north._atomic_audit_jobs(
            [(bundle, initial, "openai/gpt-5.3-codex-spark")]
        )
        self.assertEqual([len(job["input"]["candidates"]) for _, job in jobs], [5, 2])
        self.assertEqual(
            {
                row["candidate_id"]
                for _, job in jobs
                for row in job["input"]["candidates"]
            },
            {row["candidate_id"] for row in candidates},
        )
        self.assertTrue(
            all("prior_proposals" in job["input"] for _, job in jobs)
        )

    def test_gold_reliability_exposes_unstable_reject_truth(self) -> None:
        base = self.root / "gold" / "development"
        job = base / "pass-a" / "jobs" / "segment.private.json"
        job.parent.mkdir(parents=True)
        job.write_text("{}", encoding="utf-8")
        a_output = base / "pass-a" / "outputs" / "segment"
        b_output = base / "pass-b" / "outputs" / "segment"
        a_output.mkdir(parents=True)
        b_output.mkdir(parents=True)
        common = {
            "reason_code": "fixture",
            "atomic_claims": [],
        }
        (a_output / "validated.private.json").write_text(
            json.dumps(
                {
                    "items": [
                        {"candidate_id": "c1", "disposition": "reject", **common},
                        {"candidate_id": "c2", "disposition": "reject", **common},
                    ]
                }
            ),
            encoding="utf-8",
        )
        (b_output / "validated.private.json").write_text(
            json.dumps(
                {
                    "items": [
                        {"candidate_id": "c1", "disposition": "reject", **common},
                        {"candidate_id": "c2", "disposition": "hold", **common},
                    ]
                }
            ),
            encoding="utf-8",
        )
        reliability = true_north._gold_interannotator_reliability(
            self.root,
            "development",
        )
        self.assertEqual(reliability["disposition_agreement"], 0.5)
        self.assertEqual(reliability["reject_jaccard"], 0.5)
        self.assertEqual(reliability["reject_intersection_count"], 1)
        self.assertFalse(reliability["pass_gate"])

    def test_gold_reliability_excludes_consensus_contested_items(self) -> None:
        base = self.root / "gold" / "development"
        job = base / "pass-a" / "jobs" / "segment.private.json"
        job.parent.mkdir(parents=True)
        job.write_text("{}", encoding="utf-8")
        common = {"reason_code": "fixture", "atomic_claims": []}
        for pass_name, rows in (
            (
                "pass-a",
                [
                    {"candidate_id": "stable", "disposition": "reject", **common},
                    {"candidate_id": "contested", "disposition": "reject", **common},
                ],
            ),
            (
                "pass-b",
                [
                    {"candidate_id": "stable", "disposition": "reject", **common},
                    {"candidate_id": "contested", "disposition": "hold", **common},
                ],
            ),
        ):
            output = base / pass_name / "outputs" / "segment"
            output.mkdir(parents=True)
            (output / "validated.private.json").write_text(
                json.dumps({"items": rows}),
                encoding="utf-8",
            )
        final = base / "final"
        final.mkdir(parents=True)
        (final / "consensus.private.json").write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "candidate_id": "stable",
                            "strictly_scoreable": True,
                        },
                        {
                            "candidate_id": "contested",
                            "strictly_scoreable": False,
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )

        reliability = true_north._gold_interannotator_reliability(
            self.root,
            "development",
        )
        self.assertEqual(reliability["raw_item_count"], 2)
        self.assertEqual(reliability["item_count"], 1)
        self.assertEqual(reliability["contested_excluded_count"], 1)
        self.assertEqual(reliability["raw_disposition_agreement"], 0.5)
        self.assertEqual(reliability["disposition_agreement"], 1.0)
        self.assertEqual(reliability["value_state_agreement"], 1.0)
        self.assertTrue(reliability["pass_gate"])

    def test_consensus_contract_collapses_retain_revise_and_preserves_ranges(self) -> None:
        consensus = {
            "items": [
                {
                    "candidate_id": "value",
                    "consensus_state": "consensus_value",
                    "strictly_scoreable": True,
                    "minimum_atomic_count": 1,
                    "maximum_atomic_count": 2,
                    "acceptable_value_states": ["value"],
                },
                {
                    "candidate_id": "junk",
                    "consensus_state": "consensus_junk",
                    "strictly_scoreable": True,
                    "minimum_atomic_count": 0,
                    "maximum_atomic_count": 0,
                    "acceptable_value_states": ["junk"],
                },
                {
                    "candidate_id": "contested",
                    "consensus_state": "contested",
                    "strictly_scoreable": False,
                    "minimum_atomic_count": 0,
                    "maximum_atomic_count": 1,
                    "acceptable_value_states": ["junk", "value"],
                },
            ]
        }
        predicted = {
            "value": {
                "disposition": "revise",
                "atomic_claims": [{}, {}],
            },
            "junk": {"disposition": "reject", "atomic_claims": []},
            "contested": {"disposition": "hold", "atomic_claims": []},
        }
        metrics = {
            row["metric"]: row
            for row in true_north._consensus_atomic_metrics(
                consensus,
                predicted,
            )
        }
        self.assertEqual(metrics["retained_value_recall"]["value"], 1.0)
        self.assertEqual(metrics["consensus_junk_escape_rate"]["value"], 0.0)
        self.assertEqual(metrics["acceptable_atomic_count_rate"]["value"], 1.0)
        self.assertEqual(metrics["contested_safe_handling_rate"]["value"], 1.0)

    def test_compile_consensus_gold_uses_independent_state_not_exact_label(self) -> None:
        base = self.root / "gold" / "development"
        job = base / "pass-a" / "jobs" / "segment.private.json"
        job.parent.mkdir(parents=True)
        job.write_text("{}", encoding="utf-8")
        for pass_name, disposition, atomic_count in (
            ("pass-a", "retain", 1),
            ("pass-b", "revise", 2),
        ):
            output = base / pass_name / "outputs" / "segment"
            output.mkdir(parents=True)
            item = {
                "candidate_id": "c1",
                "disposition": disposition,
                "reason_code": "fixture",
                "atomic_claims": [
                    {"claim_text": f"Claim {index}"}
                    for index in range(atomic_count)
                ],
            }
            (output / "validated.private.json").write_text(
                json.dumps({"items": [item]}),
                encoding="utf-8",
            )
        preferred = {
            "gold_sha256": "a" * 64,
            "items": [
                {
                    "candidate_id": "c1",
                    "episode_id": "episode_1",
                    "segment_id": "segment_1",
                    "disposition": "revise",
                    "reason_code": "fixture",
                    "atomic_claims": [
                        {"claim_text": "Claim 0"},
                        {"claim_text": "Claim 1"},
                    ],
                }
            ],
        }
        compiled = true_north._compile_consensus_gold(
            self.root,
            "development",
            preferred_gold=preferred,
        )
        document = json.loads(Path(compiled["consensus_path"]).read_text())
        item = document["items"][0]
        self.assertEqual(item["consensus_state"], "consensus_value")
        self.assertEqual(item["acceptable_atomic_counts"], [1, 2])
        self.assertEqual(item["minimum_atomic_count"], 1)
        self.assertEqual(item["maximum_atomic_count"], 2)
        self.assertFalse(item["stable_exact_disposition"])

    def test_provider_fallback_is_only_for_operational_failures(self) -> None:
        operational, reason = true_north._is_operational_failure(
            timed_out=False,
            exit_code=1,
            stderr="429 rate limit exceeded",
            stdout="",
        )
        self.assertTrue(operational)
        self.assertIsNotNone(reason)
        semantic, reason = true_north._is_operational_failure(
            timed_out=False,
            exit_code=1,
            stderr="schema validation failed",
            stdout="",
        )
        self.assertFalse(semantic)
        self.assertIsNone(reason)
        semantic_content, reason = true_north._is_operational_failure(
            timed_out=False,
            exit_code=0,
            stderr="",
            stdout='{"claim_text":"The model is currently unavailable."}',
        )
        self.assertFalse(semantic_content)
        self.assertIsNone(reason)

    def test_canonical_gold_requires_exact_scope_and_honest_resolution(self) -> None:
        packet = {
            "output_schema": true_north._canonical_gold_schema(["gac_1"]),
            "input": {"items": [{"gold_atomic_id": "gac_1"}]},
        }
        output = {
            "schema_version": "pif_true_north_canonical_gold_v1",
            "items": [
                {
                    "gold_atomic_id": "gac_1",
                    "subject_key": "independent_evaluation",
                    "subject_text": "Independent evaluation",
                    "subject_type": "safety_control",
                    "proposition_key": "evaluation_is_required",
                    "proposition_text": "Independent evaluation is required.",
                    "direct_speaker": "Host",
                    "speaker_resolution": "unresolved",
                    "canonical_person_name": None,
                    "reported_actor": None,
                    "rationale": "The evidence names only the role Host.",
                }
            ],
        }
        true_north._validate_canonical_gold(output, packet)
        output["items"][0]["canonical_person_name"] = "Invented Person"
        with self.assertRaisesRegex(true_north.TrueNorthError, "resolved speaker"):
            true_north._validate_canonical_gold(output, packet)

    def test_run_utility_requires_all_questions_and_supported_answers(self) -> None:
        schema = true_north._run_utility_schema(["claim_1"])
        questions = [
            {"question_id": question_id, "question": question}
            for question_id, question in true_north.UTILITY_QUESTIONS
        ]
        packet = {"output_schema": schema, "input": {"questions": questions}}
        output = {
            "schema_version": "pif_true_north_utility_output_v1",
            "items": [
                {
                    "question_id": row["question_id"],
                    "answerable": False,
                    "answer": "",
                    "support_atomic_claim_ids": [],
                }
                for row in questions
            ],
        }
        true_north._validate_run_utility(output, packet)
        output["items"][0]["answerable"] = True
        with self.assertRaisesRegex(true_north.TrueNorthError, "requires answer"):
            true_north._validate_run_utility(output, packet)

    def test_run_utility_supports_bounded_single_question_packets(self) -> None:
        schema = true_north._run_utility_schema(
            ["claim_1"], ["consensus_01"]
        )
        packet = {
            "output_schema": schema,
            "input": {
                "questions": [
                    {"question_id": "consensus_01", "question": "What recurs?"}
                ]
            },
        }
        output = {
            "schema_version": "pif_true_north_utility_output_v1",
            "items": [
                {
                    "question_id": "consensus_01",
                    "answerable": True,
                    "answer": "Independent evaluation recurs.",
                    "support_atomic_claim_ids": ["claim_1"],
                }
            ],
        }
        true_north._validate_run_utility(output, packet)
        output["items"][0]["support_atomic_claim_ids"].append("claim_1")
        with self.assertRaisesRegex(true_north.TrueNorthError, "duplicate support"):
            true_north._validate_run_utility(output, packet)

    def test_benchmark_identity_schema_excludes_provisional_candidate(self) -> None:
        job = true_north._semantic_job(
            {
                "target": "identities",
                "corpus_release_id": "release_1",
                "packet_sha256": "a" * 64,
            }
        )
        properties = job["output_schema"]["properties"]["decisions"]["properties"]
        self.assertNotIn(
            "candidate",
            properties["people"]["items"]["properties"]["decision"]["enum"],
        )
        self.assertNotIn(
            "candidate",
            properties["judgments"]["items"]["properties"]["decision"]["enum"],
        )

    def test_run_clone_installs_canonical_mapping_tables(self) -> None:
        destination = self.root / "run-shadow.sqlite"
        true_north._clone_shadow_database(self.source_path, destination)
        conn = db.connect(destination)
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        finally:
            conn.close()
        self.assertIn("true_north_subject_canonical_map", tables)
        self.assertIn("true_north_variant_canonical_map", tables)

    def test_canonical_map_validators_require_total_scope(self) -> None:
        subject_packet = {
            "output_schema": true_north._compact_mapping_schema(
                schema_version="pif_true_north_subject_map_v1",
                id_field="subject_id",
                ids=["subject_1"],
                value_field="canonical_subject_key",
            ),
            "input": {"items": [{"subject_id": "subject_1"}]},
        }
        output = {
            "schema_version": "pif_true_north_subject_map_v1",
            "items": [
                {
                    "subject_id": "subject_1",
                    "canonical_subject_key": "independent_evaluation",
                }
            ],
        }
        true_north._validate_subject_map_output(output, subject_packet)
        output["items"] = []
        with self.assertRaisesRegex(true_north.TrueNorthError, "exactly cover"):
            true_north._validate_subject_map_output(output, subject_packet)

    def test_holdout_requires_two_matching_development_passes(self) -> None:
        suite_root = self.root / true_north.SUITE_ID
        suite_root.mkdir()
        shadow = suite_root / "shadow.sqlite"
        shadow.touch()
        self.assertFalse(true_north._development_gate_passed(shadow))
        true_north._write_json(
            suite_root / "development-gate.json",
            {
                "passed": True,
                "consecutive_passes": 2,
                "configuration_sha256": "a" * 64,
            },
        )
        self.assertTrue(true_north._development_gate_passed(shadow))

    def test_prompt_optimization_changes_only_agent_system_prompt(self) -> None:
        baseline = true_north._opencode_config(
            true_north.PROMPT_OPTIMIZATION_MODEL,
            stage="prompt-optimization",
            system_prompt=true_north.SYSTEM_PROMPT_ARMS["baseline"],
        )
        variant = true_north._opencode_config(
            true_north.PROMPT_OPTIMIZATION_MODEL,
            stage="prompt-optimization",
            system_prompt=true_north.SYSTEM_PROMPT_ARMS["evidence-gate"],
        )
        agent_name = "pif-true-north-prompt-optimization"
        baseline_prompt = baseline["agent"][agent_name].pop("prompt")
        variant_prompt = variant["agent"][agent_name].pop("prompt")
        self.assertNotEqual(baseline_prompt, variant_prompt)
        self.assertEqual(baseline, variant)

    def test_prompt_optimization_arms_are_unique_and_fixed_model(self) -> None:
        prompts = list(true_north.SYSTEM_PROMPT_ARMS.values())
        self.assertEqual(len(prompts), len(set(prompts)))
        self.assertEqual(
            true_north.PROMPT_OPTIMIZATION_MODEL,
            "zai-coding-plan/glm-5.2",
        )
        self.assertNotIn("holdout", true_north.PROMPT_OPTIMIZATION_SCHEMA_VERSION)

    def _multipass_job(self) -> dict:
        candidate = {
            "candidate_id": "candidate_1",
            "segment_id": "segment_1",
            "segment_index": 0,
            "event_index": 0,
            "claim_text": "The system requires independent evaluation.",
            "evidence_text": "Host: The system requires independent evaluation.",
            "evidence_start": 0,
            "evidence_end": 49,
        }
        return {
            "schema_version": true_north.RUN_SCHEMA_VERSION,
            "suite_id": true_north.SUITE_ID,
            "output_schema": true_north.atomic_output_schema(["candidate_1"]),
            "input": {
                "episode": {"episode_id": "episode_1"},
                "episode_context": {
                    "speaker_map": [
                        {
                            "name": "Jane Doe",
                            "aliases": ["Jane"],
                            "role": "host",
                        },
                        {
                            "name": "Safety Lab",
                            "role": "reported actor",
                        },
                    ]
                },
                "segment": {
                    "segment_id": "segment_1",
                    "segment_index": 0,
                    "text": candidate["evidence_text"],
                },
                "candidates": [candidate],
            },
        }

    def test_multipass_disposition_schema_is_disposition_only(self) -> None:
        packet = true_north.build_multipass_disposition_packet(
            self._multipass_job()
        )
        valid = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "disposition": "reject",
                    "junk_reason": "question_or_setup",
                }
            ],
        }
        true_north.validate_multipass_disposition(valid, packet)
        invalid = json.loads(json.dumps(valid))
        invalid["items"][0]["claim_text"] = "not allowed"
        with self.assertRaises(Exception):
            true_north.validate_multipass_disposition(invalid, packet)
        invalid = json.loads(json.dumps(valid))
        invalid["items"][0]["junk_reason"] = None
        with self.assertRaisesRegex(true_north.TrueNorthError, "junk_reason"):
            true_north.validate_multipass_disposition(invalid, packet)

    def test_multipass_decomposition_routes_only_value_candidates(self) -> None:
        base = self._multipass_job()
        disposition = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "disposition": "revise",
                    "junk_reason": None,
                }
            ],
        }
        packet = true_north.build_multipass_decomposition_packet(
            base, disposition
        )
        self.assertIsNotNone(packet)
        self.assertEqual(
            [row["candidate_id"] for row in packet["input"]["candidates"]],
            ["candidate_1"],
        )
        self.assertNotIn("claim_text", packet["input"]["candidates"][0])
        valid = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "proposition_inventory": [
                        {
                            "index": 1,
                            "subject": "the system",
                            "predicate": "requires",
                            "object_or_outcome": "independent evaluation",
                        }
                    ],
                    "atomic_claims": [
                        {
                            "inventory_index": 1,
                            "claim_text": (
                                "The system requires independent evaluation."
                            ),
                            "certainty": "high",
                            "stance": "supportive",
                            "polarity": "positive",
                            "time_horizon": "present",
                        }
                    ],
                }
            ],
        }
        true_north.validate_multipass_decomposition(valid, packet)
        repaired = json.loads(json.dumps(valid))
        repaired["items"][0]["atomic_claims"][0]["polarity"] = "mixed"
        true_north.validate_multipass_decomposition(repaired, packet)
        self.assertEqual(
            repaired["items"][0]["atomic_claims"][0]["polarity"],
            "neutral",
        )
        invalid = json.loads(json.dumps(valid))
        invalid["items"][0]["atomic_claims"][0]["inventory_index"] = 2
        with self.assertRaisesRegex(
            true_north.TrueNorthError, "missing inventory"
        ):
            true_north.validate_multipass_decomposition(invalid, packet)
        invalid = json.loads(json.dumps(valid))
        invalid["items"][0]["atomic_claims"][0]["raw_speaker"] = "Jane"
        with self.assertRaises(Exception):
            true_north.validate_multipass_decomposition(invalid, packet)
        usage = true_north._multipass_usage(
            [
                {
                    "elapsed_seconds": 0,
                    "usage": {
                        "checkpoint_reuse": True,
                        "total_tokens": 100,
                    },
                }
            ]
        )
        self.assertEqual(usage["calls"], 0)
        self.assertEqual(usage["tokens"], 0)

    def test_multipass_attribution_is_closed_set_and_composes(self) -> None:
        base = self._multipass_job()
        disposition = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "disposition": "revise",
                    "junk_reason": None,
                }
            ],
        }
        decomposition = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "proposition_inventory": [
                        {
                            "index": 1,
                            "subject": "the system",
                            "predicate": "requires",
                            "object_or_outcome": "independent evaluation",
                        }
                    ],
                    "atomic_claims": [
                        {
                            "inventory_index": 1,
                            "claim_text": (
                                "The system requires independent evaluation."
                            ),
                            "certainty": "high",
                            "stance": "supportive",
                            "polarity": "positive",
                            "time_horizon": "present",
                        }
                    ],
                }
            ],
        }
        packet = true_north.build_multipass_attribution_packet(
            base, decomposition
        )
        self.assertIsNotNone(packet)
        jane_id = next(
            row["speaker_id"]
            for row in packet["input"]["speaker_roster"]
            if row["canonical_name"] == "Jane Doe"
        )
        output = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate_1",
                    "claim_index": 0,
                    "speaker_id": jane_id,
                    "attribution_mode": "direct",
                    "actor_presence": "absent",
                    "reported_actor_id": None,
                    "reported_actor_freetext": None,
                }
            ],
        }
        true_north.validate_multipass_attribution(output, packet)
        direct_with_actor = json.loads(json.dumps(output))
        direct_with_actor["items"][0]["actor_presence"] = "present"
        direct_with_actor["items"][0]["reported_actor_freetext"] = "Safety Lab"
        true_north.validate_multipass_attribution(direct_with_actor, packet)
        composed = true_north.compose_multipass_output(
            base, disposition, decomposition, output
        )
        atomic = composed["items"][0]["atomic_claims"][0]
        self.assertEqual(atomic["raw_speaker"], "Jane Doe")
        self.assertEqual(
            atomic["evidence_text"],
            base["input"]["candidates"][0]["evidence_text"],
        )
        invalid = json.loads(json.dumps(output))
        invalid["items"][0]["speaker_id"] = "free_text_speaker"
        with self.assertRaises(Exception):
            true_north.validate_multipass_attribution(invalid, packet)
        invalid = json.loads(json.dumps(output))
        invalid["items"][0].update(
            {
                "attribution_mode": "reported",
                "actor_presence": "present",
                "reported_actor_id": jane_id,
                "reported_actor_freetext": "Another actor",
            }
        )
        with self.assertRaisesRegex(
            true_north.TrueNorthError, "mutually exclusive"
        ):
            true_north.validate_multipass_attribution(invalid, packet)

    def _multipass_suite(self) -> Path:
        suite = self.root / true_north.SUITE_ID
        bundle_path = suite / "bundles" / "fixture.private.json"
        bundle_path.parent.mkdir(parents=True)
        base = self._multipass_job()
        bundle = {
            "episode": base["input"]["episode"],
            "episode_context": base["input"]["episode_context"],
            "segments": [base["input"]["segment"]],
            "candidates": base["input"]["candidates"],
        }
        bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
        manifest = {
            "manifest_sha256": "manifest-fixture",
            "bundles": [
                {
                    "episode_id": "ep_90c3b5c995bce501c9aef55c",
                    "partition": "development",
                    "bundle_path": str(bundle_path),
                }
            ],
        }
        (suite / "manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        return suite

    def test_multipass_runner_resumes_completed_stages_and_charges_calls(self) -> None:
        self._multipass_suite()
        calls: list[str] = []

        def fake_runner(**kwargs):
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            stage = packet["multipass_stage"]
            calls.append(stage)
            if stage == "disposition":
                output = {
                    "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "candidate_1",
                            "disposition": "retain",
                            "junk_reason": None,
                        }
                    ],
                }
            elif stage == "decomposition":
                output = {
                    "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "candidate_1",
                            "proposition_inventory": [
                                {
                                    "index": 1,
                                    "subject": "the system",
                                    "predicate": "requires",
                                    "object_or_outcome": "independent evaluation",
                                }
                            ],
                            "atomic_claims": [
                                {
                                    "inventory_index": 1,
                                    "claim_text": (
                                        "The system requires independent evaluation."
                                    ),
                                    "certainty": "high",
                                    "stance": "supportive",
                                    "polarity": "positive",
                                    "time_horizon": "present",
                                }
                            ],
                        }
                    ],
                }
            else:
                roster = packet["input"]["speaker_roster"]
                speaker_id = next(
                    row["speaker_id"]
                    for row in roster
                    if row["canonical_name"] == "Jane Doe"
                )
                output = {
                    "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "candidate_1",
                            "claim_index": 0,
                            "speaker_id": speaker_id,
                            "attribution_mode": "direct",
                            "actor_presence": "absent",
                            "reported_actor_id": None,
                            "reported_actor_freetext": None,
                        }
                    ],
                }
            return (
                output,
                [
                    {
                        "elapsed_seconds": 1.0,
                        "usage": {"total_tokens": 100},
                    }
                ],
                true_north.MULTIPASS_MODEL,
            )

        first = true_north.run_multipass(
            output_root=self.root,
            episode_ids=["ep_90c3b5c995bce501c9aef55c"],
            run_id="fixture-run",
            runner=fake_runner,
            workers=1,
        )
        self.assertTrue(first["complete"])
        self.assertEqual(first["usage"]["calls"], 3)
        self.assertEqual(calls, list(true_north.MULTIPASS_STAGES))
        second = true_north.run_multipass(
            output_root=self.root,
            episode_ids=["ep_90c3b5c995bce501c9aef55c"],
            resume_run_id="fixture-run",
            runner=fake_runner,
            workers=1,
        )
        self.assertTrue(second["complete"])
        self.assertEqual(second["usage"]["calls"], 3)
        self.assertEqual(calls, list(true_north.MULTIPASS_STAGES))

    def test_multipass_budget_stops_before_unfunded_attribution(self) -> None:
        self._multipass_suite()
        stages: list[str] = []

        def fake_runner(**kwargs):
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            stage = packet["multipass_stage"]
            stages.append(stage)
            if stage == "disposition":
                output = {
                    "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "candidate_1",
                            "disposition": "retain",
                            "junk_reason": None,
                        }
                    ],
                }
            else:
                output = {
                    "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "candidate_1",
                            "proposition_inventory": [
                                {
                                    "index": 1,
                                    "subject": "system",
                                    "predicate": "requires",
                                    "object_or_outcome": "evaluation",
                                }
                            ],
                            "atomic_claims": [
                                {
                                    "inventory_index": 1,
                                    "claim_text": "The system requires evaluation.",
                                    "certainty": "high",
                                    "stance": "supportive",
                                    "polarity": "positive",
                                    "time_horizon": "present",
                                }
                            ],
                        }
                    ],
                }
            return (
                output,
                [{"elapsed_seconds": 1.0, "usage": {"total_tokens": 100}}],
                true_north.MULTIPASS_MODEL,
            )

        with self.assertRaisesRegex(
            true_north.TrueNorthError, "call budget"
        ):
            true_north.run_multipass(
                output_root=self.root,
                episode_ids=["ep_90c3b5c995bce501c9aef55c"],
                run_id="budget-run",
                runner=fake_runner,
                workers=1,
                budget={
                    "max_calls": 2,
                    "max_tokens": 100_000,
                    "max_wall_seconds": 100,
                },
            )
        self.assertEqual(stages, ["disposition", "decomposition"])

    def test_semantic_packets_require_total_accounting_and_bind_transport(self) -> None:
        packet = {
            "target": "claims",
            "corpus_release_id": "release_1",
            "packet_sha256": "a" * 64,
            "items": [{"item_id": "claim_1"}, {"item_id": "claim_2"}],
        }
        job = {
            "output_schema": {},
            "input": {"reconciliation_packet": packet},
        }
        output = {
            "target": "wrong",
            "corpus_release_id": "wrong",
            "packet_sha256": "0" * 64,
            "decisions": {
                "subjects": [{"subject_key": "subject_1"}],
                "variants": [
                    {"variant_key": "variant_1", "subject_key": "subject_1"}
                ],
                "positions": [
                    {
                        "atomic_claim_id": "claim_1",
                        "subject_key": "subject_1",
                        "variant_key": "variant_1",
                    }
                ],
            },
            "abstentions": [],
        }
        with patch.object(true_north, "_validate_schema"):
            with self.assertRaisesRegex(
                true_north.TrueNorthError, "unaccounted"
            ):
                true_north._validate_semantic_output(output, job)
            output["abstentions"] = [
                {"item_id": "claim_2", "reason": "genuinely unresolved"}
            ]
            true_north._validate_semantic_output(output, job)
        self.assertEqual(output["target"], "claims")
        self.assertEqual(output["corpus_release_id"], "release_1")
        self.assertEqual(output["packet_sha256"], "a" * 64)


def test_phase_c_disposition_prompt_contains_both_mirror_rules() -> None:
    prompt = " ".join(
        true_north.MULTIPASS_SYSTEM_PROMPTS["disposition"].split()
    )
    assert (
        "A question, fragment, or repeated construction is junk only when "
        "the evidence provides no recoverable asserted proposition"
    ) in prompt
    assert "candidate wording cannot create evidence" in prompt
    assert (
        "Treat an explicit interrogative hypothesis, risk, analogy, or "
        "uncertainty as a substantive stance"
    ) in prompt
    assert (
        "a term gloss, definition, or existence mention without a "
        "consequence, evaluation, forecast, or contested position as junk"
    ) in prompt


def test_phase_c_disposition_acceptance_requires_zero_junk_escapes() -> None:
    consensus = {
        "items": [
            {
                "candidate_id": "value",
                "strictly_scoreable": True,
                "consensus_state": "consensus_value",
            },
            {
                "candidate_id": "junk",
                "strictly_scoreable": True,
                "consensus_state": "consensus_junk",
            },
        ]
    }
    predictions = {
        "value": {"candidate_id": "value", "disposition": "retain"},
        "junk": {"candidate_id": "junk", "disposition": "retain"},
    }

    result = true_north._score_phase_c_dispositions(
        consensus, predictions
    )

    assert result["junk_escape_count"] == 1
    assert result["false_reject_count"] == 0
    assert result["acceptance"]["junk_escapes_zero"] is False
    assert result["passed"] is False


def test_phase_c_conflict_selection_is_gold_blind_and_bounded() -> None:
    first = {
        "stable_reject": {
            "candidate_id": "stable_reject",
            "disposition": "reject",
            "junk_reason": "fragment",
        },
        "risky_flip": {
            "candidate_id": "risky_flip",
            "disposition": "reject",
            "junk_reason": "question_or_setup",
        },
        "safe_flip": {
            "candidate_id": "safe_flip",
            "disposition": "reject",
            "junk_reason": "question_or_setup",
        },
    }
    second = {
        "stable_reject": {
            "candidate_id": "stable_reject",
            "disposition": "reject",
            "junk_reason": "fragment",
        },
        "risky_flip": {
            "candidate_id": "risky_flip",
            "disposition": "retain",
            "junk_reason": None,
        },
        "safe_flip": {
            "candidate_id": "safe_flip",
            "disposition": "retain",
            "junk_reason": None,
        },
    }
    candidates = {
        "stable_reject": {"flags": {"quality": []}},
        "risky_flip": {
            "flags": {"quality": ["question_frame_not_asserted_claim"]}
        },
        "safe_flip": {"flags": {"quality": []}},
    }

    selected = true_north.select_phase_c_disposition_conflicts(
        first, second, candidates
    )

    assert selected == ["risky_flip", "stable_reject"]


def test_combine_phase_c_disposition_votes_uses_spark_only_as_tiebreaker():
    first = {
        "agree": {"candidate_id": "agree", "disposition": "reject"},
        "split": {"candidate_id": "split", "disposition": "reject"},
    }
    second = {
        "agree": {"candidate_id": "agree", "disposition": "reject"},
        "split": {"candidate_id": "split", "disposition": "retain"},
    }
    escalated = {
        "agree": {"candidate_id": "agree", "disposition": "retain"},
        "split": {"candidate_id": "split", "disposition": "revise"},
    }

    combined = true_north.combine_phase_c_disposition_votes(
        first, second, escalated
    )

    assert combined["agree"]["disposition"] == "reject"
    assert combined["split"]["disposition"] == "revise"


if __name__ == "__main__":
    unittest.main()
