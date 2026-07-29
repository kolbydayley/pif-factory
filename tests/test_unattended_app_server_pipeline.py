from __future__ import annotations

import json
import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_evaluation import (
    APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
)
from research_factory.app_server_holdout import FROZEN_WINNER_VERSION
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.unattended_app_server_pipeline import (
    ArtifactRef,
    DevelopmentSelectionAdapter,
    HoldoutReferenceStratificationAdapter,
    MatrixAdapter,
    ProspectiveEpochAdapter,
    PhaseDefinition,
    PhaseResult,
    PipelineError,
    PipelineLockUnavailable,
    PipelineStopped,
    UnattendedAppServerPipeline,
    _LifetimeLock,
    _artifact_ref,
    default_paths,
)


class FakeAdapter:
    def __init__(self, root: Path, name: str, *, status: str = "succeeded", raises=False):
        self.root = root
        self.name = name
        self.status = status
        self.raises = raises
        self.calls = 0

    def run(self, context) -> PhaseResult:
        self.calls += 1
        if self.raises:
            raise RuntimeError("synthetic private failure text must not enter the journal")
        artifact = self.root / f"{self.name}.json"
        if not artifact.exists():
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_text(
                json.dumps({"schema_version": "fake_terminal_v1", "ok": True}) + "\n",
                encoding="utf-8",
            )
        return PhaseResult(
            status=self.status,
            artifacts=(_artifact_ref(artifact, expected_schema="fake_terminal_v1"),),
            reason_code=(f"{self.name}_reason" if self.status != "succeeded" else None),
        )


class UnattendedAppServerPipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name) / "repo"
        self.repo.mkdir()
        self.pipeline_root = self.repo / "work" / "pipeline"
        self.paths = default_paths(repo_root=self.repo, pipeline_root=self.pipeline_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _pipeline(self, phases):
        return UnattendedAppServerPipeline(
            paths=self.paths,
            phases=phases,
            heartbeat_seconds=0.05,
            subprocess_poll_seconds=0.01,
        )

    def test_lifetime_lock_rejects_second_pipeline(self) -> None:
        path = self.pipeline_root / "pipeline.lock"
        with _LifetimeLock(path):
            with self.assertRaises(PipelineLockUnavailable):
                with _LifetimeLock(path):
                    self.fail("second lock owner must not be admitted")

    def test_phases_run_in_order_and_success_markers_are_immutable(self) -> None:
        first = FakeAdapter(self.pipeline_root, "first")
        second = FakeAdapter(self.pipeline_root, "second")
        phases = [
            PhaseDefinition("01_first", first, False),
            PhaseDefinition("02_second", second, False),
        ]

        report = self._pipeline(phases).run()

        self.assertEqual(report["status"], "completed")
        self.assertEqual(first.calls, 1)
        self.assertEqual(second.calls, 1)
        self.assertTrue((self.pipeline_root / "phase-markers" / "01_first.json").is_file())
        self.assertTrue((self.pipeline_root / "phase-markers" / "02_second.json").is_file())
        state = json.loads((self.pipeline_root / "state.json").read_text(encoding="utf-8"))
        self.assertFalse(state["production_mutation_allowed"])
        self.assertFalse(state["api_key_billing_allowed"])

    def test_blocked_phase_prevents_every_later_phase(self) -> None:
        blocker = FakeAdapter(self.pipeline_root, "blocker", status="blocked")
        forbidden = FakeAdapter(self.pipeline_root, "forbidden")
        phases = [
            PhaseDefinition("01_blocker", blocker, False),
            PhaseDefinition("02_forbidden", forbidden, False),
        ]

        report = self._pipeline(phases).run()

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["terminal_phase"], "01_blocker")
        self.assertEqual(blocker.calls, 1)
        self.assertEqual(forbidden.calls, 0)
        self.assertFalse((self.pipeline_root / "phase-markers" / "02_forbidden.json").exists())

    def test_restart_adopts_terminal_markers_without_rerunning(self) -> None:
        first = FakeAdapter(self.pipeline_root, "first")
        blocker = FakeAdapter(self.pipeline_root, "blocker", status="blocked")
        phases = [
            PhaseDefinition("01_first", first, False),
            PhaseDefinition("02_blocker", blocker, False),
        ]
        initial = self._pipeline(phases).run()
        self.assertEqual(initial["status"], "blocked")

        replacement_first = FakeAdapter(self.pipeline_root, "replacement_first")
        replacement_blocker = FakeAdapter(self.pipeline_root, "replacement_blocker")
        restarted = self._pipeline(
            [
                PhaseDefinition("01_first", replacement_first, False),
                PhaseDefinition("02_blocker", replacement_blocker, False),
            ]
        ).run()

        self.assertEqual(restarted["status"], "blocked")
        self.assertEqual(replacement_first.calls, 0)
        self.assertEqual(replacement_blocker.calls, 0)

    def test_interrupted_nonresumable_phase_is_never_called_again(self) -> None:
        completed = FakeAdapter(self.pipeline_root, "completed")
        interrupted = FakeAdapter(self.pipeline_root, "interrupted", raises=True)
        phases = [
            PhaseDefinition("01_completed", completed, False),
            PhaseDefinition("02_nonresumable", interrupted, False),
        ]
        with self.assertRaises(RuntimeError):
            self._pipeline(phases).run()
        self.assertEqual(interrupted.calls, 1)

        replacement_completed = FakeAdapter(self.pipeline_root, "replacement_completed")
        replacement_interrupted = FakeAdapter(self.pipeline_root, "replacement_interrupted")
        report = self._pipeline(
            [
                PhaseDefinition("01_completed", replacement_completed, False),
                PhaseDefinition("02_nonresumable", replacement_interrupted, False),
            ]
        ).run()

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reason_code"], "interrupted_nonresumable_phase_requires_operator_audit")
        self.assertEqual(replacement_completed.calls, 0)
        self.assertEqual(replacement_interrupted.calls, 0)
        journal = (self.pipeline_root / "journal.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("synthetic private failure text", journal)

    def test_drifted_artifact_blocks_marker_adoption(self) -> None:
        adapter = FakeAdapter(self.pipeline_root, "artifact")
        phases = [PhaseDefinition("01_artifact", adapter, False)]
        self._pipeline(phases).run()
        artifact = self.pipeline_root / "artifact.json"
        artifact.write_text('{"schema_version":"fake_terminal_v1","ok":false}\n', encoding="utf-8")

        with self.assertRaisesRegex(PipelineError, "missing or drifted"):
            self._pipeline(
                [PhaseDefinition("01_artifact", FakeAdapter(self.pipeline_root, "new"), False)]
            ).run()

    def test_stop_sentinel_prevents_first_phase(self) -> None:
        adapter = FakeAdapter(self.pipeline_root, "never")
        self.pipeline_root.mkdir(parents=True)
        (self.pipeline_root / "STOP").write_text("stop\n", encoding="utf-8")
        pipeline = self._pipeline([PhaseDefinition("01_never", adapter, False)])

        with self.assertRaises(PipelineStopped):
            pipeline.run()

        self.assertEqual(adapter.calls, 0)

    def test_semantic_preflight_rejects_instruction_content_drift(self) -> None:
        instruction = self.repo / "AGENTS.md"
        instruction.write_text("frozen instruction\n", encoding="utf-8")
        content = instruction.read_bytes()
        paths = [str(instruction.resolve())]
        self.paths.run_spec.parent.mkdir(parents=True, exist_ok=True)
        self.paths.run_spec.write_text(
            json.dumps(
                {
                    "instruction_contract": {
                        "expected_path_set_sha256": hashlib.sha256(
                            json.dumps(
                                paths,
                                ensure_ascii=True,
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode()
                        ).hexdigest(),
                        "sources": [
                            {
                                "path": str(instruction),
                                "content_sha256": hashlib.sha256(content).hexdigest(),
                                "size_bytes": len(content),
                            }
                        ],
                    }
                }
            )
            + "\n",
            encoding="utf-8",
        )
        pipeline = self._pipeline([])
        pipeline._verify_instruction_contract()
        instruction.write_text("drifted\n", encoding="utf-8")
        with self.assertRaisesRegex(PipelineError, "content drift"):
            pipeline._verify_instruction_contract()

    def test_subprocess_output_is_logged_per_phase(self) -> None:
        class FakeProcess:
            pid = 4242

            def wait(self, timeout):
                return 0

        def popen(_command, **kwargs):
            kwargs["stdout"].write(b"aggregate stdout\n")
            kwargs["stderr"].write(b"sanitized stderr\n")
            return FakeProcess()

        pipeline = UnattendedAppServerPipeline(
            paths=self.paths,
            phases=[],
            popen_factory=popen,
        )
        pipeline.paths.pipeline_root.mkdir(parents=True, exist_ok=True)
        pipeline.state = {"active_phase": "03_development_selection"}
        pipeline._wait_for_semantic_capacity = lambda _module: None
        result = pipeline.run_module("research_factory.app_server_dev_selection", ["--help"])
        self.assertEqual(result, 0)
        log_root = self.pipeline_root / "subprocess-logs"
        self.assertIn(
            "aggregate stdout",
            (log_root / "03_development_selection.stdout.log").read_text(),
        )
        self.assertIn(
            "sanitized stderr",
            (log_root / "03_development_selection.stderr.log").read_text(),
        )

    def test_recovered_matrix_adapter_accepts_five_clean_plus_itt_mode(self) -> None:
        report_path = self.paths.matrix_root / "recovered-matrix-report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": "pif_app_server_recovered_development_matrix_v1",
                    "clean_arm_count": 5,
                    "five_clean_arm_selection_eligible": True,
                    "selection_eligible": True,
                    "clean_six_arm_matrix_achieved": False,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        class Context:
            paths = self.paths

            @staticmethod
            def check_stop():
                return None

        result = MatrixAdapter().run(Context())

        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.metrics["matrix_mode"], "five_clean_plus_terminal_interrupted")

    def test_development_selection_adapter_passes_only_five_clean_reports_and_itt(self) -> None:
        itt = (
            self.paths.matrix_root
            / "batch-5"
            / "same_thread"
            / "intent-to-treat-interruption-v1.json"
        )
        itt.parent.mkdir(parents=True, exist_ok=True)
        itt.write_text("{}\n", encoding="utf-8")
        captured = []

        class Context:
            paths = self.paths

            @staticmethod
            def run_module(module, arguments):
                captured.append((module, list(arguments)))
                output = self.paths.frozen_winner
                output.parent.mkdir(parents=True, exist_ok=True)
                config = {
                    "variant_id": "batch_3_new_thread",
                    "batch_size": 3,
                    "thread_mode": "new_thread",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "low",
                    "concurrency": 1,
                    "retry_count": 0,
                    "window_count": 4,
                    "context_chars": 900,
                    "max_events_per_segment": 32,
                    "output_schema_version": APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
                    "transport_client_version": APP_SERVER_CLIENT_VERSION,
                    "guideline_artifact_sha256": "1" * 64,
                    "core_instructions_sha256": "2" * 64,
                }
                config_sha256 = hashlib.sha256(
                    json.dumps(
                        config,
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                output.write_text(
                    json.dumps(
                        {
                            "schema_version": FROZEN_WINNER_VERSION,
                            "selection_status": "frozen_winner",
                            "winner_frozen": True,
                            "winner": {
                                "variant_id": config["variant_id"],
                                "batch_size": config["batch_size"],
                                "thread_mode": config["thread_mode"],
                                "model": config["model"],
                                "reasoning_effort": config["reasoning_effort"],
                                "config": config,
                                "config_sha256": config_sha256,
                                "report_sha256": "3" * 64,
                            },
                            "gates": {
                                "quality_noninferior": True,
                                "production_amortized_total_token_ratio_lte_0_28": True,
                            },
                            "frozen_artifact_hashes": {"fixture": "4" * 64},
                            "production_changed": False,
                            "holdout_preparation_authorized": True,
                            "holdout_model_calls_authorized": False,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return 0

        result = DevelopmentSelectionAdapter().run(Context())

        self.assertEqual(result.status, "succeeded")
        module, arguments = captured[0]
        self.assertEqual(module, "research_factory.app_server_dev_selection")
        self.assertEqual(arguments.count("--arm-report"), 5)
        self.assertIn("--interrupted-arm-provenance", arguments)
        self.assertNotIn(
            str(self.paths.matrix_root / "batch-5" / "same_thread" / "report.json"),
            arguments,
        )

    def test_development_selection_adapter_rejects_legacy_v1_winner(self) -> None:
        output = self.paths.frozen_winner
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(
                {
                    "schema_version": "pif_app_server_frozen_winner_v1",
                    "selection_status": "frozen_winner",
                    "production_changed": False,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        class Context:
            paths = self.paths

            @staticmethod
            def run_module(_module, _arguments):
                raise AssertionError("an existing legacy artifact must not be regenerated")

        with self.assertRaisesRegex(PipelineError, "invalid frozen-winner artifact"):
            DevelopmentSelectionAdapter().run(Context())

    def test_stratifier_adapter_uses_real_module_cli_and_exact_120_case_contract(self) -> None:
        captured = []

        class Context:
            paths = self.paths

            @staticmethod
            def run_module(module, arguments):
                captured.append((module, list(arguments)))
                output = (
                    self.paths.holdout_stratification_root / "stratified-selection.json"
                )
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_app_server_holdout_stratified_selection_v1",
                            "selection_status": "frozen_stratified_selection",
                            "selection_basis": "llm_reference_only_pre_candidate_pre_baseline",
                            "selection_frozen": True,
                            "candidate_outputs_observed": False,
                            "baseline_outputs_observed": False,
                            "adaptive_top_up": False,
                            "stratum_counts": {
                                "clean_no_signal_power:no_signal": 60,
                                "paired_quality:dense": 15,
                                "paired_quality:low": 15,
                                "paired_quality:medium": 15,
                                "paired_quality:no_signal": 15,
                            },
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return 0

        result = HoldoutReferenceStratificationAdapter().run(Context())

        self.assertEqual(result.status, "succeeded")
        module, arguments = captured[0]
        self.assertEqual(module, "research_factory.app_server_holdout_stratifier")
        self.assertIn("--execution-dir", arguments)
        self.assertNotIn("--selection-output", arguments)

    def test_missing_prospective_module_writes_truthful_waiting_status(self) -> None:
        covenant = self.paths.holdout_root / "covenant.json"
        covenant.parent.mkdir(parents=True, exist_ok=True)
        covenant.write_text(
            json.dumps(
                {
                    "schema_version": "pif_app_server_holdout_covenant_v1",
                    "prospective_epoch_watermarks": {
                        "acquisition": {
                            "timestamp": "2026-07-12T05:00:00+00:00",
                            "basis": "transcripts.fetched_at_else_created_at",
                            "ids_at_watermark": ["private_transcript"],
                            "ids_at_watermark_sha256": "a" * 64,
                        },
                        "publication": {
                            "timestamp": "2026-07-10T05:00:00+00:00",
                            "basis": "episodes.published_at",
                            "ids_at_watermark": ["private_episode"],
                            "ids_at_watermark_sha256": "b" * 64,
                        },
                        "prospective_eligibility": "strictly after both boundaries",
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        database = self.repo / "factory.sqlite3"
        connection = sqlite3.connect(database)
        connection.executescript(
            """
            CREATE TABLE episodes (id TEXT PRIMARY KEY, source_id TEXT, published_at TEXT);
            CREATE TABLE transcripts (
              id TEXT PRIMARY KEY, episode_id TEXT, fetched_at TEXT,
              created_at TEXT, status TEXT
            );
            CREATE TABLE segments (id TEXT PRIMARY KEY, transcript_id TEXT);
            INSERT INTO episodes VALUES ('episode_old', 'source_old', '2026-07-01T00:00:00+00:00');
            INSERT INTO transcripts VALUES (
              'transcript_old', 'episode_old', '2026-07-01T00:00:00+00:00',
              '2026-07-01T00:00:00+00:00', 'ready'
            );
            INSERT INTO segments VALUES ('segment_old', 'transcript_old');
            """
        )
        connection.commit()
        connection.close()

        class Context:
            paths = self.paths

        with patch("research_factory.paths.db_path", return_value=database):
            result = ProspectiveEpochAdapter().run(Context())

        self.assertEqual(result.status, "waiting_for_future_data")
        output = self.paths.prospective_root / "prospective-epoch-status.json"
        payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(payload["eligible_episode_count"], 0)
        self.assertFalse(payload["shadow_completed"])
        self.assertEqual(payload["model_calls_performed"], 0)
        self.assertNotIn(
            "ids_at_watermark", payload["frozen_watermarks"]["acquisition"]
        )


if __name__ == "__main__":
    unittest.main()
