from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_dev_selection import _blocked_result
from research_factory.app_server_v2_reuse import build_v2_reuse_contract
from research_factory.unattended_app_server_pipeline import PipelineError
from research_factory.unattended_app_server_pipeline_v2 import (
    PIPELINE_V2_PHASE_VERSION,
    PIPELINE_V2_TERMINAL_VERSION,
    PIPELINE_V2_VERSION,
    ShardedDevelopmentSelectionAdapter,
    build_v2_pipeline,
    default_v2_paths,
    default_v2_phases,
)


class PipelineV2ContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        if not (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v1/pipeline-terminal.json"
        ).is_file():
            raise unittest.SkipTest("immutable local pipeline-v1 evidence is unavailable")

    def test_phase_graph_has_no_extraction_matrix_provenance_or_v1_replay_phase(self) -> None:
        names = [phase.name for phase in default_v2_phases()]
        self.assertEqual(
            names,
            [
                "01_reuse_v1_evidence",
                "02_sharded_development_selection",
                "03_holdout_freeze",
                "04_holdout_reference_stratification",
                "05_holdout_execution",
                "06_holdout_shared_judge_gate",
                "07_prospective_epoch",
            ],
        )
        rendered = " ".join(names)
        self.assertNotIn("matrix", rendered)
        self.assertNotIn("extraction", rendered)
        self.assertNotIn("provenance", rendered)
        self.assertNotIn("failed_ab", rendered)

    def test_pipeline_uses_v2_state_marker_and_terminal_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = default_v2_paths(
                repo_root=self.repo,
                pipeline_root=Path(directory) / "unattended-pipeline-v2",
            )
            pipeline = build_v2_pipeline(paths)
            self.assertEqual(pipeline.state_schema_version, PIPELINE_V2_VERSION)
            self.assertEqual(
                pipeline.phase_marker_schema_version, PIPELINE_V2_PHASE_VERSION
            )
            self.assertEqual(pipeline.terminal_schema_version, PIPELINE_V2_TERMINAL_VERSION)

    def test_v2_root_cannot_overlap_pipeline_v1(self) -> None:
        v1 = self.repo / "work/app-server-development-v2/unattended-pipeline-v1"
        with self.assertRaisesRegex(PipelineError, "overlap"):
            default_v2_paths(repo_root=self.repo, pipeline_root=v1)
        with self.assertRaisesRegex(PipelineError, "overlap"):
            default_v2_paths(repo_root=self.repo, pipeline_root=v1 / "nested")

    def test_infrastructure_judge_failure_keeps_accurate_terminal_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "unattended-pipeline-v2"
            paths = default_v2_paths(repo_root=self.repo, pipeline_root=root)
            build_v2_reuse_contract(
                repo_root=self.repo,
                output_path=root / "reuse-contract-v1.json",
            )
            blocked = _blocked_result(
                reasons=["infrastructure_or_judge_attempt_failed"],
                stage="calibration_attempt",
                terminal_classification="infrastructure_or_judge_attempt_failed",
            )
            paths.frozen_winner.parent.mkdir(parents=True, exist_ok=True)
            paths.frozen_winner.write_text(
                json.dumps(blocked, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            context = SimpleNamespace(paths=paths)
            result = ShardedDevelopmentSelectionAdapter().run(context)
            self.assertEqual(result.status, "blocked")
            self.assertEqual(
                result.reason_code, "infrastructure_or_judge_attempt_failed"
            )
            self.assertNotEqual(
                result.reason_code, "development_quality_or_cost_gate_not_passed"
            )


if __name__ == "__main__":
    unittest.main()
