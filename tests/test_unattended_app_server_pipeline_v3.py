from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.unattended_app_server_pipeline import PipelineError
from research_factory.unattended_app_server_pipeline_v3 import (
    PIPELINE_V3_PHASE_VERSION,
    PIPELINE_V3_TERMINAL_VERSION,
    PIPELINE_V3_VERSION,
    build_v3_pipeline,
    default_v3_paths,
    default_v3_phases,
)


class PipelineV3ContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        if not (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v2/pipeline-terminal.json"
        ).is_file():
            raise unittest.SkipTest("immutable pipeline-v2 incident is unavailable")

    def test_phase_graph_reuses_prior_evidence_and_never_replays_failed_versions(self) -> None:
        names = [phase.name for phase in default_v3_phases()]
        self.assertEqual(names[0], "01_reuse_prior_evidence")
        self.assertEqual(names[1], "02_sharded_development_selection")
        self.assertNotIn("matrix", " ".join(names))
        self.assertNotIn("extraction", " ".join(names))
        self.assertNotIn("provenance", " ".join(names))
        self.assertNotIn("pipeline_v2", " ".join(names))

    def test_pipeline_uses_distinct_v3_state_marker_and_terminal_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = default_v3_paths(
                repo_root=self.repo,
                pipeline_root=Path(directory) / "unattended-pipeline-v3",
            )
            pipeline = build_v3_pipeline(paths)
            self.assertEqual(pipeline.state_schema_version, PIPELINE_V3_VERSION)
            self.assertEqual(
                pipeline.phase_marker_schema_version, PIPELINE_V3_PHASE_VERSION
            )
            self.assertEqual(pipeline.terminal_schema_version, PIPELINE_V3_TERMINAL_VERSION)

    def test_pipeline_v3_root_cannot_overlap_v1_or_v2(self) -> None:
        for version in ("v1", "v2"):
            source = (
                self.repo
                / "work/app-server-development-v2"
                / ("unattended-pipeline-%s" % version)
            )
            with self.assertRaisesRegex(PipelineError, "overlap"):
                default_v3_paths(repo_root=self.repo, pipeline_root=source)
            with self.assertRaisesRegex(PipelineError, "overlap"):
                default_v3_paths(repo_root=self.repo, pipeline_root=source / "nested")


if __name__ == "__main__":
    unittest.main()
