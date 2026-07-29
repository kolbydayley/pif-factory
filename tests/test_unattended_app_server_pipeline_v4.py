from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.unattended_app_server_pipeline import PipelineError
from research_factory.unattended_app_server_pipeline_v4 import (
    PIPELINE_V4_PHASE_VERSION,
    PIPELINE_V4_TERMINAL_VERSION,
    PIPELINE_V4_VERSION,
    build_v4_pipeline,
    default_v4_paths,
    default_v4_phases,
)


class PipelineV4ContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        if not (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v3/pipeline-terminal.json"
        ).is_file():
            raise unittest.SkipTest("immutable pipeline-v3 overload evidence unavailable")

    def test_readiness_precedes_full_fresh_selection_and_holdout(self) -> None:
        names = [phase.name for phase in default_v4_phases()]
        self.assertEqual(names[0], "01_freeze_predecessor_evidence")
        self.assertEqual(names[1], "02_provider_readiness")
        self.assertEqual(names[2], "03_full_fresh_sharded_development_selection")
        self.assertEqual(names[3], "04_holdout_freeze")
        self.assertNotIn("extraction", " ".join(names))
        self.assertNotIn("matrix", " ".join(names))

    def test_pipeline_uses_distinct_v4_schemas_and_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = default_v4_paths(
                repo_root=self.repo,
                pipeline_root=Path(directory) / "unattended-pipeline-v4",
            )
            pipeline = build_v4_pipeline(paths)
            self.assertEqual(pipeline.state_schema_version, PIPELINE_V4_VERSION)
            self.assertEqual(
                pipeline.phase_marker_schema_version, PIPELINE_V4_PHASE_VERSION
            )
            self.assertEqual(
                pipeline.terminal_schema_version, PIPELINE_V4_TERMINAL_VERSION
            )
            self.assertEqual(
                paths.development_selection_root.name,
                "development-selection-sharded-v3",
            )

    def test_pipeline_v4_root_cannot_overlap_any_predecessor(self) -> None:
        for version in (1, 2, 3):
            source = (
                self.repo
                / "work/app-server-development-v2"
                / ("unattended-pipeline-v%d" % version)
            )
            with self.assertRaisesRegex(PipelineError, "overlap"):
                default_v4_paths(repo_root=self.repo, pipeline_root=source)
            with self.assertRaisesRegex(PipelineError, "overlap"):
                default_v4_paths(repo_root=self.repo, pipeline_root=source / "nested")


if __name__ == "__main__":
    unittest.main()
