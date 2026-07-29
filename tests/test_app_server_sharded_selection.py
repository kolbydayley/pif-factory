from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_sharded_selection import (
    run_app_server_sharded_selection,
)
from research_factory.app_server_v2_reuse import build_v2_reuse_contract


class FailedTransport:
    async def __aenter__(self):
        raise RuntimeError("synthetic private transport detail")

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class ShardedSelectionTerminalTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        if not (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v1/pipeline-terminal.json"
        ).is_file():
            raise unittest.SkipTest("immutable local pipeline-v1 evidence is unavailable")

    async def test_transport_failure_is_not_mislabeled_as_quality_or_cost(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pipeline_root = Path(directory) / "unattended-pipeline-v2"
            contract_path = pipeline_root / "reuse-contract-v1.json"
            build_v2_reuse_contract(
                repo_root=self.repo,
                output_path=contract_path,
            )
            connection = sqlite3.connect(":memory:")
            try:
                result = await run_app_server_sharded_selection(
                    connection,
                    reuse_contract_path=contract_path,
                    output_dir=pipeline_root / "selection",
                    client_factory=FailedTransport,
                )
            finally:
                connection.close()
            self.assertEqual(result["selection_status"], "blocked")
            self.assertEqual(
                result["terminal_classification"],
                "infrastructure_or_judge_attempt_failed",
            )
            self.assertEqual(result["blocked_stage"], "persistent_app_server_transport")
            self.assertNotIn(
                "development_quality_or_cost_gate_not_passed",
                result["blocked_reasons"],
            )
            self.assertNotIn("synthetic private transport detail", str(result))


if __name__ == "__main__":
    unittest.main()
