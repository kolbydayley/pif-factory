from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from research_factory import pif_cli


class CompactPifCliTest(unittest.TestCase):
    def test_help_exposes_only_production_commands(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(pif_cli.main([]), 0)
        rendered = output.getvalue()
        for command in pif_cli.PRODUCTION_COMMANDS:
            self.assertIn(command, rendered)
        self.assertNotIn("efficiency-windowed-holdout", rendered)
        self.assertNotIn("scale-batch-enqueue", rendered)

    def test_only_daily_run_is_forwarded(self) -> None:
        error = io.StringIO()
        with redirect_stderr(error):
            self.assertEqual(pif_cli.main(["run", "worker"]), 2)
        self.assertIn("only_bounded_daily_run", error.getvalue())

        with patch.object(pif_cli.legacy_cli, "main", return_value=0) as forwarded:
            self.assertEqual(pif_cli.main(["--db", "/tmp/factory.sqlite", "run", "daily"]), 0)
        forwarded.assert_called_once_with(
            ["--db", "/tmp/factory.sqlite", "run", "daily"]
        )

    def test_legacy_lab_execution_is_frozen(self) -> None:
        error = io.StringIO()
        with redirect_stderr(error), patch.object(
            pif_cli.legacy_cli, "main", side_effect=AssertionError("must not dispatch")
        ):
            result = pif_cli.main(
                ["lab", "--allow-write", "efficiency-windowed-holdout"]
            )
        self.assertEqual(result, 2)
        self.assertIn("frozen_evaluator_execution_blocked", error.getvalue())

    def test_true_north_lab_dispatches_with_authoritative_db(self) -> None:
        with patch("research_factory.true_north.main", return_value=0) as dispatched:
            result = pif_cli.main(
                [
                    "--db",
                    "/tmp/factory.sqlite",
                    "lab",
                    "true-north",
                    "build",
                    "--suite",
                    "ai-safety-v1",
                ]
            )
        self.assertEqual(result, 0)
        dispatched.assert_called_once_with(
            [
                "--source-db",
                "/tmp/factory.sqlite",
                "build",
                "--suite",
                "ai-safety-v1",
            ]
        )


if __name__ == "__main__":
    unittest.main()
