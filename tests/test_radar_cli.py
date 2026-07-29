from __future__ import annotations

import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

from research_factory import radar_cli


def _context_service() -> tuple[MagicMock, MagicMock]:
    service = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = service
    context.__exit__.return_value = False
    return context, service


class ResearchRadarContractTest(unittest.TestCase):
    def test_frozen_bundle_and_five_exact_evidence_items_validate(self) -> None:
        receipt = radar_cli.validate_contract_bundle()

        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["contract_version"], "1")
        self.assertEqual(receipt["demo_items"], 5)
        self.assertFalse(receipt["scheduler_enabled"])
        self.assertFalse(receipt["legacy_queue_access"])
        self.assertEqual(len(receipt["bundle_sha256"]), 64)

    def test_cross_contract_scheduler_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "contracts"
            shutil.copytree(radar_cli.CONFIG_DIR, root)
            product_path = root / "product_contract_v1.json"
            product = json.loads(product_path.read_text(encoding="utf-8"))
            product["scheduler_enabled"] = True
            product_path.write_text(json.dumps(product), encoding="utf-8")

            with self.assertRaisesRegex(radar_cli.ContractError, "scheduler"):
                radar_cli.validate_contract_bundle(root)

    def test_release_lifecycle_is_frozen(self) -> None:
        schema = json.loads(
            (radar_cli.CONFIG_DIR / "schema_contract_v1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(schema["release_statuses"], radar_cli.EXPECTED_RELEASE_STATUSES)

    def test_fallback_db_is_outside_documents(self) -> None:
        with patch.object(radar_cli.Path, "home", return_value=Path("/Users/example")):
            path = radar_cli.fallback_radar_db_path()

        self.assertEqual(
            path,
            Path("/Users/example/Library/Application Support/Research Radar/research-radar.sqlite3"),
        )
        self.assertNotIn("Documents", path.parts)


class ResearchRadarCliTest(unittest.TestCase):
    def test_help_exposes_only_bounded_radar_commands(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            result = radar_cli.main([])

        self.assertEqual(result, 0)
        rendered = output.getvalue()
        for command in radar_cli.COMMANDS:
            self.assertIn(command, rendered)
        self.assertNotIn("evaluator", rendered)
        self.assertNotIn("legacy", rendered)

    def test_validate_contract_does_not_open_database(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output), patch.object(
            radar_cli, "_service", side_effect=AssertionError("database must remain closed")
        ):
            result = radar_cli.main(["validate-contract"])

        self.assertEqual(result, 0)
        self.assertTrue(json.loads(output.getvalue())["ok"])

    def test_status_of_missing_database_is_a_clean_noop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.sqlite3"
            output = io.StringIO()
            with redirect_stdout(output), patch.object(
                radar_cli, "_service", side_effect=AssertionError("missing status must not create DB")
            ):
                result = radar_cli.main(["--db", str(database), "status"])

        self.assertEqual(result, 0)
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["initialized"])
        self.assertFalse(payload["scheduler_enabled"])

    def test_init_closes_service_and_reports_contract_digest(self) -> None:
        context, service = _context_service()
        service.init.return_value = {"initialized": True}
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as directory, redirect_stdout(output), patch.object(
            radar_cli, "_service", return_value=context
        ) as factory:
            database = Path(directory) / "radar.sqlite3"
            result = radar_cli.main(["--db", str(database), "init"])

        self.assertEqual(result, 0)
        factory.assert_called_once_with(database.resolve())
        service.init.assert_called_once_with()
        context.__exit__.assert_called_once()
        payload = json.loads(output.getvalue())
        self.assertEqual(len(payload["contract_sha256"]), 64)
        self.assertFalse(payload["scheduler_enabled"])

    def test_seed_demo_passes_the_complete_frozen_fixture_once(self) -> None:
        context, service = _context_service()
        service.seed_demo.return_value = {"inserted_items": 5}
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as directory, redirect_stdout(output), patch.object(
            radar_cli, "_service", return_value=context
        ):
            database = Path(directory) / "radar.sqlite3"
            result = radar_cli.main(["--db", str(database), "seed-demo"])

        self.assertEqual(result, 0)
        service.init.assert_called_once_with()
        fixture = service.seed_demo.call_args.args[0]
        self.assertEqual(fixture["fixture_id"], "research-radar-five-item-demo-v1")
        self.assertEqual(len(fixture["items"]), 5)
        context.__exit__.assert_called_once()
        self.assertEqual(json.loads(output.getvalue())["fixture_id"], fixture["fixture_id"])

    def test_manual_cycle_is_capped_and_never_claims_model_execution(self) -> None:
        context, service = _context_service()
        service.run_cycle.return_value = {"processed": 3}
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "radar.sqlite3"
            database.touch()
            with redirect_stdout(output), patch.object(radar_cli, "_service", return_value=context):
                result = radar_cli.main(
                    ["--db", str(database), "run-cycle", "--max-items", "3"]
                )

        self.assertEqual(result, 0)
        service.run_cycle.assert_called_once_with(max_items=3)
        payload = json.loads(output.getvalue())
        self.assertTrue(payload["manual"])
        self.assertFalse(payload["model_transport_started"])
        self.assertFalse(payload["scheduler_enabled"])

    def test_manual_cycle_rejects_values_above_frozen_cap(self) -> None:
        error = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "radar.sqlite3"
            database.touch()
            with redirect_stderr(error), patch.object(
                radar_cli, "_service", side_effect=AssertionError("must not open service")
            ):
                result = radar_cli.main(
                    ["--db", str(database), "run-cycle", "--max-items", "26"]
                )

        self.assertEqual(result, 2)
        self.assertIn("max-items must be between 1 and 25", error.getvalue())

    def test_real_seed_is_idempotent_and_remains_provisional(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "radar.sqlite3"

            for _ in range(2):
                output = io.StringIO()
                with redirect_stdout(output):
                    result = radar_cli.main(["--db", str(database), "seed-demo"])
                self.assertEqual(result, 0)
                self.assertEqual(len(json.loads(output.getvalue())["result"]["items"]), 5)

            output = io.StringIO()
            with redirect_stdout(output):
                result = radar_cli.main(["--db", str(database), "status"])

        self.assertEqual(result, 0)
        status = json.loads(output.getvalue())["result"]
        self.assertEqual(status["counts"]["workspaces"], 1)
        self.assertEqual(status["counts"]["sources"], 5)
        self.assertEqual(status["counts"]["briefings"], 5)
        self.assertEqual(status["accepted_counts"]["briefings"], 0)
        self.assertFalse(status["scheduler_enabled"])
        self.assertFalse(status["legacy_database_attached"])


if __name__ == "__main__":
    unittest.main()
