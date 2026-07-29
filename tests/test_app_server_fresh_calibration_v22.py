from __future__ import annotations

import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from research_factory import app_server_judge_v5_calibration_runner as calibration_runner
from research_factory import app_server_judge_v5_diagnostic as diagnostic
from research_factory import app_server_judge_v5_fresh_calibration as fresh
from research_factory.app_server_capacity_policy_v22 import (
    CALIBRATION_TURN_NAMES,
    CapacityPolicyV22Error,
    build_calibration_authorization_audit,
    build_capacity_policy,
)
from research_factory.app_server_judge_v5_fresh_calibration_v22 import (
    FreshCalibrationV22Error,
    REFERENCE_ROOT,
    _load,
    load_frozen_fixture_reference_v21,
    run_v22_calibration,
)
from research_factory.app_server_runtime_lock_v22 import (
    EXPECTED_DELTA_RUNTIME_FILES,
    _base_runtime_files,
    _discover_python_dependencies,
    _lineage_paths,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class FrozenReferenceV21Tests(unittest.TestCase):
    def test_real_v21_reference_is_admissible_and_fully_accounted(self):
        reference = load_frozen_fixture_reference_v21()
        self.assertEqual(reference["root"], REFERENCE_ROOT)
        self.assertEqual(len(reference["expected"]["cases"]), 66)
        self.assertEqual(reference["terminal"]["turn_count"], 12)
        self.assertEqual(reference["terminal"]["new_semantic_turn_count"], 11)
        self.assertEqual(reference["terminal"]["usage"]["total_tokens"], 382480)
        self.assertEqual(
            len(reference["terminal"]["completed_checkpoint_adoptions"]), 1
        )
        self.assertTrue(reference["terminal"]["reference_frozen"])
        self.assertTrue(reference["terminal"]["fresh_calibration_authorized"])
        self.assertFalse(reference["terminal"]["selection_authorized"])

    def test_authorization_audit_does_not_predeclare_selection(self):
        audit = build_calibration_authorization_audit()
        self.assertEqual(
            audit["status"],
            "fixture_reference_v2_frozen_fresh_calibration_authorized",
        )
        self.assertEqual(audit["calibration_turn_count_minimum"], 23)
        self.assertEqual(audit["calibration_turn_count_maximum"], 24)
        self.assertEqual(audit["ordered_turn_names"], list(CALIBRATION_TURN_NAMES))
        self.assertFalse(audit["selection_authorized_before_calibration"])
        self.assertFalse(audit["holdout_authorized"])
        self.assertFalse(audit["production_mutated"])

    def test_mutated_v21_terminal_authorization_is_rejected(self):
        original_load = _load

        def mutated(path: Path, purpose: str):
            value = original_load(path, purpose)
            if path.expanduser().resolve() == REFERENCE_ROOT / "terminal.json":
                value = deepcopy(value)
                value["selection_authorized"] = True
            return value

        with patch(
            "research_factory.app_server_judge_v5_fresh_calibration_v22._load",
            side_effect=mutated,
        ):
            with self.assertRaisesRegex(
                FreshCalibrationV22Error, "terminal is not admissible"
            ):
                load_frozen_fixture_reference_v21()


class CalibrationCapacityV22Tests(unittest.TestCase):
    def test_policy_freezes_twenty_four_turn_bound_and_new_empty_root(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            output = root / "calibration"
            audit, policy = build_capacity_policy(
                audit_path=root / "authorization.json",
                policy_path=root / "policy.json",
                output_root=output,
            )
            self.assertEqual(
                policy["phase_id"], "fresh_judge_v5_4_calibration_v22"
            )
            self.assertEqual(policy["ordered_turn_names"], list(CALIBRATION_TURN_NAMES))
            self.assertEqual(len(policy["ordered_turn_names"]), 24)
            self.assertEqual(policy["phase_total_token_bound"], 2448000)
            self.assertEqual(policy["projected_phase_quota_points"], 42)
            self.assertEqual(Path(policy["phase_output_root"]), output.resolve())
            self.assertEqual(
                Path(policy["semantic_output_root"]), output.resolve() / "fresh-attempt"
            )
            self.assertFalse(output.exists())
            self.assertFalse(
                policy["authorization"]["selection_authorized_before_calibration"]
            )
            self.assertFalse(policy["authorization"]["holdout_authorized"])
            self.assertEqual(audit["reference_usage"]["total_tokens"], 382480)

    def test_runtime_dependency_and_lineage_coverage_are_deterministic(self):
        allowed = _base_runtime_files(REPO_ROOT) | set(EXPECTED_DELTA_RUNTIME_FILES)
        self.assertTrue(_discover_python_dependencies(REPO_ROOT).issubset(allowed))
        first = _lineage_paths(REPO_ROOT)
        second = _lineage_paths(REPO_ROOT)
        self.assertEqual(first, second)
        self.assertEqual(len(first), len({label for label, _path in first}))
        self.assertEqual(len(first), len({path for _label, path in first}))

    def test_existing_semantic_root_blocks_policy_freeze(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            output = root / "calibration"
            output.mkdir()
            with self.assertRaisesRegex(
                CapacityPolicyV22Error, "root exists before policy freeze"
            ):
                build_capacity_policy(
                    audit_path=root / "authorization.json",
                    policy_path=root / "policy.json",
                    output_root=output,
                )


class CalibrationWrapperV22Tests(unittest.IsolatedAsyncioTestCase):
    async def test_wrapper_binds_policy_and_restores_shared_runtime(self):
        original_loader = fresh.load_frozen_fixture_reference
        original_fresh_writer = fresh._write_immutable_json
        original_runner_writer = calibration_runner._write_immutable_json
        original_validator = diagnostic._validate_capacity_checkpoint
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            output = root / "calibration"
            policy_path = root / "policy.json"
            build_capacity_policy(
                audit_path=root / "authorization.json",
                policy_path=policy_path,
                output_root=output,
            )

            async def fake_run(**kwargs):
                self.assertIsNot(
                    fresh.load_frozen_fixture_reference, original_loader
                )
                self.assertIsNot(fresh._write_immutable_json, original_fresh_writer)
                self.assertIsNot(
                    calibration_runner._write_immutable_json,
                    original_runner_writer,
                )
                self.assertIsNot(
                    diagnostic._validate_capacity_checkpoint, original_validator
                )
                self.assertEqual(Path(kwargs["reference_root"]), REFERENCE_ROOT)
                self.assertEqual(Path(kwargs["output_dir"]), output.resolve())
                return {
                    "state": "completed",
                    "terminal_reason": "synthetic_test",
                    "calibration_passed": False,
                    "selection_authorized": False,
                }

            with patch.object(
                fresh, "run_fresh_reference_calibration", side_effect=fake_run
            ):
                terminal = await run_v22_calibration(
                    policy_path=policy_path,
                    output_dir=output,
                    reference_root=REFERENCE_ROOT,
                )
        self.assertEqual(terminal["terminal_reason"], "synthetic_test")
        self.assertIs(fresh.load_frozen_fixture_reference, original_loader)
        self.assertIs(fresh._write_immutable_json, original_fresh_writer)
        self.assertIs(calibration_runner._write_immutable_json, original_runner_writer)
        self.assertIs(diagnostic._validate_capacity_checkpoint, original_validator)


if __name__ == "__main__":
    unittest.main()
