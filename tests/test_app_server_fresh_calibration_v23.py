from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import app_server_judge_v5_diagnostic as diagnostic
from research_factory import app_server_judge_v5_fresh_calibration as fresh
from research_factory import app_server_judge_v5_fresh_calibration_v22 as v22
from research_factory.app_server_capacity_policy_v23 import (
    build_capacity_policy,
    build_v22_presemantic_failure_audit,
)
from research_factory.app_server_judge_v5_fresh_calibration_v23 import (
    run_v23_calibration,
)
from research_factory.app_server_runtime_lock_v23 import (
    EXPECTED_DELTA_RUNTIME_FILES,
    _base_runtime_files,
    _discover_python_dependencies,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class PresemanticFailureV22Tests(unittest.TestCase):
    def test_v22_failure_is_reproduced_and_has_no_semantic_attempt(self):
        audit = build_v22_presemantic_failure_audit()
        self.assertEqual(audit["state"], "failed_before_semantic_attempt")
        self.assertEqual(
            audit["classification"],
            "infrastructure_or_local_protocol_attempt_failed",
        )
        self.assertEqual(audit["error_class"], "CapacityPolicyV21Error")
        self.assertTrue(audit["failure_reproduced_offline"])
        self.assertFalse(audit["semantic_attempt_started"])
        self.assertFalse(audit["thread_started"])
        self.assertFalse(audit["turn_started"])
        self.assertEqual(audit["capacity_checkpoint_count"], 0)
        self.assertEqual(audit["sidecar_count"], 0)
        self.assertEqual(audit["semantic_output_count"], 0)
        self.assertEqual(audit["usage_status"], "not_started")
        self.assertIsNone(audit["usage"])
        self.assertFalse(audit["retry_allowed_in_v22"])
        self.assertFalse(audit["production_mutated"])

    def test_v23_policy_preserves_bound_and_uses_new_root(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            output = root / "calibration-v23"
            audit, policy = build_capacity_policy(
                failure_audit_path=root / "failure.json",
                policy_path=root / "policy.json",
                output_root=output,
            )
            self.assertEqual(
                policy["phase_id"], "fresh_judge_v5_4_calibration_v23"
            )
            self.assertEqual(len(policy["ordered_turn_names"]), 24)
            self.assertEqual(policy["phase_total_token_bound"], 2448000)
            self.assertEqual(policy["projected_phase_quota_points"], 42)
            self.assertEqual(Path(policy["phase_output_root"]), output.resolve())
            self.assertFalse(output.exists())
            self.assertTrue(
                policy["recovery"][
                    "reference_validated_before_checkpoint_validator_installation"
                ]
            )
            self.assertFalse(policy["recovery"]["v22_retry_allowed"])
            self.assertFalse(
                policy["recovery"]["selection_authorized_before_calibration"]
            )
            self.assertEqual(audit["usage_status"], "not_started")

    def test_runtime_dependencies_are_covered_by_layered_lock(self):
        allowed = _base_runtime_files(REPO_ROOT) | set(EXPECTED_DELTA_RUNTIME_FILES)
        self.assertTrue(_discover_python_dependencies(REPO_ROOT).issubset(allowed))


class CalibrationWrapperV23Tests(unittest.IsolatedAsyncioTestCase):
    async def test_reference_validation_finishes_before_validator_install(self):
        original_validator = diagnostic._validate_capacity_checkpoint
        original_loader = v22.load_frozen_fixture_reference_v21
        calls = []
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            output = root / "calibration-v23"
            policy_path = root / "policy.json"
            build_capacity_policy(
                failure_audit_path=root / "failure.json",
                policy_path=policy_path,
                output_root=output,
            )

            def observed_loader(reference_root=v22.REFERENCE_ROOT):
                calls.append(diagnostic._validate_capacity_checkpoint)
                self.assertIs(
                    diagnostic._validate_capacity_checkpoint, original_validator
                )
                return original_loader(reference_root)

            async def fake_run(**_kwargs):
                self.assertIsNot(
                    diagnostic._validate_capacity_checkpoint, original_validator
                )
                cached = fresh.load_frozen_fixture_reference(v22.REFERENCE_ROOT)
                self.assertEqual(cached["terminal"]["usage"]["total_tokens"], 382480)
                return {
                    "state": "completed",
                    "terminal_reason": "synthetic_test",
                    "calibration_passed": False,
                    "selection_authorized": False,
                }

            with patch.object(
                v22, "load_frozen_fixture_reference_v21", side_effect=observed_loader
            ), patch.object(
                fresh, "run_fresh_reference_calibration", side_effect=fake_run
            ):
                terminal = await run_v23_calibration(
                    policy_path=policy_path,
                    output_dir=output,
                    reference_root=v22.REFERENCE_ROOT,
                )
        self.assertEqual(terminal["terminal_reason"], "synthetic_test")
        self.assertEqual(len(calls), 1)
        self.assertIs(diagnostic._validate_capacity_checkpoint, original_validator)


if __name__ == "__main__":
    unittest.main()
