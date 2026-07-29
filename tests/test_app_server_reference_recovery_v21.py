from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import app_server_judge_v5_diagnostic as diagnostic
from research_factory import app_server_judge_v5_reference_adjudication as reference
from research_factory.app_server_capacity_policy_v21 import (
    DEFAULT_SOURCE_POLICY,
    DEFAULT_V20_ROOT,
    RECOVERY_AUDIT_VERSION,
    audit_v20_checkpoint_validator_failure,
    build_recovery_policy,
)
from research_factory.app_server_judge_v5_reference_adjudication_v21 import (
    ADOPTED_TURN_NAME,
    DEFAULT_V20_TURN_ROOT,
    _validate_and_load_adopted_turn,
    build_reserve_capacity_checkpoint_validator,
    run_v21_reference,
)
from research_factory.app_server_runtime_lock_v21 import (
    EXPECTED_DELTA_RUNTIME_FILES,
    RuntimeLockV21Error,
    _base_runtime_files,
    _discover_python_dependencies,
    _relative_record,
    _verify_delta_files,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
V20_TURN_ROOT = DEFAULT_V20_ROOT / "turns/reference-pointwise-shard-00"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ReferenceRecoveryV21Tests(unittest.TestCase):
    def test_v20_failure_reproduces_as_local_checkpoint_schema_mismatch(self):
        audit = audit_v20_checkpoint_validator_failure()
        self.assertEqual(audit["schema_version"], RECOVERY_AUDIT_VERSION)
        self.assertEqual(
            audit["classification"],
            "repairable_local_checkpoint_validator_schema_mismatch",
        )
        self.assertEqual(audit["completed_semantic_turn_count"], 1)
        self.assertEqual(audit["semantic_retry_count"], 0)
        self.assertEqual(audit["semantic_output_validator_error_count"], 0)
        self.assertEqual(
            audit["legacy_failure_reproduced"],
            "semantic turn capacity checkpoint is invalid",
        )
        self.assertTrue(audit["reserve_validator_accepts_completed_turn"])
        self.assertEqual(audit["usage"]["total_tokens"], 29268)
        self.assertFalse(audit["v20_replay_allowed"])
        self.assertFalse(audit["production_mutated"])
        self.assertEqual(
            _sha256_file(DEFAULT_V20_ROOT / "terminal.json"),
            "657da79567ed04657bf2c32b000f785e40d6f581ea8279490269db1cefbf2087",
        )

    def test_reserve_validator_accepts_v20_checkpoint_and_rejects_drift(self):
        validator = build_reserve_capacity_checkpoint_validator(DEFAULT_SOURCE_POLICY)
        checkpoint_path = V20_TURN_ROOT / "capacity.json"
        value = validator(checkpoint_path)
        self.assertTrue(value["cleared_for_semantic_turn"])
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            path = Path(directory) / "capacity.json"
            drifted = dict(value)
            drifted["policy_sha256"] = "0" * 64
            path.write_text(json.dumps(drifted), encoding="utf-8")
            with self.assertRaisesRegex(
                diagnostic.JudgeV5DiagnosticError, "contract is invalid"
            ):
                validator(path)
            malformed = dict(value)
            malformed["projected_remaining_tokens"] = None
            malformed["projected_remaining_quota_points"] = None
            path.write_text(json.dumps(malformed), encoding="utf-8")
            with self.assertRaisesRegex(
                diagnostic.JudgeV5DiagnosticError, "contract is invalid"
            ):
                validator(path)

    def test_recovery_policy_uses_new_root_without_replaying_v20(self):
        source_sha = _sha256_file(DEFAULT_SOURCE_POLICY)
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            semantic = root / "semantic-v21"
            audit_path = root / "recovery-audit.json"
            policy_path = root / "capacity-policy.json"
            audit, policy = build_recovery_policy(
                audit_path=audit_path,
                policy_path=policy_path,
                semantic_output_root=semantic,
            )
            self.assertEqual(policy["phase_id"], "fixture_reference_adjudication_v21")
            self.assertEqual(Path(policy["semantic_output_root"]), semantic.resolve())
            self.assertNotEqual(
                Path(policy["semantic_output_root"]), DEFAULT_V20_ROOT.resolve()
            )
            self.assertFalse(policy["recovery"]["v20_replay_allowed"])
            self.assertEqual(policy["recovery"]["semantic_attempt_count_for_v21"], 1)
            self.assertEqual(
                policy["recovery"]["adopted_completed_turn"]["turn_name"],
                ADOPTED_TURN_NAME,
            )
            self.assertTrue(
                policy["recovery"]["adopted_completed_turn"]["adoption_is_not_retry"]
            )
            self.assertNotIn(ADOPTED_TURN_NAME, policy["ordered_turn_names"])
            self.assertEqual(len(policy["ordered_turn_names"]), 11)
            self.assertEqual(policy["phase_total_token_bound"], 11 * 102000)
            self.assertEqual(policy["projected_phase_quota_points"], 20)
            self.assertEqual(audit["usage"]["total_tokens"], 29268)
            self.assertFalse(semantic.exists())
        self.assertEqual(_sha256_file(DEFAULT_SOURCE_POLICY), source_sha)

    def test_layered_runtime_delta_is_exact_and_covers_imports(self):
        base = _base_runtime_files(REPO_ROOT)
        dependencies = _discover_python_dependencies(REPO_ROOT)
        allowed = base | set(EXPECTED_DELTA_RUNTIME_FILES)
        self.assertTrue(dependencies.issubset(allowed))
        records = [
            _relative_record(REPO_ROOT, relative)
            for relative in EXPECTED_DELTA_RUNTIME_FILES
        ]
        verified = _verify_delta_files(REPO_ROOT, records)
        self.assertEqual(len(verified), len(EXPECTED_DELTA_RUNTIME_FILES))
        with self.assertRaisesRegex(RuntimeLockV21Error, "coverage is not exact"):
            _verify_delta_files(REPO_ROOT, records[:-1])


class ReferenceWrapperV21Tests(unittest.IsolatedAsyncioTestCase):
    async def test_wrapper_restores_shared_writer_and_checkpoint_validator(self):
        original_writer = reference._write_immutable_json
        original_get_or_run_turn = reference._get_or_run_turn
        original_validator = diagnostic._validate_capacity_checkpoint
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            semantic = root / "semantic-v21"
            policy_path = root / "capacity-policy.json"
            build_recovery_policy(
                audit_path=root / "recovery-audit.json",
                policy_path=policy_path,
                semantic_output_root=semantic,
            )

            async def fake_reference(**_kwargs):
                self.assertIsNot(reference._write_immutable_json, original_writer)
                self.assertIsNot(reference._get_or_run_turn, original_get_or_run_turn)
                self.assertIsNot(
                    diagnostic._validate_capacity_checkpoint, original_validator
                )
                return {"state": "failed", "terminal_reason": "synthetic_test"}

            with patch.object(
                reference,
                "run_reference_adjudication",
                side_effect=fake_reference,
            ):
                terminal = await run_v21_reference(
                    policy_path=policy_path, output_dir=semantic
                )
        self.assertEqual(terminal["terminal_reason"], "synthetic_test")
        self.assertIs(reference._write_immutable_json, original_writer)
        self.assertIs(reference._get_or_run_turn, original_get_or_run_turn)
        self.assertIs(diagnostic._validate_capacity_checkpoint, original_validator)

    async def test_completed_v20_turn_is_adopted_without_calling_client(self):
        source_paths = {
            "input": DEFAULT_V20_TURN_ROOT / "input.private.json",
            "prompt": DEFAULT_V20_TURN_ROOT / "prompt.private.md",
            "schema": DEFAULT_V20_TURN_ROOT / "schema.json",
        }
        prompt = source_paths["prompt"].read_text(encoding="utf-8")
        schema = json.loads(source_paths["schema"].read_text(encoding="utf-8"))
        pointwise_input = json.loads(source_paths["input"].read_text(encoding="utf-8"))
        output, sidecar = _validate_and_load_adopted_turn(
            current_paths=source_paths,
            prompt=prompt,
            schema=schema,
            base_instructions=reference.pointwise_support_base_instructions(),
            model="gpt-5.6-luna",
            reasoning_effort="high",
            output_validator=lambda value: reference.validate_pointwise_support_output(
                value, pointwise_input
            ),
        )
        self.assertIsInstance(output, dict)
        self.assertEqual(sidecar["state"], "completed")
        self.assertEqual(sidecar["usage"]["total_tokens"], 29268)


if __name__ == "__main__":
    unittest.main()
