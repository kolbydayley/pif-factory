from __future__ import annotations

from copy import deepcopy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_capacity_policy_v20 import (
    DEFAULT_POST_PHASE_SNAPSHOT,
    DEFAULT_PRIOR_V2_ROOT,
    DEFAULT_PRIOR_V3_ROOT,
    _interval_capacity_rates,
    _measured_fixture_rows,
)
from research_factory.app_server_judge_v5_reference_adjudication import (
    REFERENCE_ADJUDICATION_SPEC_VERSION,
)
from research_factory.app_server_judge_v5_reference_adjudication_v20 import (
    _bind_capacity_policy_to_reference_spec,
)
from research_factory.app_server_runtime_lock_v20 import (
    DEFAULT_AUDIT,
    DEFAULT_MANIFEST,
    DEFAULT_POLICY,
    EXPECTED_RUNTIME_FILES,
    PREDECESSOR_ARTIFACTS,
    RuntimeLockV20Error,
    _discover_runtime_python_dependencies,
    _relative_record,
    _validate_predecessor_states,
    _validate_policy_audit_link,
    _verify_measured_audit_records,
    _verify_record,
    _verify_runtime_file_records,
    verify_runtime_lock_v20,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SEMANTIC_PREDECESSOR_SOURCES = {
    "fixture_audit_v2_shared_witness_pool": (
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-truth-audit-gpt55-v2/shared-witness-pool.private.json"
    ),
    "fixture_audit_v2_pointwise_input": (
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-truth-audit-gpt55-v2/pointwise-input-full.private.json"
    ),
    "fixture_audit_v2_pointwise_output": (
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-truth-audit-gpt55-v2/pointwise-output-full.private.json"
    ),
    "fixture_audit_v2_provisional_truth": (
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-truth-audit-gpt55-v2/provisional-calibration-truth.private.json"
    ),
    "fixture_audit_v3_reconciled_alignment": (
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-truth-audit-gpt55-v3/reconciled-alignment.private.json"
    ),
}


def _write_waiting_predecessor_states(root: Path, *, v17_started: bool = False) -> None:
    v16 = root / "work/app-server-development-v2/unattended-control-v16"
    v17 = root / "work/app-server-development-v2/unattended-control-v17"
    v18 = root / "work/app-server-development-v2/unattended-control-v18"
    for path in (v16, v17, v18):
        path.mkdir(parents=True, exist_ok=True)
    (v16 / "waiting-intent-v16.json").write_text(
        json.dumps(
            {
                "semantic_attempt_started": False,
                "thread_started": False,
                "turn_started": False,
            }
        ),
        encoding="utf-8",
    )
    (v17 / "state.json").write_text(
        json.dumps(
            {
                "status": "waiting_for_fixture_reference_v2",
                "semantic_attempt_started": v17_started,
            }
        ),
        encoding="utf-8",
    )
    (v18 / "state.json").write_text(
        json.dumps(
            {
                "status": "waiting_for_fresh_calibration_selection_authorization",
                "semantic_attempt_started": False,
            }
        ),
        encoding="utf-8",
    )


class CapacityPolicyEvidenceTests(unittest.TestCase):
    def test_measured_fixture_evidence_reproduces_immutable_totals(self):
        rows, records = _measured_fixture_rows(
            v2_root=DEFAULT_PRIOR_V2_ROOT, v3_root=DEFAULT_PRIOR_V3_ROOT
        )
        self.assertEqual(len(rows), 24)
        self.assertEqual(len(records), 48)
        self.assertEqual(sum(row["total_tokens"] for row in rows), 854550)
        post = __import__("json").loads(
            DEFAULT_POST_PHASE_SNAPSHOT.read_text(encoding="utf-8")
        )
        intervals = _interval_capacity_rates(
            rows, post_used_percent=post["primary_used_percent"]
        )
        self.assertEqual(intervals[0]["from_used_percent"], 16)
        self.assertEqual(intervals[-1]["to_used_percent"], 21)
        self.assertGreater(
            max(row["quota_points_per_million_tokens"] for row in intervals),
            12,
        )

    def test_v20_spec_binding_replaces_legacy_ceiling_without_mutating_input(self):
        legacy = {
            "schema_version": REFERENCE_ADJUDICATION_SPEC_VERSION,
            "semantic_turn_maximum_primary_used_percent": 20,
        }
        summary = {
            "mode": "remaining_reserve_plus_projected_phase_bound",
            "minimum_remaining_reserve_percent": 20,
        }
        bound = _bind_capacity_policy_to_reference_spec(legacy, summary)
        self.assertEqual(legacy["semantic_turn_maximum_primary_used_percent"], 20)
        self.assertIsNone(bound["semantic_turn_maximum_primary_used_percent"])
        self.assertEqual(bound["semantic_capacity_policy"], summary)
        summary["minimum_remaining_reserve_percent"] = 0
        self.assertEqual(
            bound["semantic_capacity_policy"]["minimum_remaining_reserve_percent"],
            20,
        )


class RuntimeLockV20PredecessorTests(unittest.TestCase):
    def test_immutable_record_rejects_hash_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "record.json"
            path.write_text("{}", encoding="utf-8")
            record = {
                "path": "record.json",
                "sha256": "0" * 64,
                "size_bytes": path.stat().st_size,
            }
            with self.assertRaises(RuntimeLockV20Error):
                _verify_record(root, record)

    def test_predecessor_terminal_or_semantic_attempt_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_waiting_predecessor_states(root, v17_started=True)
            with self.assertRaises(RuntimeLockV20Error):
                _validate_predecessor_states(root)

    def test_forbidden_terminal_appearing_after_validation_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_waiting_predecessor_states(root)
            _validate_predecessor_states(root)
            terminal = (
                root
                / "work/app-server-development-v2/unattended-control-v17/terminal.json"
            )
            terminal.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeLockV20Error, "terminal appeared"):
                _validate_predecessor_states(root)

    def test_runtime_lock_verification_rechecks_predecessors_before_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = RuntimeLockV20Error("post-lock predecessor drift")
            with patch(
                "research_factory.app_server_runtime_lock_v20._validate_predecessor_states",
                side_effect=expected,
            ) as validator:
                with self.assertRaisesRegex(
                    RuntimeLockV20Error, "post-lock predecessor drift"
                ):
                    verify_runtime_lock_v20(
                        repo_root=root, manifest_path=root / DEFAULT_MANIFEST.name
                    )
            validator.assert_called_once_with(root.resolve())

    def test_runtime_file_coverage_must_match_exact_frozen_set(self):
        dependencies = _discover_runtime_python_dependencies(REPO_ROOT)
        self.assertIn("research_factory/db.py", dependencies)
        self.assertTrue(dependencies.issubset(set(EXPECTED_RUNTIME_FILES)))
        records = [
            _relative_record(REPO_ROOT, relative)
            for relative in EXPECTED_RUNTIME_FILES
        ]
        verified = _verify_runtime_file_records(REPO_ROOT, records)
        self.assertEqual(len(verified), len(EXPECTED_RUNTIME_FILES))
        with self.assertRaisesRegex(RuntimeLockV20Error, "coverage is not exact"):
            _verify_runtime_file_records(REPO_ROOT, records[:-1])
        with self.assertRaisesRegex(RuntimeLockV20Error, "coverage is not exact"):
            _verify_runtime_file_records(REPO_ROOT, [*records, records[-1]])

    def test_policy_and_audit_cross_link_rejects_independent_drift(self):
        policy = json.loads(DEFAULT_POLICY.read_text(encoding="utf-8"))
        audit = json.loads(DEFAULT_AUDIT.read_text(encoding="utf-8"))
        audit_relative = str(DEFAULT_AUDIT.relative_to(REPO_ROOT))
        audit_record = _relative_record(REPO_ROOT, audit_relative)
        _validate_policy_audit_link(
            root=REPO_ROOT,
            policy=policy,
            audit_record=audit_record,
            audit_path=DEFAULT_AUDIT,
            audit=audit,
        )
        drifted = dict(audit_record)
        drifted["sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeLockV20Error, "cross-link drifted"):
            _validate_policy_audit_link(
                root=REPO_ROOT,
                policy=policy,
                audit_record=drifted,
                audit_path=DEFAULT_AUDIT,
                audit=audit,
            )

    def test_all_forty_eight_measured_records_are_reverified(self):
        audit = json.loads(DEFAULT_AUDIT.read_text(encoding="utf-8"))
        _verify_measured_audit_records(REPO_ROOT, audit)
        drifted = deepcopy(audit)
        drifted["measured_evidence_records"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeLockV20Error, "record drifted"):
            _verify_measured_audit_records(REPO_ROOT, drifted)

    def test_each_semantic_predecessor_source_is_pinned_and_mutation_fails(self):
        declared = dict(PREDECESSOR_ARTIFACTS)
        for label, relative in SEMANTIC_PREDECESSOR_SOURCES.items():
            with self.subTest(label=label):
                self.assertEqual(declared.get(label), relative)
                record = _relative_record(REPO_ROOT, relative)
                _verify_record(REPO_ROOT, record)
                drifted = dict(record)
                drifted["sha256"] = "0" * 64
                with self.assertRaisesRegex(RuntimeLockV20Error, "record drifted"):
                    _verify_record(REPO_ROOT, drifted)

    def test_r2_pins_failed_first_lock_and_no_semantic_prelaunch_receipt(self):
        declared = dict(PREDECESSOR_ARTIFACTS)
        self.assertEqual(
            declared.get("runtime_lock_v20_prelaunch_drifted"),
            "work/app-server-development-v2/unattended-runtime-lock-v20.json",
        )
        old_lock_record = _relative_record(
            REPO_ROOT,
            "work/app-server-development-v2/unattended-runtime-lock-v20.json",
        )
        _verify_record(REPO_ROOT, old_lock_record)
        failure_relative = (
            "work/app-server-development-v2/unattended-control-v20/"
            "prelaunch-failure-v20.json"
        )
        self.assertEqual(declared.get("v20_prelaunch_failure"), failure_relative)
        _verify_record(REPO_ROOT, _relative_record(REPO_ROOT, failure_relative))
        failure = json.loads((REPO_ROOT / failure_relative).read_text(encoding="utf-8"))
        self.assertEqual(failure["failed_runtime_lock"], old_lock_record)
        self.assertFalse(failure["launch_receipt_created"])
        self.assertFalse(failure["semantic_attempt_started"])
        self.assertFalse(failure["thread_started"])
        self.assertFalse(failure["turn_started"])
        self.assertFalse(failure["production_mutated"])
