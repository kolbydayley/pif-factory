from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_runner as calibration_runner
from research_factory.app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from research_factory.app_server_judge_v5_calibration import (
    CALIBRATION_TRUTH_VERSION,
    make_v5_calibration_pool,
)
from research_factory.app_server_judge_v5_diagnostic import _record
from research_factory.app_server_judge_v5_fresh_calibration import (
    FreshCalibrationError,
    load_frozen_fixture_reference,
    run_fresh_reference_calibration,
)
from research_factory.app_server_judge_v5_reference_adjudication import (
    REFERENCE_ADJUDICATION_SPEC_VERSION,
    REFERENCE_ADJUDICATION_TERMINAL_VERSION,
    REFERENCE_RECEIPT_VERSION,
    REFERENCE_VERSION,
)
from research_factory.util import write_text_atomic


def _write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        path,
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _make_reference_root(root: Path, *, unresolved: bool = False) -> Path:
    _pool, _mapping, expected = make_v5_calibration_pool()
    reference = deepcopy(expected)
    reference.update(
        {
            "schema_version": REFERENCE_VERSION,
            "case_count": 66,
            "witness_count": 182,
            "pointwise_disputed_witness_count": 102,
            "alignment_disputed_case_count": 18,
            "source_provisional_truth_schema_version": expected["schema_version"],
        }
    )
    reference.pop("fixture_truth_audit_version", None)
    reference_path = root / "fixture-reference-v2.private.json"
    _write_json(reference_path, reference)
    unresolved_value = {
        "pointwise_abstain_witness_ids": ["witness_x"] if unresolved else [],
        "alignment_abstain_case_ids": [],
        "unsupported_without_specific_root_errors": [],
    }
    unresolved_path = root / "reference-unresolved.private.json"
    _write_json(unresolved_path, unresolved_value)
    manifest_path = root / "disagreement-manifest.private.json"
    _write_json(
        manifest_path,
        {
            "pointwise_disputed_witness_count": 102,
            "pointwise_disputed_case_count": 49,
            "alignment_disputed_case_count": 18,
            "candidate_labels_exposed_to_adjudicator": False,
            "model_identities_exposed_to_adjudicator": False,
            "majority_voting_allowed": False,
        },
    )
    change_path = root / "reference-change-summary.json"
    _write_json(change_path, {"changed_case_count": 49})
    spec_path = root / "reference-adjudication-spec.json"
    _write_json(
        spec_path,
        {
            "schema_version": REFERENCE_ADJUDICATION_SPEC_VERSION,
            "total_turn_count": 12,
            "retry_count_per_turn": 0,
            "managed_chatgpt_auth_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "reference_freeze_requires_zero_abstentions": True,
            "selection_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    attempts = []
    for index in range(12):
        turn_name = (
            f"reference_pointwise_shard_{index:02d}"
            if index < 9
            else f"reference_alignment_shard_{index - 9:02d}"
        )
        turn = root / "turns" / turn_name.replace("_", "-")
        capacity = turn / "capacity.json"
        output = turn / "output.private.json"
        sidecar = turn / "sidecar.json"
        _write_json(
            capacity,
            {
                "schema_version": CAPACITY_CHECKPOINT_VERSION,
                "primary_used_percent": 3,
                "primary_resets_at": None,
                "rate_limit_reached_type": None,
                "maximum_primary_used_percent": 20,
                "cleared_for_semantic_turn": True,
                "managed_chatgpt_auth_verified": True,
                "plan_type": "pro",
                "thread_started": False,
                "turn_started": False,
                "sidecar_started": False,
                "retry_checkpoint_reuse_allowed": False,
            },
        )
        _write_json(output, {"ok": True})
        _write_json(
            sidecar,
            {
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "usage_complete": True,
                "auth_type": "chatgpt",
                "transport": "stdio",
                "error_class": None,
                "recovery_reran_model": False,
                "usage": {
                    "input_tokens": 100,
                    "cached_input_tokens": 10,
                    "output_tokens": 20,
                    "reasoning_output_tokens": 5,
                    "total_tokens": 120,
                },
            },
        )
        attempts.append(
            {
                "turn_name": turn_name,
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "error_class": None,
                "capacity": _record(capacity),
                "output": _record(output),
                "sidecar": _record(sidecar),
            }
        )
    receipt_path = root / "fixture-reference-v2-receipt.json"
    _write_json(
        receipt_path,
        {
            "schema_version": REFERENCE_RECEIPT_VERSION,
            "status": "fixture_reference_v2_frozen",
            "reference": _record(reference_path),
            "disagreement_manifest": _record(manifest_path),
            "change_summary": _record(change_path),
            "unresolved": _record(unresolved_path),
            "reference_frozen": True,
            "fresh_calibration_authorized": True,
            "selection_authorized": False,
            "semantic_turn_count": 12,
            "semantic_retry_count": 0,
            "candidate_labels_exposed_to_adjudicator": False,
            "model_identities_exposed_to_adjudicator": False,
            "majority_voting_used": False,
            "usage": {
                "input_tokens": 1200,
                "cached_input_tokens": 120,
                "output_tokens": 240,
                "reasoning_output_tokens": 60,
                "total_tokens": 1440,
            },
            "usage_status": "complete",
            "production_mutation_performed": False,
        },
    )
    terminal_path = root / "terminal.json"
    _write_json(
        terminal_path,
        {
            "schema_version": REFERENCE_ADJUDICATION_TERMINAL_VERSION,
            "state": "completed",
            "terminal_reason": (
                "fixture_reference_v2_frozen_fresh_calibration_authorized"
            ),
            "reference_frozen": True,
            "fresh_calibration_authorized": True,
            "selection_authorized": False,
            "receipt": _record(receipt_path),
            "unresolved": _record(unresolved_path),
            "attempts": attempts,
            "semantic_retry_count": 0,
            "usage": {
                "input_tokens": 1200,
                "cached_input_tokens": 120,
                "output_tokens": 240,
                "reasoning_output_tokens": 60,
                "total_tokens": 1440,
            },
            "usage_status": "complete",
            "accounting_complete": True,
            "turn_count": 12,
            "production_mutated": False,
        },
    )
    return root


class FrozenReferenceContractTest(unittest.TestCase):
    def test_loads_hash_bound_zero_unresolved_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            root = _make_reference_root(Path(directory))
            loaded = load_frozen_fixture_reference(root)
        self.assertEqual(loaded["expected"]["schema_version"], CALIBRATION_TRUTH_VERSION)
        self.assertEqual(
            loaded["expected"]["fixture_reference_schema_version"], REFERENCE_VERSION
        )
        self.assertEqual(len(loaded["expected"]["cases"]), 66)
        self.assertEqual(len(loaded["terminal"]["attempts"]), 12)

    def test_rejects_unresolved_reference_even_when_receipts_are_consistent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = _make_reference_root(Path(directory), unresolved=True)
            with self.assertRaisesRegex(
                FreshCalibrationError, "has unresolved decisions"
            ):
                load_frozen_fixture_reference(root)

    def test_rejects_reference_mutation_after_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = _make_reference_root(Path(directory))
            path = root / "fixture-reference-v2.private.json"
            value = json.loads(path.read_text())
            value["case_count"] = 65
            _write_json(path, value)
            with self.assertRaisesRegex(FreshCalibrationError, "record drifted"):
                load_frozen_fixture_reference(root)


class FreshReferenceCalibrationAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_outer_attempt_binds_reference_and_restores_factory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            reference_root = _make_reference_root(base / "reference")
            output_root = base / "calibration"
            original_factory = calibration_runner.make_v5_calibration_pool
            calls = []

            async def fake_run(**kwargs):
                calls.append(kwargs)
                _pool, _mapping, expected = calibration_runner.make_v5_calibration_pool()
                self.assertEqual(expected["schema_version"], CALIBRATION_TRUTH_VERSION)
                inner_root = Path(kwargs["output_dir"])
                inner_terminal = inner_root / "terminal.json"
                _write_json(inner_root / "calibration-truth.private.json", expected)
                _write_json(inner_terminal, {"state": "completed"})
                return {
                    "state": "completed",
                    "calibration_passed": True,
                    "selection_authorized": True,
                    "terminal_reason": "judge_full_calibration_passed_selection_authorized",
                    "score": {"path": str(inner_root / "calibration-score.json")},
                    "attempts": [
                        {
                            "turn_name": f"turn_{index:02d}",
                            "state": "completed",
                            "status": "completed",
                            "usage_status": "measured",
                            "error_class": None,
                        }
                        for index in range(23)
                    ],
                    "semantic_retry_count": 0,
                    "usage": {"total_tokens": 1234},
                    "usage_status": "complete",
                    "accounting_complete": True,
                    "turn_count": 23,
                    "wall_elapsed_seconds_total": 10.0,
                }

            original_run = calibration_runner.run_v5_full_calibration
            calibration_runner.run_v5_full_calibration = fake_run
            try:
                terminal = await run_fresh_reference_calibration(
                    reference_root=reference_root,
                    output_dir=output_root,
                )
                second = await run_fresh_reference_calibration(
                    reference_root=reference_root,
                    output_dir=output_root,
                )
                spec = json.loads(
                    (output_root / "fresh-calibration-spec.json").read_text()
                )
            finally:
                calibration_runner.run_v5_full_calibration = original_run
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["selection_authorized"])
        self.assertEqual(second, terminal)
        self.assertEqual(len(calls), 1)
        self.assertIs(calibration_runner.make_v5_calibration_pool, original_factory)
        self.assertTrue(spec["reference_binding_verified_before_calls"])
        self.assertFalse(spec["reference_truth_exposed_to_model"])
        self.assertEqual(spec["retry_count_per_turn"], 0)
        self.assertNotIn("cases", spec)


if __name__ == "__main__":
    unittest.main()
