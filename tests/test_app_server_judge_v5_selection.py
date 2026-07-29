from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory import app_server_judge_v5_selection as selection
from research_factory.app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_neutral_alignment_input,
    build_pointwise_support_input,
    freeze_support_receipts,
)
from research_factory.app_server_llm_judge import SHARED_WITNESS_POOL_VERSION
from research_factory.util import sha256_text


REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_POOL = (
    REPO_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v1/"
    "development-selection/witness-pool/shared-witness-pool.private.json"
)


def _case_id(index: int) -> str:
    return "jcase_" + sha256_text(f"case-{index}")[:24]


def _witness_id(index: int) -> str:
    return "wit_" + sha256_text(f"witness-{index}")[:24]


def _small_pool(case_count: int = 32, *, witnesses_per_case: int = 1) -> dict:
    cases = []
    for case_index in range(case_count):
        source = f"Source statement {case_index} is exact."
        witnesses = []
        for witness_index in range(witnesses_per_case):
            global_index = case_index * max(1, witnesses_per_case) + witness_index
            witnesses.append(
                {
                    "witness_id": _witness_id(global_index),
                    "event": {
                        "claim_text": f"Source statement {case_index} is exact.",
                        "evidence": source,
                        "actor_name": f"Actor {witness_index}",
                    },
                }
            )
        cases.append(
            {
                "case_id": _case_id(case_index),
                "source_excerpt": source,
                "event_set_a": witnesses,
                "event_set_b": [],
            }
        )
    return {
        "schema_version": SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text("selection-test-seed"),
        "cases": cases,
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }


def _supported_receipts(pool: dict) -> dict:
    pointwise = build_pointwise_support_input(pool)
    rows = []
    for unit in pointwise["units"]:
        span = unit["source_excerpt"][:1000]
        rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "proposition_verdict": "supported",
                "proposition_evidence_spans": [span],
                "proposition_rationale": "The proposition is explicit.",
                "structured_field_verdict": "correct",
                "field_issue_fields": [],
                "field_evidence_spans": [span],
                "field_rationale": "The populated fields are licensed.",
            }
        )
    return freeze_support_receipts({"units": rows}, pointwise)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _fresh_calibration_fixture(
    root: Path, *, complete_checks: bool = True
) -> tuple[Path, Path]:
    calibration = root / "calibration"
    inner = calibration / "fresh-attempt"
    per_turn_usage = {
        "input_tokens": 100,
        "cached_input_tokens": 10,
        "output_tokens": 20,
        "reasoning_output_tokens": 5,
        "total_tokens": 120,
    }
    usage = {key: value * 23 for key, value in per_turn_usage.items()}
    checks = (
        {key: True for key in selection.CALIBRATION_CHECK_KEYS}
        if complete_checks
        else {}
    )
    _write_json(
        inner / "calibration-score.json",
        {
            "schema_version": selection.CALIBRATION_SCORE_VERSION,
            "passed": True,
            "gates": selection.CALIBRATION_GATES,
            "metrics": {},
            "checks": checks,
            "proposition_and_structured_field_scores_separate": True,
        },
    )
    _write_json(inner / "calibration-truth.private.json", {"truth": "bound"})
    _write_json(
        inner / "calibration-spec.json",
        {
            "schema_version": selection.CALIBRATION_RUN_VERSION,
            "execution_purpose": "calibration",
            "protocol_version": selection.PROTOCOL_VERSION,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "timeout_seconds": 1200.0,
            "case_count": 66,
            "witness_count": 182,
            "cases_per_shard": 6,
            "retry_count_per_turn": 0,
            "all_turns_fresh": True,
            "calibration_v1_output_reuse_allowed": False,
            "provisional_truth_exposed_to_model": False,
            "calibration_gates": selection.CALIBRATION_GATES,
            "production_mutation_allowed": False,
        },
    )
    _write_json(
        calibration / "fresh-calibration-spec.json",
        {
            "schema_version": selection.FRESH_CALIBRATION_SPEC_VERSION,
            "protocol_version": selection.PROTOCOL_VERSION,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "timeout_seconds": 1200.0,
            "case_count": 66,
            "witness_count": 182,
            "cases_per_shard": 6,
            "retry_count_per_turn": 0,
            "all_semantic_turns_fresh": True,
            "prior_calibration_output_reuse_allowed": False,
            "reference_truth_exposed_to_model": False,
            "production_mutation_allowed": False,
        },
    )
    turn_names = [
        *(f"pointwise_support_shard_{index:02d}" for index in range(11)),
        *(f"neutral_alignment_shard_{index:02d}" for index in range(11)),
        "neutral_alignment_canary",
    ]
    attempts = []
    for turn_name in turn_names:
        turn_root = inner / "turns" / turn_name.replace("_", "-")
        capacity = turn_root / "capacity.json"
        input_path = turn_root / "input.private.json"
        prompt = turn_root / "prompt.private.md"
        schema = turn_root / "schema.json"
        output = turn_root / "output.private.json"
        sidecar = turn_root / "sidecar.json"
        _write_json(
            capacity,
            {
                "schema_version": selection.CAPACITY_CHECKPOINT_VERSION,
                "managed_chatgpt_auth_verified": True,
                "plan_type": "pro",
                "cleared_for_semantic_turn": True,
                "maximum_primary_used_percent": 20,
                "primary_used_percent": 3,
                "thread_started": False,
                "turn_started": False,
                "sidecar_started": False,
                "retry_checkpoint_reuse_allowed": False,
            },
        )
        _write_json(input_path, {"turn_name": turn_name})
        prompt.parent.mkdir(parents=True, exist_ok=True)
        prompt.write_text(f"Synthetic prompt for {turn_name}.\n", encoding="utf-8")
        schema_value = {"type": "object"}
        _write_json(schema, schema_value)
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
                "model": "gpt-5.6-sol",
                "effort": "high",
                "thread_mode": "new_thread",
                "error_class": None,
                "recovery_reran_model": False,
                "prompt_sha256": selection._sha256_file(prompt),
                "prompt_bytes": prompt.stat().st_size,
                "output_schema_sha256": sha256_text(
                    selection._canonical_json(schema_value)
                ),
                "output_schema_bytes": len(
                    selection._canonical_json(schema_value).encode("utf-8")
                ),
                "output_sha256": selection._sha256_file(output),
                "output_path": str(output.resolve()),
                "usage": per_turn_usage,
                "wall_elapsed_seconds": 1.0,
            },
        )
        attempts.append(
            {
                "turn_name": turn_name,
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "error_class": None,
                "capacity": selection._record(capacity),
                "output": selection._record(output),
                "sidecar": selection._record(sidecar),
            }
        )
    _write_json(
        inner / "terminal.json",
        {
            "schema_version": selection.CALIBRATION_TERMINAL_VERSION,
            "state": "completed",
            "calibration_passed": True,
            "selection_authorized": True,
            "usage": usage,
            "attempts": attempts,
            "turn_count": 23,
            "wall_elapsed_seconds_sum": 23.0,
        },
    )
    terminal_path = calibration / "terminal.json"
    _write_json(
        terminal_path,
        {
            "schema_version": selection.FRESH_CALIBRATION_TERMINAL_VERSION,
            "state": "completed",
            "terminal_reason": (
                "fresh_reference_calibration_passed_selection_authorized"
            ),
            "reference_binding_verified": True,
            "scorer_truth_binding_verified": True,
            "accounting_contract_verified": True,
            "calibration_passed": True,
            "selection_authorized": True,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "turn_count": 23,
            "production_mutated": False,
            "usage": usage,
            "wall_elapsed_seconds_sum": 23.0,
            "attempts": attempts,
            "inner_terminal": selection._record(inner / "terminal.json"),
            "inner_truth": selection._record(
                inner / "calibration-truth.private.json"
            ),
            "inner_score": selection._record(inner / "calibration-score.json"),
        },
    )
    continuation_path = root / "continuation-terminal.json"
    _write_json(
        continuation_path,
        {
            "schema_version": selection.CONTINUATION_TERMINAL_VERSION,
            "state": "completed",
            "terminal_reason": "fresh_calibration_passed_selection_ready",
            "calibration_passed": True,
            "selection_authorized": True,
            "production_mutated": False,
            "calibration_terminal": selection._record(terminal_path),
        },
    )
    return calibration, continuation_path


class SelectionShardPlanningTest(unittest.TestCase):
    def test_production_pool_is_exactly_covered_under_request_caps(self):
        pool = json.loads(PRODUCTION_POOL.read_text(encoding="utf-8"))
        pointwise = selection.plan_pointwise_selection_shards(pool)
        self.assertEqual(len(pool["cases"]), 32)
        self.assertEqual(len(pointwise), 28)
        self.assertEqual(sum(row["witness_count"] for row in pointwise), 1960)
        self.assertLessEqual(
            max(row["prompt_bytes"] for row in pointwise),
            selection.MAX_PROMPT_BYTES,
        )
        self.assertLessEqual(
            max(row["schema_bytes"] for row in pointwise),
            selection.MAX_OUTPUT_SCHEMA_BYTES,
        )
        self.assertEqual(
            sum(
                not case["event_set_a"] and not case["event_set_b"]
                for case in pool["cases"]
            ),
            4,
        )

        receipts = _supported_receipts(pool)
        case_ids = [case["case_id"] for case in pool["cases"]]
        base = selection.plan_alignment_selection_shards(
            pool=pool,
            support_receipts=receipts,
            case_ids=case_ids,
            permutation="base",
        )
        canary = selection.plan_alignment_selection_shards(
            pool=pool,
            support_receipts=receipts,
            case_ids=case_ids[:: selection.CANARY_STRIDE],
            permutation="balanced_canary",
        )
        self.assertEqual(
            [case_id for row in base for case_id in row["case_ids"]], case_ids
        )
        self.assertEqual(
            [case_id for row in canary for case_id in row["case_ids"]],
            case_ids[:: selection.CANARY_STRIDE],
        )
        self.assertTrue(
            all(
                1 <= len(row["case_ids"])
                <= selection.MAX_ALIGNMENT_CASES_PER_SHARD
                for row in base + canary
            )
        )
        for row in base + canary:
            self.assertLessEqual(row["prompt_bytes"], selection.MAX_PROMPT_BYTES)
            self.assertLessEqual(
                row["schema_bytes"], selection.MAX_OUTPUT_SCHEMA_BYTES
            )

    def test_duplicate_alignment_case_is_rejected(self):
        pool = _small_pool(2)
        receipts = _supported_receipts(pool)
        case_id = pool["cases"][0]["case_id"]
        with self.assertRaisesRegex(selection.V5SelectionError, "duplicated"):
            selection.plan_alignment_selection_shards(
                pool=pool,
                support_receipts=receipts,
                case_ids=[case_id, case_id],
                permutation="base",
            )


class FreshCalibrationSelectionGateTest(unittest.TestCase):
    def test_accepts_only_complete_hash_bound_frozen_gate_set(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            calibration, continuation = _fresh_calibration_fixture(Path(directory))
            loaded = selection.verify_fresh_calibration_for_selection(
                calibration_root=calibration,
                continuation_terminal_path=continuation,
            )
        self.assertTrue(loaded["terminal"]["selection_authorized"])
        self.assertEqual(
            set(loaded["score"]["checks"]), selection.CALIBRATION_CHECK_KEYS
        )

    def test_rejects_request_artifact_changed_after_measured_turn(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            calibration, continuation = _fresh_calibration_fixture(Path(directory))
            prompt = next((calibration / "fresh-attempt/turns").glob("*/prompt.private.md"))
            prompt.write_text(prompt.read_text(encoding="utf-8") + "changed\n")
            with self.assertRaisesRegex(
                selection.V5SelectionError, "sidecar contract drifted"
            ):
                selection.verify_fresh_calibration_for_selection(
                    calibration_root=calibration,
                    continuation_terminal_path=continuation,
                )

    def test_rejects_vacuously_passing_empty_check_map(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            calibration, continuation = _fresh_calibration_fixture(
                Path(directory), complete_checks=False
            )
            with self.assertRaisesRegex(
                selection.V5SelectionError, "does not pass every frozen gate"
            ):
                selection.verify_fresh_calibration_for_selection(
                    calibration_root=calibration,
                    continuation_terminal_path=continuation,
                )


class SelectionProjectionTest(unittest.TestCase):
    def test_no_signal_and_abstained_cases_project_without_invented_matches(self):
        pool = _small_pool(2)
        pool["cases"][0]["event_set_a"] = []
        receipts = _supported_receipts(pool)
        witness_id = pool["cases"][1]["event_set_a"][0]["witness_id"]
        reconciled = {
            "cases": [
                {
                    "case_id": pool["cases"][0]["case_id"],
                    "status": "accepted_base",
                    "equivalence_groups": [],
                    "alignment_pairs": [],
                    "unpaired_witness_ids": [],
                },
                {
                    "case_id": pool["cases"][1]["case_id"],
                    "status": "abstain",
                    "equivalence_groups": [],
                    "alignment_pairs": [],
                    "unpaired_witness_ids": [],
                },
            ]
        }
        consensus = selection.project_v5_selection_consensus(
            pool=pool,
            support_receipts=receipts,
            reconciled_alignment=reconciled,
        )
        first, second = consensus["cases"]
        self.assertEqual(first["status"], "completed")
        self.assertEqual(first["support_results"], [])
        self.assertEqual(first["equivalence_groups"], [])
        self.assertEqual(second["status"], "abstain")
        self.assertEqual(second["partition_abstained_witness_ids"], [witness_id])
        self.assertEqual(consensus["abstentions"]["cases"]["numerator"], 1)

    def test_relation_must_match_equivalence_partition(self):
        pool = _small_pool(1, witnesses_per_case=2)
        receipts = _supported_receipts(pool)
        alignment_input = build_neutral_alignment_input(pool, receipts)
        witness_ids = [
            witness["witness_id"]
            for witness in pool["cases"][0]["event_set_a"]
        ]
        checklist = [
            {
                "field": field,
                "decision": "same",
                "source_evidence_spans": [],
                "witness_evidence_ids": witness_ids,
                "rationale": "No material difference is asserted.",
            }
            for field in CHECKLIST_FIELDS
        ]
        output = {
            "cases": [
                {
                    "case_id": pool["cases"][0]["case_id"],
                    "equivalence_groups": [
                        {"witness_ids": [witness_ids[0]], "rationale": "First."},
                        {"witness_ids": [witness_ids[1]], "rationale": "Second."},
                    ],
                    "alignment_pairs": [
                        {
                            "witness_id_1": witness_ids[0],
                            "witness_id_2": witness_ids[1],
                            "relation": "equivalent",
                            "checklist": checklist,
                            "rationale": "Equivalent relation conflicts with groups.",
                        }
                    ],
                    "unpaired_witness_ids": [],
                }
            ]
        }
        self.assertEqual(
            selection.validate_selection_alignment_output(output, alignment_input),
            ["case_0_pair_0_relation_partition_conflict"],
        )


class _FailingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def run_ephemeral_structured_turn(self, **_kwargs):
        self.calls += 1
        raise RuntimeError("synthetic transport failure")


class SelectionFailureContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_failed_turn_is_immutable_unknown_usage_and_never_retried(self):
        pool = _small_pool()
        client = _FailingClient()
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory) / "selection-judge"
            with self.assertRaisesRegex(
                selection.V5SelectionError, "selection judge attempt failed"
            ):
                await selection.run_v5_selection_judge(
                    pool=pool,
                    output_dir=root,
                    model="gpt-5.6-sol",
                    reasoning_effort="high",
                    timeout_seconds=1200.0,
                    client_factory=lambda: client,
                )
            report = json.loads((root / "report.json").read_text())
            self.assertEqual(report["terminal_reason"], "infrastructure_or_judge_attempt_failed")
            self.assertEqual(report["usage_status"], "unknown")
            self.assertFalse(report["accounting_complete"])
            self.assertEqual(len(report["attempts"]), 1)
            self.assertEqual(len(report["unstarted_request_names"]), 31)
            self.assertEqual(client.calls, 1)

            adopted = await selection.run_v5_selection_judge(
                pool=pool,
                output_dir=root,
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
                client_factory=lambda: client,
            )
            self.assertEqual(adopted, report)
            self.assertEqual(client.calls, 1)

    async def test_failed_fresh_calibration_blocks_before_selection_client(self):
        client = _FailingClient()
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            calibration = root / "calibration"
            calibration.mkdir()
            (calibration / "terminal.json").write_text(
                json.dumps({"state": "failed"}) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                selection.V5SelectionError, "does not authorize selection"
            ):
                await selection.run_v5_five_arm_selection(
                    repo_root=REPO_ROOT,
                    fresh_calibration_root=calibration,
                    continuation_terminal_path=root / "missing-continuation.json",
                    output_dir=root / "selection",
                    client_factory=lambda: client,
                )
        self.assertEqual(client.calls, 0)


if __name__ == "__main__":
    unittest.main()
