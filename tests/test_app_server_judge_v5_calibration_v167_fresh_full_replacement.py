from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v166_capped_owner_reconciliation as v166
from research_factory import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v167_fresh_full_replacement import (
    TURN_NAMES,
    _apply_verifier,
    _pair_rows,
    _validate_v166,
    build_v167_inputs,
    freeze_v167,
)


def test_v167_validates_v166_reference_protocol_and_usage():
    source = _validate_v166()
    terminal = source["values"]["terminal"]
    assert terminal["state"] == "completed"
    assert terminal["fresh_full_replacement_calibration_authorized"] is True
    assert terminal["usage"]["total_tokens"] == 30423
    assert source["cumulative_usage"]["total_tokens"] == 3216474
    assert source["values"]["reference"]["reference_version"] == "v15_v165"


def test_v167_builds_fixed_fresh_turn_plan_from_corrected_truth():
    source = _validate_v166()
    data = build_v167_inputs(source)
    roles = [row["turn_role"] for row in data["static_turns"]]
    assert len(data["support_case_shards"]) == 10
    assert sorted(value for shard in data["support_case_shards"] for value in shard) == sorted(
        row["case_id"] for row in data["pool"]["cases"]
    )
    assert roles.count("support_primary") == 10
    assert roles.count("support_canary") == 1
    assert roles.count("field_singleton") == 28
    assert roles.count("field_repeat_batch") == 1
    repeat = next(row for row in data["static_turns"] if row["turn_role"] == "field_repeat_batch")
    assert repeat["value"]["task_count"] == 4
    assert len(data["alignment_case_shards"]) == 11
    assert data["truth"] == source["values"]["truth"]


def test_v167_pair_verifier_selects_only_primary_equivalences_and_patches_owner():
    alignment_input = {
        "cases": [
            {
                "case_id": "case_a",
                "source_excerpt": "source",
                "witnesses": [
                    {"witness_id": "w1", "support_receipt": {}},
                    {"witness_id": "w2", "support_receipt": {}},
                    {"witness_id": "w3", "support_receipt": {}},
                ],
            }
        ]
    }
    normalized = {
        "cases": [
            {
                "case_id": "case_a",
                "alignment_pairs": [
                    {"witness_ids": ["w1", "w2"], "relation": "equivalent", "mismatch_fields": [], "checklist_decisions": {field: "same" for field in CHECKLIST_FIELDS}},
                ],
                "equivalence_groups": [["w1", "w2"], ["w3"]],
                "unpaired_witness_ids": ["w3"],
            }
        ]
    }
    rows = _pair_rows(normalized=normalized, alignment_input=alignment_input, prefix="test")
    assert len(rows) == 1
    pair_case_id = rows[0]["pair_case_id"]
    verifier = {
        "cases": [
            {
                "case_id": pair_case_id,
                "alignment_pairs": [
                    {"witness_ids": ["w1", "w2"], "relation": "non_equivalent", "mismatch_fields": ["stance"], "checklist_decisions": {field: ("different" if field == "stance" else "same") for field in CHECKLIST_FIELDS}},
                ],
                "equivalence_groups": [["w1"], ["w2"]],
                "unpaired_witness_ids": [],
            }
        ]
    }
    patched = _apply_verifier(normalized=normalized, pair_rows=rows, verifier=verifier)
    case = patched["cases"][0]
    assert case["alignment_pairs"][0]["relation"] == "non_equivalent"
    assert case["equivalence_groups"] == [["w1"], ["w2"], ["w3"]]


def _perfect_score_inputs(data: dict) -> dict:
    truth = data["truth"]
    support = {
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "support_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "grounded",
            }
            for row in truth["support"]
        ]
    }
    by_witness = {row["witness_id"]: row for row in support["units"]}
    support_canary = {"units": [deepcopy(by_witness[key]) for key in truth["support_canary_witness_ids"]]}
    fields = {"decisions": []}
    for row in truth["field_tasks"]:
        if row["field"] == "unsupported_inference":
            continue
        fields["decisions"].append(
            {
                "task_id": row["task_id"],
                "field_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "grounded",
            }
        )
    field_by_id = {row["task_id"]: row for row in fields["decisions"]}
    repeats = {"decisions": [deepcopy(field_by_id[key]) for key in truth["field_repeat_task_ids"]]}
    alignment_cases = []
    for row in truth["alignment_cases"]:
        expected = row["expected"]
        alignment_cases.append(
            {
                "case_id": row["case_id"],
                "alignment_pairs": [
                    {
                        "witness_ids": pair["witness_ids"],
                        "relation": pair["relation"],
                        "mismatch_fields": pair["mismatch_fields"],
                        "checklist_decisions": {
                            field: ("different" if field in pair["mismatch_fields"] else "same")
                            for field in CHECKLIST_FIELDS
                        },
                    }
                    for pair in expected["pairs"]
                ],
                "equivalence_groups": expected["equivalence_groups"],
                "unpaired_witness_ids": expected["unpaired_witness_ids"],
            }
        )
    return {
        "support": support,
        "support_canary": support_canary,
        "fields": fields,
        "field_repeats": repeats,
        "alignment": {"cases": alignment_cases},
    }


def test_v167_corrected_truth_can_clear_every_frozen_full_gate():
    data = build_v167_inputs(_validate_v166())
    values = _perfect_score_inputs(data)
    score = v155.score_v155(
        **values,
        truth=data["truth"],
        raw_alignment_disagreement_count=0,
        final_canary_exact_count=len(data["truth"]["alignment_canary_case_ids"]),
    )
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["structured_field_accuracy"] == 1.0
    assert score["metrics"]["equivalent_specificity"] == 1.0


def test_v167_freeze_is_idempotent_capacity_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v167"
    first = freeze_v167(output_dir=root)
    second = freeze_v167(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 67
    assert first["spec"]["minimum_turn_count"] == 67
    assert first["spec"]["maximum_turn_count"] == 67
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["all_semantic_turns_fresh"] is True
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = __import__("json").loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 2680000
    assert policy["projected_phase_quota_points"] == 46
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v167_freeze_does_not_mutate_v166(tmp_path: Path):
    paths = [v166.DEFAULT_OUTPUT_ROOT / "terminal.json", v166.DEFAULT_OUTPUT_ROOT / "alignment-protocol-v166.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v167(output_dir=tmp_path / "v167")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
