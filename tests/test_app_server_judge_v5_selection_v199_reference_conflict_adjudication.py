from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_selection_v199_reference_conflict_adjudication import (
    TURN_NAME,
    _validate_v198_conflict,
    build_v199_input,
    freeze_v199,
    reconcile_v199,
)


@pytest.fixture(scope="module")
def bundle():
    source = _validate_v198_conflict()
    value, plan = build_v199_input(source)
    return source, value, plan


def _singleton_output(value):
    cases = []
    for case in value["cases"]:
        witness_ids = [row["witness_id"] for row in case["witnesses"]]
        cases.append(
            {
                "case_id": case["case_id"],
                "alignment_pairs": [],
                "unpaired_witness_ids": witness_ids,
                "equivalence_groups": [
                    {
                        "witness_ids": [witness_id],
                        "rationale": "Synthetic singleton group.",
                    }
                    for witness_id in witness_ids
                ],
            }
        )
    return {"cases": cases}


def test_v199_binds_completed_v198_conflict_terminal():
    source = _validate_v198_conflict()
    assert source["terminal"]["usage"]["total_tokens"] == 283920
    assert source["terminal"]["frozen_reference_partition_conflict_count"] == 7
    assert source["terminal"]["scoring_authorized"] is False


def test_v199_selects_only_three_semantic_repair_placement_disputes(bundle):
    _source, value, plan = bundle
    assert len(plan["conflicts"]) == 7
    assert plan["frozen_reference_only_conflict_count"] == 4
    assert plan["repair_placement_dispute_count"] == 3
    assert len(value["cases"]) == 3
    assert all(len(row["witnesses"]) == 3 for row in value["cases"])
    assert value["side_labels_present"] is False
    assert value["system_identity_present"] is False
    assert value["adjudication_only"] is True


def test_v199_singleton_adjudication_preserves_reference_partition(bundle):
    source, value, plan = bundle
    reconciliation, audit = reconcile_v199(
        source=source,
        plan=plan,
        adjudication_output=_singleton_output(value),
        adjudication_input=value,
    )
    assert audit["post_reconciliation_reference_conflict_count"] == 0
    assert audit["decision_counts"] == {
        "matched_reference": 0,
        "novel": 3,
        "abstain": 0,
    }
    assert audit["scoring_authorized"] is True
    assert reconciliation["abstained_original_case_ids"] == []


def test_v199_merged_references_abstain_instead_of_guessing(bundle):
    source, value, plan = bundle
    output = _singleton_output(value)
    first = output["cases"][0]
    ids = [row["witness_id"] for row in value["cases"][0]["witnesses"]]
    first["equivalence_groups"] = [
        {"witness_ids": ids, "rationale": "Synthetic unresolved group."}
    ]
    reconciliation, audit = reconcile_v199(
        source=source,
        plan=plan,
        adjudication_output=output,
        adjudication_input=value,
    )
    assert audit["decision_counts"]["abstain"] == 1
    assert audit["abstained_original_case_count"] == 1
    assert len(reconciliation["abstained_original_case_ids"]) == 1
    assert audit["post_reconciliation_reference_conflict_count"] == 0


def test_v199_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v199"
    first = freeze_v199(output_dir=root)
    second = freeze_v199(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["adjudication_case_count"] == 3
    assert first["spec"]["adjudication_call_cap"] == 1
    assert first["spec"]["new_judge_prompt_or_rubric_created"] is False
    assert first["spec"]["scoring_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 100000
    assert policy["projected_phase_quota_points"] == 2
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
