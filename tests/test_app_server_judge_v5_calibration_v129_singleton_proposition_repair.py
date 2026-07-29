from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v129_singleton_proposition_repair import (
    _validate_v128,
    build_v129_inputs,
    finalize_v128_primary,
    freeze_v129,
    score_v129,
)


def _passing_outputs(rows):
    outputs = {}
    for row in rows:
        truth = row["truth"]
        outputs[row["turn_name"]] = {
            "decisions": [
                {
                    "task_id": truth["task_id"],
                    "proposition_status": truth.get("control_expected_status") or "supported",
                    "source_evidence_spans": [row["value"]["tasks"][0]["source_excerpt"]],
                    "rationale": "synthetic singleton decision",
                }
            ]
        }
    return outputs


def test_v129_selects_one_trigger_and_two_balanced_singleton_controls():
    rows, truth, selection = build_v129_inputs(_validate_v128())
    assert len(rows) == 3
    assert truth["singleton_control_count"] == 2
    assert truth["singleton_owner_count"] == 1
    assert selection["observable_repair_trigger_count"] == 1
    assert selection["trigger_role"] == "legacy_unsupported_definition_migration"
    assert selection["control_status_counts"] == {"supported": 1, "unsupported": 1}
    assert selection["maximum_tasks_per_turn"] == 1
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert all(row["value"]["task_count"] == 1 for row in rows)


def test_v129_score_and_final_projection_are_fail_closed():
    predecessor = _validate_v128()
    rows, truth, _ = build_v129_inputs(predecessor)
    outputs = _passing_outputs(rows)
    passed = score_v129(outputs, truth)
    assert passed["passed"] is True
    assert passed["metrics"]["singleton_control_exact_count"] == 2
    final = finalize_v128_primary(v128=predecessor, truth=truth, outputs=outputs)
    assert final["singleton_repair_count"] == 1
    assert len(final["decisions"]) == 10

    owner = next(row for row in truth["tasks"] if row["role"] == "singleton_owner")
    for output in outputs.values():
        if output["decisions"][0]["task_id"] == owner["task_id"]:
            output["decisions"][0]["proposition_status"] = "abstain"
    assert score_v129(outputs, truth)["passed"] is False


def test_v129_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v129"
    first = freeze_v129(output_dir=root)
    second = freeze_v129(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-terra"
    assert len(first["spec"]["turn_plan"]) == 3
    assert first["spec"]["maximum_tasks_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["retained_proposition_reference_patch_authorized"] is False
    assert first["spec"]["proposition_reference_frozen"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 210000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
