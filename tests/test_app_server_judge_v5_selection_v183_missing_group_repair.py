from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v177_alignment_scale_diagnostic import (
    decode_lossless_compact_case,
)
from research_factory.app_server_judge_v5_selection_v183_missing_group_repair import (
    _validate_v182_failure,
    apply_group_repair,
    build_repair_input,
    freeze_v183,
    repair_prompt,
    repair_schema,
    validate_repair_output,
)


def _singleton_output(value):
    return {
        "assignments": [
            {
                "witness_id": witness_id,
                "placement": "new_singleton",
                "target_group_anchor_witness_id": "",
                "source_evidence_spans": [],
                "witness_evidence_ids": [witness_id],
                "rationale": "No existing representative is truth-conditionally equivalent.",
            }
            for witness_id in value["omitted_witness_ids"]
        ]
    }


def test_v183_preserves_v182_complete_usage_and_exact_two_witness_defect():
    predecessor = _validate_v182_failure()
    assert predecessor["values"]["terminal"]["state"] == "failed"
    assert predecessor["usage"]["total_tokens"] == 361183
    assert predecessor["missing_ids"] == sorted(predecessor["missing_ids"])
    assert len(predecessor["missing_ids"]) == 2
    assert len(predecessor["completed"]) == 4
    assert len(predecessor["normalized_prefix"]["cases"]) == 8


def test_v183_repair_input_is_lossless_side_free_and_bounded():
    predecessor = _validate_v182_failure()
    value = build_repair_input(predecessor)
    decoded = decode_lossless_compact_case(value["lossless_compact_case"])
    assert len(value["omitted_witness_ids"]) == 2
    assert len(value["existing_group_representatives"]) == 52
    assert len(decoded["witnesses"]) == 54
    assert value["origin_neutral"] is True
    assert len(repair_prompt(value).encode()) <= 90000
    assert len(json.dumps(repair_schema(value), separators=(",", ":")).encode()) <= 20000


def test_v183_applies_only_llm_group_placements_and_structural_coverage():
    predecessor = _validate_v182_failure()
    value = build_repair_input(predecessor)
    output = _singleton_output(value)
    assert validate_repair_output(output, value) == []
    repaired, audit = apply_group_repair(predecessor, output)
    assert audit["repaired_witness_count"] == 2
    assert audit["prior_valid_group_memberships_changed"] is False
    assert audit["prior_alignment_pairs_changed"] is False
    assert audit["prior_checklists_changed"] is False
    assert audit["llm_owned_group_placement"] is True
    assert len(repaired["cases"][0]["equivalence_groups"]) == 54


def test_v183_rejects_assignment_contract_drift():
    predecessor = _validate_v182_failure()
    value = build_repair_input(predecessor)
    output = _singleton_output(value)
    output["assignments"][0]["witness_evidence_ids"] = []
    assert validate_repair_output(output, value)


def test_v183_freeze_is_one_turn_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v183"
    first = freeze_v183(output_dir=root)
    second = freeze_v183(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["v182_completed_turn_replayed"] is False
    assert first["spec"]["full_case_reread"] is False
    assert first["spec"]["omitted_witness_count"] == 2
    assert first["spec"]["existing_group_representative_count"] == 52
    assert first["spec"]["lossless_compact_serialization"] is True
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 120000
    assert policy["projected_phase_quota_points"] == 3
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
