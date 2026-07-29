from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v177_alignment_scale_diagnostic import (
    _validate_v176_success,
    build_lossless_compact_case,
    compact_alignment_prompt,
    decode_lossless_compact_case,
    freeze_v177,
)


def test_v177_preserves_the_complete_v176_support_phase():
    predecessor = _validate_v176_success()
    assert predecessor["values"]["terminal"]["usage"]["total_tokens"] == 905090
    assert predecessor["values"]["terminal"]["support_receipts_frozen"] is True
    assert len(predecessor["attempts"]) == 28
    assert len(predecessor["receipts"]["units"]) == 1960


def test_v177_compact_packet_round_trips_without_semantic_pruning(tmp_path: Path):
    frozen = freeze_v177(output_dir=tmp_path / "v177-roundtrip")
    for turn in frozen["turns"]:
        case = turn["value"]["cases"][0]
        compact = build_lossless_compact_case(case)
        assert decode_lossless_compact_case(compact) == case
        assert compact["semantic_fields_pruned"] is False
        prompt, rebuilt = compact_alignment_prompt(turn["value"])
        assert rebuilt == compact
        assert len(prompt.encode()) <= 90000
        assert len(compact["event_definitions"]) <= len(case["witnesses"])


def test_v177_freeze_is_idempotent_presemantic_and_strictly_permuted(tmp_path: Path):
    root = tmp_path / "v177"
    first = freeze_v177(output_dir=root)
    second = freeze_v177(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [
        "selection_alignment_scale_base",
        "selection_alignment_scale_canary",
    ]
    assert first["spec"]["lossless_compact_serialization"] is True
    assert first["spec"]["semantic_fields_pruned"] is False
    assert first["spec"]["full_alignment_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    base = first["turns"][0]["value"]["cases"][0]["witnesses"]
    canary = first["turns"][1]["value"]["cases"][0]["witnesses"]
    assert [row["witness_id"] for row in base] == list(
        reversed([row["witness_id"] for row in canary])
    )
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 200000
    assert policy["projected_phase_quota_points"] == 4
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
