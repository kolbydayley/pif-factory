from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v175_support import (
    DEFAULT_REUSE_CONTRACT,
    _support_value,
    _validate_selection_sources,
    _validate_v174_selection_gate,
    build_exact_claim_dedup,
    compact_support_prompt,
    expand_support_output,
    freeze_v175,
    plan_support_shards,
)


def test_v175_requires_the_frozen_passing_v174_gate():
    gate = _validate_v174_selection_gate()
    assert gate["values"]["terminal"]["development_judge_frozen"] is True
    assert gate["values"]["terminal"]["selection_authorized"] is True
    assert gate["values"]["terminal"]["holdout_authorized"] is False


def test_v175_exact_claim_dedup_and_shards_are_complete():
    source = _validate_selection_sources(DEFAULT_REUSE_CONTRACT)
    representatives, expansion, audit = build_exact_claim_dedup(source["pointwise"])
    shards = plan_support_shards(representatives)
    assert len(representatives) == 1566
    assert sum(len(values) for values in expansion.values()) == 1960
    assert audit["exact_duplicate_witness_count"] == 394
    assert audit["semantic_similarity_used"] is False
    assert len(shards) == 15
    assert sum(map(len, shards)) == 1566
    assert max(map(len, shards)) <= 128
    assert all(len(compact_support_prompt(shard).encode()) <= 240000 for shard in shards)


def test_v175_exact_identity_expansion_preserves_model_decisions():
    source = _validate_selection_sources(DEFAULT_REUSE_CONTRACT)
    representatives, expansion, _ = build_exact_claim_dedup(source["pointwise"])
    rows = []
    for unit in representatives:
        rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "support_status": "supported",
                "source_evidence_spans": [unit["source_excerpt"][:1000]],
                "rationale": "Synthetic exact-identity test receipt.",
            }
        )
    expanded = expand_support_output(
        representative_output={"units": rows},
        expansion=expansion,
        full_value=_support_value(source["pointwise"]["units"]),
    )
    assert len(expanded["units"]) == 1960
    by_id = {row["witness_id"]: row for row in expanded["units"]}
    representative = representatives[0]["witness_id"]
    for witness_id in expansion[representative]:
        assert by_id[witness_id]["support_status"] == "supported"


def test_v175_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v175"
    first = freeze_v175(output_dir=root)
    second = freeze_v175(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["input_witness_count"] == 1960
    assert first["spec"]["representative_count"] == 1566
    assert len(first["spec"]["turn_plan"]) == 15
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["exact_identity_expansion_only"] is True
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 45000
    assert policy["phase_total_token_bound"] == 675000
    assert policy["projected_phase_quota_points"] == 12
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
    assert first["spec"]["alignment_authorized"] is False
    assert first["spec"]["selection_winner_frozen"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
