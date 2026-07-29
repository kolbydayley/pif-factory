from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from research_factory import app_server_judge_v5_selection_v192_residual_repair_design as v192
from research_factory.app_server_judge_v5_selection_v207_adaptive_router_design import (
    BASE_ARM,
    CANARY_CASE_COUNT,
    MAX_CANARY_TOTAL_TOKENS,
    MAX_FULL_ROUTER_TOTAL_TOKENS,
    JudgeV5SelectionV207Error,
    _router_cases,
    _selected_canary_rows,
    _validate_v206_checkpoint,
    freeze_v207,
    strip_evidence_markers,
)


@pytest.fixture(scope="module")
def frozen_v207(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("v207") / "attempt"
    freeze_v207(output_dir=root)
    return root


def test_v207_binds_v206_without_authorizing_extraction():
    predecessor = _validate_v206_checkpoint()
    assert predecessor["terminal"]["overall_evaluation_complete"] is False
    assert predecessor["terminal"]["development_winner_frozen"] is False
    assert predecessor["terminal"]["holdout_authorized"] is False
    assert predecessor["terminal"]["production_mutated"] is False
    assert predecessor["terminal"]["cumulative_known_usage_lower_bound"]["total_tokens"] == 8_900_303


def test_v207_hash_canary_preserves_every_source_character_and_base_claim():
    sources = v186._selection_sources()
    _augmented, score, _combinations = v192._all_composite_score()
    rows = _selected_canary_rows(score)
    cases = _router_cases(rows=rows, sources=sources)
    pool = {str(row["case_id"]): row for row in sources["pool"]["cases"]}
    assert len(rows) == CANARY_CASE_COUNT
    assert sum(row["density_stratum"] == "no_signal" for row in rows) == 4
    assert sum(row["density_stratum"] != "no_signal" for row in rows) == 4
    assert len({str(row["source_id"]) for row in rows}) == 4
    for case in cases:
        assert strip_evidence_markers(case["annotated_source"]) == pool[case["case_id"]][
            "source_excerpt"
        ]
        assert all(set(row) == {"id", "claim"} for row in case["base_event_claims"])


def test_v207_freezes_real_zero_extraction_quality_and_cost_ceiling(frozen_v207: Path):
    audit = json.loads((frozen_v207 / "adaptive-router-oracle-audit.json").read_text())
    full = audit["full_development"]
    canary = audit["canary"]
    assert audit["base_arm_always_observed_and_charged"] == BASE_ARM
    assert audit["router_model_never_receives_reference_scores_or_density"] is True
    assert full["minimum_passing_extraction_cost_upper_rounded_tokens"] == 755_000
    assert full["remaining_router_headroom_tokens"] >= MAX_FULL_ROUTER_TOTAL_TOKENS
    assert full["bootstrap"]["ci_lower"] >= -0.03
    assert full["worst_source_delta"] >= -0.05
    assert canary["declared_router_bound_tokens"] == MAX_CANARY_TOTAL_TOKENS
    assert canary["best_affordable_candidate_f1"] == 0.832541
    assert canary["best_affordable_extraction_cost_upper_rounded_tokens"] == 217_000


def test_v207_design_is_opaque_presemantic_and_idempotent(frozen_v207: Path):
    first = json.loads((frozen_v207 / "terminal.json").read_text())
    second = freeze_v207(output_dir=frozen_v207)
    design = json.loads((frozen_v207 / "adaptive-router-design.json").read_text())
    prompt = (frozen_v207 / "canary-prompt.private.md").read_text()
    schema = json.loads((frozen_v207 / "canary-schema.json").read_text())
    assert first == second
    assert first["semantic_attempt_authorized"] is True
    assert first["extraction_model_calls_authorized"] == 0
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert design["semantic_output_scope"] == "package_selection_only_no_event_extraction_or_rewrite"
    assert design["extraction_replay_allowed"] is False
    assert design["batch_5_replay_allowed"] is False
    assert design["full_development_router_authorized"] is False
    assert "batch_5_new_thread" not in prompt
    assert "batch_3_same_thread" not in prompt
    assert schema["properties"]["cases"]["minItems"] == CANARY_CASE_COUNT
    assert schema["properties"]["cases"]["maxItems"] == CANARY_CASE_COUNT
    assert not list(frozen_v207.rglob("capacity.json"))
    assert not list(frozen_v207.rglob("sidecar.json"))
    assert not list(frozen_v207.rglob("output.private.json"))


def test_v207_rejects_nonempty_unterminaled_root(tmp_path: Path):
    root = tmp_path / "v207"
    root.mkdir()
    (root / "unexpected.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV207Error, match="nonempty"):
        freeze_v207(output_dir=root)
