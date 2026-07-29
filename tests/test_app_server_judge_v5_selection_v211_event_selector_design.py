from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_selection_v211_event_selector_design import (
    CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
    CANARY_SELECTOR_TOTAL_TOKEN_GATE,
    JudgeV5SelectionV211Error,
    _validate_v210_checkpoint,
    freeze_v211,
    strip_selector_markers,
)
from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)


@pytest.fixture(scope="module")
def frozen_v211(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v211") / "design"
    terminal = freeze_v211(output_dir=root)
    return root, terminal


def test_v211_binds_v210_nonacceptance_without_replaying_v209():
    predecessor = _validate_v210_checkpoint()
    assert predecessor["terminal"]["overall_evaluation_complete"] is False
    assert predecessor["terminal"]["holdout_authorized"] is False
    assert predecessor["v209"]["usage"]["total_tokens"] == 31_139
    assert predecessor["v209"]["sidecar"]["auth_type"] == "chatgpt"
    assert predecessor["v209"]["sidecar"]["usage_complete"] is True


def test_v211_oracle_supports_small_zero_extraction_selector(frozen_v211):
    root, terminal = frozen_v211
    oracle = json.loads((root / "event-selector-oracle-audit.json").read_text())
    design = json.loads((root / "event-selector-design.json").read_text())
    assert oracle["candidate_event_count"] == 79
    assert oracle["maximum_candidate_event_count"] == 25
    assert oracle["perfect_selector_mean_f1"] == 0.81506
    assert oracle["best_affordable_oracle_mean_f1"] == 0.832541
    assert oracle["mean_f1_regret_to_oracle"] == 0.017481
    assert oracle["maximum_dense_case_regret_to_oracle"] == 0.124286
    assert oracle["dense_improvement_count"] == 4
    assert oracle["selected_extraction_cost_tokens"] == 166_642.75
    assert oracle["joint_canary_token_bound"] == 232_781.75
    assert oracle["scope_budget_tokens"] == 295_704.133333
    assert all(oracle["checks"].values())
    assert design["selector_runtime_total_token_bound"] == (
        CANARY_SELECTOR_RUNTIME_TOKEN_BOUND
    )
    assert design["selector_promotion_total_token_gate"] == (
        CANARY_SELECTOR_TOTAL_TOKEN_GATE
    )
    assert design["extraction_model_calls_authorized"] == 0
    assert design["extraction_replay_allowed"] is False
    assert design["holdout_authorized"] is False
    assert terminal["semantic_attempt_authorized"] is True


def test_v211_packet_preserves_complete_source_and_all_event_coverage(frozen_v211):
    root, _terminal = frozen_v211
    packet = json.loads((root / "selector-input.private.json").read_text())
    provenance = json.loads((root / "selector-provenance.private.json").read_text())
    assert len(packet["cases"]) == 4
    assert len(provenance["cases"]) == 4
    assert sum(len(row["candidate_events"]) for row in packet["cases"]) == 79
    assert sum(row["event_count"] for row in provenance["cases"]) == 79
    for model_case, private_case in zip(
        packet["cases"], provenance["cases"], strict=True
    ):
        assert model_case["case_id"] == private_case["case_id"]
        assert len(model_case["candidate_events"]) == private_case["event_count"]
        assert strip_selector_markers(model_case["annotated_source"])
        assert [row["event_id"] for row in model_case["candidate_events"]] == [
            row["event_id"] for row in private_case["events"]
        ]
    assert set(packet) == {
        "evidence_marker_contract",
        "event_field_legend",
        "cases",
    }
    assert all(
        set(row) == {"case_id", "annotated_source", "candidate_events"}
        for row in packet["cases"]
    )


def test_v211_schema_is_supported_and_prompt_is_model_blind(frozen_v211):
    root, _terminal = frozen_v211
    schema = json.loads((root / "selector-schema.json").read_text())
    packet = json.loads((root / "selector-input.private.json").read_text())
    prompt = (root / "selector-prompt.private.md").read_text()
    assert validate_app_server_output_schema_subset(schema) == []
    assert "$schema" not in schema
    assert "uniqueItems" not in json.dumps(schema)
    assert "arms" not in json.dumps(packet)
    assert "density_stratum" not in json.dumps(packet)
    assert "source_id" not in json.dumps(packet)
    assert "affordable_oracle_f1" not in json.dumps(packet)
    assert "package_id" not in prompt


def test_v211_freeze_is_immutable_and_presemantic(frozen_v211):
    root, terminal = frozen_v211
    assert freeze_v211(output_dir=root) == terminal
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["production_mutated"] is False
    assert terminal["holdout_authorized"] is False


def test_v211_rejects_nonempty_unterminaled_root(tmp_path: Path):
    root = tmp_path / "partial"
    root.mkdir()
    (root / "unexpected.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV211Error, match="nonempty"):
        freeze_v211(output_dir=root)
