from __future__ import annotations

import hashlib
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v3.json"
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-ordered-recurrent-full-schema-fold-v3.json"
)


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_epoch3_plan_is_strict_and_binds_the_directive():
    plan = _load(PLAN_PATH)
    assert set(plan) == {"schema_version", "thread_id", "plan_epoch", "state", "step"}
    assert set(plan["step"]) == {
        "step_id",
        "state",
        "max_model_calls",
        "max_total_tokens",
        "expected_receipt_path",
        "accepted_receipt_states",
        "directive_path",
        "directive_sha256",
    }
    assert plan["plan_epoch"] == 3
    assert plan["step"]["max_model_calls"] == 2
    assert plan["step"]["max_total_tokens"] == 72891
    assert Path(plan["step"]["directive_path"]) == DIRECTIVE_PATH
    assert plan["step"]["directive_sha256"] == _sha(DIRECTIVE_PATH)


def test_epoch3_token_order_thread_and_cost_contract_are_exact():
    directive = _load(DIRECTIVE_PATH)
    execution = directive["execution_contract"]
    turns = directive["architecture_contract"]["representative_development_canary"]
    assert execution["model"] == "gpt-5.6-sol"
    assert execution["reasoning_effort"] == "high"
    assert execution["app_server_process_count"] == 1
    assert execution["thread_count"] == 1
    assert execution["thread_ephemeral"] is True
    assert execution["thread_reused_for_both_turns"] is True
    assert execution["semantic_retry_cap"] == 0
    assert execution["turn_total_token_caps"] == [24000, 48891]
    assert sum(execution["turn_total_token_caps"]) == 72891
    assert [row["density_stratum"] for row in turns["turn_order"]] == [
        "no_signal",
        "dense",
    ]
    cost = execution["production_cost_projection"]
    expected_total = cost["production_amortized_context_tokens"] + (
        cost["combined_extraction_total_token_cap"] * cost["production_scale"]
    )
    assert expected_total == cost["projected_production_amortized_total_tokens"]
    assert round(expected_total / cost["baseline_end_to_end_tokens"], 6) == 0.276915
    assert cost["projected_production_amortized_total_token_ratio"] == 0.276915


def test_epoch3_duplicate_detection_is_nonblocking_and_evaluator_is_full_output():
    directive = _load(DIRECTIVE_PATH)
    architecture = directive["architecture_contract"]
    structural = directive["structural_acceptance_contract"]
    assert architecture["deterministic_deduplication_allowed"] is False
    assert architecture["exact_identity_duplicate_detection"].startswith("diagnostic_only")
    assert structural["exact_identity_duplicate_count_reported_diagnostic_only"] is True
    assert "exact_identity_duplicate_count" not in structural
    assert directive["branch_after_receipt"]["passed"] == (
        "run_checksum_bound_full_event_ab_ba_evaluator_over_every_baseline_and_every_"
        "recurrent_canary_event_without_support_or_alignment_prefilter"
    )


def test_epoch3_frozen_inputs_and_epoch2_plan_remain_hash_valid():
    directive = _load(DIRECTIVE_PATH)
    for row in directive["frozen_inputs"]:
        path = Path(row["path"])
        assert path.is_file()
        assert _sha(path) == row["sha256"]

    epoch2_lock = _load(
        PROJECT_ROOT
        / "work/app-server-development-v2/unattended-pipeline-v5"
        / "development-selection-v249-true-full-output-shared-reference-v2"
        / "runtime-lock.json"
    )
    rows = [
        row
        for row in epoch2_lock["source_records"]
        if row["path"].endswith("automation/pif-evaluation-semantic-plan-v1.json")
    ]
    assert len(rows) == 1
    assert _sha(Path(rows[0]["path"])) == rows[0]["sha256"]
