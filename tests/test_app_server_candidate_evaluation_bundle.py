from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from research_factory import app_server_candidate_evaluation_bundle as evaluator
from research_factory import app_server_judge_v5 as judge


PIPELINE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
)
V249_ROOT = PIPELINE_ROOT / "development-selection-v5_4-v249-explicit-applicability"
SOURCE_PATH = (
    V249_ROOT
    / "turns"
    / "v249-explicit-applicability-d7c914bc1cee2c432b36"
    / "input.private.json"
)
CANDIDATE_PATH = V249_ROOT / "normalized-output.private.json"
PROVENANCE_PATH = V249_ROOT / "evidence-provenance.private.json"
REFERENCE_PATH = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v220-fresh-integrated-base-design"
    / "shared-reference-seed-v1.json"
)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _bundle() -> dict[str, Any]:
    return evaluator.build_support_bundle(
        evaluation_id="candidate_evaluator_v249_fixture",
        source=_load(SOURCE_PATH),
        candidate=_load(CANDIDATE_PATH),
        provenance=_load(PROVENANCE_PATH),
        shared_reference=_load(REFERENCE_PATH),
    )


def _all_supported_output(bundle: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for unit in bundle["support_value"]["units"]:
        source = str(unit["source_excerpt"])
        rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "support_status": "supported",
                "source_evidence_spans": [source[: min(40, len(source))]],
                "rationale": "Synthetic exact-span support fixture.",
            }
        )
    return {"units": rows}


def _contains_forbidden_model_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        if set(value) & {"origin", "system_id", "system_identity", "candidate_system"}:
            return True
        return any(_contains_forbidden_model_key(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_forbidden_model_key(item) for item in value)
    return False


def _alignment_output(
    alignment_input: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    unmatched_reference_count: int = 0,
) -> dict[str, Any]:
    origin_by_id = {
        str(row["witness_id"]): str(row["origin"])
        for row in mapping["rows"]
        if row["witness_id"]
    }
    cases = []
    for input_case in alignment_input["cases"]:
        witness_ids = [str(row["witness_id"]) for row in input_case["witnesses"]]
        references = sorted(
            witness_id for witness_id in witness_ids if origin_by_id[witness_id] == "reference"
        )
        candidates = sorted(
            witness_id for witness_id in witness_ids if origin_by_id[witness_id] == "candidate"
        )
        pair_count = max(
            0, min(len(references), len(candidates)) - unmatched_reference_count
        )
        pairs = list(zip(references[:pair_count], candidates[:pair_count]))
        paired_ids = {item for pair in pairs for item in pair}
        unpaired = sorted(set(witness_ids) - paired_ids)
        groups = [
            {"witness_ids": [first, second], "rationale": "Equivalent fixture pair."}
            for first, second in pairs
        ]
        groups.extend(
            {"witness_ids": [witness_id], "rationale": "Unpaired fixture witness."}
            for witness_id in unpaired
        )
        alignment_pairs = []
        for first, second in pairs:
            alignment_pairs.append(
                {
                    "witness_id_1": first,
                    "witness_id_2": second,
                    "relation": "equivalent",
                    "checklist": [
                        {
                            "field": field,
                            "decision": "same",
                            "source_evidence_spans": [],
                            "witness_evidence_ids": [first, second],
                            "rationale": "No material truth-conditional difference.",
                        }
                        for field in judge.CHECKLIST_FIELDS
                    ],
                    "rationale": "Equivalent across the complete checklist.",
                }
            )
        cases.append(
            {
                "case_id": input_case["case_id"],
                "equivalence_groups": groups,
                "alignment_pairs": alignment_pairs,
                "unpaired_witness_ids": unpaired,
            }
        )
    return {"cases": cases}


def test_v249_bundle_uses_dynamic_counts_and_exact_grounding() -> None:
    bundle = _bundle()
    assert bundle["counts"]["reference_witness_count"] == 27
    assert bundle["counts"]["candidate_witness_count"] == 33
    assert bundle["counts"]["counts_are_observed_not_targets"] is True
    assert bundle["grounding"]["candidate_event_count"] == 33
    assert bundle["grounding"]["exact_evidence_rate"] == 1.0
    assert bundle["grounding"]["metric_grounding_error_event_count"] == 0
    assert bundle["grounding"]["event_cap_violation_count"] == 0
    assert not _contains_forbidden_model_key(bundle["support_value"])
    assert (
        bundle["request_sizes"]["prompt_bytes"]
        <= evaluator.SUPPORT_MAX_PROMPT_BYTES
    )
    assert (
        bundle["request_sizes"]["schema_bytes"]
        <= evaluator.SUPPORT_MAX_SCHEMA_BYTES
    )


def test_alignment_bundle_is_opaque_balanced_and_uses_exact_15_row_rubric() -> None:
    support = _bundle()
    _audit, private_score, receipts = evaluator.score_support_output(
        output=_all_supported_output(support), support_bundle=support
    )
    alignment = evaluator.build_alignment_bundle(
        support_bundle=support,
        support_private_score=private_score,
        support_receipts=receipts,
    )
    assert alignment["counts"]["checklist_field_count"] == 15
    assert alignment["counts"]["counts_are_observed_not_targets"] is True
    assert not _contains_forbidden_model_key(alignment["turns"][0]["value"])
    base_cases = alignment["turns"][0]["value"]["cases"]
    canary_cases = alignment["turns"][1]["value"]["cases"]
    assert [row["case_id"] for row in canary_cases] == list(
        reversed([row["case_id"] for row in base_cases])
    )
    canary_by_id = {row["case_id"]: row for row in canary_cases}
    for base_case in base_cases:
        assert [row["witness_id"] for row in canary_by_id[base_case["case_id"]]["witnesses"]] == list(
            reversed([row["witness_id"] for row in base_case["witnesses"]])
        )
    assert alignment["turns"][0]["value"]["checklist_field_order"] == list(
        judge.CHECKLIST_FIELDS
    )
    assert (
        alignment["turns"][0]["prompt_bytes"]
        <= evaluator.ALIGNMENT_MAX_PROMPT_BYTES
    )
    assert (
        alignment["turns"][0]["schema_bytes"]
        <= evaluator.ALIGNMENT_MAX_SCHEMA_BYTES
    )


def test_dynamic_score_passes_consistent_full_coverage_without_count_target() -> None:
    support = _bundle()
    _audit, private_score, receipts = evaluator.score_support_output(
        output=_all_supported_output(support), support_bundle=support
    )
    alignment = evaluator.build_alignment_bundle(
        support_bundle=support,
        support_private_score=private_score,
        support_receipts=receipts,
    )
    base_output = _alignment_output(
        alignment["turns"][0]["value"], alignment["mapping"]
    )
    canary_output = _alignment_output(
        alignment["turns"][1]["value"], alignment["mapping"]
    )
    score = evaluator.score_alignment(
        base_output=base_output,
        canary_output=canary_output,
        alignment_bundle=alignment,
        support_bundle=support,
        support_private_score=private_score,
        production_amortized_total_token_ratio=0.279,
    )
    assert score["passed"] is True
    assert score["metrics"]["candidate_witness_count"] == 33
    assert score["metrics"]["reference_semantic_unit_count"] == 27
    assert score["metrics"]["development_strict_full_field_macro_f1"] == 1.0


def test_dynamic_score_rejects_quality_miss_and_permutation_disagreement() -> None:
    support = _bundle()
    _audit, private_score, receipts = evaluator.score_support_output(
        output=_all_supported_output(support), support_bundle=support
    )
    alignment = evaluator.build_alignment_bundle(
        support_bundle=support,
        support_private_score=private_score,
        support_receipts=receipts,
    )
    base_output = _alignment_output(
        alignment["turns"][0]["value"],
        alignment["mapping"],
        unmatched_reference_count=4,
    )
    canary_output = _alignment_output(
        alignment["turns"][1]["value"],
        alignment["mapping"],
        unmatched_reference_count=5,
    )
    score = evaluator.score_alignment(
        base_output=base_output,
        canary_output=canary_output,
        alignment_bundle=alignment,
        support_bundle=support,
        support_private_score=private_score,
        production_amortized_total_token_ratio=0.279,
    )
    assert score["passed"] is False
    assert "permutation_projection_exact" in score["failed_checks"]
    assert "strict_full_field_macro_f1_gte_0_97" in score["failed_checks"]


def test_observable_disagreement_gets_one_side_free_adjudication() -> None:
    support = _bundle()
    _audit, private_score, receipts = evaluator.score_support_output(
        output=_all_supported_output(support), support_bundle=support
    )
    alignment = evaluator.build_alignment_bundle(
        support_bundle=support,
        support_private_score=private_score,
        support_receipts=receipts,
    )
    base_output = _alignment_output(
        alignment["turns"][0]["value"],
        alignment["mapping"],
        unmatched_reference_count=4,
    )
    canary_output = _alignment_output(
        alignment["turns"][1]["value"],
        alignment["mapping"],
        unmatched_reference_count=5,
    )
    adjudication = evaluator.build_adjudication_bundle(
        base_output=base_output,
        canary_output=canary_output,
        alignment_bundle=alignment,
    )
    assert adjudication["adjudication_required"] is True
    assert adjudication["adjudication_call_cap"] == 1
    assert adjudication["turn"]["model"] == "gpt-5.5"
    assert adjudication["turn"]["effort"] == "high"
    assert not _contains_forbidden_model_key(adjudication["turn"]["value"])
    adjudication_output = _alignment_output(
        adjudication["turn"]["value"], alignment["mapping"]
    )
    score = evaluator.score_alignment(
        base_output=base_output,
        canary_output=canary_output,
        alignment_bundle=alignment,
        support_bundle=support,
        support_private_score=private_score,
        production_amortized_total_token_ratio=0.279,
        adjudication_bundle=adjudication,
        adjudication_output=adjudication_output,
    )
    assert score["checks"]["observable_disagreement_adjudication_complete"] is True
    assert score["checks"]["adjudication_call_cap_lte_1"] is True
    assert score["checks"]["unresolved_alignment_cases_0"] is True
    assert score["metrics"]["adjudication_call_count"] == 1
    assert score["metrics"]["unresolved_alignment_case_count"] == 0
    assert score["checks"]["permutation_projection_exact"] is False


def test_no_disagreement_does_not_authorize_adjudication_turn() -> None:
    support = _bundle()
    _audit, private_score, receipts = evaluator.score_support_output(
        output=_all_supported_output(support), support_bundle=support
    )
    alignment = evaluator.build_alignment_bundle(
        support_bundle=support,
        support_private_score=private_score,
        support_receipts=receipts,
    )
    base_output = _alignment_output(
        alignment["turns"][0]["value"], alignment["mapping"]
    )
    canary_output = _alignment_output(
        alignment["turns"][1]["value"], alignment["mapping"]
    )
    adjudication = evaluator.build_adjudication_bundle(
        base_output=base_output,
        canary_output=canary_output,
        alignment_bundle=alignment,
    )
    assert adjudication["adjudication_required"] is False
    assert adjudication["turn"] is None


def test_evaluator_runtime_does_not_import_numbered_strategy_modules() -> None:
    source = Path(evaluator.__file__).read_text(encoding="utf-8")
    assert "from . import app_server_judge_v5_selection_v" not in source


def test_protocol_freeze_is_zero_turn_and_fails_on_direct_drift(tmp_path: Path) -> None:
    frozen = evaluator.freeze_protocol(tmp_path / "candidate-evaluator")
    assert frozen["semantic_turn_count"] == 0
    assert not (frozen["root"] / "launch-receipt.json").exists()
    assert not (frozen["root"] / "turns").exists()
    evaluator.verify_protocol(frozen["root"])
    protocol_path = frozen["root"] / "protocol.json"
    protocol = _load(protocol_path)
    protocol["quality_threshold"] = 0.96
    protocol_path.write_text(
        json.dumps(protocol, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(evaluator.CandidateEvaluationError):
        evaluator.verify_protocol(frozen["root"])


def test_nonexact_candidate_evidence_fails_before_bundle_creation() -> None:
    source = _load(SOURCE_PATH)
    candidate = copy.deepcopy(_load(CANDIDATE_PATH))
    candidate["segments"][0]["events"][0]["evidence"] = "not in source"
    with pytest.raises(evaluator.CandidateEvaluationError, match="exact source span"):
        evaluator.build_support_bundle(
            evaluation_id="candidate_evaluator_nonexact_fixture",
            source=source,
            candidate=candidate,
            provenance=None,
            shared_reference=_load(REFERENCE_PATH),
        )


def test_zero_precision_and_recall_score_zero() -> None:
    assert evaluator._f1(0.0, 0.0) == 0.0  # noqa: SLF001
