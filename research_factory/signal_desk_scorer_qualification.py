"""A2 scorer-case selection and aggregate qualification receipts."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_scorer import (
    event_eligibility,
    qualify_scorer,
    scorer_sha256,
    scorer_specification,
)
from .signal_desk_gold_atomicity import ATOMICITY_REVIEW_WINDOW_IDS
from .util import now_iso


SELECTION_VERSION = "pif_signal_desk_scorer_qualification_selection_v2"
RECEIPT_VERSION = "pif_signal_desk_scorer_qualification_receipt_v1"
MINIMUM_SPLIT_MERGE_STRESS_CASES = 6


class ScorerQualificationError(RuntimeError):
    pass


def _sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _family(decision: Mapping[str, Any], structure: str) -> str:
    if decision["eligible"]:
        if decision.get("unsupported_attribution"):
            return f"eligible:unsupported_attribution:{structure}"
        for field, agreed in (decision.get("field_agreement") or {}).items():
            if not agreed:
                return f"eligible:{field}_disagreement:{structure}"
        return f"eligible:all_fields:{structure}"
    failures = list(decision.get("failures") or ["unknown"])
    return f"{failures[0]}:{structure}"


def _round_robin(groups: Mapping[str, list[dict[str, Any]]], count: int) -> list[dict[str, Any]]:
    queues = {key: list(values) for key, values in sorted(groups.items()) if values}
    selected: list[dict[str, Any]] = []
    while queues and len(selected) < count:
        for key in list(queues):
            if not queues[key]:
                del queues[key]
                continue
            selected.append(queues[key].pop(0))
            if len(selected) == count:
                break
    return selected


def select_qualification_cases(
    rows: Sequence[Mapping[str, Any]], *, total: int = 100,
    priority_window_ids: Sequence[str] = tuple(ATOMICITY_REVIEW_WINDOW_IDS),
    minimum_split_merge_stress_cases: int = MINIMUM_SPLIT_MERGE_STRESS_CASES,
) -> dict[str, Any]:
    """Freeze balanced real A1-vs-gold scorer decisions for adjudication."""

    if total < 100 or total > 500 or total % 50:
        raise ScorerQualificationError("A2 case count must be 100-500 in 50-case blocks")
    if minimum_split_merge_stress_cases < 0 or minimum_split_merge_stress_cases > total:
        raise ScorerQualificationError("A2 split/merge stress count is invalid")
    priority_ids = {str(window_id) for window_id in priority_window_ids}
    positive: dict[str, list[dict[str, Any]]] = defaultdict(list)
    negative: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        structure = str(row["transcript_structure"])
        window_id = str(row["window_id"])
        gold_span_counts = defaultdict(int)
        predicted_span_counts = defaultdict(int)
        for event in row["gold"]["events"]:
            gold_span_counts[(int(event["evidence_start"]), int(event["evidence_end"]))] += 1
        for event in row["predicted"]["events"]:
            predicted_span_counts[(int(event["evidence_start"]), int(event["evidence_end"]))] += 1
        for gold_index, gold in enumerate(row["gold"]["events"]):
            for predicted_index, predicted in enumerate(row["predicted"]["events"]):
                decision = event_eligibility(
                    gold, predicted, transcript_structure=structure
                )
                family = _family(decision, structure)
                case = {
                    "case_id": _sha(
                        [row["window_id"], gold_index, predicted_index, scorer_sha256()]
                    )[:24],
                    "window_id": window_id,
                    "gold_index": gold_index,
                    "predicted_index": predicted_index,
                    "transcript_structure": structure,
                    "scorer_match": bool(decision["eligible"]),
                    "scorer_error_family": family,
                    "scorer_failures": list(decision["failures"]),
                    "evidence_overlap": decision["evidence_overlap"],
                    "claim_text_f1": decision["claim_text_f1"],
                    "gold_same_span_multiplicity": gold_span_counts[
                        (int(gold["evidence_start"]), int(gold["evidence_end"]))
                    ],
                    "predicted_same_span_multiplicity": predicted_span_counts[
                        (int(predicted["evidence_start"]), int(predicted["evidence_end"]))
                    ],
                }
                case["split_merge_stress"] = bool(
                    window_id in priority_ids
                    and max(
                        int(case["gold_same_span_multiplicity"]),
                        int(case["predicted_same_span_multiplicity"]),
                    ) >= 2
                )
                (positive if decision["eligible"] else negative)[family].append(case)
    half = total // 2
    selected: list[dict[str, Any]] = []
    # Split stress cases across scorer matches and non-matches when possible.
    # That prevents a selector from satisfying the quota with only easy
    # duplicates or only obvious false pairs.
    desired_positive = minimum_split_merge_stress_cases // 2
    desired_negative = minimum_split_merge_stress_cases - desired_positive
    selected_positive = _round_robin(
        {
            family: [case for case in cases if case["split_merge_stress"]]
            for family, cases in positive.items()
        },
        desired_positive,
    )
    selected_negative = _round_robin(
        {
            family: [case for case in cases if case["split_merge_stress"]]
            for family, cases in negative.items()
        },
        desired_negative,
    )
    selected.extend(selected_positive)
    selected.extend(selected_negative)
    selected_ids = {case["case_id"] for case in selected}
    remaining_positive = {
        family: [case for case in cases if case["case_id"] not in selected_ids]
        for family, cases in positive.items()
    }
    remaining_negative = {
        family: [case for case in cases if case["case_id"] not in selected_ids]
        for family, cases in negative.items()
    }
    selected.extend(_round_robin(remaining_positive, half - len(selected_positive)))
    selected.extend(_round_robin(remaining_negative, half - len(selected_negative)))
    if len(selected) != total:
        raise ScorerQualificationError(
            f"A2 cannot form a {half}/{half} match balance from real pairs"
        )
    stress_count = sum(bool(case["split_merge_stress"]) for case in selected)
    if stress_count < minimum_split_merge_stress_cases:
        raise ScorerQualificationError(
            "A2 selection lacks the required one-span/many-claim split/merge stress cases"
        )
    families = {case["scorer_error_family"] for case in selected}
    if len(families) < 10:
        raise ScorerQualificationError(
            f"A2 selection has only {len(families)} error families; ten are required"
        )
    selected.sort(key=lambda case: case["case_id"])
    payload: dict[str, Any] = {
        "schema_version": SELECTION_VERSION,
        "created_at": now_iso(),
        "scorer_sha256": scorer_sha256(),
        "scorer_specification": scorer_specification(),
        "case_count": len(selected),
        "expected_balance": {"scorer_match": half, "scorer_no_match": half},
        "error_family_count": len(families),
        "split_merge_stress_case_count": stress_count,
        "minimum_split_merge_stress_cases": minimum_split_merge_stress_cases,
        "cases": selected,
    }
    payload["selection_sha256"] = _sha(payload)
    return payload


def qualification_receipt(
    selection: Mapping[str, Any], adjudications: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_case = {str(row["case_id"]): row for row in adjudications}
    selected_ids = [str(row["case_id"]) for row in selection["cases"]]
    if set(by_case) != set(selected_ids):
        raise ScorerQualificationError("A2 adjudications do not exactly cover the frozen selection")
    decisions = [
        {
            "expected_match": bool(by_case[case_id]["expected_match"]),
            "scorer_match": bool(case["scorer_match"]),
            "error_family": str(case["scorer_error_family"]),
        }
        for case_id, case in zip(selected_ids, selection["cases"])
    ]
    result = qualify_scorer(decisions)
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_VERSION,
        "created_at": now_iso(),
        **result,
        "selection_sha256": selection["selection_sha256"],
        "adjudication_model": "gpt-5.6-sol",
        "adjudication_effort": "medium",
        "item_outputs_exposed": False,
    }
    receipt["receipt_sha256"] = _sha(receipt)
    return receipt
