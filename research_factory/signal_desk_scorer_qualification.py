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
from .util import now_iso


SELECTION_VERSION = "pif_signal_desk_scorer_qualification_selection_v1"
RECEIPT_VERSION = "pif_signal_desk_scorer_qualification_receipt_v1"


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
    rows: Sequence[Mapping[str, Any]], *, total: int = 100
) -> dict[str, Any]:
    """Freeze balanced real A1-vs-gold scorer decisions for adjudication."""

    if total < 100 or total > 500 or total % 50:
        raise ScorerQualificationError("A2 case count must be 100-500 in 50-case blocks")
    positive: dict[str, list[dict[str, Any]]] = defaultdict(list)
    negative: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        structure = str(row["transcript_structure"])
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
                    "window_id": row["window_id"],
                    "gold_index": gold_index,
                    "predicted_index": predicted_index,
                    "transcript_structure": structure,
                    "scorer_match": bool(decision["eligible"]),
                    "scorer_error_family": family,
                    "scorer_failures": list(decision["failures"]),
                    "evidence_overlap": decision["evidence_overlap"],
                    "claim_text_f1": decision["claim_text_f1"],
                }
                (positive if decision["eligible"] else negative)[family].append(case)
    half = total // 2
    selected = _round_robin(positive, half) + _round_robin(negative, half)
    if len(selected) != total:
        raise ScorerQualificationError(
            f"A2 cannot form a {half}/{half} match balance from real pairs"
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
