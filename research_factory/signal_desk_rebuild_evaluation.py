"""Deterministic aggregate evaluation for clean-event gold and predictions."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_scorer import (
    CLAIM_TEXT_F1_FLOOR,
    EVIDENCE_OVERLAP_FLOOR,
    _maximum_weight_assignment,
    evidence_overlap,
    event_eligibility,
    match_events,
)


EVALUATION_VERSION = "pif_signal_desk_rebuild_evaluation_v2"


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 1.0


def diagnostic_pairs(
    gold_events: Sequence[Mapping[str, Any]],
    predicted_events: Sequence[Mapping[str, Any]],
    *,
    transcript_structure: str,
) -> list[dict[str, Any]]:
    """Pair semantically coextensive events before field-level diagnostics.

    Evidence and atomic-claim agreement determine the pairing. Attribution,
    issue, and stance deliberately do not: making those fields eligibility
    constraints would hide their errors by converting them into recall misses.
    """

    decisions = [
        [
            event_eligibility(
                gold,
                predicted,
                transcript_structure=transcript_structure,
            )
            for predicted in predicted_events
        ]
        for gold in gold_events
    ]
    matrix = [
        [
            (0.70 * cell["evidence_overlap"] + 0.30 * cell["claim_text_f1"])
            if cell["evidence_overlap"] >= EVIDENCE_OVERLAP_FLOOR
            and cell["claim_text_f1"] >= CLAIM_TEXT_F1_FLOOR
            else 0.0
            for cell in row
        ]
        for row in decisions
    ]
    result = []
    for gold_index, predicted_index in _maximum_weight_assignment(matrix):
        weight = matrix[gold_index][predicted_index]
        if weight <= 0:
            continue
        result.append(
            {
                "gold_index": gold_index,
                "predicted_index": predicted_index,
                "weight": round(weight, 6),
                **decisions[gold_index][predicted_index],
            }
        )
    return result


def evaluate_windows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate scored window pairs without exposing item-level sealed data."""

    totals = defaultdict(int)
    per_stratum: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    per_window_scores: list[dict[str, Any]] = []
    for row in rows:
        gold = row["gold"]
        predicted = row["predicted"]
        structure = str(row["transcript_structure"])
        gold_events = list(gold["events"])
        predicted_events = list(predicted["events"])
        strict = match_events(
            gold_events,
            predicted_events,
            transcript_structure=structure,
        )
        pairs = diagnostic_pairs(
            gold_events,
            predicted_events,
            transcript_structure=structure,
        )
        field_correct = defaultdict(int)
        for pair in pairs:
            agreement = pair["field_agreement"]
            for field in ("speaker", "speaker_role", "issue", "stance"):
                if agreement[field]:
                    field_correct[field] += 1
        unsupported = strict["unsupported_attributions"]
        atomic_one_to_one = 0
        for pair in pairs:
            gold_event = gold_events[int(pair["gold_index"])]
            overlapping_predictions = sum(
                evidence_overlap(gold_event, predicted_event) >= EVIDENCE_OVERLAP_FLOOR
                for predicted_event in predicted_events
            )
            if overlapping_predictions == 1:
                atomic_one_to_one += 1
        correct_empty = not gold_events and not predicted_events
        counts = {
            "windows": 1,
            "gold_events": len(gold_events),
            "predicted_events": len(predicted_events),
            "matched_events": strict["matched_events"],
            "diagnostic_pairs": len(pairs),
            "speaker_correct": field_correct["speaker"],
            "speaker_role_correct": field_correct["speaker_role"],
            "issue_correct": field_correct["issue"],
            "stance_correct": field_correct["stance"],
            "unsupported_attributions": unsupported,
            "contaminants": len(predicted_events) if not gold_events else 0,
            "schema_valid": 1,
            "evidence_grounded_events": len(predicted_events),
            "atomic_one_to_one_matched": atomic_one_to_one,
            "correct_empty_windows": int(correct_empty),
        }
        for key, value in counts.items():
            totals[key] += value
            per_stratum[structure][key] += value
        precision = _ratio(strict["matched_events"], len(predicted_events))
        recall = _ratio(strict["matched_events"], len(gold_events))
        density_ratio = (
            min(len(gold_events), len(predicted_events))
            / max(len(gold_events), len(predicted_events))
            if gold_events or predicted_events
            else 1.0
        )
        per_window_scores.append(
            {
                "window_id_sha256": hashlib.sha256(str(row["window_id"]).encode()).hexdigest(),
                "show_id": row.get("show_id"),
                "episode_id_sha256": hashlib.sha256(str(row.get("episode_id") or "").encode()).hexdigest(),
                "transcript_structure": structure,
                "event_precision": precision,
                "event_recall": recall,
                "event_f1": _ratio(2 * strict["matched_events"], len(gold_events) + len(predicted_events)),
                "atomicity": _ratio(atomic_one_to_one, len(pairs)),
                "density_ratio": density_ratio,
                "correct_empty": correct_empty,
            }
        )

    def metrics(counts: Mapping[str, int]) -> dict[str, float]:
        precision = _ratio(counts["matched_events"], counts["predicted_events"])
        recall = _ratio(counts["matched_events"], counts["gold_events"])
        field_total = counts["diagnostic_pairs"]
        event_f1 = _ratio(2 * counts["matched_events"], counts["gold_events"] + counts["predicted_events"])
        attribution = _ratio(counts["speaker_correct"], field_total)
        speaker_role = _ratio(counts["speaker_role_correct"], field_total)
        issue = _ratio(counts["issue_correct"], field_total)
        stance = _ratio(counts["stance_correct"], field_total)
        atomicity = _ratio(counts["atomic_one_to_one_matched"], counts["diagnostic_pairs"])
        density_ratio = (
            min(counts["gold_events"], counts["predicted_events"])
            / max(counts["gold_events"], counts["predicted_events"])
            if counts["gold_events"] or counts["predicted_events"]
            else 1.0
        )
        contamination = _ratio(counts["contaminants"], counts["predicted_events"]) if counts["predicted_events"] else 0.0
        components = (event_f1, attribution, speaker_role, issue, stance, atomicity)
        return {
            "macro_composite": sum(components) / len(components),
            "event_recall": recall,
            "event_precision": precision,
            "attribution": attribution,
            "speaker_role": speaker_role,
            "issue": issue,
            "stance": stance,
            "atomicity": atomicity,
            "density_ratio": density_ratio,
            "contamination": contamination,
            "schema_validity": _ratio(counts["schema_valid"], counts["windows"]),
            "evidence_grounding": _ratio(
                counts["evidence_grounded_events"], counts["predicted_events"]
            ),
        }

    aggregate_metrics = metrics(totals)
    return {
        "schema_version": EVALUATION_VERSION,
        "counts": dict(totals),
        "metrics": {key: round(value, 6) for key, value in aggregate_metrics.items()},
        "strata": {
            name: {
                "counts": dict(counts),
                "metrics": {key: round(value, 6) for key, value in metrics(counts).items()},
            }
            for name, counts in sorted(per_stratum.items())
        },
        "per_window_scores": per_window_scores,
    }


def artifact_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
