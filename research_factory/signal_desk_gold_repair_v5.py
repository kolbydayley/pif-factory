"""Full-development provenance recovery for V3 structured quarantines.

V5 applies exactly one new capability to the immutable V3 projection:
source-wide speaker provenance.  It accepts either the V4 explicit-primary
episode context rule or the nearest preceding roster-bound transcript label.
Everything else remains quarantined and is enumerated for a separate, explicit
GPT-5.5 wider-context lane.  This module never calls a provider.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_attribution_gate import audit_events
from .signal_desk_gold_repair_v3 import (
    MUTABLE_ATTRIBUTION_FIELDS, STRICT_STRUCTURES, _canonical, _sha_text,
)
from .signal_desk_gold_repair_v4 import (
    _context_rows, _digest_outputs, _sha_json, restore_window,
)
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_evaluation import evaluate_windows


SCHEMA_VERSION = "pif_signal_desk_gold_repair_v5"
VARIANT_ID = "gold-c-full-source-provenance-v5"
PARENT_VARIANT_ID = "gold-c-structured-speaker-projection-v3"
FAMILY_ID = "signal-desk-gold-attribution-projection"
GENERIC_SOURCE_LABELS = frozenset({
    "army film clip", "archival tape", "clip", "speaker 1", "speaker 2",
    "unknown speaker", "narrator", "advertisement", "sponsor",
})
PARTICIPANT_ROLE_TOKENS = (
    "host", "guest", "speaker", "interviewer", "interviewee", "primary",
)
EXCLUDED_ROLE_TOKENS = ("reported", "quoted", "external", "historical", "company", "organization")


def _participant_roster(
    *, outputs: Sequence[Mapping[str, Any]], context_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    names: dict[str, str] = {}
    for output in outputs:
        for event in output.get("events") or []:
            name = " ".join(str(event.get("speaker_id") or "").split())
            if name and _canonical(name) not in GENERIC_SOURCE_LABELS:
                names.setdefault(_canonical(name), name)
    if context_rows:
        raw = json.loads(str(context_rows[0].get("speaker_map_json") or "[]"))
        for row in raw if isinstance(raw, list) else []:
            if not isinstance(row, Mapping):
                continue
            role = _canonical(row.get("role"))
            name = " ".join(str(row.get("name") or "").split())
            if (
                name and any(token in role for token in PARTICIPANT_ROLE_TOKENS)
                and not any(token in role for token in EXCLUDED_ROLE_TOKENS)
                and _canonical(name) not in GENERIC_SOURCE_LABELS
            ):
                names.setdefault(_canonical(name), name)
    return sorted(names.values(), key=lambda value: (-len(value), _canonical(value)))


def _source_markers(transcript: str, roster: Sequence[str]) -> list[tuple[int, str]]:
    markers: list[tuple[int, str]] = []
    for speaker in roster:
        escaped = re.escape(speaker)
        patterns = (
            re.compile(r"(?mi)^\s*" + escaped + r"\s*$"),
            re.compile(r"(?i)(?<![A-Za-z0-9_])" + escaped + r"\s*:\s*"),
        )
        for pattern in patterns:
            markers.extend((match.start(), speaker) for match in pattern.finditer(transcript))
    return sorted(set(markers), key=lambda row: (row[0], _canonical(row[1])))


def _colon_label_barriers(transcript: str) -> list[tuple[int, str]]:
    """Capture intervening source labels, including generic archival clips.

    A participant marker may precede a quoted tape or film clip.  Assigning
    the participant across that generic label is exactly the third-party-as-own
    failure this repair must prevent.
    """

    pattern = re.compile(
        r"(?m)(?:^|\n)\s*([A-Za-z][A-Za-z0-9 .,'’&()/-]{1,80})\s*:\s*"
    )
    return [(match.start(1), " ".join(match.group(1).split())) for match in pattern.finditer(transcript)]


def _marker_speaker(
    *, event: Mapping[str, Any], window_start: int,
    markers: Sequence[tuple[int, str]], barriers: Sequence[tuple[int, str]],
) -> str | None:
    position = int(window_start) + int(event.get("evidence_start") or 0)
    preceding = [row for row in markers if row[0] <= position]
    if not preceding:
        return None
    marker_position, candidate = preceding[-1]
    intervening = [row for row in barriers if marker_position < row[0] <= position]
    if intervening and _canonical(intervening[-1][1]) != _canonical(candidate):
        return None
    referenced = {
        _canonical(event.get("quoted_person_id")),
        *(_canonical(value) for value in (event.get("mentioned_person_ids") or [])),
    }
    return None if _canonical(candidate) in referenced else candidate


def recover_window(
    *, metadata: Mapping[str, Any], transcript: str,
    gold_a: Mapping[str, Any], gold_b: Mapping[str, Any],
    baseline_c: Mapping[str, Any], v3_c: Mapping[str, Any],
    v3_quarantine: Mapping[str, Any], context_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Restore explicit-primary then source-label-supported events."""

    explicit_output, explicit_receipt = restore_window(
        metadata=metadata, transcript=transcript, baseline_c=baseline_c,
        v3_c=v3_c, v3_quarantine=v3_quarantine, context_rows=context_rows,
    )
    remaining_ids = {str(row["event_id"]) for row in explicit_receipt["quarantined"]}
    events = {str(event["event_id"]): json.loads(json.dumps(event)) for event in explicit_output["events"]}
    baseline_events = list(baseline_c.get("events") or [])
    roster = _participant_roster(outputs=(gold_a, gold_b, baseline_c), context_rows=context_rows)
    markers = _source_markers(transcript, roster)
    barriers = _colon_label_barriers(transcript)
    surface_restored: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for index, original in enumerate(baseline_events):
        event_id = str(original["event_id"])
        if event_id not in remaining_ids:
            continue
        speaker = _marker_speaker(
            event=original, window_start=int(metadata["start_char"]), markers=markers,
            barriers=barriers,
        )
        if speaker:
            event = json.loads(json.dumps(original))
            event["speaker_id"] = speaker
            if event.get("attribution_type") == "unresolved_speaker":
                event["attribution_type"] = "direct_speech"
            event["attribution_confidence"] = 1.0
            events[event_id] = event
            surface_restored.append(event_id)
        else:
            reason = (
                "no_participant_roster" if not roster
                else "no_preceding_roster_bound_source_label"
            )
            unresolved.append({
                "event_id": event_id, "event_index": index, "reason": reason,
                "event_sha256": _sha_json(original),
            })
    order = {str(event["event_id"]): index for index, event in enumerate(baseline_events)}
    output = json.loads(json.dumps(explicit_output))
    output["events"] = sorted(events.values(), key=lambda event: order[str(event["event_id"])])
    if output["events"]:
        output["window_disposition"] = baseline_c["window_disposition"]
    start, end = int(metadata["start_char"]), int(metadata["end_char"])
    validate_output(
        output, transcript_window=transcript[start:end],
        expected_window_id=str(metadata["window_id"]),
    )
    reasons = Counter(row["reason"] for row in unresolved)
    receipt = {
        "schema_version": "pif_signal_desk_gold_v5_provenance_v1",
        "window_id": str(metadata["window_id"]),
        "explicit_episode_context_restored": int(explicit_receipt["restored_event_count"]),
        "source_label_restored": len(surface_restored),
        "source_label_restored_ids_sha256": _sha_json(sorted(surface_restored)),
        "unresolved_event_count": len(unresolved),
        "unresolved_reason_counts": dict(sorted(reasons.items())),
        "unresolved": unresolved,
        "context_provenance": explicit_receipt.get("provenance"),
        "participant_roster_sha256": _sha_json(roster),
        "source_marker_count": len(markers),
        "contains_claim_evidence_or_speaker_text": False,
    }
    receipt["receipt_sha256"] = _sha_json(receipt)
    return output, receipt


def _context_bindings(
    *, connection: sqlite3.Connection, metadata: Mapping[str, Mapping[str, Any]],
    window_ids: Sequence[str],
) -> list[dict[str, str]]:
    result = []
    for window_id in window_ids:
        row = metadata[window_id]
        contexts = _context_rows(
            connection, episode_id=str(row["episode_id"]),
            transcript_id=str(row["transcript_id"]),
        )
        if contexts:
            result.append({
                "window_id": window_id, "context_run_id": str(contexts[0]["id"]),
                "speaker_map_sha256": _sha_text(str(contexts[0]["speaker_map_json"])),
            })
    return result


def build_plan(
    *, manifest_path: Path, baseline_root: Path, v3_root: Path,
    database_path: Path, output_root: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    v3_plan = json.loads((v3_root / "repair-plan.json").read_text())
    selected = [str(value) for value in v3_plan["selected_window_ids"]]
    if len(selected) != 189:
        raise ValueError(f"V5 requires all 189 development windows, found {len(selected)}")
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    connection = sqlite3.connect(database_path)
    try:
        bindings = _context_bindings(connection=connection, metadata=metadata, window_ids=selected)
    finally:
        connection.close()
    plan = {
        "schema_version": SCHEMA_VERSION, "variant_id": VARIANT_ID,
        "parent_variant_id": PARENT_VARIANT_ID, "family_id": FAMILY_ID,
        "changed_dimension": "full_transcript_speaker_provenance",
        "eligible_splits": ["development"], "selected_window_count": len(selected),
        "selected_window_ids": selected, "selected_window_ids_sha256": _sha_json(selected),
        "manifest_sha256": manifest["manifest_sha256"],
        "v3_plan_sha256": v3_plan["plan_sha256"],
        "v3_c_digest": _digest_outputs(v3_root / "C", selected),
        "baseline_abc_digest": _sha_json({
            turn: _digest_outputs(baseline_root / turn, selected) for turn in ("A", "B", "C")
        }),
        "context_bindings_digest": _sha_json(bindings),
        "context_binding_count": len(bindings),
        "expected_calls": {"deterministic": 0, "gpt_5_5_wider_context_planned": 9, "started": 0},
        "provider_calls_started": False,
        "one_change_invariant": {
            "semantic_fields_frozen": True,
            "new_capability": "source_wide_roster_bound_speaker_provenance",
            "generic_or_ambiguous_labels_rejected": True,
            "unresolved_events_remain_quarantined": True,
            "sealed_items_opened": False,
        },
        "pass_criteria": {
            "structured_missing_speakers": 0,
            "fabricated_or_unsupported_attribution": 0,
            "third_party_presented_as_own": 0,
            "evidence_grounding": 1.0,
            "event_recall_minimum": 0.99,
            "macro_delta_vs_baseline_minimum": -0.02,
            "semantic_field_drift": 0,
        },
        "output_root": str(output_root),
    }
    plan["plan_sha256"] = _sha_json(plan)
    return plan


def execute(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    v3_root: Path, database_path: Path, output_root: Path,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    selected = [str(value) for value in plan["selected_window_ids"]]
    if _digest_outputs(v3_root / "C", selected) != plan["v3_c_digest"]:
        raise RuntimeError("V3 full projection changed after planning")
    current_abc = _sha_json({turn: _digest_outputs(baseline_root / turn, selected) for turn in ("A", "B", "C")})
    if current_abc != plan["baseline_abc_digest"]:
        raise RuntimeError("frozen baseline A/B/C changed after planning")
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    output_root.joinpath("C").mkdir(parents=True, exist_ok=True)
    output_root.joinpath("provenance").mkdir(parents=True, exist_ok=True)
    totals = Counter()
    unresolved_windows = []
    connection = sqlite3.connect(database_path)
    try:
        bindings = _context_bindings(connection=connection, metadata=metadata, window_ids=selected)
        if _sha_json(bindings) != plan["context_bindings_digest"]:
            raise RuntimeError("episode-context bindings changed after planning")
        for window_id in selected:
            row = metadata[window_id]
            path = Path(str(row["transcript_path"])); path = path if path.is_absolute() else project_root / path
            transcript = path.read_text(encoding="utf-8")
            contexts = _context_rows(connection, episode_id=str(row["episode_id"]), transcript_id=str(row["transcript_id"]))
            a = json.loads((baseline_root / "A" / f"{window_id}.json").read_text())
            b = json.loads((baseline_root / "B" / f"{window_id}.json").read_text())
            c = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
            v3 = json.loads((v3_root / "C" / f"{window_id}.json").read_text())
            q3 = json.loads((v3_root / "quarantine" / f"{window_id}.json").read_text())
            output, receipt = recover_window(
                metadata=row, transcript=transcript, gold_a=a, gold_b=b,
                baseline_c=c, v3_c=v3, v3_quarantine=q3, context_rows=contexts,
            )
            (output_root / "C" / f"{window_id}.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
            (output_root / "provenance" / f"{window_id}.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
            totals["windows"] += 1
            totals["explicit_episode_context_restored"] += receipt["explicit_episode_context_restored"]
            totals["source_label_restored"] += receipt["source_label_restored"]
            totals["unresolved_events"] += receipt["unresolved_event_count"]
            if receipt["unresolved_event_count"]:
                unresolved_windows.append({
                    "window_id": window_id, "show_id": str(row["show_id"]),
                    "transcript_structure": str(row["transcript_structure"]),
                    "unresolved_event_count": receipt["unresolved_event_count"],
                    "reason_counts": receipt["unresolved_reason_counts"],
                })
    finally:
        connection.close()
    wider = {
        "schema_version": "pif_signal_desk_gold_v5_wider_context_plan_v1",
        "model": "gpt-5.5", "action": "request_wider_context",
        "provider_calls_started": False, "expected_calls": len(unresolved_windows),
        "packet_limit": {"windows_per_call": 1, "max_input_tokens": 12000},
        "windows": unresolved_windows,
        "input_content_in_receipt": False,
        "instruction": "Review full segment plus/minus 3 turns; return only evidence-backed speaker patches or reject.",
    }
    wider["plan_sha256"] = _sha_json(wider)
    (output_root / "wider-context-plan.json").write_text(json.dumps(wider, indent=2, sort_keys=True) + "\n")
    receipt = {"schema_version": "pif_signal_desk_gold_repair_v5_run_v1",
               "variant_id": VARIANT_ID, "provider_calls_started": 0,
               "complete": totals["windows"] == len(selected), "totals": dict(totals),
               "wider_context_expected_calls": len(unresolved_windows), "plan_sha256": plan["plan_sha256"]}
    receipt["receipt_sha256"] = _sha_json(receipt)
    return receipt


def compare(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    truth_root: Path, variant_root: Path, window_ids: Sequence[str],
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    rows, audits = [], []
    semantic_drift = 0
    for window_id in sorted(window_ids):
        row = metadata[window_id]
        path = Path(str(row["transcript_path"])); path = path if path.is_absolute() else project_root / path
        transcript = path.read_text(encoding="utf-8")
        predicted = json.loads((variant_root / "C" / f"{window_id}.json").read_text())
        truth = json.loads((truth_root / "C" / f"{window_id}.json").read_text())
        baseline_payload = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        baseline = {str(event["event_id"]): event for event in baseline_payload["events"]}
        for event in predicted["events"]:
            original = baseline[str(event["event_id"])]
            semantic_drift += sum(1 for key in set(event) | set(original)
                                  if key not in MUTABLE_ATTRIBUTION_FIELDS and event.get(key) != original.get(key))
        rows.append({"window_id": window_id, "show_id": row["show_id"], "episode_id": row["episode_id"],
                     "transcript_structure": row["transcript_structure"], "gold": truth, "predicted": predicted})
        audits.append({"metadata": row, "text": transcript, "events": predicted["events"]})
    evaluation = evaluate_windows(rows)
    baseline_evaluation = evaluate_windows([{**row, "predicted": json.loads((baseline_root / "C" / f"{row['window_id']}.json").read_text())} for row in rows])
    audit = audit_events(windows=audits, source_name="gold_c_v5_full_transcript_identity_scope")
    unresolved = sum(json.loads(path.read_text())["unresolved_event_count"] for path in (variant_root / "provenance").glob("*.json"))
    metrics, base = evaluation["metrics"], baseline_evaluation["metrics"]
    delta = metrics["macro_composite"] - base["macro_composite"]
    totals = audit["totals"]
    gates = {
        "full_attribution": audit["passed"],
        "recall_at_least_0_99": metrics["event_recall"] >= 0.99,
        "grounding_exact": metrics["evidence_grounding"] == 1.0,
        "fabricated_attribution_zero": int(totals.get("fabricated_or_unsupported_attribution", 0)) == 0,
        "third_party_as_own_zero": int(totals.get("third_party_presented_as_own", 0)) == 0,
        "macro_tolerance": delta >= -0.02,
        "semantic_field_drift_zero": semantic_drift == 0,
    }
    receipt = {"schema_version": "pif_signal_desk_gold_repair_v5_comparison_v1",
               "variant_id": VARIANT_ID, "windows": len(rows), "metrics": metrics,
               "baseline_metrics": base, "macro_delta_vs_baseline": delta,
               "attribution_gate": {"passed": audit["passed"], "totals": totals,
                                    "context_scope": "full_frozen_transcript_identity_only"},
               "semantic_field_drift": semantic_drift, "unresolved_quarantined_events": unresolved,
               "gates": gates, "passed": all(gates.values()),
               "development_only": True, "sealed_items_opened": False}
    receipt["receipt_sha256"] = _sha_json(receipt)
    return receipt
