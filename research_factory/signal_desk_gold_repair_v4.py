"""V4: restore V3 quarantines from explicit episode speaker provenance only.

The sole new input is a completed episode-context ``speaker_map_json`` bound to
the same frozen episode and transcript.  A quarantined event is restored only
when exactly one high-confidence primary speaker covers its window and that
speaker's surface name occurs in the frozen transcript.  No claim, evidence,
issue, stance, quoted-person, or mentioned-person field may change.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_attribution_gate import audit_events
from .signal_desk_gold_repair_v3 import (
    MUTABLE_ATTRIBUTION_FIELDS, STRICT_STRUCTURES, _canonical, _sha_text,
)
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_evaluation import evaluate_windows


SCHEMA_VERSION = "pif_signal_desk_gold_repair_v4"
VARIANT_ID = "gold-c-explicit-episode-speaker-map-v4"
PARENT_VARIANT_ID = "gold-c-structured-speaker-projection-v3"
FAMILY_ID = "signal-desk-gold-attribution-projection"
MIN_CONFIDENCE = 0.98
PRIMARY_ROLE_TOKENS = ("primary_speaker", "host_author")


def _sha_json(value: Any) -> str:
    return _sha_text(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _context_rows(
    connection: sqlite3.Connection, *, episode_id: str, transcript_id: str,
) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT id, episode_id, transcript_id, model, status, speaker_map_json,
               completed_at, updated_at
          FROM episode_context_runs
         WHERE episode_id = ? AND transcript_id = ? AND status = 'completed'
         ORDER BY COALESCE(completed_at, updated_at) DESC, id DESC
        """,
        (episode_id, transcript_id),
    ).fetchall()
    return [dict(row) for row in rows]


def explicit_primary_speaker(
    *, context_rows: Sequence[Mapping[str, Any]], transcript: str,
    window_index: int,
) -> dict[str, Any] | None:
    """Return one source-supported primary speaker, never a best-effort guess."""

    if not context_rows:
        return None
    latest = context_rows[0]
    raw = json.loads(str(latest.get("speaker_map_json") or "[]"))
    eligible = []
    lowered = _canonical(transcript)
    for row in raw if isinstance(raw, list) else []:
        if not isinstance(row, Mapping):
            continue
        role = _canonical(row.get("role"))
        confidence = float(row.get("confidence") or 0.0)
        name = " ".join(str(row.get("name") or "").split())
        segments = row.get("segments") or []
        if (
            confidence >= MIN_CONFIDENCE
            and any(token in role for token in PRIMARY_ROLE_TOKENS)
            and int(window_index) in {int(value) for value in segments}
            and _canonical(name) in lowered
        ):
            eligible.append({
                "speaker_id": name,
                "confidence": confidence,
                "context_run_id": str(latest["id"]),
                "context_model": str(latest.get("model") or ""),
                "speaker_map_sha256": _sha_text(str(latest.get("speaker_map_json") or "[]")),
            })
    unique = {_canonical(row["speaker_id"]): row for row in eligible}
    return next(iter(unique.values())) if len(unique) == 1 else None


def restore_window(
    *, metadata: Mapping[str, Any], transcript: str,
    baseline_c: Mapping[str, Any], v3_c: Mapping[str, Any],
    v3_quarantine: Mapping[str, Any], context_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    structure = str(metadata.get("transcript_structure") or "unknown")
    speaker = explicit_primary_speaker(
        context_rows=context_rows, transcript=transcript,
        window_index=int(metadata.get("window_index") or 0),
    ) if structure in STRICT_STRUCTURES else None
    quarantined_ids = {
        str(row["event_id"]) for row in v3_quarantine.get("quarantined") or []
    }
    current = {str(event["event_id"]): json.loads(json.dumps(event)) for event in v3_c.get("events") or []}
    restored_ids: list[str] = []
    still_quarantined: list[dict[str, Any]] = []
    baseline_events = list(baseline_c.get("events") or [])
    for index, original in enumerate(baseline_events):
        event_id = str(original["event_id"])
        if event_id not in quarantined_ids:
            continue
        referenced = {
            _canonical(original.get("quoted_person_id")),
            *(_canonical(value) for value in (original.get("mentioned_person_ids") or [])),
        }
        if speaker and _canonical(speaker["speaker_id"]) not in referenced:
            event = json.loads(json.dumps(original))
            event["speaker_id"] = speaker["speaker_id"]
            if event.get("attribution_type") == "unresolved_speaker":
                event["attribution_type"] = "direct_speech"
            event["attribution_confidence"] = speaker["confidence"]
            current[event_id] = event
            restored_ids.append(event_id)
        else:
            still_quarantined.append({
                "event_id": event_id,
                "event_index": index,
                "reason": "no_unique_explicit_episode_primary_speaker",
                "event_sha256": _sha_json(original),
            })
    order = {str(event["event_id"]): index for index, event in enumerate(baseline_events)}
    output = json.loads(json.dumps(v3_c))
    output["events"] = sorted(current.values(), key=lambda event: order[str(event["event_id"])])
    if output["events"]:
        output["window_disposition"] = baseline_c["window_disposition"]
    start, end = int(metadata["start_char"]), int(metadata["end_char"])
    validate_output(
        output, transcript_window=transcript[start:end],
        expected_window_id=str(metadata["window_id"]),
    )
    receipt = {
        "schema_version": "pif_signal_desk_gold_v4_quarantine_v1",
        "window_id": str(metadata["window_id"]),
        "restored_event_count": len(restored_ids),
        "restored_event_ids_sha256": _sha_json(sorted(restored_ids)),
        "quarantined_event_count": len(still_quarantined),
        "quarantined": still_quarantined,
        "provenance": None if not speaker else {
            "context_run_id": speaker["context_run_id"],
            "context_model": speaker["context_model"],
            "speaker_map_sha256": speaker["speaker_map_sha256"],
            "full_transcript_surface_supported": True,
        },
        "contains_claim_evidence_or_speaker_text": False,
    }
    receipt["receipt_sha256"] = _sha_json(receipt)
    return output, receipt


def build_plan(
    *, manifest_path: Path, baseline_root: Path, v3_root: Path,
    database_path: Path, output_root: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    v3_plan = json.loads((v3_root / "repair-plan.json").read_text())
    selected = [str(value) for value in v3_plan["selected_window_ids"]]
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    context_bindings = []
    connection = sqlite3.connect(database_path)
    try:
        for window_id in selected:
            row = metadata[window_id]
            for context in _context_rows(
                connection, episode_id=str(row["episode_id"]),
                transcript_id=str(row["transcript_id"]),
            )[:1]:
                context_bindings.append({
                    "window_id": window_id,
                    "context_run_id": context["id"],
                    "speaker_map_sha256": _sha_text(str(context["speaker_map_json"])),
                })
    finally:
        connection.close()
    plan = {
        "schema_version": SCHEMA_VERSION,
        "variant_id": VARIANT_ID,
        "parent_variant_id": PARENT_VARIANT_ID,
        "family_id": FAMILY_ID,
        "changed_dimension": "explicit_episode_speaker_map_provenance",
        "eligible_splits": ["development"],
        "selected_window_ids": selected,
        "selected_window_ids_sha256": _sha_json(selected),
        "selected_window_count": len(selected),
        "manifest_sha256": manifest["manifest_sha256"],
        "v3_plan_sha256": v3_plan["plan_sha256"],
        "v3_c_digest": _digest_outputs(v3_root / "C", selected),
        "baseline_c_digest": _digest_outputs(baseline_root / "C", selected),
        "context_bindings_digest": _sha_json(context_bindings),
        "context_binding_count": len(context_bindings),
        "expected_calls": {"gpt_5_5_wider_context": 0, "total": 0},
        "provider_calls_started": False,
        "one_change_invariant": {
            "semantic_fields_frozen": True,
            "only_new_input": "completed_explicit_episode_speaker_map",
            "minimum_context_confidence": MIN_CONFIDENCE,
            "full_transcript_surface_support_required": True,
            "ambiguous_events_remain_quarantined": True,
            "sealed_items_opened": False,
        },
        "pass_criteria": {
            "structured_missing_speakers": 0,
            "fabricated_or_unsupported_attribution": 0,
            "third_party_presented_as_own": 0,
            "evidence_grounding": 1.0,
            "event_recall_minimum": 0.99,
            "macro_composite_minimum": 0.93,
            "macro_delta_vs_baseline_minimum": -0.02,
            "semantic_field_drift": 0,
            "remaining_quarantine_maximum": 1,
        },
        "output_root": str(output_root),
    }
    plan["plan_sha256"] = _sha_json(plan)
    return plan


def _digest_outputs(root: Path, window_ids: Sequence[str]) -> str:
    return _sha_json([
        {"window_id": window_id, "sha256": _sha_text((root / f"{window_id}.json").read_text())}
        for window_id in sorted(window_ids)
    ])


def execute(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    v3_root: Path, database_path: Path, output_root: Path,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    selected = [str(value) for value in plan["selected_window_ids"]]
    if _digest_outputs(v3_root / "C", selected) != plan["v3_c_digest"]:
        raise RuntimeError("V3 projection changed after V4 planning")
    if _digest_outputs(baseline_root / "C", selected) != plan["baseline_c_digest"]:
        raise RuntimeError("baseline C changed after V4 planning")
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    output_root.joinpath("C").mkdir(parents=True, exist_ok=True)
    output_root.joinpath("quarantine").mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    totals = {"windows": 0, "events_restored": 0, "events_quarantined": 0}
    bindings = []
    try:
        for window_id in selected:
            row = metadata[window_id]
            path = Path(str(row["transcript_path"]))
            if not path.is_absolute(): path = project_root / path
            transcript = path.read_text(encoding="utf-8")
            contexts = _context_rows(
                connection, episode_id=str(row["episode_id"]),
                transcript_id=str(row["transcript_id"]),
            )
            if contexts:
                bindings.append({"window_id": window_id, "context_run_id": contexts[0]["id"],
                                 "speaker_map_sha256": _sha_text(str(contexts[0]["speaker_map_json"]))})
            baseline = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
            v3 = json.loads((v3_root / "C" / f"{window_id}.json").read_text())
            quarantine = json.loads((v3_root / "quarantine" / f"{window_id}.json").read_text())
            output, receipt = restore_window(
                metadata=row, transcript=transcript, baseline_c=baseline,
                v3_c=v3, v3_quarantine=quarantine, context_rows=contexts,
            )
            (output_root / "C" / f"{window_id}.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
            (output_root / "quarantine" / f"{window_id}.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
            totals["windows"] += 1
            totals["events_restored"] += int(receipt["restored_event_count"])
            totals["events_quarantined"] += int(receipt["quarantined_event_count"])
    finally:
        connection.close()
    if _sha_json(bindings) != plan["context_bindings_digest"]:
        raise RuntimeError("episode speaker-map bindings changed after V4 planning")
    return {"schema_version": "pif_signal_desk_gold_repair_v4_run_v1",
            "variant_id": VARIANT_ID, "provider_calls_started": 0,
            "complete": totals["windows"] == len(selected), "totals": totals,
            "plan_sha256": plan["plan_sha256"]}


def compare(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    truth_root: Path, variant_root: Path, window_ids: Sequence[str],
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    rows = []
    audit_rows = []
    semantic_drift = 0
    for window_id in sorted(window_ids):
        row = metadata[window_id]
        path = Path(str(row["transcript_path"])); path = path if path.is_absolute() else project_root / path
        full_text = path.read_text(encoding="utf-8")
        predicted = json.loads((variant_root / "C" / f"{window_id}.json").read_text())
        truth = json.loads((truth_root / "C" / f"{window_id}.json").read_text())
        baseline = {str(e["event_id"]): e for e in json.loads((baseline_root / "C" / f"{window_id}.json").read_text())["events"]}
        for event in predicted["events"]:
            original = baseline[str(event["event_id"])]
            semantic_drift += sum(
                1 for key in set(event) | set(original)
                if key not in MUTABLE_ATTRIBUTION_FIELDS and event.get(key) != original.get(key)
            )
        rows.append({"window_id": window_id, "show_id": row["show_id"],
                     "episode_id": row["episode_id"], "transcript_structure": row["transcript_structure"],
                     "gold": truth, "predicted": predicted})
        # V4's evidence scope is the same frozen transcript, widened from the
        # 6k window solely for identity support.  No transcript text is emitted.
        audit_rows.append({"metadata": row, "text": full_text, "events": predicted["events"]})
    evaluation = evaluate_windows(rows)
    baseline_evaluation = evaluate_windows([
        {
            **row,
            "predicted": json.loads(
                (baseline_root / "C" / f"{row['window_id']}.json").read_text()
            ),
        }
        for row in rows
    ])
    audit = audit_events(windows=audit_rows, source_name="gold_c_v4_full_transcript_identity_scope")
    remaining = sum(json.loads(path.read_text())["quarantined_event_count"]
                    for path in (variant_root / "quarantine").glob("*.json"))
    metrics = evaluation["metrics"]
    baseline_metrics = baseline_evaluation["metrics"]
    macro_delta = metrics["macro_composite"] - baseline_metrics["macro_composite"]
    totals = audit["totals"]
    criteria = {
        "attribution_gate": audit["passed"],
        "structured_missing_speakers_zero": (
            int(totals.get("missing_speaker_assignment", 0))
            + int(totals.get("missing_supported_speaker", 0)) == 0
        ),
        "fabricated_attribution_zero": int(totals.get("fabricated_or_unsupported_attribution", 0)) == 0,
        "third_party_as_own_zero": int(totals.get("third_party_presented_as_own", 0)) == 0,
        "evidence_grounding": metrics["evidence_grounding"] == 1.0,
        "event_recall": metrics["event_recall"] >= 0.99,
        "macro_composite": metrics["macro_composite"] >= 0.93,
        "macro_delta_vs_baseline": macro_delta >= -0.02,
        "semantic_field_drift": semantic_drift == 0,
        "remaining_quarantine": remaining <= 1,
    }
    receipt = {"schema_version": "pif_signal_desk_gold_repair_v4_comparison_v1",
               "variant_id": VARIANT_ID, "windows": len(rows), "metrics": metrics,
               "baseline_metrics": baseline_metrics,
               "macro_delta_vs_baseline": macro_delta,
               "attribution_gate": {"passed": audit["passed"], "totals": audit["totals"],
                                    "context_scope": "full_frozen_transcript_identity_only"},
               "semantic_field_drift": semantic_drift, "remaining_quarantined_events": remaining,
               "pass_criteria": criteria, "passed": all(criteria.values()),
               "development_only": True, "sealed_items_opened": False}
    receipt["receipt_sha256"] = _sha_json(receipt)
    return receipt
