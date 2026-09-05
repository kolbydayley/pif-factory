"""Deterministic, speaker-only Gold-C repair for structured transcripts.

V2 asked Gold C to adjudicate the entire event set after adding a speaker-map
header.  That let a speaker experiment rewrite claims, issues, and stances, and
its colon-only parser produced empty maps for the actual frozen formats.  V3
instead freezes every baseline semantic field.  It may only attach a speaker
proved by an evidence-local transcript label, or remove the unresolved event
from the publishable Gold-C view and record a content-free quarantine receipt.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_attribution_gate import audit_gold_c
from .signal_desk_gold_repair_v2 import (
    FULL_WINDOW_COUNT,
    PILOT_EVENT_FLOOR,
    baseline_ab_digest,
    development_windows,
    select_powered_pilot_windows,
)
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_evaluation import evaluate_windows
from .signal_desk_rebuild_gold import verify_frozen_manifest


SCHEMA_VERSION = "pif_signal_desk_gold_repair_v3"
VARIANT_ID = "gold-c-structured-speaker-projection-v3"
PARENT_VARIANT_ID = "gold-authoring-baseline-v2"
FAMILY_ID = "signal-desk-gold-attribution-projection"
STRICT_STRUCTURES = frozenset({"speaker_turn", "paragraph"})
CONSEQUENTIAL_ACTS = frozenset({
    "assertion", "forecast", "explanation", "recommendation",
    "commitment", "disagreement", "reported_position",
})
MUTABLE_ATTRIBUTION_FIELDS = frozenset({
    "speaker_id", "attribution_type", "attribution_confidence",
})
WIDE_CONTEXT_CHARS = 2_000


def _sha_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical(value: Any) -> str:
    return " ".join(str(value or "").casefold().replace("’", "'").split())


def _known_speakers(*outputs: Mapping[str, Any]) -> list[str]:
    values: dict[str, str] = {}
    for output in outputs:
        for event in output.get("events") or []:
            speaker = str(event.get("speaker_id") or "").strip()
            if speaker:
                values.setdefault(_canonical(speaker), speaker)
    return sorted(values.values(), key=lambda value: (-len(value), _canonical(value)))


def _speaker_markers(
    *, transcript: str, start_char: int, end_char: int, structure: str,
    speakers: Sequence[str],
) -> list[tuple[int, str]]:
    """Return absolute transcript offsets for exact, roster-bound labels.

    The roster comes only from frozen A/B/C outputs.  It prevents arbitrary
    colon-bearing page chrome from becoming a person.  Speaker-turn transcripts
    use bare-name lines; paragraph transcripts use inline ``Name:`` markers.
    Up to 2,000 preceding characters are included solely to recover a turn that
    crosses the frozen window boundary.
    """

    context_start = max(0, int(start_char) - WIDE_CONTEXT_CHARS)
    context = transcript[context_start:int(end_char)]
    markers: list[tuple[int, str]] = []
    for speaker in speakers:
        escaped = re.escape(speaker)
        if structure == "speaker_turn":
            pattern = re.compile(r"(?mi)^\s*" + escaped + r"\s*$")
        else:
            pattern = re.compile(r"(?i)(?<![A-Za-z0-9_])" + escaped + r"\s*:\s*")
        markers.extend((context_start + match.start(), speaker) for match in pattern.finditer(context))
    return sorted(set(markers), key=lambda row: (row[0], _canonical(row[1])))


def _active_speaker(
    *, event: Mapping[str, Any], start_char: int, markers: Sequence[tuple[int, str]],
) -> str | None:
    absolute = int(start_char) + int(event.get("evidence_start") or 0)
    preceding = [row for row in markers if row[0] <= absolute]
    if not preceding:
        return None
    candidate = preceding[-1][1]
    referenced = {
        _canonical(event.get("quoted_person_id")),
        *(_canonical(value) for value in (event.get("mentioned_person_ids") or [])),
    }
    # If the proposed speaker is also encoded as the quoted/discussed person,
    # role separation is ambiguous.  Quarantine instead of converting a
    # third-party mention into the person's own statement.
    return None if _canonical(candidate) in referenced else candidate


def project_window(
    *, metadata: Mapping[str, Any], transcript: str, gold_a: Mapping[str, Any],
    gold_b: Mapping[str, Any], baseline_c: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project a baseline C window without permitting semantic rewrites."""

    structure = str(metadata.get("transcript_structure") or "unknown")
    start_char, end_char = int(metadata["start_char"]), int(metadata["end_char"])
    window_text = transcript[start_char:end_char]
    speakers = _known_speakers(gold_a, gold_b, baseline_c)
    markers = _speaker_markers(
        transcript=transcript, start_char=start_char, end_char=end_char,
        structure=structure, speakers=speakers,
    ) if structure in STRICT_STRUCTURES else []
    projected = json.loads(json.dumps(baseline_c))
    kept: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    assigned = 0
    for index, original in enumerate(baseline_c.get("events") or []):
        event = json.loads(json.dumps(original))
        consequential = str(event.get("speech_act") or "") in CONSEQUENTIAL_ACTS
        if not consequential or structure not in STRICT_STRUCTURES or event.get("speaker_id"):
            kept.append(event)
            continue
        candidate = _active_speaker(event=event, start_char=start_char, markers=markers)
        if candidate:
            event["speaker_id"] = candidate
            if event.get("attribution_type") == "unresolved_speaker":
                event["attribution_type"] = "direct_speech"
            event["attribution_confidence"] = 1.0
            assigned += 1
            kept.append(event)
            continue
        quarantined.append({
            "event_id": str(event.get("event_id") or f"index-{index}"),
            "event_index": index,
            "reason": "structured_speaker_not_transcript_supported",
            "event_sha256": _sha_text(json.dumps(original, sort_keys=True, separators=(",", ":"))),
        })
    projected["events"] = kept
    if not kept:
        projected["window_disposition"] = "no_consequential_claims"
    validate_output(
        projected,
        transcript_window=window_text,
        expected_window_id=str(metadata["window_id"]),
    )
    receipt = {
        "schema_version": "pif_signal_desk_gold_v3_quarantine_v1",
        "window_id": str(metadata["window_id"]),
        "transcript_structure": structure,
        "baseline_event_count": len(baseline_c.get("events") or []),
        "retained_event_count": len(kept),
        "speaker_assignments_added": assigned,
        "quarantined_event_count": len(quarantined),
        "quarantined": quarantined,
        "contains_claim_or_evidence_text": False,
    }
    receipt["receipt_sha256"] = _sha_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return projected, receipt


def _output_digest(root: Path, turns: Sequence[str], window_ids: Sequence[str]) -> str:
    rows = []
    for turn in turns:
        for window_id in sorted(window_ids):
            path = root / turn / f"{window_id}.json"
            if not path.exists():
                raise ValueError(f"missing frozen {turn} output: {window_id}")
            rows.append({"turn": turn, "window_id": window_id, "sha256": _sha_text(path.read_text())})
    return _sha_text(json.dumps(rows, sort_keys=True, separators=(",", ":")))


def build_plan(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    output_root: Path, pilot: bool,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    verify_frozen_manifest(manifest)
    rows = development_windows(manifest)
    selected = (
        select_powered_pilot_windows(manifest=manifest, baseline_c_root=baseline_root / "C")
        if pilot else sorted(str(row["window_id"]) for row in rows)
    )
    plan = {
        "schema_version": SCHEMA_VERSION,
        "variant_id": VARIANT_ID,
        "parent_variant_id": PARENT_VARIANT_ID,
        "family_id": FAMILY_ID,
        "changed_dimension": "speaker_attribution_projection",
        "eligible_splits": ["development"],
        "manifest_sha256": manifest["manifest_sha256"],
        "selected_window_ids": selected,
        "selected_window_ids_sha256": _sha_text(json.dumps(selected, separators=(",", ":"))),
        "selected_window_count": len(selected),
        "pilot": pilot,
        "pilot_event_floor": PILOT_EVENT_FLOOR if pilot else None,
        "expected_calls": {"A": 0, "B": 0, "C": 0, "total": 0},
        "provider_calls_started": False,
        "baseline_ab_digest": baseline_ab_digest(baseline_root=baseline_root, window_ids=selected),
        "baseline_c_digest": _output_digest(baseline_root, ("C",), selected),
        "one_change_invariant": {
            "semantic_fields_frozen": True,
            "mutable_fields": sorted(MUTABLE_ATTRIBUTION_FIELDS),
            "unresolved_structured_events_quarantined": True,
            "frozen_baseline_ab_reused": True,
            "provider_calls": 0,
            "sealed_items_opened": False,
        },
        "output_root": str(output_root),
    }
    plan["plan_sha256"] = _sha_text(json.dumps(plan, sort_keys=True, separators=(",", ":")))
    return plan


def execute_projection(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    output_root: Path, plan: Mapping[str, Any],
) -> dict[str, Any]:
    selected = [str(value) for value in plan["selected_window_ids"]]
    if baseline_ab_digest(baseline_root=baseline_root, window_ids=selected) != plan["baseline_ab_digest"]:
        raise RuntimeError("frozen baseline A/B changed after planning")
    if _output_digest(baseline_root, ("C",), selected) != plan["baseline_c_digest"]:
        raise RuntimeError("frozen baseline C changed after planning")
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    totals = {"windows": 0, "speaker_assignments_added": 0, "quarantined_events": 0}
    for turn in ("A", "B"):
        (output_root / turn).mkdir(parents=True, exist_ok=True)
        for window_id in selected:
            shutil.copyfile(baseline_root / turn / f"{window_id}.json", output_root / turn / f"{window_id}.json")
    (output_root / "C").mkdir(parents=True, exist_ok=True)
    (output_root / "quarantine").mkdir(parents=True, exist_ok=True)
    for window_id in selected:
        row = metadata[window_id]
        transcript_path = Path(str(row["transcript_path"]))
        if not transcript_path.is_absolute():
            transcript_path = project_root / transcript_path
        transcript = transcript_path.read_text(encoding="utf-8")
        a = json.loads((baseline_root / "A" / f"{window_id}.json").read_text())
        b = json.loads((baseline_root / "B" / f"{window_id}.json").read_text())
        c = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        projected, quarantine = project_window(
            metadata=row, transcript=transcript, gold_a=a, gold_b=b, baseline_c=c,
        )
        (output_root / "C" / f"{window_id}.json").write_text(json.dumps(projected, indent=2, sort_keys=True) + "\n")
        (output_root / "quarantine" / f"{window_id}.json").write_text(json.dumps(quarantine, indent=2, sort_keys=True) + "\n")
        totals["windows"] += 1
        totals["speaker_assignments_added"] += int(quarantine["speaker_assignments_added"])
        totals["quarantined_events"] += int(quarantine["quarantined_event_count"])
    receipt = {
        "schema_version": "pif_signal_desk_gold_repair_v3_run_v1",
        "variant_id": VARIANT_ID,
        "provider_calls_started": 0,
        "complete": totals["windows"] == len(selected),
        "totals": totals,
        "plan_sha256": plan["plan_sha256"],
    }
    receipt["receipt_sha256"] = _sha_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return receipt


def _assert_semantic_freeze(
    *, baseline_root: Path, variant_root: Path, window_ids: Sequence[str],
) -> dict[str, int]:
    retained = removed = attribution_changes = 0
    for window_id in window_ids:
        baseline = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        variant = json.loads((variant_root / "C" / f"{window_id}.json").read_text())
        base_by_id = {str(event["event_id"]): event for event in baseline["events"]}
        for event in variant["events"]:
            original = base_by_id.pop(str(event["event_id"]))
            for key in set(original) | set(event):
                if key not in MUTABLE_ATTRIBUTION_FIELDS and original.get(key) != event.get(key):
                    raise RuntimeError(f"semantic drift in {window_id}:{event['event_id']}:{key}")
            if any(original.get(key) != event.get(key) for key in MUTABLE_ATTRIBUTION_FIELDS):
                attribution_changes += 1
            retained += 1
        removed += len(base_by_id)
    return {"retained_events": retained, "quarantined_events": removed, "attribution_changes": attribution_changes}


def compare(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    truth_root: Path, variant_root: Path, window_ids: Sequence[str],
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    rows = []
    for window_id in sorted(window_ids):
        truth = json.loads((truth_root / "C" / f"{window_id}.json").read_text())
        predicted = json.loads((variant_root / "C" / f"{window_id}.json").read_text())
        rows.append({
            "window_id": window_id, "show_id": metadata[window_id]["show_id"],
            "episode_id": metadata[window_id]["episode_id"],
            "transcript_structure": metadata[window_id]["transcript_structure"],
            "gold": truth, "predicted": predicted,
        })
    variant_eval = evaluate_windows(rows)
    baseline_eval = evaluate_windows([
        {**row, "predicted": json.loads((baseline_root / "C" / f"{row['window_id']}.json").read_text())}
        for row in rows
    ])
    gate = audit_gold_c(manifest_path=manifest_path, result_root=variant_root, project_root=project_root)
    return {
        "schema_version": "pif_signal_desk_gold_repair_v3_comparison_v1",
        "variant_id": VARIANT_ID,
        "development_only": True,
        "sealed_items_opened": False,
        "windows": len(rows),
        "semantic_freeze": _assert_semantic_freeze(
            baseline_root=baseline_root, variant_root=variant_root, window_ids=window_ids,
        ),
        "attribution_gate": {
            "passed": gate["passed"], "totals": gate["totals"],
            "by_transcript_structure": gate["by_transcript_structure"],
        },
        "variant_metrics": variant_eval["metrics"],
        "baseline_metrics": baseline_eval["metrics"],
        "delta": {
            key: variant_eval["metrics"][key] - baseline_eval["metrics"][key]
            for key in ("macro_composite", "event_precision", "event_recall", "attribution", "speaker_role", "stance")
        },
    }
