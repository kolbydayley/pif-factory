"""Plan and compare the isolated development Gold-C repair v2.

Unlike the failed v1 prompt variant, this family never reruns Gold A or B.
Those outputs are frozen seeds; only C is eligible for a provider call.  The
sole experimental change is a deterministic speaker-map representation header.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_attribution_gate import audit_gold_c
from .signal_desk_gold_representation import (
    FAMILY_ID,
    FAMILY_TYPE,
    REPRESENTATION_VARIANT_ID,
    build_speaker_map_header,
    headers_digest,
)
from .signal_desk_rebuild_evaluation import evaluate_windows
from .signal_desk_rebuild_gold import verify_frozen_manifest


SCHEMA_VERSION = "pif_signal_desk_gold_repair_v2"
MODEL = "gpt-5.6-sol"
EFFORT = "medium"
SCORER_VERSION = "scorer-v6"
PROMPT_VARIANT_ID = "gold-authoring-baseline-v2"
PILOT_WINDOW_COUNT = 40
PILOT_EVENT_FLOOR = 1_000
FULL_WINDOW_COUNT = 189
RESERVED_C_TOKENS = 57_000


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def development_windows(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = [dict(row) for row in manifest.get("windows", []) if row.get("split") == "development"]
    if len(rows) != FULL_WINDOW_COUNT:
        raise ValueError(f"expected {FULL_WINDOW_COUNT} development windows, found {len(rows)}")
    return rows


def _baseline_event_count(*, baseline_c_root: Path, window_id: str) -> int:
    path = baseline_c_root / f"{window_id}.json"
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    return sum(
        1 for event in payload.get("events", [])
        if isinstance(event, Mapping)
        and str(event.get("speech_act") or "") in {
            "assertion", "forecast", "explanation", "recommendation",
            "commitment", "disagreement", "reported_position",
        }
    )


def select_powered_pilot_windows(
    *, manifest: Mapping[str, Any], baseline_c_root: Path, count: int = PILOT_WINDOW_COUNT,
) -> list[str]:
    """Select one high-density window per show, then fill deterministically.

    The pilot is intentionally broad across shows and transcript structures,
    while the baseline event floor makes it useful for an aggregate comparison.
    It is a development-only planning aid, not a replacement for the 189-window
    run or any per-show release gate.
    """

    rows = development_windows(manifest)
    enriched = [
        {**row, "baseline_event_count": _baseline_event_count(
            baseline_c_root=baseline_c_root, window_id=str(row["window_id"])
        )}
        for row in rows
    ]
    chosen: list[dict[str, Any]] = []
    # First guarantee representation coverage, picking the densest row in each
    # structure.  Then guarantee show breadth with one row per show.
    for structure in ("speaker_turn", "paragraph", "flattened", "asr_diarized"):
        candidates = [row for row in enriched if row.get("transcript_structure") == structure]
        if candidates:
            chosen.append(max(candidates, key=lambda row: (row["baseline_event_count"], str(row["window_id"]))))
    remaining = [row for row in enriched if row not in chosen]
    remaining.sort(key=lambda row: (-int(row["baseline_event_count"]), str(row["window_id"])))
    chosen.extend(remaining[: max(0, count - len(chosen))])
    selected = sorted({str(row["window_id"]) for row in chosen[:count]})
    event_count = sum(_baseline_event_count(baseline_c_root=baseline_c_root, window_id=value) for value in selected)
    if len(selected) != count or event_count < PILOT_EVENT_FLOOR:
        raise ValueError(f"pilot selection is underpowered: windows={len(selected)} events={event_count}")
    return selected


def _baseline_ab_digest(*, baseline_root: Path, window_ids: Sequence[str]) -> str:
    rows = []
    for turn in ("A", "B"):
        for window_id in sorted(window_ids):
            path = baseline_root / turn / f"{window_id}.json"
            if not path.exists():
                raise ValueError(f"missing frozen baseline {turn} output: {window_id}")
            rows.append({"turn": turn, "window_id": window_id, "sha256": _sha(path.read_text(encoding="utf-8"))})
    return _sha(json.dumps(rows, sort_keys=True, separators=(",", ":")))


def baseline_ab_digest(*, baseline_root: Path, window_ids: Sequence[str]) -> str:
    """Public integrity check used immediately before a provider run."""

    return _baseline_ab_digest(baseline_root=baseline_root, window_ids=window_ids)


def build_repair_plan(
    *, manifest_path: Path, project_root: Path, baseline_root: Path, output_root: Path,
    pilot: bool = True,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    rows = development_windows(manifest)
    baseline_c_root = baseline_root / "C"
    selected = (
        select_powered_pilot_windows(manifest=manifest, baseline_c_root=baseline_c_root)
        if pilot else sorted(str(row["window_id"]) for row in rows)
    )
    selected_set = set(selected)
    headers = {}
    for row in rows:
        if str(row["window_id"]) not in selected_set:
            continue
        path = Path(str(row["transcript_path"]))
        if not path.is_absolute():
            path = project_root / path
        raw = path.read_text(encoding="utf-8")
        headers[str(row["window_id"])] = build_speaker_map_header(
            metadata=row,
            window_text=raw[int(row["start_char"]):int(row["end_char"])],
        )
    # The path expression above is resolved again by the executor; this plan
    # construction is deliberately local and fail-closed on stale bytes.
    entry = {
        "schema_version": SCHEMA_VERSION,
        "variant_id": REPRESENTATION_VARIANT_ID,
        "family_id": FAMILY_ID,
        "family_type": FAMILY_TYPE,
        "parent_variant_id": PROMPT_VARIANT_ID,
        "changed_dimension": "representation",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "scorer_version": SCORER_VERSION,
        "manifest_sha256": manifest["manifest_sha256"],
        "eligible_splits": ["development"],
        "selected_window_ids_sha256": _sha(json.dumps(selected, separators=(",", ":"))),
        "selected_window_count": len(selected),
        "expected_calls": {"A": 0, "B": 0, "C": len(selected), "total": len(selected)},
        "reserved_token_ceiling": len(selected) * RESERVED_C_TOKENS,
        "baseline_ab_digest": _baseline_ab_digest(baseline_root=baseline_root, window_ids=selected),
        "representation_headers_digest": headers_digest(headers),
        "one_change_invariant": {
            "changed": ["representation"],
            "prompt_wording_unchanged": True,
            "frozen_baseline_ab_reused": True,
            "schema_unchanged": True,
            "scorer_unchanged": True,
            "sealed_items_opened": False,
        },
        "pilot": {
            "powered": pilot,
            "event_floor": PILOT_EVENT_FLOOR if pilot else None,
            "baseline_event_count": sum(_baseline_event_count(baseline_c_root=baseline_c_root, window_id=value) for value in selected),
            "transcript_structures": dict(sorted(Counter(str(row["transcript_structure"]) for row in rows if str(row["window_id"]) in selected_set).items())),
        },
        "output_root": str(output_root),
        "provider_calls_started": False,
        "execute_command": (
            "python3 -B scripts/pif_signal_desk_gold_speaker_map_variant.py "
            + ("--pilot " if pilot else "--full ")
            + "--execute --concurrency 2"
        ),
    }
    entry["plan_sha256"] = _sha(json.dumps(entry, sort_keys=True, separators=(",", ":")))
    return entry


def compare_outputs(
    *, manifest_path: Path, baseline_root: Path, truth_root: Path, variant_root: Path,
    window_ids: Sequence[str], project_root: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    rows = []
    for window_id in sorted(window_ids):
        variant = variant_root / "C" / f"{window_id}.json"
        if not variant.exists():
            raise RuntimeError(f"variant C incomplete: {window_id}")
        base = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        truth = json.loads((truth_root / "C" / f"{window_id}.json").read_text())
        predicted = json.loads(variant.read_text())
        rows.append({"window_id": window_id, "show_id": metadata[window_id]["show_id"],
                     "episode_id": metadata[window_id]["episode_id"],
                     "transcript_structure": metadata[window_id]["transcript_structure"],
                     "gold": truth, "predicted": predicted})
    variant_eval = evaluate_windows(rows)
    baseline_eval = evaluate_windows([{**row, "predicted": json.loads((baseline_root / "C" / f"{row['window_id']}.json").read_text())} for row in rows])
    def compact(result: Mapping[str, Any]) -> dict[str, Any]:
        return {key: result.get(key) for key in ("evaluation_version", "metrics", "micro_metrics", "aggregation", "strata")}
    receipt = {
        "schema_version": "pif_signal_desk_gold_repair_v2_comparison",
        "variant_id": REPRESENTATION_VARIANT_ID,
        "development_only": True,
        "sealed_items_opened": False,
        "windows": len(rows),
        "variant": compact(variant_eval),
        "baseline": compact(baseline_eval),
        "delta": {
            key: variant_eval.get("metrics", {}).get(key, 0) - baseline_eval.get("metrics", {}).get(key, 0)
            for key in ("macro_composite", "event_precision", "event_recall", "attribution", "speaker_role", "stance", "atomicity")
        },
    }
    gate = audit_gold_c(
        manifest_path=manifest_path, result_root=variant_root, project_root=project_root
    )
    receipt["attribution_gate"] = {
        "passed": gate["passed"], "totals": gate["totals"],
        "by_transcript_structure": gate["by_transcript_structure"],
    }
    receipt["receipt_sha256"] = _sha(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return receipt
