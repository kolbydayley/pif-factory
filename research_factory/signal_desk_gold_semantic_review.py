"""GPT-5.5 adjudication contract for Gold-C semantic audit candidates."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_audit import _load_frozen_window_text


SCHEMA_VERSION = "pif_signal_desk_gold_semantic_review_v1"
MODEL = "gpt-5.5"
VERDICTS = ("equivalent", "reversed_meaning", "different_claim", "uncertain")
ATOMICITY_VERDICTS = (
    "valid_atomic_split", "independent_audit_over_split", "gold_c_needs_correction",
    "both_need_correction", "uncertain",
)
MAX_CANDIDATES = 25
MAX_INPUT_BYTES = 12_000


def output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["model", "decisions"],
        "properties": {
            "model": {"type": "string", "const": MODEL},
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["candidate_id", "verdict", "rationale"],
                    "properties": {
                        "candidate_id": {"type": "string"},
                        "verdict": {"type": "string", "enum": list(VERDICTS)},
                        "rationale": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def _compact_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "claim": event.get("claim_text"),
        "evidence": event.get("evidence_text"),
        "start": event.get("evidence_start"),
        "end": event.get("evidence_end"),
        "speaker": event.get("speaker_id"),
        "role": event.get("attribution_type"),
        "stance": event.get("stance"),
    }


def build_review_batches(
    *,
    manifest: Mapping[str, Any],
    audit_receipt: Mapping[str, Any],
    result_root: Path,
    project_root: Path,
) -> list[dict[str, Any]]:
    """Create private, source-grounded batches without sealed-split content."""

    metadata = {
        str(row["window_id"]): row
        for row in manifest.get("windows", [])
        if row.get("split") == "development"
    }
    candidates = audit_receipt.get("semantic_reversal_review", {}).get("candidates", [])
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        window_id = str(candidate.get("window_id") or "")
        if window_id not in metadata:
            raise ValueError("semantic review candidate is not in the development split")
        grouped[window_id].append(candidate)

    batches: list[dict[str, Any]] = []
    for window_id, rows in sorted(grouped.items()):
        gold = json.loads((result_root / "C" / f"{window_id}.json").read_text(encoding="utf-8"))
        independent = json.loads(
            (result_root / "AUDIT" / f"{window_id}.json").read_text(encoding="utf-8")
        )
        window_text = _load_frozen_window_text(metadata[window_id], project_root=project_root)
        compact = []
        for row in rows:
            gold_index = int(row["gold_index"])
            independent_index = int(row["independent_index"])
            compact.append(
                {
                    "candidate_id": str(row["candidate_id"]),
                    "gold_c": _compact_event(gold["events"][gold_index]),
                    "independent_audit": _compact_event(independent["events"][independent_index]),
                }
            )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "window_id": window_id,
            "transcript_structure": metadata[window_id]["transcript_structure"],
            "transcript_window": window_text,
            "candidates": compact,
        }
        if len(compact) > MAX_CANDIDATES:
            raise ValueError("semantic review batch exceeds 25 candidates")
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if len(serialized.encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError(f"semantic review batch exceeds 12,000 bytes: {window_id}")
        payload["input_sha256"] = hashlib.sha256(serialized.encode()).hexdigest()
        batches.append(payload)
    return batches


def validate_decisions(
    output: Mapping[str, Any], *, expected_candidate_ids: Sequence[str]
) -> dict[str, str]:
    if output.get("model") != MODEL:
        raise ValueError("semantic review response did not attest gpt-5.5")
    decisions = output.get("decisions")
    if not isinstance(decisions, list):
        raise ValueError("semantic review decisions must be a list")
    result: dict[str, str] = {}
    for row in decisions:
        if not isinstance(row, Mapping):
            raise ValueError("semantic review decision must be an object")
        candidate_id = str(row.get("candidate_id") or "")
        verdict = str(row.get("verdict") or "")
        rationale = str(row.get("rationale") or "").strip()
        if candidate_id in result or verdict not in VERDICTS or not rationale:
            raise ValueError("semantic review decision violates the frozen contract")
        result[candidate_id] = verdict
    if set(result) != set(expected_candidate_ids):
        raise ValueError("semantic review response does not cover the exact candidate set")
    return result


def build_atomicity_batches(
    *, manifest: Mapping[str, Any], window_ids: Sequence[str], result_root: Path,
    project_root: Path,
) -> list[dict[str, Any]]:
    metadata = {str(row["window_id"]): row for row in manifest.get("windows", [])}
    batches = []
    for window_id in sorted(set(window_ids)):
        meta = metadata.get(window_id)
        if not meta or meta.get("split") != "development":
            raise ValueError("atomicity review is restricted to development windows")
        payload: dict[str, Any] = {
            "schema_version": "pif_signal_desk_gold_atomicity_review_v1",
            "window_id": window_id,
            "transcript_structure": meta["transcript_structure"],
            "transcript_window": _load_frozen_window_text(meta, project_root=project_root),
            "outputs": {},
        }
        for turn in ("C", "AUDIT"):
            output = json.loads((result_root / turn / f"{window_id}.json").read_text())
            spans: dict[tuple[Any, Any], list[dict[str, Any]]] = defaultdict(list)
            for index, event in enumerate(output["events"]):
                spans[(event.get("evidence_start"), event.get("evidence_end"))].append(
                    {"index": index, **_compact_event(event)}
                )
            payload["outputs"][turn] = [
                {"span": [span[0], span[1]], "events": events}
                for span, events in spans.items() if len(events) >= 4
            ]
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if len(serialized.encode()) > MAX_INPUT_BYTES:
            raise ValueError(f"atomicity review batch exceeds 12,000 bytes: {window_id}")
        payload["input_sha256"] = hashlib.sha256(serialized.encode()).hexdigest()
        batches.append(payload)
    return batches


def atomicity_output_schema() -> dict[str, Any]:
    return {
        "type": "object", "additionalProperties": False,
        "required": ["model", "window_id", "verdict", "rationale", "gold_c_drop_indices"],
        "properties": {
            "model": {"type": "string", "const": MODEL},
            "window_id": {"type": "string"},
            "verdict": {"type": "string", "enum": list(ATOMICITY_VERDICTS)},
            "rationale": {"type": "string", "minLength": 1},
            "gold_c_drop_indices": {"type": "array", "items": {"type": "integer", "minimum": 0}},
        },
    }


def validate_atomicity_decision(output: Mapping[str, Any], *, window_id: str) -> dict[str, Any]:
    if output.get("model") != MODEL or output.get("window_id") != window_id:
        raise ValueError("atomicity response identity or model attestation mismatch")
    verdict = str(output.get("verdict") or "")
    rationale = str(output.get("rationale") or "").strip()
    drop = output.get("gold_c_drop_indices")
    if verdict not in ATOMICITY_VERDICTS or not rationale or not isinstance(drop, list):
        raise ValueError("atomicity response violates the frozen contract")
    if any(isinstance(index, bool) or not isinstance(index, int) or index < 0 for index in drop):
        raise ValueError("atomicity drop indices are invalid")
    if verdict not in {"gold_c_needs_correction", "both_need_correction"} and drop:
        raise ValueError("atomicity response may drop Gold-C events only when correction is required")
    return {"verdict": verdict, "gold_c_drop_indices": sorted(set(drop))}
