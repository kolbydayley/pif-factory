"""Project private GPT-5.5 disagreement decisions into immutable dev truth.

The adjudicator's rationales and transcript-backed case packets stay private.  This
module emits a new, hash-bound result tree and an aggregate-only receipt; it never
mutates the authored Gold-C files or puts source text in a receipt.
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_gold_audit import _load_frozen_window_text
from .signal_desk_gold_disagreement import build_cases, validate_decisions
from .signal_desk_rebuild_contracts import ATTRIBUTION_TYPES, validate_output
from .util import now_iso, write_text_atomic

SCHEMA_VERSION = "pif_signal_desk_gold_adjudicated_truth_v1"
DECISION_SOURCE = "gpt-5.5-disagreement-review-dev"
_CORRECTION_KEYS = {
    "claim_text", "evidence_text", "evidence_start", "evidence_end",
    "speaker_id", "attribution_type", "stance",
    "mentioned_people", "quoted_person", "speaker_role", "event_presence",
}
_FIELD_ALIASES = {
    "mentioned_people": "mentioned_person_ids",
    "quoted_person": "quoted_person_id",
    # GPT-5.5 sometimes used the diagnostic name for this field.  The event
    # contract's canonical field is attribution_type.
    "speaker_role": "attribution_type",
}


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parse_correction(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("correction_json is not valid JSON") from exc
    if not isinstance(value, dict) or set(value) - _CORRECTION_KEYS:
        raise ValueError("correction_json contains unsupported fields")
    return value


def _apply_correction(event: Mapping[str, Any], raw: str) -> dict[str, Any]:
    correction = _parse_correction(raw)
    if correction.get("event_presence") not in (None, "present"):
        raise ValueError("accepted event correction cannot mark event absent")
    result = copy.deepcopy(dict(event))
    for key, value in correction.items():
        if key == "event_presence":
            continue
        if key == "speaker_role" and value not in ATTRIBUTION_TYPES:
            # GPT-5.5 occasionally returned a human occupation here (for
            # example, "CEO and co-founder").  The v2 event contract has no
            # occupation field; do not coerce it into an attribution type.
            continue
        target = _FIELD_ALIASES.get(key, key)
        if target == "evidence_start" and "evidence_end" not in correction:
            if not isinstance(value, int) or not isinstance(result.get("evidence_text"), str):
                raise ValueError("evidence_start correction requires evidence text")
            result["evidence_end"] = value + len(result["evidence_text"])
        result[target] = value
    if "evidence_text" in correction and "evidence_end" not in correction:
        start = result.get("evidence_start")
        if isinstance(start, int):
            result["evidence_end"] = start + len(str(result["evidence_text"]))
    return result


def apply_decisions(*, manifest_path: Path, result_root: Path, project_root: Path,
                    private_root: Path, selected_window_ids: list[str],
                    decisions_root: Path, output_root: Path) -> dict[str, Any]:
    """Create a fresh adjudicated result tree from exact case decisions.

    ``output_root`` must not be the authored result root.  Existing output is
    accepted only when its bytes exactly match the newly computed tree, making
    reruns idempotent while preserving the original lineage.
    """
    if output_root.resolve() == result_root.resolve():
        raise ValueError("adjudicated truth must not overwrite authored Gold C")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    cases = build_cases(manifest_path=manifest_path, result_root=result_root,
                        project_root=project_root, selected_window_ids=selected_window_ids)
    expected_ids = [str(case["case_id"]) for case in cases]
    raw_decisions: dict[str, dict[str, Any]] = {}
    for path in sorted(decisions_root.glob("*.output.json")):
        value = json.loads(path.read_text(encoding="utf-8"))
        raw_decisions.update(validate_decisions(value, [str(x["case_id"]) for x in cases
                                                        if str(x["case_id"]) in {
                                                            str(r.get("case_id")) for r in value.get("decisions", [])
                                                        }]))
    if set(raw_decisions) != set(expected_ids):
        raise ValueError(f"adjudication decisions cover {len(raw_decisions)}/{len(expected_ids)} cases")
    case_by_id = {str(case["case_id"]): case for case in cases}
    selected_set = {str(x) for x in selected_window_ids}
    by_window: dict[str, list[dict[str, Any]]] = {}
    for case_id, decision in raw_decisions.items():
        by_window.setdefault(str(case_by_id[case_id]["window_id"]), []).append(
            {"case": case_by_id[case_id], **decision}
        )

    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "C").mkdir(parents=True, exist_ok=True)
    (output_root / "AUDIT").mkdir(parents=True, exist_ok=True)
    counts = {"gold_supported": 0, "audit_supported": 0, "both_supported": 0,
              "neither_supported": 0, "uncertain": 0}
    removed: list[str] = []
    changed = 0
    all_window_ids = sorted(
        str(row["window_id"]) for row in manifest["windows"]
        if str(row.get("split")) == "development"
    )
    for window_id in all_window_ids:
        gold_path = result_root / "C" / f"{window_id}.json"
        audit_path = result_root / "AUDIT" / f"{window_id}.json"
        gold = json.loads(gold_path.read_text(encoding="utf-8"))
        if window_id not in selected_set:
            shutil.copyfile(gold_path, output_root / "C" / f"{window_id}.json")
            continue
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        gold_by_index = {i: e for i, e in enumerate(gold["events"])}
        audit_by_index = {i: e for i, e in enumerate(audit["events"])}
        replacements: dict[int, dict[str, Any] | None] = {}
        for row in by_window.get(window_id, []):
            case = row["case"]
            decision = row["decision"]
            counts[decision] += 1
            gi = case.get("gold_index")
            ai = case.get("audit_index")
            if decision == "audit_supported":
                chosen = audit_by_index.get(ai) if ai is not None else None
            elif decision in {"gold_supported", "both_supported"}:
                chosen = gold_by_index.get(gi) if gi is not None else None
            else:
                chosen = None
            if chosen is None:
                if gi is not None:
                    replacements[int(gi)] = None
                    removed.append(f"{window_id}:{gi}")
                continue
            replacement = _apply_correction(chosen, row.get("correction_json", ""))
            # event_id is the stable Gold-C identity, not the temporary audit
            # candidate identity.  Retain it when projecting an audit-backed
            # event so replacing one pair cannot introduce duplicate IDs.
            if gi is not None and int(gi) in gold_by_index:
                replacement["event_id"] = gold_by_index[int(gi)]["event_id"]
            replacements[int(gi)] = replacement
        events = []
        for index, event in enumerate(gold["events"]):
            if index in replacements:
                if replacements[index] is not None:
                    events.append(replacements[index])
            else:
                events.append(event)
        corrected = {**gold, "events": events}
        transcript = _load_frozen_window_text(metadata[window_id], project_root=project_root)
        corrected = validate_output(corrected, transcript_window=transcript,
                                    expected_window_id=window_id)
        encoded = (json.dumps(corrected, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":")) + "\n").encode()
        target = output_root / "C" / f"{window_id}.json"
        if target.exists() and target.read_bytes() != encoded:
            raise ValueError(f"immutable adjudicated output collision: {window_id}")
        write_text_atomic(target, encoded.decode())
        shutil.copyfile(audit_path, output_root / "AUDIT" / f"{window_id}.json")
        if encoded != (json.dumps(gold, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":")) + "\n").encode():
            changed += 1

    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "decision_source": DECISION_SOURCE, "decision_count": len(raw_decisions),
        "selected_windows": len(by_window), "output_windows": len(all_window_ids), "decision_counts": counts,
        "changed_windows": changed, "removed_event_count": len(removed),
        "output_root": str(output_root.relative_to(private_root.parent)),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "decisions_sha256": hashlib.sha256(json.dumps(raw_decisions, sort_keys=True,
                                                       separators=(",", ":")).encode()).hexdigest(),
        "privacy": "private truth outputs; receipt contains counts and hashes only; no transcript text",
    }
    receipt["receipt_sha256"] = hashlib.sha256(json.dumps(receipt, sort_keys=True,
                                                          separators=(",", ":")).encode()).hexdigest()
    return receipt
