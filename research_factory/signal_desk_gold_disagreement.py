"""Build and validate the complete development Gold-C disagreement packet.

This module deliberately keeps transcript text in private prompt artifacts only.
The resulting decision and correction receipts contain hashes and field names,
never transcript text or claim text.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_audit import _load_frozen_window_text
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_evaluation import diagnostic_pairs

SCHEMA_VERSION = "pif_signal_desk_gold_disagreement_packet_v2"
MODEL = "gpt-5.5"
DECISIONS = ("gold_supported", "audit_supported", "both_supported", "neither_supported", "uncertain")
DISPUTED_FIELDS = ("speaker", "speaker_role", "quoted_person", "mentioned_people", "stance")


def _event(event: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    return {k: event.get(k) for k in (
        "claim_text", "evidence_text", "evidence_start", "evidence_end",
        "speaker_id", "attribution_type", "attribution_confidence", "speaker_role",
        "quoted_person_id", "mentioned_person_ids", "quoted_person", "mentioned_people", "stance",
    )}


def _case_id(window_id: str, kind: str, gold_index: int | None, audit_index: int | None, fields: Sequence[str]) -> str:
    body = json.dumps([window_id, kind, gold_index, audit_index, sorted(fields)], separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def build_cases(*, manifest_path: Path, result_root: Path, project_root: Path,
                selected_window_ids: Sequence[str]) -> list[dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(r["window_id"]): r for r in manifest["windows"]}
    cases: list[dict[str, Any]] = []
    for window_id in sorted(set(selected_window_ids)):
        meta = metadata[window_id]
        window_text = _load_frozen_window_text(meta, project_root=project_root)
        gold = validate_output(json.loads((result_root / "C" / f"{window_id}.json").read_text()),
                               transcript_window=window_text, expected_window_id=window_id)
        audit = validate_output(json.loads((result_root / "AUDIT" / f"{window_id}.json").read_text()),
                                transcript_window=window_text, expected_window_id=window_id)
        pairs = diagnostic_pairs(gold["events"], audit["events"], transcript_structure=meta["transcript_structure"])
        paired_gold = {int(p["gold_index"]) for p in pairs}
        paired_audit = {int(p["predicted_index"]) for p in pairs}
        for gi, event in enumerate(gold["events"]):
            if gi not in paired_gold:
                cases.append({"case_id": _case_id(window_id, "gold_unmatched", gi, None, ()),
                              "window_id": window_id, "kind": "gold_unmatched", "gold_index": gi,
                              "audit_index": None, "fields": ["event_presence"], "gold": _event(event), "audit": None})
        # The reliability receipt's critical-error denominator is asymmetric:
        # it charges omitted Gold-C events and paired field/attribution errors,
        # while extra independent-audit events remain ordinary precision data.
        # Keep this packet exactly aligned to that frozen 731-case authority.
        for pair in pairs:
            agreement = pair["field_agreement"]
            fields = [f for f in DISPUTED_FIELDS if not agreement[f]]
            if pair["unsupported_attribution"]:
                fields.append("unsupported_attribution")
            if fields or not agreement["stance"]:
                if not fields:
                    fields.append("stance")
                gi, ai = int(pair["gold_index"]), int(pair["predicted_index"])
                cases.append({"case_id": _case_id(window_id, "paired", gi, ai, fields),
                              "window_id": window_id, "kind": "paired", "gold_index": gi,
                              "audit_index": ai, "fields": sorted(set(fields)),
                              "gold": _event(gold["events"][gi]), "audit": _event(audit["events"][ai])})
    return cases


def build_batches(*, cases: Sequence[Mapping[str, Any]], window_texts: Mapping[str, str],
                  max_cases: int = 4, max_bytes: int = 28_000) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for case in cases:
        window_id = str(case["window_id"])
        candidate = {"case_id": case["case_id"], "kind": case["kind"], "fields": case["fields"],
                     "gold": case["gold"], "audit": case["audit"]}
        if batches and (batches[-1]["window_id"] != window_id or len(batches[-1]["cases"]) >= max_cases):
            batches.append({"schema_version": SCHEMA_VERSION, "window_id": window_id, "transcript_window": window_texts[window_id], "cases": [candidate]})
        elif not batches or batches[-1]["window_id"] != window_id:
            batches.append({"schema_version": SCHEMA_VERSION, "window_id": window_id, "transcript_window": window_texts[window_id], "cases": [candidate]})
        else:
            trial = dict(batches[-1]); trial["cases"] = [*batches[-1]["cases"], candidate]
            if len(json.dumps(trial, ensure_ascii=False).encode()) > max_bytes:
                batches.append({"schema_version": SCHEMA_VERSION, "window_id": window_id, "transcript_window": window_texts[window_id], "cases": [candidate]})
            else:
                batches[-1]["cases"].append(candidate)
    for batch in batches:
        body = json.dumps(batch, ensure_ascii=False, separators=(",", ":")).encode()
        batch["input_sha256"] = hashlib.sha256(body).hexdigest()
    return batches


def validate_decisions(output: Mapping[str, Any], expected_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if output.get("model") != MODEL or not isinstance(output.get("decisions"), list):
        raise ValueError("disagreement output model/schema mismatch")
    decisions: dict[str, dict[str, Any]] = {}
    for row in output["decisions"]:
        cid, decision = str(row.get("case_id") or ""), str(row.get("decision") or "")
        rationale = str(row.get("rationale") or "").strip()
        if cid in decisions or decision not in DECISIONS or not rationale:
            raise ValueError("invalid disagreement decision")
        decisions[cid] = {"decision": decision, "rationale": rationale,
                          "correction_json": str(row.get("correction_json") or "")}
    if set(decisions) != set(expected_ids):
        raise ValueError("disagreement output does not cover exact case set")
    return decisions
