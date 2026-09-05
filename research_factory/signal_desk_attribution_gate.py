"""Fail-closed speaker attribution completeness audit.

This module deliberately emits only aggregate diagnostics.  Transcript text is
read to establish whether an identity is supported, but never written to the
receipt.  It is usable for frozen Gold-C directories and for production-shaped
JSON payloads with the same event contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_contracts import ATTRIBUTION_TYPES, validate_output
from .util import now_iso

SCHEMA_VERSION = "pif_signal_desk_attribution_completeness_v1"
STRICT_STRUCTURES = frozenset({"speaker_turn", "paragraph"})
INDETERMINATE_STRUCTURES = frozenset({"flattened", "asr_diarized", "asr"})
CLAIM_ATTRIBUTIONS = frozenset({"direct_speech", "quoted_speech", "reported_paraphrase"})
_LABEL_RE = re.compile(r"(?m)(?:^|\n)\s*([A-Za-z][A-Za-z0-9 .,'’&()/-]{1,80})\s*:\s*")


class AttributionGateError(ValueError):
    pass


def _canonical(value: Any) -> str:
    return " ".join(str(value or "").casefold().replace("’", "'").split())


def _names_for_id(speaker_id: str, metadata: Mapping[str, Any]) -> set[str]:
    """Return safe textual aliases from optional manifest speaker-map metadata."""
    aliases: set[str] = {_canonical(speaker_id)}
    raw = metadata.get("speaker_map") or metadata.get("speakers") or []
    if isinstance(raw, Mapping):
        raw = raw.values()
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            ids = {str(item.get(key) or "") for key in ("speaker_id", "id", "canonical_id")}
            if speaker_id not in ids:
                continue
            for key in ("name", "display_name", "surface_name", "alias", "aliases"):
                values = item.get(key)
                if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                    aliases.update(_canonical(v) for v in values)
                elif values:
                    aliases.add(_canonical(values))
    return {value for value in aliases if value}


def _supported_labels(text: str) -> list[str]:
    return [_canonical(match.group(1)) for match in _LABEL_RE.finditer(text)]


def _is_supported_speaker(event: Mapping[str, Any], text: str, metadata: Mapping[str, Any], structure: str) -> bool:
    speaker = event.get("speaker_id")
    if not speaker:
        return False
    aliases = _names_for_id(str(speaker), metadata)
    lowered = _canonical(text)
    # A speaker-map identity is valid only if its surface identity is present
    # in the supplied text.  This avoids treating arbitrary canonical IDs as
    # evidence of who spoke.
    if any(alias and re.search(r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])", lowered) for alias in aliases):
        return True
    if structure in STRICT_STRUCTURES:
        return any(label in aliases for label in _supported_labels(text))
    return False


def _has_identity_evidence(text: str, metadata: Mapping[str, Any], structure: str) -> bool:
    return bool(_supported_labels(text)) or bool(metadata.get("speaker_map") or metadata.get("speakers"))


def _has_any_supported_identity(text: str, metadata: Mapping[str, Any]) -> bool:
    """Whether flattened text contains a name from the supplied speaker map."""
    lowered = _canonical(text)
    raw = metadata.get("speaker_map") or metadata.get("speakers") or []
    if isinstance(raw, Mapping):
        raw = raw.values()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return False
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        for key in ("name", "display_name", "surface_name", "alias", "aliases"):
            values = item.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                names = values
            else:
                names = [values]
            if any(value and re.search(r"(?<![a-z0-9])" + re.escape(_canonical(value)) + r"(?![a-z0-9])", lowered) for value in names):
                return True
    return False


def _event_is_consequential(event: Mapping[str, Any]) -> bool:
    return str(event.get("speech_act") or "") in {
        "assertion", "forecast", "explanation", "recommendation", "commitment", "disagreement", "reported_position"
    }


def audit_events(*, windows: Sequence[Mapping[str, Any]], source_name: str = "corpus") -> dict[str, Any]:
    """Audit already-materialized windows; each row contains ``metadata``, ``text``, and ``events``."""
    totals = Counter()
    by_structure: dict[str, Counter[str]] = defaultdict(Counter)
    by_show: dict[str, Counter[str]] = defaultdict(Counter)
    examples: list[dict[str, str]] = []
    for row in windows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), Mapping) else row
        text = str(row.get("text") or "")
        events = row.get("events") or []
        structure = str(metadata.get("transcript_structure") or "unknown")
        show_id = str(metadata.get("show_id") or "unknown")
        for event in events:
            if not isinstance(event, Mapping) or not _event_is_consequential(event):
                continue
            totals["consequential_claims"] += 1
            by_structure[structure]["consequential_claims"] += 1
            by_show[show_id]["consequential_claims"] += 1
            attribution = str(event.get("attribution_type") or "")
            mentioned = {str(v) for v in (event.get("mentioned_person_ids") or [])}
            if attribution == "third_party_mention" and event.get("speaker_id") and str(event.get("speaker_id")) in mentioned:
                totals["third_party_presented_as_own"] += 1
                by_structure[structure]["third_party_presented_as_own"] += 1
            # An unresolved label is itself a missing assignment on structured
            # transcripts.  It is permitted on flattened/ASR only when no
            # supported identity is available in the supplied context.
            if attribution == "unresolved_speaker" and not event.get("speaker_id"):
                should_flag = structure in STRICT_STRUCTURES or (
                    structure in INDETERMINATE_STRUCTURES and _has_any_supported_identity(text, metadata)
                )
                if should_flag:
                    key = "missing_speaker_assignment" if structure in STRICT_STRUCTURES else "missing_supported_speaker"
                    totals[key] += 1; by_structure[structure][key] += 1; by_show[show_id][key] += 1
                    if len(examples) < 25: examples.append({"window_id": str(metadata.get("window_id") or ""), "reason": key})
                else:
                    totals["indeterminable_claims"] += 1
                    by_structure[structure]["indeterminable_claims"] += 1
                    by_show[show_id]["indeterminable_claims"] += 1
                continue
            if attribution not in CLAIM_ATTRIBUTIONS:
                if attribution == "unresolved_speaker":
                    totals["indeterminable_claims"] += 1
                    by_structure[structure]["indeterminable_claims"] += 1
                continue
            supported = _is_supported_speaker(event, text, metadata, structure)
            if not event.get("speaker_id"):
                should_flag = structure in STRICT_STRUCTURES or (
                    structure in INDETERMINATE_STRUCTURES and _has_any_supported_identity(text, metadata)
                )
                if should_flag:
                    key = "missing_speaker_assignment" if structure in STRICT_STRUCTURES else "missing_supported_speaker"
                    totals[key] += 1; by_structure[structure][key] += 1; by_show[show_id][key] += 1
                    if len(examples) < 25: examples.append({"window_id": str(metadata.get("window_id") or ""), "reason": key})
            elif not supported and _has_identity_evidence(text, metadata, structure):
                totals["fabricated_or_unsupported_attribution"] += 1
                by_structure[structure]["fabricated_or_unsupported_attribution"] += 1
                by_show[show_id]["fabricated_or_unsupported_attribution"] += 1
    missing = totals["missing_speaker_assignment"] + totals["missing_supported_speaker"]
    passed = not missing and not totals["fabricated_or_unsupported_attribution"] and not totals["third_party_presented_as_own"]
    def clean(mapping: Mapping[str, Counter[str]]) -> dict[str, dict[str, int]]:
        return {key: dict(sorted(counter.items())) for key, counter in sorted(mapping.items())}
    receipt = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(), "source": source_name,
        "passed": passed, "totals": dict(sorted(totals.items())),
        "by_transcript_structure": clean(by_structure), "by_show": clean(by_show),
        "sample_window_ids": examples, "receipt_exposes_transcript_text": False,
    }
    receipt["receipt_sha256"] = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return receipt


def audit_gold_c(*, manifest_path: Path, result_root: Path, project_root: Path | None = None) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = project_root or manifest_path.parent
    windows = []
    for metadata in manifest.get("windows", []):
        window_id = str(metadata.get("window_id") or "")
        path = Path(str(metadata.get("transcript_path") or ""))
        if not path.is_absolute(): path = root / path
        text = path.read_text(encoding="utf-8")[int(metadata["start_char"]):int(metadata["end_char"])]
        result = result_root / "C" / f"{window_id}.json"
        if not result.exists(): continue
        output = json.loads(result.read_text(encoding="utf-8"))
        windows.append({"metadata": metadata, "text": text, "events": output.get("events", [])})
    return audit_events(windows=windows, source_name="gold_c")


def repair_gold_c(*, manifest_path: Path, result_root: Path, output_root: Path,
                  project_root: Path | None = None) -> dict[str, Any]:
    """Create a resumable, append-only repaired view of Gold-C.

    Structured unresolved claims are quarantined unless a frozen speaker map
    proves an identity.  The current benchmark manifest has no per-window
    speaker map, so this intentionally repairs zero by guessing and quarantines
    every unresolved structured claim.  Original C files are never overwritten.
    """
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = project_root or manifest_path.parent
    output_root.mkdir(parents=True, exist_ok=True)
    quarantined: list[dict[str, Any]] = []
    repaired = 0
    for metadata in manifest.get("windows", []):
        window_id = str(metadata.get("window_id") or "")
        source = result_root / "C" / f"{window_id}.json"
        if not source.exists():
            continue
        output = json.loads(source.read_text(encoding="utf-8"))
        kept = []
        structure = str(metadata.get("transcript_structure") or "unknown")
        for index, event in enumerate(output.get("events", [])):
            unresolved = (
                isinstance(event, Mapping)
                and _event_is_consequential(event)
                and event.get("attribution_type") == "unresolved_speaker"
                and not event.get("speaker_id")
                and structure in STRICT_STRUCTURES
            )
            if not unresolved:
                kept.append(event)
                continue
            quarantined.append({
                "window_id": window_id, "event_id": str(event.get("event_id") or ""),
                "event_index": index, "transcript_structure": structure,
                "reason": "attribution_indeterminable_no_frozen_speaker_map",
                "evidence_sha256": hashlib.sha256(str(event.get("evidence_text") or "").encode()).hexdigest(),
            })
        if len(kept) != len(output.get("events", [])):
            output = {**output, "events": kept}
            if not kept:
                output["window_disposition"] = "no_consequential_claims"
            repaired += 1
        (output_root / f"{window_id}.json").write_text(json.dumps(output, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    receipt: dict[str, Any] = {
        "schema_version": "pif_signal_desk_attribution_repair_v1",
        "created_at": now_iso(), "source": "gold_c", "immutable_source": True,
        "repaired_claims": 0, "quarantined_claims": len(quarantined),
        "quarantined_windows": len({row["window_id"] for row in quarantined}),
        "policy": "never guess; unresolved structured attribution is quarantined",
        "quarantine_records": quarantined,
        "receipt_exposes_transcript_text": False,
    }
    receipt["receipt_sha256"] = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    (output_root / "../attribution-repair-receipt.json").resolve().write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt
