"""Deterministic speaker-map headers for the development Gold C experiment.

This is a representation family, deliberately separate from prompt variants.
It never invents a host or guest from show metadata.  A name enters the header
only when it is present as a transcript label or in an explicit frozen episode
speaker map and also appears in the supplied window text.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "pif_signal_desk_gold_speaker_map_header_v1"
REPRESENTATION_VARIANT_ID = "gold-c-speaker-map-header-v2"
FAMILY_ID = "signal-desk-gold-authoring-representation"
FAMILY_TYPE = "representation"

# This intentionally matches only line-start labels.  Web-page title chrome
# often contains colons, but is not a line-start dialogue label in a clean
# speaker-turn transcript.
_LABEL_RE = re.compile(
    r"(?m)^(?:\s*)([A-Za-z][A-Za-z0-9 .,'’&()/-]{1,80})\s*:\s*\S"
)
_NOISE_LABELS = {
    "general", "substack", "more from these contributors", "transcript",
    "show notes", "sponsor", "advertisement", "episode", "description",
}
_MAP_KEYS = ("speaker_map", "speakers", "episode_speakers", "participants")


def _canonical(value: Any) -> str:
    return " ".join(str(value or "").casefold().replace("’", "'").split())


def _display(value: Any) -> str:
    return " ".join(str(value or "").replace("’", "'").split()).strip()


def _metadata_speakers(metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Read only an explicit, frozen speaker map; show/episode names are not maps."""

    raw: Any = None
    for key in _MAP_KEYS:
        if metadata.get(key):
            raw = metadata[key]
            break
    if isinstance(raw, Mapping):
        rows = []
        for identifier, entry in raw.items():
            if isinstance(entry, Mapping):
                rows.append({"identifier": identifier, **entry})
            else:
                rows.append({"identifier": identifier, "name": entry})
        return rows
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return [dict(entry) for entry in raw if isinstance(entry, Mapping)]
    return []


def _metadata_aliases(row: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for key in ("name", "display_name", "surface_name", "alias", "aliases"):
        raw = row.get(key)
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            values.extend(_display(value) for value in raw)
        elif raw:
            values.append(_display(raw))
    return tuple(value for value in values if value)


def _label_candidates(text: str, structure: str) -> list[dict[str, Any]]:
    matches = list(_LABEL_RE.finditer(text))
    counts: dict[str, int] = {}
    first: dict[str, int] = {}
    displays: dict[str, str] = {}
    for match in matches:
        display = _display(match.group(1))
        key = _canonical(display)
        if not key or key in _NOISE_LABELS:
            continue
        counts[key] = counts.get(key, 0) + 1
        first.setdefault(key, match.start())
        displays.setdefault(key, display)

    candidates: list[dict[str, Any]] = []
    for key, count in counts.items():
        # A single label in paragraph/flattened text is too easily webpage
        # chrome.  Speaker-turn text is already structurally authoritative;
        # repeated labels are required for paragraph text.
        if structure == "speaker_turn" or count >= 2:
            candidates.append({
                "speaker_id": displays[key],
                "surface_forms": [displays[key]],
                "basis": "transcript_label",
                "first_char": first[key],
                "label_count": count,
            })
    return sorted(candidates, key=lambda row: (int(row["first_char"]), str(row["speaker_id"])))


def build_speaker_map_header(*, metadata: Mapping[str, Any], window_text: str) -> dict[str, Any]:
    """Build a stable, private header without guessing identities."""

    structure = str(metadata.get("transcript_structure") or "unknown")
    lowered = _canonical(window_text)
    entries: dict[str, dict[str, Any]] = {}
    for row in _label_candidates(window_text, structure):
        key = _canonical(row["speaker_id"])
        entries[key] = {k: v for k, v in row.items() if k != "first_char"}

    # Explicit episode metadata may strengthen a label, but cannot create an
    # identity absent from the transcript surface.
    for row in _metadata_speakers(metadata):
        aliases = _metadata_aliases(row)
        present = [alias for alias in aliases if _canonical(alias) and _canonical(alias) in lowered]
        if not present:
            continue
        identifier = _display(row.get("speaker_id") or row.get("id") or row.get("canonical_id"))
        identifier = identifier or present[0]
        key = _canonical(identifier)
        if key in entries:
            entries[key]["basis"] = "transcript_label+frozen_episode_metadata"
            entries[key]["surface_forms"] = sorted(set(entries[key]["surface_forms"] + present))
        else:
            entries[key] = {
                "speaker_id": identifier,
                "surface_forms": sorted(set(present)),
                "basis": "frozen_episode_metadata_surface_supported",
                "label_count": 0,
            }

    speaker_map = [
        {key: value for key, value in row.items() if key != "label_count"}
        for row in sorted(entries.values(), key=lambda value: (_canonical(value["speaker_id"]), value["speaker_id"]))
    ]
    header = {
        "schema_version": SCHEMA_VERSION,
        "representation_variant_id": REPRESENTATION_VARIANT_ID,
        "transcript_structure": structure,
        "episode_id": str(metadata.get("episode_id") or ""),
        "window_id": str(metadata.get("window_id") or ""),
        "speaker_map": speaker_map,
        "identity_policy": (
            "Use speaker_id only when supported by this transcript-surface map. "
            "Otherwise set speaker_id null, attribution_type unresolved_speaker, "
            "and publishability_state quarantined. Never infer from show or episode title."
        ),
        "map_status": "supported" if speaker_map else "unresolved",
    }
    header["header_sha256"] = hashlib.sha256(
        json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return header


def build_headers(*, manifest_path: Path, project_root: Path, window_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    wanted = set(window_ids)
    headers: dict[str, dict[str, Any]] = {}
    for metadata in manifest.get("windows", []):
        window_id = str(metadata.get("window_id") or "")
        if window_id not in wanted:
            continue
        path = Path(str(metadata["transcript_path"]))
        if not path.is_absolute():
            path = project_root / path
        raw = path.read_text(encoding="utf-8")
        text = raw[int(metadata["start_char"]):int(metadata["end_char"])]
        headers[window_id] = build_speaker_map_header(metadata=metadata, window_text=text)
    if set(headers) != wanted:
        raise ValueError("speaker-map header selection does not match manifest window IDs")
    return headers


def headers_digest(headers: Mapping[str, Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(headers, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
