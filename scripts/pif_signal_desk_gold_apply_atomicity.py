#!/usr/bin/env python3
"""Apply only GPT-5.5-approved Gold-C atomicity removals with immutable lineage."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_audit import _load_frozen_window_text
from research_factory.signal_desk_rebuild_contracts import validate_output
from research_factory.util import now_iso, write_text_atomic

ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    authority_path = ROOT / "artifacts/gold-atomicity-adjudication-dev.json"
    authority = json.loads(authority_path.read_text())
    if not authority.get("complete") or authority.get("model") != "gpt-5.5":
        raise RuntimeError("complete GPT-5.5 atomicity authority is required")
    manifest = json.loads((PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json").read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    history = ROOT / "results/development/C-history" / authority["receipt_sha256"][:12]
    history.mkdir(parents=True, exist_ok=True)
    corrections = []
    for window_id, decision in sorted(authority["decisions"].items()):
        drop = list(decision.get("gold_c_drop_indices") or [])
        if not drop:
            continue
        path = ROOT / "results/development/C" / f"{window_id}.json"
        before = path.read_bytes()
        original = json.loads(before)
        if any(index >= len(original["events"]) for index in drop):
            raise RuntimeError(f"approved drop index is out of range: {window_id}")
        archive = history / path.name
        if archive.exists() and archive.read_bytes() != before:
            raise RuntimeError(f"atomicity history collision: {window_id}")
        if not archive.exists():
            shutil.copyfile(path, archive)
        corrected = {**original, "events": [
            event for index, event in enumerate(original["events"]) if index not in set(drop)
        ]}
        text = _load_frozen_window_text(metadata[window_id], project_root=PIF_ROOT)
        corrected = validate_output(corrected, transcript_window=text, expected_window_id=window_id)
        encoded = (json.dumps(corrected, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        write_text_atomic(path, encoded.decode())
        corrections.append({
            "window_id": window_id, "dropped_event_indices": drop,
            "before_events": len(original["events"]), "after_events": len(corrected["events"]),
            "before_sha256": _sha(before), "after_sha256": _sha(encoded),
            "archived_original": str(archive.relative_to(ROOT)),
        })
    receipt = {
        "schema_version": "pif_signal_desk_gold_atomicity_correction_v1",
        "created_at": now_iso(), "authority_model": "gpt-5.5",
        "authority_receipt_sha256": authority["receipt_sha256"],
        "corrections": corrections, "complete": bool(corrections),
    }
    body = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    receipt["receipt_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    output = ROOT / "artifacts/gold-atomicity-correction-dev.json"
    write_text_atomic(output, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
