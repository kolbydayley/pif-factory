#!/usr/bin/env python3
"""Reconstruct approved speaker projections for unchanged dev windows only.

No model calls; no sealed text; no mutation of original gold. This produces an
audit candidate, not accepted gold. Residual quarantines remain in the receipt.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.signal_desk_gold_repair_v4 import _digest_outputs, _sha_json
from research_factory.signal_desk_gold_wider_projection import _load_decisions, project_window
from research_factory.signal_desk_rebuild_gold import verify_frozen_manifest
from research_factory.signal_desk_rebuild_contracts import validate_output
from research_factory.signal_desk_attribution_gate import audit_events


def checked_text(row, root):
    if row.get("split") != "development":
        raise ValueError("protected split cannot enter this projection")
    path = Path(row["transcript_path"])
    text = (path if path.is_absolute() else root / path).read_text()
    window = text[int(row["start_char"]):int(row["end_char"])]
    for value, expected in ((text, row["transcript_sha256"]), (window, row["text_sha256"])):
        if hashlib.sha256(value.encode()).hexdigest() != expected:
            raise ValueError("frozen source digest mismatch")
    return text, window


def immutable_json(path, value):
    encoded = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text() != encoded:
            raise ValueError(f"immutable output conflict: {path.name}")
    else:
        with path.open("x") as handle:
            handle.write(encoded)


def main():
    g = ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    r = g / "development-source-reauthor-v1"
    out = r / "provenance-merged-development-v1"
    base = json.loads((ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json").read_text())
    merged = json.loads((r / "merged-manifest.json").read_text())
    for manifest in (base, merged):
        verify_frozen_manifest(manifest)
    old = {w["window_id"]: w for w in base["windows"] if w["split"] == "development"}
    current = {w["window_id"]: w for w in merged["windows"] if w["split"] == "development"}
    if len(current) != 189 or len(set(current) - set(old)) != 9:
        raise ValueError("expected 180 unchanged and nine replacement dev windows")
    variants = g / "results/development-attribution-variants"
    v5 = variants / "gold-c-full-source-provenance-v5/full"
    v6 = variants / "gold-c-gpt55-wider-speaker-projection-v6/full"
    baseline = g / "results/development"
    plan = json.loads((v6 / "repair-plan.json").read_text())
    if _sha_json({k: v for k, v in plan.items() if k != "plan_sha256"}) != plan["plan_sha256"]:
        raise ValueError("V6 plan digest mismatch")
    if plan["manifest_sha256"] != base["manifest_sha256"]:
        raise ValueError("V6 manifest binding mismatch")
    for path, expected in ((baseline / "C", plan["baseline_c_digest"]), (v5 / "C", plan["v5_c_digest"])):
        if _digest_outputs(path, plan["selected_window_ids"]) != expected:
            raise ValueError("V6 input digest mismatch")
    decisions, bindings = _load_decisions(g / "private-gpt55-wider-speaker-v1", g / "dispatch-gpt55-wider-speaker-v1.sqlite")
    if _sha_json(bindings) != plan["accepted_decisions_digest"]:
        raise ValueError("GPT-5.5 decision binding mismatch")
    artifacts, audits, residuals = [], [], []
    pending = []
    for wid, row in sorted(current.items()):
        transcript, window = checked_text(row, ROOT)
        if wid in old and row != old[wid]:
            raise ValueError("unchanged window metadata drift")
        raw = json.loads((r / "merged-development/C" / f"{wid}.json").read_text())
        if wid in old:
            original = json.loads((baseline / "C" / f"{wid}.json").read_text())
            if raw != original:
                raise ValueError("merged baseline C drift")
            projected, provenance = project_window(
                baseline_c=original,
                v5_c=json.loads((v5 / "C" / f"{wid}.json").read_text()),
                v5_provenance=json.loads((v5 / "provenance" / f"{wid}.json").read_text()),
                decision=decisions.get(wid), transcript_window=window)
            if projected != json.loads((v6 / "C" / f"{wid}.json").read_text()):
                raise ValueError("reconstructed V6 differs from saved projection")
            if provenance["residual_count"]:
                residuals.append({"window_id": wid, "show_id": row["show_id"],
                                  "quarantined_events": provenance["residual_count"]})
        else:
            projected = raw
            replacement = json.loads((r / "replacement-results/C" / f"{wid}.json").read_text())
            if raw != replacement:
                raise ValueError("replacement C drift")
            provenance = {"window_id": wid, "source": "fresh_replacement_gold_C", "residual_count": 0}
        validate_output(projected, transcript_window=window, expected_window_id=wid)
        pending.extend([(out / "C" / f"{wid}.json", projected),
                        (out / "provenance" / f"{wid}.json", provenance)])
        for turn in ("A", "B"):
            value = json.loads((r / "merged-development" / turn / f"{wid}.json").read_text())
            source = (baseline if wid in old else r / "replacement-results") / turn / f"{wid}.json"
            if value != json.loads(source.read_text()):
                raise ValueError("merged A/B drift")
            pending.append((out / turn / f"{wid}.json", value))
        artifacts.append({"window_id": wid, "c_sha256": _sha_json(projected),
                          "text_sha256": row["text_sha256"], "provenance_sha256": _sha_json(provenance)})
        audits.append({"metadata": row, "text": transcript, "events": projected["events"]})
    # Validate every source and projection before writing any candidate output.
    for path, value in pending:
        immutable_json(path, value)
    audit = audit_events(windows=audits, source_name="reconstructed_dev_full_frozen_source_identity_diagnostic")
    receipt = {"schema_version": "pif_gold_dev_provenance_merge_v1", "windows": len(current),
               "merged_manifest_sha256": merged["manifest_sha256"], "parent_plan_sha256": plan["plan_sha256"],
               "artifacts": artifacts, "artifacts_digest": _sha_json(artifacts),
               "residual_quarantines": residuals, "quarantined_events": sum(v["quarantined_events"] for v in residuals),
               "provider_calls": 0, "sealed_text_opened": False, "accepted_gold": False,
               "next_gate": "fresh_blind_audit_and_residual_attribution_review"}
    immutable_json(out / "receipt.json", receipt)
    # Audit timestamps intentionally change; keep each diagnostic separate.
    immutable_json(out / f"attribution-{audit['receipt_sha256']}.json", audit)
    print(json.dumps({"windows": len(current), "quarantined_events": receipt["quarantined_events"],
                      "attribution_diagnostic_passed": audit["passed"], "totals": audit["totals"],
                      "accepted_gold": False, "output_root": str(out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
