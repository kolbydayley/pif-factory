#!/usr/bin/env python3
"""Freeze attribution-only diagnostic inputs, with no provider calls."""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_rubric_reference_review import QUAL, R
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_attribution_contrasts import build, SYSTEM, response_schema


def main():
    result = build(qualification_plan=json.loads((QUAL / "plan.json").read_text()),
        manifest=json.loads((R / "merged-manifest.json").read_text()), result_root=QUAL / "results", project_root=ROOT)
    out = QUAL / "attribution-schema-v3-contrasts-v1"
    for packet in result["packets"]:
        immutable_json(out / f"{packet['packet_sha256']}.packet.json", packet)
        immutable_json(out / f"{packet['packet_sha256']}.schema.json", response_schema(packet))
    immutable_json(out / "plan.json", {k: v for k, v in result.items() if k != "packets"} | {
        "packet_digests": [p["packet_sha256"] for p in result["packets"]], "system_prompt": SYSTEM,
        "window_count": len(result["packets"]), "anchor_count": sum(len(p["anchors"]) for p in result["packets"]),
        "empty_anchor_windows": sum(p["empty_anchor_window"] for p in result["packets"]),
        "provider_calls_started": 0})
    print(json.dumps({"windows": len(result["packets"]), "anchors": sum(len(p["anchors"]) for p in result["packets"]),
        "provider_calls_started": 0, "qualified": False}))


if __name__ == "__main__":
    main()
