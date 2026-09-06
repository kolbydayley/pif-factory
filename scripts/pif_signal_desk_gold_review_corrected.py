#!/usr/bin/env python3
"""Six-case, source-bound GPT-5.5 pilot before full corrected audit adjudication."""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_disagreement import build_cases, validate_decisions
from research_factory.signal_desk_gold_wider_speaker import reserve_call, settle_call
from research_factory.signal_desk_rebuild_approval import ApprovalBudgetConfig
from research_factory.signal_desk_gold_repair_v4 import _sha_json
from research_factory.signal_desk_gold_audit import _load_frozen_window_text
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from scripts.pif_signal_desk_gold_disagreement import SCHEMA

G = ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
R = G / "development-source-reauthor-v1"
A = R / "fresh-dev-audit-v1"
OUT = R / "corrected-review-pilot-v1"
SYSTEM = """You are GPT-5.5, final adjudication authority for a private podcast gold reliability audit.
Judge the exact disputed event fields against the frozen transcript, not whether the independent
audit happens to agree. Gold and audit can both be wrong. Distinguish speaker_id, quoted_person_id,
and mentioned_person_ids. A name occurring in text does not establish that person as its speaker.
No outside knowledge or guessed identity. ASR transcript spellings are legitimate surface forms.
Attribution may be genuinely indeterminable in flattened/ASR text. If the supplied window cannot
resolve the disputed speaker or meaning, return uncertain and explain what wider context is needed;
do not approve a context-dependent identity from guesswork. Equivalent supported propositions can
be both_supported; an unmatched gold event can be gold_supported even if the independent pass
omitted it. Audit_supported requires an actual audit candidate. Corrections may only name disputed
fields and values, never fabricate evidence. Return one decision per exact case_id, attest gpt-5.5.
Rationale must distinguish direct source evidence from missing context. No acceptance is automatic:
this is a diagnostic pilot, and uncertain cases must undergo wider-context review before rejection."""


def prepare():
    manifest_path = R / "merged-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    audit = json.loads((A / "audit.json").read_text())
    if audit["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("audit manifest mismatch")
    selected = audit["audit_selection"]["window_ids"]
    cases = build_cases(manifest_path=manifest_path, result_root=A / "results", project_root=ROOT, selected_window_ids=selected)
    if len(cases) != audit["critical_errors"]["errors"]:
        raise ValueError("disagreement inventory does not reconcile")
    metadata = {w["window_id"]: w for w in manifest["windows"] if w["split"] == "development"}
    # Diagnostic-only pilot; deterministic diversity across shows, not a gate sample.
    chosen, shows = [], set()
    for c in sorted(cases, key=lambda c: _sha_json(["corrected-review-pilot-v1", c["case_id"]])):
        show = metadata[c["window_id"]]["show_id"]
        if show not in shows:
            chosen.append(c)
            shows.add(show)
        if len(chosen) == 6:
            break
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    packets = []
    for case in chosen:
        row = metadata[case["window_id"]]
        packet = {"case": case, "transcript_structure": row["transcript_structure"],
                  "transcript_window": _load_frozen_window_text(row, project_root=ROOT),
                  "text_sha256": row["text_sha256"], "manifest_sha256": manifest["manifest_sha256"],
                  "audit_receipt_sha256": audit["receipt_sha256"], "system_sha256": _sha_json(SYSTEM)}
        # UTF-8 bytes are a deliberately conservative token upper bound, including system text.
        if len((SYSTEM + json.dumps(packet, ensure_ascii=False)).encode()) > 12000:
            raise ValueError("pilot packet exceeds conservative 12k input-token bound")
        packet["packet_sha256"] = _sha_json(packet)
        immutable_json(OUT / f"{packet['packet_sha256']}.packet.json", packet)
        packets.append(packet)
    plan = {"pilot_calls": len(packets), "full_disagreement_cases": len(cases),
            "case_families": dict(Counter(c["kind"] for c in cases)),
            "field_flags": dict(Counter(f for c in cases for f in c["fields"])),
            "packet_digests": [p["packet_sha256"] for p in packets], "model": "gpt-5.5",
            "reserved_token_ceiling": len(packets) * 50000, "concurrency": 1, "gold_accepted": False}
    immutable_json(OUT / "plan.json", plan)
    return packets, plan


async def execute(packets):
    db = sqlite3.connect(ROOT / "data/factory.sqlite", timeout=30)
    budget = ApprovalBudgetConfig(campaign_id="signal-desk-clean-corpus-2026-08-31",
        grant_path=ROOT / "config/signal_desk_rebuild_budget_grant.json", budget_dir=ROOT / "work/pif-ops/budget")
    try:
        async with CodexAppServerClient(command=["codex", "app-server", "--stdio", "--strict-config"], expected_cli_version="0.147.0") as client:
            for packet in packets:
                digest = packet["packet_sha256"]
                output = OUT / f"{digest}.decision.json"
                if output.exists():
                    validate_decisions(json.loads(output.read_text()), [packet["case"]["case_id"]])
                    continue
                key = "corrected-review-pilot-v1:" + digest
                reservation = reserve_call(db, task_key=key, budget=budget)
                if reservation.get("existing"):
                    raise RuntimeError("existing reservation without decision; inspect sidecar before any retry")
                usage = None
                try:
                    result = await client.run_ephemeral_structured_turn(model="gpt-5.5", effort="high",
                        base_instructions=SYSTEM, prompt=json.dumps(packet, ensure_ascii=False), output_schema=SCHEMA,
                        cwd=ROOT, sidecar_path=OUT / f"{digest}.sidecar.json", output_path=OUT / f"{digest}.output.json",
                        timeout_seconds=900)
                    usage = result.usage.total_tokens if result.usage else None
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    decisions = validate_decisions(result.output, [packet["case"]["case_id"]])
                    if packet["case"]["audit"] is None and any(d["decision"] in {"audit_supported", "both_supported"} for d in decisions.values()):
                        raise ValueError("judge supported a nonexistent audit candidate")
                    immutable_json(output, result.output)
                finally:
                    settle_call(db, reservation_id=reservation["reservation_id"], task_key=key, actual_tokens=usage)
    finally:
        db.close()
    counts = Counter()
    for packet in packets:
        output = json.loads((OUT / f"{packet['packet_sha256']}.decision.json").read_text())
        counts.update(d["decision"] for d in output["decisions"])
    immutable_json(OUT / "receipt.json", {"complete": True, "pilot_only": True,
        "decisions": dict(counts), "gold_accepted": False, "next": "inspect rationale and wider-context needs before expanding"})
    print(json.dumps({"pilot_complete": True, "decisions": dict(counts), "gold_accepted": False}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    packets, plan = prepare()
    print(json.dumps(plan), flush=True)
    if args.execute:
        asyncio.run(execute(packets))
