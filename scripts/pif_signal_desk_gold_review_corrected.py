#!/usr/bin/env python3
"""Six-case, source-bound GPT-5.5 pilot before full corrected audit adjudication."""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
import json
import hashlib
import re
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
CONTRACT_VERSION = 1
FULL_REVIEW = False
FIELD_NAMES = {"speaker": "speaker_id", "speaker_role": "attribution_type",
               "quoted_person": "quoted_person_id", "mentioned_people": "mentioned_person_ids",
               "stance": "stance", "unsupported_attribution": "attribution_support",
               "event_presence": "event_presence"}
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


def canonical_fields(case):
    result = json.loads(json.dumps(case))
    result["fields"] = [FIELD_NAMES[f] for f in case["fields"]]
    for key in ("gold", "audit"):
        if result[key] is not None:
            for unused in ("speaker_role", "quoted_person", "mentioned_people"):
                result[key].pop(unused, None)
    return result


def wider_context(row):
    path = Path(row["transcript_path"])
    text = (path if path.is_absolute() else ROOT / path).read_text()
    if hashlib.sha256(text.encode()).hexdigest() != row["transcript_sha256"]:
        raise ValueError("wider transcript hash mismatch")
    start, end = int(row["start_char"]), int(row["end_char"])
    marks = [m.start() for m in re.finditer(r"(?m)(?:^|\n)\s*[A-Za-z][A-Za-z .,'’-]{1,65}:\s*", text)]
    before = [m for m in marks if m < start]
    after = [m for m in marks if m >= end]
    lo = before[-3] if len(before) >= 3 else 0
    hi = after[3] if len(after) >= 4 else len(text)
    return {"start_char": lo, "end_char": hi, "original_window_start_char": start,
            "text": text[lo:hi], "transcript_sha256": row["transcript_sha256"],
            "offset_rule": "candidate evidence offsets remain relative to original transcript_window"}


def packet_cases(packet):
    return packet["cases"] if "cases" in packet else [packet["case"]]


def batch_packets(packets):
    """Same source/context only, maximum four independent case decisions per call."""
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    groups = []
    for packet in packets:
        common = {k: v for k, v in packet.items() if k not in {"case", "packet_sha256"}}
        candidate = {**common, "cases": [packet["case"]]}
        if groups:
            previous = {k: v for k, v in groups[-1].items() if k != "cases"}
            trial = {**common, "cases": groups[-1]["cases"] + [packet["case"]]}
            if previous == common and len(trial["cases"]) <= 4 and len(enc.encode(SYSTEM + json.dumps(trial, ensure_ascii=False))) + 1500 <= 12000:
                groups[-1] = trial
                continue
        if len(enc.encode(SYSTEM + json.dumps(candidate, ensure_ascii=False))) + 1500 > 12000:
            raise ValueError("batch framing exceeded input bound")
        groups.append(candidate)
    for group in groups:
        group["packet_sha256"] = _sha_json(group)
    return groups


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
    if FULL_REVIEW:
        chosen = sorted(cases, key=lambda c: (c["window_id"], c["case_id"]))
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    packets = []
    oversized = []
    for case in chosen:
        row = metadata[case["window_id"]]
        packet = {"case": canonical_fields(case) if CONTRACT_VERSION == 2 else case, "transcript_structure": row["transcript_structure"],
                  "transcript_window": _load_frozen_window_text(row, project_root=ROOT),
                  "text_sha256": row["text_sha256"], "manifest_sha256": manifest["manifest_sha256"],
                  "audit_receipt_sha256": audit["receipt_sha256"], "system_sha256": _sha_json(SYSTEM)}
        if CONTRACT_VERSION == 2:
            packet["field_contract"] = {"attribution_type": "speech attribution class, NEVER occupation or professional role",
                "speaker_id": "person who spoke, not a mentioned person", "attribution_support": "identity is supported by supplied source context"}
            if "speaker" in case["fields"] or "unsupported_attribution" in case["fields"]:
                packet["wider_context_retry"] = wider_context(row)
        # UTF-8 bytes are a deliberately conservative token upper bound, including system text.
        encoded_input = SYSTEM + json.dumps(packet, ensure_ascii=False)
        if CONTRACT_VERSION == 2:
            import tiktoken
            input_tokens = len(tiktoken.get_encoding("o200k_base").encode(encoded_input)) + 1500
        else:
            input_tokens = len(encoded_input.encode())
        if input_tokens > 12000:
            if FULL_REVIEW:
                oversized.append({"case_id": case["case_id"], "window_id": case["window_id"],
                                  "input_tokens_with_allowance": input_tokens,
                                  "reason": "wider_context_requires_bounded_repack_before_review"})
                continue
            raise ValueError("pilot packet exceeds conservative 12k input-token bound")
        packet["packet_sha256"] = _sha_json(packet)
        immutable_json(OUT / f"{packet['packet_sha256']}.packet.json", packet)
        packets.append(packet)
    eligible_case_count = len(packets)
    if FULL_REVIEW:
        packets = batch_packets(packets)
        for packet in packets:
            immutable_json(OUT / f"{packet['packet_sha256']}.packet.json", packet)
    plan = {"contract_version": CONTRACT_VERSION, "full_review": FULL_REVIEW, "eligible_case_count": eligible_case_count,
            "pilot_calls": len(packets), "full_disagreement_cases": len(cases), "oversized_pending": oversized,
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
                    validate_decisions(json.loads(output.read_text()), [c["case_id"] for c in packet_cases(packet)])
                    continue
                key = f"corrected-review-{'full' if FULL_REVIEW else 'pilot'}-v{CONTRACT_VERSION}:" + digest
                reservation = reserve_call(db, task_key=key, budget=budget)
                if reservation.get("existing"):
                    # Preserve paid output. Invalid case coverage remains pending,
                    # never accepted or silently retried at additional cost.
                    sidecar = json.loads((OUT / f"{digest}.sidecar.json").read_text())
                    raw = (OUT / f"{digest}.output.json").read_text()
                    if (sidecar.get("state") != "completed" or sidecar.get("model") != "gpt-5.5"
                            or sidecar.get("prompt_sha256") != hashlib.sha256(json.dumps(packet, ensure_ascii=False).encode()).hexdigest()
                            or sidecar.get("output_sha256") not in {hashlib.sha256(raw.encode()).hexdigest(), hashlib.sha256(raw.rstrip("\n").encode()).hexdigest()}):
                        raise RuntimeError("saved response provenance mismatch; manual recovery required")
                    try:
                        validate_packet_decisions(json.loads(raw), packet)
                    except ValueError as exc:
                        immutable_json(OUT / f"{digest}.pending.json", {"packet_sha256": digest, "reason": str(exc), "gold_accepted": False})
                        continue
                    immutable_json(output, json.loads(raw))
                    continue
                usage = None
                try:
                    result = await client.run_ephemeral_structured_turn(model="gpt-5.5", effort="high",
                        base_instructions=SYSTEM, prompt=json.dumps(packet, ensure_ascii=False), output_schema=SCHEMA,
                        cwd=ROOT, sidecar_path=OUT / f"{digest}.sidecar.json", output_path=OUT / f"{digest}.output.json",
                        timeout_seconds=900)
                    usage = result.usage.total_tokens if result.usage else None
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    try:
                        validate_packet_decisions(result.output, packet)
                    except ValueError as exc:
                        immutable_json(OUT / f"{digest}.pending.json", {"packet_sha256": digest, "reason": str(exc), "gold_accepted": False})
                        continue
                    immutable_json(output, result.output)
                finally:
                    settle_call(db, reservation_id=reservation["reservation_id"], task_key=key, actual_tokens=usage)
    finally:
        db.close()
    counts = Counter()
    pending = []
    for packet in packets:
        if not (OUT / f"{packet['packet_sha256']}.decision.json").exists():
            pending.append(packet['packet_sha256'])
            continue
        output = json.loads((OUT / f"{packet['packet_sha256']}.decision.json").read_text())
        counts.update(d["decision"] for d in output["decisions"])
    plan = json.loads((OUT / "plan.json").read_text())
    immutable_json(OUT / "receipt.json", {"complete": not plan["oversized_pending"] and not pending, "pilot_only": not FULL_REVIEW,
        "reviewed_cases": sum(counts.values()), "pending_batches": pending, "oversized_pending": len(plan["oversized_pending"]),
        "decisions": dict(counts), "gold_accepted": False, "next": "inspect rationale and wider-context needs before expanding"})
    print(json.dumps({"eligible_review_complete": not pending, "all_cases_complete": not plan["oversized_pending"] and not pending, "pending_batches": len(pending), "decisions": dict(counts), "gold_accepted": False}))


def validate_packet_decisions(output, packet):
    decisions = validate_decisions(output, [c["case_id"] for c in packet_cases(packet)])
    for case in packet_cases(packet):
        if case["audit"] is None and decisions[case["case_id"]]["decision"] in {"audit_supported", "both_supported"}:
            raise ValueError("judge supported a nonexistent audit candidate")
    return decisions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--contract-v2", action="store_true")
    parser.add_argument("--full", action="store_true", help="review all corrected disagreement cases")
    args = parser.parse_args()
    if args.contract_v2:
        CONTRACT_VERSION = 2
        OUT = R / "corrected-review-pilot-v2"
        SYSTEM += """\nFIELD CONTRACT CORRECTION: Judge only the actual fields listed in case.fields.
attribution_type means direct_speech, quoted_speech, reported_paraphrase, third_party_mention,
or unresolved_speaker. It NEVER means occupation, professional role, or job title.
We do not supply any speaker_role property. A host directly asserting a fact about a third
person is not automatically that third person's speech. Decide the utterance's attribution,
while preserving people merely mentioned in mentioned_person_ids.
Some pilot cases have a wider_context_retry with up to three source turns around the original
window (or full source when no turns exist). Use it for attribution before choosing an unresolved
speaker. Evidence offsets still index the unchanged original window, not the wider text.
When a supported source label is before the window, the window clipping is not grounds to
reject that identity. Mention alone is not a speaker label. If still indeterminable after the
wider retry, say uncertain and explicitly state that wider context was reviewed.
Your rationale MUST identify the actual disputed field and its competing values; never declare
both_supported because a nonexistent field is null. Both_supported requires BOTH actual
disputed values to be defensible. Do not change factual claims to make labels agree."""
    if args.full:
        if not args.contract_v2:
            parser.error("full review requires the corrected v2 contract")
        FULL_REVIEW = True
        OUT = R / "corrected-review-full-batched-v2"
    packets, plan = prepare()
    print(json.dumps({k: v for k, v in plan.items() if k not in {"packet_digests", "oversized_pending"}}), flush=True)
    if args.execute:
        asyncio.run(execute(packets))
