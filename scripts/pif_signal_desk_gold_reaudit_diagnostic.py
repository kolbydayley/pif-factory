#!/usr/bin/env python3
"""Bounded diagnostic of re-audit churn; never a release or gold-acceptance gate."""
import asyncio
import hashlib
import json
import sys
from collections import Counter,defaultdict
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_gold_review_corrected as review
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_gold_audit import _load_frozen_window_text

R=review.R
OUT=R/"reaudit-churn-diagnostic-v1"


def cohort(case, previous, verdict):
    if previous is None:return "newly_disputed_gold_events"
    if previous["gold"]!=case["gold"]:return "changed_gold_disputed_again"
    if not set(case["fields"])<=set(previous["fields"]):return "unchanged_gold_new_disputed_fields"
    return "unchanged_gold_same_or_subset_fields_"+verdict


def prepare():
    import tiktoken
    enc=tiktoken.get_encoding("o200k_base")
    new=R/"repaired-dev-reaudit-v1"
    a=json.loads((new/"audit.json").read_text());old=json.loads((review.A/"audit.json").read_text())
    manifest=json.loads((R/"merged-manifest.json").read_text())
    if a["manifest_sha256"]!=manifest["manifest_sha256"]:raise ValueError("manifest mismatch")
    kw={"manifest_path":R/"merged-manifest.json","project_root":ROOT}
    oldcases=review.build_cases(**kw,result_root=review.A/"results",selected_window_ids=old["audit_selection"]["window_ids"])
    newcases=review.build_cases(**kw,result_root=new/"results",selected_window_ids=a["audit_selection"]["window_ids"])
    prev={(c["window_id"],c["gold_index"]):c for c in oldcases}
    decisions=json.loads((R/"corrected-review-reconciliation-v1/decisions.json").read_text())
    grouped=defaultdict(list)
    for c in newcases:
        p=prev.get((c["window_id"],c["gold_index"]))
        label=cohort(c,p,decisions[p["case_id"]]["decision"] if p else "")
        grouped[label].append(c)
    rows={w["window_id"]:w for w in manifest["windows"] if w["split"]=="development"}
    selected=[]
    # Bounded, deterministic and show-diverse within each error family.
    for label,cases in sorted(grouped.items()):
        seen=set(); ordered=[]
        ranked=sorted(cases,key=lambda c:review._sha_json(["churn-v1",c["case_id"]]))
        for c in ranked:
            show=rows[c["window_id"]]["show_id"]
            if show not in seen:ordered.append(c);seen.add(show)
        ordered.extend(c for c in ranked if c not in ordered)
        selected.extend((label,c) for c in ordered[:6])
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    packets=[]; selection=[]
    for label,c in selected:
        row=rows[c["window_id"]];path=Path(row["transcript_path"])
        text=(path if path.is_absolute() else ROOT/path).read_text()
        if hashlib.sha256(text.encode()).hexdigest()!=row["transcript_sha256"]:raise ValueError("source changed")
        start,end=int(row["start_char"]),int(row["end_char"])
        lo,hi=max(0,start-6000),min(len(text),end+6000)
        packet={"case":review.canonical_fields(c),"transcript_window":_load_frozen_window_text(row,project_root=ROOT),
            "text_sha256":row["text_sha256"],"transcript_structure":row["transcript_structure"],
            "manifest_sha256":manifest["manifest_sha256"],"audit_receipt_sha256":review._sha_json(a),
            "system_sha256":review._sha_json(review.SYSTEM),"wider_context_retry":{
                "start_char":lo,"end_char":hi,"text":text[lo:hi],"original_window_start_char":start,
                "source_length":len(text),"transcript_sha256":row["transcript_sha256"],
                "warning":"Only this contiguous neighborhood is supplied, not full transcript. Do not invent missing labels or infer speaker by conversational alternation."}}
        if len(enc.encode(review.SYSTEM+json.dumps(packet,ensure_ascii=False)))+1500>12000:raise ValueError("diagnostic packet too large")
        packet["packet_sha256"]=review._sha_json(packet)
        immutable_json(OUT/f"{packet['packet_sha256']}.packet.json",packet);packets.append(packet)
        selection.append({"case_id":c["case_id"],"cohort":label,"show_id":row["show_id"]})
    immutable_json(OUT/"selection.json",selection)
    plan={"packet_digests":[p["packet_sha256"] for p in packets],"oversized_pending":[],"diagnostic_only":True,
        "cohort_counts":{k:len(v) for k,v in grouped.items()},"selected":len(packets),"gold_accepted":False,
        "warning":"Purposive stratified diagnostic, not prevalence estimate or gate sample; no gold changes."}
    immutable_json(OUT/"plan.json",plan)
    print(json.dumps({k:v for k,v in plan.items() if k!="packet_digests"}),flush=True)
    return packets


if __name__=="__main__":
    review.OUT=OUT
    review.CONTRACT_VERSION=2
    review.FULL_REVIEW=True
    review.SYSTEM += """\nDIAGNOSTIC FIELD CONTRACT: attribution_type means speech attribution, never job role.
A known utterer's own assertion remains their speech even when it mentions someone else;
the subject is recorded separately in mentioned_person_ids. Unknown actual speaker is null
and unresolved_speaker. Do not assign a mentioned person as utterer. Stance is expressed
attitude toward this specific claim, not mere assertion or sentiment elsewhere. An unmatched
gold event must still be a consequential supported assertion, not a question or ad. This is
a fixed bounded diagnostic: choose uncertain when context or rubric cannot justify a choice.
Rationale must identify source wording and whether the issue is missing extraction, split/merge
matching, attribution semantics, unsupported identity, or stance ambiguity. No previous judge
verdict is supplied; assess source independently. No gate or gold acceptance follows this run."""
    packets=prepare()
    if "--execute" in sys.argv:asyncio.run(review.execute(packets))
