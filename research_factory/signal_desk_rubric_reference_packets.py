"""Source-bound reference-qualification packets, distinct from full gold audit.

Requires every selected A/B/C output; never substitutes a smaller completed set.
Packets use the same 6k source contract and shared rubric, no prior judge answers.
No scorer comparisons or acceptance are inferred from successful preparation.
"""
import hashlib
import json
from pathlib import Path
from .signal_desk_gold_shared_rubric import TEXT,receipt as rubric_receipt
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_gold_audit import _load_frozen_window_text


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()


REFERENCE_SYSTEM = """You are GPT-5.5 adjudicating a bounded development gold reference under a shared rubric.
Read the original window and evaluate EACH supplied Gold-C event independently.
The author is not an authority. Verify proposition, speech attribution, stance and
contamination. Return supported, needs_correction, or unresolved, plus a precise
source-grounded explanation. A supported but strategically incidental fact is not
automatically useful: classify strategic relevance separately, without deleting it
from the benchmark denominator. Never manufacture identity or evidence. Requests
for missing context remain unresolved. Do not infer approval of Gold A/B or corpus
quality from the candidates supplied in this packet. This run creates reference
proposals only; no automatic gold/rubric acceptance follows.
The following shared contract governs semantic decisions:\n""" + TEXT


def build_reference_packets(*,plan,manifest,result_root,project_root,max_events=4,max_input_tokens=12000,token_count):
    if plan["rubric"] != rubric_receipt():
        raise ValueError("qualification rubric changed")
    if plan["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("manifest mismatch")
    ids=plan["window_ids"]
    if len(ids)!=16 or len(set(ids))!=16:
        raise ValueError("qualification requires the complete frozen 16 windows")
    rows={w["window_id"]:w for w in manifest["windows"] if w["split"]=="development"}
    if not set(ids)<=set(rows):
        raise ValueError("non-development qualification item")
    # Validate the entire inventory before yielding or dispatching even one call.
    validated={};inventory={}
    for wid in ids:
        text=_load_frozen_window_text(rows[wid],project_root=project_root)
        validated[wid]={"text":text}
        for phase in ("A","B","C"):
            path=Path(result_root)/phase/f"{wid}.json"
            if not path.exists():raise ValueError(f"qualification incomplete: missing {phase} for {wid}")
            value=json.loads(path.read_text())
            validate_output(value,transcript_window=text,expected_window_id=wid)
            validated[wid][phase]=value
            inventory[f"{wid}:{phase}"]=digest(value)
    packets=[]
    for wid in ids:
        value=validated[wid]
        events=value["C"]["events"]
        # Empty windows still need explicit independent reference review.
        groups=[events[i:i+max_events] for i in range(0,len(events),max_events)] or [[]]
        for group in groups:
            packet={"window_id":wid,"transcript_window":value["text"],"transcript_structure":rows[wid]["transcript_structure"],
                "text_sha256":rows[wid]["text_sha256"],"manifest_sha256":manifest["manifest_sha256"],
                "rubric_sha256":plan["rubric"]["sha256"],"candidate_events":group,
                "empty_window_review":not events,"c_sha256":inventory[f"{wid}:C"],"system_sha256":digest(REFERENCE_SYSTEM)}
            if token_count(REFERENCE_SYSTEM+json.dumps(packet,ensure_ascii=False))+1500>max_input_tokens:
                raise ValueError("reference packet oversized; no truncation permitted")
            packet["packet_sha256"]=digest(packet);packets.append(packet)
    return {"packets":packets,"inventory":inventory,"rubric_qualified":False,"gold_accepted":False,
            "requires_independent_role_checks":True}
