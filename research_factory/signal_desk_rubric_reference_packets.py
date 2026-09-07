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
Return one decision per exact event_id, with source_basis explaining the concrete
source wording and any speaker boundary. needs_correction is not approval of a
proposed correction. supported requires an empty correction_json object; unresolved
requires an explicit missing_context explanation. Do not patch claims or evidence.
For strategically relevant, explain a plausible industry decision or change the
event informs; for incidental, explain why factual support alone is insufficient.
For an empty candidate window, explicitly judge whether it is genuinely empty of
extractable events; never pass it merely because no candidates were supplied.
The following shared contract governs semantic decisions:\n""" + TEXT


def reference_schema(packet):
    ids=[e["event_id"] for e in packet["candidate_events"]]
    return {"type":"object","additionalProperties":False,"required":["model","decisions","empty_window_verdict","empty_window_rationale"],"properties":{
        "model":{"type":"string","const":"gpt-5.5"},
        "decisions":{"type":"array","maxItems":len(ids),"items":{"type":"object","additionalProperties":False,
            "required":["event_id","verdict","source_basis","correction_json","missing_context","strategic_relevance","relevance_rationale"],
            "properties":{"event_id":{"type":"string", "enum":ids or ["no_candidates"]},
                "verdict":{"type":"string","enum":["supported","needs_correction","unresolved"]},
                "source_basis":{"type":"string","minLength":1},"correction_json":{"type":"string"},
                "missing_context":{"type":"string"},"strategic_relevance":{"type":"string","enum":["industry_relevant","incidental","undetermined"]},
                "relevance_rationale":{"type":"string","minLength":1}}}},
        "empty_window_verdict":{"type":"string","enum":["not_applicable","supported_empty","missed_events","unresolved"]},
        "empty_window_rationale":{"type":"string"}}}


def validate_reference(output,packet):
    if output.get("model")!="gpt-5.5" or not isinstance(output.get("decisions"),list):
        raise ValueError("reference response model/envelope invalid")
    expected={e["event_id"] for e in packet["candidate_events"]};seen=set()
    allowed={"speaker_id","attribution_type","quoted_person_id","mentioned_person_ids","stance"}
    for row in output["decisions"]:
        if row.get("event_id") not in expected or row["event_id"] in seen:
            raise ValueError("unexpected or duplicate reference event")
        seen.add(row["event_id"])
        if row.get("verdict") not in {"supported","needs_correction","unresolved"} or not row.get("source_basis"):
            raise ValueError("reference decision lacks verdict/source explanation")
        correction=json.loads(row.get("correction_json") or "{}")
        if not isinstance(correction,dict) or set(correction)-allowed:
            raise ValueError("reference correction exceeds field scope")
        if row["verdict"]=="supported" and correction:
            raise ValueError("supported verdict cannot silently change gold")
        if row["verdict"]=="unresolved" and not row.get("missing_context"):
            raise ValueError("unresolved reference requires explicit limitation")
        if row.get("strategic_relevance") not in {"industry_relevant","incidental","undetermined"} or not row.get("relevance_rationale"):
            raise ValueError("strategic relevance must be separately justified")
    if seen!=expected:raise ValueError("reference response incomplete")
    if expected:
        if output.get("empty_window_verdict")!="not_applicable":raise ValueError("nonempty window cannot pass empty check")
    elif output.get("empty_window_verdict") not in {"supported_empty","missed_events","unresolved"} or not output.get("empty_window_rationale"):
        raise ValueError("empty window requires explicit source judgment")
    return output


def bounded_groups(events, *, max_events, fits):
    """Greedy lossless packing; an oversized single event must fail closed."""
    if not 1 <= max_events <= 25:
        raise ValueError("reference batch limit must be between 1 and 25")
    if not events:
        if not fits([]): raise ValueError("reference packet oversized; no truncation permitted")
        return [[]]
    groups=[]; current=[]
    for event in events:
        candidate=current+[event]
        if len(candidate)<=max_events and fits(candidate):
            current=candidate
        else:
            if current: groups.append(current)
            current=[event]
            if not fits(current): raise ValueError("reference packet oversized; no truncation permitted")
    if current: groups.append(current)
    return groups


def build_reference_packets(*,plan,manifest,result_root,project_root,max_events=25,max_input_tokens=12000,token_count):
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
        def make_packet(group):
            return {"window_id":wid,"transcript_window":value["text"],"transcript_structure":rows[wid]["transcript_structure"],
                "window_alignment":rows[wid].get("alignment"),
                "text_sha256":rows[wid]["text_sha256"],"manifest_sha256":manifest["manifest_sha256"],
                "rubric_sha256":plan["rubric"]["sha256"],"candidate_events":group,
                "empty_window_review":not events,"c_sha256":inventory[f"{wid}:C"],"system_sha256":digest(REFERENCE_SYSTEM)}
        # Preserve source text and every event; shrink batches, never evidence.
        groups=bounded_groups(events,max_events=max_events,
            fits=lambda group: token_count(REFERENCE_SYSTEM+json.dumps(make_packet(group),ensure_ascii=False))+1500<=max_input_tokens)
        for group in groups:
            packet=make_packet(group)
            packet["packet_sha256"]=digest(packet);packets.append(packet)
    return {"packets":packets,"inventory":inventory,"rubric_qualified":False,"gold_accepted":False,
            "requires_independent_role_checks":True}
