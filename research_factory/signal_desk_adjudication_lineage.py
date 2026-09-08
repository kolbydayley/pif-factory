"""Experimental C envelope: preserve input accountability, not duplicate events."""
from . import signal_desk_full_event_v5 as records
from . import signal_desk_full_event_v5_prompts as author_prompts
from .signal_desk_rubric_reference_packets import digest

VERSION = "pif_signal_desk_adjudication_lineage_v1"
RULES = """ADJUDICATION LINEAGE: The canonical event set contains distinct research
records, not the union of A and B IDs. Merge equivalent propositions, including
paraphrases with different evidence spans. Do not relabel a redundant claim as
supporting_context just to preserve its input ID. Distinct propositions can share
evidence; do not merge them merely because their spans match. Preserve actual
qualifications, disagreements, attribution uncertainty and useful question context.
The family rules about preserving candidate IDs apply to the nested assignments
of CANONICAL OUTPUT events, not to retaining every A/B input as a separate event.
Account for EVERY author/input_event_id exactly once in input_dispositions:
retained or corrected maps to one output; merged maps to one output also reached
by another input; split maps to multiple outputs; rejected maps to none and must
explain why. An unresolved input maps to an uncertain/quarantined output, never
silently disappears. Each output must have input lineage or an explicit
source-discovered omission entry in additions. An addition cannot also be mapped
from an input. IDs may coincide across A and B: author plus ID is the input key.
Use exact source evidence under the unchanged semantic-record contract. This
lineage ledger is accountability, not new evidence, approval or an extra claim.
"""


def schema():
    string = {"type":"string", "minLength":1}
    def obj(properties):
        return {"type":"object", "additionalProperties":False,
                "required":list(properties), "properties":properties}
    return obj({"schema_version":{"const":VERSION,"type":"string"},
        "records":records.schema(),
        "input_dispositions":{"type":"array","items":obj({
            "author":{"type":"string","enum":["A","B"]}, "input_event_id":string,
            "action":{"type":"string","enum":["retained","corrected","merged","split","rejected","unresolved"]},
            "output_event_ids":{"type":"array","items":string},"reason":string})},
        "additions":{"type":"array","items":obj({"output_event_id":string,"reason":string})}})


def validate(value, *, source, window_id, author_a, author_b):
    if not isinstance(value,dict) or set(value)!={"schema_version","records","input_dispositions","additions"} or value["schema_version"]!=VERSION:
        raise ValueError("invalid lineage envelope")
    for field, keys in (("input_dispositions",{"author","input_event_id","action","output_event_ids","reason"}),
                        ("additions",{"output_event_id","reason"})):
        if not isinstance(value[field],list): raise ValueError("lineage arrays required")
        for row in value[field]:
            if not isinstance(row,dict) or set(row)!=keys: raise ValueError("invalid lineage row")
            if any(not isinstance(row[k],str) or not row[k] for k in keys-{"output_event_ids"}):
                raise ValueError("lineage strings required")
            if field=="input_dispositions":
                if row["author"] not in {"A","B"} or row["action"] not in {"retained","corrected","merged","split","rejected","unresolved"}:
                    raise ValueError("invalid lineage action or author")
                if not isinstance(row["output_event_ids"],list) or any(not isinstance(i,str) or not i for i in row["output_event_ids"]):
                    raise ValueError("output IDs required")
    authors = {"A":author_a,"B":author_b}
    for original in authors.values(): records.validate(original,source=source,window_id=window_id)
    records.validate(value["records"],source=source,window_id=window_id)
    expected={(a,e["event_id"]) for a,v in authors.items() for e in v["events"]}
    outputs={e["event_id"]:e for e in value["records"]["events"]}
    seen=set(); mapped={}; merged=[]
    for row in value["input_dispositions"]:
        key=(row["author"],row["input_event_id"]); ids=row["output_event_ids"]; action=row["action"]
        if key not in expected or key in seen: raise ValueError("unknown or duplicate input lineage")
        seen.add(key)
        if not row["reason"].strip() or len(ids)!=len(set(ids)) or not set(ids)<=outputs.keys():
            raise ValueError("invalid lineage reason or output references")
        count=len(ids)
        if (action=="rejected" and count!=0 or action=="split" and count<2 or
            action not in {"rejected","split"} and count!=1): raise ValueError("action cardinality mismatch")
        if action=="unresolved" and any(outputs[i]["publishability_state"] not in {"uncertain","quarantined"} for i in ids):
            raise ValueError("unresolved input cannot produce candidate")
        for i in ids: mapped.setdefault(i,set()).add(key)
        if action=="merged": merged.extend(ids)
    if seen!=expected: raise ValueError("missing input lineage")
    if any(len(mapped[i])<2 for i in merged): raise ValueError("merged output lacks multiple inputs")
    additions=set()
    for row in value["additions"]:
        i=row["output_event_id"]
        if i not in outputs or i in additions or i in mapped or not row["reason"].strip():
            raise ValueError("invalid or duplicated addition")
        additions.add(i)
    if set(mapped)|additions != set(outputs): raise ValueError("unaccounted output")
    return value


def receipt():
    return {"version":VERSION,"schema_sha256":digest(schema()),"rules_sha256":digest(RULES),
            "system_sha256":digest(system()),
            "records_contract":records.receipt(),"qualified":False,"gold_accepted":False,
            "semantic_dedup_requires_independent_source_review":True}


def system():
    return author_prompts.prompts()["C"]+"\n"+RULES+"\nReturn the lineage envelope, with the unchanged v5 full-event response nested under records."


def packet(source_packet, *, author_a, author_b):
    # Reuse exact source/parent validation; never expose blind-audit answers.
    original=author_prompts.packet(source_packet,"C",author_a=author_a,author_b=author_b)
    result={k:v for k,v in original.items() if k not in {"packet_sha256","system_sha256","schema_sha256","schema_version"}}
    result.update(schema_version=VERSION,system_sha256=digest(system()),schema_sha256=digest(schema()),
                  predecessor_packet_sha256=original["packet_sha256"])
    result["packet_sha256"]=digest(result)
    return result
