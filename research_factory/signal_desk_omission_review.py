"""Source-bound diagnostic adjudication; never an acceptance or recall gate."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_experiment import _shape

SYSTEM = """Audit extraction disagreements against the COMPLETE supplied source.
All versions, including prior GPT-5.5 corrections, are fallible hypotheses, not
gold. Repetition across authors is not independent evidence. Do not rewrite gold.
For each case distinguish factual fidelity from research usefulness. A faithful
advertisement, community invitation, schedule, greeting, or incidental anecdote
can still be unsuitable intelligence. Do not exclude a consequential non-tech
claim merely because it is non-tech. Consequential means a substantive position,
explanation, forecast, decision, experience with generalizable implications, or
material factual development, not every literal sentence. Mark borderline
usefulness explicitly rather than manufacturing certainty.
Use supported for faithful paraphrases; stylistic enrichment, harmless equivalent
wording, and generic singular/plural are not material errors. Material errors
include unsupported/reversed meaning, changed modality/actor, or fabricated
attribution. Do not guess an unknown narrator from a quotation owner or metadata.
Negative evaluations express attitude even without a recommendation; factual
limitations alone need not express a negative attitude. Identify stance-label
ambiguity in the rationale; do not force a label repair to hide rubric ambiguity.
For correction cases, compare before and proposed versions separately and judge
whether the proposed change is necessary, equivalent, harmful, or unresolved.
The current_C_index is a comparison population, NOT an answer key. Find semantically
covering C IDs, allowing split/merge and paraphrase, rather than relying on lexical
matching. Missing supported consequential claims are potential omissions; missing
promotion is successful filtering. Coverage is separate from correctness of C's
other fields. Each decision cites verbatim unique source snippets sufficient to
check the reasoning. Do not quote model rationales as transcript evidence.
Return diagnosis only, no approval, no fabricated identities, no external facts.
"""


def schema():
    enums = {
        "verdict": ["supported", "material_error", "ambiguous"],
        "utility": ["consequential", "promotion_or_chrome", "incidental", "borderline"],
        "coverage": ["covered", "partial", "missing", "not_applicable"],
        "correction": ["not_applicable", "necessary", "equivalent", "harmful", "unresolved"],
    }
    fields = {k: {"type": "string", "enum": values} for k, values in enums.items()}
    fields.update({"case_id": {"type": "string"}, "rationale": {"type": "string", "minLength": 1},
        "current_C_ids": {"type": "array", "items": {"type": "string"}},
        "source_quotes": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}}})
    return {"type": "object", "additionalProperties": False, "required": ["decisions"],
        "properties": {"decisions": {"type": "array", "items": {"type": "object",
            "additionalProperties": False, "required": list(fields), "properties": fields}}}}


def validate(value, packet):
    _shape(value, schema())
    expected = {c["case_id"]: c for c in packet["cases"]}; seen = set()
    cids = {e["event_id"] for e in packet["current_C_index"]}
    for row in value["decisions"]:
        cid = row["case_id"]
        if cid not in expected or cid in seen: raise ValueError("unexpected/duplicate case")
        seen.add(cid)
        if len(set(row["current_C_ids"])) != len(row["current_C_ids"]) or not set(row["current_C_ids"]) <= cids:
            raise ValueError("invalid covering C IDs")
        if (row["coverage"] in {"covered", "partial"}) != bool(row["current_C_ids"]):
            raise ValueError("coverage requires exactly the applicable C references")
        is_correction = expected[cid]["kind"] == "correction"
        if is_correction == (row["correction"] == "not_applicable"):
            raise ValueError("correction decision missing or spurious")
        for quote in row["source_quotes"]:
            if not quote.strip() or packet["transcript_window"].count(quote) != 1:
                raise ValueError("source quote must be exact and uniquely locatable")
    if seen != set(expected): raise ValueError("missing case decision")
    return value


def packets(*, window_id, source, structure, cases, current, provenance, token_count):
    if not cases: raise ValueError("empty windows need an explicit empty-source case")
    if len({c["case_id"] for c in cases}) != len(cases): raise ValueError("duplicate case population")
    # Complete claims, spans, and attribution in cases; C index is only semantic
    # coverage context, not a replacement for a full event when adjudicating it.
    index = [{k: e[k] for k in ("event_id", "claim_text", "evidence_text")} for e in current]
    def build(rows):
        p = {"window_id": window_id, "transcript_window": source, "transcript_structure": structure,
            "cases": rows, "current_C_index": index, "provenance": provenance,
            "source_sha256": digest(source), "system_sha256": digest(SYSTEM),
            "schema_sha256": digest(schema()), "gold_accepted": False, "gate_eligible": False}
        p["packet_sha256"] = digest(p)
        return p
    def fits(p):
        return len(p["cases"]) <= 25 and token_count(SYSTEM + json.dumps(p, ensure_ascii=False) + json.dumps(schema())) + 1500 <= 12000
    result = []; rows = []
    for case in cases:
        if not fits(build(rows + [case])):
            if not rows: raise ValueError("full source and single case exceed token limit")
            result.append(build(rows)); rows = []
        rows.append(case)
        if not fits(build(rows)): raise ValueError("full source and single case exceed token limit")
    if rows: result.append(build(rows))
    return result
