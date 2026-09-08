"""Candidate-ID joins across unqualified semantic families, never publication."""
from . import signal_desk_attribution_experiment as attribution
from . import signal_desk_evidence_role_v2 as role
from . import signal_desk_position_status_v2 as position
from . import signal_desk_voice_continuity_v2 as voice
from . import signal_desk_attitude_v2 as attitude
from .signal_desk_rubric_reference_packets import digest

RULES = """Join all semantic dimensions by the exact supplied candidate ID.
Every candidate must appear in every dimension, including uncertain, context and
promotional records. No index-order joins, partial populations or implicit defaults.
transcript_voice matches the separately grounded voice decision by surface name
and binding anchor. Proposition ownership remains separate: quoted/reported
people are not narrator identities. own_statement uses the same owner as voice,
including null. Non-transcript article prose cannot acquire a spoken narrator.
Anticipated positions may retain their imagined owner for argument comprehension,
but they do not route to observed personal views or add observed source breadth.
Target attitude is the specified proposition owner's evaluation, not the narrator's
automatic endorsement of a quotation. Unresolved ownership cannot be repaired by
borrowing a mentioned name. Evidence-role context links point to substantive
candidates in this same full population. Components never increase claim counts.
The join is an internal consistency check; all families and outputs are unqualified.
It does not adjudicate whether grounding truly supports the semantic labels.
"""


def validate(bundle, *, source, window_id, candidates):
    fields = {"attribution", "evidence_role", "position", "voice", "attitude"}
    if not isinstance(bundle, dict) or set(bundle) != fields:
        raise ValueError("incomplete semantic bundle")
    ids = list(candidates)
    if not isinstance(bundle["attribution"], dict) or set(bundle["attribution"]) != set(ids):
        raise ValueError("attribution population mismatch")
    role.validate(bundle["evidence_role"], ids)
    position.validate(bundle["position"], source=source, candidate_ids=ids)
    voice.validate(bundle["voice"], source=source, window_id=window_id, candidates=candidates)
    attitude.validate(bundle["attitude"], source=source, candidate_ids=ids)
    voices = {r["candidate_id"]: r for r in bundle["voice"]["decisions"]}
    for cid in ids:
        record = attribution.validate(bundle["attribution"][cid], source=source)
        speaker = record["transcript_voice"]; detail = voices[cid]
        if detail["voice_surface"] is None:
            if speaker is not None: raise ValueError("attribution invents voice absent from voice contract")
        elif speaker is None or speaker["kind"] != "person" or attribution.normalize(speaker["surface_name"]) != attribution.normalize(detail["voice_surface"]) or speaker["binding_span"] != detail["identity_anchor"]:
            raise ValueError("voice and attribution identity/anchor disagree")
    return bundle


def research_routes(bundle, *, source, window_id, candidates):
    """Diagnostic route proposals. Acceptance and real source breadth are external."""
    validate(bundle, source=source, window_id=window_id, candidates=candidates)
    positions = {r["candidate_id"]: r for r in bundle["position"]["decisions"]}
    roles = {r["candidate_id"]: r for r in bundle["evidence_role"]["decisions"]}
    voices = {r["candidate_id"]: r for r in bundle["voice"]["decisions"]}
    rows = []
    for cid in candidates:
        a = bundle["attribution"][cid]; owner = a["proposition_owner"]
        observed = positions[cid]["position_status"] == "actual_position"
        substantive = roles[cid]["role"] == "substantive_claim"
        # This is only eligibility if later accepted, never an observed count.
        person_path = None
        if observed and owner and owner["kind"] == "person" and roles[cid]["role"] not in {"promotion_housekeeping", "research_limitation"} and roles[cid]["context_parent_status"] != "missing":
            person_path = attribution.proposed_person_section(a, owner["surface_name"])
            if voices[cid]["source_kind"] != "spoken_transcript" and a["relation"] == "own_statement":
                person_path = None
        rows.append({"candidate_id": cid, "role": roles[cid]["role"],
            "observed_person_path_if_independently_accepted": person_path,
            "independent_claim_eligible_if_accepted": substantive and observed,
            "accepted": False, "counted_source_breadth": 0})
    return rows


def receipt():
    families = {"attribution": attribution.receipt(), "evidence_role": role.receipt(),
        "position": position.receipt(), "voice": voice.receipt(), "attitude": attitude.receipt()}
    return {"family_id": "signal-desk-semantic-join-v2", "families": families,
        "sha256": digest({"rules": RULES, "families": families}),
        "qualified": False, "production_enabled": False,
        "source_review_and_fresh_shared_qualification_required": True}
