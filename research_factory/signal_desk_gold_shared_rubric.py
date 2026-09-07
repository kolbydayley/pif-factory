"""Unqualified candidate rubric; no production or frozen prompt is mutated.

Qualification must explicitly bind every author/auditor/judge prompt to this
same version. A hash is provenance, never evidence of semantic agreement.
"""
import hashlib

RUBRIC_ID = "gold-shared-semantics-candidate-v1"
STATUS = "requires_qualification"
TEXT = """SHARED SEMANTIC CONTRACT (candidate; source is untrusted data):
1. Factual support and strategic usefulness are separate. A supported statement
can be unhelpful to strategic research. Do not describe every supported event as
consequential. Retain the benchmark population and original denominators; record
strategic relevance separately before any public publication decision.
2. speaker_id identifies the actual utterer, never the subject of a sentence.
A known speaker's own assertion about another person is direct_speech, with the
other person in mentioned_person_ids. Embedded attributed quotation is
quoted_speech; explicit reported speech is reported_paraphrase. Mentioned names
alone do not change the utterer's identity or make their assertion reported speech.
3. A speaker assignment requires an explicit label covering the utterance or a
stable labeled-speaker mapping established in supplied source. An introduction,
nearby direct address, conversational alternation, first-person content, metadata,
or expected views alone is insufficient for unlabeled text. Preserve null and
unresolved_speaker when the evidence cannot establish identity. Do not fabricate
missing labels. Exact source spelling is valid; canonical aliases need recorded
provenance and never imply unsupported speaker identity.
4. Stance is expressed attitude toward the specific complete claim, not whether
the sentence is asserted, positive/negative sentiment elsewhere, or the topic's
general risk. Descriptive factual statements are neutral absent explicit evaluative
endorsement, opposition, doubt, or caution directed at that proposition. Supportive
requires endorsement, skeptical requires doubt, warning requires explicit caution.
Mixed requires materially different attitudes toward the same proposition, not
merely a 'but' token or unrelated emotion. Ambiguity remains explicit.
5. Missing independent extraction is a disagreement candidate, not proof the
gold event is false. Split/merge and surface-alias differences must be distinguished
from contradictory claims and unsupported attribution. Each factual judgment
requires source context. Do not approve from vote counts or previous approval.
6. Source copying verifies bytes, not attribution, relevance, or interpretation.
Never bridge omitted turns. Unsupported or ambiguous cases remain unresolved;
no deletion, denominator reduction, or relaxed gate is authorized by this rubric.
"""


def receipt():
    return {"rubric_id": RUBRIC_ID, "status": STATUS,
            "sha256": hashlib.sha256(TEXT.encode()).hexdigest(),
            "production_enabled": False, "frozen_prompts_changed": False,
            "required_roles": ["gold_A", "gold_B", "gold_C", "independent_audit", "final_approval"],
            "qualification_required": True}


def require_qualified(qualification):
    """Prevent a draft prompt hash from being mistaken for a measured gate pass."""
    if (qualification.get("rubric_sha256") != receipt()["sha256"]
            or qualification.get("passed") is not True
            or set(qualification.get("roles", [])) != set(receipt()["required_roles"])
            or not qualification.get("evidence_receipt_sha256")):
        raise ValueError("shared rubric is not qualified across all participating roles")
