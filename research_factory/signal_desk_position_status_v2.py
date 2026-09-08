"""Versioned clarification after source-bound review; v1 remains frozen."""
import hashlib
import json
from . import signal_desk_position_status_experiment as v1

FAMILY_ID = "signal-desk-position-status-v2"
RULES = v1.RULES + """
CLARIFICATIONS: A speaker's own forecast, conditional or counterfactual is an
actual_position when the source presents that speaker as expressing it. Future
tense alone does not mean anticipated_position. Anticipated positions are imagined
or predicted responses/objections/quotations attributed to other speakers, rather
than actually expressed views. If someone actually voices a hypothetical example,
that is their actual illustrative argument; the imagined character's position is
not an observed additional voice. Preserve this proposition-owner distinction.
Source-presented aggregate reactions and reported tweets are actual expressed
positions; that neither proves their factual content nor creates independently
identified speakers beyond what the source supplies. Do not manufacture new
candidates for unindexed mentions while classifying a fixed supplied population.
Evidence offsets are zero-based Unicode code-point indices into supplied source,
using half-open [start,end) ranges: end > start and source[start:end] == text.
"""
schema = v1.schema
validate = v1.validate
proposed_observed_position_eligible = v1.proposed_observed_position_eligible


def receipt():
    return {"family_id": FAMILY_ID, "parent": v1.receipt(),
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "clarify_position_actuality_without_schema_change",
        "qualified": False, "production_enabled": False,
        "semantic_source_review_required": True}
