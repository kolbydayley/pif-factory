"""Retain unlinked useful context explicitly, without inventing parent claims."""
from copy import deepcopy
import hashlib
import json
from . import signal_desk_evidence_role_experiment as v1
from .signal_desk_full_event_experiment import _shape

FAMILY_ID = "signal-desk-evidence-role-v2"
RULES = v1.RULES + """
V2: context_parent_status is linked, missing, or not_applicable. For supporting
context with a supplied substantive parent use linked and context_for IDs. If a
useful caveat/example has no supplied substantive parent, use missing with empty
context_for and decision boundary. Explain what argument is missing in rationale;
do not invent an ID or a substantive claim merely to satisfy the link requirement.
This missing-parent exception supersedes the blanket supplied-parent requirement
above. Such context stays internally recoverable, not an independent signal or a
public orphan quote. All other roles require not_applicable and empty context_for.
In full extraction, include the real substantive parent when present in source;
do not use missing as a substitute for extracting it. Bare irrelevant maxims are
not useful context simply because a missing-parent option exists.
Theme music and show production/format chatter are promotion_housekeeping.
First-person research interests can be metadata, but a specific forecast or
mechanism expressed with that framing remains substantive with attributed scope.
No original candidate may disappear; this is not permission to hide denominators.
"""


def schema():
    result = deepcopy(v1.schema()); item = result["properties"]["decisions"]["items"]
    item["properties"]["context_parent_status"] = {"type": "string", "enum": ["linked", "missing", "not_applicable"]}
    item["required"] = list(item["properties"])
    return result


def validate(value, candidate_ids):
    _shape(value, schema())
    if any(not isinstance(x, str) or not x.strip() for x in candidate_ids) or len(set(candidate_ids)) != len(candidate_ids):
        raise ValueError("invalid input IDs")
    rows = value["decisions"]; by_id = {r["candidate_id"]: r for r in rows}
    if len(by_id) != len(rows) or set(by_id) != set(candidate_ids): raise ValueError("changed population")
    for row in rows:
        if any(not row[k].strip() for k in ("candidate_id", "rationale")):
            raise ValueError("empty ID or rationale")
        links = row["context_for"]
        if any(not x.strip() for x in links) or len(links) != len(set(links)): raise ValueError("invalid context links")
        if row["role"] == "supporting_context":
            if row["context_parent_status"] == "missing":
                if links or row["decision"] != "boundary": raise ValueError("missing parent requires unresolved boundary without invented links")
            elif row["context_parent_status"] == "linked":
                if not links or any(p not in by_id or p == row["candidate_id"] or by_id[p]["role"] != "substantive_claim" for p in links):
                    raise ValueError("context needs real substantive parent")
            else: raise ValueError("context must specify parent status")
        elif links or row["context_parent_status"] != "not_applicable":
            raise ValueError("only supporting context can have parent status")
        if (row["role"] == "research_limitation") != (row["needs"] != "none"):
            raise ValueError("source limitation requires recovery need")
    return value


def receipt():
    return {"family_id": FAMILY_ID, "parent": v1.receipt(),
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "explicit_missing_context_parent",
        "qualified": False, "production_enabled": False, "independent_source_review_required": True}
