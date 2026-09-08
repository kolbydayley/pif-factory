from copy import deepcopy

import pytest

from research_factory import signal_desk_position_status_experiment as experiment


def packet(source, status):
    return {"decisions": [{"candidate_id": "c1", "position_status": status,
        "source_evidence": [{"text": source, "start": 0, "end": len(source)}],
        "rationale": "Source-bound proposal, pending independent review."}]}


@pytest.mark.parametrize("source,status,eligible", [
    ("People will say that regulation is impossible.", "anticipated_position", False),
    ('Imagine Alex saying "I object".', "anticipated_position", False),
    ('Alex said "I object".', "actual_position", True),
    ("I think we should regulate.", "actual_position", True),
    ("The host alleged that the company broke the law.", "actual_position", True),
    ("They say it cannot work.", "indeterminate", False),
])
def test_proposal_population_and_research_routing(source, status, eligible):
    value = packet(source, status)
    assert experiment.validate(value, source=source, candidate_ids=["c1"]) == value
    assert experiment.proposed_observed_position_eligible(value["decisions"][0]) is eligible


def test_exact_grounding_is_not_semantic_approval():
    source = "Imagine Alex objecting."
    wrong = packet(source, "actual_position")
    # Deliberate semantic counterexample: a deterministic span checker cannot
    # adjudicate actuality. Independent review must reject this proposal.
    experiment.validate(wrong, source=source, candidate_ids=["c1"])
    assert experiment.receipt()["qualified"] is False
    assert experiment.receipt()["production_enabled"] is False


@pytest.mark.parametrize("mutation", ["offset", "boolean", "extra", "missing", "duplicate", "status"])
def test_invalid_contracts_fail(mutation):
    source = "Alex said no."
    value = packet(source, "actual_position")
    row = value["decisions"][0]
    if mutation == "offset":
        row["source_evidence"][0]["end"] -= 1
    elif mutation == "boolean":
        row["source_evidence"][0]["start"] = False
    elif mutation == "extra":
        row["accepted"] = True
    elif mutation == "missing":
        value["decisions"] = []
    elif mutation == "duplicate":
        value["decisions"].append(deepcopy(row))
    else:
        row["position_status"] = "verified_fact"
    with pytest.raises(ValueError):
        experiment.validate(value, source=source, candidate_ids=["c1"])
