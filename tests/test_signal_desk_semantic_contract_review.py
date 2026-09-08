from copy import deepcopy
import pytest
from scripts.pif_signal_desk_semantic_contract_review import validate


def fixture():
    packet = {"cases": [{"case_id": "__contract__"}, {"case_id": "c1"}],
        "transcript_window": "People will say no. I am Alex."}
    value = {"decisions": [{"case_id": key, "verdict": "supported",
        "rationale": "Hypothetical objections must not count as observed views.",
        "proposed_rule_change": "", "source_quotes": ["People will say no."]}
        for key in ("__contract__", "c1")]}
    return packet, value


def test_complete_population_and_exact_quotes():
    packet, value = fixture()
    assert validate(value, packet) == value


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "quote", "revision", "extra", "verdict"])
def test_bad_reviews_fail_closed(mutation):
    packet, value = fixture()
    row = value["decisions"][0]
    if mutation == "missing":
        value["decisions"].pop()
    elif mutation == "duplicate":
        value["decisions"].append(deepcopy(row))
    elif mutation == "quote":
        row["source_quotes"] = ["People said no."]
    elif mutation == "revision":
        row["verdict"] = "revise"
    elif mutation == "extra":
        row["accepted"] = True
    else:
        row["verdict"] = "accepted"
    with pytest.raises(ValueError):
        validate(value, packet)
