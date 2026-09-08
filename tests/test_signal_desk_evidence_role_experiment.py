import copy
import pytest
from research_factory.signal_desk_evidence_role_experiment import validate, receipt


def row(key, role="substantive_claim", **kw):
    return dict(candidate_id=key, role=role, decision="proposed", context_for=[],
                scope="attributed_view", needs="none", rationale="Source-bound proposal.", **kw)


def test_context_does_not_remove_original_candidate():
    a, b = row("claim"), row("caveat", "supporting_context")
    b["context_for"] = ["claim"]
    result = validate({"decisions": [a, b]}, ["claim", "caveat"])
    assert len(result["decisions"]) == 2


@pytest.mark.parametrize("keys", [["a"], ["a", "a"], ["a", "foreign"]])
def test_no_missing_duplicate_or_foreign_outputs(keys):
    with pytest.raises(ValueError, match="population"):
        validate({"decisions": [row(x) for x in keys]}, ["a", "b"])


@pytest.mark.parametrize("parent", ["unknown", "context", "metadata"])
def test_context_links_cannot_cycle_or_invent_parent(parent):
    c = row("context", "supporting_context"); c["context_for"] = [parent]
    with pytest.raises(ValueError):
        validate({"decisions": [c, row("metadata", "voice_source_metadata")]}, ["context", "metadata"])


def test_limitation_requires_explicit_evidence_recovery():
    value = {"decisions": [row("a", "research_limitation")]}
    with pytest.raises(ValueError): validate(value, ["a"])
    value["decisions"][0]["needs"] = "wider_context"
    validate(value, ["a"])


def test_boundary_is_not_automatically_a_source_failure():
    a = row("a"); a["decision"] = "boundary"
    validate({"decisions": [a]}, ["a"])


def test_firsthand_and_interested_party_are_not_forced_to_promotion():
    for scope in ["firsthand_account", "interested_party", "reported_hearsay"]:
        a = row("a"); a["scope"] = scope
        validate({"decisions": [a]}, ["a"])


def test_no_approval_field_or_accepted_role():
    value = {"decisions": [row("a")]}
    bad = copy.deepcopy(value); bad["accepted"] = True
    with pytest.raises(ValueError): validate(bad, ["a"])
    bad = copy.deepcopy(value); bad["decisions"][0]["role"] = "accepted"
    with pytest.raises(ValueError): validate(bad, ["a"])
    assert not receipt()["qualified"] and not receipt()["production_enabled"]


def test_contract_receipt_is_deterministic():
    assert receipt() == receipt()
    assert receipt()["changed_dimension"] == "research_usefulness_roles_only"
