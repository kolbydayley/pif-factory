import copy
import pytest
from research_factory.signal_desk_attitude_experiment import validate, receipt

SOURCE = "None of us support factory farming. It is horrible."


def span(text):
    start = SOURCE.index(text)
    return dict(text=text, start=start, end=start+len(text))


def value():
    return {"decisions": [dict(candidate_id="a", proposition_status="asserted",
        epistemic="certain", attitude="negative", target=span("factory farming"),
        evaluation_evidence=[span("horrible")], status="proposed",
        rationale="Asserts opposition, negative toward factory farming.")]}


def test_asserted_opposition_has_negative_target_attitude():
    result = validate(value(), source=SOURCE, candidate_ids=["a"])
    assert result["decisions"][0]["proposition_status"] == "asserted"
    assert result["decisions"][0]["attitude"] == "negative"


@pytest.mark.parametrize("field,replacement", [("target", None), ("evaluation_evidence", []), ("attitude", "supportive")])
def test_evaluation_requires_grounding_and_new_vocabulary(field, replacement):
    v = value(); v["decisions"][0][field] = replacement
    with pytest.raises(ValueError): validate(v, source=SOURCE, candidate_ids=["a"])


def test_exact_span_not_sufficient_semantic_proof():
    v = value(); v["decisions"][0]["attitude"] = "positive"
    # Validator checks grounding, not truth. A semantic judge must reject this.
    validate(v, source=SOURCE, candidate_ids=["a"])
    assert receipt()["semantic_source_review_required"]


def test_no_fabricated_target_or_boolean_offset():
    for change in [{"text": "Alice"}, {"start": True}]:
        v = value(); v["decisions"][0]["target"].update(change)
        with pytest.raises(ValueError): validate(v, source=SOURCE, candidate_ids=["a"])


def test_missing_duplicate_and_extra_candidates_fail():
    v = value()
    with pytest.raises(ValueError): validate(v, source=SOURCE, candidate_ids=["a", "b"])
    v["decisions"].append(copy.deepcopy(v["decisions"][0]))
    with pytest.raises(ValueError): validate(v, source=SOURCE, candidate_ids=["a"])


def test_indeterminate_requires_review():
    v = value(); v["decisions"][0]["epistemic"] = "indeterminate"
    with pytest.raises(ValueError): validate(v, source=SOURCE, candidate_ids=["a"])
    v["decisions"][0]["status"] = "needs_review"
    validate(v, source=SOURCE, candidate_ids=["a"])


def test_not_production_approved():
    assert receipt() == receipt()
    assert not receipt()["qualified"] and not receipt()["production_enabled"]
