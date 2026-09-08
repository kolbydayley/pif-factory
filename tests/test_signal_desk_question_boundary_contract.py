"""Boundary-unit tests, not model-quality or gold-acceptance evidence."""
from copy import deepcopy
import pytest
from research_factory import signal_desk_question_boundary_contract as contract


def record(role='supporting_context'):
    return {'schema_version': contract.VERSION, 'events': [{
        'speech_act': 'question', 'evidence_role': {'role': role},
        'attitude': {'proposition_status': 'questioned'}}]}


@pytest.mark.parametrize('role', sorted(contract.QUESTION_ROLES))
def test_question_boundary_delegates_all_parent_checks_without_mutation(monkeypatch, role):
    value = record(role)
    before = deepcopy(value)
    calls = []
    def parent(projected, *, source, window_id):
        calls.append((projected, source, window_id))
    monkeypatch.setattr(contract.previous.parent, 'validate', parent)
    assert contract.validate(value, source='source', window_id='window') == before
    assert value == before
    assert len(calls) == 1
    projected, source, wid = calls[0]
    assert source == 'source' and wid == 'window'
    assert projected['events'][0]['evidence_role']['role'] == role
    assert projected['events'][0]['speech_act'] == 'assertion'
    assert projected['events'][0]['attitude']['proposition_status'] == 'questioned'
    assert projected['schema_version'] == contract.previous.parent.VERSION


@pytest.mark.parametrize('role', ['substantive_claim', 'voice_source_metadata', 'invented'])
def test_question_cannot_become_factual_or_metadata_evidence(role):
    with pytest.raises(ValueError, match='question requires'):
        contract.validate(record(role), source='', window_id='window')


def test_question_premise_cannot_be_asserted():
    value = record()
    value['events'][0]['attitude']['proposition_status'] = 'asserted'
    with pytest.raises(ValueError, match='questioned'):
        contract.validate(value, source='', window_id='window')


@pytest.mark.parametrize('failure', ['source limitation requires recovery need',
                                   'span not exact source', 'unsupported attribution'])
def test_parent_rejections_are_not_suppressed(monkeypatch, failure):
    def reject(*args, **kwargs):
        raise ValueError(failure)
    monkeypatch.setattr(contract.previous.parent, 'validate', reject)
    with pytest.raises(ValueError, match=failure):
        contract.validate(record('research_limitation'), source='', window_id='window')


def test_isolated_version_not_qualified_or_auto_migrated():
    old = deepcopy(contract.previous.schema())
    new = contract.schema()
    assert new['properties']['schema_version']['const'] == contract.VERSION
    assert old == contract.previous.schema()
    with pytest.raises(ValueError, match='wrong question boundary'):
        contract.validate({'schema_version': contract.previous.VERSION}, source='', window_id='window')
    receipt = contract.receipt()
    for field in ('qualified', 'gold_accepted', 'dispatch_enabled', 'automatic_migration',
                  'source_need_clarification_included'):
        assert receipt[field] is False
