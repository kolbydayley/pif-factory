from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_question_contract as question
from scripts import pif_signal_desk_opening_voice_review as review


def example():
    wid = 'sdw_8bc85d983495f3780cb2'
    directory = review.run.OUT / 'calls' / wid / 'A'
    value = json.loads(next(directory.glob('*.output.json')).read_text())
    source = json.loads((directory / 'packet.json').read_text())['transcript_window']
    # Single-record boundary-context test fixture, never a corpus repair.
    event = value['events'][7]
    event['speech_act'] = 'question'
    event['evidence_role'].update(role='supporting_context', context_for=[], context_parent_status='missing', decision='boundary')
    event['publishability_state'] = 'uncertain'
    event['attitude']['status'] = 'needs_review'
    value['events'] = [event]
    value['voice_bindings'] = [b for b in value['voice_bindings'] if b['voice_binding_id'] == event['voice_binding_id']]
    value['schema_version'] = question.VERSION
    return value, source, wid


def test_question_without_frozen_contract_mutation():
    before = deepcopy(question.parent.schema())
    value, source, wid = example()
    original = deepcopy(value)
    assert question.validate(value, source=source, window_id=wid) == original
    assert value == original
    assert question.parent.schema() == before
    assert 'question' not in before['properties']['events']['items']['properties']['speech_act']['enum']
    assert question.receipt()['qualified'] is False


@pytest.mark.parametrize('field', ['substantive', 'asserted', 'wrong_source'])
def test_question_cannot_bypass_existing_gates(field):
    value, source, wid = example()
    event = value['events'][0]
    if field == 'substantive': event['evidence_role']['role'] = 'substantive_claim'
    if field == 'asserted': event['attitude']['proposition_status'] = 'asserted'
    if field == 'wrong_source': event['evidence_start'] += 1
    with pytest.raises(ValueError): question.validate(value, source=source, window_id=wid)
