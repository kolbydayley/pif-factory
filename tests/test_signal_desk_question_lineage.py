from copy import deepcopy
import pytest
from research_factory import signal_desk_question_lineage as family
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_question_contract import example


def fixture():
    records, source, wid = example()
    eid = records['events'][0]['event_id']
    envelope = {'schema_version': family.VERSION, 'records': records, 'additions': [],
        'input_dispositions': [{'author': a, 'input_event_id': eid, 'action': 'merged',
            'output_event_ids': [eid], 'reason': 'Same question from both authors.'} for a in ('A', 'B')]}
    return envelope, source, wid


def test_question_remains_question_and_frozen_lineage_unchanged():
    value, source, wid = fixture(); before = deepcopy(value); old = family.parent.schema()
    assert family.validate(value, source=source, window_id=wid, author_a=value['records'], author_b=value['records']) == before
    assert value == before and family.parent.schema() == old
    assert value['records']['events'][0]['speech_act'] == 'question'


@pytest.mark.parametrize('failure', ['missing_input', 'unknown_output', 'question_as_claim', 'missing_source'])
def test_no_question_or_lineage_bypass(failure):
    value, source, wid = fixture(); authors = deepcopy(value['records'])
    if failure == 'missing_input': value['input_dispositions'].pop()
    if failure == 'unknown_output': value['input_dispositions'][0]['output_event_ids'] = ['invented']
    if failure == 'question_as_claim': value['records']['events'][0]['evidence_role']['role'] = 'substantive_claim'
    if failure == 'missing_source': value['records']['events'][0]['evidence_start'] += 1
    with pytest.raises(ValueError): family.validate(value, source=source, window_id=wid, author_a=authors, author_b=authors)


def test_packet_binds_full_source_both_parents_and_new_contract():
    value, source, wid = fixture()
    p = {'window_id': wid, 'transcript_window': source, 'transcript_structure': 'speaker_turn'}; p['packet_sha256'] = digest(p)
    result = family.packet(p, author_a=value['records'], author_b=value['records'])
    assert result['transcript_window'] == source
    assert result['author_a'] == result['author_b'] == value['records']
    assert result['schema_sha256'] == digest(family.schema())
    assert result['system_sha256'] == digest(family.system())
    assert not family.receipt()['qualified'] and not family.receipt()['dispatch_enabled']
