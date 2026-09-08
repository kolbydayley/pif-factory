from copy import deepcopy
import pytest
from research_factory import signal_desk_question_prompts as family
from research_factory.signal_desk_rubric_reference_packets import digest


def source():
    value = {'window_id': 'dev', 'transcript_window': 'HOST: Is that true?', 'transcript_structure': 'speaker_turn'}
    value['packet_sha256'] = digest(value)
    return value


def test_all_roles_bind_new_rules_without_changing_frozen_prompts():
    old = deepcopy(family.parent.prompts())
    for role in ('A', 'B', 'AUDIT'):
        p = family.packet(source(), role)
        assert p['transcript_window'] == source()['transcript_window']
        assert p['system_sha256'] == digest(family.prompts()[role])
        assert p['schema_sha256'] == digest(family.contract.schema())
        assert family.contract.RULES in family.prompts()[role]
    assert old == family.parent.prompts()
    assert not family.receipt()['dispatch_enabled']


@pytest.mark.parametrize('role', ['A', 'B', 'AUDIT'])
def test_independent_roles_cannot_receive_parent_answers(role):
    with pytest.raises(ValueError, match='cannot see answers'):
        family.packet(source(), role, author_a={})


def test_tampered_source_and_missing_c_parents_fail_closed():
    bad = source(); bad['transcript_window'] += ' changed'
    with pytest.raises(ValueError, match='source lineage changed'):
        family.packet(bad, 'A')
    with pytest.raises(ValueError, match='both independent authors'):
        family.packet(source(), 'C')
