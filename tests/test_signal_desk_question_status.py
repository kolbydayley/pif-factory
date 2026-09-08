import pytest
from scripts import pif_signal_desk_question_status as status
from test_signal_desk_question_qualification import setup, response, save


def test_empty_population_keeps_all_denominators(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    result = status.summarize()
    assert len(result['calls']) == 64
    assert result['states'] == {'not_started': 48, 'waiting_for_verified_parents': 16}
    assert result['raw_returns'] == result['complete_windows'] == 0
    assert result['process_liveness'] == 'not_checked'
    assert not result['gold_accepted'] and not result['qualified']


def test_invalid_raw_remains_held_and_parent_unfulfilled(tmp_path, monkeypatch):
    plan, out = setup(tmp_path, monkeypatch)
    p = status.run.packet(status.run.source_for(plan, 'w0'), 'A', {})
    value = response(p)
    value['events'][0]['evidence_start'] = 99999
    save(out / 'calls' / 'w0' / 'A', p, value)
    result = status.summarize()
    assert result['states']['held'] == 1
    assert result['states']['waiting_for_verified_parents'] == 16
    assert result['raw_returns'] == 1 and result['raw_structurally_valid'] == 0
    assert len(result['calls']) == 64


def test_valid_result_still_not_accepted_and_wrong_provider_held(tmp_path, monkeypatch):
    plan, out = setup(tmp_path, monkeypatch)
    source = status.run.source_for(plan, 'w0')
    for role, model in [('A', 'gpt-5.6-sol'), ('B', 'wrong')]:
        p = status.run.packet(source, role, {})
        save(out / 'calls' / 'w0' / role, p, response(p), model=model)
    result = status.summarize()
    assert result['states']['authored_not_accepted'] == 1
    assert result['states']['held'] == 1
    assert result['raw_structurally_valid'] == 2
    assert result['complete_windows'] == 0 and not result['qualified']


def test_changed_plan_fails_closed(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    monkeypatch.setattr(status.run, 'prepare', lambda **kwargs: {})
    with pytest.raises(ValueError, match='frozen question plan changed'):
        status.summarize()
