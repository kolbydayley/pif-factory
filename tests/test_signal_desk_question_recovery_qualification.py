import asyncio
import hashlib
import json
import pytest
from scripts import pif_signal_desk_question_recovery_qualification as run
from test_signal_desk_question_qualification import setup as setup_baseline, response


def setup(tmp_path, monkeypatch):
    setup_baseline(tmp_path, monkeypatch)
    monkeypatch.setattr(run, 'OUT', tmp_path / 'recovery')
    return run.prepare()


def save(directory, p, value):
    sha = p['packet_sha256']
    h = lambda t: hashlib.sha256(t.encode()).hexdigest()
    run.immutable_json(directory / 'packet.json', p)
    for suffix in ('result', 'output'):
        run.immutable_json(directory / f'{sha}.{suffix}.json', value)
    run.immutable_json(directory / f'{sha}.sidecar.json', {
        'state': 'completed', 'model': 'gpt-5.6-sol', 'effort': 'medium',
        'prompt_sha256': h(json.dumps(p, ensure_ascii=False)),
        'base_instructions_sha256': h(run.system(p['role']))})


def test_all_fresh_roles_and_idempotent_resume(tmp_path, monkeypatch):
    plan = setup(tmp_path, monkeypatch)
    seen = []
    async def fake(ps, **kw):
        p = ps[0]
        seen.append((p['window_id'], p['role']))
        save(kw['output_root'], p, response(p))
        return 0
    monkeypatch.setattr(run, 'metered_execute', fake)
    assert asyncio.run(run.execute(plan)) == 0
    assert len(set(seen)) == 64
    seen.clear()
    assert asyncio.run(run.execute(plan)) == 0 and not seen
    result = json.loads(next((run.OUT / 'runs').glob('*.json')).read_text())
    assert len(result['windows']) == 16 and not result['gold_accepted'] and not result['qualified']


def test_single_dimension_same_sources_schema_and_independence(tmp_path, monkeypatch):
    plan = setup(tmp_path, monkeypatch)
    source = run.source_for(plan, 'w0')
    for role in ('A', 'B', 'AUDIT'):
        old = run.baseline.packet(source, role, {})
        fresh = run.packet(source, role, {})
        assert fresh['packet_sha256'] != old['packet_sha256']
        assert fresh['schema_sha256'] == old['schema_sha256']
        assert fresh['transcript_window'] == old['transcript_window']
        assert 'author_a' not in fresh and 'author_b' not in fresh
        assert run.system(role) == run.baseline.system(role) + '\n' + run.CLARIFICATION
    assert len(plan['window_ids']) == 16 and plan['role_outputs_required'] == 64
    assert not plan['question_boundary_v2_included']


def test_hold_and_changed_plan_prevent_dispatch(tmp_path, monkeypatch):
    plan = setup(tmp_path, monkeypatch)
    async def forbidden(*a, **kw):
        raise AssertionError('unexpected paid dispatch')
    monkeypatch.setattr(run, 'metered_execute', forbidden)
    run.immutable_json(run.OUT / 'ADMISSION-HOLD.json', {'reason': 'test'})
    assert asyncio.run(run.execute(plan)) == 2
    plan['max_concurrency'] = 8
    with pytest.raises(ValueError, match='frozen recovery plan changed'):
        asyncio.run(run.execute(plan))


def test_old_provider_prompt_cannot_be_relabelled(tmp_path, monkeypatch):
    plan = setup(tmp_path, monkeypatch)
    p = run.packet(run.source_for(plan, 'w0'), 'A', {})
    directory = run.OUT / 'probe'
    save(directory, p, response(p))
    run.verified_call(directory, p)
    monkeypatch.setattr(run, 'system', run.baseline.system)
    with pytest.raises(ValueError, match='provider provenance'):
        run.verified_call(directory, p)
