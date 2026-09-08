"""Fresh question-v1 recovery-rule prompt comparison; no schema or label repairs."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_question_qualification as baseline
from research_factory.signal_desk_source_need_prompts import CLARIFICATION

FAMILY = 'question-v1-recovery-rule-clarification-v1'
OUT = baseline.OUT.parent / FAMILY
ROLES = baseline.ROLES
digest = baseline.digest
immutable_json = baseline.immutable_json
metered_execute = baseline.metered_execute
schema = baseline.schema
validate = baseline.validate
source_for = baseline.source_for


def system(role):
    return baseline.system(role) + '\n' + CLARIFICATION


def packet(source, role, outputs):
    original = baseline.packet(source, role, outputs)
    value = {k: v for k, v in original.items() if k != 'packet_sha256'}
    value.update(experiment_family=FAMILY, baseline_packet_sha256=original['packet_sha256'],
                 system_sha256=digest(system(role)))
    value['packet_sha256'] = digest(value)
    return value


def prepare(*, write=True):
    old = json.loads((baseline.OUT / 'plan.json').read_text())
    if old != baseline.prepare(write=False):
        raise ValueError('frozen baseline changed')
    value = {'family': FAMILY, 'baseline_plan_sha256': digest(old),
             'window_ids': old['window_ids'], 'source_packets': old['source_packets'],
             'role_hashes': {r: digest(system(r)) for r in ROLES},
             'schema_hashes': {r: digest(schema(r)) for r in ROLES},
             'role_outputs_required': 64, 'max_concurrency': 2,
             'model': 'gpt-5.6-sol', 'effort': 'medium',
             'changed_dimension': 'existing_recovery_need_rule_prompt_only',
             'old_results_reusable': False, 'question_boundary_v2_included': False,
             'qualified': False, 'gold_accepted': False}
    if write:
        OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
        immutable_json(OUT / 'plan.json', value)
        for role in ROLES:
            immutable_json(OUT / f'{role}.provider-schema.json', baseline.lower(schema(role))[0])
    return value


def verified_call(directory, p):
    sha = p['packet_sha256']
    side = json.loads((directory / f'{sha}.sidecar.json').read_text())
    h = lambda text: hashlib.sha256(text.encode()).hexdigest()
    if (json.loads((directory / 'packet.json').read_text()) != p
        or side.get('state') != 'completed' or side.get('error_class')
        or side.get('model') != 'gpt-5.6-sol' or side.get('effort') != 'medium'
        or side.get('base_instructions_sha256') != h(system(p['role']))
        or side.get('prompt_sha256') != h(json.dumps(p, ensure_ascii=False))):
        raise ValueError('recovery-family provider provenance mismatch')
    value = json.loads((directory / f'{sha}.result.json').read_text())
    raw = json.loads((directory / f'{sha}.output.json').read_text())
    if value != raw:
        raise ValueError('no automatic semantic or offset repairs')
    validate(value, p)
    return value, {'packet_sha256': sha, 'sidecar_sha256': digest(side),
                   'raw_sha256': digest(raw), 'gold_accepted': False}


async def execute(plan):
    if plan != prepare(write=False):
        raise ValueError('frozen recovery plan changed')
    slots, stop = asyncio.Semaphore(2), asyncio.Event()
    async def window(wid):
        async with slots:
            outputs, proofs = {}, {}
            for role in ROLES:
                if stop.is_set() or (OUT / 'ADMISSION-HOLD.json').exists():
                    return {'window_id': wid, 'state': 'checkpointed', 'completed_roles': list(outputs)}
                try:
                    p = packet(source_for(plan, wid), role, outputs)
                    sha, directory = p['packet_sha256'], OUT / 'calls' / wid / role
                    immutable_json(directory / 'packet.json', p)
                    if not (directory / f'{sha}.result.json').exists():
                        if (directory / f'{sha}.output.json').exists():
                            return {'window_id': wid, 'role': role, 'state': 'held_existing_response'}
                        code = await metered_execute([p], output_root=directory, task_prefix=FAMILY,
                            system_for_packet=lambda q: system(q['role']),
                            schema_for_packet=lambda q: baseline.lower(schema(q['role']))[0],
                            validator=validate, turn_for_packet=lambda q: q['role'])
                        if code != 0:
                            stop.set()
                            return {'window_id': wid, 'role': role, 'state': 'held_new_call'}
                    outputs[role], proofs[role] = verified_call(directory, p)
                except Exception as exc:
                    stop.set()
                    return {'window_id': wid, 'role': role, 'state': 'checkpointed_failure', 'reason': str(exc)[:240]}
            immutable_json(OUT / 'windows' / f'{wid}.json', {'outputs': {r: digest(v) for r, v in outputs.items()},
                           'provenance': proofs, 'gold_accepted': False})
            return {'window_id': wid, 'state': 'authored_not_accepted', 'completed_roles': list(outputs)}
    rows = await asyncio.gather(*(window(wid) for wid in plan['window_ids']))
    result = {'windows': rows, 'expected_windows': 16, 'expected_role_outputs': 64,
              'all_roles_complete': all(r['state'] == 'authored_not_accepted' for r in rows),
              'qualified': False, 'gold_accepted': False, 'independent_review_required': True}
    immutable_json(OUT / 'runs' / f'{digest(result)}.json', result)
    print(json.dumps(result), flush=True)
    return 0 if result['all_roles_complete'] else 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (baseline.previous.previous.parent.OUT / 'runner.lock').open('a') as v4, (baseline.previous.previous.OUT / 'runner.lock').open('a') as v5:
        for lock in (v4, v5):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = prepare()
        print(json.dumps({'family': FAMILY, 'windows': 16, 'fresh_role_outputs_required': 64, 'gold_accepted': False}), flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(execute(plan)))
