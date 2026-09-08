"""Read-only, complete-denominator status; never infers process liveness or acceptance."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_question_qualification as run


def summarize():
    plan = json.loads((run.OUT / 'plan.json').read_text())
    if plan != run.prepare(write=False):
        raise ValueError('frozen question plan changed')
    rows = []
    for wid in plan['window_ids']:
        source = run.source_for(plan, wid)
        outputs = {}
        for role in run.ROLES:
            row = {'window_id': wid, 'role': role, 'state': 'not_started'}
            if role == 'C' and not {'A', 'B'} <= outputs.keys():
                row['state'] = 'waiting_for_verified_parents'
                rows.append(row)
                continue
            p = run.packet(source, role, outputs)
            directory = run.OUT / 'calls' / wid / role
            sha = p['packet_sha256']
            raw = directory / f'{sha}.output.json'
            result = directory / f'{sha}.result.json'
            side = directory / f'{sha}.sidecar.json'
            try:
                if raw.exists():
                    row['raw_returned'] = True
                    value = json.loads(raw.read_text())
                    run.validate(value, p)
                    row['raw_structurally_valid'] = True
                if result.exists():
                    outputs[role], _ = run.verified_call(directory, p)
                    row['state'] = 'authored_not_accepted'
                elif raw.exists():
                    row['state'] = 'held_unverified_response'
                elif side.exists():
                    receipt = json.loads(side.read_text())
                    row['provider_state'] = receipt.get('state')
                    row['error_class'] = receipt.get('error_class')
                    # Sidecars are not proof that a process is currently alive.
                    row['state'] = 'held_provider_error' if receipt.get('error_class') else 'awaiting_result'
                elif (directory / 'packet.json').exists():
                    row['state'] = 'prepared'
            except (ValueError, TypeError, KeyError, OSError) as exc:
                row.update(state='held', reason=str(exc)[:240])
            rows.append(row)
    states = Counter(r['state'] for r in rows)
    complete = sum(all(r['state'] == 'authored_not_accepted' for r in rows if r['window_id'] == wid)
                   for wid in plan['window_ids'])
    return {'planned_windows': 16, 'planned_role_outputs': 64, 'complete_windows': complete,
            'states': dict(states), 'raw_returns': sum(r.get('raw_returned', False) for r in rows),
            'raw_structurally_valid': sum(r.get('raw_structurally_valid', False) for r in rows),
            'calls': rows, 'qualified': False, 'gold_accepted': False,
            'process_liveness': 'not_checked', 'independent_review_required': True}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--calls', action='store_true')
    args = parser.parse_args()
    result = summarize()
    if not args.calls:
        result.pop('calls')
    print(json.dumps(result, sort_keys=True))
