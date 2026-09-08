"""Explicit three-field C proposal; independent review, never automatic acceptance."""
import argparse
import asyncio
from copy import deepcopy
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

WID = 'sdw_99a1771e94fa2b923f9e'
OUT = run.OUT / 'hidden-brain-c-explicit-review-v1'
SYSTEM = run.previous.review.SYSTEM + """\nEXPLICIT C REPAIR REVIEW:
Review only the assigned records against the complete source. c2 changes only
needs wider_context to none: unspecified study methods limit verification, while
the hedged attributed estimate is intelligible. Its uncertainty remains. Decide
independently whether this distinction is warranted; parent author approvals do
not approve C. c11 changes two endpoints from 5779 to 5780 to cover the unchanged
exact quote. Verify the host's interpretation is not recast as the guest's view.
No claim text, voice binding, candidate count, or input disposition changes.
Schema validity is not semantic approval. Reject or request correction as needed.
"""


def prepare(*, write=True):
    import tiktoken
    plan = json.loads((run.OUT / 'plan.json').read_text())
    source = json.loads((run.previous.BASE / f"{plan['source_packets'][WID]}.packet.json").read_text())
    parents = {}
    for role in ('A', 'B'):
        p = run.packet(source, role, {})
        old = run.previous.OUT / 'calls' / WID / role
        imported = (old / 'packet.json').exists()
        directory = old if imported else run.OUT / 'calls' / WID / role
        parents[role], _ = run.verified_call(directory, p, imported=imported)
    p = run.packet(source, 'C', parents)
    d = run.OUT / 'calls' / WID / 'C'
    side = run.verify_provider(d, p)
    raw = json.loads((d / f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256'] != '9cdf99fa7325e0bef3734d56de0c96984438ff75aacdb2949ade94079b7722b9' or digest(raw) != 'd2100a6d40958696bd38e7b75eab1678e628f597b1963d5d624d4e1e8d89fa26':
        raise ValueError('not the inspected original C')
    records = raw['records']
    if len(records['events']) != 12 or records['events'][1]['publishability_state'] != 'uncertain':
        raise ValueError('population or uncertainty changed')
    changes = [
        {'path': ['events', 1, 'evidence_role', 'needs'], 'before': 'wider_context', 'after': 'none', 'reason': 'Methods are unavailable for verification; the attributed hedged claim is intelligible. Preserve uncertainty.'},
        {'path': ['events', 10, 'evidence_end'], 'before': 5779, 'after': 5780, 'reason': 'Unchanged unique exact source quote ends at 5780.'},
        {'path': ['events', 10, 'position', 'source_evidence', 0, 'end'], 'before': 5779, 'after': 5780, 'reason': 'The same unchanged source quote ends at 5780.'}]
    fixed_records, proof = propose(records, source=p['transcript_window'], window_id=WID,
        expected_original_sha256=digest(records), replacements=changes, output_validator=run.previous.contract.validate)
    fixed = deepcopy(raw)
    fixed['records'] = fixed_records
    run.lineage.validate(fixed, source=p['transcript_window'], window_id=WID, author_a=parents['A'], author_b=parents['B'])
    proof.update(original_envelope_sha256=digest(raw), proposed_envelope_sha256=digest(fixed),
        lineage_unchanged=True, source_packet_sha256=p['packet_sha256'], sidecar_sha256=digest(side))
    enc = tiktoken.get_encoding('o200k_base')
    review = run.previous.review
    assigned = {records['events'][i]['event_id'] for i in (1, 10)}
    ps = []
    for item in review.packets(fixed_records, source=p['transcript_window'], window_id=WID, token_count=lambda s: len(enc.encode(s))):
        chosen = [e for e in item['candidates'] if e['event_id'] in assigned]
        if not chosen:
            continue
        item.pop('packet_sha256')
        item['candidates'] = chosen
        refs = {e['voice_binding_id'] for e in chosen}
        item['voice_bindings'] = [b for b in item['voice_bindings'] if b['voice_binding_id'] in refs]
        item['repair_provenance'] = proof
        item['system_sha256'] = digest(SYSTEM)
        item['schema_sha256'] = digest(review.schema(item))
        item['packet_sha256'] = digest(item)
        if len(enc.encode(SYSTEM + json.dumps(item, ensure_ascii=False) + json.dumps(review.schema(item)))) + 1500 > 12000:
            raise ValueError('full-source packet exceeds review limit')
        ps.append(item)
    if {e['event_id'] for item in ps for e in item['candidates']} != assigned:
        raise ValueError('assigned review population changed')
    if write:
        OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            run.immutable_json(OUT / f'{name}.json', value)
        for item in ps:
            run.immutable_json(OUT / f"{item['packet_sha256']}.packet.json", item)
        run.immutable_json(OUT / 'plan.json', {'packets': [item['packet_sha256'] for item in ps],
            'records': 12, 'assigned_records': 2, 'input_dispositions': 20, 'applied': False, 'gold_accepted': False})
    return ps, fixed, proof


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (run.previous.OUT / 'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps, _, _ = prepare()
        if args.execute:
            review = run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='hidden-brain-c-explicit-review-v1',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))


if __name__ == '__main__':
    main()
