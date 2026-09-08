"""Review changed C mechanism plus affected and previously invalid lineage items."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_tsmc_c_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_tsmc_c_consultation_proposal import prepare as proposal
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_actual_review_receipt import verify

OUT = parent.OUT.parent/'tsmc-c-consultation-and-lineage-review-v2'
EVENT = 'evt_08_nvidia_damage_resolution_mechanism'
LINEAGE_IDS = {f'input-{i:04d}' for i in (0, 1, 2, 3, 12, 13, 25, 26)} | {'addition-0000', 'addition-0001'}
RECORD_SYSTEM = parent.SYSTEM + '\nCheck the consultation mechanism against the original source, not earlier approval.\n'
LEDGER_SYSTEM = parent.ledger.SYSTEM + '\nEvery source_quotes entry must be a literal substring of the supplied source. Do not add quotation delimiters or normalize punctuation/capitalization. Review independently; previous invalid responses are not approvals.\n'


def prepare(*, write=True):
    import tiktoken
    fixed, proof, packet = proposal()
    enc = tiktoken.get_encoding('o200k_base')
    count = lambda s: len(enc.encode(s))
    records = parent.packets(fixed['records'], source=packet['transcript_window'], window_id=packet['window_id'],
        token_count=count, system=RECORD_SYSTEM, schema_for_packet=parent.run.previous.review.schema,
        output_validator=parent.run.previous.contract.validate)
    ledger = parent.ledger.packets(fixed, packet, token_count=count, system=LEDGER_SYSTEM)
    selected = {}
    for kind, ps, key, ids, provider in [
        ('records', records, 'event_id', {EVENT}, parent.run.previous.review),
        ('ledger', ledger, 'decision_id', LINEAGE_IDS, parent.ledger)]:
        out = []
        for p in ps:
            candidates = [c for c in p['candidates'] if c[key] in ids]
            if not candidates:
                continue
            q = dict(p)
            q.pop('packet_sha256')
            q['candidates'] = candidates
            q['schema_sha256'] = digest(provider.schema(q))
            q['packet_sha256'] = digest(q)
            out.append(q)
        actual = [c[key] for p in out for c in p['candidates']]
        if len(actual) != len(ids) or set(actual) != ids:
            raise ValueError('delta review population changed')
        selected[kind] = out
    plan = dict(proposal_sha256=digest(fixed), provenance_sha256=digest(proof),
        packets={k:[p['packet_sha256'] for p in ps] for k,ps in selected.items()},
        full_records=15, full_lineage_items=29, delta_records=1, delta_lineage_items=10,
        qualified=False, gold_accepted=False)
    if write:
        for name, value in [('proposal', fixed), ('provenance', proof), ('plan', plan)]:
            parent.run.immutable_json(OUT/f'{name}.json', value)
        for kind, ps in selected.items():
            for p in ps:
                parent.run.immutable_json(OUT/kind/f"{p['packet_sha256']}.packet.json", p)
    return selected, plan


def verified_reviews():
    selected, _ = prepare(write=False)
    values = []
    for kind, system, provider in [('records', RECORD_SYSTEM, parent.run.previous.review),
                                    ('ledger', LEDGER_SYSTEM, parent.ledger)]:
        validator = provider.validate_review if kind == 'records' else provider.validate
        for p in selected[kind]:
            v, proof = verify(OUT/kind, p, system=system, validator=validator)
            values.append(dict(kind=kind, review=v, proof=proof))
    return values


async def run_reviews(selected):
    for kind, system, provider in [('records', RECORD_SYSTEM, parent.run.previous.review),
                                    ('ledger', LEDGER_SYSTEM, parent.ledger)]:
        code = await execute(selected[kind], output_root=OUT/kind,
            task_prefix='tsmc-c-delta-'+kind+'-v2', system=system, schema_for_packet=provider.schema,
            validator=provider.validate_review if kind=='records' else provider.validate,
            packet_id=lambda p:p['packet_sha256'], verdict_rows=lambda v:v['decisions'])
        if code:
            return code
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        selected, _ = prepare()
        if args.execute:
            raise SystemExit(asyncio.run(run_reviews(selected)))
