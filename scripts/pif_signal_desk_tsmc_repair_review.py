"""Review the complete explicit TSMC proposal; never apply on mere completion."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_tsmc_explicit_repair import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest

OUT = run.OUT / 'tsmc-explicit-proposal-review-v1'
SYSTEM = run.previous.review.SYSTEM + """\nEXPLICIT TSMC PROPOSAL REVIEW:
Review all assigned proposed records against the complete source independently.
The original opening voice remains unresolved, and its readable statements remain
uncertain; needs is now none and firsthand scope is not_applicable for those three
records. The unnamed executive remains unnamed. The solar-failure causal assertion
is certain rather than hedged: the removed hedge qualified a distinct counterfactual
opportunity. The MediaTek answer has linked antecedent context but no recovery need.
Eight nested source spans have explicit inspected offsets, quoted text unchanged.
No claim text, event population, attribution identity, or voice corridor changed.
Verify these choices rather than treating prior diagnosis as automatic approval.
Check all candidate dimensions, including meaningful uncertainty and causal scope.
Preserve Ben's strategic interpretation separately from Morris's account and keep
both sponsor records quarantined. Never infer the opening speaker from later turns.
Reject or request correction whenever the actual source does not support a field.
"""


def prepare(*, write=True):
    import tiktoken
    fixed, proof, packet = proposal()
    enc = tiktoken.get_encoding('o200k_base')
    ps = packets(fixed, source=packet['transcript_window'], window_id=packet['window_id'],
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM,
        schema_for_packet=run.previous.review.schema, output_validator=run.previous.contract.validate)
    if [e['event_id'] for p in ps for e in p['candidates']] != [e['event_id'] for e in fixed['events']]:
        raise ValueError('proposal review population changed')
    if write:
        OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
        run.immutable_json(OUT / 'proposal.json', fixed)
        run.immutable_json(OUT / 'provenance.json', proof)
        for p in ps:
            run.immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
        run.immutable_json(OUT / 'plan.json', {'packets': [p['packet_sha256'] for p in ps],
            'proposal_sha256': digest(fixed), 'records': 14, 'applied': False, 'gold_accepted': False})
    return ps


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (run.previous.OUT / 'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='tsmc-explicit-proposal-review-v1',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))


if __name__ == '__main__':
    main()
