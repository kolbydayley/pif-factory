"""Independent full-population TSMC audit proposal review."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_tsmc_audit_proposal import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=run.OUT/'tsmc-audit-full-proposal-review-v1'
SYSTEM=run.previous.review.SYSTEM+'''\nIndependently review every assigned audit record against the complete source.
Do not inherit approvals or labels from A/B/C. Opening voices remain unknown.
Check the departure/MediaTek context, approximation of time spent, technological
dependency, consultation mechanism, dinner scheduling relevance, settlement terms,
relationship versus causation, autobiography context, and sponsor quarantine.
Check all fields, claim atomicity and omissions, not only proposed corrections.
'''


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base')
    ps=packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=run.previous.contract.validate)
    if [e['event_id'] for q in ps for e in q['candidates']]!=[e['event_id'] for e in fixed['events']]:raise ValueError('audit review coverage changed')
    if write:
        for name,value in [('proposal',fixed),('provenance',proof)]:run.immutable_json(OUT/f'{name}.json',value)
        for q in ps:run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        run.immutable_json(OUT/'plan.json',dict(packets=[q['packet_sha256'] for q in ps],records=15,
            proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='tsmc-audit-full-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
