"""Independent review of every record in the explicit TSMC B proposal."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_tsmc_b_proposal import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=run.OUT/'tsmc-b-explicit-proposal-review-v1'
SYSTEM=run.previous.review.SYSTEM+'''\nReview every assigned B record against the full source independently.
The first three opening records remain speaker-unresolved and uncertain; readable
content is not inherently a source-recovery need, and interested-party status
cannot be inferred from an unknown speaker. Check the executive-role scope, the
arbitration statement used as context, and the noncausal partnership summary.
Verify all fields and exact source support, not just proposed corrections. Keep
sponsor exclusions. No A-review approval applies to these B records.
'''


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base')
    ps=packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=run.previous.contract.validate)
    if [e['event_id'] for packet in ps for e in packet['candidates']]!=[e['event_id'] for e in fixed['events']]:
        raise ValueError('B review population changed')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        run.immutable_json(OUT/'proposal.json',fixed);run.immutable_json(OUT/'provenance.json',proof)
        for packet in ps:run.immutable_json(OUT/f"{packet['packet_sha256']}.packet.json",packet)
        run.immutable_json(OUT/'plan.json',{'packets':[p['packet_sha256'] for p in ps],'records':13,
            'proposal_sha256':digest(fixed),'provenance_sha256':digest(proof),'applied':False,'gold_accepted':False})
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='tsmc-b-explicit-proposal-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
