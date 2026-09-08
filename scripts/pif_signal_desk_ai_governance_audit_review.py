"""Review the full held audit proposal, not just numeric repairs."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_ai_governance_audit_proposal import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=run.OUT/'ai-governance-audit-explicit-proposal-review-v1'
SYSTEM=run.previous.review.SYSTEM+'''\nIndependently inspect every assigned audit record against the whole source.
No author A/B/C answers or approvals are evidence for this audit. Separate the
explicit Leopold quotation from the unidentified narrator and from an anticipated
objection. Do not invent a narrator name from metadata or a mentioned person.
Check reported/anticipated positions, nuclear analogy, conditional forecasts,
surveillance targets, and the narrator's own epistemic caveat. Unknown voice does
not make readable meaning a source-recovery need. Assess every field and coverage;
neither structural validity nor the proposal establishes semantic correctness.
'''


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base')
    ps=packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=run.previous.contract.validate)
    if [e['event_id'] for packet in ps for e in packet['candidates']]!=[e['event_id'] for e in fixed['events']]:
        raise ValueError('audit review population changed')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        run.immutable_json(OUT/'proposal.json',fixed);run.immutable_json(OUT/'provenance.json',proof)
        for packet in ps:run.immutable_json(OUT/f"{packet['packet_sha256']}.packet.json",packet)
        run.immutable_json(OUT/'plan.json',dict(packets=[p['packet_sha256'] for p in ps],records=15,
            proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='ai-governance-audit-proposal-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
