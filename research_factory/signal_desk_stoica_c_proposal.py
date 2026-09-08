"""One explicit numeric offset correction; preserve every C record and disposition."""
import json
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_ff2331e598d948e07cc9';d=run.OUT/'calls'/wid/'C'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text());parents={}
    for role in ('A','B'):
        q=run.packet(source,role,{})
        parents[role]=run.verified_call(run.OUT/'calls'/wid/role,q)[0]
    p=run.packet(source,'C',parents);run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='6840afa57a942b48e558336279252351dae8eb02c1b55adb82d325e439622e5d' or digest(raw)!='9acb181361c4eb90255bfef3b778806ec22490c4af9a1b21ded553ca20063eee':
        raise ValueError('Stoica C source/request changed')
    records,proof=propose(raw['records'],source=p['transcript_window'],window_id=wid,
        expected_original_sha256=digest(raw['records']), replacements=[dict(
            path=['events',8,'position','source_evidence',0,'start'],before=3917,after=3918,
            reason='Exclude the preceding space; preserve the exact existing unique source quotation and end offset.')],
        output_validator=run.previous.contract.validate)
    fixed=deepcopy(raw);fixed['records']=records;run.validate(fixed,p)
    proof.update(original_envelope_sha256=digest(raw),proposed_envelope_sha256=digest(fixed),
        source_packet_sha256=p['packet_sha256'],lineage_unchanged=True,
        independent_record_and_ledger_review_required=True)
    return fixed,proof,p
