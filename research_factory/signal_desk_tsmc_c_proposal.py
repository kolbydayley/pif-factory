"""Explicit C record corrections; the original adjudication ledger is immutable."""
import json
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_4acaaec65ea71658126b';d=run.OUT/'calls'/wid/'C'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text());parents={}
    for role in ('A','B'):
        q=run.packet(source,role,{})
        parents[role]=run.verified_call(run.OUT/'calls'/wid/role,q)[0]
    p=run.packet(source,'C',parents);run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='9164ad07fc5068cf87951cc8967806c3f52d8624a16b5d4ffc68063134b7c35d' or digest(raw)!='713ba7b86574018a1a14befef884321fa3ec94466f667415359f385263c94c98':
        raise ValueError('C source/request changed')
    records=raw['records'];changes=[]
    def change(path,after,reason):
        before=records
        for key in path:before=before[key]
        if before!=after:changes.append(dict(path=path,before=before,after=after,reason=reason))
    for i in range(3):
        change(['events',i,'evidence_role','needs'],'none','Legible opening content; null voice and uncertainty remain unchanged.')
    change(['events',3,'evidence_role','scope'],'not_applicable','Executive role/navigation context does not establish firsthand observation.')
    text=p['transcript_window'][499:634]
    if text!='So a few years later, the CEO that was put on the new businesses decided that his new assignment wasn’t working out either, so he quit.':
        raise ValueError('executive antecedent changed')
    change(['events',3,'context_evidence'],records['events'][3]['context_evidence']+
        [dict(text=text,start=499,end=634,purpose='antecedent')],'Provide explicit source antecedent for the former executive.')
    change(['events',4,'claim_text'],'Morris estimates that, after retaking the CEO job, he probably spent almost half of his first four or five weeks working on resolving the NVIDIA problem.','Retain the explicit probably hedge in the claim summary.')
    change(['events',5,'evidence_role','rationale'],'This scope clarification identifies additional customers implicated in the discussion preceding Morris’s process-development explanation.','Avoid silent correction of the source word note to node.')
    change(['events',6,'claim_text'],'Morris characterizes 40 nanometers as important in the progression of Moore’s Law and says doing 40 nanometers well was necessary before moving to 28 nanometers.','Describe source meaning without silently normalizing note to node.')
    change(['events',6,'issue_label'],'Semiconductor process development','Avoid the same normalization in the issue label.')
    change(['events',6,'evidence_role','rationale'],'The statement gives a specific technological dependency between 40-nanometer and 28-nanometer development.','Keep the dependency source-bound.')
    change(['events',6,'attitude','rationale'],'Morris explicitly evaluates 40-nanometer development as important.','Preserve evaluation without replacing source terminology.')
    fixed_records,proof=propose(records,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(records),
        replacements=changes,output_validator=run.previous.contract.validate)
    fixed=deepcopy(raw);fixed['records']=fixed_records;run.validate(fixed,p)
    if len(fixed_records['events'])!=15 or fixed_records['voice_bindings']!=records['voice_bindings']:raise ValueError('C population changed')
    proof.update(original_envelope_sha256=digest(raw),proposed_envelope_sha256=digest(fixed),
        source_packet_sha256=p['packet_sha256'],lineage_unchanged=True,
        independent_record_and_ledger_review_required=True)
    return fixed,proof,p
