"""Source-bound audit proposal, independently reviewed without C approvals."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_4acaaec65ea71658126b';d=run.OUT/'calls'/wid/'AUDIT'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'AUDIT',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text());s=p['transcript_window']
    if p['packet_sha256']!='21fa3ec6898cf83379653bbd23b7c36ccb2aaffd0149631625d7c42623d850e3' or digest(raw)!='b03ae0d6702214e2e7c3cdd582f9486678817568f2d74a3f7493d40f43c27425':
        raise ValueError('audit original changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        if before!=after:changes.append(dict(path=path,before=before,after=after,reason=reason))
    for i in range(4):
        change(['events',i,'evidence_role','needs'],'none','Readable meaning with available in-window context; unknown identities remain unresolved and uncertain.')
    change(['events',2,'evidence_role','role'],'substantive_claim','The executive departure is a distinct organizational consequence, not merely descriptive background.')
    change(['events',2,'evidence_role','context_for'],[],'Retain the organizational outcome as its own proposition.')
    change(['events',2,'evidence_role','context_parent_status'],'not_applicable','Substantive organizational consequence does not require a context parent.')
    change(['events',3,'evidence_role','role'],'supporting_context','The executive role supplies context for the preceding departure, not metadata about the speaker.')
    change(['events',3,'evidence_role','context_for'],['evt_003'],'Link to the unnamed executive departure.')
    change(['events',3,'evidence_role','context_parent_status'],'linked','Explicit supporting-context parent.')
    change(['events',3,'evidence_role','scope'],'not_applicable','Role context does not establish firsthand observation.')
    change(['events',3,'evidence_role','rationale'],'The role confirmation supplies subsequent context for the unnamed executive whose departure was just described.','Preserve unnamed identity while resolving the antecedent.')
    antecedent=dict(text=s[499:634],start=499,end=634,purpose='antecedent')
    change(['events',3,'context_evidence'],raw['events'][3]['context_evidence']+[antecedent],'Include the source antecedent for the executive.')
    change(['events',4,'claim_text'],'After retaking the CEO job, Morris estimates that he probably spent almost half of his first four or five weeks working on the NVIDIA dispute.','Preserve the explicit probability and approximation.')
    prefix='I called the salespeople that happened in direct contact with NVIDIA. Of course, I called everybody that was somewhat involved in the problem.'
    if s.count(prefix)!=1:raise ValueError('consultation source changed')
    start=s.index(prefix);end=raw['events'][6]['evidence_end']
    change(['events',6,'evidence_start'],start,'Restore the personnel-consultation mechanism in the evidence.')
    change(['events',6,'evidence_text'],s[start:end],'Use the complete contiguous consultation and calculation account.')
    change(['events',6,'claim_text'],"Morris says TSMC was already doing what it could on manufacturing and NVIDIA had borne the brunt of the damage, so the remaining resolution was monetary. He consulted salespeople in direct contact with NVIDIA and other involved personnel, reviewed the problem and NVIDIA customers’ demands, and derived a number.",'Preserve the source consultation mechanism rather than compressing it into generic intelligence.')
    change(['events',11,'claim_text'],'Ben praises the settlement as an example of working out differences, describing a longstanding partnership, a close personal relationship, a settlement exceeding $100 million, and subsequent business worth many billions.','Do not turn juxtaposed relationship and business outcomes into proven causation.')
    change(['events',11,'evidence_role','rationale'],'Ben offers an evaluative interpretation of the relationship, settlement and subsequent business, not proof of a causal mechanism.','Remove overstated causal classification.')
    for path,start in [(['events',5,'attitude','evaluation_evidence',0],1409),
                       (['events',11,'position','source_evidence',0],4089),
                       (['events',12,'attitude','target'],4609),
                       (['events',14,'position','source_evidence',0],5624)]:
        span=raw
        for key in path:span=span[key]
        end=start+len(span['text'])
        if s[start:end]!=span['text']:raise ValueError('inspected exact quote changed')
        change(path+['start'],start,'Correct numeric offset only to inspected exact source occurrence.')
        change(path+['end'],end,'Correct numeric offset only to inspected exact source occurrence.')
    fixed,proof=propose(raw,source=s,window_id=wid,expected_original_sha256=digest(raw),
                        replacements=changes,output_validator=run.previous.contract.validate)
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True)
    return fixed,proof,p
