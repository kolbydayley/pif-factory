"""Source-bound semantic corrections after independent C review."""
from copy import deepcopy
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_actual_review_receipt import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_stoica_c_review as parent
    original,prior,p=parent.proposal();rows=[];proofs=[]
    for q in parent.prepare(write=False)[0]:
        v,proof=verify(parent.OUT/'records',q,system=parent.SYSTEM,validator=parent.run.previous.review.validate_review)
        rows.extend(v['decisions']);proofs.append(proof)
    ids={original['records']['events'][n-1]['event_id'] for n in (5,7,8,9,12)}
    if len(rows)!=15 or {d['event_id'] for d in rows if d['verdict']!='supported'}!=ids:
        raise ValueError('Stoica correction population changed')
    raw=original['records'];s=p['transcript_window'];changes=[]
    def change(i,path,after,reason):
        path=['events',i-1]+path;before=raw
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    def span(text,purpose=None):
        if s.count(text)!=1:raise ValueError('ambiguous source span')
        a=s.index(text);v=dict(text=text,start=a,end=a+len(text))
        if purpose:v['purpose']=purpose
        return v
    change(5,['claim_text'],'Sonya Huang summarizes the prior discussion as saying the two next Databricks will be distributed compute across heterogeneous hardware and autonomous compound AI systems.','Do not promote the host recap to an independent forecast.')
    change(5,['speech_act'],'explanation','This utterance summarizes preceding themes.')
    role=deepcopy(raw['events'][4]['evidence_role'])
    role.update(role='supporting_context',decision='boundary',context_for=[raw['events'][i]['event_id'] for i in (0,3)],context_parent_status='linked',
        rationale='The labeled host recaps the preceding infrastructure and autonomy themes; it is not an independent company-building thesis.')
    change(5,['evidence_role'],role,'Keep the recap as linked context rather than a separate signal.')
    change(5,['position','rationale'],'Sonya actually utters this summary; this does not independently establish that she originated the preceding thesis.','Distinguish the uttered recap from an independent position.')
    change(5,['attitude','rationale'],'The recap is stated without an explicit evaluative polarity.','Remove independent-forecast characterization.')
    change(7,['claim_text'],"Ion Stoica presents the historically effective U.S. partnership among academia, government and industry, with the internet, which he describes as a DARPA project, as the baseline for his comparison with today's AI ecosystem.",'Retain attributed DARPA wording without adding an origin claim.')
    change(8,['context_evidence'],[span('the collaboration and the partnership, the three-way partnership between academia and government and industry','antecedent')],'Resolve that kind of partnership explicitly.')
    e=raw['events'][7]
    change(9,['context_evidence'],[dict(text=e['evidence_text'],start=e['evidence_start'],end=e['evidence_end'],purpose='antecedent')],'Ground the causal thats why connection to the preceding diagnosis.')
    change(12,['attitude','target'],span('come up with new model architectures and innovate'),'The difficulty evaluates innovation, not the architectures themselves.')
    change(12,['attitude','rationale'],'Stoica negatively evaluates the difficulty of coming up with new model architectures and innovating, while retaining the not-impossible caveat.','Aim the evaluation at the action prospect.')
    records,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),replacements=changes,output_validator=parent.run.previous.contract.validate)
    fixed=deepcopy(original);fixed['records']=records;ledger_changes=[]
    target=raw['events'][4]['event_id']
    for i,d in enumerate(fixed['input_dispositions']):
        if d['output_event_ids']==[target]:
            before=deepcopy(d);d.update(action='corrected',reason='Represent the labeled Sonya recap as supporting context for the preceding infrastructure and autonomy themes, not an independent forecast.')
            ledger_changes.append(dict(index=i,before=before,after=deepcopy(d)))
    if len(ledger_changes)!=2:raise ValueError('recap disposition coverage changed')
    parent.run.validate(fixed,p)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs,ledger_changes=ledger_changes,
        original_envelope_sha256=digest(original),proposed_envelope_sha256=digest(fixed),independent_record_and_ledger_review_required=True)
    return fixed,proof,p
