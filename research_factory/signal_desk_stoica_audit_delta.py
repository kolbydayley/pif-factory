"""Five semantic corrections grounded in the completed independent AUDIT review."""
from copy import deepcopy
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_stoica_audit_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    ids={raw['events'][i-1]['event_id'] for i in (3,5,7,8,9)}
    if len(rows)!=13 or {r['event_id'] for r in rows if r['verdict']!='supported'}!=ids:
        raise ValueError('Stoica AUDIT correction population changed')
    s=p['transcript_window'];changes=[]
    def change(n,path,after,reason):
        path=['events',n-1]+path;before=raw
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    def span(text,purpose=None):
        if s.count(text)!=1:raise ValueError('ambiguous source span')
        a=s.index(text);v=dict(text=text,start=a,end=a+len(text))
        if purpose:v['purpose']=purpose
        return v
    for n,targets in [(3,[('to go from having the human in the loop to being autonomous','And to go from having the human in the loop to being autonomous is a huge gap, right?'),
                          ('the human in the loop, and the human','human is a bottleneck')]),
                      (9,[('some of the academics are going to give up and try to innovate around the edges','danger'),
                          ('come up with new model architectures and innovate','It will be harder.')])]:
        attitude=deepcopy(raw['events'][n-1]['attitude'])
        attitude.update(attitude='indeterminate',target=None,status='split_required',
            target_components=[dict(target=span(t),attitude='negative',evaluation_evidence=[span(e)]) for t,e in targets],
            rationale='Preserve the two explicitly evaluated constraints as separate source-grounded targets rather than treating an evaluative cue as the target.')
        change(n,['attitude'],attitude,'Separate distinct evaluated constraints without duplicating the claim.')
    change(5,['claim_text'],'Sonya Huang recaps the preceding discussion as describing distributed compute across heterogeneous hardware and autonomous compound AI systems as the two next Databricks.','Preserve the host characterization without turning it into an independent forecast.')
    change(5,['speech_act'],'explanation','The labeled turn is a recap.')
    role=deepcopy(raw['events'][4]['evidence_role'])
    role.update(role='supporting_context',decision='boundary',context_for=['evt_001','evt_003','evt_004'],context_parent_status='linked',
        rationale='Sonya recaps the preceding infrastructure and autonomy themes, not an independent prediction.')
    change(5,['evidence_role'],role,'Link the recap to its substantive parents.')
    change(5,['position','rationale'],'Sonya actually utters this recap; it is not independent evidence that she originated the forecast.','Distinguish expressed recap from independent forecast.')
    change(7,['speech_act'],'assertion','The explicitly labeled speaker asserts his own endorsement, not another persons position.')
    change(8,['context_evidence'],[deepcopy(raw['events'][6]['context_evidence'][0])],'Resolve this to the preceding broken partnership and resource diagnosis.')
    attitude=deepcopy(raw['events'][7]['attitude'])
    attitude.update(attitude='neutral',target=None,evaluation_evidence=[],rationale='This is a hedged conditional adverse forecast, not negative evaluation of the United States or California themselves.')
    change(8,['attitude'],attitude,'Separate adverse outcome from entity-directed attitude while preserving modality.')
    fixed,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
