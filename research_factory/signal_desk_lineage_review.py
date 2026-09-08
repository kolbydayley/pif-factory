"""Review every input disposition and new output against the complete source."""
import json
from . import signal_desk_adjudication_lineage as lineage
from . import signal_desk_full_event_v5_prompts as author
from .signal_desk_full_event_v4_review import _shape
from .signal_desk_rubric_reference_packets import digest

SYSTEM=author.COMMON+'\n'+lineage.RULES+"""\nYou are an independent final lineage
reviewer, not the author. Check every assigned decision_id exactly once against
the complete source. Is the retained/merged/split/rejected/unresolved disposition
faithful, or did it erase a distinct claim, duplicate a proposition, conflate
speakers, or hide a limitation? Check source-discovered additions too. Input IDs
are historical accountability, not proof of correctness. Canonical output records
are reviewed separately; here check the mappings, their rationale, and the meaning
they preserve or lose. Explain needed corrections precisely, with exact source
quotes. Quarantined does not mean fabricated: supported but underspecified records
may remain excluded from publication with an explicit recovery need. Return only
assigned IDs; no automatic labels, corpus acceptance, or outside knowledge.
"""


def schema(p):
    fields={'decision_id':{'type':'string','enum':[c['decision_id'] for c in p['candidates']]},
            'verdict':{'type':'string','enum':['supported','material_error','unresolved']},
            'rationale':{'type':'string','minLength':1},'correction':{'type':'string'},
            'source_quotes':{'type':'array','minItems':1,'items':{'type':'string','minLength':1}}}
    return {'type':'object','additionalProperties':False,'required':['decisions'],'properties':{
        'decisions':{'type':'array','minItems':len(p['candidates']),'maxItems':len(p['candidates']),
            'items':{'type':'object','additionalProperties':False,'required':list(fields),'properties':fields}}}}


def validate(v,p):
    _shape(v,schema(p));ids=[r['decision_id'] for r in v['decisions']]
    if len(ids)!=len(set(ids)) or set(ids)!={c['decision_id'] for c in p['candidates']}:raise ValueError('lineage review coverage changed')
    for r in v['decisions']:
        if not r['rationale'].strip() or any(not q.strip() or q not in p['transcript_window'] for q in r['source_quotes']):raise ValueError('review not grounded in source')
        if r['verdict']=='material_error' and not r['correction'].strip():raise ValueError('material error requires specific correction')
    return v


def packets(output, author_packet, *, token_count):
    source=author_packet['transcript_window'];wid=author_packet['window_id']
    lineage.validate(output,source=source,window_id=wid,author_a=author_packet['author_a'],author_b=author_packet['author_b'])
    inputs={(a,e['event_id']):e for a in ('A','B') for e in author_packet['author_'+a.lower()]['events']}
    events={e['event_id']:e for e in output['records']['events']};candidates=[]
    for i,row in enumerate(output['input_dispositions']):
        candidates.append({'decision_id':f'input-{i:04d}','kind':'input_disposition','disposition':row,
            'input_record':inputs[(row['author'],row['input_event_id'])],
            'output_records':[events[eid] for eid in row['output_event_ids']]})
    for i,row in enumerate(output['additions']):
        candidates.append({'decision_id':f'addition-{i:04d}','kind':'addition','disposition':row,
            'output_records':[events[row['output_event_id']]]})
    def build(items):
        p={'window_id':wid,'transcript_window':source,'author_packet_sha256':author_packet['packet_sha256'],
           'lineage_output_sha256':digest(output),'candidate_population':len(candidates),'candidates':items,
           'system_sha256':digest(SYSTEM)}
        p['schema_sha256']=digest(schema(p));p['packet_sha256']=digest(p);return p
    def fits(items):
        p=build(items)
        return len(items)<=25 and token_count(SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(schema(p)))+1500<=12000
    batches=[];current=[]
    for candidate in candidates:
        if not fits([candidate]):raise ValueError('one full-source lineage item exceeds review limit; do not truncate')
        if current and not fits(current+[candidate]):batches.append(build(current));current=[]
        current.append(candidate)
    if current:batches.append(build(current))
    # Empty lineage needs no invented decision; canonical empty-window review
    # still evaluates whether source omissions made this empty result incorrect.
    return batches


def receipt():return {'system_sha256':digest(SYSTEM),'lineage_contract':lineage.receipt(),'gold_accepted':False,'qualified':False}
