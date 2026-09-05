from __future__ import annotations
import json
from research_factory.signal_desk_gold_repair_v5 import _participant_roster,_source_markers,recover_window

def event(speaker=None,start=0):return {'event_id':'e1','claim_text':'claim','speech_act':'assertion','evidence_text':'claim','evidence_start':start,'evidence_end':start+5,'speaker_id':speaker,'quoted_person_id':None,'mentioned_person_ids':[],'attribution_type':'direct_speech' if speaker else 'unresolved_speaker','attribution_confidence':1,'issue_label':'issue','issue_aliases':[],'stance':'neutral','publishability_state':'candidate'}
def output(e):return {'schema_version':'pif_signal_desk_clean_event_v2','window_id':'w','window_disposition':'claims_found','events':[e]}

def test_roster_excludes_reported_actors_and_generic_labels():
 ctx=[{'speaker_map_json':json.dumps([{'name':'Host Name','role':'host','confidence':.99},{'name':'Reported Person','role':'reported_actor','confidence':.99}])}]
 assert _participant_roster(outputs=(output(event('Army Film Clip')),),context_rows=ctx)==['Host Name']

def test_markers_cover_bare_lines_and_inline_colons():
 markers=_source_markers('Alice Smith\nwords\nBob Jones: more',['Alice Smith','Bob Jones'])
 assert [name for _,name in markers]==['Alice Smith','Bob Jones']

def test_full_source_marker_restores_without_semantic_change():
 transcript='Alice Smith: earlier words. '+('x'*30)+'claim';start=27;pos=transcript[start:].index('claim');base_event=event(start=pos);base=output(base_event);v3=output(base_event);v3['events']=[];v3['window_disposition']='no_consequential_claims'
 a=output({**base_event,'speaker_id':'Alice Smith','attribution_type':'direct_speech'})
 got,receipt=recover_window(metadata={'window_id':'w','start_char':start,'end_char':len(transcript),'window_index':0,'transcript_structure':'paragraph'},transcript=transcript,gold_a=a,gold_b=base,baseline_c=base,v3_c=v3,v3_quarantine={'quarantined':[{'event_id':'e1'}]},context_rows=[])
 assert got['events'][0]['speaker_id']=='Alice Smith';assert got['events'][0]['claim_text']=='claim';assert receipt['source_label_restored']==1

def test_unproven_event_stays_content_free_quarantine():
 transcript='anonymous claim';base_event=event(start=10);base=output(base_event);v3=output(base_event);v3['events']=[];v3['window_disposition']='no_consequential_claims'
 got,receipt=recover_window(metadata={'window_id':'w','start_char':0,'end_char':len(transcript),'window_index':0,'transcript_structure':'speaker_turn'},transcript=transcript,gold_a=base,gold_b=base,baseline_c=base,v3_c=v3,v3_quarantine={'quarantined':[{'event_id':'e1'}]},context_rows=[])
 assert got['events']==[];assert receipt['unresolved_event_count']==1;assert receipt['contains_claim_evidence_or_speaker_text'] is False;assert 'anonymous' not in str(receipt)

def test_generic_clip_label_blocks_carrying_prior_person_forward():
    transcript='Katie Mingle: setup.\n\nArmy Film Clip: claim';start=0;base_event=event(start=transcript.index('claim'));base=output(base_event);v3=output(base_event);v3['events']=[];v3['window_disposition']='no_consequential_claims'
    a=output({**base_event,'speaker_id':'Katie Mingle','attribution_type':'direct_speech'})
    got,receipt=recover_window(metadata={'window_id':'w','start_char':start,'end_char':len(transcript),'window_index':0,'transcript_structure':'speaker_turn'},transcript=transcript,gold_a=a,gold_b=base,baseline_c=base,v3_c=v3,v3_quarantine={'quarantined':[{'event_id':'e1'}]},context_rows=[])
    assert got['events']==[];assert receipt['unresolved_event_count']==1
