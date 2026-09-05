from __future__ import annotations
from copy import deepcopy
from research_factory.signal_desk_gold_repair_v4 import explicit_primary_speaker, restore_window

def event(speaker=None):
    return {"event_id":"e1","claim_text":"claim","speech_act":"assertion","evidence_text":"claim","evidence_start":0,"evidence_end":5,"speaker_id":speaker,"quoted_person_id":None,"mentioned_person_ids":[],"attribution_type":"direct_speech" if speaker else "unresolved_speaker","attribution_confidence":1,"issue_label":"issue","issue_aliases":[],"stance":"neutral","publishability_state":"candidate"}
def output(e): return {"schema_version":"pif_signal_desk_clean_event_v2","window_id":"w","window_disposition":"claims_found","events":[e]}
def context(name="Dwarkesh Patel",confidence=.99,role="host_author_primary_speaker",segments=(1,)):
    import json
    return [{"id":"ctx1","model":"gpt-5.5","speaker_map_json":json.dumps([{"name":name,"confidence":confidence,"role":role,"segments":list(segments)}])}]

def test_explicit_primary_requires_unique_high_confidence_surface_supported_identity():
    assert explicit_primary_speaker(context_rows=context(),transcript="By Dwarkesh Patel",window_index=1)["speaker_id"] == "Dwarkesh Patel"
    assert explicit_primary_speaker(context_rows=context(confidence=.9),transcript="Dwarkesh Patel",window_index=1) is None
    assert explicit_primary_speaker(context_rows=context(),transcript="anonymous",window_index=1) is None

def test_restore_changes_only_attribution_and_preserves_semantics():
    original=event(); baseline=output(original); v3=output(original); v3["events"]=[]; v3["window_disposition"]="no_consequential_claims"
    quarantine={"quarantined":[{"event_id":"e1"}]}
    restored,receipt=restore_window(metadata={"window_id":"w","start_char":17,"end_char":22,"window_index":1,"transcript_structure":"paragraph"},transcript="Dwarkesh Patel...claim",baseline_c=baseline,v3_c=v3,v3_quarantine=quarantine,context_rows=context())
    got=restored["events"][0]
    assert got["speaker_id"] == "Dwarkesh Patel"
    assert got["claim_text"] == original["claim_text"] and got["issue_label"] == original["issue_label"]
    assert receipt["restored_event_count"] == 1 and receipt["contains_claim_evidence_or_speaker_text"] is False

def test_ambiguous_third_party_collision_stays_quarantined():
    original=event(); original["mentioned_person_ids"]=["Dwarkesh Patel"]; baseline=output(original); v3=deepcopy(baseline); v3["events"]=[]; v3["window_disposition"]="no_consequential_claims"
    restored,receipt=restore_window(metadata={"window_id":"w","start_char":17,"end_char":22,"window_index":1,"transcript_structure":"paragraph"},transcript="Dwarkesh Patel...claim",baseline_c=baseline,v3_c=v3,v3_quarantine={"quarantined":[{"event_id":"e1"}]},context_rows=context())
    assert restored["events"] == [] and receipt["quarantined_event_count"] == 1
