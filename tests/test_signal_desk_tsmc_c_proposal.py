import json
from research_factory.signal_desk_tsmc_c_proposal import prepare
from scripts import pif_signal_desk_lineage_qualification as run


def test_full_c_lineage_and_voices_preserved():
    fixed,proof,p=prepare()
    raw=json.loads((run.OUT/'calls'/p['window_id']/'C'/f"{p['packet_sha256']}.output.json").read_text())
    assert len(fixed['records']['events'])==15
    assert fixed['input_dispositions']==raw['input_dispositions']
    assert len(fixed['input_dispositions'])==27
    assert fixed['additions']==raw['additions'] and len(fixed['additions'])==2
    assert fixed['records']['voice_bindings']==raw['records']['voice_bindings']
    for old,new in zip(raw['records']['events'],fixed['records']['events']):
        assert old['attribution']==new['attribution']
        assert old['evidence_text']==new['evidence_text']
        assert old['publishability_state']==new['publishability_state']
    assert all(e['attribution']['transcript_voice'] is None for e in fixed['records']['events'][:3])
    assert proof['independent_record_and_ledger_review_required'] and not proof['gold_accepted']
