import json
from scripts.pif_signal_desk_full_event_v5_status import summarize
from research_factory import signal_desk_full_event_v5_prompts as author
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v5 import value,SOURCE


def test_context_spans_never_inflate_records_or_voice_counts(tmp_path):
    base=tmp_path/'base';root=tmp_path/'out';base.mkdir();root.mkdir()
    source={"window_id":"dev","transcript_window":SOURCE,"transcript_structure":"speaker_turn"}
    source["packet_sha256"]=digest(source);sha=source["packet_sha256"]
    (base/f'{sha}.packet.json').write_text(json.dumps(source))
    (root/'plan.json').write_text(json.dumps({"window_ids":["dev"],"source_packets":{"dev":sha},"contract":author.receipt()}))
    p=author.packet(source,'A');d=root/'calls'/'dev'/'A';d.mkdir(parents=True)
    (d/'packet.json').write_text(json.dumps(p));sha=p['packet_sha256']
    v=value();v['events'][0]['context_evidence']=[{'text':'Alex:','start':0,'end':5,'purpose':'antecedent'}]
    for suffix in ('output','result'):(d/f'{sha}.{suffix}.json').write_text(json.dumps(v))
    (d/f'{sha}.sidecar.json').write_text(json.dumps({'state':'completed','usage':{'total_tokens':50}}))
    s=summarize(root,base);a=s['by_role']['A']
    assert a['records']==a['spoken_named_voice']==a['records_with_context']==1
    assert a['context_spans_not_claims']==1
    assert s['states']['authored_not_accepted']==1 and not s['accepted_gold']
