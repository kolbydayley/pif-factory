from copy import deepcopy
import json
import pytest
from scripts import pif_signal_desk_lineage_qualification as run
from research_factory import signal_desk_lineage_audit_offsets as offsets


@pytest.fixture
def inspected():
    directory=run.OUT/'calls'/offsets.WID/'AUDIT'
    if not (directory/'packet.json').exists():pytest.skip('private development fixture not installed')
    p=json.loads((directory/'packet.json').read_text())
    return p,json.loads((directory/f"{p['packet_sha256']}.output.json").read_text())


def test_only_numeric_fields_change_and_repeated_spans_use_inspected_occurrences(inspected):
    p,raw=inspected;fixed,proof=offsets.recover(raw,p)
    assert len(fixed['events'])==len(raw['events'])==9 and proof['inspected_spans']==19
    assert len(proof['replacements'])==29
    restored=deepcopy(fixed)
    for change in proof['replacements']:
        assert change['path'][-1] in {'start','end'}
        node=restored
        for key in change['path'][:-1]:node=node[key]
        node[change['path'][-1]]=change['before']
    assert restored==raw
    target=fixed['events'][5]['attitude']['target']
    assert target['text']=='community' and target['start']==4441
    assert p['transcript_window'].find('community')!=target['start']
    assert fixed['events'][5]['attitude']['modality_evidence'][3]['start']==4714


@pytest.mark.parametrize('change',['raw','source','role'])
def test_no_generic_recovery_for_changed_input(inspected,change):
    p,raw=inspected
    if change=='raw':raw['events'][0]['claim_text']='Different'
    elif change=='source':p['transcript_window']+='changed'
    else:p['role']='A'
    with pytest.raises(ValueError):offsets.recover(raw,p)
