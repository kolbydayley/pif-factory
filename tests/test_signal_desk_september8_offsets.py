from copy import deepcopy
import json
import pytest
from research_factory.signal_desk_september8_offsets import CASES,recover
from scripts import pif_signal_desk_lineage_qualification as run


def inputs(case):
    spec=CASES[case];d=run.OUT/'calls'/spec['window']/spec['role']
    return json.loads((d/(spec['packet']+'.output.json')).read_text()),json.loads((d/'packet.json').read_text())


@pytest.mark.parametrize('case',list(CASES))
def test_only_explicit_numeric_fields_change(case):
    original,p=inputs(case);fixed,proof=recover(case,original,p)
    restored=deepcopy(fixed)
    for change in proof['replacements']:
        assert change['path'][-1] in {'start','end'}
        assert type(change['before']) is type(change['after']) is int
        target=restored
        for key in change['path'][:-1]:target=target[key]
        target[change['path'][-1]]=change['before']
    assert restored==original
    records=fixed['records'] if CASES[case]['role']=='C' else fixed
    assert len(records['events'])==CASES[case]['records']
    if CASES[case]['role']=='C':
        assert fixed['input_dispositions']==original['input_dispositions']
        assert fixed['additions']==original['additions']
    assert proof['semantic_fields_changed'] is False
    assert proof['gold_accepted'] is False


@pytest.mark.parametrize('case',list(CASES))
def test_changed_raw_rejected(case):
    raw,p=inputs(case);records=raw['records'] if CASES[case]['role']=='C' else raw;records['events'][0]['claim_text']+='changed'
    with pytest.raises(ValueError,match='inspected original'):recover(case,raw,p)
