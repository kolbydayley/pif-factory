import json
from unittest.mock import patch
import pytest
from research_factory import signal_desk_hidden_brain_c_recovery as recovery
from scripts import pif_signal_desk_hidden_brain_c_repair_review as review


def inputs():
    d=review.run.OUT/'calls'/review.WID/'C'
    return json.loads(next(d.glob('*.output.json')).read_text()),json.loads((d/'packet.json').read_text())


def test_actual_review_chain_and_unchanged_lineage():
    raw,p=inputs();fixed,proof=recovery.recover(raw,p)
    assert len(fixed['records']['events'])==12
    assert fixed['input_dispositions']==raw['input_dispositions']
    assert len(proof['reviews'])==2
    assert proof['gold_accepted'] is False


def test_unsupported_review_cannot_be_applied():
    raw,p=inputs()
    with patch.object(recovery,'verify',return_value=([{'event_id':review.WID+'_c2','verdict':'needs_correction'}],[])):
        with pytest.raises(ValueError,match='independent support'):recovery.recover(raw,p)
