import json
from unittest.mock import patch
import pytest
from research_factory import signal_desk_tsmc_recovery as recovery
from scripts import pif_signal_desk_tsmc_repair_review as review


def inputs():
    _,_,packet=review.proposal()
    d=review.run.OUT/'calls'/packet['window_id']/'A'
    return json.loads((d/f"{packet['packet_sha256']}.output.json").read_text()),packet


def test_missing_approval_fails_closed():
    raw,p=inputs()
    with patch.object(recovery,'review_proof',side_effect=ValueError('missing approval')):
        with pytest.raises(ValueError,match='missing approval'):recovery.recover(raw,p)


def test_one_unsupported_record_blocks_application():
    raw,p=inputs()
    rows=[{'event_id':e['event_id'],'verdict':'supported'} for e in raw['events']]
    rows[0]['verdict']='needs_correction'
    with patch.object(recovery,'review_proof',return_value=(rows,[])):
        with pytest.raises(ValueError,match='not independently supported'):recovery.recover(raw,p)


def test_delta_needs_actual_approval():
    raw,p=inputs()
    with patch.object(recovery,'verify',side_effect=ValueError('missing delta approval')):
        with pytest.raises(ValueError,match='missing delta approval'):recovery.recover(raw,p)


def test_delta_rejection_blocks_application():
    raw,p=inputs()
    rows=[{'event_id':raw['events'][10]['event_id'],'verdict':'needs_correction'}]
    with patch.object(recovery,'verify',return_value=(rows,[])):
        with pytest.raises(ValueError,match='delta not independently supported'):recovery.recover(raw,p)
