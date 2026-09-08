import json
from unittest.mock import patch
import pytest
from research_factory import signal_desk_tsmc_b_recovery as recovery
from scripts import pif_signal_desk_tsmc_b_review as review


def inputs():
    _,_,p=review.proposal();d=review.run.OUT/'calls'/p['window_id']/'B'
    return json.loads((d/f"{p['packet_sha256']}.output.json").read_text()),p


def test_missing_independent_review_blocks_application():
    raw,p=inputs()
    with patch.object(recovery,'verify',side_effect=ValueError('missing review')):
        with pytest.raises(ValueError,match='missing review'):recovery.recover(raw,p)


def test_one_unsupported_record_blocks_application():
    raw,p=inputs();rows=[{'event_id':e['event_id'],'verdict':'supported'} for e in raw['events']];rows[0]['verdict']='unresolved'
    with patch.object(recovery,'verify',return_value=(rows,[])):
        with pytest.raises(ValueError,match='not independently supported'):recovery.recover(raw,p)
