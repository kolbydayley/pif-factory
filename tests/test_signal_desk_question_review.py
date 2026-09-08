import pytest
import tiktoken
from research_factory import signal_desk_question_review as review
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_question_lineage import fixture


def test_question_and_both_author_dispositions_receive_full_source():
    value,source,wid=fixture()
    count=lambda s:len(tiktoken.get_encoding('o200k_base').encode(s))
    records=review.packets(value['records'],source=source,window_id=wid,token_count=count)
    assert len(records)==1 and records[0]['transcript_window']==source
    assert records[0]['candidates'][0]['speech_act']=='question'
    assert records[0]['system_sha256']==digest(review.SYSTEM)
    p={'window_id':wid,'transcript_window':source,'transcript_structure':'speaker_turn'};p['packet_sha256']=digest(p)
    author=review.lineage.packet(p,author_a=value['records'],author_b=value['records'])
    ledgers=review.ledger_packets(value,author,token_count=count)
    assert sum(len(p['candidates']) for p in ledgers)==2
    assert all(p['transcript_window']==source and p['system_sha256']==digest(review.LEDGER_SYSTEM) for p in ledgers)


def test_oversize_source_fails_instead_of_truncating():
    value,source,wid=fixture()
    with pytest.raises(ValueError,match='exceed'):
        review.packets(value['records'],source=source,window_id=wid,token_count=lambda s:20000)
