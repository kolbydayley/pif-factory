import asyncio
import json
import pytest
from scripts import pif_signal_desk_full_event_v4_run as runner
from research_factory import signal_desk_full_event_v5_prompts as author
from research_factory import signal_desk_full_event_v5_review as review
from research_factory import signal_desk_full_event_v5 as contract
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4_run import setup
from test_signal_desk_full_event_v5 import value, SOURCE


def source():
    s={"window_id":"dev","transcript_window":SOURCE,"transcript_structure":"speaker_turn"};s["packet_sha256"]=digest(s);return s


@pytest.mark.parametrize("role",["A","B","AUDIT"])
def test_independent_roles_preserve_source_and_cannot_see_peer_labels(role):
    p=author.packet(source(),role)
    assert p["transcript_window"]==SOURCE and "author_a" not in p and "author_b" not in p
    assert p["system_sha256"]==digest(author.prompts()[role])
    assert p["schema_sha256"]==digest(contract.schema())
    with pytest.raises(ValueError):author.packet(source(),role,author_a=value())


def test_all_roles_and_final_share_context_rules():
    for text in author.prompts().values():assert contract.CONTEXT_RULES in text
    assert author.COMMON in review.SYSTEM
    p=review.packets(value(),source=SOURCE,window_id="dev",token_count=lambda text:1)[0]
    assert "context_evidence" in p["candidates"][0]
    assert p["schema_sha256"]==digest(review.schema(p))
    assert p["system_sha256"]==digest(review.SYSTEM)


def test_v5_pipeline_uses_v5_everywhere_and_independent_audit(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);roles=[]
    async def fake(packets,**kwargs):
        p=packets[0];roles.append(p["role"])
        assert contract.CONTEXT_RULES in kwargs["system_for_packet"](p)
        assert kwargs["schema_for_packet"](p)["properties"]["schema_version"]["const"]==contract.VERSION
        if p["role"]=="C":assert p["author_a"]==p["author_b"]==value()
        else:assert "author_a" not in p and "author_b" not in p
        result=kwargs["validator"](value(),p)
        runner.immutable_json(kwargs["output_root"]/f"{p['packet_sha256']}.result.json",result)
        return 0
    monkeypatch.setattr(runner,"metered_execute",fake)
    result=asyncio.run(runner.execute(plan,output_root=out,packet_builder=author.packet,prompt_factory=author.prompts,
        review_contract=review,output_validator=contract.validate,output_schema=contract.schema,task_prefix="v5-test"))
    assert result==0 and roles==["A","B","C","AUDIT"]
    p=json.loads(next((out/"final-packets").glob('*.json')).read_text())
    assert p["system_sha256"]==digest(review.SYSTEM) and not p["gold_accepted"]
