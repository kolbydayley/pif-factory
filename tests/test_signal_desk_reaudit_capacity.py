import asyncio
from scripts import pif_signal_desk_gold_reaudit_repairs as runner


def test_capacity_deferral_waits_and_reenters_existing_dispatch(tmp_path,monkeypatch):
    calls=[];waits=[]
    async def run(**kwargs):
        calls.append(kwargs)
        if len(calls)==1:raise runner.GoldCapacityDeferred({"reason":"model_capacity_backoff","retry_after_seconds":130})
        return {"complete":True}
    async def sleep(seconds):waits.append(seconds)
    monkeypatch.setattr(runner,"OUT",tmp_path)
    monkeypatch.setattr(runner,"run_gold_split_phase",run)
    monkeypatch.setattr(runner.asyncio,"sleep",sleep)
    assert asyncio.run(runner.run_with_capacity_recovery(dispatch_database="same"))["complete"]
    assert calls==[{"dispatch_database":"same"}]*2
    assert waits==[60,60,10]
