import json
import sys
from types import SimpleNamespace

import pytest

from scripts import pif_signal_desk_completion_hook as hook


@pytest.mark.parametrize("code", [0, 2])
def test_dispatch_runs_after_child_exit_with_saved_receipt(tmp_path, monkeypatch, code):
    event_id = "01a04040-77ee-77f2-9d26-a5ca1ae56986"
    monkeypatch.setattr(hook, "EVENTS", tmp_path)
    monkeypatch.setattr(hook, "ROOT", tmp_path)
    seen = []
    class IPC:
        sock = SimpleNamespace(close=lambda: None)
        def deliver(self, event, exit_code, canary):
            receipt = json.loads((tmp_path / f"{event}.json").read_text())
            assert receipt["status"] == "child_exited"
            assert receipt["child_exit_code"] == exit_code == code
            seen.append(event)
            return {"resultType": "success"}
    monkeypatch.setattr(hook, "DesktopIPC", IPC)
    monkeypatch.setattr(sys, "argv", ["hook", "--event-id", event_id, "--",
                                    sys.executable, "-c", f"raise SystemExit({code})"])
    hook.main()
    assert seen == [event_id]
    receipt = json.loads((tmp_path / f"{event_id}.json").read_text())
    assert receipt["status"] == "dispatch_accepted"
    assert not receipt["autonomous_wake_verified"]
    with pytest.raises(FileExistsError):
        hook.main()
    assert seen == [event_id]


def test_prompt_distinguishes_delivery_from_acknowledgement():
    prompt = hook.completion_prompt("event", 0, True)
    assert "acknowledgement, not dispatch success" in prompt
    assert "keep signal-desk-completion-monitor active" in prompt
    assert "signal-desk-gold-rebuild-supervisor paused" in prompt


def test_retry_only_known_predispatch_owner_failure(tmp_path,monkeypatch):
    calls=[]
    class IPC:
        sock=SimpleNamespace(close=lambda:None)
        def deliver(self,*args):
            calls.append(args)
            if len(calls)==1:raise hook.OwnerUnavailable()
            return {"resultType":"success"}
    monkeypatch.setattr(hook,"DesktopIPC",IPC)
    monkeypatch.setattr(hook.time,"sleep",lambda seconds:None)
    result=hook.deliver_with_owner_retry("event",0,False,tmp_path/"r.json",{})
    assert result["resultType"]=="success" and len(calls)==2


def test_uncertain_delivery_is_never_retried(tmp_path,monkeypatch):
    calls=[]
    class IPC:
        sock=SimpleNamespace(close=lambda:None)
        def deliver(self,*args):
            calls.append(args);raise RuntimeError("connection lost after send")
    monkeypatch.setattr(hook,"DesktopIPC",IPC)
    with pytest.raises(RuntimeError):hook.deliver_with_owner_retry("event",0,False,tmp_path/"r.json",{})
    assert len(calls)==1
