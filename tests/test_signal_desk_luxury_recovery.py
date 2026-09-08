from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_luxury_recovery as recovery
from scripts import pif_signal_desk_luxury_delta_review as delta


def test_reviewed_content_only_and_no_invented_voice():
    original, _, _ = delta.parent.proposal()
    fixed, _, _ = delta.proposal()
    recovery.unchanged_outside_delta(original, fixed, delta.NUMBERS)
    bad = deepcopy(fixed)
    bad['events'][0]['attribution']['transcript_voice'] = 'invented'
    with pytest.raises(ValueError, match='attribution change'):
        recovery.unchanged_outside_delta(original, bad, delta.NUMBERS)
    bad = deepcopy(fixed)
    bad['events'][12]['publishability_state'] = 'accepted'
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_outside_delta(original, bad, delta.NUMBERS)


@pytest.mark.parametrize('missing', [True, False])
def test_missing_or_rejected_delta_blocks(monkeypatch, missing):
    def check():
        if missing:
            raise ValueError('missing approval')
        return [dict(event_id=e['event_id'], verdict='needs_correction') for p in delta.prepare(write=False) for e in p['candidates']], []
    monkeypatch.setattr(recovery, 'verified_delta', check)
    _, _, p = delta.parent.proposal()
    d = delta.parent.run.OUT/'calls'/p['window_id']/'A'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError, match='missing approval|not independently supported'):
        recovery.recover(raw, p)
