from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_flattened_recovery as recovery
from scripts import pif_signal_desk_flattened_delta_review as delta


def test_only_reviewed_delta_can_change():
    baseline, _, _ = delta.parent.proposal()
    fixed, _, _ = delta.proposal()
    recovery.unchanged_outside_delta(baseline, fixed, delta.NUMBERS)
    changed = deepcopy(fixed)
    changed['events'][0]['claim_text'] += ' Unsupported addition.'
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_outside_delta(baseline, changed, delta.NUMBERS)
    changed = deepcopy(fixed)
    changed['events'][8]['attribution']['transcript_voice'] = 'invented speaker'
    with pytest.raises(ValueError, match='speaker assignment'):
        recovery.unchanged_outside_delta(baseline, changed, delta.NUMBERS)


@pytest.mark.parametrize('missing', [True, False])
def test_missing_or_rejected_delta_blocks(monkeypatch, missing):
    def fail(module, packets):
        if missing:
            raise ValueError('missing approval')
        return [dict(event_id=e['event_id'], verdict='needs_correction') for p in packets for e in p['candidates']], []
    monkeypatch.setattr(recovery, 'verify', fail)
    _, _, p = delta.parent.proposal()
    d = delta.parent.run.OUT/'calls'/p['window_id']/'A'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError, match='missing approval|not independently supported'):
        recovery.recover(raw, p)
