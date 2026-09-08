from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_flattened_context_recovery as recovery
from scripts import pif_signal_desk_flattened_context_review as delta


def test_actual_full_approval_chain_preserves_acceptance_gate():
    _, _, p = delta.parent.parent.proposal()
    d = delta.parent.parent.run.OUT/'calls'/p['window_id']/'A'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    fixed, receipt = recovery.recover(raw, p)
    assert fixed == delta.proposal()[0]
    assert receipt['approved_records'] == 27
    assert receipt['gold_accepted'] is False
    assert receipt['qualified'] is False


def test_unreviewed_voice_or_other_record_cannot_change():
    baseline, _, _ = delta.parent.proposal()
    fixed, _, _ = delta.proposal()
    recovery.unchanged_except_context_targets(baseline, fixed)
    bad = deepcopy(fixed)
    bad['events'][13]['attribution']['transcript_voice'] = 'invented'
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_except_context_targets(baseline, bad)
    bad = deepcopy(fixed)
    bad['events'][24]['claim_text'] += ' Unreviewed.'
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_except_context_targets(baseline, bad)


@pytest.mark.parametrize('missing', [True, False])
def test_missing_or_rejected_last_delta_blocks(monkeypatch, missing):
    actual = recovery.verify
    def check(module, packets):
        if module is delta:
            if missing:
                raise ValueError('missing approval')
            return [dict(event_id=e['event_id'], verdict='needs_correction') for p in packets for e in p['candidates']], []
        return actual(module, packets)
    monkeypatch.setattr(recovery, 'verify', check)
    _, _, p = delta.parent.parent.proposal()
    d = delta.parent.parent.run.OUT/'calls'/p['window_id']/'A'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError, match='missing approval|not independently supported'):
        recovery.recover(raw, p)
