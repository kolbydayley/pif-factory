from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_tsmc_audit_recovery as recovery
from scripts import pif_signal_desk_tsmc_audit_delta_review as delta


def test_actual_full_chain_covers_fifteen_records():
    _, _, p = delta.parent.proposal()
    d = delta.parent.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    fixed, receipt = recovery.recover(raw, p)
    assert len(fixed['events']) == receipt['approved_records'] == 15
    assert not receipt['gold_accepted'] and not receipt['qualified']
    assert fixed['events'][13]['publishability_state'] == 'quarantined'
    assert fixed['events'][14]['publishability_state'] == 'quarantined'


def test_only_reviewed_metadata_can_change():
    baseline, _, _ = delta.parent.proposal()
    fixed, _, _ = delta.proposal()
    recovery.unchanged_except_metadata(baseline, fixed)
    bad = deepcopy(fixed)
    bad['events'][5]['attribution']['transcript_voice'] = None
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_except_metadata(baseline, bad)
    bad = deepcopy(fixed)
    bad['events'][0]['claim_text'] += ' Unsupported.'
    with pytest.raises(ValueError, match='unreviewed'):
        recovery.unchanged_except_metadata(baseline, bad)


@pytest.mark.parametrize('missing', [True, False])
def test_missing_or_rejected_delta_blocks(monkeypatch, missing):
    actual = recovery.verify
    def check(module, packets):
        if module is delta:
            if missing:
                raise ValueError('missing approval')
            return [dict(event_id=delta.EVENT, verdict='needs_correction')], []
        return actual(module, packets)
    monkeypatch.setattr(recovery, 'verify', check)
    _, _, p = delta.parent.proposal()
    d = delta.parent.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError, match='missing approval|not independently supported'):
        recovery.recover(raw, p)
