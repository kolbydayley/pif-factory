import pytest
from research_factory import signal_desk_ai_governance_audit_context_recovery as recovery
from tests.test_signal_desk_ai_governance_audit_recovery import inputs


def test_missing_third_review_cannot_be_applied(monkeypatch):
    from scripts import pif_signal_desk_ai_governance_audit_context_review as third
    actual=recovery.verify
    def missing(module,packets):
        if module is third:raise FileNotFoundError('missing review')
        return actual(module,packets)
    monkeypatch.setattr(recovery,'verify',missing)
    with pytest.raises(FileNotFoundError):
        recovery.recover(*inputs())


def test_actual_complete_chain_covers_fifteen_records():
    fixed,proof=recovery.recover(*inputs())
    assert len(fixed['events'])==proof['approved_records']==15
    assert len(proof['generations'])==3
    assert not proof['gold_accepted']


def test_unsupported_latest_review_blocks(monkeypatch):
    from scripts import pif_signal_desk_ai_governance_audit_context_review as third
    actual=recovery.verify
    def reject(module,packets):
        rows,proofs=actual(module,packets)
        if module is third:rows[0]['verdict']='needs_correction'
        return rows,proofs
    monkeypatch.setattr(recovery,'verify',reject)
    with pytest.raises(ValueError,match='not independently supported'):
        recovery.recover(*inputs())
