import pytest
from research_factory import signal_desk_ai_governance_audit_context_recovery as recovery
from tests.test_signal_desk_ai_governance_audit_recovery import inputs


def test_unrun_third_review_cannot_be_applied():
    with pytest.raises(FileNotFoundError):
        recovery.recover(*inputs())
