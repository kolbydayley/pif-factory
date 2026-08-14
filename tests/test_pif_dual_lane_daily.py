from research_factory.pif_dual_lane_daily import evaluate_receipt


def _receipt(**kw):
    base = {"drafted": 100, "failed": 0, "audit_pass_rate": 1.0}
    base.update(kw)
    return base


def test_green_on_healthy_day():
    assert evaluate_receipt(_receipt(), 100)["green"] is True


def test_glm_tier1_debut_numbers_are_green():
    # 88/100 drafted, post-fix audit pass 0.923 — the accepted precedent day.
    assert evaluate_receipt(_receipt(drafted=88, audit_pass_rate=0.923), 100)["green"] is True


def test_red_on_missing_receipt_and_abort():
    assert evaluate_receipt(None, 100)["reason"] == "no_receipt"
    assert evaluate_receipt(_receipt(aborted="codex_budget"), 100)["green"] is False


def test_red_on_low_drafting_or_low_audit():
    assert evaluate_receipt(_receipt(drafted=60), 100)["green"] is False
    assert evaluate_receipt(_receipt(audit_pass_rate=0.5), 100)["green"] is False


def test_red_on_missing_audit_sample():
    assert evaluate_receipt(_receipt(audit_pass_rate=None), 100)["reason"] == "no_audit_sample"
