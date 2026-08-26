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


def test_consumption_summary_totals_and_warns(tmp_path, monkeypatch):
    import json as _json
    import time as _time
    from research_factory import pif_dual_lane_daily as daily
    monkeypatch.setattr(daily, "SHADOW_ROOT", tmp_path)
    now = _time.time()
    for i, (calls, age_days) in enumerate([(500, 1), (400, 2), (300, 9)]):  # 3rd is stale
        p = tmp_path / f"receipt-bulk-grok-2026081{i}T000000.json"
        p.write_text(_json.dumps({"calls_made": calls}))
        import os
        os.utime(p, (now - age_days * 86400, now - age_days * 86400))
    s = daily.consumption_summary(
        "grok", {"calls_made": 200, "failure_counts": {"timeout": 2}}, now=now)
    assert s["calls_7d"] == 900          # stale receipt excluded
    assert s["calls_today"] == 200
    assert s["failures_today"] == {"timeout": 2}
    assert s["weekly_call_budget"] == 650
    assert "quota_warning" in s          # 900 >= 0.8 * 650


def test_consumption_summary_no_budget_lane(tmp_path, monkeypatch):
    from research_factory import pif_dual_lane_daily as daily
    monkeypatch.setattr(daily, "SHADOW_ROOT", tmp_path)
    s = daily.consumption_summary("glm", None)
    assert s["calls_7d"] == 0 and "weekly_call_budget" not in s
    assert "quota_warning" not in s


def test_draft_with_omission_counts_calls():
    from research_factory.cheap_lane_adapters import draft_with_omission
    from research_factory.lane_profiles import OMISSION_SUFFIX
    calls = []

    def fake(prompt):
        calls.append(prompt)
        label = {"claims": [{"claim_text": f"c{len(calls)}", "claim_type": "assessment",
                             "evidence": "e", "confidence": 0.9}]}
        return {"ok": True, "label": label, "elapsed": 1.0, "calls": 1}

    res = draft_with_omission(fake, "T {SEGMENT_TEXT}", "w", 1, OMISSION_SUFFIX)
    assert res["calls"] == 2


def test_no_audit_lane_green_without_audit_sample():
    r = _receipt(audit_pass_rate=None)
    assert evaluate_receipt(r, 100, require_audit=False)["green"] is True
    assert evaluate_receipt(r, 100)["green"] is False  # audited lanes unchanged
    assert evaluate_receipt(_receipt(drafted=50, audit_pass_rate=None), 100,
                            require_audit=False)["green"] is False  # draft floor still applies


def test_runner_lock_held_detected_from_stdout():
    from research_factory.pif_dual_lane_daily import _runner_lock_held
    out = '{"aborted": "runner_lock_held", "holder_pid": "123"}\n'
    assert _runner_lock_held(out) is True
    assert _runner_lock_held("[glm] rolling...\n" + out) is True


def test_runner_lock_held_ignores_normal_output():
    from research_factory.pif_dual_lane_daily import _runner_lock_held
    assert _runner_lock_held(None) is False
    assert _runner_lock_held("") is False
    assert _runner_lock_held('{"run_id": "bulk-glm-x", "drafted": 94}\n') is False
    assert _runner_lock_held('not json {curly\n') is False


def test_provider_quota_exhaustion_detected():
    from research_factory.pif_dual_lane_daily import _provider_quota_exhausted
    assert _provider_quota_exhausted(
        {"drafted": 0, "calls_made": 100, "failure_counts": {"provider": 100}}) is True
    assert _provider_quota_exhausted(
        {"drafted": 95, "calls_made": 138, "failure_counts": {"provider": 5}}) is False
    assert _provider_quota_exhausted(
        {"drafted": 0, "calls_made": 0, "failure_counts": {}}) is False
    assert _provider_quota_exhausted(
        {"drafted": 0, "calls_made": 10, "failure_counts": {"timeout": 10}}) is False


def test_lane_partitions_are_disjoint_and_cover():
    from research_factory.pif_bulk_draft_runner import (
        LANE_PARTITIONS, N_PARTITIONS, segment_partition)
    all_parts = [p for parts in LANE_PARTITIONS.values() for p in parts]
    assert len(all_parts) == len(set(all_parts)) == N_PARTITIONS
    for seg in ("seg_a", "seg_b", "seg_1234", "seg_cbe1f0bdf1da18d2f4542da4"):
        assert segment_partition(seg) == segment_partition(seg)  # stable
        assert 0 <= segment_partition(seg) < N_PARTITIONS
