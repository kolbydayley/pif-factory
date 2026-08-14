import json

import pytest

from research_factory import pif_lane_promotion as promo


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    policy = {
        "providers": {"codex": {}, "glm_opencode": {}},
        "stages": {"label_segment": {"provider": "codex", "model": "gpt-5.5"}},
    }
    policy_path = tmp_path / "provider_policy.json"
    policy_path.write_text(json.dumps(policy))
    tier_path = tmp_path / "tier_state.json"
    monkeypatch.setattr(promo, "POLICY_PATH", policy_path)
    monkeypatch.setattr(promo, "TIER_STATE_PATH", tier_path)
    monkeypatch.setattr(promo, "PIF_ROOT", tmp_path)
    monkeypatch.setattr(promo, "_git_commit", lambda paths, message: None)
    monkeypatch.setattr(promo, "_notify", lambda title, body: None)
    return tmp_path


def _report(tmp_path, lane, green):
    path = tmp_path / "report.json"
    path.write_text(json.dumps({"lanes": {lane: {"gates_passed": green}}}))
    return path


def test_promote_refuses_red_gates(sandbox):
    report = _report(sandbox, "glm", green=False)
    with pytest.raises(SystemExit):
        promo.promote(report, "glm")


def test_promote_flips_policy_and_seeds_tier(sandbox):
    report = _report(sandbox, "glm", green=True)
    result = promo.promote(report, "glm")
    policy = json.loads(promo.POLICY_PATH.read_text())
    stage = policy["stages"]["label_segment"]
    assert stage["provider"] == "glm_opencode"
    assert stage["frozen_fallback"] == {"provider": "codex", "model": "gpt-5.5"}
    tier = json.loads(promo.TIER_STATE_PATH.read_text())
    assert tier["daily_cap"] == 100 and tier["frozen"] is False
    assert result["promoted"] == "glm"


def test_freeze_reverts_to_fallback(sandbox):
    promo.promote(_report(sandbox, "glm", green=True), "glm")
    promo.freeze("glm", "audit breach")
    policy = json.loads(promo.POLICY_PATH.read_text())
    assert policy["stages"]["label_segment"]["provider"] == "codex"
    tier = json.loads(promo.TIER_STATE_PATH.read_text())
    assert tier["frozen"] is True


def test_tier_promotes_after_seven_contiguous_green_days(sandbox):
    promo.promote(_report(sandbox, "glm", green=True), "glm")
    for day in range(14, 21):
        state = promo.tier_tick("glm", f"2026-08-{day}", green=True)
    assert state["daily_cap"] == 400
    assert state["streak_days"] == 0


def test_tier_streak_resets_on_gap_day(sandbox):
    promo.promote(_report(sandbox, "glm", green=True), "glm")
    for day in range(14, 18):
        promo.tier_tick("glm", f"2026-08-{day}", green=True)
    state = promo.tier_tick("glm", "2026-08-19", green=True)  # 08-18 missing
    assert state["streak_days"] == 1


def test_tier_tick_refuses_frozen_lane(sandbox):
    promo.promote(_report(sandbox, "glm", green=True), "glm")
    promo.freeze("glm", "breach")
    with pytest.raises(SystemExit):
        promo.tier_tick("glm", "2026-08-14", green=True)


def test_tier_state_paths_are_per_lane(sandbox):
    assert promo.tier_state_path("glm") == promo.TIER_STATE_PATH  # legacy live file
    grok_path = promo.tier_state_path("grok")
    assert grok_path != promo.TIER_STATE_PATH
    assert grok_path.name == "tier_state_grok.json"


def test_init_tier_seeds_shadow_lane_without_policy_edit(sandbox):
    report = _report(sandbox, "grok", green=True)
    policy_before = promo.POLICY_PATH.read_text()
    state = promo.init_tier("grok", report)
    assert promo.POLICY_PATH.read_text() == policy_before  # policy untouched
    assert state["daily_cap"] == 100 and state["shadow_lane"] is True
    saved = json.loads(promo.tier_state_path("grok").read_text())
    assert saved["lane"] == "grok" and saved["frozen"] is False


def test_init_tier_refuses_red_gates_and_existing_state(sandbox):
    with pytest.raises(SystemExit):
        promo.init_tier("grok", _report(sandbox, "grok", green=False))
    promo.init_tier("grok", _report(sandbox, "grok", green=True))
    with pytest.raises(SystemExit):
        promo.init_tier("grok", _report(sandbox, "grok", green=True))


def test_init_tier_accepts_recalibrated_report_shape(sandbox):
    path = sandbox / "recal.json"
    path.write_text(json.dumps({"grok": {"gates_passed": True, "metrics": {}}}))
    state = promo.init_tier("grok", path)
    assert state["lane"] == "grok"


def test_dual_lane_tier_ticks_are_independent(sandbox):
    promo.promote(_report(sandbox, "glm", green=True), "glm")
    promo.init_tier("grok", _report(sandbox, "grok", green=True))
    promo.tier_tick("glm", "2026-08-14", green=True)
    state = promo.tier_tick("grok", "2026-08-14", green=True)
    glm_state = json.loads(promo.TIER_STATE_PATH.read_text())
    assert glm_state["lane"] == "glm" and glm_state["streak_days"] == 1
    assert state["lane"] == "grok" and state["streak_days"] == 1
