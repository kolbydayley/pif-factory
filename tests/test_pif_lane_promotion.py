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
