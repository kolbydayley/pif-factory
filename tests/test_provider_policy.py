"""Fail-closed stage->(provider, model) routing (durability plan Phase 2)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import provider_policy as pp


def test_live_policy_loads_and_pins_codex_for_all_stages() -> None:
    policy = pp.load_policy()
    for stage in ("label_segment", "episode_context", "audit_label", "reviewer_audit"):
        resolved = pp.stage_policy(stage, policy=policy)
        assert resolved.provider == "codex"
        assert resolved.model == "gpt-5.5"


def test_unknown_stage_raises() -> None:
    with pytest.raises(pp.ProviderPolicyError, match="unknown stage"):
        pp.stage_policy("mystery_stage")


def test_unknown_provider_in_policy_raises(tmp_path: Path) -> None:
    bad = tmp_path / "policy.json"
    bad.write_text(
        json.dumps(
            {
                "schema_version": "pif_provider_policy_v1",
                "fail_closed": True,
                "providers": {"codex": {}},
                "stages": {"label_segment": {"provider": "made_up", "model": "x"}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(pp.ProviderPolicyError, match="unknown provider"):
        pp.stage_policy("label_segment", policy=pp.load_policy(bad))


def test_missing_model_raises(tmp_path: Path) -> None:
    bad = tmp_path / "policy.json"
    bad.write_text(
        json.dumps(
            {
                "schema_version": "pif_provider_policy_v1",
                "fail_closed": True,
                "providers": {"codex": {}},
                "stages": {"label_segment": {"provider": "codex"}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(pp.ProviderPolicyError, match="model"):
        pp.stage_policy("label_segment", policy=pp.load_policy(bad))


def test_assert_stage_model_matches_policy() -> None:
    # The worker's old hard pins become policy assertions: the same failure
    # class (wrong model reaches a stage) still raises, but the truth lives
    # in config/provider_policy.json instead of four scattered constants.
    pp.assert_stage_model("label_segment", "gpt-5.5")
    with pytest.raises(pp.ProviderPolicyError, match="does not match policy"):
        pp.assert_stage_model("label_segment", "gpt-4o")
