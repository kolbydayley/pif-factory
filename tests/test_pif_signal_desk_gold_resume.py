"""Offline contract tests for the explicitly throttled Gold-launch CLI."""

from __future__ import annotations

import importlib.util
import json
import stat
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "pif_signal_desk_gold_resume.py"


def _load_resume_script():
    spec = importlib.util.spec_from_file_location(
        "test_pif_signal_desk_gold_resume_script", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_throttled_current_turn_launch_requires_fixed_four_configured_slots(tmp_path, monkeypatch):
    script = _load_resume_script()
    observed: dict[str, object] = {}

    async def fake_supervisor(**kwargs):
        # No provider client is constructed: this is only a CLI/context test.
        observed.update(kwargs)
        from research_factory.signal_desk_background_admission import local_background_admission

        admission = local_background_admission(configured_concurrency=4)
        assert admission.allowed
        assert admission.reason == "operator_current_turn_foreground_override"
        assert admission.provider_concurrency_cap == 3
        return {
            "status": "deferred",
            "reason": "gold_model_capacity_backoff",
            "retry_after_seconds": 1,
            "contains_a1_or_a2": False,
            "provider_calls_started_during_deferred_check": 0,
        }

    monkeypatch.setattr(script, "PIF_ROOT", tmp_path)
    monkeypatch.setattr(script, "run_gold_resume_supervisor", fake_supervisor)

    exit_code = script.main(
        [
            "--allow-sealed-holdout",
            "--allow-current-turn-foreground",
            "--foreground-override-source",
            "kolby_current_turn_throttled_gold_2026_09_02",
            "--concurrency",
            "4",
            "--once",
        ]
    )

    assert exit_code == 0
    assert observed["concurrency"] == 4
    artifacts = tmp_path / "work/signal-desk-rebuild/gold-authoring-v2/artifacts"
    receipts = list((artifacts / "receipts").glob("gold-current-turn-foreground-override-*.json"))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert receipt["provider_calls_started"] == 0
    assert receipt["contains_a1_or_a2"] is False
    assert receipt["foreground_override"] == {
        "enabled": True,
        "source": "kolby_current_turn_throttled_gold_2026_09_02",
        "scope": "current_process_only",
        "configured_concurrency_cap": 4,
        "provider_concurrency_cap": 3,
        "normal_foreground_policy_preserved_outside_scope": True,
    }
    assert stat.S_IMODE(receipts[0].stat().st_mode) == 0o600


def test_current_turn_flag_refuses_a_broader_configured_pool():
    script = _load_resume_script()

    with pytest.raises(SystemExit) as exc_info:
        script.main(
            [
                "--allow-sealed-holdout",
                "--allow-current-turn-foreground",
                "--foreground-override-source",
                "kolby_current_turn_throttled_gold_2026_09_02",
                "--concurrency",
                "2",
                "--once",
            ]
        )
    assert exc_info.value.code == 2


def test_foreground_source_cannot_be_supplied_without_explicit_override_flag():
    script = _load_resume_script()

    with pytest.raises(SystemExit) as exc_info:
        script.main(
            [
                "--allow-sealed-holdout",
                "--foreground-override-source",
                "kolby_current_turn_throttled_gold_2026_09_02",
                "--once",
            ]
        )
    assert exc_info.value.code == 2
