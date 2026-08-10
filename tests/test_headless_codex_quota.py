"""Quota-aware executor behavior (durability plan Phase 0.2).

The Aug 6-7 outage burned three full 25-job waves against an exhausted Codex
subscription: the usage-limit error was not recognized as provider pressure,
every failure consumed a durable attempt, and the wave kept dispatching.
"""

from __future__ import annotations

from pathlib import Path

from research_factory import headless_codex as hc


USAGE_LIMIT_LINE = (
    "ERROR: You've hit your usage limit. Visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits or "
    "try again at Aug 8th, 2026 1:30 AM."
)


def test_pressure_signals_recognize_the_usage_limit_string(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text(USAGE_LIMIT_LINE + "\n", encoding="utf-8")
    signals = hc._provider_pressure_signals(log)
    assert any("usage limit" in signal for signal in signals)


def test_pressure_signals_unchanged_for_ordinary_failures(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text("ERROR: something else broke entirely\n", encoding="utf-8")
    assert not any(
        "usage limit" in signal
        for signal in hc._provider_pressure_signals(log)
    )


def test_usage_limit_retry_at_is_extracted(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text(USAGE_LIMIT_LINE + "\n", encoding="utf-8")
    assert hc._usage_limit_retry_at(log) == "Aug 8th, 2026 1:30 AM"


def test_usage_limit_retry_at_absent_returns_none(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text("turn.completed\n", encoding="utf-8")
    assert hc._usage_limit_retry_at(log) is None


def test_attempt_consumption_rule() -> None:
    # Ordinary failure with a real provider call: attempt is spent.
    assert hc._attempt_consumed(
        provider_call_started=True, timed_out=False, status="codex_exec_failed"
    )
    # Timeout carve-out (pre-existing behavior) survives.
    assert not hc._attempt_consumed(
        provider_call_started=True, timed_out=True, status="codex_exec_timeout"
    )
    # Usage-limit failures must not spend durable attempts.
    assert not hc._attempt_consumed(
        provider_call_started=True, timed_out=False, status="codex_usage_limit"
    )
    # Jobs skipped because the wave aborted never started a call.
    assert not hc._attempt_consumed(
        provider_call_started=False,
        timed_out=False,
        status="provider_quota_exhausted",
    )


def test_budget_cap_hit_never_consumes_attempts() -> None:
    assert not hc._attempt_consumed(
        provider_call_started=False,
        timed_out=False,
        status="budget_cap_hit",
    )


def test_every_codex_exec_command_carries_json_for_metering() -> None:
    """The ledger meters from --json usage events; a text-mode call would be
    invisible to the budget. The executor must always request JSON."""
    import ast
    from pathlib import Path as _P

    source = (
        _P(hc.__file__).read_text(encoding="utf-8")
    )
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.List):
            continue
        values = [
            item.value
            for item in node.elts
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        ]
        if "exec" in values and "--ephemeral" in values:
            assert "--json" in values, (
                "codex exec command must include --json unconditionally so "
                "the subscription budget ledger can meter it"
            )


def test_usage_limit_reclassifies_failed_status(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    log.write_text(USAGE_LIMIT_LINE + "\n", encoding="utf-8")
    item = {"status": "codex_exec_failed"}
    signals = hc._provider_pressure_signals(log)
    status, retry_at = hc._quota_reclassification(
        item["status"], signals, log
    )
    assert status == "codex_usage_limit"
    assert retry_at == "Aug 8th, 2026 1:30 AM"


def test_completed_runs_are_never_reclassified(tmp_path: Path) -> None:
    log = tmp_path / "run.log"
    # Even if the transcript mentions limits, a rc=0 run stands.
    log.write_text(USAGE_LIMIT_LINE + "\n", encoding="utf-8")
    status, retry_at = hc._quota_reclassification(
        "codex_exec_completed", hc._provider_pressure_signals(log), log
    )
    assert status == "codex_exec_completed"
    assert retry_at is None
