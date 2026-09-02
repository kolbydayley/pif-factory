from __future__ import annotations

import asyncio

import pytest

from research_factory.signal_desk_background_admission import (
    BackgroundWorkDeferred,
    BackgroundWorkPreempted,
    evaluate_background_admission,
    run_foreground_preemptible,
)


def test_foreground_codex_always_blocks_background_provider_admission():
    state = evaluate_background_admission(
        configured_concurrency=8,
        input_idle_seconds=9_000,
        frontmost_application="ChatGPT",
    )
    assert not state.allowed
    assert state.reason == "foreground_codex_active"
    assert state.provider_concurrency_cap == 0


@pytest.mark.parametrize(
    ("idle", "expected_cap"),
    [
        (120, 1),
        (899, 1),
        (900, 2),
        (1_800, 4),
        (2_700, 8),
    ],
)
def test_unattended_ramp_bounds_background_concurrency(idle, expected_cap):
    state = evaluate_background_admission(
        configured_concurrency=8,
        input_idle_seconds=idle,
        frontmost_application="Finder",
    )
    assert state.allowed
    assert state.provider_concurrency_cap == expected_cap


def test_recent_or_unknown_input_fails_closed():
    recent = evaluate_background_admission(
        configured_concurrency=4,
        input_idle_seconds=15,
        frontmost_application="Finder",
    )
    unknown = evaluate_background_admission(
        configured_concurrency=4,
        input_idle_seconds=None,
        frontmost_application=None,
    )
    assert recent.reason == "recent_local_input"
    assert not recent.allowed
    assert unknown.reason == "foreground_state_unknown"
    assert not unknown.allowed


def test_preemptible_turn_never_starts_while_foreground_is_active():
    async def scenario():
        called = False

        async def operation():
            nonlocal called
            called = True
            return "unexpected"

        def blocked(**_kwargs):
            return evaluate_background_admission(
                configured_concurrency=1,
                input_idle_seconds=1,
                frontmost_application="ChatGPT",
            )

        with pytest.raises(BackgroundWorkDeferred):
            await run_foreground_preemptible(
                operation, configured_concurrency=1, admission=blocked, poll_seconds=0.001
            )
        assert not called

    asyncio.run(scenario())


def test_preemptible_turn_cancels_when_foreground_activity_resumes():
    async def scenario():
        calls = 0
        cancelled = asyncio.Event()
        started = asyncio.Event()

        async def operation():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        def changing_admission(**_kwargs):
            nonlocal calls
            calls += 1
            return evaluate_background_admission(
                configured_concurrency=1,
                input_idle_seconds=1 if calls > 1 else 900,
                frontmost_application="Finder",
            )

        with pytest.raises(BackgroundWorkPreempted) as exc_info:
            await run_foreground_preemptible(
                operation,
                configured_concurrency=1,
                admission=changing_admission,
                poll_seconds=0.001,
            )
        assert started.is_set()
        assert cancelled.is_set()
        assert exc_info.value.admission.reason == "recent_local_input"

    asyncio.run(scenario())
