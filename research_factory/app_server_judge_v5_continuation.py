from __future__ import annotations

"""Supervise fixture-reference completion and one fresh calibration attempt."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from .app_server_capacity_probe import wait_for_launch_capacity
from .app_server_judge_v5_diagnostic import _record, _write_immutable_json
from .app_server_judge_v5_fresh_calibration import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_REFERENCE_ROOT,
    FreshCalibrationError,
    load_frozen_fixture_reference,
    run_fresh_reference_calibration,
)
from .app_server_runtime_lock_v17 import (
    DEFAULT_CONTROL_ROOT,
    DEFAULT_MANIFEST,
    build_runtime_lock_v17,
    verify_wait_intent,
    write_launch_receipt,
)
from .util import now_iso, write_text_atomic


CONTINUATION_TERMINAL_VERSION = (
    "pif_app_server_reference_to_calibration_continuation_terminal_v1"
)
CONTINUATION_STATE_VERSION = (
    "pif_app_server_reference_to_calibration_continuation_state_v1"
)


class ContinuationStopped(RuntimeError):
    """A frozen STOP sentinel halted the continuation before another turn."""


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("continuation artifact is not an object")
    return value


def _write_state(path: Path, *, status: str, **values: Any) -> None:
    payload = {
        "schema_version": CONTINUATION_STATE_VERSION,
        "updated_at": now_iso(),
        "status": status,
        "production_mutated": False,
        **values,
    }
    write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _stop_present(*paths: Path) -> bool:
    return any(path.expanduser().resolve().exists() for path in paths)


async def run_continuation(
    *,
    repo_root: Path,
    reference_root: Path = DEFAULT_REFERENCE_ROOT,
    calibration_root: Path = DEFAULT_OUTPUT_ROOT,
    control_root: Path = DEFAULT_CONTROL_ROOT,
    manifest_path: Path = DEFAULT_MANIFEST,
    poll_seconds: float = 300.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    if poll_seconds <= 0:
        raise ValueError("continuation poll interval must be positive")
    os.environ.pop("OPENAI_API_KEY", None)
    repo = repo_root.expanduser().resolve()
    reference = reference_root.expanduser().resolve()
    calibration = calibration_root.expanduser().resolve()
    control = control_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    control.mkdir(parents=True, exist_ok=True)
    state_path = control / "state.json"
    terminal_path = control / "terminal.json"
    wait_intent_path = control / "wait-intent-v17.json"
    capacity_path = control / "prelaunch-capacity.json"
    launch_receipt_path = control / "launch-receipt-v17.json"
    stop_paths = (
        repo / "work/app-server-development-v2/unattended-pipeline-v5/STOP",
        reference / "STOP",
        calibration / "STOP",
        control / "STOP",
    )
    if terminal_path.exists():
        return _load(terminal_path)
    verify_wait_intent(repo_root=repo, path=wait_intent_path)
    try:
        while not (reference / "terminal.json").is_file():
            if _stop_present(*stop_paths):
                raise ContinuationStopped("STOP before fixture reference")
            _write_state(
                state_path,
                status="waiting_for_fixture_reference_v2",
                semantic_attempt_started=False,
                reference_terminal_present=False,
            )
            await sleep(poll_seconds)
        if _stop_present(*stop_paths):
            raise ContinuationStopped("STOP after fixture reference")
        _write_state(
            state_path,
            status="verifying_fixture_reference_v2",
            semantic_attempt_started=False,
            reference_terminal_present=True,
        )
        try:
            frozen_reference = load_frozen_fixture_reference(reference)
        except FreshCalibrationError as exc:
            terminal = {
                "schema_version": CONTINUATION_TERMINAL_VERSION,
                "state": "stopped",
                "terminal_at": now_iso(),
                "terminal_reason": "fixture_reference_not_admissible_fresh_calibration_not_started",
                "error_class": type(exc).__name__,
                "reference_terminal": _record(reference / "terminal.json"),
                "semantic_attempt_started": False,
                "calibration_passed": False,
                "selection_authorized": False,
                "production_mutated": False,
            }
            _write_immutable_json(terminal_path, terminal)
            _write_state(state_path, status="stopped_reference_not_admissible")
            return terminal
        verify_wait_intent(repo_root=repo, path=wait_intent_path)
        _write_state(
            state_path,
            status="waiting_for_fresh_calibration_capacity",
            semantic_attempt_started=False,
            reference_terminal_present=True,
            reference_frozen=True,
        )

        async def capacity_sleep(seconds: float) -> None:
            if _stop_present(*stop_paths):
                raise ContinuationStopped("STOP while waiting for capacity")
            _write_state(
                state_path,
                status="waiting_for_fresh_calibration_capacity",
                semantic_attempt_started=False,
                reference_terminal_present=True,
                reference_frozen=True,
            )
            await sleep(seconds)

        await wait_for_launch_capacity(
            output_path=capacity_path,
            sleep=capacity_sleep,
            recheck_seconds=poll_seconds,
        )
        if _stop_present(*stop_paths):
            raise ContinuationStopped("STOP after capacity clearance")
        lock = build_runtime_lock_v17(
            repo_root=repo,
            manifest_path=manifest,
            wait_intent_path=wait_intent_path,
            launch_capacity_path=capacity_path,
            reference_root=reference,
        )
        write_launch_receipt(
            output_path=launch_receipt_path,
            manifest_path=manifest,
            capacity_path=capacity_path,
            supervisor_pid=os.getpid(),
        )
        _write_state(
            state_path,
            status="running_fresh_reference_calibration",
            semantic_attempt_started=True,
            reference_terminal_present=True,
            reference_frozen=True,
            runtime_lock_schema_version=lock["schema_version"],
        )
        calibration_terminal = await run_fresh_reference_calibration(
            reference_root=reference,
            output_dir=calibration,
            model="gpt-5.6-sol",
            reasoning_effort="high",
            timeout_seconds=1200.0,
        )
        passed = (
            calibration_terminal.get("state") == "completed"
            and calibration_terminal.get("calibration_passed") is True
            and calibration_terminal.get("selection_authorized") is True
        )
        terminal = {
            "schema_version": CONTINUATION_TERMINAL_VERSION,
            "state": calibration_terminal.get("state", "failed"),
            "terminal_at": now_iso(),
            "terminal_reason": (
                "fresh_calibration_passed_selection_ready"
                if passed
                else calibration_terminal.get(
                    "terminal_reason", "infrastructure_or_judge_attempt_failed"
                )
            ),
            "reference_terminal": frozen_reference["records"]["terminal"],
            "runtime_lock": _record(manifest),
            "launch_receipt": _record(launch_receipt_path),
            "calibration_terminal": _record(calibration / "terminal.json"),
            "semantic_attempt_started": True,
            "calibration_passed": passed,
            "selection_authorized": passed,
            "production_mutated": False,
        }
        _write_immutable_json(terminal_path, terminal)
        _write_state(
            state_path,
            status=("selection_ready" if passed else "terminal"),
            semantic_attempt_started=True,
            calibration_passed=passed,
            selection_authorized=passed,
        )
        return terminal
    except ContinuationStopped as exc:
        terminal = {
            "schema_version": CONTINUATION_TERMINAL_VERSION,
            "state": "stopped",
            "terminal_at": now_iso(),
            "terminal_reason": "operator_stop_before_next_semantic_turn",
            "error_class": type(exc).__name__,
            "semantic_attempt_started": calibration.exists(),
            "calibration_passed": False,
            "selection_authorized": False,
            "production_mutated": False,
        }
        _write_immutable_json(terminal_path, terminal)
        _write_state(state_path, status="stopped")
        return terminal
    except Exception as exc:
        terminal = {
            "schema_version": CONTINUATION_TERMINAL_VERSION,
            "state": "failed",
            "terminal_at": now_iso(),
            "terminal_reason": "infrastructure_or_judge_attempt_failed",
            "error_class": type(exc).__name__,
            "semantic_attempt_started": calibration.exists(),
            "calibration_passed": False,
            "selection_authorized": False,
            "production_mutated": False,
        }
        if (calibration / "terminal.json").is_file():
            terminal["calibration_terminal"] = _record(calibration / "terminal.json")
        _write_immutable_json(terminal_path, terminal)
        _write_state(state_path, status="failed", error_class=type(exc).__name__)
        return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Wait for fixture reference v2, then run one fresh calibration"
    )
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--reference-root", default=str(DEFAULT_REFERENCE_ROOT))
    parser.add_argument("--calibration-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--control-root", default=str(DEFAULT_CONTROL_ROOT))
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--poll-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_continuation(
            repo_root=Path(args.repo_root),
            reference_root=Path(args.reference_root),
            calibration_root=Path(args.calibration_root),
            control_root=Path(args.control_root),
            manifest_path=Path(args.manifest),
            poll_seconds=args.poll_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
