from __future__ import annotations

"""Wait for fresh calibration, then supervise one immutable v5 selection."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from .app_server_capacity_probe import wait_for_launch_capacity
from .app_server_judge_v5_diagnostic import _record, _write_immutable_json
from .app_server_judge_v5_selection import (
    DEFAULT_CONTINUATION_TERMINAL,
    DEFAULT_FRESH_CALIBRATION_ROOT,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_REUSE_CONTRACT,
    V5SelectionError,
    run_v5_five_arm_selection,
    verify_fresh_calibration_for_selection,
)
from .app_server_runtime_lock_v18 import (
    DEFAULT_CONTROL_ROOT,
    DEFAULT_LAUNCH_CAPACITY,
    DEFAULT_LAUNCH_RECEIPT,
    DEFAULT_MANIFEST,
    DEFAULT_WAIT_INTENT,
    build_runtime_lock_v18,
    verify_wait_intent,
    write_launch_receipt,
)
from .app_server_v5_reuse import verify_v5_reuse_contract
from .util import now_iso, write_text_atomic


CONTINUATION_TERMINAL_VERSION = (
    "pif_app_server_calibration_to_selection_continuation_terminal_v1"
)
CONTINUATION_STATE_VERSION = (
    "pif_app_server_calibration_to_selection_continuation_state_v1"
)


class SelectionContinuationStopped(RuntimeError):
    """A STOP sentinel halted v18 before another semantic turn."""


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("selection continuation artifact is not an object")
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


def _selection_semantic_attempt_started(selection_root: Path) -> bool:
    turns = selection_root / "selection/full-judge/turns"
    if not turns.is_dir():
        return False
    return any(
        (turn / filename).is_file()
        for turn in turns.iterdir()
        if turn.is_dir()
        for filename in ("capacity.json", "sidecar.json", "output.private.json")
    )


def _terminal_state_for_selection(*, winner_frozen: bool, terminal_reason: str) -> str:
    if winner_frozen:
        return "completed"
    if terminal_reason == "infrastructure_or_judge_attempt_failed":
        return "failed"
    return "stopped"


async def run_continuation(
    *,
    repo_root: Path,
    calibration_root: Path = DEFAULT_FRESH_CALIBRATION_ROOT,
    calibration_continuation_terminal: Path = DEFAULT_CONTINUATION_TERMINAL,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT,
    selection_root: Path = DEFAULT_OUTPUT_ROOT,
    control_root: Path = DEFAULT_CONTROL_ROOT,
    manifest_path: Path = DEFAULT_MANIFEST,
    poll_seconds: float = 300.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    if poll_seconds <= 0:
        raise ValueError("selection continuation poll interval must be positive")
    os.environ.pop("OPENAI_API_KEY", None)
    repo = repo_root.expanduser().resolve()
    calibration = calibration_root.expanduser().resolve()
    predecessor_terminal = calibration_continuation_terminal.expanduser().resolve()
    reuse_contract = reuse_contract_path.expanduser().resolve()
    selection = selection_root.expanduser().resolve()
    control = control_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    control.mkdir(parents=True, exist_ok=True)
    state_path = control / "state.json"
    terminal_path = control / "terminal.json"
    wait_intent_path = control / DEFAULT_WAIT_INTENT.name
    capacity_path = control / DEFAULT_LAUNCH_CAPACITY.name
    launch_receipt_path = control / DEFAULT_LAUNCH_RECEIPT.name
    stop_paths = (
        repo / "work/app-server-development-v2/unattended-pipeline-v5/STOP",
        calibration / "STOP",
        selection / "STOP",
        control / "STOP",
    )
    if terminal_path.exists():
        return _load(terminal_path)
    verify_wait_intent(
        repo_root=repo,
        path=wait_intent_path,
        expected_selection_root=selection,
        expected_calibration_terminal=predecessor_terminal,
    )
    try:
        while not predecessor_terminal.is_file():
            if _stop_present(*stop_paths):
                raise SelectionContinuationStopped("STOP before fresh calibration terminal")
            _write_state(
                state_path,
                status="waiting_for_fresh_calibration_selection_authorization",
                semantic_attempt_started=False,
                fresh_calibration_terminal_present=False,
            )
            await sleep(poll_seconds)
        if _stop_present(*stop_paths):
            raise SelectionContinuationStopped("STOP after fresh calibration terminal")
        _write_state(
            state_path,
            status="verifying_fresh_calibration_and_five_arm_provenance",
            semantic_attempt_started=False,
            fresh_calibration_terminal_present=True,
        )
        try:
            fresh = verify_fresh_calibration_for_selection(
                calibration_root=calibration,
                continuation_terminal_path=predecessor_terminal,
            )
            contract = verify_v5_reuse_contract(reuse_contract)
        except (V5SelectionError, ValueError) as exc:
            terminal = {
                "schema_version": CONTINUATION_TERMINAL_VERSION,
                "state": "stopped",
                "terminal_at": now_iso(),
                "terminal_reason": (
                    "fresh_calibration_or_five_arm_provenance_not_admissible_"
                    "selection_not_started"
                ),
                "error_class": type(exc).__name__,
                "fresh_calibration_terminal": _record(predecessor_terminal),
                "semantic_attempt_started": False,
                "winner_frozen": False,
                "holdout_preparation_authorized": False,
                "production_mutated": False,
            }
            _write_immutable_json(terminal_path, terminal)
            _write_state(state_path, status="stopped_preflight_not_admissible")
            return terminal
        verify_wait_intent(
            repo_root=repo,
            path=wait_intent_path,
            expected_selection_root=selection,
            expected_calibration_terminal=predecessor_terminal,
        )
        _write_state(
            state_path,
            status="waiting_for_five_arm_selection_capacity",
            semantic_attempt_started=False,
            fresh_calibration_passed=True,
            five_clean_arms_verified=len(contract["clean_arms"]),
        )

        async def capacity_sleep(seconds: float) -> None:
            if _stop_present(*stop_paths):
                raise SelectionContinuationStopped("STOP while waiting for selection capacity")
            _write_state(
                state_path,
                status="waiting_for_five_arm_selection_capacity",
                semantic_attempt_started=False,
                fresh_calibration_passed=True,
                five_clean_arms_verified=len(contract["clean_arms"]),
            )
            await sleep(seconds)

        await wait_for_launch_capacity(
            output_path=capacity_path,
            sleep=capacity_sleep,
            recheck_seconds=poll_seconds,
        )
        if _stop_present(*stop_paths):
            raise SelectionContinuationStopped("STOP after selection capacity clearance")
        lock = build_runtime_lock_v18(
            repo_root=repo,
            manifest_path=manifest,
            wait_intent_path=wait_intent_path,
            launch_capacity_path=capacity_path,
            calibration_root=calibration,
            continuation_terminal_path=predecessor_terminal,
            reuse_contract_path=reuse_contract,
            selection_root=selection,
        )
        write_launch_receipt(
            output_path=launch_receipt_path,
            manifest_path=manifest,
            capacity_path=capacity_path,
            supervisor_pid=os.getpid(),
        )
        _write_state(
            state_path,
            status="running_reference_bound_five_arm_selection",
            semantic_attempt_started=False,
            fresh_calibration_passed=True,
            runtime_lock_schema_version=lock["schema_version"],
        )
        selection_terminal = await run_v5_five_arm_selection(
            repo_root=repo,
            reuse_contract_path=reuse_contract,
            fresh_calibration_root=calibration,
            continuation_terminal_path=predecessor_terminal,
            output_dir=selection,
        )
        winner_frozen = selection_terminal.get("winner_frozen") is True
        reason = str(
            selection_terminal.get("terminal_reason")
            or "infrastructure_or_judge_attempt_failed"
        )
        semantic_started = _selection_semantic_attempt_started(selection)
        terminal = {
            "schema_version": CONTINUATION_TERMINAL_VERSION,
            "state": _terminal_state_for_selection(
                winner_frozen=winner_frozen, terminal_reason=reason
            ),
            "terminal_at": now_iso(),
            "terminal_reason": (
                "five_arm_selection_passed_winner_frozen"
                if winner_frozen
                else reason
            ),
            "fresh_calibration_terminal": fresh["records"]["outer_terminal"],
            "runtime_lock": _record(manifest),
            "launch_receipt": _record(launch_receipt_path),
            "selection_terminal": _record(selection / "v5-selection-terminal.json"),
            "semantic_attempt_started": semantic_started,
            "winner_frozen": winner_frozen,
            "holdout_preparation_authorized": winner_frozen,
            "holdout_model_calls_authorized": False,
            "prospective_gate_authorized": winner_frozen,
            "production_mutated": False,
        }
        judge_report = selection / "selection/full-judge/report.json"
        if judge_report.is_file():
            terminal["selection_judge_report"] = _record(judge_report)
        score_report = selection / "selection/score-report.json"
        if score_report.is_file():
            terminal["selection_score_report"] = _record(score_report)
        _write_immutable_json(terminal_path, terminal)
        _write_state(
            state_path,
            status=("winner_frozen" if winner_frozen else terminal["state"]),
            semantic_attempt_started=semantic_started,
            winner_frozen=winner_frozen,
            holdout_preparation_authorized=winner_frozen,
        )
        return terminal
    except SelectionContinuationStopped as exc:
        terminal = {
            "schema_version": CONTINUATION_TERMINAL_VERSION,
            "state": "stopped",
            "terminal_at": now_iso(),
            "terminal_reason": "operator_stop_before_next_semantic_turn",
            "error_class": type(exc).__name__,
            "semantic_attempt_started": _selection_semantic_attempt_started(selection),
            "winner_frozen": False,
            "holdout_preparation_authorized": False,
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
            "semantic_attempt_started": _selection_semantic_attempt_started(selection),
            "winner_frozen": False,
            "holdout_preparation_authorized": False,
            "production_mutated": False,
        }
        selection_terminal_path = selection / "v5-selection-terminal.json"
        if selection_terminal_path.is_file():
            terminal["selection_terminal"] = _record(selection_terminal_path)
        _write_immutable_json(terminal_path, terminal)
        _write_state(state_path, status="failed", error_class=type(exc).__name__)
        return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Wait for fresh v5 calibration, then run five-arm selection"
    )
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--calibration-root", default=str(DEFAULT_FRESH_CALIBRATION_ROOT))
    parser.add_argument(
        "--calibration-continuation-terminal",
        default=str(DEFAULT_CONTINUATION_TERMINAL),
    )
    parser.add_argument("--reuse-contract", default=str(DEFAULT_REUSE_CONTRACT))
    parser.add_argument("--selection-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--control-root", default=str(DEFAULT_CONTROL_ROOT))
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--poll-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_continuation(
            repo_root=Path(args.repo_root),
            calibration_root=Path(args.calibration_root),
            calibration_continuation_terminal=Path(
                args.calibration_continuation_terminal
            ),
            reuse_contract_path=Path(args.reuse_contract),
            selection_root=Path(args.selection_root),
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
                "winner_frozen": terminal.get("winner_frozen", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("winner_frozen") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
