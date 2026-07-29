from __future__ import annotations

"""Run v23 calibration after validating the reference before validator patching."""

import argparse
import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_runner as calibration_runner
from . import app_server_judge_v5_diagnostic as diagnostic
from . import app_server_judge_v5_fresh_calibration as fresh
from . import app_server_judge_v5_fresh_calibration_v22 as v22
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
    load_reserve_capacity_policy,
)
from .app_server_judge_v5_reference_adjudication_v21 import (
    build_reserve_capacity_checkpoint_validator,
)


DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-reference-v2-capacity-v23"
).resolve()


class FreshCalibrationV23Error(v22.FreshCalibrationV22Error):
    """The v23 pre-validation or calibration binding is unsafe."""


def _policy_summary(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "phase_id": policy["phase_id"],
        "minimum_remaining_reserve_percent": policy[
            "minimum_remaining_reserve_percent"
        ],
        "maximum_total_tokens_per_turn": policy["maximum_total_tokens_per_turn"],
        "phase_total_token_bound": policy["phase_total_token_bound"],
        "projected_phase_quota_points": policy["projected_phase_quota_points"],
        "reprobe_before_every_semantic_turn": True,
        "retry_count_per_turn": 0,
        "reference_validated_before_checkpoint_validator_installation": True,
        "selection_authorized_before_calibration": False,
        "holdout_authorized": False,
    }


async def run_v23_calibration(
    *,
    policy_path: Path,
    output_dir: Path,
    reference_root: Path = v22.REFERENCE_ROOT,
) -> dict[str, Any]:
    policy_file = policy_path.expanduser().resolve()
    policy = load_reserve_capacity_policy(policy_file)
    root = output_dir.expanduser().resolve()
    if (
        policy.get("phase_id") != "fresh_judge_v5_4_calibration_v23"
        or Path(str(policy.get("phase_output_root") or "")).resolve() != root
        or Path(policy["semantic_output_root"]).resolve() != root / "fresh-attempt"
    ):
        raise FreshCalibrationV23Error("v23 calibration policy/root binding drifted")

    # This must finish before diagnostic._validate_capacity_checkpoint changes.
    frozen_reference = v22.load_frozen_fixture_reference_v21(reference_root)

    def cached_reference_loader(selected_root: Path = v22.REFERENCE_ROOT) -> dict[str, Any]:
        if selected_root.expanduser().resolve() != frozen_reference["root"]:
            raise FreshCalibrationV23Error("v23 cached reference root drifted")
        return deepcopy(frozen_reference)

    def client_factory() -> ReserveCapacityGatedCodexAppServerClient:
        return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_file)

    original_loader = fresh.load_frozen_fixture_reference
    original_fresh_writer = fresh._write_immutable_json
    original_runner_writer = calibration_runner._write_immutable_json
    original_validator = diagnostic._validate_capacity_checkpoint
    summary = _policy_summary(policy)
    validator = build_reserve_capacity_checkpoint_validator(policy_file)

    def bind_value(path: Path, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        bound = deepcopy(value)
        if path.name in {"fresh-calibration-spec.json", "calibration-spec.json"}:
            bound["semantic_capacity_policy"] = deepcopy(summary)
            bound["capacity_policy"] = v22._record(policy_file)
            bound["capacity_adapter_source"] = v22._record(Path(__file__).resolve())
            bound["reference_prevalidation"] = {
                "completed_before_validator_installation": True,
                "reference_terminal": deepcopy(
                    frozen_reference["records"]["terminal"]
                ),
                "reference_receipt": deepcopy(
                    frozen_reference["records"]["receipt"]
                ),
            }
        elif path.name == "terminal.json":
            bound["semantic_capacity_policy"] = deepcopy(summary)
            bound["capacity_policy"] = v22._record(policy_file)
            bound["reference_validated_before_checkpoint_validator_installation"] = True
        return bound

    def fresh_writer(path: Path, value: Any) -> None:
        original_fresh_writer(path, bind_value(path, value))

    def runner_writer(path: Path, value: Any) -> None:
        original_runner_writer(path, bind_value(path, value))

    fresh.load_frozen_fixture_reference = cached_reference_loader
    fresh._write_immutable_json = fresh_writer
    calibration_runner._write_immutable_json = runner_writer
    diagnostic._validate_capacity_checkpoint = validator
    try:
        return await fresh.run_fresh_reference_calibration(
            reference_root=reference_root,
            output_dir=root,
            model="gpt-5.6-sol",
            reasoning_effort="high",
            timeout_seconds=1200.0,
            client_factory=client_factory,
        )
    finally:
        diagnostic._validate_capacity_checkpoint = original_validator
        calibration_runner._write_immutable_json = original_runner_writer
        fresh._write_immutable_json = original_fresh_writer
        fresh.load_frozen_fixture_reference = original_loader


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v23 fresh reference calibration")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--reference-root", default=str(v22.REFERENCE_ROOT))
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v23_calibration(
            policy_path=Path(args.policy),
            output_dir=Path(args.output_dir),
            reference_root=Path(args.reference_root),
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "calibration_passed": terminal.get("calibration_passed", False),
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
