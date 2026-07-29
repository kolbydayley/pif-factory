from __future__ import annotations

"""Run fixture-reference adjudication under the frozen v20 reserve policy."""

import argparse
import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Optional, Sequence

from . import app_server_judge_v5_reference_adjudication as reference_adjudication
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
    load_reserve_capacity_policy,
)


def _policy_summary(policy: dict) -> dict:
    return {
        "mode": "remaining_reserve_plus_projected_phase_bound",
        "phase_id": policy["phase_id"],
        "minimum_remaining_reserve_percent": policy[
            "minimum_remaining_reserve_percent"
        ],
        "quota_points_per_million_tokens": policy[
            "quota_points_per_million_tokens"
        ],
        "maximum_total_tokens_per_turn": policy[
            "maximum_total_tokens_per_turn"
        ],
        "phase_total_token_bound": policy["phase_total_token_bound"],
        "projected_phase_quota_points": policy[
            "projected_phase_quota_points"
        ],
        "reprobe_before_every_semantic_turn": True,
        "retry_count_per_turn": 0,
        "unknown_usage_hard_stop": True,
    }


def _bind_capacity_policy_to_reference_spec(value: dict, summary: dict) -> dict:
    if (
        value.get("schema_version")
        != reference_adjudication.REFERENCE_ADJUDICATION_SPEC_VERSION
        or value.get("semantic_turn_maximum_primary_used_percent") != 20
        or "semantic_capacity_policy" in value
    ):
        raise ValueError("legacy reference spec contract drifted before v20 binding")
    bound = deepcopy(value)
    bound["semantic_turn_maximum_primary_used_percent"] = None
    bound["semantic_capacity_policy"] = deepcopy(summary)
    return bound


async def run_v20_reference(
    *,
    policy_path: Path,
    output_dir: Path,
    v2_root: Path = reference_adjudication.DEFAULT_V2_ROOT,
    v3_root: Path = reference_adjudication.DEFAULT_V3_ROOT,
) -> dict:
    policy_file = policy_path.expanduser().resolve()
    policy = load_reserve_capacity_policy(policy_file)
    if output_dir.expanduser().resolve() != Path(
        policy["semantic_output_root"]
    ).expanduser().resolve():
        raise ValueError("v20 output root does not match the frozen capacity policy")

    def client_factory() -> ReserveCapacityGatedCodexAppServerClient:
        return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_file)

    policy_summary = _policy_summary(policy)
    original_writer = reference_adjudication._write_immutable_json

    def v20_writer(path: Path, value: dict) -> None:
        if path.name == "reference-adjudication-spec.json":
            value = _bind_capacity_policy_to_reference_spec(value, policy_summary)
        original_writer(path, value)

    # Keep the immutable predecessor runner byte-identical; v20 changes only its
    # spec serialization while this single serial phase is executing.
    reference_adjudication._write_immutable_json = v20_writer
    try:
        return await reference_adjudication.run_reference_adjudication(
            v2_root=v2_root,
            v3_root=v3_root,
            output_dir=output_dir,
            model="gpt-5.6-luna",
            reasoning_effort="high",
            timeout_seconds=1200.0,
            client_factory=client_factory,
        )
    finally:
        reference_adjudication._write_immutable_json = original_writer


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v20 bounded fixture reference")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--v2-root", default=str(reference_adjudication.DEFAULT_V2_ROOT))
    parser.add_argument("--v3-root", default=str(reference_adjudication.DEFAULT_V3_ROOT))
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v20_reference(
            policy_path=Path(args.policy),
            output_dir=Path(args.output_dir),
            v2_root=Path(args.v2_root),
            v3_root=Path(args.v3_root),
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_calibration_authorized": terminal.get(
                    "fresh_calibration_authorized", False
                ),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
