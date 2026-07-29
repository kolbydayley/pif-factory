from __future__ import annotations

"""Run fixture-reference adjudication with reserve-aware checkpoint validation."""

import argparse
import asyncio
import hashlib
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_diagnostic as diagnostic
from . import app_server_judge_v5_reference_adjudication as reference_adjudication
from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
    ReserveCapacityGatedCodexAppServerClient,
    load_reserve_capacity_policy,
)
from .app_server_judge_v5_reference_adjudication_v20 import (
    _bind_capacity_policy_to_reference_spec,
    _policy_summary,
)


ADOPTED_TURN_NAME = "reference_pointwise_shard_00"
ADOPTION_RECEIPT_VERSION = "pif_app_server_completed_turn_adoption_v21"
DEFAULT_V20_POLICY = Path(
    "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()
DEFAULT_V20_TURN_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
    "reference-pointwise-shard-00"
).resolve()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve()
    if not target.is_file():
        raise diagnostic.JudgeV5DiagnosticError(
            "completed predecessor artifact is missing"
        )
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _verify_record(record: Any, expected_path: Path) -> None:
    target = expected_path.expanduser().resolve()
    if (
        not isinstance(record, Mapping)
        or Path(str(record.get("path") or "")).expanduser().resolve() != target
        or not target.is_file()
        or record.get("sha256") != _sha256_file(target)
        or record.get("size_bytes") != target.stat().st_size
    ):
        raise diagnostic.JudgeV5DiagnosticError(
            "completed predecessor artifact record drifted"
        )


def _source_turn_paths() -> dict[str, Path]:
    return {
        "root": DEFAULT_V20_TURN_ROOT,
        "input": DEFAULT_V20_TURN_ROOT / "input.private.json",
        "prompt": DEFAULT_V20_TURN_ROOT / "prompt.private.md",
        "schema": DEFAULT_V20_TURN_ROOT / "schema.json",
        "capacity": DEFAULT_V20_TURN_ROOT / "capacity.json",
        "sidecar": DEFAULT_V20_TURN_ROOT / "sidecar.json",
        "output": DEFAULT_V20_TURN_ROOT / "output.private.json",
    }


def _verify_adoption_binding(policy: Mapping[str, Any]) -> Mapping[str, Any]:
    recovery = policy.get("recovery")
    adoption = recovery.get("adopted_completed_turn") if isinstance(recovery, Mapping) else None
    if (
        not isinstance(adoption, Mapping)
        or adoption.get("turn_name") != ADOPTED_TURN_NAME
        or adoption.get("source_phase_id") != "fixture_reference_adjudication_v20"
        or adoption.get("semantic_output_validated") is not True
        or adoption.get("capacity_checkpoint_validated") is not True
        or adoption.get("adoption_is_not_retry") is not True
        or ADOPTED_TURN_NAME in policy["ordered_turn_names"]
        or len(policy["ordered_turn_names"]) != 11
    ):
        raise diagnostic.JudgeV5DiagnosticError(
            "completed predecessor adoption contract drifted"
        )
    source_records = adoption.get("source_artifacts")
    if not isinstance(source_records, Mapping):
        raise diagnostic.JudgeV5DiagnosticError(
            "completed predecessor source records are missing"
        )
    for kind, path in _source_turn_paths().items():
        if kind == "root":
            continue
        _verify_record(source_records.get(kind), path)
    return adoption


def _validate_and_load_adopted_turn(
    *,
    current_paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    reasoning_effort: str,
    output_validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_paths = _source_turn_paths()
    for kind in ("input", "prompt", "schema"):
        current = current_paths[kind].expanduser().resolve()
        source = source_paths[kind]
        if (
            not current.is_file()
            or current.stat().st_size != source.stat().st_size
            or _sha256_file(current) != _sha256_file(source)
        ):
            raise diagnostic.JudgeV5DiagnosticError(
                "v21 frozen request differs from adopted predecessor request"
            )
    source_validator = build_reserve_capacity_checkpoint_validator(
        DEFAULT_V20_POLICY
    )
    active_validator = diagnostic._validate_capacity_checkpoint
    diagnostic._validate_capacity_checkpoint = source_validator
    try:
        return diagnostic._validate_completed_turn(
            paths=source_paths,
            prompt=prompt,
            schema=schema,
            base_instructions=base_instructions,
            model=model,
            reasoning_effort=reasoning_effort,
            output_validator=output_validator,
        )
    finally:
        diagnostic._validate_capacity_checkpoint = active_validator


def build_reserve_capacity_checkpoint_validator(
    policy_path: Path,
) -> Callable[[Path], dict[str, Any]]:
    policy_file = policy_path.expanduser().resolve()
    policy = load_reserve_capacity_policy(policy_file)
    policy_sha256 = _sha256_file(policy_file)
    turn_names = list(policy["ordered_turn_names"])
    maximum_tokens = int(policy["maximum_total_tokens_per_turn"])
    quota_rate = int(policy["quota_points_per_million_tokens"])
    reserve = int(policy["minimum_remaining_reserve_percent"])

    def validate(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise diagnostic.JudgeV5DiagnosticError(
                "reserve capacity checkpoint is missing or invalid"
            ) from exc
        if not isinstance(value, dict):
            raise diagnostic.JudgeV5DiagnosticError(
                "reserve capacity checkpoint is not an object"
            )
        used = value.get("primary_used_percent")
        remaining = value.get("primary_remaining_percent")
        remaining_turn_count = value.get("remaining_turn_count")
        projected_tokens = value.get("projected_remaining_tokens")
        projected_points = value.get("projected_remaining_quota_points")
        terminal_remaining = value.get("projected_terminal_remaining_percent")
        turn_ordinal = value.get("turn_ordinal")
        expected_points = None
        if isinstance(projected_tokens, int) and not isinstance(projected_tokens, bool):
            expected_points = math.ceil(projected_tokens * quota_rate / 1_000_000)
        valid_turn = (
            isinstance(turn_ordinal, int)
            and not isinstance(turn_ordinal, bool)
            and 0 <= turn_ordinal < len(turn_names)
            and value.get("turn_name") == turn_names[turn_ordinal]
        )
        if (
            value.get("schema_version") != RESERVE_CAPACITY_CHECKPOINT_VERSION
            or value.get("policy_path") != str(policy_file)
            or value.get("policy_sha256") != policy_sha256
            or value.get("phase_id") != policy["phase_id"]
            or value.get("managed_chatgpt_auth_verified") is not True
            or value.get("plan_type") != "pro"
            or value.get("rate_limit_reached_type") is not None
            or isinstance(used, bool)
            or not isinstance(used, int)
            or not 0 <= used <= 100
            or remaining != 100 - used
            or value.get("minimum_remaining_reserve_percent") != reserve
            or isinstance(remaining_turn_count, bool)
            or not isinstance(remaining_turn_count, int)
            or not 1 <= remaining_turn_count <= len(turn_names)
            or isinstance(projected_tokens, bool)
            or not isinstance(projected_tokens, int)
            or projected_tokens != remaining_turn_count * maximum_tokens
            or isinstance(projected_points, bool)
            or not isinstance(projected_points, int)
            or projected_points != expected_points
            or isinstance(terminal_remaining, bool)
            or not isinstance(terminal_remaining, int)
            or terminal_remaining != remaining - projected_points
            or terminal_remaining < reserve
            or value.get("cleared_for_semantic_turn") is not True
            or value.get("thread_started") is not False
            or value.get("turn_started") is not False
            or value.get("sidecar_started") is not False
            or value.get("retry_checkpoint_reuse_allowed") is not False
            or not valid_turn
        ):
            raise diagnostic.JudgeV5DiagnosticError(
                "reserve capacity checkpoint contract is invalid"
            )
        return value

    return validate


async def run_v21_reference(
    *,
    policy_path: Path,
    output_dir: Path,
    v2_root: Path = reference_adjudication.DEFAULT_V2_ROOT,
    v3_root: Path = reference_adjudication.DEFAULT_V3_ROOT,
) -> dict[str, Any]:
    policy_file = policy_path.expanduser().resolve()
    policy = load_reserve_capacity_policy(policy_file)
    adoption = _verify_adoption_binding(policy)
    if output_dir.expanduser().resolve() != Path(
        policy["semantic_output_root"]
    ).expanduser().resolve():
        raise ValueError("v21 output root does not match the frozen capacity policy")

    def client_factory() -> ReserveCapacityGatedCodexAppServerClient:
        return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_file)

    policy_summary = _policy_summary(policy)
    original_writer = reference_adjudication._write_immutable_json
    original_get_or_run_turn = reference_adjudication._get_or_run_turn
    original_checkpoint_validator = diagnostic._validate_capacity_checkpoint
    reserve_checkpoint_validator = build_reserve_capacity_checkpoint_validator(
        policy_file
    )
    adoption_receipt_path = output_dir.expanduser().resolve() / "completed-turn-adoption.json"

    def write_adoption_receipt() -> dict[str, Any]:
        turn_root = output_dir.expanduser().resolve() / "turns" / ADOPTED_TURN_NAME.replace(
            "_", "-"
        )
        current_paths = {
            "input": turn_root / "input.private.json",
            "prompt": turn_root / "prompt.private.md",
            "schema": turn_root / "schema.json",
        }
        source_paths = _source_turn_paths()
        for kind in ("input", "prompt", "schema"):
            if (
                not current_paths[kind].is_file()
                or current_paths[kind].stat().st_size != source_paths[kind].stat().st_size
                or _sha256_file(current_paths[kind]) != _sha256_file(source_paths[kind])
            ):
                raise diagnostic.JudgeV5DiagnosticError(
                    "v21 request identity check failed before predecessor adoption"
                )
        receipt = {
            "schema_version": ADOPTION_RECEIPT_VERSION,
            "turn_name": ADOPTED_TURN_NAME,
            "source_phase_id": adoption["source_phase_id"],
            "source_artifacts": deepcopy(adoption["source_artifacts"]),
            "v21_request_artifacts": {
                kind: _record(path) for kind, path in current_paths.items()
            },
            "request_identity_verified": True,
            "semantic_output_validated": True,
            "capacity_checkpoint_validated": True,
            "usage": deepcopy(adoption["usage"]),
            "semantic_call_performed_by_v21": False,
            "adoption_is_not_retry": True,
            "production_mutated": False,
        }
        original_writer(adoption_receipt_path, receipt)
        return receipt

    def v21_writer(path: Path, value: dict[str, Any]) -> None:
        if path.name == "reference-adjudication-spec.json":
            write_adoption_receipt()
            value = _bind_capacity_policy_to_reference_spec(value, policy_summary)
            value["completed_checkpoint_adoption"] = _record(
                adoption_receipt_path
            )
            value["new_semantic_turn_count"] = len(policy["ordered_turn_names"])
        elif path.name in {"fixture-reference-v2-receipt.json", "terminal.json"}:
            value = deepcopy(value)
            value["completed_checkpoint_adoptions"] = [
                _record(adoption_receipt_path)
            ]
            value["new_semantic_turn_count"] = len(policy["ordered_turn_names"])
        original_writer(path, value)

    async def v21_get_or_run_turn(**kwargs: Any) -> tuple[dict[str, Any], dict[str, Any], bool]:
        if kwargs.get("turn_name") != ADOPTED_TURN_NAME:
            return await original_get_or_run_turn(**kwargs)
        if not adoption_receipt_path.is_file():
            raise diagnostic.JudgeV5DiagnosticAttemptFailed(
                turn_name=ADOPTED_TURN_NAME,
                error_class="missing_completed_turn_adoption_receipt",
            )
        try:
            output, sidecar = _validate_and_load_adopted_turn(
                current_paths=kwargs["paths"],
                prompt=kwargs["prompt"],
                schema=kwargs["schema"],
                base_instructions=kwargs["base_instructions"],
                model=kwargs["model"],
                reasoning_effort=kwargs["reasoning_effort"],
                output_validator=kwargs["output_validator"],
            )
        except Exception as exc:
            raise diagnostic.JudgeV5DiagnosticAttemptFailed(
                turn_name=ADOPTED_TURN_NAME,
                error_class=type(exc).__name__,
            ) from exc
        return output, sidecar, True

    reference_adjudication._write_immutable_json = v21_writer
    reference_adjudication._get_or_run_turn = v21_get_or_run_turn
    diagnostic._validate_capacity_checkpoint = reserve_checkpoint_validator
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
        diagnostic._validate_capacity_checkpoint = original_checkpoint_validator
        reference_adjudication._get_or_run_turn = original_get_or_run_turn
        reference_adjudication._write_immutable_json = original_writer


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v21 bounded fixture reference")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--v2-root", default=str(reference_adjudication.DEFAULT_V2_ROOT))
    parser.add_argument("--v3-root", default=str(reference_adjudication.DEFAULT_V3_ROOT))
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v21_reference(
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
