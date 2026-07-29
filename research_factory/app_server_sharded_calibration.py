from __future__ import annotations

"""Versioned six-case calibration shards over one frozen 66-case fixture."""

import argparse
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from .app_server_capacity import (
    CAPACITY_CHECKPOINT_VERSION,
    CapacityGatedCodexAppServerClient,
)
from .app_server_llm_judge import (
    JUDGE_CALIBRATION_VERSION,
    PARTITION_CALIBRATION_GATES,
    JudgeArtifactError,
    JudgeAttemptFailed,
    build_judge_prompt,
    build_judge_variants,
    make_v2_calibration_pool,
    run_app_server_semantic_judge,
    score_calibration_variant,
    semantic_judge_output_schema,
    validate_judge_output,
    validate_shared_witness_pool,
    write_immutable_json,
)
from .codex_app_server import CodexAppServerClient
from .util import sha256_text, write_text_atomic


SHARDED_CALIBRATION_VERSION = "pif_app_server_sharded_judge_calibration_v3"
SHARD_PLAN_VERSION = "pif_app_server_calibration_shard_plan_v3"
SHARD_FAILURE_VERSION = "pif_app_server_calibration_shard_failure_v3"
MIN_SHARD_CASES = 6
MAX_SHARD_CASES = 6
MAX_PROMPT_BYTES = 32_768
MAX_OUTPUT_SCHEMA_BYTES = 12_288
EXPECTED_FIXTURE_CASES = 66
EXPECTED_SAME_SIDE_CASES = 6
ZERO_RETRY_POLICY = "zero_retries_immutable_terminal_attempts"


class ShardedCalibrationError(JudgeArtifactError):
    """The versioned calibration envelope or checkpoint set is inconsistent."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShardedCalibrationError("%s is missing or invalid JSON" % purpose) from exc
    if not isinstance(value, dict):
        raise ShardedCalibrationError("%s is not a JSON object" % purpose)
    return value


def _write_immutable_text(path: Path, value: str) -> None:
    resolved = path.expanduser().resolve()
    if resolved.exists():
        if resolved.read_text(encoding="utf-8") != value:
            raise ShardedCalibrationError("immutable calibration text artifact changed")
        return
    write_text_atomic(resolved, value)


def balanced_shard_sizes(
    case_count: int,
    *,
    minimum: int = MIN_SHARD_CASES,
    maximum: int = MAX_SHARD_CASES,
) -> list[int]:
    if (
        isinstance(case_count, bool)
        or not isinstance(case_count, int)
        or case_count < 1
        or minimum < 1
        or maximum < minimum
    ):
        raise ValueError("invalid calibration shard size boundary")
    shard_count = (case_count + maximum - 1) // maximum
    if case_count // shard_count < minimum:
        raise ValueError("case count cannot be partitioned within the shard bounds")
    base, remainder = divmod(case_count, shard_count)
    sizes = [base + (1 if index < remainder else 0) for index in range(shard_count)]
    if sum(sizes) != case_count or any(size < minimum or size > maximum for size in sizes):
        raise AssertionError("balanced calibration shard planner violated its bounds")
    return sizes


def _subset_pool(pool: Mapping[str, Any], cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {**deepcopy(dict(pool)), "cases": deepcopy(list(cases))}


def build_calibration_shard_plan(
    *,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    expected: Mapping[str, Any],
    prompt_byte_cap: int = MAX_PROMPT_BYTES,
    output_schema_byte_cap: int = MAX_OUTPUT_SCHEMA_BYTES,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise ShardedCalibrationError("invalid full calibration pool: %s" % "; ".join(errors))
    cases = pool.get("cases")
    if not isinstance(cases, list) or len(cases) != EXPECTED_FIXTURE_CASES:
        raise ShardedCalibrationError("sharded calibration requires the frozen 66-case pool")
    full_case_ids = [str(case["case_id"]) for case in cases]
    if len(set(full_case_ids)) != len(full_case_ids):
        raise ShardedCalibrationError("calibration case IDs are not unique")
    expected_cases = expected.get("cases")
    if not isinstance(expected_cases, dict) or set(expected_cases) != set(full_case_ids):
        raise ShardedCalibrationError("calibration expected-case coverage drifted")
    mapping_cases = mapping.get("cases")
    if not isinstance(mapping_cases, list):
        raise ShardedCalibrationError("calibration private mapping is malformed")
    mapping_by_id = {str(item.get("case_id")): item for item in mapping_cases if isinstance(item, dict)}
    if set(mapping_by_id) != set(full_case_ids):
        raise ShardedCalibrationError("calibration private mapping coverage drifted")
    same_side_ids = {
        case_id
        for case_id, truth in expected_cases.items()
        if isinstance(truth, dict) and truth.get("same_side_equivalence_required") is True
    }
    if len(same_side_ids) != EXPECTED_SAME_SIDE_CASES:
        raise ShardedCalibrationError("same-side calibration partition coverage drifted")
    sizes = balanced_shard_sizes(len(cases))
    shards = []
    records = []
    cursor = 0
    all_members = []
    for index, size in enumerate(sizes):
        selected_cases = cases[cursor : cursor + size]
        cursor += size
        shard_pool = _subset_pool(pool, selected_cases)
        shard_case_ids = [str(case["case_id"]) for case in selected_cases]
        shard_mapping = {
            **deepcopy(dict(mapping)),
            "cases": [deepcopy(mapping_by_id[case_id]) for case_id in shard_case_ids],
        }
        shard_expected = {
            **deepcopy(dict(expected)),
            "cases": {case_id: deepcopy(expected_cases[case_id]) for case_id in shard_case_ids},
        }
        variants = build_judge_variants(shard_pool)
        ab_ids = [str(case["case_id"]) for case in variants["ab"]["cases"]]
        ba_ids = [str(case["case_id"]) for case in variants["ba"]["cases"]]
        if ab_ids != shard_case_ids or ba_ids != shard_case_ids:
            raise ShardedCalibrationError("AB/BA shard membership or order changed")
        envelopes = {}
        for name in ("ab", "ba"):
            prompt = build_judge_prompt(variants[name])
            schema = semantic_judge_output_schema(variants[name])
            prompt_bytes = len(prompt.encode("utf-8"))
            schema_bytes = len(_canonical_json(schema).encode("utf-8"))
            if prompt_bytes > prompt_byte_cap or schema_bytes > output_schema_byte_cap:
                raise ShardedCalibrationError(
                    "calibration shard request exceeds the frozen byte cap"
                )
            envelopes[name] = {
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "prompt_sha256": sha256_text(prompt),
                "output_schema_bytes": schema_bytes,
                "output_schema_sha256": sha256_text(_canonical_json(schema)),
            }
        shard_id = "calibration-shard-%03d" % index
        shards.append(
            {
                "shard_id": shard_id,
                "shard_index": index,
                "pool": shard_pool,
                "mapping": shard_mapping,
                "expected": shard_expected,
                "variants": variants,
                "envelopes": envelopes,
            }
        )
        records.append(
            {
                "shard_id": shard_id,
                "shard_index": index,
                "case_count": size,
                "case_ids": shard_case_ids,
                "case_ids_sha256": sha256_text(_canonical_json(shard_case_ids)),
                "same_side_case_count": len(set(shard_case_ids) & same_side_ids),
                "ab_ba_case_membership_and_order_identical": True,
                "envelopes": {
                    name: {
                        key: value
                        for key, value in envelopes[name].items()
                        if key not in {"prompt", "schema"}
                    }
                    for name in ("ab", "ba")
                },
            }
        )
        all_members.extend(shard_case_ids)
    if all_members != full_case_ids or len(set(all_members)) != len(full_case_ids):
        raise ShardedCalibrationError("calibration shard coverage is not exact and ordered")
    plan = {
        "schema_version": SHARD_PLAN_VERSION,
        "state": "frozen_before_model_calls",
        "case_count": len(full_case_ids),
        "case_ids": full_case_ids,
        "case_ids_sha256": sha256_text(_canonical_json(full_case_ids)),
        "shard_count": len(records),
        "shard_sizes": sizes,
        "minimum_shard_cases": MIN_SHARD_CASES,
        "maximum_shard_cases": MAX_SHARD_CASES,
        "prompt_byte_cap": prompt_byte_cap,
        "output_schema_byte_cap": output_schema_byte_cap,
        "same_side_case_count": len(same_side_ids),
        "exact_case_coverage_once": True,
        "ab_ba_membership_and_order_identical": True,
        "zero_retry_policy": ZERO_RETRY_POLICY,
        "shards": records,
    }
    return plan, shards


def freeze_calibration_shards(
    *, output_dir: Path, fixture_path: Optional[Path] = None
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    pool, mapping, expected = make_v2_calibration_pool(fixture_path=fixture_path)
    write_immutable_json(root / "shared-witness-pool.private.json", pool)
    write_immutable_json(root / "private-mapping.json", mapping)
    write_immutable_json(root / "expected.private.json", expected)
    plan, shards = build_calibration_shard_plan(
        pool=pool, mapping=mapping, expected=expected
    )
    for shard in shards:
        shard_root = root / "shards" / shard["shard_id"]
        judge_root = shard_root / "judge"
        write_immutable_json(shard_root / "pool.private.json", shard["pool"])
        write_immutable_json(shard_root / "private-mapping.json", shard["mapping"])
        write_immutable_json(shard_root / "expected.private.json", shard["expected"])
        for name in ("ab", "ba"):
            envelope = shard["envelopes"][name]
            _write_immutable_text(
                judge_root / ("prompt-%s.private.md" % name), envelope["prompt"]
            )
            write_immutable_json(judge_root / ("schema-%s.json" % name), envelope["schema"])
        shard_spec = next(
            item for item in plan["shards"] if item["shard_id"] == shard["shard_id"]
        )
        write_immutable_json(shard_root / "shard-spec.json", shard_spec)
        shard["root"] = shard_root
    plan["full_pool_sha256"] = _sha256_file(root / "shared-witness-pool.private.json")
    plan["private_mapping_sha256"] = _sha256_file(root / "private-mapping.json")
    plan["expected_sha256"] = _sha256_file(root / "expected.private.json")
    plan["shard_spec_sha256"] = {
        shard["shard_id"]: _sha256_file(shard["root"] / "shard-spec.json")
        for shard in shards
    }
    write_immutable_json(root / "shard-plan.json", plan)
    return plan, shards, expected


def _sum_usage(usages: Sequence[Mapping[str, Any]]) -> Optional[dict[str, int]]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    total = {field: 0 for field in fields}
    for usage in usages:
        if not isinstance(usage, Mapping):
            return None
        for field in fields:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return None
            total[field] += value
    if (
        total["cached_input_tokens"] > total["input_tokens"]
        or total["reasoning_output_tokens"] > total["output_tokens"]
        or total["total_tokens"] != total["input_tokens"] + total["output_tokens"]
    ):
        return None
    return total


def _merge_variant_outputs(
    *,
    variant_name: str,
    full_pool: Mapping[str, Any],
    shard_outputs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    full_variant = build_judge_variants(full_pool)[variant_name]
    expected_ids = [str(case["case_id"]) for case in full_variant["cases"]]
    by_id = {}
    for output in shard_outputs:
        for row in output.get("cases") or []:
            case_id = str(row.get("case_id"))
            if case_id in by_id:
                raise ShardedCalibrationError("duplicate case in merged calibration output")
            by_id[case_id] = deepcopy(row)
    if set(by_id) != set(expected_ids):
        raise ShardedCalibrationError("merged calibration output coverage is incomplete")
    merged = {"cases": [by_id[case_id] for case_id in expected_ids]}
    errors = validate_judge_output(merged, full_variant)
    if errors:
        raise ShardedCalibrationError(
            "merged calibration output is invalid: %s" % "; ".join(errors)
        )
    return merged


def _failure_report(
    *,
    plan_path: Path,
    model: str,
    reasoning_effort: str,
    failed_shard_id: Optional[str],
    failed_variant: Optional[str],
    error_class: str,
    completed_shards: Sequence[Mapping[str, Any]],
    provider_error_code: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "schema_version": SHARDED_CALIBRATION_VERSION,
        "state": "blocked",
        "calibrated": False,
        "fail_closed_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "infrastructure_or_judge_attempt_failed",
        "failed_shard_id": failed_shard_id,
        "failed_variant": failed_variant,
        "error_class": error_class,
        "failure_origin": (
            "external_provider"
            if provider_error_code == "serverOverloaded"
            else "transport_or_judge"
        ),
        "provider_error_code": provider_error_code,
        "completed_shard_count": len(completed_shards),
        "aggregate_authorized": False,
        "accounting_complete": False,
        "usage_status": "unknown",
        "usage": None,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "shard_plan_sha256": _sha256_file(plan_path),
        "retry_count": 0,
        "production_changed": False,
    }


def _validate_terminal_report(root: Path, report: Mapping[str, Any]) -> None:
    if report.get("schema_version") != SHARDED_CALIBRATION_VERSION:
        raise ShardedCalibrationError("sharded calibration terminal version changed")
    plan_path = root / "shard-plan.json"
    if (
        not plan_path.is_file()
        or report.get("shard_plan_sha256") != _sha256_file(plan_path)
    ):
        raise ShardedCalibrationError("sharded calibration plan changed after terminal state")
    state = report.get("state")
    if state == "blocked":
        if (
            report.get("calibrated") is not False
            or report.get("fail_closed_reason")
            != "infrastructure_or_judge_attempt_failed"
            or report.get("aggregate_authorized") is not False
            or report.get("accounting_complete") is not False
            or report.get("usage_status") != "unknown"
            or report.get("usage") is not None
            or report.get("retry_count") != 0
        ):
            raise ShardedCalibrationError("blocked calibration terminal contract drifted")
        failed_shard = report.get("failed_shard_id")
        if failed_shard is not None:
            failure_path = root / "shards" / str(failed_shard) / "failure.json"
            failure = _load_json(failure_path, purpose="terminal shard failure")
            if (
                failure.get("schema_version") != SHARD_FAILURE_VERSION
                or failure.get("retry_allowed") is not False
                or failure.get("aggregate_authorized") is not False
                or failure.get("usage_status") != "unknown"
            ):
                raise ShardedCalibrationError("terminal shard failure checkpoint drifted")
            if report.get("provider_error_code") != failure.get(
                "provider_error_code"
            ):
                raise ShardedCalibrationError("terminal provider failure code drifted")
        return
    if state != "completed":
        raise ShardedCalibrationError("calibration terminal state is unsupported")
    required = report.get("required_shard_count")
    shards = report.get("shards")
    if (
        isinstance(required, bool)
        or not isinstance(required, int)
        or required < 1
        or not isinstance(shards, list)
        or len(shards) != required
        or report.get("completed_shard_count") != required
        or report.get("required_turn_count") != required * 2
        or report.get("completed_turn_count") != required * 2
        or report.get("aggregate_authorized") is not True
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or report.get("retry_count") != 0
    ):
        raise ShardedCalibrationError("completed calibration terminal counts drifted")
    usages = []
    for shard in shards:
        report_path = Path(str(shard.get("report_path") or "")).expanduser().resolve()
        if not report_path.is_file() or _sha256_file(report_path) != shard.get(
            "report_sha256"
        ):
            raise ShardedCalibrationError("completed calibration shard report drifted")
        sidecars = shard.get("sidecars")
        if not isinstance(sidecars, dict) or set(sidecars) != {"ab", "ba"}:
            raise ShardedCalibrationError("completed calibration sidecar set drifted")
        for sidecar in sidecars.values():
            sidecar_path = Path(str(sidecar.get("path") or "")).expanduser().resolve()
            capacity_path = Path(
                str(sidecar.get("capacity_checkpoint_path") or "")
            ).expanduser().resolve()
            if (
                not sidecar_path.is_file()
                or _sha256_file(sidecar_path) != sidecar.get("sha256")
                or not capacity_path.is_file()
                or _sha256_file(capacity_path)
                != sidecar.get("capacity_checkpoint_sha256")
            ):
                raise ShardedCalibrationError("completed sidecar/capacity checkpoint drifted")
        usages.append(shard.get("usage"))
    if _sum_usage(usages) != report.get("usage"):
        raise ShardedCalibrationError("completed calibration usage aggregate drifted")
    for name in ("ab", "ba"):
        if not (root / ("merged-output-%s.private.json" % name)).is_file():
            raise ShardedCalibrationError("completed calibration merged output is missing")


class _BorrowedClientContext:
    def __init__(self, client: CodexAppServerClient):
        self.client = client

    async def __aenter__(self) -> CodexAppServerClient:
        return self.client

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        return False


async def run_app_server_sharded_judge_calibration(
    *,
    output_dir: Path,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    fixture_path: Optional[Path] = None,
    evaluator_spec_path: Optional[Path] = None,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    judge_runner: Callable[..., Awaitable[dict[str, Any]]] = run_app_server_semantic_judge,
) -> dict[str, Any]:
    from .windowed_evaluation import (
        DEFAULT_EVALUATOR_SPEC_PATH,
        combine_expanded_judge_calibration_scores,
        load_windowed_evaluator_spec,
    )

    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "report.json"
    if terminal_path.exists():
        prior = _load_json(terminal_path, purpose="sharded calibration terminal report")
        _validate_terminal_report(root, prior)
        return prior

    try:
        plan, shards, expected = freeze_calibration_shards(
            output_dir=root, fixture_path=fixture_path
        )
    except Exception as exc:
        # No semantic client has been opened at this point. Preserve this as an
        # infrastructure/preflight failure, never as a quality result.
        plan_path = root / "shard-plan.json"
        if not plan_path.exists():
            write_immutable_json(
                plan_path,
                {
                    "schema_version": SHARD_PLAN_VERSION,
                    "state": "preflight_failed_before_model_calls",
                    "error_class": type(exc).__name__,
                    "zero_retry_policy": ZERO_RETRY_POLICY,
                },
            )
        report = _failure_report(
            plan_path=plan_path,
            model=model,
            reasoning_effort=reasoning_effort,
            failed_shard_id=None,
            failed_variant=None,
            error_class=type(exc).__name__,
            completed_shards=[],
        )
        write_immutable_json(terminal_path, report)
        return report

    full_pool = _load_json(
        root / "shared-witness-pool.private.json", purpose="full calibration pool"
    )
    shard_results = []
    variant_outputs: dict[str, list[dict[str, Any]]] = {"ab": [], "ba": []}
    persistent_context: Optional[Any] = None
    persistent_client: Optional[Any] = None
    try:
        for shard in shards:
            shard_root = Path(shard["root"])
            judge_root = shard_root / "judge"
            failure_path = shard_root / "failure.json"
            if failure_path.exists():
                failure = _load_json(failure_path, purpose="immutable shard failure")
                report = _failure_report(
                    plan_path=root / "shard-plan.json",
                    model=model,
                    reasoning_effort=reasoning_effort,
                    failed_shard_id=shard["shard_id"],
                    failed_variant=failure.get("failed_variant"),
                    error_class=str(failure.get("error_class") or "prior_shard_failure"),
                    completed_shards=shard_results,
                )
                write_immutable_json(terminal_path, report)
                return report
            if not (judge_root / "report.json").exists() and persistent_client is None:
                persistent_context = client_factory()
                persistent_client = await persistent_context.__aenter__()
            borrowed_factory = (
                (lambda: _BorrowedClientContext(persistent_client))
                if persistent_client is not None
                else client_factory
            )
            try:
                judge_report = await judge_runner(
                    pool_path=shard_root / "pool.private.json",
                    output_dir=judge_root,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    client_factory=borrowed_factory,
                )
            except Exception as exc:
                failed_variant = exc.variant if isinstance(exc, JudgeAttemptFailed) else None
                sidecar_record = None
                provider_error_code = None
                if isinstance(exc, JudgeAttemptFailed) and exc.sidecar_path.is_file():
                    sidecar_payload = _load_json(
                        exc.sidecar_path, purpose="failed shard sidecar"
                    )
                    turn_error = sidecar_payload.get("turn_error")
                    if (
                        isinstance(turn_error, Mapping)
                        and turn_error.get("codex_error_info") == "serverOverloaded"
                    ):
                        provider_error_code = "serverOverloaded"
                    sidecar_record = {
                        "path": str(exc.sidecar_path),
                        "sha256": _sha256_file(exc.sidecar_path),
                        "size_bytes": exc.sidecar_path.stat().st_size,
                    }
                failure = {
                    "schema_version": SHARD_FAILURE_VERSION,
                    "shard_id": shard["shard_id"],
                    "failed_variant": failed_variant,
                    "error_class": type(exc).__name__,
                    "failure_origin": (
                        "external_provider"
                        if provider_error_code == "serverOverloaded"
                        else "transport_or_judge"
                    ),
                    "provider_error_code": provider_error_code,
                    "usage_status": "unknown",
                    "usage": None,
                    "retry_allowed": False,
                    "sidecar": sidecar_record,
                    "aggregate_authorized": False,
                }
                write_immutable_json(failure_path, failure)
                report = _failure_report(
                    plan_path=root / "shard-plan.json",
                    model=model,
                    reasoning_effort=reasoning_effort,
                    failed_shard_id=shard["shard_id"],
                    failed_variant=failed_variant,
                    error_class=type(exc).__name__,
                    completed_shards=shard_results,
                    provider_error_code=provider_error_code,
                )
                write_immutable_json(terminal_path, report)
                return report
            if judge_report.get("accounting_complete") is not True or not isinstance(
                judge_report.get("usage"), dict
            ):
                failure = {
                    "schema_version": SHARD_FAILURE_VERSION,
                    "shard_id": shard["shard_id"],
                    "failed_variant": None,
                    "error_class": "incomplete_or_unknown_usage",
                    "usage_status": "unknown",
                    "usage": None,
                    "retry_allowed": False,
                    "aggregate_authorized": False,
                }
                write_immutable_json(failure_path, failure)
                report = _failure_report(
                    plan_path=root / "shard-plan.json",
                    model=model,
                    reasoning_effort=reasoning_effort,
                    failed_shard_id=shard["shard_id"],
                    failed_variant=None,
                    error_class="incomplete_or_unknown_usage",
                    completed_shards=shard_results,
                )
                write_immutable_json(terminal_path, report)
                return report
            outputs = {}
            sidecars = {}
            for name in ("ab", "ba"):
                output_path = judge_root / ("output-%s.private.json" % name)
                sidecar_path = judge_root / "sidecars" / ("%s.json" % name)
                capacity_path = judge_root / "sidecars" / ("%s.capacity.json" % name)
                output = _load_json(output_path, purpose="completed shard judge output")
                sidecar = _load_json(sidecar_path, purpose="completed shard sidecar")
                capacity = _load_json(
                    capacity_path, purpose="completed shard capacity checkpoint"
                )
                if (
                    sidecar.get("state") != "completed"
                    or sidecar.get("status") != "completed"
                    or sidecar.get("usage_complete") is not True
                    or not isinstance(sidecar.get("usage"), dict)
                    or sidecar.get("recovery_reran_model") is not False
                ):
                    raise ShardedCalibrationError(
                        "completed shard sidecar does not prove zero-retry complete usage"
                    )
                if (
                    capacity.get("schema_version") != CAPACITY_CHECKPOINT_VERSION
                    or capacity.get("maximum_primary_used_percent") != 20
                    or isinstance(capacity.get("primary_used_percent"), bool)
                    or not isinstance(capacity.get("primary_used_percent"), int)
                    or capacity["primary_used_percent"] > 20
                    or capacity.get("rate_limit_reached_type") is not None
                    or capacity.get("cleared_for_semantic_turn") is not True
                    or capacity.get("managed_chatgpt_auth_verified") is not True
                    or capacity.get("thread_started") is not False
                    or capacity.get("turn_started") is not False
                    or capacity.get("sidecar_started") is not False
                    or capacity.get("retry_checkpoint_reuse_allowed") is not False
                ):
                    raise ShardedCalibrationError(
                        "completed shard lacks a valid pre-turn 20-percent capacity checkpoint"
                    )
                errors = validate_judge_output(output, shard["variants"][name])
                if errors:
                    raise ShardedCalibrationError(
                        "completed shard output is invalid: %s" % "; ".join(errors)
                    )
                outputs[name] = output
                sidecars[name] = {
                    "path": str(sidecar_path),
                    "sha256": _sha256_file(sidecar_path),
                    "capacity_checkpoint_path": str(capacity_path),
                    "capacity_checkpoint_sha256": _sha256_file(capacity_path),
                    "capacity_primary_used_percent": capacity[
                        "primary_used_percent"
                    ],
                    "usage": sidecar["usage"],
                }
                variant_outputs[name].append(output)
            shard_results.append(
                {
                    "shard_id": shard["shard_id"],
                    "case_count": len(shard["pool"]["cases"]),
                    "report_path": str(judge_root / "report.json"),
                    "report_sha256": _sha256_file(judge_root / "report.json"),
                    "sidecars": sidecars,
                    "usage": judge_report["usage"],
                    "accounting_complete": True,
                }
            )
    except Exception as exc:
        failed_shard = shards[len(shard_results)]["shard_id"] if len(shard_results) < len(shards) else None
        if failed_shard is not None:
            failure_path = root / "shards" / failed_shard / "failure.json"
            write_immutable_json(
                failure_path,
                {
                    "schema_version": SHARD_FAILURE_VERSION,
                    "shard_id": failed_shard,
                    "failed_variant": None,
                    "error_class": type(exc).__name__,
                    "usage_status": "unknown",
                    "usage": None,
                    "retry_allowed": False,
                    "aggregate_authorized": False,
                },
            )
        report = _failure_report(
            plan_path=root / "shard-plan.json",
            model=model,
            reasoning_effort=reasoning_effort,
            failed_shard_id=failed_shard,
            failed_variant=None,
            error_class=type(exc).__name__,
            completed_shards=shard_results,
        )
        write_immutable_json(terminal_path, report)
        return report
    finally:
        if persistent_context is not None:
            await persistent_context.__aexit__(None, None, None)

    if len(shard_results) != plan["shard_count"]:
        raise ShardedCalibrationError("calibration aggregation attempted before every shard")
    merged_outputs = {
        name: _merge_variant_outputs(
            variant_name=name, full_pool=full_pool, shard_outputs=variant_outputs[name]
        )
        for name in ("ab", "ba")
    }
    full_variants = build_judge_variants(full_pool)
    scores = {
        name: score_calibration_variant(
            merged_outputs[name], variant=full_variants[name], expected=expected
        )
        for name in ("ab", "ba")
    }
    for name in ("ab", "ba"):
        write_immutable_json(root / ("merged-output-%s.private.json" % name), merged_outputs[name])
    evaluator_file = Path(
        evaluator_spec_path or DEFAULT_EVALUATOR_SPEC_PATH
    ).expanduser().resolve()
    evaluator = load_windowed_evaluator_spec(evaluator_file)
    combined = combine_expanded_judge_calibration_scores(
        scores, gates=evaluator["judge_calibration"]["gates"]
    )
    partition_metrics = {
        "pairwise_f1": min(
            score["equivalence_partition_pairwise_f1"] for score in scores.values()
        ),
        "exact_case_rate": min(
            score["equivalence_partition_exact_case_rate"] for score in scores.values()
        ),
        "same_side_exact_rate": min(
            score["same_side_partition_exact_rate"] for score in scores.values()
        ),
        "same_side_case_count": min(
            score["same_side_partition_case_count"] for score in scores.values()
        ),
    }
    partition_checks = {
        "pairwise_f1": partition_metrics["pairwise_f1"]
        >= PARTITION_CALIBRATION_GATES["pairwise_f1_min"],
        "exact_case_rate": partition_metrics["exact_case_rate"]
        >= PARTITION_CALIBRATION_GATES["exact_case_rate_min"],
        "same_side_exact_rate": partition_metrics["same_side_exact_rate"]
        >= PARTITION_CALIBRATION_GATES["same_side_exact_rate_min"],
        "minimum_same_side_cases": partition_metrics["same_side_case_count"]
        >= PARTITION_CALIBRATION_GATES["minimum_same_side_cases"],
    }
    partition_calibration = {
        "passed": all(partition_checks.values()),
        "gates": PARTITION_CALIBRATION_GATES,
        "metrics": partition_metrics,
        "checks": partition_checks,
    }
    usage = _sum_usage([item["usage"] for item in shard_results])
    accounting_complete = usage is not None and len(shard_results) == plan["shard_count"]
    calibrated = bool(
        combined["passed"]
        and partition_calibration["passed"]
        and accounting_complete
        and all(score["abstention_count"] == 0 for score in scores.values())
    )
    fail_reason = None
    if not calibrated:
        if not accounting_complete:
            fail_reason = "infrastructure_or_judge_attempt_failed"
        elif any(score["abstention_count"] for score in scores.values()):
            fail_reason = "calibration_abstentions"
        else:
            fail_reason = "fixture_gates_failed"
    report = {
        "schema_version": SHARDED_CALIBRATION_VERSION,
        "source_calibration_schema_version": JUDGE_CALIBRATION_VERSION,
        "state": "completed",
        "calibrated": calibrated,
        "fail_closed_reason": fail_reason,
        "terminal_classification": (
            "passed"
            if calibrated
            else "infrastructure_or_judge_attempt_failed"
            if fail_reason == "infrastructure_or_judge_attempt_failed"
            else "judge_calibration_gate_not_passed"
        ),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "fixture_sha256": expected["fixture_sha256"],
        "fixture_case_count": len(expected["cases"]),
        "evaluator_spec_sha256": _sha256_file(evaluator_file),
        "shard_plan_sha256": _sha256_file(root / "shard-plan.json"),
        "required_shard_count": plan["shard_count"],
        "completed_shard_count": len(shard_results),
        "required_turn_count": plan["shard_count"] * 2,
        "completed_turn_count": len(shard_results) * 2,
        "aggregate_authorized": True,
        "retry_count": 0,
        "capacity_gate_maximum_primary_used_percent": 20,
        "scores": scores,
        "combined": combined,
        "equivalence_partition_calibration": partition_calibration,
        "shards": shard_results,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": usage,
        "production_changed": False,
    }
    write_immutable_json(terminal_path, report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run versioned sharded app-server calibration")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--fixture")
    parser.add_argument("--evaluator-spec")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    import asyncio

    report = asyncio.run(
        run_app_server_sharded_judge_calibration(
            output_dir=Path(args.output_dir),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
            fixture_path=Path(args.fixture) if args.fixture else None,
            evaluator_spec_path=Path(args.evaluator_spec) if args.evaluator_spec else None,
        )
    )
    print(_pretty_json(report), end="")
    return 0 if report.get("calibrated") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
