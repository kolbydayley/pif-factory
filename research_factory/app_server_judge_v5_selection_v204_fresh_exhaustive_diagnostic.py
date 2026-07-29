from __future__ import annotations

"""Run the frozen v203 high-reasoning extraction diagnostic exactly once."""

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_evaluation as app_eval
from . import codex_app_server
from . import efficient_backtest
from . import app_server_judge_v5_calibration_v25_diagnostic as v25_module
from . import app_server_judge_v5_calibration_v26_diagnostic as v26_module
from . import app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic as v86_module
from . import app_server_judge_v5_diagnostic as diagnostic_module
from . import app_server_judge_v5_selection_v203_fresh_exhaustive_design as v203
from . import paths as paths_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .paths import db_path
from .util import now_iso, stable_id


V204_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V204_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V204_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v204_spec_v1"
V204_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v204_runtime_lock_v1"
V204_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v204_launch_v1"
V204_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v204_structural_gate_v1"
V204_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v204_failure_v1"
V204_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v204_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v204_fresh_exhaustive_diagnostic"
DEFAULT_OUTPUT_ROOT = (
    v203.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v204-fresh-exhaustive-diagnostic"
).resolve()
ARM_DIRNAME = "arm"
TIMEOUT_SECONDS = 1200.0
MINIMUM_REMAINING_RESERVE_PERCENT = 20


class JudgeV5SelectionV204Error(RuntimeError):
    """The v204 attempt cannot be frozen, executed, or scored safely."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _error_receipt(exc: BaseException) -> dict[str, Any]:
    message = str(exc).encode("utf-8", errors="replace")
    return {
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
    }


def _validate_v203_authorization() -> dict[str, Any]:
    root = v203.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "fresh-exhaustive-spec.json",
        "design": root / "fresh-exhaustive-design.json",
        "manifest": root / "manifest-v3.json",
        "selection_audit": root / "fresh-development-selection-audit.json",
        "shared_reference_seed": root / "shared-reference-seed-v1.json",
        "reference_noise": root / "reference-noise-v1.json",
    }
    values = {name: _load_json(path, f"v203 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    spec = values["spec"]
    design = values["design"]
    manifest = values["manifest"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v203_fresh_exhaustive_diagnostic_frozen_semantic_attempt_authorized"
        or terminal.get("semantic_attempt_authorized") is not True
        or terminal.get("authorized_turn_count") != v203.TURN_COUNT
        or terminal.get("authorized_model") != v203.MODEL
        or terminal.get("authorized_effort") != v203.EFFORT
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("manifest") != _record(paths["manifest"])
        or terminal.get("selection_audit") != _record(paths["selection_audit"])
        or terminal.get("design") != _record(paths["design"])
        or terminal.get("spec") != _record(paths["spec"])
        or spec.get("semantic_model_calls_declared") != v203.TURN_COUNT
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("retry_count_per_turn") != 0
        or spec.get("managed_chatgpt_auth_only") is not True
        or spec.get("official_persistent_codex_app_server_only") is not True
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or design.get("model") != v203.MODEL
        or design.get("reasoning_effort") != v203.EFFORT
        or design.get("batch_size") != v203.BATCH_SIZE
        or design.get("thread_mode") != v203.THREAD_MODE
        or design.get("guideline_prompt_changed") is not False
        or design.get("prior_episode_or_text_replayed") is not False
        or design.get("cost_bound", {}).get("passed_lte_0_28") is not True
        or manifest.get("segment_count") != v203.EPISODE_COUNT * v203.SEGMENTS_PER_EPISODE
        or manifest.get("episode_count") != v203.EPISODE_COUNT
        or manifest.get("source_count") != v203.EPISODE_COUNT
        or manifest.get("unique_text_sha256_count")
        != v203.EPISODE_COUNT * v203.SEGMENTS_PER_EPISODE
        or manifest.get("density_counts") != {"dense": 12, "no_signal": 4}
        or manifest.get("event_cap") != v203.MAX_EVENTS_PER_SEGMENT
    ):
        raise JudgeV5SelectionV204Error("v203 authorization contract drifted")
    for record in [
        *spec.get("runtime_files", []),
        *spec.get("exclusion_manifests", []),
        *spec.get("predecessor", {}).values(),
        *spec.get("frozen_inputs", {}).values(),
    ]:
        if not _verify_record(record):
            raise JudgeV5SelectionV204Error("v203 frozen record drifted")
    v203._validate_v202_nonacceptance()
    return {
        "root": root,
        "paths": paths,
        **values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _ordered_batch_ids(manifest_path: Path, manifest: Mapping[str, Any]) -> list[str]:
    result = []
    for episode in manifest.get("episodes") or []:
        segments = episode.get("segments") or []
        if len(segments) != v203.SEGMENTS_PER_EPISODE:
            raise JudgeV5SelectionV204Error("v203 episode no longer maps to one batch")
        result.append(
            stable_id(
                manifest_path.resolve().as_posix(),
                v203.THREAD_MODE,
                str(v203.BATCH_SIZE),
                str(episode["episode_id"]),
                "0",
                prefix="asb_",
            )
        )
    if len(result) != v203.TURN_COUNT or len(result) != len(set(result)):
        raise JudgeV5SelectionV204Error("v204 ordered turn identities drifted")
    return result


def _build_capacity_policy(
    root: Path,
    predecessor: Mapping[str, Any],
    turn_names: Sequence[str],
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    total_bound = len(turn_names) * v203.MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        total_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V204_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v203_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": v203.MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": total_bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V204_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": v203.MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": total_bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        path.resolve()
        for path in (
            Path(__file__),
            Path(v203.__file__),
            Path(app_eval.__file__),
            Path(efficient_backtest.__file__),
            Path(capacity.__file__),
            Path(reserve.__file__),
            Path(codex_app_server.__file__),
            Path(v25_module.__file__),
            Path(v26_module.__file__),
            Path(v86_module.__file__),
            Path(diagnostic_module.__file__),
            Path(paths_module.__file__),
            Path(util_module.__file__),
            v203.DEFAULT_WINDOWED_GUIDELINES_PATH,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    capacity_paths: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V204_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v203_inputs": [
            predecessor["records"][name]
            for name in (
                "terminal",
                "spec",
                "design",
                "manifest",
                "selection_audit",
                "shared_reference_seed",
                "reference_noise",
            )
        ],
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path, predecessor=predecessor)
    return path


def verify_runtime_lock(
    path: Path,
    *,
    predecessor: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    lock = _load_json(path, "v204 runtime lock")
    expected = {str(item) for item in _expected_runtime_paths()}
    actual = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V204_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual != expected
        or not _verify_record(lock.get("pinned_codex_cli"))
        or not _verify_record(lock.get("attempt_spec"))
        or not _verify_record(lock.get("capacity_audit"))
        or not _verify_record(lock.get("capacity_policy"))
        or any(not _verify_record(record) for record in lock.get("runtime_files") or [])
        or any(not _verify_record(record) for record in lock.get("v203_inputs") or [])
    ):
        raise JudgeV5SelectionV204Error("v204 runtime lock drifted")
    checked = predecessor or _validate_v203_authorization()
    expected_inputs = [
        checked["records"][name]
        for name in (
            "terminal",
            "spec",
            "design",
            "manifest",
            "selection_audit",
            "shared_reference_seed",
            "reference_noise",
        )
    ]
    if lock.get("v203_inputs") != expected_inputs:
        raise JudgeV5SelectionV204Error("v204 predecessor lock set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def freeze_v204(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v204 terminal")}
    if (root / "launch-receipt.json").exists():
        raise JudgeV5SelectionV204Error("v204 launch already exists; replay is prohibited")
    predecessor = _validate_v203_authorization()
    manifest_path = predecessor["paths"]["manifest"]
    turn_names = _ordered_batch_ids(manifest_path, predecessor["manifest"])
    if not (root / "attempt-spec.json").exists():
        unexpected = [item for item in root.iterdir() if item.name != ".DS_Store"]
        if unexpected:
            raise JudgeV5SelectionV204Error("v204 root is nonempty without a frozen spec")
        capacity_paths = _build_capacity_policy(root, predecessor, turn_names)
        spec = {
            "schema_version": V204_SPEC_VERSION,
            "state": "frozen_before_semantic_attempt",
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "model": v203.MODEL,
            "reasoning_effort": v203.EFFORT,
            "batch_size": v203.BATCH_SIZE,
            "thread_mode": v203.THREAD_MODE,
            "concurrency": 1,
            "timeout_seconds_per_turn": timeout_seconds,
            "window_count": 4,
            "context_chars": 900,
            "max_events_per_segment": v203.MAX_EVENTS_PER_SEGMENT,
            "turn_plan": turn_names,
            "semantic_model_calls_declared": v203.TURN_COUNT,
            "retry_count_per_turn": 0,
            "persistent_app_server_process_count": 1,
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "codex_exec_semantic_calls_allowed": False,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "semantic_regex_or_keyword_pruning_allowed": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
            "capacity_policy": _record(capacity_paths["policy"]),
            "capacity_audit": _record(capacity_paths["audit"]),
            "v203": predecessor["records"],
            "guideline": _record(v203.DEFAULT_WINDOWED_GUIDELINES_PATH),
            "privacy": "private_inputs_outputs_and_mapping_sanitized_reports_only",
        }
        spec_path = root / "attempt-spec.json"
        _write_stable_time(spec_path, spec, "created_at")
        lock_path = _freeze_runtime_lock(
            root=root,
            predecessor=predecessor,
            spec_path=spec_path,
            capacity_paths=capacity_paths,
        )
    else:
        spec_path = root / "attempt-spec.json"
        spec = _load_json(spec_path, "v204 spec")
        capacity_paths = {
            "policy": root / "capacity-policy.json",
            "audit": root / "capacity-policy-audit.json",
        }
        lock_path = root / "runtime-lock.json"
        if (
            spec.get("schema_version") != V204_SPEC_VERSION
            or spec.get("turn_plan") != turn_names
            or spec.get("retry_count_per_turn") != 0
            or spec.get("holdout_authorized") is not False
            or spec.get("production_mutation_allowed") is not False
            or spec.get("capacity_policy") != _record(capacity_paths["policy"])
            or spec.get("capacity_audit") != _record(capacity_paths["audit"])
        ):
            raise JudgeV5SelectionV204Error("v204 frozen spec drifted")
        verify_runtime_lock(lock_path, predecessor=predecessor)
    return {
        "root": root,
        "predecessor": predecessor,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity_paths["policy"],
        "capacity_audit": capacity_paths["audit"],
        "runtime_lock": lock_path,
        "turn_names": turn_names,
    }


class CoreArmReserveClient:
    """Map core-arm artifact paths into the frozen reserve turn sequence."""

    def __init__(
        self,
        *,
        policy_path: Path,
        arm_root: Path,
        inner_factory: Callable[[], Any],
    ) -> None:
        self.policy_path = policy_path.expanduser().resolve()
        self.arm_root = arm_root.expanduser().resolve()
        self.client = ReserveCapacityGatedCodexAppServerClient(
            policy_path=self.policy_path,
            inner_factory=inner_factory,
        )

    async def __aenter__(self) -> "CoreArmReserveClient":
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.client.__aexit__(exc_type, exc, traceback)

    def _checkpoint(self, kwargs: Mapping[str, Any]) -> Path:
        sidecar = Path(str(kwargs.get("sidecar_path") or "")).expanduser().resolve()
        output = Path(str(kwargs.get("output_path") or "")).expanduser().resolve()
        if (
            sidecar.parent != (self.arm_root / "sidecars")
            or output.parent != (self.arm_root / "raw_outputs")
            or sidecar.stem != output.stem
            or sidecar.suffix != ".json"
            or output.suffix != ".json"
        ):
            raise JudgeV5SelectionV204Error("core-arm semantic artifact layout drifted")
        policy = self.client.policy
        turn_name = sidecar.stem
        if turn_name not in policy["ordered_turn_names"]:
            raise JudgeV5SelectionV204Error("core-arm turn is outside frozen order")
        turn_root = (
            Path(policy["semantic_output_root"])
            / "turns"
            / turn_name.replace("_", "-")
        )
        turn_root.mkdir(parents=True, exist_ok=True)
        links = {
            turn_root / "sidecar.json": sidecar,
            turn_root / "output.private.json": output,
        }
        for link, target in links.items():
            if link.exists() or link.is_symlink():
                raise JudgeV5SelectionV204Error("semantic artifact link already exists")
            link.symlink_to(target)
        return turn_root / "capacity.json"

    async def run_ephemeral_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        if "capacity_checkpoint_path" in kwargs:
            raise JudgeV5SelectionV204Error("caller supplied an unfrozen capacity checkpoint")
        kwargs["capacity_checkpoint_path"] = self._checkpoint(kwargs)
        return await self.client.run_ephemeral_structured_turn(*args, **kwargs)

    async def run_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        raise JudgeV5SelectionV204Error("v204 same-thread turns are not authorized")


def _production_cost(total_tokens: int) -> dict[str, Any]:
    scaled = total_tokens * v203.BASELINE_SEGMENT_SCOPE / (
        v203.EPISODE_COUNT * v203.SEGMENTS_PER_EPISODE
    )
    numerator = math.ceil(scaled) + v203.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    ratio = round(numerator / v203.BASELINE_END_TO_END_TOKENS, 6)
    return {
        "measured_development_extraction_tokens": total_tokens,
        "development_segment_count": v203.EPISODE_COUNT * v203.SEGMENTS_PER_EPISODE,
        "scaled_extraction_tokens_to_60_segments": math.ceil(scaled),
        "production_amortized_context_tokens": v203.PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
        "production_amortized_candidate_total_tokens": numerator,
        "baseline_end_to_end_tokens": v203.BASELINE_END_TO_END_TOKENS,
        "production_amortized_total_token_ratio": ratio,
        "passed_lte_0_28": ratio <= 0.28,
    }


def _candidate_event_counts(
    *, arm_root: Path, manifest: Mapping[str, Any]
) -> dict[str, int]:
    expected = {
        str(segment["segment_id"])
        for episode in manifest.get("episodes") or []
        for segment in episode.get("segments") or []
    }
    counts: dict[str, int] = {}
    for path in sorted((arm_root / "normalized_outputs").glob("*.json")):
        value = _load_json(path, "v204 normalized output")
        for row in value.get("segments") or []:
            segment_id = str(row.get("segment_id") or "")
            events = row.get("events")
            if segment_id in counts or not isinstance(events, list):
                raise JudgeV5SelectionV204Error("normalized segment coverage drifted")
            counts[segment_id] = len(events)
    if set(counts) != expected:
        raise JudgeV5SelectionV204Error("normalized outputs do not cover the manifest")
    return counts


def evaluate_structural_gate(
    *,
    report: Mapping[str, Any],
    manifest: Mapping[str, Any],
    arm_root: Path,
) -> dict[str, Any]:
    usage = report.get("usage")
    total_tokens = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if isinstance(total_tokens, bool) or not isinstance(total_tokens, int) or total_tokens < 0:
        total_tokens = 0
    cost = _production_cost(total_tokens)
    counts = _candidate_event_counts(arm_root=arm_root, manifest=manifest)
    dense_ratios = []
    for episode in manifest.get("episodes") or []:
        for segment in episode.get("segments") or []:
            if segment.get("density_stratum") != "dense":
                continue
            golden = int(segment.get("golden_event_count") or 0)
            if golden < 1:
                raise JudgeV5SelectionV204Error("dense reference event count drifted")
            dense_ratios.append(counts[str(segment["segment_id"])] / golden)
    if len(dense_ratios) != 12:
        raise JudgeV5SelectionV204Error("dense stratum coverage drifted")
    dense_median = round(float(statistics.median(dense_ratios)), 6)
    checks = {
        "all_16_segments_validated": report.get("validated_segments") == 16,
        "schema_status_success_rate_1": report.get("schema_status_success_rate") == 1.0,
        "all_4_calls_attempted": report.get("attempted_calls") == 4,
        "all_4_usage_measured": (
            report.get("accounting_complete") is True
            and report.get("usage_status") == "complete"
            and report.get("usage_measured_attempts") == 4
            and report.get("usage_unknown_attempts") == 0
        ),
        "normalized_exact_evidence_rate_1": report.get(
            "normalized_exact_evidence_rate"
        )
        == 1.0,
        "no_signal_candidate_positive_segments_0": report.get(
            "no_signal_candidate_positive_segments_unadjudicated"
        )
        == 0,
        "metric_grounding_error_events_0": report.get(
            "metric_grounding_error_events"
        )
        == 0,
        "dense_median_event_count_ratio_gte_0_75": dense_median >= 0.75,
        "production_amortized_total_token_ratio_lte_0_28": cost[
            "passed_lte_0_28"
        ]
        is True,
    }
    return {
        "schema_version": V204_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(name for name, passed in checks.items() if not passed),
        "dense_segment_count": len(dense_ratios),
        "dense_median_candidate_to_reference_event_count_ratio": dense_median,
        "cost": cost,
        "semantic_quality_scored": False,
        "fresh_support_alignment_judge_authorized": all(checks.values()),
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _sidecar_accounting(root: Path) -> dict[str, Any]:
    sidecars = []
    unknown = 0
    for path in sorted((root / ARM_DIRNAME / "sidecars").glob("*.json")):
        try:
            sidecar = _load_json(path, "v204 sidecar")
            sidecars.append(sidecar)
            _validate_usage(sidecar)
        except Exception:
            unknown += 1
    usage = {field: 0 for field in USAGE_FIELDS}
    measured = 0
    for sidecar in sidecars:
        try:
            item = _validate_usage(sidecar)
        except Exception:
            continue
        measured += 1
        for field in USAGE_FIELDS:
            usage[field] += item[field]
    return {
        "attempted_turn_count": len(sidecars),
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage_status": "complete" if sidecars and unknown == 0 else "partial_unknown" if measured else "unknown" if sidecars else "not_started",
        "accounting_complete": len(sidecars) == v203.TURN_COUNT and unknown == 0,
        "usage": usage,
    }


def _write_failure_terminal(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = _sidecar_accounting(root)
    error = _error_receipt(exc)
    failure = {
        "schema_version": V204_FAILURE_VERSION,
        "failed_at": now_iso(),
        **error,
        **accounting,
        "semantic_retry_allowed": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_no_private_prompt_or_output",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    known = dict(frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"])
    for field in USAGE_FIELDS:
        known[field] += int(accounting["usage"].get(field) or 0)
    unknown_turns = int(
        frozen["predecessor"]["terminal"]["cumulative_unknown_usage_turn_count"]
    ) + int(accounting["unknown_usage_turn_count"])
    unknown_upper = int(
        frozen["predecessor"]["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
    ) + int(accounting["unknown_usage_turn_count"]) * v203.MAXIMUM_TOTAL_TOKENS_PER_TURN
    terminal = {
        "schema_version": V204_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_extraction_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "usage_status": accounting["usage_status"],
        "accounting_complete": accounting["accounting_complete"],
        "usage": accounting["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown_turns,
        "cumulative_conservative_unknown_usage_upper_bound": unknown_upper,
        "failure": _record(failure_path),
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


async def run_v204(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
    inner_factory: Callable[[], Any] = _inner_factory,
    arm_runner: Callable[..., Any] = app_eval.run_app_server_core_arm,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v204 terminal")
    frozen = freeze_v204(output_dir=root, timeout_seconds=timeout_seconds)
    verify_runtime_lock(frozen["runtime_lock"], predecessor=frozen["predecessor"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV204Error("v204 launch receipt exists; semantic replay is prohibited")
    launch = {
        "schema_version": V204_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": v203.TURN_COUNT,
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(launch_path, launch, "launched_at")

    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    arm_root = root / ARM_DIRNAME

    def client_factory() -> CoreArmReserveClient:
        return CoreArmReserveClient(
            policy_path=frozen["capacity_policy"],
            arm_root=arm_root,
            inner_factory=inner_factory,
        )

    try:
        report = await arm_runner(
            conn,
            manifest_path=frozen["predecessor"]["paths"]["manifest"],
            output_dir=arm_root,
            batch_size=v203.BATCH_SIZE,
            thread_mode=v203.THREAD_MODE,
            model=v203.MODEL,
            reasoning_effort=v203.EFFORT,
            concurrency=1,
            timeout_seconds=timeout_seconds,
            window_count=4,
            context_chars=900,
            max_events_per_segment=v203.MAX_EVENTS_PER_SEGMENT,
            guideline_path=v203.DEFAULT_WINDOWED_GUIDELINES_PATH,
            client_factory=client_factory,
        )
        gate = evaluate_structural_gate(
            report=report,
            manifest=frozen["predecessor"]["manifest"],
            arm_root=arm_root,
        )
        gate_path = root / "structural-gate.json"
        _write_immutable(gate_path, gate)
        usage = dict(report["usage"])
        known = dict(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"]
        )
        for field in USAGE_FIELDS:
            known[field] += int(usage.get(field) or 0)
        passed = gate["passed"] is True
        terminal = {
            "schema_version": V204_TERMINAL_VERSION,
            "state": "completed" if passed else "failed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v204_fresh_exhaustive_structural_gate_passed_fresh_judge_authorized"
                if passed
                else "v204_extraction_structural_quality_gate_not_passed"
            ),
            "terminal_classification": "active_development_recovery_required" if passed else "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "development_winner_frozen": False,
            "fresh_judge_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": known,
            "cumulative_unknown_usage_turn_count": frozen["predecessor"]["terminal"]["cumulative_unknown_usage_turn_count"],
            "cumulative_conservative_unknown_usage_upper_bound": frozen["predecessor"]["terminal"]["cumulative_conservative_unknown_usage_upper_bound"],
            "production_amortized_total_token_ratio": gate["cost"]["production_amortized_total_token_ratio"],
            "production_amortized_token_target_passed": gate["cost"]["passed_lte_0_28"],
            "structural_gate": _record(gate_path),
            "arm_report": _record(arm_root / "report.json"),
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v205-fresh-support-alignment-score"
                / "terminal.json"
            )
            if passed
            else None,
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _write_failure_terminal(root=root, frozen=frozen, exc=exc)
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v204 fresh exhaustive diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v204(
            output_dir=Path(args.output_dir),
            database_path=Path(args.database),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "usage_status": terminal["usage_status"],
                "accounting_complete": terminal["accounting_complete"],
                "fresh_judge_authorized": terminal["fresh_judge_authorized"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
