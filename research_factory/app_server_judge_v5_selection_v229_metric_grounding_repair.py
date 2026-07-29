from __future__ import annotations

"""Repair one unsupported metric field after the v228 exact-span pass."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v228_exact_span_repair as v228
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v229_metric_repair_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v229_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v229_terminal_v1"
PHASE_ID = "development_selection_v5_4_v229_metric_grounding_repair"
TURN_NAME = "v229_metric_grounding_repair"
MODEL = "gpt-5.6-luna"
EFFORT = "low"
MAX_NEW_TOKENS = 25_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
TIMEOUT_SECONDS = 1200.0
PINNED_CODEX_0_144_1 = v228.PINNED_CODEX_0_144_1
USAGE_FIELDS = v228.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = v228.PIPELINE_ROOT
V228_ROOT = v228.DEFAULT_OUTPUT_ROOT
V228_TURN_ROOT = V228_ROOT / "turns/v228-exact-span-repair"
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v229-metric-grounding-repair"
).resolve()
PRIOR_DEVELOPMENT_DIAGNOSTIC_TOKENS = 91_186
EXPECTED_V228_HASHES = {
    "attempt_spec": "947f855cc4e7a0bcafa22319ed06bfe51a7e98e1f1d1b8e9c0a8181058cdedf9",
    "runtime_lock": "99220c5f803af471bc32018ea7fb7b6f060cdf217c4fdec3070f9725b5615fc3",
    "terminal": "19e07e41c4627eb4767b91c889bca673f1d62bc51ff9b05d164eba86d8d36454",
    "output": "32d7ffc9e2794d6bff08b62f485c265519c03fdc066eedf90d42180ff74ca240",
    "normalized": "7a9e5bbcdcaa22d790719309c552c2302651b0185d60c33c986b21c8a87e3631",
    "gate": "44709b90bf6acb69eb54add3b2015d1808443715c98123ff77accf025c9684a5",
    "sidecar": "4aeefebe4219d559d59a74553e42a8c71236adcc0bedcb197bb21c2a8243b298",
}
METRIC_FIELDS = (
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
)


class V229MetricRepairError(RuntimeError):
    """The v229 metric repair cannot be frozen or executed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V229MetricRepairError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, indent=2
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V229MetricRepairError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V229MetricRepairError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    data = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _v228_paths() -> dict[str, Path]:
    return {
        "attempt_spec": V228_ROOT / "attempt-spec.json",
        "runtime_lock": V228_ROOT / "runtime-lock.json",
        "terminal": V228_ROOT / "terminal.json",
        "output": V228_TURN_ROOT / "output.private.json",
        "normalized": V228_TURN_ROOT / "normalized.private.json",
        "gate": V228_TURN_ROOT / "exact-span-gate.json",
        "sidecar": V228_TURN_ROOT / "sidecar.json",
    }


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
        "gate": turn_root / "metric-repair-gate.json",
    }


def _schema(repair_id: str) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["repair_id", "action"],
        "properties": {
            "repair_id": {"type": "string", "enum": [repair_id]},
            "action": {
                "type": "string",
                "enum": [
                    "keep",
                    "clear_metric_unit",
                    "clear_all_metric_fields",
                    "drop_event",
                ],
            },
        },
    }


def _base_instructions() -> str:
    return """You repair metric grounding for one frozen structured event.
The event evidence is an exact source span. Decide only whether the existing
metric fields are literally and semantically supported by that evidence.
Choose keep only when every nonempty metric field is supported. Choose
clear_metric_unit when the value/comparator/raw text remain valid but the unit
is unsupported. Choose clear_all_metric_fields when the event remains valid
but its metric tuple does not. Choose drop_event when removing metric fields
would materially change the event. Do not rewrite evidence or any other field.
Return only the schema-valid JSON object. Do not use tools."""


def _event_for_prompt(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in event.items()
        if key not in {"confidence", "window_id"}
        and value not in (None, "", [], {})
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _v228_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_V228_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V229MetricRepairError(f"v228 {name} drifted")
    v228.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v228 terminal")
    gate = _load_json(paths["gate"], "v228 gate")
    sidecar = _load_json(paths["sidecar"], "v228 sidecar")
    normalized = _load_json(paths["normalized"], "v228 normalized")
    if (
        terminal.get("terminal_reason")
        != "v228_exact_span_quality_proxy_not_passed"
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or gate.get("failed_checks") != ["metric_grounding_error_events_0"]
        or gate.get("checks", {}).get("all_exactness_pruned_events_0")
        is not True
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
    ):
        raise V229MetricRepairError("v228 metric-only failure contract drifted")
    repair_rows = normalized["normalized"]["repair"]["normalized"]["segments"]
    if len(repair_rows) != 1 or len(repair_rows[0].get("events") or []) != 1:
        raise V229MetricRepairError("v228 repair event is unavailable")
    event = repair_rows[0]["events"][0]
    errors = v228.predecessor._metric_grounding_errors(event)
    if errors != ["metric_unit_not_in_evidence"]:
        raise V229MetricRepairError("v228 metric diagnostic drifted")
    return {
        "records": records,
        "terminal": terminal,
        "gate": gate,
        "sidecar": sidecar,
        "normalized": normalized,
        "event": event,
    }


def _project_action(
    output: Any, frozen: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(output, Mapping):
        raise V229MetricRepairError("metric repair output is not an object")
    try:
        _validate_schema(frozen["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V229MetricRepairError("metric repair schema failed") from exc
    if output.get("repair_id") != frozen["request"]["repair_id"]:
        raise V229MetricRepairError("metric repair id drifted")
    action = str(output["action"])
    event = copy.deepcopy(frozen["event"])
    if action == "clear_metric_unit":
        event["metric_unit"] = ""
    elif action == "clear_all_metric_fields":
        for field in METRIC_FIELDS:
            event[field] = ""
    elif action == "drop_event":
        event = None
    elif action != "keep":
        raise V229MetricRepairError("metric repair action is invalid")
    return {"action": action, "event": event}


def _normalize_repair(
    projection: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    packet = frozen["predecessor_frozen"]["repair_packet"]
    source = frozen["predecessor_frozen"]["repair_source"]
    events = [] if projection["event"] is None else [projection["event"]]
    payload = {
        "episode_id": "repair_episode",
        "segments": [
            {
                "segment_id": packet["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": packet["segment_source_context"],
                "no_signal_reason": (
                    "" if events else "The LLM explicitly dropped the event."
                ),
                "events": events,
            }
        ],
    }
    return v228.predecessor._normalize_gap_output(
        payload,
        {
            "episode_id": "repair_episode",
            "private_input": {"normalization_segments": [source]},
        },
    )


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    try:
        return v228.predecessor._validate_usage(sidecar)
    except Exception as exc:
        raise V229MetricRepairError("usage is incomplete") from exc


def _gate(
    *,
    usage: Mapping[str, int],
    projection: Mapping[str, Any],
    repair_diagnostics: Sequence[Mapping[str, Any]],
    second_audit: Mapping[str, Any],
) -> dict[str, Any]:
    second = second_audit["sanitized"]
    production = v228._production_projection()
    checks = {
        "llm_metric_action_declared": projection["action"]
        in {
            "keep",
            "clear_metric_unit",
            "clear_all_metric_fields",
            "drop_event",
        },
        "all_exactness_pruned_events_0": (
            sum(
                int(row["exactness_pruned_events"])
                for row in repair_diagnostics
            )
            + int(second["exactness_pruned_events"])
            == 0
        ),
        "metric_grounding_error_events_0": (
            sum(
                int(row["metric_grounding_error_events"])
                for row in repair_diagnostics
            )
            + int(second["metric_grounding_error_events"])
            == 0
        ),
        "second_episode_schema_status_success": second[
            "schema_status_success"
        ],
        "second_dense_added_grounded_event": second[
            "dense_new_event_count"
        ]
        >= 1,
        "event_cap_violations_0": (
            sum(
                1
                for row in repair_diagnostics
                if row["combined_event_cap_exceeded"]
            )
            + int(second["event_cap_violations"])
            == 0
        ),
        "exact_existing_duplicate_events_0": (
            sum(
                int(row["exact_existing_duplicate_events"])
                for row in repair_diagnostics
            )
            + int(second["exact_existing_duplicate_events"])
            == 0
        ),
        "new_turn_usage_lte_25000": int(usage["total_tokens"])
        <= MAX_NEW_TOKENS,
        "production_amortized_ratio_lte_0_28": production[
            "production_amortized_total_token_ratio"
        ]
        <= 0.28,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(
            name for name, passed in checks.items() if not passed
        ),
        "metric_repair_action": projection["action"],
        "repair_retained_event_count": sum(
            int(row["new_event_count"]) for row in repair_diagnostics
        ),
        "second_episode": second,
        "new_turn_usage": dict(usage),
        "cumulative_development_diagnostic_tokens": (
            PRIOR_DEVELOPMENT_DIAGNOSTIC_TOKENS
            + int(usage["total_tokens"])
        ),
        **production,
        "support_alignment_quality_measured": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _capacity_policy(root: Path, lineage: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    points = math.ceil(
        MAX_NEW_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v228_terminal": lineage["records"]["terminal"],
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
            "phase_total_token_bound": MAX_NEW_TOKENS,
            "projected_phase_quota_points": points,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
        "phase_total_token_bound": MAX_NEW_TOKENS,
        "projected_phase_quota_points": points,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                *v228._runtime_files(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v229 runtime lock")
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or actual_runtime != expected_runtime
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V229MetricRepairError("runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V229MetricRepairError("runtime lock record drifted")
    _validate_lineage()
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    predecessor_frozen = v228._load_frozen(V228_ROOT)
    lineage = _validate_lineage()
    request = _load_json(paths["input"], "v229 input")
    return {
        "root": root,
        "paths": paths,
        "spec": _load_json(root / "attempt-spec.json", "v229 spec"),
        "spec_path": root / "attempt-spec.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "request": request,
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "base": paths["base"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], "v229 schema"),
        "event": lineage["event"],
        "predecessor_frozen": predecessor_frozen,
    }


def freeze_v229(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V229MetricRepairError("unfinished v229 root is not replayable")
        if (root / "launch-receipt.json").exists():
            raise V229MetricRepairError("v229 launch exists; replay prohibited")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    repair_id = str(
        v228._load_frozen(V228_ROOT)["request"]["repair_id"]
    )
    request = {
        "repair_id": repair_id,
        "event": _event_for_prompt(lineage["event"]),
    }
    prompt = "Repair this metric tuple without changing any other field.\n" + _canonical_json(request)
    base = _base_instructions()
    schema = _schema(repair_id)
    paths = _turn_paths(root)
    _write_immutable(paths["input"], request)
    _write_private_text(paths["prompt"], prompt)
    _write_private_text(paths["base"], base)
    _write_immutable(paths["schema"], schema)
    capacity_paths = _capacity_policy(root, lineage)
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_single_semantic_turn",
        "declared_turn_count": 1,
        "retry_count": 0,
        "model": MODEL,
        "effort": EFFORT,
        "maximum_new_turn_tokens": MAX_NEW_TOKENS,
        "semantic_delta": "repair only the unsupported metric tuple",
        "v228_replayed": False,
        "development_diagnostic_only": True,
        "production_protocol_additional_turns": 0,
        "support_alignment_allowed_after_pass": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "direct_lineage": lineage["records"],
        "frozen_request": [
            _record(paths["input"]),
            _record(paths["prompt"]),
            _record(paths["base"]),
            _record(paths["schema"]),
        ],
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        "privacy": "private_event_and_output_sanitized_metrics_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": spec["frozen_request"],
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = frozen["paths"]
    usage = {field: 0 for field in USAGE_FIELDS}
    usage_status = "unknown"
    accounting_complete = False
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar = _load_json(paths["sidecar"], "v229 sidecar")
        sidecar_record = _record(paths["sidecar"])
        try:
            usage = _usage(sidecar)
            usage_status = str(sidecar.get("usage_status"))
            accounting_complete = sidecar.get("usage_complete") is True
        except Exception:
            pass
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "v229_metric_repair_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(
            message.decode("utf-8", errors="replace")
        ),
        "error_message_bytes": len(message),
        "semantic_attempt_count": 1 if paths["capacity"].exists() else 0,
        "semantic_retry_count": 0,
        "usage_status": usage_status,
        "accounting_complete": accounting_complete,
        "usage": usage,
        "sidecar": sidecar_record,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "support_alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v229(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v229 terminal")
    frozen = freeze_v229(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise V229MetricRepairError("v229 launch exists; replay prohibited")
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "managed_chatgpt_auth_only": True,
            "v228_replayed": False,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["base"],
                prompt=frozen["prompt"],
                output_schema=frozen["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=frozen["paths"]["sidecar"],
                output_path=frozen["paths"]["output"],
                batch_size=1,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=frozen["paths"]["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V229MetricRepairError("v229 turn did not complete")
        sidecar = _load_json(frozen["paths"]["sidecar"], "v229 sidecar")
        usage = _usage(sidecar)
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
            or sidecar.get("model") != MODEL
            or sidecar.get("effort") != EFFORT
            or sidecar.get("error_class") is not None
        ):
            raise V229MetricRepairError("v229 measured sidecar contract failed")
        projection = _project_action(result.output, frozen)
        repair_normalized, repair_diagnostics = _normalize_repair(
            projection, frozen
        )
        second_audit = v228._second_episode_audit(
            frozen["predecessor_frozen"]
        )
        _write_immutable(
            frozen["paths"]["normalized"],
            {
                "metric_repair_action": projection["action"],
                "repair": repair_normalized,
                "repair_diagnostics": repair_diagnostics,
                "second_episode": second_audit["normalized"],
                "second_episode_diagnostics": second_audit["diagnostics"],
            },
        )
        gate = _gate(
            usage=usage,
            projection=projection,
            repair_diagnostics=repair_diagnostics,
            second_audit=second_audit,
        )
        _write_immutable(frozen["paths"]["gate"], gate)
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": (
                "v229_metric_grounding_repair_passed"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "terminal_reason": (
                "v229_structural_cost_gate_passed"
                if passed
                else "v229_metric_repair_quality_proxy_not_passed"
            ),
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "structural_gate_passed": passed,
            "support_alignment_quality_measured": False,
            "support_alignment_authorized": passed,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "gate": _record(frozen["paths"]["gate"]),
            "sidecar": _record(frozen["paths"]["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": (
                "run the frozen side-free support and alignment audit"
                if passed
                else "audit this immutable diagnostic before a new version"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v229 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v229 metric repair")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v229(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v229(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "structural_gate_passed": terminal.get(
                "structural_gate_passed", False
            ),
            "support_alignment_authorized": terminal.get(
                "support_alignment_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
