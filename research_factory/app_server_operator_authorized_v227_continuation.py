from __future__ import annotations

"""Run the one operator-authorized continuation of the frozen v227 attempt."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import codex_app_server
from . import efficient_backtest as efficient
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_operator_authorized_v227_continuation_v1"
RUNTIME_LOCK_VERSION = "pif_operator_authorized_v227_runtime_lock_v1"
TERMINAL_VERSION = "pif_operator_authorized_v227_terminal_v1"
PHASE_ID = "operator_authorized_v227_continuation_2026_07_16"
TURN_NAME = "operator_authorized_v227_combined_continuation"
MODEL = "gpt-5.6-luna"
EFFORT = "low"
MAX_NEW_TOKENS = 44_980
MAX_COMBINED_TOKENS = 77_250
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 130_000
MAX_SCHEMA_BYTES = 30_000
BASELINE_END_TO_END_TOKENS = 10_065_426
BASE_CANDIDATE_PROJECTED_TOKENS = 1_659_553
PRODUCTION_SEGMENT_SCOPE = 60
OBSERVED_SEGMENT_SCOPE = 4
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PINNED_CODEX_0_144_1 = (
    Path.home()
    / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin"
    / "bin/codex"
).resolve()
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
MIN_REMAINING_RESERVE_PERCENT = 20
TIMEOUT_SECONDS = 1200.0
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V227_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v227-independent-completeness-strategy"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "operator-authorized-v227-continuation-2026-07-16"
).resolve()
HOLD_ROOT = (PIPELINE_ROOT / "operator-hold-2026-07-16").resolve()
EXPECTED_DIRECT_HASHES = {
    "v227_terminal": "69cce40396009c21e2ac4a44092e623e2df37cdb83cc9f9e3b2b33852159fe5e",
    "v227_gate": "6971b97a998e63bced5d11cf67f2b7d2a6e437d2db09a1e77aa142b7c84e38aa",
    "v227_sidecar": "6f5824d9fc96684ce3daa6370278b97150e265725ad4f6bc183d4f7284c96115",
    "v227_runtime_lock": (
        "304658cd77dd488d14fbe5bdc27e54de1be0769de344b9e6af25207830225358"
    ),
}


class OperatorAuthorizedV227Error(RuntimeError):
    """The bounded continuation cannot be frozen or executed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OperatorAuthorizedV227Error(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        indent=2,
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise OperatorAuthorizedV227Error(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


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
        path = Path(str(record["path"])).expanduser().resolve()
        return _record(path) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _validate_usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    value = sidecar.get("usage")
    if not isinstance(value, Mapping):
        raise OperatorAuthorizedV227Error("usage is missing")
    usage = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if not isinstance(item, int) or isinstance(item, bool) or item < 0:
            raise OperatorAuthorizedV227Error("usage is malformed")
        usage[field] = item
    if usage["total_tokens"] != (
        usage["input_tokens"] + usage["output_tokens"]
    ):
        raise OperatorAuthorizedV227Error("usage total is inconsistent")
    return usage


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise OperatorAuthorizedV227Error(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(
    left: Mapping[str, int], right: Mapping[str, int]
) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _event_identity(event: Mapping[str, Any]) -> str:
    return _canonical_json(
        {
            key: value
            for key, value in event.items()
            if key not in {"confidence", "window_id"}
        }
    )


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
    }


def _normalize_gap_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    prepared = turn["private_input"]["normalization_segments"]
    if output.get("episode_id") != str(turn["episode_id"]):
        raise OperatorAuthorizedV227Error("normalization episode id drifted")
    rows = output.get("segments") or []
    expected_ids = [str(row["segment_id"]) for row in prepared]
    if [str(row.get("segment_id")) for row in rows] != expected_ids:
        raise OperatorAuthorizedV227Error("normalization segment coverage drifted")
    prepared_by_id = {str(row["segment_id"]): row for row in prepared}
    normalized_rows = []
    diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        source = prepared_by_id[segment_id]
        normalized, ownership = efficient.normalize_windowed_core_payload(
            dict(row),
            segment_text=str(source["segment_text"]),
            boundaries=source["boundaries"],
            max_events=MAX_EVENTS_PER_SEGMENT,
        )
        existing = source["existing_events"]
        existing_identities = {_event_identity(event) for event in existing}
        exact_duplicates = [
            event
            for event in normalized.get("events") or []
            if _event_identity(event) in existing_identities
        ]
        output_count = len(normalized.get("events") or [])
        novel_count = output_count - len(exact_duplicates)
        combined_count = len(existing) + output_count
        metric_errors = [
            {
                "event_index": index,
                "errors": _metric_grounding_errors(event),
            }
            for index, event in enumerate(normalized.get("events") or [])
            if _metric_grounding_errors(event)
        ]
        normalized_rows.append(normalized)
        diagnostics.append(
            {
                "segment_id": segment_id,
                "input_events": ownership["input_events"],
                "output_events": ownership["output_events"],
                "exactness_pruned_events": ownership[
                    "exactness_pruned_events"
                ],
                "event_cap_hit": ownership["event_cap_hit"],
                "metric_grounding_error_events": len(metric_errors),
                "metric_grounding_errors": metric_errors,
                "status_ok": not metric_errors,
                "exact_existing_duplicate_events": len(exact_duplicates),
                "new_event_count": novel_count,
                "existing_event_count": len(existing),
                "combined_event_count": combined_count,
                "combined_event_cap_exceeded": (
                    combined_count > MAX_EVENTS_PER_SEGMENT
                ),
                "density_stratum": source["density_stratum"],
            }
        )
    return {
        "episode_id": str(turn["episode_id"]),
        "segments": normalized_rows,
    }, diagnostics


def _metric_grounding_errors(event: Mapping[str, Any]) -> list[str]:
    evidence = str(event.get("evidence") or "")
    metric_raw = str(event.get("metric_raw_text") or "")
    parts = {
        key: str(event.get(key) or "")
        for key in ("metric_value", "metric_unit", "metric_comparator")
        if str(event.get(key) or "")
    }
    errors = []
    if metric_raw and metric_raw not in evidence:
        errors.append("metric_raw_text_not_in_evidence")
    if parts and not metric_raw:
        errors.append("metric_components_without_raw_text")
    for key, value in parts.items():
        if value not in evidence and value not in metric_raw:
            errors.append(f"{key}_not_in_evidence")
    return errors


def _validate_gap_output(
    output: Any,
    *,
    schema: Mapping[str, Any],
    episode_id: str,
    segment_ids: Sequence[str],
) -> list[str]:
    if not isinstance(output, Mapping):
        return ["output_not_object"]
    try:
        _validate_schema(dict(schema), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    if output.get("episode_id") != episode_id:
        return ["episode_id"]
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(segment_ids):
        return ["segment_order_or_coverage"]
    for row in rows:
        events = row.get("events") or []
        if (row.get("status") == "coded") != bool(events):
            return ["coded_status_event_presence"]
        if not events and row.get("status") != "no_signal":
            return ["empty_gap_output_must_be_no_signal"]
    return []


def _direct_paths() -> dict[str, Path]:
    first_root = (
        V227_ROOT
        / "turns"
        / "v227-luna-gap-ba5e86bd4ce77f079d3379fb"
    )
    return {
        "v227_terminal": V227_ROOT / "terminal.json",
        "v227_gate": V227_ROOT / "completeness-gate.json",
        "v227_sidecar": first_root / "sidecar.json",
        "v227_runtime_lock": V227_ROOT / "runtime-lock.json",
        "operator_hold": HOLD_ROOT / "operator-hold-receipt.json",
    }


def _sha256(path: Path) -> str:
    return str(_record(path)["sha256"])


def _unexpected_numbered_successors() -> list[str]:
    unexpected: list[str] = []
    for path in PIPELINE_ROOT.iterdir():
        match = re.search(r"(?:^|[-_])v(\d+)(?:[-_]|$)", path.name)
        if match is not None and int(match.group(1)) >= 228:
            unexpected.append(path.name)
    return sorted(unexpected)


def validate_direct_lineage() -> dict[str, Any]:
    paths = _direct_paths()
    if any(not path.is_file() for path in paths.values()):
        raise OperatorAuthorizedV227Error("direct lineage artifact is missing")
    for name, expected in EXPECTED_DIRECT_HASHES.items():
        if _sha256(paths[name]) != expected:
            raise OperatorAuthorizedV227Error(f"direct lineage drifted: {name}")
    terminal = _load_json(paths["v227_terminal"], "v227 terminal")
    gate = _load_json(paths["v227_gate"], "v227 gate")
    sidecar = _load_json(paths["v227_sidecar"], "v227 sidecar")
    hold = _load_json(paths["operator_hold"], "operator hold")
    if (
        terminal.get("state") != "development_strategy_not_accepted"
        or terminal.get("attempted_turn_count") != 1
        or terminal.get("measured_turn_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage", {}).get("total_tokens") != 32_270
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or gate.get("completed_turn_count") != 1
        or gate.get("not_started_turn_count") != 1
        or gate.get("new_event_count") != 1
        or sidecar.get("state") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or hold.get("state") != "operator_hold"
        or hold.get("latest_frozen_version") != "v227"
        or hold.get("v228_exists") is not False
        or hold.get("v229_exists") is not False
        or hold.get("semantic_child_active") is not False
        or hold.get("production_mutated") is not False
    ):
        raise OperatorAuthorizedV227Error("direct lineage contract drifted")
    if _unexpected_numbered_successors():
        raise OperatorAuthorizedV227Error("numbered successor unexpectedly exists")
    return {
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "gate": gate,
        "sidecar": sidecar,
        "hold": hold,
    }


def _v227_request_paths() -> dict[str, Path]:
    spec = _load_json(V227_ROOT / "attempt-spec.json", "v227 spec")
    first, second = spec["frozen_turns"]
    first_paths = _turn_paths(V227_ROOT, str(first["turn_name"]))
    second_paths = _turn_paths(V227_ROOT, str(second["turn_name"]))
    return {
        "first_input": first_paths["input"],
        "first_output": first_paths["output"],
        "first_normalized": first_paths["normalized"],
        "second_input": second_paths["input"],
        "second_prompt": second_paths["prompt"],
        "second_base": second_paths["base"],
        "second_schema": second_paths["schema"],
    }


def _single_failed_evidence_packet(
    lineage: Mapping[str, Any], request_paths: Mapping[str, Path]
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_path = request_paths["first_output"]
    sidecar_output = Path(
        str(lineage["sidecar"].get("output_path") or "")
    ).expanduser().resolve()
    if sidecar_output != raw_path.resolve():
        raise OperatorAuthorizedV227Error("v227 sidecar output path drifted")
    raw = _load_json(raw_path, "v227 first output")
    normalized = _load_json(
        request_paths["first_normalized"], "v227 first normalized output"
    )
    frozen_input = _load_json(request_paths["first_input"], "v227 first input")
    retained = {
        _event_identity(event)
        for row in normalized.get("segments") or []
        for event in row.get("events") or []
    }
    dropped = [
        (
            str(row["segment_id"]),
            dict(event),
            dict(row["segment_source_context"]),
        )
        for row in raw.get("segments") or []
        for event in row.get("events") or []
        if _event_identity(event) not in retained
    ]
    if len(dropped) != 1:
        raise OperatorAuthorizedV227Error("expected one nonexact v227 proposal")
    segment_id, event, segment_source_context = dropped[0]
    source_rows = {
        str(row["segment_id"]): row
        for row in frozen_input["normalization_segments"]
    }
    source = source_rows.get(segment_id)
    if source is None or not str(source.get("segment_text") or ""):
        raise OperatorAuthorizedV227Error("repair source unit is missing")
    repair_id = "repair_" + sha256_text(
        _canonical_json({"segment_id": segment_id, "event": event})
    )[:24]
    packet = {
        "repair_id": repair_id,
        "segment_id": segment_id,
        "source_unit": str(source["segment_text"]),
        "event": event,
        "segment_source_context": segment_source_context,
    }
    return packet, dict(source)


def combined_instructions(second_base: str) -> str:
    return second_base + """

# Observable exact-evidence repair in this same turn
The prompt also contains exactly one frozen event whose prior evidence was not
an exact source substring. Make no semantic change to that event. Choose patch
only when one exact contiguous substring from source_unit supports every
material event claim, including actor, attribution, stance, target, time,
metric, negation, and causal mechanism. Return that exact substring. Otherwise
choose drop and return an empty evidence string. Do not infer support merely
because similar words occur.

Also perform the independent omission-only task for the second episode exactly
as specified above. The two tasks are independent. Return one JSON object with
evidence_repair plus the requested episode_id and segments. Return JSON only.
""".strip()


def combined_prompt(repair: Mapping[str, Any], second_prompt: str) -> str:
    return (
        "# Single observable evidence repair\n"
        + json.dumps(repair, ensure_ascii=True, separators=(",", ":"))
        + "\n# Never-started second episode omission packet\n"
        + second_prompt
    )


def combined_schema(
    repair_id: str, second_schema: Mapping[str, Any]
) -> dict[str, Any]:
    schema = copy.deepcopy(dict(second_schema))
    schema["required"] = ["evidence_repair", *schema["required"]]
    schema["properties"] = {
        "evidence_repair": {
            "type": "object",
            "additionalProperties": False,
            "required": ["repair_id", "action", "evidence"],
            "properties": {
                "repair_id": {"type": "string", "enum": [repair_id]},
                "action": {"type": "string", "enum": ["patch", "drop"]},
                "evidence": {"type": "string"},
            },
        },
        **schema["properties"],
    }
    return schema


def _request_paths(root: Path) -> dict[str, Path]:
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
        "gate": turn_root / "structural-cost-gate.json",
    }


def _capacity_policy(root: Path, lineage: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_NEW_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v227_terminal": lineage["records"]["v227_terminal"],
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
            "phase_total_token_bound": MAX_NEW_TOKENS,
            "projected_phase_quota_points": projected_points,
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
        "projected_phase_quota_points": projected_points,
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
                Path(efficient.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "operator continuation runtime lock")
    expected_runtime = {str(path) for path in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or actual_runtime != expected_runtime
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("numbered_successor_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise OperatorAuthorizedV227Error("runtime lock contract drifted")
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
        raise OperatorAuthorizedV227Error("runtime lock record drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def freeze_authorized_continuation(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "terminal")}
    if any(root.iterdir()):
        lock_path = root / "runtime-lock.json"
        spec_path = root / "attempt-spec.json"
        if not lock_path.is_file() or not spec_path.is_file():
            raise OperatorAuthorizedV227Error("unfinished root is not replayable")
        if (root / "launch-receipt.json").exists():
            raise OperatorAuthorizedV227Error("launch exists; replay prohibited")
        verify_runtime_lock(lock_path)
        return _load_frozen(root)
    lineage = validate_direct_lineage()
    source_paths = _v227_request_paths()
    repair, repair_source = _single_failed_evidence_packet(lineage, source_paths)
    second_input = _load_json(source_paths["second_input"], "v227 second input")
    second_schema = _load_json(source_paths["second_schema"], "v227 second schema")
    second_prompt = source_paths["second_prompt"].read_text(encoding="utf-8")
    second_base = source_paths["second_base"].read_text(encoding="utf-8")
    prompt = combined_prompt(repair, second_prompt)
    base = combined_instructions(second_base)
    schema = combined_schema(str(repair["repair_id"]), second_schema)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise OperatorAuthorizedV227Error("combined prompt exceeds frozen cap")
    if schema_bytes > MAX_SCHEMA_BYTES:
        raise OperatorAuthorizedV227Error("combined schema exceeds frozen cap")
    request = {
        "schema_version": SCHEMA_VERSION,
        "repair": repair,
        "repair_normalization_source": repair_source,
        "second_episode_input": second_input,
        "privacy": "private_source_windows_context_and_events",
    }
    paths = _request_paths(root)
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
        "operator_authorization_scope": "one_combined_v227_continuation_turn",
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_new_turn_tokens": MAX_NEW_TOKENS,
        "maximum_combined_v227_tokens": MAX_COMBINED_TOKENS,
        "completed_v227_turn_adopted": True,
        "completed_v227_turn_replay_allowed": False,
        "v227_second_turn_previously_started": False,
        "new_extraction_strategy_allowed": False,
        "support_or_alignment_allowed": False,
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
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
        "privacy": "private_inputs_outputs_and_sanitized_terminal_metrics",
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
        "numbered_successor_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "created_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _load_frozen(root: Path) -> dict[str, Any]:
    paths = _request_paths(root)
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "operator continuation spec")
    for record in spec.get("frozen_request") or []:
        if not _verify_record(record):
            raise OperatorAuthorizedV227Error("frozen request drifted")
    return {
        "root": root,
        "paths": paths,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "input": _load_json(paths["input"], "combined input"),
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "base": paths["base"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], "combined schema"),
    }


def validate_combined_output(
    output: Any, frozen: Mapping[str, Any]
) -> list[str]:
    if not isinstance(output, Mapping):
        return ["output_not_object"]
    try:
        _validate_schema(frozen["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    repair = output.get("evidence_repair") or {}
    expected = frozen["input"]["repair"]
    if repair.get("repair_id") != expected["repair_id"]:
        return ["repair_id"]
    action = repair.get("action")
    evidence = str(repair.get("evidence") or "")
    source = str(expected["source_unit"])
    if action == "patch" and (not evidence or evidence not in source):
        return ["repair_evidence_not_exact"]
    if action == "drop" and evidence:
        return ["drop_evidence_must_be_empty"]
    second = frozen["input"]["second_episode_input"]
    second_rows = second["normalization_segments"]
    episode_output = {
        "episode_id": output.get("episode_id"),
        "segments": output.get("segments"),
    }
    episode_schema = copy.deepcopy(frozen["schema"])
    episode_schema["required"] = ["episode_id", "segments"]
    episode_schema["properties"].pop("evidence_repair", None)
    errors = _validate_gap_output(
        episode_output,
        schema=episode_schema,
        episode_id=str(second["episode_id"]),
        segment_ids=[str(row["segment_id"]) for row in second_rows],
    )
    return errors


def _normalize_result(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    request = frozen["input"]
    repair_packet = request["repair"]
    repair_source = request["repair_normalization_source"]
    repair_value = output["evidence_repair"]
    repaired_events = []
    if repair_value["action"] == "patch":
        event = copy.deepcopy(repair_packet["event"])
        event["evidence"] = repair_value["evidence"]
        repaired_events.append(event)
    repair_payload = {
        "episode_id": "repair_episode",
        "segments": [
            {
                "segment_id": repair_packet["segment_id"],
                "status": "coded" if repaired_events else "no_signal",
                "segment_source_context": repair_packet[
                    "segment_source_context"
                ],
                "no_signal_reason": (
                    ""
                    if repaired_events
                    else "The LLM explicitly dropped the proposal."
                ),
                "events": repaired_events,
            }
        ],
    }
    repair_turn = {
        "episode_id": "repair_episode",
        "private_input": {"normalization_segments": [repair_source]},
    }
    repair_normalized, repair_diagnostics = _normalize_gap_output(
        repair_payload, repair_turn
    )
    second_input = request["second_episode_input"]
    second_payload = {
        "episode_id": output["episode_id"],
        "segments": output["segments"],
    }
    second_turn = {
        "episode_id": second_input["episode_id"],
        "private_input": {
            "normalization_segments": second_input["normalization_segments"]
        },
    }
    second_normalized, second_diagnostics = _normalize_gap_output(
        second_payload, second_turn
    )
    normalized = {
        "repair": {
            "repair_id": repair_value["repair_id"],
            "action": repair_value["action"],
            "normalized": repair_normalized,
        },
        "second_episode": second_normalized,
    }
    diagnostics = {
        "repair": repair_diagnostics,
        "second_episode": second_diagnostics,
    }
    return normalized, diagnostics


def _gate(
    *, usage: Mapping[str, int], normalized: Mapping[str, Any],
    diagnostics: Mapping[str, Any]
) -> dict[str, Any]:
    prior_usage = 32_270
    combined_tokens = prior_usage + int(usage["total_tokens"])
    scaled = math.ceil(
        combined_tokens
        * PRODUCTION_SEGMENT_SCOPE
        / OBSERVED_SEGMENT_SCOPE
    )
    projected = BASE_CANDIDATE_PROJECTED_TOKENS + scaled
    ratio = projected / BASELINE_END_TO_END_TOKENS
    repair_rows = diagnostics["repair"]
    second_rows = diagnostics["second_episode"]
    second_dense = [
        row for row in second_rows if row.get("density_stratum") == "dense"
    ]
    final_rows = [*repair_rows, *second_rows]
    checks = {
        "repair_decision_patch_or_drop": normalized["repair"]["action"]
        in {"patch", "drop"},
        "repair_trigger_cleared": all(
            int(row["exactness_pruned_events"]) == 0 for row in repair_rows
        ),
        "second_episode_completed": len(second_rows) == 2,
        "second_dense_added_grounded_event": len(second_dense) == 1
        and int(second_dense[0]["new_event_count"]) > 0,
        "exact_evidence_rate_1": all(
            int(row["exactness_pruned_events"]) == 0 for row in final_rows
        ),
        "metric_grounding_error_events_0": all(
            int(row["metric_grounding_error_events"]) == 0
            for row in final_rows
        ),
        "event_cap_violation_0": all(
            row["combined_event_cap_exceeded"] is False for row in final_rows
        ),
        "new_turn_usage_lte_44980": int(usage["total_tokens"])
        <= MAX_NEW_TOKENS,
        "combined_v227_usage_lte_77250": combined_tokens
        <= MAX_COMBINED_TOKENS,
        "production_amortized_ratio_lte_0_28": projected * 25
        <= BASELINE_END_TO_END_TOKENS * 7,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(
            name for name, passed in checks.items() if not passed
        ),
        "repair_action": normalized["repair"]["action"],
        "repair_retained_event_count": sum(
            int(row["new_event_count"]) for row in repair_rows
        ),
        "second_dense_new_event_count": (
            int(second_dense[0]["new_event_count"]) if second_dense else 0
        ),
        "exact_existing_duplicate_event_count": sum(
            int(row["exact_existing_duplicate_events"]) for row in final_rows
        ),
        "second_episode_total_new_event_count": sum(
            int(row["new_event_count"]) for row in second_rows
        ),
        "new_turn_usage": dict(usage),
        "prior_v227_usage_tokens": prior_usage,
        "combined_v227_diagnostic_tokens": combined_tokens,
        "production_amortized_total_tokens": projected,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "support_alignment_quality_measured": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _client_factory(
    policy_path: Path,
) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path,
        inner_factory=_inner_factory,
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException):
    paths = frozen["paths"]
    usage = {field: 0 for field in USAGE_FIELDS}
    usage_status = "unknown"
    accounting_complete = False
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar = _load_json(paths["sidecar"], "continuation sidecar")
        sidecar_record = _record(paths["sidecar"])
        try:
            usage = _validate_usage(sidecar)
            usage_status = str(sidecar.get("usage_status"))
            accounting_complete = sidecar.get("usage_complete") is True
        except Exception:
            pass
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "operator_authorized_continuation_not_accepted",
        "terminal_reason": "bounded_continuation_attempt_failed",
        "terminal_classification": "operator_hold_verification_required",
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
        "support_alignment_quality_measured": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "goal_status_required": "paused",
        "operator_hold_restored": False,
        "operator_hold_verification_required": True,
        "successor_authorized": False,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_authorized_continuation(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "operator continuation terminal")
    frozen = freeze_authorized_continuation(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise OperatorAuthorizedV227Error("launch exists; replay prohibited")
    launch = {
        "schema_version": SCHEMA_VERSION,
        "launched_at": now_iso(),
        "phase_id": PHASE_ID,
        "declared_turn_count": 1,
        "retry_count": 0,
        "model": MODEL,
        "effort": EFFORT,
        "managed_chatgpt_auth_only": True,
        "completed_v227_turn_replayed": False,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
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
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=frozen["paths"]["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise OperatorAuthorizedV227Error("combined turn did not complete")
        errors = validate_combined_output(result.output, frozen)
        if errors:
            raise OperatorAuthorizedV227Error(
                f"combined structured output failed: {len(errors)}"
            )
        sidecar = _load_json(frozen["paths"]["sidecar"], "continuation sidecar")
        usage = _validate_usage(sidecar)
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
            raise OperatorAuthorizedV227Error("measured sidecar contract failed")
        normalized, diagnostics = _normalize_result(result.output, frozen)
        _write_immutable(
            frozen["paths"]["normalized"],
            {"normalized": normalized, "diagnostics": diagnostics},
        )
        gate = _gate(
            usage=usage, normalized=normalized, diagnostics=diagnostics
        )
        if _unexpected_numbered_successors():
            raise OperatorAuthorizedV227Error(
                "numbered successor appeared during bounded continuation"
            )
        _write_immutable(frozen["paths"]["gate"], gate)
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": (
                "operator_authorized_structural_cost_pass"
                if passed
                else "operator_authorized_continuation_not_accepted"
            ),
            "terminal_reason": (
                "bounded_v227_continuation_structural_cost_gate_passed"
                if passed
                else "bounded_v227_continuation_gate_not_passed"
            ),
            "terminal_classification": "operator_hold_verification_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "combined_v227_diagnostic_tokens": gate[
                "combined_v227_diagnostic_tokens"
            ],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "structural_cost_gate_passed": passed,
            "support_alignment_quality_measured": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "goal_status_required": "paused",
            "operator_hold_restored": False,
            "operator_hold_verification_required": True,
            "successor_authorized": False,
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(frozen["paths"]["gate"]),
            "sidecar": _record(frozen["paths"]["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_operator_decision": (
                "Kolby must decide whether the structurally valid candidate "
                "may receive a separately authorized support/alignment audit."
                if passed
                else "No successor is authorized under the bounded relaunch."
            ),
        }
        _write_stable_time(terminal_path, terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if terminal_path.exists():
            return _load_json(terminal_path, "operator continuation terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the operator-authorized v227 continuation"
    )
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_authorized_continuation(output_dir=Path(args.output_dir))
        result = {
            "state": "frozen_before_single_semantic_turn",
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_authorized_continuation(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "structural_cost_gate_passed": terminal.get(
                "structural_cost_gate_passed", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
