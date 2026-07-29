from __future__ import annotations

"""Validate exact evidence through LLM-selected opaque source units."""

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
from . import app_server_operator_authorized_v227_continuation as predecessor
from . import codex_app_server
from . import efficient_backtest as efficient
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v228_exact_span_repair_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v228_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v228_terminal_v1"
PHASE_ID = "development_selection_v5_4_v228_exact_span_repair"
TURN_NAME = "v228_exact_span_repair"
MODEL = "gpt-5.6-luna"
EFFORT = "low"
MAX_NEW_TOKENS = 30_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
TIMEOUT_SECONDS = 1200.0
PINNED_CODEX_0_144_1 = predecessor.PINNED_CODEX_0_144_1
USAGE_FIELDS = predecessor.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
PREDECESSOR_ROOT = (
    PIPELINE_ROOT / "operator-authorized-v227-continuation-2026-07-16"
).resolve()
PREDECESSOR_TURN_ROOT = (
    PREDECESSOR_ROOT
    / "turns/operator-authorized-v227-combined-continuation"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v228-exact-span-repair"
).resolve()
BASELINE_END_TO_END_TOKENS = 10_065_426
BASE_CANDIDATE_PROJECTED_TOKENS = 1_659_553
PRIOR_V227_TOKENS = 32_270
PREDECESSOR_TOKENS = 37_386
PRODUCTION_SEGMENT_SCOPE = 60
OBSERVED_SEGMENT_SCOPE = 4
EXPECTED_PREDECESSOR_HASHES = {
    "attempt_spec": "f7226282c2a43de075631d7b9840bcb831fc6a9e89f5a6dfa668664528808c97",
    "runtime_lock": "ded4f8f243937dd4fbf979908b74fe85121546c2def61e9b6fad8605fa24470c",
    "terminal": "9628febe47d2edb5bb6d1882f2664460519279320df8e8a9bfdfa3aa112c7b29",
    "input": "3a85eacb9ff9b47f71c0e92bb89c1450b1138b0250f2d6baf88da6c82ba93231",
    "output": "1988c3350c4211158050296ffbdec2627b68e32c6df9e7bc089ea14af05442ef",
    "sidecar": "be4f221ee930906906768434c6071842d67d833e1cb01e83f2ec5ab6e49b012c",
}


class V228ExactSpanError(RuntimeError):
    """The exact-span repair cannot be frozen or executed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V228ExactSpanError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, indent=2
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V228ExactSpanError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V228ExactSpanError(f"frozen {path.name} drifted")
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


def _predecessor_paths() -> dict[str, Path]:
    return {
        "attempt_spec": PREDECESSOR_ROOT / "attempt-spec.json",
        "runtime_lock": PREDECESSOR_ROOT / "runtime-lock.json",
        "terminal": PREDECESSOR_ROOT / "terminal.json",
        "input": PREDECESSOR_TURN_ROOT / "input.private.json",
        "output": PREDECESSOR_TURN_ROOT / "output.private.json",
        "sidecar": PREDECESSOR_TURN_ROOT / "sidecar.json",
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
        "gate": turn_root / "exact-span-gate.json",
    }


def _source_units(source: str) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    cursor = 0
    for raw_line in source.splitlines(keepends=True):
        body = raw_line.rstrip("\r\n")
        line_end = cursor + len(body)
        if body.strip():
            units.append(
                {
                    "unit_id": f"U{len(units):03d}",
                    "start_char": cursor,
                    "end_char": line_end,
                    "text": body,
                }
            )
        cursor += len(raw_line)
    if cursor < len(source):
        body = source[cursor:]
        if body.strip():
            units.append(
                {
                    "unit_id": f"U{len(units):03d}",
                    "start_char": cursor,
                    "end_char": len(source),
                    "text": body,
                }
            )
    if not units:
        raise V228ExactSpanError("source has no selectable units")
    return units


def _public_units(units: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return [
        {"unit_id": str(unit["unit_id"]), "text": str(unit["text"])}
        for unit in units
    ]


def _event_without_evidence(event: Mapping[str, Any]) -> dict[str, Any]:
    omitted = {"evidence", "window_id", "confidence"}
    return {
        key: value
        for key, value in event.items()
        if key not in omitted and value not in (None, "", [], {})
    }


def _repair_schema(repair_id: str, unit_ids: Sequence[str]) -> dict[str, Any]:
    choices = ["NONE", *unit_ids]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "repair_id",
            "action",
            "start_unit_id",
            "end_unit_id",
        ],
        "properties": {
            "repair_id": {"type": "string", "enum": [repair_id]},
            "action": {"type": "string", "enum": ["patch", "drop"]},
            "start_unit_id": {"type": "string", "enum": choices},
            "end_unit_id": {"type": "string", "enum": choices},
        },
    }


def _base_instructions() -> str:
    return """You select exact source evidence for one frozen structured event.
The source is supplied as opaque ordered source units whose text is verbatim.
Do not rewrite, summarize, or quote source text. If one minimal contiguous unit
range entails every material event field, choose patch and return its first and
last unit IDs. Material fields include actor, speaker, reported actor, target,
stance, certainty, temporal horizon, event type, metric, negation, attribution,
and causal mechanism. Similar language is insufficient. If no contiguous range
supports every material field, choose drop and return NONE for both unit IDs.
Return only the schema-valid JSON object. Do not use tools."""


def _prompt(request: Mapping[str, Any]) -> str:
    return (
        "Select an exact evidence range or drop this event. Source-unit IDs are "
        "opaque and ordered.\n"
        + _canonical_json(request)
    )


def _validate_second_episode(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> list[str]:
    second = frozen["predecessor_input"]["second_episode_input"]
    schema = copy.deepcopy(frozen["predecessor_schema"])
    schema["required"] = ["episode_id", "segments"]
    schema["properties"].pop("evidence_repair", None)
    episode_output = {
        "episode_id": output.get("episode_id"),
        "segments": output.get("segments"),
    }
    try:
        _validate_schema(schema, episode_output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    if episode_output["episode_id"] != second["episode_id"]:
        return ["episode_id"]
    expected_ids = [
        str(row["segment_id"]) for row in second["normalization_segments"]
    ]
    rows = episode_output.get("segments") or []
    if [str(row.get("segment_id")) for row in rows] != expected_ids:
        return ["segment_order_or_coverage"]
    for row in rows:
        status = row.get("status")
        events = row.get("events") or []
        if bool(events) != (status == "coded"):
            return ["coded_status_event_presence"]
        if not events and status not in {
            "no_signal",
            "excluded_source_context",
        }:
            return ["empty_status_not_allowed"]
    return []


def _second_episode_audit(frozen: Mapping[str, Any]) -> dict[str, Any]:
    output = frozen["predecessor_output"]
    errors = _validate_second_episode(output, frozen)
    second = frozen["predecessor_input"]["second_episode_input"]
    episode_output = {
        "episode_id": output["episode_id"],
        "segments": output["segments"],
    }
    normalized, diagnostics = predecessor._normalize_gap_output(
        episode_output,
        {
            "episode_id": second["episode_id"],
            "private_input": {
                "normalization_segments": second["normalization_segments"]
            },
        },
    )
    dense = [
        row for row in diagnostics if row.get("density_stratum") == "dense"
    ]
    return {
        "errors": errors,
        "normalized": normalized,
        "diagnostics": diagnostics,
        "sanitized": {
            "schema_status_success": not errors,
            "segment_count": len(diagnostics),
            "dense_segment_count": len(dense),
            "dense_new_event_count": sum(
                int(row["new_event_count"]) for row in dense
            ),
            "total_new_event_count": sum(
                int(row["new_event_count"]) for row in diagnostics
            ),
            "exactness_pruned_events": sum(
                int(row["exactness_pruned_events"]) for row in diagnostics
            ),
            "metric_grounding_error_events": sum(
                int(row["metric_grounding_error_events"])
                for row in diagnostics
            ),
            "event_cap_violations": sum(
                1 for row in diagnostics if row["combined_event_cap_exceeded"]
            ),
            "exact_existing_duplicate_events": sum(
                int(row["exact_existing_duplicate_events"])
                for row in diagnostics
            ),
        },
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _predecessor_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_PREDECESSOR_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V228ExactSpanError(f"predecessor {name} drifted")
    predecessor.verify_runtime_lock(paths["runtime_lock"])
    frozen = predecessor._load_frozen(PREDECESSOR_ROOT)
    terminal = _load_json(paths["terminal"], "predecessor terminal")
    sidecar = _load_json(paths["sidecar"], "predecessor sidecar")
    output = _load_json(paths["output"], "predecessor output")
    if (
        terminal.get("state") != "operator_authorized_continuation_not_accepted"
        or terminal.get("usage_status") != "measured"
        or terminal.get("semantic_retry_count") != 0
        or (terminal.get("usage") or {}).get("total_tokens")
        != PREDECESSOR_TOKENS
        or sidecar.get("state") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_complete") is not True
        or predecessor.validate_combined_output(output, frozen)
        != ["repair_evidence_not_exact"]
    ):
        raise V228ExactSpanError("predecessor failure contract drifted")
    return {
        "records": records,
        "predecessor_frozen": frozen,
        "terminal": terminal,
        "sidecar": sidecar,
        "output": output,
    }


def _project_repair(
    output: Any, frozen: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(output, Mapping):
        raise V228ExactSpanError("repair output is not an object")
    try:
        _validate_schema(frozen["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V228ExactSpanError("repair schema validation failed") from exc
    request = frozen["request"]
    if output.get("repair_id") != request["repair_id"]:
        raise V228ExactSpanError("repair id drifted")
    action = str(output.get("action"))
    start_id = str(output.get("start_unit_id"))
    end_id = str(output.get("end_unit_id"))
    if action == "drop":
        if start_id != "NONE" or end_id != "NONE":
            raise V228ExactSpanError("drop must use NONE unit ids")
        return {
            "repair_id": request["repair_id"],
            "action": "drop",
            "start_unit_id": "NONE",
            "end_unit_id": "NONE",
            "start_char": None,
            "end_char": None,
            "evidence": "",
        }
    units = frozen["source_units"]
    by_id = {str(unit["unit_id"]): unit for unit in units}
    if start_id not in by_id or end_id not in by_id:
        raise V228ExactSpanError("patch unit id is unknown")
    indices = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
    if indices[start_id] > indices[end_id]:
        raise V228ExactSpanError("patch unit range is reversed")
    start = int(by_id[start_id]["start_char"])
    end = int(by_id[end_id]["end_char"])
    source = frozen["source"]
    evidence = source[start:end]
    if not evidence or evidence != source[start:end]:
        raise V228ExactSpanError("projected evidence is not exact")
    return {
        "repair_id": request["repair_id"],
        "action": "patch",
        "start_unit_id": start_id,
        "end_unit_id": end_id,
        "start_char": start,
        "end_char": end,
        "evidence": evidence,
    }


def _normalize_repair(
    projection: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    packet = frozen["repair_packet"]
    source = frozen["repair_source"]
    events: list[dict[str, Any]] = []
    if projection["action"] == "patch":
        event = copy.deepcopy(packet["event"])
        event["evidence"] = projection["evidence"]
        events.append(event)
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
    return predecessor._normalize_gap_output(
        payload,
        {
            "episode_id": "repair_episode",
            "private_input": {"normalization_segments": [source]},
        },
    )


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    try:
        usage = predecessor._validate_usage(sidecar)
    except Exception as exc:
        raise V228ExactSpanError("usage is incomplete") from exc
    return usage


def _production_projection() -> dict[str, Any]:
    combined = PRIOR_V227_TOKENS + PREDECESSOR_TOKENS
    scaled = math.ceil(
        combined * PRODUCTION_SEGMENT_SCOPE / OBSERVED_SEGMENT_SCOPE
    )
    total = BASE_CANDIDATE_PROJECTED_TOKENS + scaled
    return {
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(
            total / BASELINE_END_TO_END_TOKENS, 6
        ),
        "development_diagnostic_tokens_excluded_from_production_path": True,
        "production_protocol_delta": (
            "replace copied evidence text with opaque source-unit selection "
            "inside the existing extractor turn"
        ),
    }


def _gate(
    *,
    usage: Mapping[str, int],
    projection: Mapping[str, Any],
    repair_diagnostics: Sequence[Mapping[str, Any]],
    second_audit: Mapping[str, Any],
) -> dict[str, Any]:
    second = second_audit["sanitized"]
    production = _production_projection()
    checks = {
        "repair_decision_patch_or_drop": projection["action"]
        in {"patch", "drop"},
        "repair_exact_span_projected_or_dropped": (
            projection["action"] == "drop"
            or bool(projection["evidence"])
        ),
        "repair_exactness_pruned_events_0": all(
            int(row["exactness_pruned_events"]) == 0
            for row in repair_diagnostics
        ),
        "second_episode_schema_status_success": second[
            "schema_status_success"
        ],
        "second_dense_added_grounded_event": second[
            "dense_new_event_count"
        ]
        >= 1,
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
        "new_turn_usage_lte_30000": int(usage["total_tokens"])
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
            key for key, passed in checks.items() if not passed
        ),
        "repair_action": projection["action"],
        "repair_retained_event_count": sum(
            int(row["new_event_count"]) for row in repair_diagnostics
        ),
        "second_episode": second,
        "new_turn_usage": dict(usage),
        "cumulative_development_diagnostic_tokens": (
            PRIOR_V227_TOKENS + PREDECESSOR_TOKENS + int(usage["total_tokens"])
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
        "predecessor_terminal": lineage["records"]["terminal"],
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
                Path(efficient.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(predecessor.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v228 runtime lock")
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
        raise V228ExactSpanError("runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("second_episode_salvage"),
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V228ExactSpanError("runtime lock record drifted")
    _validate_lineage()
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    request = _load_json(paths["input"], "v228 input")
    predecessor_input = _load_json(
        PREDECESSOR_TURN_ROOT / "input.private.json", "predecessor input"
    )
    predecessor_output = _load_json(
        PREDECESSOR_TURN_ROOT / "output.private.json", "predecessor output"
    )
    predecessor_schema = _load_json(
        PREDECESSOR_TURN_ROOT / "schema.json", "predecessor schema"
    )
    return {
        "root": root,
        "paths": paths,
        "spec_path": root / "attempt-spec.json",
        "spec": _load_json(root / "attempt-spec.json", "v228 spec"),
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "request": request,
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "base": paths["base"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], "v228 schema"),
        "source": predecessor_input["repair"]["source_unit"],
        "source_units": request["source_units_private"],
        "repair_packet": predecessor_input["repair"],
        "repair_source": predecessor_input["repair_normalization_source"],
        "predecessor_input": predecessor_input,
        "predecessor_output": predecessor_output,
        "predecessor_schema": predecessor_schema,
    }


def freeze_v228(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V228ExactSpanError("unfinished v228 root is not replayable")
        if (root / "launch-receipt.json").exists():
            raise V228ExactSpanError("v228 launch exists; replay prohibited")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    predecessor_frozen = lineage["predecessor_frozen"]
    predecessor_input = predecessor_frozen["input"]
    packet = predecessor_input["repair"]
    source = str(packet["source_unit"])
    units = _source_units(source)
    request = {
        "repair_id": str(packet["repair_id"]),
        "event": _event_without_evidence(packet["event"]),
        "source_units": _public_units(units),
        "source_units_private": units,
    }
    model_request = {
        "repair_id": request["repair_id"],
        "event": request["event"],
        "source_units": request["source_units"],
    }
    schema = _repair_schema(
        request["repair_id"], [str(unit["unit_id"]) for unit in units]
    )
    prompt = _prompt(model_request)
    base = _base_instructions()
    paths = _turn_paths(root)
    _write_immutable(paths["input"], request)
    _write_private_text(paths["prompt"], prompt)
    _write_private_text(paths["base"], base)
    _write_immutable(paths["schema"], schema)
    frozen = _load_frozen_after_request(root, lineage, paths)
    second_audit = _second_episode_audit(frozen)
    if second_audit["errors"]:
        raise V228ExactSpanError("second episode salvage audit failed")
    salvage_path = root / "second-episode-salvage.json"
    _write_stable_time(
        salvage_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "predecessor_output": lineage["records"]["output"],
            "status_rule": (
                "empty schema-valid rows may retain no_signal or "
                "excluded_source_context; no semantic relabeling"
            ),
            **second_audit["sanitized"],
            "production_mutated": False,
        },
        "created_at",
    )
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
        "semantic_delta": "select opaque exact source-unit range or drop",
        "predecessor_calls_replayed": False,
        "predecessor_second_episode_reused": True,
        "development_diagnostic_only": True,
        "production_protocol_additional_turns": 0,
        "support_alignment_allowed_after_pass": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "direct_lineage": lineage["records"],
        "second_episode_salvage": _record(salvage_path),
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
        "source_unit_count": len(units),
        "privacy": "private_source_units_and_output_sanitized_metrics_only",
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
        "second_episode_salvage": _record(salvage_path),
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


def _load_frozen_after_request(
    root: Path,
    lineage: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    predecessor_frozen = lineage["predecessor_frozen"]
    request = _load_json(paths["input"], "v228 input")
    return {
        "root": root,
        "paths": paths,
        "request": request,
        "schema": _load_json(paths["schema"], "v228 schema"),
        "source": predecessor_frozen["input"]["repair"]["source_unit"],
        "source_units": request["source_units_private"],
        "repair_packet": predecessor_frozen["input"]["repair"],
        "repair_source": predecessor_frozen["input"][
            "repair_normalization_source"
        ],
        "predecessor_input": predecessor_frozen["input"],
        "predecessor_output": lineage["output"],
        "predecessor_schema": predecessor_frozen["schema"],
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
        sidecar = _load_json(paths["sidecar"], "v228 sidecar")
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
        "terminal_reason": "v228_exact_span_diagnostic_failed",
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


async def run_v228(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v228 terminal")
    frozen = freeze_v228(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise V228ExactSpanError("v228 launch exists; replay prohibited")
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
            "predecessor_calls_replayed": False,
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
            raise V228ExactSpanError("v228 turn did not complete")
        sidecar = _load_json(frozen["paths"]["sidecar"], "v228 sidecar")
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
            raise V228ExactSpanError("v228 measured sidecar contract failed")
        projection = _project_repair(result.output, frozen)
        repair_normalized, repair_diagnostics = _normalize_repair(
            projection, frozen
        )
        second_audit = _second_episode_audit(frozen)
        normalized = {
            "repair": {
                "action": projection["action"],
                "start_unit_id": projection["start_unit_id"],
                "end_unit_id": projection["end_unit_id"],
                "normalized": repair_normalized,
            },
            "second_episode": second_audit["normalized"],
        }
        diagnostics = {
            "repair": repair_diagnostics,
            "second_episode": second_audit["diagnostics"],
        }
        _write_immutable(
            frozen["paths"]["normalized"],
            {"normalized": normalized, "diagnostics": diagnostics},
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
                "v228_exact_span_protocol_passed"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "terminal_reason": (
                "v228_exact_span_structural_gate_passed"
                if passed
                else "v228_exact_span_quality_proxy_not_passed"
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
            return _load_json(root / "terminal.json", "v228 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v228 exact-span repair")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v228(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v228(
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
