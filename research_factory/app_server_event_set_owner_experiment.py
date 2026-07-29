from __future__ import annotations

"""Run a compact, config-driven global event-set owner experiment."""

import argparse
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import codex_app_server
from .app_server_capacity_reserve import (
    ReserveCapacityError,
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


CONFIG_VERSION = "pif_app_server_event_set_owner_experiment_v1"
LOCK_VERSION = "pif_app_server_event_set_owner_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_event_set_owner_terminal_v1"
ARCHITECTURE_ID = "full_schema_proposal_then_compact_source_unit_global_owner"
TURN_NAME = "compact_source_unit_global_owner"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PINNED_CODEX = configured.PINNED_CODEX
LINEAGE_KEYS = (
    "source_input",
    "base_output",
    "base_provenance",
    "projection_schema",
    "base_instructions",
    "base_runtime_lock",
    "base_terminal",
    "base_alignment_score",
    "architecture_comparison",
    "rejected_adoption_score",
    "rejected_adoption_terminal",
    "runtime_verifier_benchmark",
    "architecture_design",
    "architecture_ranking",
)

OWNER_INSTRUCTIONS = """You are the blind episode-level owner of a complete event set.

You receive every source unit and a full-schema event proposal from an independent LLM. Re-read every source unit. Treat every proposed event as fallible and make one complete transformation over the event set without using a reference, expected count, topic list, or target answer.

Every base event ID must occur exactly once in one non-add operation. A keep operation preserves those events byte-for-byte. A drop operation removes them. A replace operation may merge, split, or fully rewrite its input events and must return every replacement as a complete full-schema event. An add operation has no input IDs and returns genuinely missed events. Operations may contain IDs from only one segment.

Preserve independently truth-valued event boundaries and all material semantics, including actor, speaker, reported actor, attribution, target, claim, stance, certainty, temporal horizon, causal mechanism, event type, and metric applicability. Use keep when the proposal is already correct. Every replacement evidence range must be contiguous within one segment and entail every material field. Every nonempty metric field must be a literal source-evidence substring.

Do not use keywords, regex rules, embeddings, semantic heuristics, confidence voting, or reference-aware hints. Return only schema-valid JSON."""


class EventSetOwnerError(RuntimeError):
    pass


class EventSetOwnerOutputError(EventSetOwnerError):
    pass


class EventSetOwnerStop(EventSetOwnerError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventSetOwnerError(f"cannot read {label}") from exc


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise EventSetOwnerError("frozen direct-lineage artifact drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted({Path(__file__).resolve(), *configured._runtime_files()}, key=str))  # noqa: SLF001


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "experiment config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("retry_count") != 0
        or value.get("model") != "gpt-5.6-sol"
        or value.get("effort") != "low"
        or value.get("new_turn_total_tokens_maximum") != 28000
        or value.get("adopted_base_total_tokens") != 44474
        or value.get("combined_total_tokens_maximum") != 72474
        or value.get("minimum_remaining_reserve_percent") != 20
        or value.get("quota_points_per_million_tokens") != 17
        or value.get("production_amortized_context_tokens") != 600538
        or value.get("production_scale") != 30
        or value.get("baseline_total_tokens") != 10065426
        or value.get("prompt_bytes_maximum") != 50000
        or value.get("schema_bytes_maximum") != 6000
    ):
        raise EventSetOwnerError("experiment config contract drifted")
    output_root = Path(str(value.get("output_root") or "")).expanduser().resolve()
    if output_root != config_path.parent.resolve():
        raise EventSetOwnerError("experiment output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    projected = (
        int(value["production_amortized_context_tokens"])
        + int(value["combined_total_tokens_maximum"]) * int(value["production_scale"])
    ) / int(value["baseline_total_tokens"])
    if projected > 0.28:
        raise EventSetOwnerError("projected production token ratio exceeds 0.28")
    return value


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == []


def _build_packet(
    source: Mapping[str, Any],
    base_output: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], int]:
    source_segments = list(source.get("segments") or [])
    base_segments = list(base_output.get("segments") or [])
    if (
        source.get("episode_id") != base_output.get("episode_id")
        or [row.get("segment_id") for row in source_segments]
        != [row.get("segment_id") for row in base_segments]
    ):
        raise EventSetOwnerError("base episode or segment order drifted")
    provenance_rows = list(provenance.get("events") or [])
    provenance_by_key = {
        (str(row.get("segment_id")), int(row.get("event_index"))): row
        for row in provenance_rows
    }
    catalog: dict[str, dict[str, Any]] = {}
    omitted_empty_fields = 0
    packet_segments = []
    packet_events = []
    for segment_position, (source_segment, base_segment) in enumerate(
        zip(source_segments, base_segments)
    ):
        segment_id = str(source_segment["segment_id"])
        packet_segments.append(
            {
                "segment_id": segment_id,
                "source_units": [
                    {"unit_id": str(unit["unit_id"]), "text": str(unit["text"])}
                    for unit in source_segment["units"]
                ],
            }
        )
        compact_events = []
        for event_position, original in enumerate(base_segment.get("events") or []):
            event_id = f"S{segment_position}E{event_position:03d}"
            provenance_row = provenance_by_key.get((segment_id, event_position))
            if provenance_row is None:
                raise EventSetOwnerError("base event provenance is incomplete")
            compact = {}
            for key, item in dict(original).items():
                if key in {"evidence", "window_id"}:
                    continue
                if _is_empty(item):
                    omitted_empty_fields += 1
                    continue
                compact[key] = item
            compact.update(
                {
                    "event_id": event_id,
                    "evidence_start_unit_id": provenance_row["evidence_start_unit_id"],
                    "evidence_end_unit_id": provenance_row["evidence_end_unit_id"],
                }
            )
            compact_events.append(compact)
            catalog[event_id] = {
                "segment_id": segment_id,
                "event_position": event_position,
                "event": copy.deepcopy(dict(original)),
                "provenance": copy.deepcopy(dict(provenance_row)),
            }
        packet_events.append({"segment_id": segment_id, "events": compact_events})
    if len(catalog) != len(provenance_rows):
        raise EventSetOwnerError("base output and provenance counts differ")
    return (
        {
            "episode_id": str(source["episode_id"]),
            "segments": packet_segments,
            "base_events": packet_events,
        },
        catalog,
        omitted_empty_fields,
    )


def _owner_schema(
    source: Mapping[str, Any],
    projection_schema: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    event = copy.deepcopy(
        projection_schema["properties"]["segments"]["items"]["properties"]["events"][
            "items"
        ]
    )
    unit_ids = [
        str(unit["unit_id"])
        for segment in source["segments"]
        for unit in segment["units"]
    ]
    for field in ("evidence_start_unit_id", "evidence_end_unit_id"):
        event["properties"][field] = {"type": "string", "enum": unit_ids}
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    event_ids = sorted(catalog)
    operation = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "action",
            "input_event_ids",
            "replacement_events",
            "rationale",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": segment_ids},
            "action": {"type": "string", "enum": ["keep", "drop", "replace", "add"]},
            "input_event_ids": {
                "type": "array",
                "minItems": 0,
                "maxItems": len(event_ids),
                "items": {"type": "string", "enum": event_ids},
            },
            "replacement_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": 32,
                "items": event,
            },
            "rationale": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "operations"],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "operations": {
                "type": "array",
                "minItems": 1,
                "maxItems": 45,
                "items": operation,
            },
        },
    }


def _evidence(
    source_segment: Mapping[str, Any], start_id: str, end_id: str
) -> tuple[str, int, int, int]:
    return configured._evidence(source_segment, start_id, end_id)  # noqa: SLF001


def _validate_replacement(
    raw_event: Mapping[str, Any],
    segment: Mapping[str, Any],
    event_schema: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        _validate_schema(event_schema, raw_event, path="$.replacement_event")
    except (ValidationError, TypeError, ValueError) as exc:
        raise EventSetOwnerOutputError("replacement event schema failed") from exc
    event = copy.deepcopy(dict(raw_event))
    start_id = str(event.pop("evidence_start_unit_id"))
    end_id = str(event.pop("evidence_end_unit_id"))
    evidence, start_char, end_char, window_id = _evidence(segment, start_id, end_id)
    metric_fields = (
        "metric_value",
        "metric_unit",
        "metric_comparator",
        "metric_raw_text",
    )
    metric_values = [str(event.get(field) or "") for field in metric_fields]
    if any(value and value not in evidence for value in metric_values):
        raise EventSetOwnerOutputError("replacement metric is not literal evidence")
    has_metric = any(metric_values)
    if has_metric == (event.get("metric_direction") == "not_applicable"):
        raise EventSetOwnerOutputError("replacement metric applicability drifted")
    event["window_id"] = window_id
    event["evidence"] = evidence
    return event, {
        "evidence_start_unit_id": start_id,
        "evidence_end_unit_id": end_id,
        "start_char": start_char,
        "end_char": end_char,
        "window_id": window_id,
        "evidence_sha256": sha256_text(evidence),
    }


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    schema = _load_json(frozen["turn"]["schema"], "owner schema")
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise EventSetOwnerOutputError("owner output schema failed") from exc
    source = frozen["source"]
    catalog = frozen["catalog"]
    if output.get("episode_id") != source.get("episode_id"):
        raise EventSetOwnerOutputError("owner episode id drifted")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    projected: dict[str, list[dict[str, Any]]] = {key: [] for key in source_by_id}
    provenance: list[dict[str, Any]] = []
    accounted: list[str] = []
    operation_counts = {name: 0 for name in ("keep", "drop", "replace", "add")}
    event_schema = schema["properties"]["operations"]["items"]["properties"][
        "replacement_events"
    ]["items"]
    for operation in output["operations"]:
        action = str(operation["action"])
        segment_id = str(operation["segment_id"])
        input_ids = [str(item) for item in operation["input_event_ids"]]
        replacements = list(operation["replacement_events"])
        operation_counts[action] += 1
        if action == "add":
            if input_ids or not replacements:
                raise EventSetOwnerOutputError("add operation contract failed")
        else:
            if not input_ids:
                raise EventSetOwnerOutputError("non-add operation has no input events")
            if any(catalog.get(item, {}).get("segment_id") != segment_id for item in input_ids):
                raise EventSetOwnerOutputError("operation moved input across segments")
            accounted.extend(input_ids)
        if action in {"keep", "drop"} and replacements:
            raise EventSetOwnerOutputError("keep or drop returned replacements")
        if action == "replace" and not replacements:
            raise EventSetOwnerOutputError("replace operation returned no event")
        if action == "keep":
            for event_id in input_ids:
                projected[segment_id].append(copy.deepcopy(catalog[event_id]["event"]))
                provenance.append(copy.deepcopy(catalog[event_id]["provenance"]))
        elif action in {"replace", "add"}:
            for raw_event in replacements:
                event, record = _validate_replacement(
                    raw_event, source_by_id[segment_id], event_schema
                )
                projected[segment_id].append(event)
                provenance.append({"segment_id": segment_id, **record})
    if set(accounted) != set(catalog) or len(accounted) != len(set(accounted)):
        raise EventSetOwnerOutputError("base event accounting is not an exact partition")
    normalized_rows = []
    projected_provenance = []
    seen = set()
    for source_segment, base_segment in zip(source["segments"], frozen["base_output"]["segments"]):
        segment_id = str(source_segment["segment_id"])
        events = projected[segment_id]
        units = {str(unit["unit_id"]): int(unit["start_char"]) for unit in source_segment["units"]}
        records = [row for row in provenance if str(row["segment_id"]) == segment_id]
        paired = sorted(
            zip(events, records),
            key=lambda pair: (
                int(pair[1].get("start_char", 0)),
                int(pair[1].get("end_char", 0)),
            ),
        )
        events = [pair[0] for pair in paired]
        records = [pair[1] for pair in paired]
        if len(events) > 32:
            raise EventSetOwnerOutputError("projected event cap exceeded")
        for event_position, (event, record) in enumerate(zip(events, records)):
            identity = _canonical_json(
                {field: event.get(field) for field in configured.IDENTITY_FIELDS}
            )
            if identity in seen:
                raise EventSetOwnerOutputError("projected exact identity duplicate")
            seen.add(identity)
            projected_provenance.append(
                {"segment_id": segment_id, "event_index": event_position, **record}
            )
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": "coded" if events else "no_signal",
                "segment_source_context": base_segment["segment_source_context"],
                "no_signal_reason": "" if events else base_segment["no_signal_reason"],
                "events": events,
            }
        )
    return (
        {"episode_id": source["episode_id"], "segments": normalized_rows},
        {
            "schema_version": "pif_event_set_owner_provenance_v1",
            "episode_id": source["episode_id"],
            "events": projected_provenance,
        },
        {
            "schema_version": "pif_event_set_owner_diagnostics_v1",
            "base_event_count": len(catalog),
            "final_event_count": sum(len(row["events"]) for row in normalized_rows),
            "accounted_base_event_count": len(accounted),
            "operation_counts": operation_counts,
            "all_source_units_presented": True,
            "semantic_event_count_proxy_used": False,
        },
    )


def _capacity_config(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "experiment_id": config["experiment_id"],
        "capacity_maximum_total_tokens_per_turn": config[
            "new_turn_total_tokens_maximum"
        ],
        "quota_points_per_million_tokens": config["quota_points_per_million_tokens"],
        "minimum_remaining_reserve_percent": config[
            "minimum_remaining_reserve_percent"
        ],
        "turns": [{"name": TURN_NAME}],
    }


def freeze_experiment(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    base_output = _load_json(Path(config["base_output"]["path"]), "base output")
    provenance = _load_json(Path(config["base_provenance"]["path"]), "base provenance")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    packet, catalog, omitted = _build_packet(source, base_output, provenance)
    schema = _owner_schema(source, projection_schema, catalog)
    prompt = "# Complete source-unit packet and base full-schema proposal\n" + _canonical_json(packet) + "\n"
    base_instructions = Path(config["base_instructions"]["path"]).read_text(
        encoding="utf-8"
    )
    base_text = base_instructions + "\n\n# Compact global event-set owner\n" + OWNER_INSTRUCTIONS + "\n"
    if len(prompt.encode("utf-8")) > config["prompt_bytes_maximum"]:
        raise EventSetOwnerError("frozen prompt exceeds byte cap")
    schema_text = _canonical_json(schema)
    if len(schema_text.encode("utf-8")) > config["schema_bytes_maximum"]:
        raise EventSetOwnerError("frozen schema exceeds byte cap")
    turn = _turn_paths(root)
    _write_immutable(turn["input"], packet)
    _write_private_text(turn["prompt"], prompt)
    _write_private_text(turn["base"], base_text)
    _write_immutable(turn["schema"], schema)
    _write_immutable(
        root / "serialization-audit.json",
        {
            "schema_version": "pif_event_set_owner_serialization_audit_v1",
            "source_unit_count": sum(len(row["source_units"]) for row in packet["segments"]),
            "base_event_count": len(catalog),
            "omitted_empty_base_event_field_count": omitted,
            "semantic_fields_omitted_when_nonempty": 0,
            "prompt_bytes": len(prompt.encode("utf-8")),
            "prompt_bytes_maximum": config["prompt_bytes_maximum"],
            "schema_bytes": len(schema_text.encode("utf-8")),
            "schema_bytes_maximum": config["schema_bytes_maximum"],
            "reference_visible_to_model": False,
            "target_count_visible_to_model": False,
        },
    )
    capacity_paths = configured._capacity_policy(root, _capacity_config(config))  # noqa: SLF001
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "declared_turn_count": 1,
        "retry_count": 0,
        "new_turn_total_tokens_maximum": config["new_turn_total_tokens_maximum"],
        "combined_total_tokens_maximum": config["combined_total_tokens_maximum"],
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
        "capacity_audit": _record(capacity_paths["audit"], cache=cache),
        "capacity_policy": _record(capacity_paths["policy"], cache=cache),
        "static_request": [
            _record(turn[name], cache=cache)
            for name in ("input", "prompt", "base", "schema")
        ],
        "serialization_audit": _record(root / "serialization-audit.json", cache=cache),
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "runtime closure")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "architecture_id": ARCHITECTURE_ID,
                "declared_turn_count": 1,
                "retry_count": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise EventSetOwnerError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise EventSetOwnerError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    base_output = _load_json(Path(config["base_output"]["path"]), "base output")
    provenance = _load_json(Path(config["base_provenance"]["path"]), "base provenance")
    packet, catalog, _omitted = _build_packet(source, base_output, provenance)
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        launch = _load_json(launch_path, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(launch.get(key) or {}, cache=cache)
        if launch.get("semantic_attempt_count") != 1 or launch.get("retry_count") != 0:
            raise EventSetOwnerError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": source,
        "base_output": base_output,
        "packet": packet,
        "catalog": catalog,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _sidecar_usage(path: Path, config: Mapping[str, Any]) -> dict[str, int]:
    return configured._usage_from_sidecar(  # noqa: SLF001
        path, {"model": config["model"], "effort": config["effort"]}
    )


def _terminal_usage(root: Path, config: Mapping[str, Any]) -> tuple[dict[str, int], int]:
    sidecar = _turn_paths(root)["sidecar"]
    if not sidecar.exists():
        return {field: 0 for field in configured.USAGE_FIELDS}, 0
    try:
        return _sidecar_usage(sidecar, config), 0
    except Exception:
        return {field: 0 for field in configured.USAGE_FIELDS}, 1


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    usage, unknown = _terminal_usage(root, frozen["config"])
    semantic = isinstance(exc, (EventSetOwnerOutputError, EventSetOwnerStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "event_set_owner_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_model_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": 1 if _turn_paths(root)["sidecar"].exists() else 0,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "measured_usage": usage,
        "unknown_usage_attempt_count": unknown,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return terminal


def _gate(
    frozen: Mapping[str, Any], usage: Mapping[str, int], diagnostics: Mapping[str, Any]
) -> dict[str, Any]:
    config = frozen["config"]
    combined = config["adopted_base_total_tokens"] + usage["total_tokens"]
    production_total = (
        config["production_amortized_context_tokens"]
        + combined * config["production_scale"]
    )
    ratio = production_total / config["baseline_total_tokens"]
    checks = {
        "one_turn_measured": True,
        "new_turn_tokens_lte_28000": usage["total_tokens"] <= 28000,
        "combined_tokens_lte_72474": combined <= 72474,
        "all_base_events_accounted_exactly_once": diagnostics[
            "accounted_base_event_count"
        ]
        == diagnostics["base_event_count"],
        "all_source_units_presented": diagnostics["all_source_units_presented"] is True,
        "semantic_event_count_proxy_not_used": diagnostics[
            "semantic_event_count_proxy_used"
        ]
        is False,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "exact_identity_duplicates_0": True,
        "event_cap_violations_0": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_event_set_owner_structural_gate_v1",
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "adopted_base_total_tokens": config["adopted_base_total_tokens"],
        "combined_total_tokens": combined,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "base_event_count": diagnostics["base_event_count"],
        "final_event_count": diagnostics["final_event_count"],
        "support_alignment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


async def run_experiment(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_experiment(config_path)
    try:
        verify_frozen(root)
        launch_path = root / "launch-receipt.json"
        if not launch_path.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch_path,
                {
                    "schema_version": "pif_event_set_owner_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                    "runtime_lock": _record(frozen["runtime_lock"]),
                    "runtime_closure": _record(frozen["runtime_closure"]),
                    "config": _record(config_path),
                },
                "launched_at",
            )
        verify_frozen(root)
        turn = frozen["turn"]
        if not turn["sidecar"].exists() and (
            turn["capacity"].exists() or turn["output"].exists()
        ):
            raise EventSetOwnerError("attempt artifact exists without a sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=frozen["config"]["model"],
                    effort=frozen["config"]["effort"],
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "owner schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = _sidecar_usage(turn["sidecar"], frozen["config"])
        if usage["total_tokens"] > frozen["config"]["new_turn_total_tokens_maximum"]:
            raise EventSetOwnerStop("owner turn exceeded its frozen token bound")
        output = _load_json(turn["output"], "owner output")
        normalized, provenance, diagnostics = validate_and_project_output(output, frozen)
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(frozen, usage, diagnostics)
        _write_immutable(root / "architecture-structural-gate.json", gate)
        if not gate["passed"]:
            raise EventSetOwnerStop("event-set owner structural or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "event_set_owner_structural_cost_passed",
            "terminal_reason": "event_set_owner_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(root / "architecture-structural-gate.json"),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "exact_next_action": "run the unchanged frozen support-first and two-permutation alignment gate; do not field-patch this architecture",
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        result = freeze_experiment(args.config)
        print(_canonical_json({"state": "frozen", "root": str(result["root"])}))
        return 0
    if args.command == "verify":
        result = verify_frozen(args.config.expanduser().resolve().parent)
        print(_canonical_json({"state": "verified", "root": str(result["root"])}))
        return 0
    terminal = asyncio.run(run_experiment(args.config))
    print(_canonical_json(terminal))
    return 0 if terminal.get("support_alignment_authorized") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
