from __future__ import annotations

"""Config-driven compact protocol for a source-complete global event owner."""

import argparse
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import tiktoken

from . import app_server_configured_experiment as configured
from . import app_server_event_set_owner_experiment as owner
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .util import now_iso


CONFIG_VERSION = "pif_app_server_compact_owner_protocol_v1"
LOCK_VERSION = "pif_app_server_compact_owner_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_compact_owner_terminal_v1"
ARCHITECTURE_ID = "full_schema_proposal_then_source_complete_columnar_global_owner"
TURN_NAME = "source_complete_columnar_global_owner"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PINNED_CODEX = configured.PINNED_CODEX
MAX_SERIALIZED_TOKENS = 7000
MAX_FIXED_INPUT_OVERHEAD_TOKENS = 19563
MIN_OUTPUT_HEADROOM_TOKENS = 2800

INPUT_CORE_FIELDS = (
    "event_type",
    "actor_name",
    "actor_type",
    "speaker_name",
    "speaker_role",
    "reported_actor_name",
    "reported_actor_type",
    "source_context_kind",
    "target_concept",
    "claim_text",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "metric_raw_text",
)
FULL_EVENT_FIELDS = (
    "event_type",
    "event_subtype",
    "claim_type",
    "actor_name",
    "actor_type",
    "speaker_name",
    "speaker_role",
    "reported_actor_name",
    "reported_actor_type",
    "source_context_kind",
    "target_concept",
    "claim_text",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "counterclaim",
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
    "signal_reason",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "confidence",
    "evidence_start_unit_id",
    "evidence_end_unit_id",
)
SHORT_KEYS = tuple("abcdefghijklmnopqrstuvwxyzABCD")
SHORT_TO_FULL = dict(zip(SHORT_KEYS, FULL_EVENT_FIELDS))
ACTION_TO_LONG = {"k": "keep", "d": "drop", "r": "replace", "n": "add"}

INSTRUCTIONS = """You are the blind global semantic owner of a complete podcast event set.

The compact packet is source-complete. e is episode id. s contains [segment_id, [[unit_id, exact_text], ...]]. f is the ordered list of material base-event fields. b contains [base_event_id, segment_id, start_unit_id, end_unit_id, field_values]. Re-read every source unit; base events are proposals, not truth.

Return ops. Each op has s=segment, a=k keep/d drop/r replace/n add, i=input base IDs, and o=full replacement events. Account for every base ID exactly once in a non-add op. Keep preserves complete base events byte-for-byte. Drop removes them. Replace may merge, split, or rewrite. Add has no input IDs. Operations cannot cross segments.

Replacement keys map in order as follows: a event_type; b event_subtype; c claim_type; d actor_name; e actor_type; f speaker_name; g speaker_role; h reported_actor_name; i reported_actor_type; j source_context_kind; k target_concept; l claim_text; m stance; n certainty; o temporal_horizon; p causal_mechanism; q counterclaim; r metric_value; s metric_unit; t metric_comparator; u metric_direction; v metric_raw_text; w signal_reason; x model_names; y product_names; z organizations; A people; B confidence; C evidence_start_unit_id; D evidence_end_unit_id.

Make all semantic decisions from the source without references, expected counts, topics, confidence voting, keywords, regex rules, embeddings, tools, or external context. Preserve independently truth-valued event boundaries and every material field. Evidence must be contiguous and entail the event. Every nonempty metric field must be a literal evidence substring. Return schema-valid JSON only."""


class CompactOwnerError(RuntimeError):
    pass


class CompactOwnerStop(CompactOwnerError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompactOwnerError(f"cannot read {label}") from exc


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise CompactOwnerError("frozen direct-lineage artifact drifted")


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
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(owner.__file__).resolve(),
                Path(tiktoken.__file__).resolve(),
                *owner._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "compact config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("model") != "gpt-5.6-sol"
        or value.get("effort") != "low"
        or value.get("retry_count") != 0
        or value.get("new_turn_total_tokens_maximum") != 29400
        or value.get("adopted_base_total_tokens") != 44474
        or value.get("combined_total_tokens_maximum") != 73874
        or value.get("minimum_remaining_reserve_percent") != 20
        or value.get("quota_points_per_million_tokens") != 17
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_amortized_context_tokens") != 600538
        or value.get("production_scale") != 30
        or value.get("baseline_total_tokens") != 10065426
        or value.get("serialized_tokens_maximum") != MAX_SERIALIZED_TOKENS
        or value.get("fixed_input_overhead_tokens_maximum")
        != MAX_FIXED_INPUT_OVERHEAD_TOKENS
        or value.get("minimum_output_headroom_tokens") != MIN_OUTPUT_HEADROOM_TOKENS
    ):
        raise CompactOwnerError("compact config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise CompactOwnerError("compact output root drifted")
    for key in (*owner.LINEAGE_KEYS, "prelaunch_nonacceptance", "protocol_design"):
        normalize_record(value.get(key) or {})
    projected = (
        value["production_amortized_context_tokens"]
        + value["combined_total_tokens_maximum"] * value["production_scale"]
    ) / value["baseline_total_tokens"]
    if projected > 0.28:
        raise CompactOwnerError("compact production ratio exceeds 0.28")
    return value


def _packet(
    source: Mapping[str, Any],
    base_output: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    _old_packet, catalog, _omitted = owner._build_packet(  # noqa: SLF001
        source, base_output, provenance
    )
    segments = [
        [
            str(row["segment_id"]),
            [[str(unit["unit_id"]), str(unit["text"])] for unit in row["units"]],
        ]
        for row in source["segments"]
    ]
    base_events = []
    for event_id in sorted(catalog):
        row = catalog[event_id]
        event = row["event"]
        record = row["provenance"]
        base_events.append(
            [
                event_id.replace("S", "B", 1).replace("E", "", 1),
                row["segment_id"],
                record["evidence_start_unit_id"],
                record["evidence_end_unit_id"],
                [event[field] for field in INPUT_CORE_FIELDS],
            ]
        )
    compact_catalog = {
        event_id.replace("S", "B", 1).replace("E", "", 1): row
        for event_id, row in catalog.items()
    }
    return {
        "e": str(source["episode_id"]),
        "f": list(INPUT_CORE_FIELDS),
        "s": segments,
        "b": base_events,
    }, compact_catalog


def _schema(
    source: Mapping[str, Any],
    projection_schema: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    source_event = projection_schema["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]
    unit_ids = [
        str(unit["unit_id"])
        for segment in source["segments"]
        for unit in segment["units"]
    ]
    replacement_properties = {}
    for short, full in SHORT_TO_FULL.items():
        replacement_properties[short] = copy.deepcopy(source_event["properties"][full])
    replacement_properties["C"] = {"type": "string", "enum": unit_ids}
    replacement_properties["D"] = {"type": "string", "enum": unit_ids}
    replacement = {
        "type": "object",
        "additionalProperties": False,
        "required": list(SHORT_KEYS),
        "properties": replacement_properties,
    }
    event_ids = sorted(catalog)
    operation = {
        "type": "object",
        "additionalProperties": False,
        "required": ["s", "a", "i", "o"],
        "properties": {
            "s": {
                "type": "string",
                "enum": [str(row["segment_id"]) for row in source["segments"]],
            },
            "a": {"type": "string", "enum": list(ACTION_TO_LONG)},
            "i": {
                "type": "array",
                "minItems": 0,
                "maxItems": len(event_ids),
                "items": {"type": "string", "enum": event_ids},
            },
            "o": {
                "type": "array",
                "minItems": 0,
                "maxItems": 32,
                "items": replacement,
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["e", "ops"],
        "properties": {
            "e": {"type": "string", "enum": [str(source["episode_id"])]},
            "ops": {"type": "array", "minItems": 1, "maxItems": 45, "items": operation},
        },
    }


def _translate_output(output: Mapping[str, Any]) -> dict[str, Any]:
    operations = []
    for row in output.get("ops") or []:
        action = ACTION_TO_LONG.get(str(row.get("a")))
        if action is None:
            raise CompactOwnerError("compact action is invalid")
        replacements = []
        for raw in row.get("o") or []:
            if set(raw) != set(SHORT_TO_FULL):
                raise CompactOwnerError("compact replacement field coverage drifted")
            replacements.append({full: raw[short] for short, full in SHORT_TO_FULL.items()})
        operations.append(
            {
                "segment_id": row.get("s"),
                "action": action,
                "input_event_ids": list(row.get("i") or []),
                "replacement_events": replacements,
                "rationale": "compact protocol semantic operation",
            }
        )
    return {"episode_id": output.get("e"), "operations": operations}


def _token_count(*texts: str) -> int:
    encoding = tiktoken.get_encoding("o200k_base")
    return sum(len(encoding.encode(text)) for text in texts)


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
    if (root / "runtime-lock.json").exists():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in (*owner.LINEAGE_KEYS, "prelaunch_nonacceptance", "protocol_design"):
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source")
    base_output = _load_json(Path(config["base_output"]["path"]), "base output")
    provenance = _load_json(Path(config["base_provenance"]["path"]), "provenance")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    packet, catalog = _packet(source, base_output, provenance)
    schema = _schema(source, projection_schema, catalog)
    prompt = "# Source-complete columnar event-owner packet\n" + _canonical_json(packet) + "\n"
    base_text = INSTRUCTIONS + "\n"
    schema_text = _canonical_json(schema)
    serialized_tokens = _token_count(prompt, base_text, schema_text)
    projected_input_tokens = serialized_tokens + config[
        "fixed_input_overhead_tokens_maximum"
    ]
    output_headroom = config["new_turn_total_tokens_maximum"] - projected_input_tokens
    if serialized_tokens > config["serialized_tokens_maximum"]:
        raise CompactOwnerError("compact serialized token cap exceeded")
    if output_headroom < config["minimum_output_headroom_tokens"]:
        raise CompactOwnerError("compact output headroom is insufficient")
    turn = _turn_paths(root)
    configured._write_immutable(turn["input"], packet)  # noqa: SLF001
    configured._write_private_text(turn["prompt"], prompt)  # noqa: SLF001
    configured._write_private_text(turn["base"], base_text)  # noqa: SLF001
    configured._write_immutable(turn["schema"], schema)  # noqa: SLF001
    translated_schema = owner._owner_schema(  # noqa: SLF001
        source, projection_schema, catalog
    )
    configured._write_immutable(root / "translated-owner-schema.json", translated_schema)  # noqa: SLF001
    configured._write_immutable(  # noqa: SLF001
        root / "token-envelope-audit.json",
        {
            "schema_version": "pif_compact_owner_token_envelope_v1",
            "serialized_o200k_tokens": serialized_tokens,
            "serialized_tokens_maximum": config["serialized_tokens_maximum"],
            "fixed_input_overhead_tokens_maximum": config[
                "fixed_input_overhead_tokens_maximum"
            ],
            "projected_input_tokens_maximum": projected_input_tokens,
            "minimum_output_headroom_tokens": config["minimum_output_headroom_tokens"],
            "projected_output_headroom_tokens": output_headroom,
            "source_unit_count": sum(len(row[1]) for row in packet["s"]),
            "base_event_count": len(catalog),
            "reference_visible_to_model": False,
            "target_count_visible_to_model": False,
            "semantic_source_units_pruned": 0,
        },
    )
    capacity = configured._capacity_policy(root, _capacity_config(config))  # noqa: SLF001
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
        "direct_lineage": [
            copy.deepcopy(config[key])
            for key in (*owner.LINEAGE_KEYS, "prelaunch_nonacceptance", "protocol_design")
        ],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "static_request": [
            _record(turn[name], cache=cache) for name in ("input", "prompt", "base", "schema")
        ],
        "translated_validation_schema": _record(
            root / "translated-owner-schema.json", cache=cache
        ),
        "token_envelope": _record(root / "token-envelope-audit.json", cache=cache),
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    configured._write_immutable(  # noqa: SLF001
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "closure")
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
        raise CompactOwnerError("compact runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise CompactOwnerError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in (*owner.LINEAGE_KEYS, "prelaunch_nonacceptance", "protocol_design"):
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source")
    base_output = _load_json(Path(config["base_output"]["path"]), "base output")
    provenance = _load_json(Path(config["base_provenance"]["path"]), "provenance")
    packet, catalog = _packet(source, base_output, provenance)
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": source,
        "base_output": base_output,
        "catalog": catalog,
        "packet": packet,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    sidecar = frozen["turn"]["sidecar"]
    usage = {field: 0 for field in configured.USAGE_FIELDS}
    unknown = 0
    if sidecar.exists():
        try:
            usage = owner._sidecar_usage(sidecar, frozen["config"])  # noqa: SLF001
        except Exception:
            unknown = 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "compact_owner_structural_or_cost_gate_not_passed"
            if isinstance(exc, (owner.EventSetOwnerOutputError, CompactOwnerStop))
            else "infrastructure_or_model_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": 1 if sidecar.exists() else 0,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "measured_usage": usage,
        "unknown_usage_attempt_count": unknown,
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


async def run_experiment(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_experiment(config_path)
    try:
        verify_frozen(root)
        turn = frozen["turn"]
        if not turn["sidecar"].exists() and (
            turn["capacity"].exists() or turn["output"].exists()
        ):
            raise CompactOwnerError("attempt artifact exists without a sidecar")
        if not (root / "launch-receipt.json").exists():
            configured._write_stable_time(  # noqa: SLF001
                root / "launch-receipt.json",
                {
                    "schema_version": "pif_compact_owner_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
                    "runtime_lock": _record(frozen["runtime_lock"]),
                    "runtime_closure": _record(frozen["runtime_closure"]),
                    "config": _record(config_path),
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
                "launched_at",
            )
        verify_frozen(root)
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=frozen["config"]["model"],
                effort=frozen["config"]["effort"],
                base_instructions=turn["base"].read_text(encoding="utf-8"),
                prompt=turn["prompt"].read_text(encoding="utf-8"),
                output_schema=_load_json(turn["schema"], "compact schema"),
                cwd=PROJECT_ROOT,
                sidecar_path=turn["sidecar"],
                output_path=turn["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=turn["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise CompactOwnerError("compact owner turn did not complete")
        usage = owner._sidecar_usage(turn["sidecar"], frozen["config"])  # noqa: SLF001
        if usage["total_tokens"] > frozen["config"]["new_turn_total_tokens_maximum"]:
            raise CompactOwnerStop("compact owner exceeded token bound")
        translated = _translate_output(result.output)
        owner_frozen = {
            "turn": {"schema": _translated_schema_path(root, frozen)},
            "source": frozen["source"],
            "catalog": frozen["catalog"],
            "base_output": frozen["base_output"],
        }
        normalized, provenance, diagnostics = owner.validate_and_project_output(
            translated, owner_frozen
        )
        configured._write_immutable(root / "translated-output.private.json", translated)  # noqa: SLF001
        configured._write_immutable(root / "normalized-output.private.json", normalized)  # noqa: SLF001
        configured._write_immutable(root / "evidence-provenance.private.json", provenance)  # noqa: SLF001
        configured._write_immutable(root / "diagnostics.private.json", diagnostics)  # noqa: SLF001
        combined = frozen["config"]["adopted_base_total_tokens"] + usage["total_tokens"]
        ratio = (
            frozen["config"]["production_amortized_context_tokens"]
            + combined * frozen["config"]["production_scale"]
        ) / frozen["config"]["baseline_total_tokens"]
        if combined > frozen["config"]["combined_total_tokens_maximum"] or ratio > 0.28:
            raise CompactOwnerStop("compact owner production cost gate failed")
        gate = {
            "schema_version": "pif_compact_owner_structural_gate_v1",
            "passed": True,
            "usage": usage,
            "combined_total_tokens": combined,
            "production_amortized_total_token_ratio": round(ratio, 6),
            "all_base_events_accounted_exactly_once": True,
            "all_source_units_presented": True,
            "semantic_event_count_proxy_used": False,
            "exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "event_cap_violations": 0,
            "support_alignment_authorized": True,
            "holdout_authorized": False,
            "production_mutated": False,
        }
        configured._write_immutable(root / "architecture-structural-gate.json", gate)  # noqa: SLF001
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "compact_owner_structural_cost_passed",
            "terminal_reason": "compact_owner_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": round(ratio, 6),
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(root / "architecture-structural-gate.json"),
            "normalized_output": _record(root / "normalized-output.private.json"),
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _failure_terminal(root, frozen, exc)


def _translated_schema_path(root: Path, frozen: Mapping[str, Any]) -> Path:
    path = root / "translated-owner-schema.json"
    if not path.is_file():
        raise CompactOwnerError("translated validation schema is missing")
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        frozen = freeze_experiment(args.config)
        print(_canonical_json({"state": "frozen", "root": str(frozen["root"])}))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(args.config.expanduser().resolve().parent)
        print(_canonical_json({"state": "verified", "root": str(frozen["root"])}))
        return 0
    terminal = asyncio.run(run_experiment(args.config))
    print(_canonical_json(terminal))
    return 0 if terminal.get("support_alignment_authorized") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
