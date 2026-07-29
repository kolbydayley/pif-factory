from __future__ import annotations

"""Config-driven, immutable app-server extraction experiments."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


CONFIG_VERSION = "pif_app_server_configured_experiment_v1"
LOCK_VERSION = "pif_app_server_configured_experiment_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_configured_experiment_terminal_v1"
CORE_SCHEMA_VERSION = "pif_compact_segment_event_cores_v1"
JOIN_SCHEMA_VERSION = "pif_episode_global_owner_join_v1"
CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PINNED_CODEX = (
    Path.home()
    / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin/bin/codex"
).resolve()
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
IDENTITY_FIELDS = (
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
)
CORE_FIELDS = (
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
    "evidence_start_unit_id",
    "evidence_end_unit_id",
)

CORE_INSTRUCTIONS = """You are the compact event-core discovery stage for one blind podcast episode packet.

Inspect every source unit. Work independently inside each segment before returning the complete episode packet. Discover every independently truth-valued eligible event without using a topic list, expected count, prior extraction, or reference answer. Preserve exact event boundaries and material semantics: actor, speaker, reported actor, attribution context, target, claim, stance, certainty, temporal horizon, causal mechanism, metric wording, and exact evidence-unit range.

The compact output is not a keyword inventory. Each row must already be a coherent event core. Merge only exact semantic duplicates. Evidence may span contiguous units inside one segment and must entail every material field. A metric string must be a literal contiguous substring of its evidence. Mark every supplied unit id as covered in source order. Return only schema-valid JSON."""

JOIN_INSTRUCTIONS = """You are the episode-level semantic owner and join stage.

You receive the complete blind source-unit packet plus compact event cores produced by an independent LLM stage. Re-read the source and make every final semantic decision yourself. The cores are proposals, not truth and not a reference. You may merge, split, drop, retain, or add events when the source warrants it. Account for every supplied core id exactly once, either on one final event or in dropped_core_events. A newly discovered or split child may use an empty core_ids list.

Return the complete final full-schema extraction in source order. Every material field must be entailed by the selected contiguous evidence-unit range. Keep actor, speaker, reported actor, attribution, stance, certainty, temporal horizon, metric, target, event type, and event boundary distinct. A metric string must be a literal contiguous substring of its evidence. Do not use a topic list, expected count, prior reference, keywords, regex rules, embeddings, or semantic heuristics. Return only schema-valid JSON."""


class ConfiguredExperimentError(RuntimeError):
    """The configured immutable experiment cannot proceed safely."""


class ConfiguredOutputContractError(ConfiguredExperimentError):
    """A measured LLM output violated the frozen semantic-output contract."""


class ConfiguredArchitectureStop(ConfiguredExperimentError):
    """The measured architecture failed a predeclared structural or cost gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfiguredExperimentError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise ConfiguredExperimentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise ConfiguredExperimentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise ConfiguredExperimentError("frozen artifact record drifted")


def _turn_paths(root: Path, name: str) -> dict[str, Path]:
    turn_root = root / "turns" / name.replace("_", "-")
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


def _validate_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path, "experiment config")
    turns = config.get("turns") if isinstance(config, dict) else None
    if (
        not isinstance(config, dict)
        or config.get("schema_version") != CONFIG_VERSION
        or config.get("architecture_id")
        != "compact_segment_event_cores_then_episode_global_llm_owner_join"
        or config.get("managed_chatgpt_auth_only") is not True
        or config.get("official_persistent_codex_app_server_only") is not True
        or config.get("semantic_regex_or_keyword_filtering") is not False
        or config.get("production_mutation_allowed") is not False
        or config.get("holdout_authorized") is not False
        or config.get("retry_count_per_turn") != 0
        or not isinstance(turns, list)
        or len(turns) != 2
        or [row.get("name") for row in turns]
        != ["compact_segment_event_cores", "episode_global_owner_join"]
        or any(row.get("model") != "gpt-5.6-sol" for row in turns)
        or any(row.get("effort") != "low" for row in turns)
        or [row.get("total_tokens_maximum") for row in turns] != [33000, 40000]
        or config.get("combined_total_tokens_maximum") != 73000
        or config.get("capacity_maximum_total_tokens_per_turn") != 45000
        or config.get("minimum_remaining_reserve_percent") != 20
        or config.get("quota_points_per_million_tokens") != 17
    ):
        raise ConfiguredExperimentError("experiment config contract drifted")
    output_root = Path(str(config.get("output_root") or "")).expanduser().resolve()
    if not output_root.is_absolute() or output_root != config_path.parent.resolve():
        raise ConfiguredExperimentError("experiment output root drifted")
    for key in (
        "source_input",
        "base_instructions",
        "projection_schema",
        "architecture_comparison",
        "phase_boundary",
        "verifier_benchmark",
        "v277_runtime_lock",
        "v277_terminal",
    ):
        normalize_record(config.get(key) or {})
    return config


def _source_packet(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "episode_id": source["episode_id"],
        "segments": [
            {
                "segment_id": row["segment_id"],
                "source_units": [
                    {
                        "unit_id": unit["unit_id"],
                        "window_id": unit["window_id"],
                        "text": unit["text"],
                    }
                    for unit in row["units"]
                ],
            }
            for row in source["segments"]
        ],
    }


def _core_schema(source: Mapping[str, Any], projection_schema: Mapping[str, Any]) -> dict[str, Any]:
    source_event = projection_schema["properties"]["segments"]["items"]["properties"]["events"]["items"]
    event_properties = {
        field: copy.deepcopy(source_event["properties"][field]) for field in CORE_FIELDS
    }
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in source["segments"]
        for unit in segment["units"]
    ]
    segment_ids = [str(segment["segment_id"]) for segment in source["segments"]]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "segments": {
                "type": "array",
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["segment_id", "status", "covered_unit_ids", "events"],
                    "properties": {
                        "segment_id": {"type": "string", "enum": segment_ids},
                        "status": {"type": "string", "enum": ["coded", "no_signal"]},
                        "covered_unit_ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": max(len(row["units"]) for row in source["segments"]),
                            "items": {"type": "string", "enum": all_unit_ids},
                        },
                        "events": {
                            "type": "array",
                            "minItems": 0,
                            "maxItems": 40,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": list(CORE_FIELDS),
                                "properties": event_properties,
                            },
                        },
                    },
                },
            },
        },
    }


def _join_schema(
    source: Mapping[str, Any], projection_schema: Mapping[str, Any]
) -> dict[str, Any]:
    source_segments = projection_schema["properties"]["segments"]
    source_segment = source_segments["items"]
    source_event = source_segment["properties"]["events"]["items"]
    event = copy.deepcopy(source_event)
    event["required"] = [*event["required"], "core_ids"]
    event["properties"]["core_ids"] = {
        "type": "array",
        "minItems": 0,
        "maxItems": 40,
        "items": {"type": "string", "pattern": "^S[0-9]+C[0-9]{3}$"},
    }
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": segment_ids},
            "status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(
                source_segment["properties"]["segment_source_context"]
            ),
            "no_signal_reason": copy.deepcopy(source_segment["properties"]["no_signal_reason"]),
            "events": {
                "type": "array",
                "minItems": 0,
                "maxItems": 32,
                "items": event,
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments", "dropped_core_events"],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "segments": {
                "type": "array",
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": segment,
            },
            "dropped_core_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": 80,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["core_id", "reason"],
                    "properties": {
                        "core_id": {"type": "string", "pattern": "^S[0-9]+C[0-9]{3}$"},
                        "reason": {"type": "string"},
                    },
                },
            },
        },
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    maximum = int(config["capacity_maximum_total_tokens_per_turn"])
    names = [str(row["name"]) for row in config["turns"]]
    phase_bound = maximum * len(names)
    projected = math.ceil(
        phase_bound * int(config["quota_points_per_million_tokens"]) / 1_000_000
    )
    _write_stable_time(
        audit_path,
        {
            "schema_version": CAPACITY_AUDIT_VERSION,
            "phase_id": config["experiment_id"],
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": len(names),
                "maximum_total_tokens_per_turn": maximum,
                "phase_total_token_bound": phase_bound,
                "projected_phase_quota_points": projected,
                "minimum_remaining_reserve_percent": config[
                    "minimum_remaining_reserve_percent"
                ],
            },
        },
        "created_at",
    )
    _write_stable_time(
        policy_path,
        {
            "schema_version": CAPACITY_POLICY_VERSION,
            "phase_id": config["experiment_id"],
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": names,
            "minimum_remaining_reserve_percent": config[
                "minimum_remaining_reserve_percent"
            ],
            "quota_points_per_million_tokens": config[
                "quota_points_per_million_tokens"
            ],
            "maximum_total_tokens_per_turn": maximum,
            "phase_total_token_bound": phase_bound,
            "projected_phase_quota_points": projected,
            "semantic_output_root": str(root),
            "audit": _record(audit_path),
        },
        "created_at",
    )
    reserve_module.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def freeze_experiment(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in (
        "source_input",
        "base_instructions",
        "projection_schema",
        "architecture_comparison",
        "phase_boundary",
        "verifier_benchmark",
        "v277_runtime_lock",
        "v277_terminal",
    ):
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    base = Path(config["base_instructions"]["path"]).read_text(encoding="utf-8")
    packet = _source_packet(source)
    core_schema = _core_schema(source, projection_schema)
    join_schema = _join_schema(source, projection_schema)
    core_turn = _turn_paths(root, config["turns"][0]["name"])
    join_turn = _turn_paths(root, config["turns"][1]["name"])
    _write_immutable(core_turn["input"], source)
    _write_private_text(
        core_turn["prompt"],
        "# Blind source-unit packet\n" + _canonical_json(packet) + "\n",
    )
    _write_private_text(core_turn["base"], base + "\n\n# Compact event-core stage\n" + CORE_INSTRUCTIONS + "\n")
    _write_immutable(core_turn["schema"], core_schema)
    _write_private_text(join_turn["base"], base + "\n\n# Episode global owner/join stage\n" + JOIN_INSTRUCTIONS + "\n")
    _write_immutable(join_turn["schema"], join_schema)
    capacity_paths = _capacity_policy(root, config)
    static_request = [
        _record(core_turn[name], cache=cache) for name in ("input", "prompt", "base", "schema")
    ] + [_record(join_turn[name], cache=cache) for name in ("base", "schema")]
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": config["architecture_id"],
        "declared_turn_count": 2,
        "retry_count_per_turn": 0,
        "combined_total_tokens_maximum": config["combined_total_tokens_maximum"],
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [
            copy.deepcopy(config[key])
            for key in (
                "architecture_comparison",
                "phase_boundary",
                "verifier_benchmark",
                "v277_runtime_lock",
                "v277_terminal",
                "source_input",
                "base_instructions",
                "projection_schema",
            )
        ],
        "capacity_audit": _record(capacity_paths["audit"], cache=cache),
        "capacity_policy": _record(capacity_paths["policy"], cache=cache),
        "static_request": static_request,
        "dynamic_join_request_requires_interstage_receipt": True,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "frozen_at")
    receipt = closure_receipt(lock_path, cache=ContentHashCache())
    _write_immutable(root / "runtime-lock-closure.json", receipt)
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config_path = root / "experiment-config.json"
    config = _validate_config(config_path)
    closure_path = root / "runtime-lock-closure.json"
    receipt = _load_json(closure_path, "runtime closure receipt")
    if receipt.get("schema_version") != "pif_app_server_runtime_verifier_receipt_v1":
        raise ConfiguredExperimentError("runtime closure receipt drifted")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "architecture_id": config["architecture_id"],
                "declared_turn_count": 2,
                "retry_count_per_turn": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise ConfiguredExperimentError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise ConfiguredExperimentError("pinned Codex binary drifted")
    reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        launch = _load_json(launch_path, "launch receipt")
        cache = ContentHashCache()
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(launch.get(key) or {}, cache=cache)
        if launch.get("semantic_attempt_count") != 2 or launch.get("retry_count") != 0:
            raise ConfiguredExperimentError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": closure_path,
        "capacity_policy": root / "capacity-policy.json",
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
        "projection_schema": _load_json(
            Path(config["projection_schema"]["path"]), "projection schema"
        ),
        "turns": [_turn_paths(root, row["name"]) for row in config["turns"]],
    }


def _evidence(
    source_segment: Mapping[str, Any], start_id: str, end_id: str
) -> tuple[str, int, int, int]:
    units = list(source_segment["units"])
    index = {str(unit["unit_id"]): position for position, unit in enumerate(units)}
    if start_id not in index or end_id not in index:
        raise ConfiguredOutputContractError("evidence unit belongs to another segment")
    start_index, end_index = index[start_id], index[end_id]
    if start_index > end_index:
        raise ConfiguredOutputContractError("evidence unit range is reversed")
    start_char = int(units[start_index]["start_char"])
    end_char = int(units[end_index]["end_char"])
    evidence = str(source_segment["segment_text"])[start_char:end_char]
    if not evidence:
        raise ConfiguredOutputContractError("projected evidence is empty")
    boundaries = list(source_segment["boundaries"])
    if not any(
        int(row["extract_start"]) <= start_char <= end_char <= int(row["extract_end"])
        for row in boundaries
    ):
        raise ConfiguredOutputContractError("evidence is outside fixed windows")
    owners = [
        row
        for row in boundaries
        if int(row["owner_start"]) <= start_char < int(row["owner_end"])
    ]
    if len(owners) != 1:
        raise ConfiguredOutputContractError("evidence owner window is ambiguous")
    return evidence, start_char, end_char, int(owners[0]["window_id"])


def validate_core_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    schema = _load_json(frozen["turns"][0]["schema"], "core schema")
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise ConfiguredOutputContractError("core output schema failed") from exc
    source = frozen["source"]
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    rows = list(output.get("segments") or [])
    if output.get("episode_id") != source["episode_id"] or [
        row.get("segment_id") for row in rows
    ] != segment_ids:
        raise ConfiguredOutputContractError("core episode or segment order drifted")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    packet_rows = []
    counts = {}
    seen = set()
    for segment_position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        source_segment = source_by_id[segment_id]
        expected_units = [str(unit["unit_id"]) for unit in source_segment["units"]]
        if list(row["covered_unit_ids"]) != expected_units:
            raise ConfiguredOutputContractError("core source-unit coverage drifted")
        events = list(row["events"])
        if (row["status"] == "coded") != bool(events):
            raise ConfiguredOutputContractError("core status does not match events")
        prior_start = -1
        unit_index = {unit_id: position for position, unit_id in enumerate(expected_units)}
        packet_events = []
        for event_position, raw_event in enumerate(events):
            event = copy.deepcopy(dict(raw_event))
            start_id = str(event["evidence_start_unit_id"])
            end_id = str(event["evidence_end_unit_id"])
            evidence, _start, _end, _window = _evidence(source_segment, start_id, end_id)
            if unit_index[start_id] < prior_start:
                raise ConfiguredOutputContractError("core event order drifted")
            prior_start = unit_index[start_id]
            metric = str(event.get("metric_raw_text") or "")
            if metric and metric not in evidence:
                raise ConfiguredOutputContractError("core metric is not literal evidence")
            identity = _canonical_json(event)
            if identity in seen:
                raise ConfiguredOutputContractError("core exact identity duplicate")
            seen.add(identity)
            packet_events.append(
                {
                    "core_id": f"S{segment_position}C{event_position:03d}",
                    **event,
                }
            )
        packet_rows.append({"segment_id": segment_id, "events": packet_events})
        counts[segment_id] = len(events)
    dense_id = str(source["segments"][0]["segment_id"])
    residual_id = str(source["segments"][1]["segment_id"])
    checks = {
        "all_source_units_covered": True,
        "dense_core_event_count_gte_27": counts[dense_id] >= 27,
        "nominal_no_signal_core_event_count_lte_1": counts[residual_id] <= 1,
        "core_exact_evidence_rate_1": True,
        "core_metric_grounding_error_count_0": True,
        "core_exact_identity_duplicate_count_0": True,
    }
    failed = [name for name, passed in checks.items() if not passed]
    diagnostics = {
        "schema_version": CORE_SCHEMA_VERSION,
        "checks": checks,
        "failed_checks": failed,
        "event_counts": counts,
        "core_event_count": sum(counts.values()),
    }
    if failed:
        raise ConfiguredArchitectureStop("core-stage structural gate failed")
    return {
        "schema_version": CORE_SCHEMA_VERSION,
        "episode_id": source["episode_id"],
        "segments": packet_rows,
    }, diagnostics


def _join_prompt(source: Mapping[str, Any], core_packet: Mapping[str, Any]) -> str:
    packet = {
        "source": _source_packet(source),
        "compact_event_cores": core_packet,
    }
    return "# Blind source and compact event-core packet\n" + _canonical_json(packet) + "\n"


def validate_join_output(
    output: Mapping[str, Any], core_packet: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    schema = _load_json(frozen["turns"][1]["schema"], "join schema")
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise ConfiguredOutputContractError("join output schema failed") from exc
    source = frozen["source"]
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    rows = list(output.get("segments") or [])
    if output.get("episode_id") != source["episode_id"] or [
        row.get("segment_id") for row in rows
    ] != segment_ids:
        raise ConfiguredOutputContractError("join episode or segment order drifted")
    core_to_segment = {
        str(event["core_id"]): str(segment["segment_id"])
        for segment in core_packet["segments"]
        for event in segment["events"]
    }
    all_core_ids = set(core_to_segment)
    accounted = []
    for row in rows:
        for event in row["events"]:
            core_ids = [str(item) for item in event.pop("core_ids")]
            if any(core_to_segment.get(item) != row["segment_id"] for item in core_ids):
                raise ConfiguredOutputContractError("core event moved across segments")
            accounted.extend(core_ids)
    accounted.extend(str(item["core_id"]) for item in output["dropped_core_events"])
    if set(accounted) != all_core_ids or len(accounted) != len(set(accounted)):
        raise ConfiguredOutputContractError("core-event accounting is not an exact partition")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    normalized_rows = []
    provenance = []
    seen = set()
    diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        events = list(row["events"])
        if (row["status"] == "coded") != bool(events):
            raise ConfiguredOutputContractError("join status does not match events")
        if len(events) > 32:
            raise ConfiguredOutputContractError("join event cap exceeded")
        source_segment = source_by_id[segment_id]
        units = list(source_segment["units"])
        unit_index = {str(unit["unit_id"]): position for position, unit in enumerate(units)}
        prior_start = -1
        normalized_events = []
        for event_position, raw_event in enumerate(events):
            event = copy.deepcopy(dict(raw_event))
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            evidence, start_char, end_char, window_id = _evidence(
                source_segment, start_id, end_id
            )
            if unit_index[start_id] < prior_start:
                raise ConfiguredOutputContractError("join event order drifted")
            prior_start = unit_index[start_id]
            metric_fields = (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            )
            metric_values = [str(event.get(field) or "") for field in metric_fields]
            if any(value and value not in evidence for value in metric_values):
                raise ConfiguredOutputContractError("join metric is not literal evidence")
            if any(metric_values) == (event.get("metric_direction") == "not_applicable"):
                raise ConfiguredOutputContractError("join metric applicability drifted")
            identity = _canonical_json({field: event.get(field) for field in IDENTITY_FIELDS})
            if identity in seen:
                raise ConfiguredOutputContractError("join exact identity duplicate")
            seen.add(identity)
            event["window_id"] = window_id
            event["evidence"] = evidence
            normalized_events.append(event)
            provenance.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_position,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": start_char,
                    "end_char": end_char,
                    "window_id": window_id,
                    "evidence_sha256": sha256_text(evidence),
                }
            )
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "events": normalized_events,
            }
        )
        source_count = len(units)
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source_segment["density_stratum"],
                "source_unit_count": source_count,
                "reviewed_source_unit_count": source_count,
                "event_count": len(normalized_events),
                "unresolved_count": 0,
            }
        )
    return (
        {"episode_id": source["episode_id"], "segments": normalized_rows},
        {"schema_version": JOIN_SCHEMA_VERSION, "episode_id": source["episode_id"], "events": provenance},
        {
            "schema_version": JOIN_SCHEMA_VERSION,
            "diagnostics": diagnostics,
            "core_event_count": len(all_core_ids),
            "dropped_core_event_count": len(output["dropped_core_events"]),
            "accounted_core_event_count": len(accounted),
        },
    )


def _usage_from_sidecar(path: Path, expected: Mapping[str, Any]) -> dict[str, int]:
    sidecar = _load_json(path, "turn sidecar")
    values = sidecar.get("usage")
    if not isinstance(values, Mapping):
        raise ConfiguredExperimentError("turn usage is absent")
    usage = {}
    for field in USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfiguredExperimentError("turn usage is incomplete")
        usage[field] = value
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != expected["model"]
        or sidecar.get("effort") != expected["effort"]
        or sidecar.get("error_class") is not None
        or usage["cached_input_tokens"] > usage["input_tokens"]
        or usage["reasoning_output_tokens"] > usage["output_tokens"]
        or usage["total_tokens"]
        != usage["input_tokens"] + usage["output_tokens"]
    ):
        raise ConfiguredExperimentError("measured sidecar contract failed")
    return usage


def _aggregate_usage(usages: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(row[field]) for row in usages) for field in USAGE_FIELDS}


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _terminal_usage(root: Path, config: Mapping[str, Any]) -> tuple[list[dict[str, int]], int]:
    measured = []
    unknown = 0
    for row in config["turns"]:
        paths = _turn_paths(root, row["name"])
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        if not paths["sidecar"].is_file():
            unknown += 1
            continue
        try:
            measured.append(_usage_from_sidecar(paths["sidecar"], row))
        except Exception:
            unknown += 1
    return measured, unknown


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    config = frozen["config"]
    measured, unknown = _terminal_usage(root, config)
    semantic = isinstance(exc, (ConfiguredOutputContractError, ConfiguredArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "configured_canary_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": len(measured) + unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "measured_usage": _aggregate_usage(measured) if measured else {field: 0 for field in USAGE_FIELDS},
        "unknown_usage_attempt_count": unknown,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": (
            "freeze this architecture as rejected; no successor field patch or numbered strategy"
            if semantic
            else "audit the immutable infrastructure attempt; no turn retry in this root"
        ),
    }
    for name in ("architecture-structural-gate.json", "interstage-receipt.json"):
        path = root / name
        if path.is_file():
            terminal[name.removesuffix(".json").replace("-", "_")] = _record(path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _gate(
    frozen: Mapping[str, Any], usages: Sequence[Mapping[str, int]], diagnostics: Mapping[str, Any]
) -> dict[str, Any]:
    config = frozen["config"]
    aggregate = _aggregate_usage(usages)
    rows = diagnostics["diagnostics"]
    source = frozen["source"]
    dense_id = str(source["segments"][0]["segment_id"])
    residual_id = str(source["segments"][1]["segment_id"])
    counts = {str(row["segment_id"]): int(row["event_count"]) for row in rows}
    production_total = 600538 + aggregate["total_tokens"] * 30
    ratio = production_total / 10065426
    checks = {
        "two_turns_measured": len(usages) == 2,
        "core_turn_tokens_lte_33000": usages[0]["total_tokens"] <= 33000,
        "join_turn_tokens_lte_40000": usages[1]["total_tokens"] <= 40000,
        "combined_tokens_lte_73000": aggregate["total_tokens"] <= 73000,
        "all_core_events_accounted_exactly_once": diagnostics["accounted_core_event_count"] == diagnostics["core_event_count"],
        "dense_event_count_gte_27": counts.get(dense_id, -1) >= 27,
        "nominal_no_signal_event_count_lte_1": 0 <= counts.get(residual_id, -1) <= 1,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "exact_identity_duplicates_0": True,
        "event_cap_violations_0": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_configured_experiment_structural_gate_v1",
        "experiment_id": config["experiment_id"],
        "architecture_id": config["architecture_id"],
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": aggregate,
        "per_turn_usage": list(usages),
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": counts.get(dense_id, -1),
        "candidate_only_nominal_no_signal_event_count": counts.get(residual_id, -1),
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
            _write_stable_time(
                launch_path,
                {
                    "schema_version": "pif_configured_experiment_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 2,
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
        core_turn, join_turn = frozen["turns"]
        core_config, join_config = frozen["config"]["turns"]
        if not core_turn["sidecar"].exists() and (
            core_turn["capacity"].exists() or core_turn["output"].exists()
        ):
            raise ConfiguredExperimentError(
                "core attempt artifact exists without a measured sidecar"
            )
        async with client_factory(frozen["capacity_policy"]) as client:
            if not core_turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=core_config["model"],
                    effort=core_config["effort"],
                    base_instructions=core_turn["base"].read_text(encoding="utf-8"),
                    prompt=core_turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(core_turn["schema"], "core schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=core_turn["sidecar"],
                    output_path=core_turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=core_turn["capacity"],
                )
            core_usage = _usage_from_sidecar(core_turn["sidecar"], core_config)
            if core_usage["total_tokens"] > core_config["total_tokens_maximum"]:
                raise ConfiguredArchitectureStop("core turn exceeded its frozen token bound")
            core_output = _load_json(core_turn["output"], "core output")
            core_packet, core_diagnostics = validate_core_output(core_output, frozen)
            _write_immutable(root / "core-structural-gate.json", core_diagnostics)
            join_input = {
                "schema_version": JOIN_SCHEMA_VERSION,
                "source": frozen["source"],
                "compact_event_cores": core_packet,
                "privacy": "private source and LLM event cores",
            }
            join_prompt = _join_prompt(frozen["source"], core_packet)
            _write_immutable(join_turn["input"], join_input)
            _write_private_text(join_turn["prompt"], join_prompt)
            interstage_path = root / "interstage-receipt.json"
            _write_immutable(
                interstage_path,
                {
                    "schema_version": "pif_configured_experiment_interstage_v1",
                    "core_sidecar": _record(core_turn["sidecar"]),
                    "core_output": _record(core_turn["output"]),
                    "core_gate": _record(root / "core-structural-gate.json"),
                    "join_input": _record(join_turn["input"]),
                    "join_prompt": _record(join_turn["prompt"]),
                    "join_base": _record(join_turn["base"]),
                    "join_schema": _record(join_turn["schema"]),
                    "semantic_projection_performed": False,
                },
            )
            for record in _load_json(interstage_path, "interstage receipt").values():
                if isinstance(record, dict) and {"path", "sha256", "size_bytes"}.issubset(record):
                    _verify_record(record, cache=ContentHashCache())
            if not join_turn["sidecar"].exists() and (
                join_turn["capacity"].exists() or join_turn["output"].exists()
            ):
                raise ConfiguredExperimentError(
                    "join attempt artifact exists without a measured sidecar"
                )
            if not join_turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=join_config["model"],
                    effort=join_config["effort"],
                    base_instructions=join_turn["base"].read_text(encoding="utf-8"),
                    prompt=join_prompt,
                    output_schema=_load_json(join_turn["schema"], "join schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=join_turn["sidecar"],
                    output_path=join_turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=join_turn["capacity"],
                )
        join_usage = _usage_from_sidecar(join_turn["sidecar"], join_config)
        if join_usage["total_tokens"] > join_config["total_tokens_maximum"]:
            raise ConfiguredArchitectureStop("join turn exceeded its frozen token bound")
        join_output = _load_json(join_turn["output"], "join output")
        normalized, provenance, diagnostics = validate_join_output(
            join_output, core_packet, frozen
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(frozen, [core_usage, join_usage], diagnostics)
        _write_immutable(root / "architecture-structural-gate.json", gate)
        if not gate["passed"]:
            raise ConfiguredArchitectureStop("configured architecture structural gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "configured_canary_structural_cost_passed",
            "terminal_reason": "configured_canary_structural_cost_gate_passed_support_alignment_required",
            "semantic_attempt_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": gate["usage"],
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
            "interstage_receipt": _record(root / "interstage-receipt.json"),
            "gate": _record(root / "architecture-structural-gate.json"),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "exact_next_action": "run unchanged frozen side-free support and two-permutation alignment; do not create a field-repair successor",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
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
    return 0 if terminal.get("terminal_reason", "").endswith("support_alignment_required") else 2


if __name__ == "__main__":
    raise SystemExit(main())
