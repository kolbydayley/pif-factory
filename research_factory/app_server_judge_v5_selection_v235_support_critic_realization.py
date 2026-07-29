from __future__ import annotations

"""Run the bounded v235 source-support critic and realization canary."""

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
from . import app_server_judge_v5_selection_v232_window_ledger as v232
from . import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from . import app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = (
    "pif_app_server_judge_v5_selection_v235_support_critic_realization_v1"
)
RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v235_support_critic_runtime_lock_v1"
)
TERMINAL_VERSION = (
    "pif_app_server_judge_v5_selection_v235_support_critic_terminal_v1"
)
PHASE_ID = "development_selection_v5_4_v235_support_critic_realization"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_NEW_TURN_TOKENS = 35_000
REUSED_INVENTORY_TOKENS = 29_362
COMBINED_PRODUCTION_TOKEN_BOUND = 70_000
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 100_000
MAX_SCHEMA_BYTES = 45_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
DENSE_SEGMENT_ID = v234.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v234.NO_SIGNAL_SEGMENT_ID
MIN_DENSE_EVENTS = v234.MIN_DENSE_PROPOSITIONS
USAGE_FIELDS = v234.USAGE_FIELDS
IDENTITY_FIELDS = v234.IDENTITY_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V234_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v234-two-pass-blind-inventory-realization"
).resolve()
V234_TURN_ROOT = (
    V234_ROOT
    / "turns/v234-blind-inventory-d7c914bc1cee2c432b36"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v235-support-critic-realization"
).resolve()
PINNED_CODEX_0_144_1 = v234.PINNED_CODEX_0_144_1
EXPECTED_LINEAGE_HASHES = {
    "v234_terminal": "35d610922d38b1564fc9d927ad7e8b49e1592ccaeea78d66a8ddb76b7522806c",
    "v234_gate": "31b642bb3ecc5d02d9ccadc1ac82c015208f04f15dae260c7eb406b24bee8451",
    "v234_runtime_lock": "3a224938629d6b38e1c6c0b5635dd5108581d493632816b9f06333a7ff76e778",
    "v234_sidecar": "6d4104fc62be59e4fa32c5d9e8244ea5cca98343e93c1e49a84ac22b8d94c65c",
    "v234_output": "509db47938262116004db047a4fab10bf723824206a68c6aca1d410ba1e0d61b",
    "v234_normalized": "47c72eb66f66cde2b5185b1281ef3c85d47dd2005cfc696d207a46a9acef1a17",
    "v234_provenance": "14c6d6b9ddcf0bf69479006f54dc056d1235633c3e2cebd5bd6de0d30d50fc77",
    "v234_input": "c0897603a74f9fb45ff8af8932e16f0caf0d5654170542fc22c22c2c6f7a461e",
    "v234_attempt_spec": "33fae33961a1ecaa0f0b0086adae4eec2d50413dc1ec69c29a106644d432020c",
    "v234_ranking": "af0e1166376fa562a906cd18fff2201c6c0ccc2ecee105bf3d4f0a2c1b009338",
}


class V235SupportCriticError(RuntimeError):
    """The v235 support-critic architecture cannot proceed safely."""


class V235OutputContractError(V235SupportCriticError):
    """A completed critic output violated the frozen structural contract."""


class V235ArchitectureStop(V235SupportCriticError):
    """The critic architecture cannot reach the frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V235SupportCriticError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V235SupportCriticError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V235SupportCriticError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
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


def _lineage_paths() -> dict[str, Path]:
    return {
        "v234_terminal": V234_ROOT / "terminal.json",
        "v234_gate": V234_ROOT / "inventory-structural-gate.json",
        "v234_runtime_lock": V234_ROOT / "runtime-lock.json",
        "v234_sidecar": V234_TURN_ROOT / "sidecar.json",
        "v234_output": V234_TURN_ROOT / "output.private.json",
        "v234_normalized": V234_TURN_ROOT / "normalized-output.private.json",
        "v234_provenance": V234_TURN_ROOT / "evidence-provenance.private.json",
        "v234_input": V234_TURN_ROOT / "input.private.json",
        "v234_attempt_spec": V234_ROOT / "attempt-spec.json",
        "v234_ranking": V234_ROOT / "architecture-ranking.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V235SupportCriticError(f"frozen lineage {name} drifted")
    terminal = _load_json(paths["v234_terminal"], "v234 terminal")
    gate = _load_json(paths["v234_gate"], "v234 inventory gate")
    sidecar = _load_json(paths["v234_sidecar"], "v234 sidecar")
    normalized = _load_json(paths["v234_normalized"], "v234 inventory")
    counts = {
        str(row["segment_id"]): len(row.get("propositions") or [])
        for row in normalized.get("segments") or []
    }
    if (
        terminal.get("terminal_reason")
        != "v234_two_pass_architecture_structural_or_count_gate_not_passed"
        or terminal.get("next_distinct_architecture_authorized") is not True
        or terminal.get("isolated_field_repair_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or gate.get("passed") is not False
        or gate.get("dense_proposition_count") != 32
        or gate.get("no_signal_proposition_count") != 1
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or (sidecar.get("usage") or {}).get("total_tokens")
        != REUSED_INVENTORY_TOKENS
        or counts != {DENSE_SEGMENT_ID: 32, NO_SIGNAL_SEGMENT_ID: 1}
    ):
        raise V235SupportCriticError("v234 predecessor state drifted")
    return {"paths": paths, "records": records}


def _ranking() -> list[dict[str, Any]]:
    return [
        {
            "rank": 1,
            "architecture_id": "frozen_inventory_source_support_critic_realization",
            "distinctive_mechanism": (
                "an exhaustive blind inventory is followed by a source-side LLM critic "
                "that may drop unsupported or non-event proposals before full realization"
            ),
            "measured_basis": (
                "v234 reached 32 dense propositions but repeated one no-signal proposal; "
                "the unresolved decision is proposal validity rather than recall"
            ),
            "cost_risk": "moderate_with_70000_combined_hard_ceiling",
        },
        {
            "rank": 2,
            "architecture_id": "exact_coverage_mask_gap_extraction_and_llm_reconciliation",
            "distinctive_mechanism": (
                "extract only from exact spans not covered by a base event set, then "
                "reconcile new and base events semantically"
            ),
            "measured_basis": (
                "one blind inventory already used 29362 tokens, leaving too little "
                "headroom for both additive gap extraction and reconciliation"
            ),
            "cost_risk": "high",
        },
        {
            "rank": 3,
            "architecture_id": "independent_specialist_ensemble_with_llm_consolidation",
            "distinctive_mechanism": (
                "independent topic-general proposition lenses followed by an LLM merge"
            ),
            "measured_basis": (
                "multiple full source passes repeat transcript input and prior specialist "
                "experiments overproduced boundaries"
            ),
            "cost_risk": "very_high",
        },
    ]


CRITIC_INSTRUCTIONS = """You are the source-support critic and full-schema realization stage of a topic-general semantic event extractor for a private podcast research corpus. A prior blind LLM pass produced a frozen exhaustive proposition inventory. You have no existing events, reference answer, density label, target count, expected topic, or quality hint.

For every frozen proposition_id, make one independent semantic decision against the exact source units. KEEP only when the source entails every material part of the proposition and the proposition is a distinct research-useful event. DROP when it is unsupported, materially overstates the source, is not a research event, or duplicates another frozen proposition. A verbatim phrase is not enough if the asserted proposition is not entailed. A supported paraphrase or coreference may pass. Do not use keywords, regex, fixed-topic rules, target counts, or verbal confidence.

Return one decision per proposition in exact input order. examined_start_unit_id and examined_end_unit_id identify the smallest contiguous source range used for the decision. verdict=keep requires drop_reason=keep and exactly one event carrying that proposition_id. verdict=drop requires one of unsupported, not_research_event, or duplicate and no event with that proposition_id. For duplicate, keep the earliest source-supported atomic proposition and drop only the redundant one. Do not add, merge, split, or omit proposition IDs.

For each kept proposition, populate every full event field from the source. Select the smallest self-contained contiguous evidence-unit range supporting every material event field, including adjacent attribution or coreference only when needed. Return only evidence_start_unit_id and evidence_end_unit_id; deterministic code projects exact evidence and offsets. speaker is who says the words; actor is whose position or action is represented; reported_actor is a quoted or reported source. claim_text is a concise complete proposition naming the relevant actor and target.

Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the selected evidence. Use metric_direction=not_applicable when all metric strings are empty. Classify event_type by the proposition's main predicate and evidential commitment, not its topic.

Use coded if and only if at least one proposition is kept for the segment. Otherwise use no_signal with events=[] and explain why no proposal survived source-support review. Classify segment_source_context from the source. Keep segments, decisions, and kept events in source order. Return schema-valid JSON only."""


def _turn_name(episode_id: str) -> str:
    return "v235_support_critic_" + sha256_text(episode_id)[:20]


def _output_schema(
    *,
    packet: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    source_segment = packet["source_schema"]["properties"]["segments"]["items"]
    proposition_ids = [
        proposition["proposition_id"]
        for row in inventory["segments"]
        for proposition in row["propositions"]
    ]
    unit_ids = list(packet["all_unit_ids"])
    decision = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "proposition_id",
            "verdict",
            "drop_reason",
            "examined_start_unit_id",
            "examined_end_unit_id",
            "rationale",
        ],
        "properties": {
            "proposition_id": {"type": "string", "enum": proposition_ids},
            "verdict": {"type": "string", "enum": ["keep", "drop"]},
            "drop_reason": {
                "type": "string",
                "enum": [
                    "keep",
                    "unsupported",
                    "not_research_event",
                    "duplicate",
                ],
            },
            "examined_start_unit_id": {"type": "string", "enum": unit_ids},
            "examined_end_unit_id": {"type": "string", "enum": unit_ids},
            "rationale": {"type": "string", "minLength": 1, "maxLength": 320},
        },
    }
    event = v232._event_schema(packet["source_schema"], unit_ids)
    event["properties"]["proposition_id"] = {
        "type": "string",
        "enum": proposition_ids,
    }
    event["required"].append("proposition_id")
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "decisions",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(packet["segment_ids"])},
            "status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(
                source_segment["properties"]["segment_source_context"]
            ),
            "no_signal_reason": copy.deepcopy(
                source_segment["properties"]["no_signal_reason"]
            ),
            "decisions": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": decision,
            },
            "events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": event,
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [packet["episode_id"]]},
            "segments": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": segment,
            },
        },
    }


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    private_input = _load_json(paths["v234_input"], "v234 private input")
    inventory = _load_json(paths["v234_normalized"], "v234 inventory")
    packet = v234._source_packet(v234._validate_lineage())
    if (
        private_input.get("episode_id") != packet["episode_id"]
        or inventory.get("episode_id") != packet["episode_id"]
    ):
        raise V235SupportCriticError("v235 episode id lineage drifted")
    inventory_by_id = {
        str(row["segment_id"]): row for row in inventory["segments"]
    }
    prompt_segments = []
    for source in packet["prompt_segments"]:
        segment_id = str(source["segment_id"])
        prompt_segments.append(
            {
                "segment_id": segment_id,
                "source_units": source["source_units"],
                "frozen_propositions": inventory_by_id[segment_id]["propositions"],
            }
        )
    prompt_packet = {"episode_id": packet["episode_id"], "segments": prompt_segments}
    prompt = "# Frozen inventory source-support packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        CRITIC_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _output_schema(packet=packet, inventory=inventory)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise V235SupportCriticError("v235 prompt exceeds frozen byte cap")
    if schema_bytes > MAX_SCHEMA_BYTES:
        raise V235SupportCriticError("v235 schema exceeds frozen byte cap")
    return {
        "turn_name": _turn_name(packet["episode_id"]),
        "episode_id": packet["episode_id"],
        "segment_ids": packet["segment_ids"],
        "source_input": private_input,
        "inventory": inventory,
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": packet["episode_id"],
            "segments": prompt_segments,
            "privacy": "private source units frozen inventory and context",
        },
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": schema_bytes,
    }


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "decisions": turn_root / "decisions.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_NEW_TURN_TOKENS
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_new_turn_count": 1,
            "reused_measured_inventory_tokens": REUSED_INVENTORY_TOKENS,
            "maximum_total_tokens_per_turn": MAX_NEW_TURN_TOKENS,
            "phase_total_token_bound": MAX_NEW_TURN_TOKENS,
            "combined_production_token_bound": COMBINED_PRODUCTION_TOKEN_BOUND,
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
        "ordered_turn_names": [turn_name],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_NEW_TURN_TOKENS,
        "phase_total_token_bound": MAX_NEW_TURN_TOKENS,
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
                Path(v234.__file__).resolve(),
                Path(v233.__file__).resolve(),
                Path(v232.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(root: Path, turn_name: str) -> list[dict[str, Any]]:
    paths = _turn_paths(root, turn_name)
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def _projected_ratio(tokens: int) -> float:
    total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + tokens * PRODUCTION_SCALE
    return total / BASELINE_END_TO_END_TOKENS


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v235 runtime lock")
    root = path.parent.resolve()
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    turn_name = str(lock.get("turn_name") or "")
    expected_request = {row["path"] for row in _request_records(root, turn_name)}
    actual_request = {row.get("path") for row in lock.get("frozen_request") or []}
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_new_turn_count") != 1
        or lock.get("retry_count") != 0
        or not turn_name
        or actual_runtime != expected_runtime
        or actual_request != expected_request
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V235SupportCriticError("v235 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("authorization"),
        lock.get("architecture_ranking"),
        lock.get("architecture_design"),
        lock.get("reuse_contract"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V235SupportCriticError("v235 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        record["path"] for record in lineage["records"].values()
    }:
        raise V235SupportCriticError("v235 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v235 attempt spec")
    turn_name = str(spec["turn_name"])
    paths = _turn_paths(root, turn_name)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "ranking_path": root / "architecture-ranking.json",
        "reuse_path": root / "reuse-contract.json",
        "authorization_path": root / "authorization.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            "turn_name": turn_name,
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "paths": paths,
            "private_input": _load_json(paths["input"], "v235 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v235 schema"),
            "source_input": _load_json(V234_TURN_ROOT / "input.private.json", "v234 source input"),
            "inventory": _load_json(V234_TURN_ROOT / "normalized-output.private.json", "v234 inventory"),
        },
    }


def freeze_v235(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v235 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V235SupportCriticError("unfinished v235 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = _prepare_turn(lineage)
    paths = _turn_paths(root, turn["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_immutable(paths["schema"], turn["schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "authority": "direct_operator_steering_2026_07_17",
        "scope": "one bounded distinct proposal-critic architecture canary",
        "managed_chatgpt_app_server_only": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    authorization_path = root / "authorization.json"
    _write_stable_time(authorization_path, authorization, "created_at")
    ranking = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "selected_architecture_id": (
            "frozen_inventory_source_support_critic_realization"
        ),
        "architectures": _ranking(),
        "selection_basis": (
            "v234 established exhaustive dense recall but repeated one no-signal "
            "proposal; the smallest decision-changing architecture adds a side-free "
            "source-support critic rather than another extraction or field patch"
        ),
        "on_failure": (
            "freeze v235 and choose the next genuinely distinct architecture; do not "
            "repair an isolated field or validator"
        ),
    }
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(ranking_path, ranking, "created_at")
    reuse = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "reused_attempt": "v234 blind inventory turn only",
        "reused_semantic_turn_count": 1,
        "reused_usage_status": "measured",
        "reused_total_tokens": REUSED_INVENTORY_TOKENS,
        "replay_allowed": False,
        "reused_output_was_frozen_before_v235_design": True,
        "v234_terminal_remains_rejected": True,
        "production_accounting_includes_reused_inventory": True,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    reuse_path = root / "reuse-contract.json"
    _write_stable_time(reuse_path, reuse, "created_at")
    design = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "architecture_id": "frozen_inventory_source_support_critic_realization",
        "hypothesis": (
            "a source-side LLM critic with explicit keep/drop authority can remove the "
            "unsupported or non-event no-signal proposal while retaining at least 24 "
            "of the 32 dense propositions and realizing them as grounded full events"
        ),
        "representative_canary": {
            "episode_count": 1,
            "segment_count": 2,
            "dense_inventory_propositions": 32,
            "no_signal_inventory_propositions": 1,
            "new_semantic_turn_count": 1,
            "reference_existing_events_density_visible_to_model": False,
        },
        "production_cost_projection": {
            "episode_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
            "inventory_turn_hard_max": v234.MAX_TOTAL_TOKENS_PER_TURN,
            "critic_turn_hard_max": MAX_NEW_TURN_TOKENS,
            "combined_hard_max": COMBINED_PRODUCTION_TOKEN_BOUND,
            "measured_reused_inventory_tokens": REUSED_INVENTORY_TOKENS,
            "projected_production_amortized_total_token_ratio": round(
                _projected_ratio(COMBINED_PRODUCTION_TOKEN_BOUND), 6
            ),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "new_turn_retry_count": 0,
            "new_turn_total_tokens_max": MAX_NEW_TURN_TOKENS,
            "combined_inventory_and_critic_tokens_max": COMBINED_PRODUCTION_TOKEN_BOUND,
            "every_inventory_proposition_decided_once": True,
            "no_signal_kept_event_count": 0,
            "dense_kept_event_count_minimum": MIN_DENSE_EVENTS,
            "exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "event_cap_violations": 0,
            "exact_identity_duplicates": 0,
            "production_amortized_total_token_ratio_max": 0.28,
            "support_alignment_required_after_structural_pass": True,
            "on_failure": "reject v235 and advance to a distinct architecture",
        },
        "semantic_boundary": {
            "llm_owned": [
                "source support and research-event keep/drop decisions",
                "duplicate decisions",
                "every full event semantic field",
                "evidence selection",
            ],
            "deterministic_only": [
                "schema IDs decision coverage and keep-to-event projection",
                "exact evidence and offset projection",
                "literal metric grounding",
                "exact identity duplicate detection",
                "caps provenance lifecycle and accounting",
            ],
        },
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    design_path = root / "architecture-design.json"
    _write_stable_time(design_path, design, "created_at")
    request_records = _request_records(root, turn["turn_name"])
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_one_turn_support_critic_canary",
        "declared_new_turn_count": 1,
        "reused_semantic_turn_count": 1,
        "turn_name": turn["turn_name"],
        "episode_id": turn["episode_id"],
        "segment_ids": turn["segment_ids"],
        "model": MODEL,
        "effort": EFFORT,
        "retry_count": 0,
        "maximum_new_turn_tokens": MAX_NEW_TURN_TOKENS,
        "combined_production_token_bound": COMBINED_PRODUCTION_TOKEN_BOUND,
        "prompt_bytes": turn["prompt_bytes"],
        "base_instructions_bytes": turn["base_bytes"],
        "schema_bytes": turn["schema_bytes"],
        "existing_events_visible_to_model": False,
        "reference_visible_to_model": False,
        "density_visible_to_model": False,
        "support_alignment_quality_measured": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "authorization": _record(authorization_path),
        "architecture_ranking": _record(ranking_path),
        "architecture_design": _record(design_path),
        "reuse_contract": _record(reuse_path),
        "direct_lineage": lineage["records"],
        "frozen_request": request_records,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "privacy": "private source prompts outputs; sanitized terminal and gate",
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
        "authorization": _record(authorization_path),
        "architecture_ranking": _record(ranking_path),
        "architecture_design": _record(design_path),
        "reuse_contract": _record(reuse_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": request_records,
        "turn_name": turn["turn_name"],
        "model": MODEL,
        "effort": EFFORT,
        "declared_new_turn_count": 1,
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _validate_schema_output(schema: Mapping[str, Any], output: Mapping[str, Any]) -> None:
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V235OutputContractError(
            f"structured output validation failed: {type(exc).__name__}"
        ) from exc


def _project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
]:
    _validate_schema_output(turn["schema"], output)
    if output.get("episode_id") != turn["episode_id"]:
        raise V235OutputContractError("episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V235OutputContractError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["source_input"]["segments"]
    }
    inventory_by_id = {
        str(row["segment_id"]): row for row in turn["inventory"]["segments"]
    }
    normalized_rows = []
    private_decision_rows = []
    provenance_rows = []
    diagnostics = []
    seen_identity = set()
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        propositions = inventory_by_id[segment_id]["propositions"]
        expected_ids = [item["proposition_id"] for item in propositions]
        decisions = list(row.get("decisions") or [])
        if [item.get("proposition_id") for item in decisions] != expected_ids:
            raise V235OutputContractError("decision proposition coverage or order drifted")
        keep_ids = []
        projected_decisions = []
        for decision in decisions:
            start_id = str(decision["examined_start_unit_id"])
            end_id = str(decision["examined_end_unit_id"])
            projection = v234._project_unit_range(
                source=source, start_id=start_id, end_id=end_id
            )
            verdict = decision["verdict"]
            drop_reason = decision["drop_reason"]
            if (verdict == "keep") != (drop_reason == "keep"):
                raise V235OutputContractError("decision verdict and drop reason drifted")
            if verdict == "keep":
                keep_ids.append(str(decision["proposition_id"]))
            projected_decisions.append(
                {
                    **dict(decision),
                    "examined_evidence_sha256": sha256_text(projection["evidence"]),
                }
            )
        events = list(row.get("events") or [])
        if [event.get("proposition_id") for event in events] != keep_ids:
            raise V235OutputContractError("kept proposition to event projection drifted")
        if (row.get("status") == "coded") != bool(events):
            raise V235OutputContractError("coded status does not match kept events")
        if not events and row.get("status") != "no_signal":
            raise V235OutputContractError("empty critic output must be no_signal")
        if len(events) > MAX_EVENTS_PER_SEGMENT:
            raise V235OutputContractError("event cap exceeded")
        prior_start = -1
        projected_events = []
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            proposition_id = str(event.pop("proposition_id"))
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            projection = v234._project_unit_range(
                source=source, start_id=start_id, end_id=end_id
            )
            if projection["start_index"] < prior_start:
                raise V235OutputContractError("event evidence order drifted")
            prior_start = int(projection["start_index"])
            metric_values = [
                str(event.get(field) or "")
                for field in (
                    "metric_value",
                    "metric_unit",
                    "metric_comparator",
                    "metric_raw_text",
                )
            ]
            if any(
                value and value not in projection["evidence"]
                for value in metric_values
            ):
                raise V235OutputContractError("metric literal is not in evidence")
            if any(metric_values) == (
                event.get("metric_direction") == "not_applicable"
            ):
                raise V235OutputContractError("metric direction applicability drifted")
            identity = _canonical_json(
                {field: event.get(field) for field in IDENTITY_FIELDS}
            )
            if identity in seen_identity:
                raise V235OutputContractError("exact event identity duplicate")
            seen_identity.add(identity)
            event["window_id"] = projection["window_id"]
            event["evidence"] = projection["evidence"]
            projected_events.append(event)
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "proposition_id": proposition_id,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": projection["start_char"],
                    "end_char": projection["end_char"],
                    "window_id": projection["window_id"],
                    "evidence_sha256": sha256_text(projection["evidence"]),
                }
            )
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "events": projected_events,
            }
        )
        private_decision_rows.append(
            {"segment_id": segment_id, "decisions": projected_decisions}
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "inventory_proposition_count": len(propositions),
                "decision_count": len(decisions),
                "kept_event_count": len(projected_events),
                "dropped_proposition_count": len(propositions) - len(projected_events),
            }
        )
    normalized = {"episode_id": turn["episode_id"], "segments": normalized_rows}
    decision_output = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "segments": private_decision_rows,
    }
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "events": provenance_rows,
    }
    return normalized, decision_output, provenance, diagnostics


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V235SupportCriticError("turn usage is absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V235SupportCriticError("turn usage is incomplete")
        result[field] = value
    return result


def _validate_measured_sidecar(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "v235 sidecar")
    usage = _usage(sidecar)
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > MAX_NEW_TURN_TOKENS
    ):
        raise V235SupportCriticError("v235 measured sidecar contract failed")
    return usage


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _build_gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_segment = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(
        by_segment.get(DENSE_SEGMENT_ID, {}).get("kept_event_count", -1)
    )
    no_signal_count = int(
        by_segment.get(NO_SIGNAL_SEGMENT_ID, {}).get("kept_event_count", -1)
    )
    combined_tokens = REUSED_INVENTORY_TOKENS + int(usage["total_tokens"])
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined_tokens * PRODUCTION_SCALE
    )
    production_ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": set(by_segment)
        == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "every_inventory_proposition_decided_once": all(
            int(row["inventory_proposition_count"]) == int(row["decision_count"])
            for row in diagnostics
        ),
        "no_signal_kept_event_count_0": no_signal_count == 0,
        "dense_kept_event_count_gte_24": dense_count >= MIN_DENSE_EVENTS,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_35000": int(usage["total_tokens"])
        <= MAX_NEW_TURN_TOKENS,
        "combined_tokens_lte_70000": combined_tokens
        <= COMBINED_PRODUCTION_TOKEN_BOUND,
        "production_amortized_total_token_ratio_lte_0_28": production_ratio
        <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "reused_inventory_tokens": REUSED_INVENTORY_TOKENS,
        "new_turn_usage": dict(usage),
        "combined_total_tokens": combined_tokens,
        "diagnostics": list(diagnostics),
        "dense_kept_event_count": dense_count,
        "no_signal_kept_event_count": no_signal_count,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(production_ratio, 6),
        "support_alignment_quality_measured": False,
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
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


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = _zero_usage()
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            usage = _usage(_load_json(paths["sidecar"], "v235 sidecar"))
            unknown = 0
        except V235SupportCriticError:
            unknown = 1
    semantic_failure = isinstance(
        exc, (V235OutputContractError, V235ArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v235_support_critic_structural_or_count_gate_not_passed"
            if semantic_failure
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "reused_semantic_attempt_count": 1,
        "new_semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "reused_inventory_tokens": REUSED_INVENTORY_TOKENS,
        "new_turn_usage": usage,
        "combined_total_tokens": REUSED_INVENTORY_TOKENS + usage["total_tokens"],
        "unknown_usage_attempt_count": unknown,
        "sidecar": sidecar_record,
        "architecture_strategy_rejected": semantic_failure,
        "isolated_field_repair_authorized": False,
        "next_distinct_architecture_authorized": semantic_failure,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "architecture_ranking": _record(frozen["ranking_path"]),
        "exact_next_action": (
            "freeze v235 and select the next distinct architecture without a field patch"
            if semantic_failure
            else "audit the immutable infrastructure attempt; no silent retry"
        ),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v235(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v235 terminal")
    frozen = freeze_v235(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root,
            frozen,
            V235SupportCriticError("launch receipt exists; replay prohibited"),
        )
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_new_turn_count": 1,
            "reused_semantic_turn_count": 1,
            "turn_name": frozen["turn"]["turn_name"],
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "managed_chatgpt_auth_only": True,
            "official_persistent_app_server": True,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    paths = frozen["turn"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["turn"]["base"],
                prompt=frozen["turn"]["prompt"],
                output_schema=frozen["turn"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V235SupportCriticError("support critic turn did not complete")
        usage = _validate_measured_sidecar(paths["sidecar"])
        normalized, decisions, provenance, diagnostics = _project_output(
            result.output, frozen["turn"]
        )
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["decisions"], decisions)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["diagnostics"], diagnostics)
        gate = _build_gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V235ArchitectureStop("v235 structural or count gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v235_architecture_structural_gate_passed",
            "terminal_reason": "v235_support_critic_structural_cost_gate_passed",
            "reused_semantic_attempt_count": 1,
            "new_semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "reused_inventory_tokens": REUSED_INVENTORY_TOKENS,
            "new_turn_usage": usage,
            "combined_total_tokens": gate["combined_total_tokens"],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "structural_cost_gate_passed": True,
            "support_alignment_quality_measured": False,
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(gate_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "architecture_ranking": _record(frozen["ranking_path"]),
            "sidecar": _record(paths["sidecar"]),
            "exact_next_action": (
                "run the frozen side-free support and neutral alignment canary over "
                "v235 kept events without changing the extractor"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v235 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v235 support critic canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v235(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v235 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "new_turn_count": frozen["spec"]["declared_new_turn_count"],
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v235(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "combined_total_tokens": terminal.get("combined_total_tokens"),
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
