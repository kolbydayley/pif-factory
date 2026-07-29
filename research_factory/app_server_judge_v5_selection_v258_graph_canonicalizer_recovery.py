from __future__ import annotations

"""Recover the v253 graph canonicalizer with a compatible output schema."""

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
from . import app_server_judge_v5_selection_v253_graph_canonicalizer as v253
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v258_graph_canonicalizer_recovery_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v258_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v258_terminal_v1"
PHASE_ID = "development_selection_v5_4_v258_graph_canonicalizer_recovery"
MODEL = v253.MODEL
EFFORT = v253.EFFORT
MAX_TOTAL_TOKENS = v253.MAX_TOTAL_TOKENS
MAX_COMBINED_TOKENS = v253.MAX_COMBINED_TOKENS
TIMEOUT_SECONDS = v253.TIMEOUT_SECONDS
MAX_PROMPT_BYTES = v253.MAX_PROMPT_BYTES
MAX_BASE_BYTES = v253.MAX_BASE_BYTES
MAX_SCHEMA_BYTES = v253.MAX_SCHEMA_BYTES
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
USAGE_FIELDS = v253.USAGE_FIELDS
PROJECT_ROOT = v253.PROJECT_ROOT
PIPELINE_ROOT = v253.PIPELINE_ROOT
V253_ROOT = v253.DEFAULT_OUTPUT_ROOT
V257_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v257-frozen-alignment-v255"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v258-graph-canonicalizer-recovery"
).resolve()
PINNED_CODEX_0_144_1 = v253.PINNED_CODEX_0_144_1

EXPECTED_ADDITIONAL_LINEAGE_HASHES = {
    "v253_terminal": "96a1ddbb836f324f2419769da29d79b19c81696ef722ad2e94b2a3723c675804",
    "v253_runtime_lock": "98c53924e44806ea87c2b0341cc2d54a88c3bb60026331014a09c88a5150470b",
    "v253_sidecar": "51fb9c747ce8477222df5f94ac07a00b1158976973e32f6324fd5dcd5ef722ca",
    "v253_schema": "9b3bf6b1005175a9ee7d5e39c92433a93258802a2229f33717dbf1326e74bfa0",
    "v257_terminal": "70bb6ae63b31c6a5d6bb35ccbe246e10adf0744f77f7ac54d540d255d9926bed",
    "v257_score": "b5c1accbb4190e2e1f911c3b94a9e3aeaded93b6da2eb197357876b985aa28f5",
}


class V258GraphCanonicalizerError(RuntimeError):
    """The v258 recovery cannot proceed or be adopted safely."""


class V258OutputContractError(V258GraphCanonicalizerError):
    """A completed output violated the frozen graph-owner contract."""


class V258ArchitectureStop(V258GraphCanonicalizerError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V258GraphCanonicalizerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V258GraphCanonicalizerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V258GraphCanonicalizerError(f"frozen {path.name} drifted")
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


def _v253_turn_root() -> Path:
    return next(V253_ROOT.glob("turns/v253-graph-canonicalizer-*"))


def _additional_lineage_paths() -> dict[str, Path]:
    turn_root = _v253_turn_root()
    return {
        "v253_terminal": V253_ROOT / "terminal.json",
        "v253_runtime_lock": V253_ROOT / "runtime-lock.json",
        "v253_sidecar": turn_root / "sidecar.json",
        "v253_schema": turn_root / "schema.json",
        "v257_terminal": V257_ROOT / "terminal.json",
        "v257_score": V257_ROOT / "alignment-score.json",
    }


def _validate_lineage() -> dict[str, Any]:
    base = v253._validate_lineage()
    paths = _additional_lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_ADDITIONAL_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V258GraphCanonicalizerError(f"frozen lineage {name} drifted")
    v253.verify_runtime_lock(paths["v253_runtime_lock"])
    t253 = _load_json(paths["v253_terminal"], "v253 terminal")
    s253 = _load_json(paths["v253_sidecar"], "v253 sidecar")
    t257 = _load_json(paths["v257_terminal"], "v257 terminal")
    q257 = _load_json(paths["v257_score"], "v257 score")
    if (
        t253.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t253.get("error_class") != "ReserveCapacityError"
        or t253.get("usage_status") != "unknown"
        or s253.get("state") != "failed"
        or s253.get("usage_status") != "unknown"
        or s253.get("usage_complete") is not False
        or float(s253.get("wall_elapsed_seconds") or 0) >= 10
        or t257.get("terminal_reason")
        != "v257_alignment_quality_or_permutation_gate_not_passed"
        or (q257.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.77027
        or (q257.get("metrics") or {}).get(
            "strictly_equivalent_reference_semantic_unit_count"
        )
        != 10
        or t253.get("production_mutated") is not False
        or t257.get("production_mutated") is not False
        or t257.get("holdout_authorized") is not False
    ):
        raise V258GraphCanonicalizerError("v258 predecessor evidence drifted")
    return {
        **base,
        "records": {**base["records"], **records},
    }


def _schema_unique_items_paths(value: Any, path: str = "$") -> list[str]:
    result: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if key == "uniqueItems":
                result.append(child)
            result.extend(_schema_unique_items_paths(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.extend(_schema_unique_items_paths(item, f"{path}[{index}]"))
    return result


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    prior = v253.prepare_turn(lineage)
    schema = copy.deepcopy(prior["schema"])
    source_ids = schema["properties"]["segments"]["items"]["properties"]["events"][
        "items"
    ]["properties"]["source_graph_event_ids"]
    removed = source_ids.pop("uniqueItems", None)
    if removed is not True or _schema_unique_items_paths(schema):
        raise V258GraphCanonicalizerError("v258 schema compatibility delta drifted")
    sizes = {
        "prompt_bytes": len(prior["prompt"].encode("utf-8")),
        "base_bytes": len(prior["base"].encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V258GraphCanonicalizerError("v258 request size cap exceeded")
    return {
        **prior,
        "turn_name": "v258_graph_canonicalizer_" + sha256_text(prior["episode_id"])[:20],
        "schema": schema,
        "removed_schema_keywords": [
            "$.properties.segments.items.properties.events.items.properties.source_graph_event_ids.uniqueItems"
        ],
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    try:
        result = v253.project_output(output, turn)
    except v253.V253OutputContractError as exc:
        raise V258OutputContractError(str(exc)) from exc
    coverage = dict(result[4])
    coverage["schema_version"] = SCHEMA_VERSION
    return result[0], result[1], result[2], result[3], coverage


def _production_ratio(combined_tokens: int) -> tuple[int, float]:
    return v253._production_ratio(combined_tokens)


def _gate(
    *,
    usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    gate = v253._gate(usage=usage, diagnostics=diagnostics, coverage=coverage)
    gate["schema_version"] = SCHEMA_VERSION
    gate["phase_id"] = PHASE_ID
    return gate


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    return v253._turn_paths(root, turn_name)


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [
        _record(paths[name])
        for name in (
            "input",
            "prompt",
            "base",
            "schema",
            "projection_schema",
            "direct_schema",
        )
    ]


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected,
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
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
        "phase_total_token_bound": MAX_TOTAL_TOKENS,
        "projected_phase_quota_points": projected,
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
                *v253._runtime_files(),
                Path(__file__).resolve(),
                Path(v253.__file__).resolve(),
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


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v258 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v258 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("max_total_tokens") != MAX_TOTAL_TOKENS
        or lock.get("schema_compatibility_delta")
        != "remove_uniqueItems_only_deterministic_exact_once_retained"
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(file) for file in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V258GraphCanonicalizerError("v258 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("recovery_audit"),
        lock.get("authorization"),
        lock.get("ranking"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V258GraphCanonicalizerError("v258 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V258GraphCanonicalizerError("v258 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v258 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v258 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v258 schema"),
            "projection_schema": _load_json(
                paths["projection_schema"], "v258 projection schema"
            ),
            "direct_schema": _load_json(paths["direct_schema"], "v258 direct schema"),
            "paths": paths,
        },
    }


def freeze_v258(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v258 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V258GraphCanonicalizerError("unfinished v258 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    paths = _turn_paths(root, turn["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_immutable(paths["schema"], turn["schema"])
    _write_immutable(paths["projection_schema"], turn["projection_schema"])
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    recovery_path = root / "v253-infrastructure-recovery-audit.json"
    _write_stable_time(
        recovery_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "v253_terminal": lineage["records"]["v253_terminal"],
            "v253_sidecar": lineage["records"]["v253_sidecar"],
            "v253_schema": lineage["records"]["v253_schema"],
            "v253_usage_status": "unknown",
            "v253_wall_seconds_lt_10": True,
            "v253_output_exists": False,
            "v253_replayed": False,
            "semantic_prompt_changed": False,
            "model_or_effort_changed": False,
            "schema_keywords_removed": turn["removed_schema_keywords"],
            "deterministic_exact_once_validation_retained": True,
            "production_mutated": False,
        },
        "created_at",
    )
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "one recovered source_graph_then_global_llm_owner_reduce canary",
            "semantic_attempt_count": 1,
            "retry_count": 0,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(
        ranking_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "selected_architecture_id": "source_graph_then_global_llm_owner_reduce",
            "architectures": [
                {"rank": 1, "id": "source_graph_then_global_llm_owner_reduce"},
                {"rank": 2, "id": "overlapping_window_maps_then_global_llm_owner_reduce"},
                {"rank": 3, "id": "episode_bootstrap_segment_specialists_then_global_llm_join"},
            ],
            "measured_basis": {
                "v244_graph_tokens": 38_162,
                "v253_failure_class": "pre_output_schema_or_request_infrastructure_failure",
                "v255_v257_macro_f1": 0.77027,
                "v255_v257_strict_reference_units": 10,
            },
            "on_failure": "freeze and reject graph canonicalizer; advance to rank 2 without field repair",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(MAX_COMBINED_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "source_graph_then_global_llm_owner_reduce",
            "hypothesis": (
                "the frozen v244 recall graph plus one global Sol-low owner pass can correct boundaries "
                "and full-field semantics while retaining exact source grounding and fitting the 28 percent target"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "adopted_graph_semantic_turn_count": 1,
                "new_semantic_turn_count": 1,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "adopted_graph_tokens": 38_162,
                "new_turn_hard_max": MAX_TOTAL_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(
                    projected_ratio, 6
                ),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_graph_nodes_accounted_exactly_once": True,
                "dense_event_count_minimum": v253.MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": v253.MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "new_turn_tokens_max": MAX_TOTAL_TOKENS,
                "combined_tokens_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
                "on_structural_pass": "run frozen side-free support then alignment",
                "on_failure": "reject architecture without isolated field repair",
            },
            "semantic_regex_or_keyword_filtering": False,
            "deterministic_semantic_decisions": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_graph_canonicalizer_recovery",
            "turn_name": turn["turn_name"],
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "max_total_tokens": MAX_TOTAL_TOKENS,
            "prompt_bytes": turn["prompt_bytes"],
            "base_bytes": turn["base_bytes"],
            "schema_bytes": turn["schema_bytes"],
            "schema_compatibility_delta": "remove_uniqueItems_only_deterministic_exact_once_retained",
            "semantic_regex_or_keyword_filtering": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "max_total_tokens": MAX_TOTAL_TOKENS,
        "schema_compatibility_delta": "remove_uniqueItems_only_deterministic_exact_once_retained",
        "semantic_regex_or_keyword_filtering": False,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(file) for file in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "recovery_audit": _record(recovery_path),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "request": _request_records(paths),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _measured_usage(sidecar: Mapping[str, Any]) -> dict[str, int] | None:
    try:
        usage = {
            field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS
        }
    except (KeyError, TypeError, ValueError):
        return None
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
    ):
        return None
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
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
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        measured = _measured_usage(_load_json(paths["sidecar"], "v258 sidecar"))
        if measured is not None:
            usage = measured
            unknown = 0
            measured_over_cap = usage["total_tokens"] > MAX_TOTAL_TOKENS
    semantic = isinstance(exc, (V258OutputContractError, V258ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v258_graph_canonicalizer_structural_quality_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "unknown_usage_attempt_count": unknown,
        "measured_token_bound_exceeded": measured_over_cap,
        "sidecar": sidecar_record,
        "architecture_strategy_rejected": semantic,
        "next_distinct_architecture_authorized": semantic,
        "isolated_field_repair_authorized": False,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": (
            "reject v258 and advance to ranked architecture 2 without field repair"
            if semantic
            else "audit immutable v258 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v258(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v258 terminal")
    frozen = freeze_v258(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V258GraphCanonicalizerError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "managed_chatgpt_auth_only": True,
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
            raise V258GraphCanonicalizerError("v258 turn did not complete")
        usage = _measured_usage(_load_json(paths["sidecar"], "v258 sidecar"))
        if usage is None:
            raise V258GraphCanonicalizerError("v258 sidecar accounting or auth failed")
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V258ArchitectureStop("v258 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, applicability, coverage = project_output(
            result.output, frozen["turn"]
        )
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "applicability": root / "applicability-receipt.json",
            "coverage": root / "graph-coverage-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["applicability"], applicability)
        _write_immutable(artifacts["coverage"], coverage)
        gate = _gate(usage=usage, diagnostics=diagnostics, coverage=coverage)
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V258ArchitectureStop("v258 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v258_architecture_structural_gate_passed",
            "terminal_reason": "v258_graph_canonicalizer_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_turn_usage": usage,
            "adopted_v244_graph_tokens": 38_162,
            "combined_tokens": gate["combined_tokens"],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "dense_event_count": gate["dense_event_count"],
            "candidate_only_nominal_no_signal_event_count": gate[
                "candidate_only_nominal_no_signal_event_count"
            ],
            "residual_support_audit_required": gate["residual_support_audit_required"],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(artifacts["gate"]),
            "normalized_output": _record(artifacts["normalized"]),
            "evidence_provenance": _record(artifacts["provenance"]),
            "graph_coverage_receipt": _record(artifacts["coverage"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v258 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v258 graph canonicalizer recovery")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v258(output_dir=Path(args.output_dir))
        spec = _load_json(frozen["spec_path"], "v258 spec")
        result = {
            "state": spec["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": spec["prompt_bytes"],
            "base_bytes": spec["base_bytes"],
            "schema_bytes": spec["schema_bytes"],
            "projected_ratio": round(_production_ratio(MAX_COMBINED_TOKENS)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v258(
                output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "new_turn_tokens": (terminal.get("new_turn_usage") or terminal.get("usage") or {}).get(
                "total_tokens"
            ),
            "combined_tokens": terminal.get("combined_tokens"),
            "production_amortized_total_token_ratio": terminal.get(
                "production_amortized_total_token_ratio"
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
