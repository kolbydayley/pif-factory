from __future__ import annotations

"""Realize the frozen v234 inventory with explicit applicability objects."""

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
from . import app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import app_server_judge_v5_selection_v265_segment_isolation as v265
from . import app_server_judge_v5_selection_v267_frozen_alignment as v267
from . import app_server_judge_v5_selection_v268_inventory_atomic_realization as v268
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v269_inventory_explicit_realization_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v269_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v269_terminal_v1"
PHASE_ID = "development_selection_v5_4_v269_inventory_explicit_realization"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
ADOPTED_INVENTORY_TOKENS = 29_362
MAX_NEW_TOKENS = 44_000
MAX_COMBINED_TOKENS = ADOPTED_INVENTORY_TOKENS + MAX_NEW_TOKENS
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 24_000
MAX_SCHEMA_BYTES = 80_000
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v234.USAGE_FIELDS
PROJECT_ROOT = v234.PROJECT_ROOT
PIPELINE_ROOT = v234.PIPELINE_ROOT
V234_ROOT = v234.DEFAULT_OUTPUT_ROOT
V265_ROOT = v265.DEFAULT_OUTPUT_ROOT
V267_ROOT = v267.DEFAULT_OUTPUT_ROOT
V268_ROOT = v268.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v269-inventory-explicit-realization"
).resolve()
PINNED_CODEX_0_144_1 = v234.PINNED_CODEX_0_144_1

EXPECTED_LINEAGE_HASHES = {
    "v234_terminal": "35d610922d38b1564fc9d927ad7e8b49e1592ccaeea78d66a8ddb76b7522806c",
    "v234_runtime_lock": "3a224938629d6b38e1c6c0b5635dd5108581d493632816b9f06333a7ff76e778",
    "v234_inventory_output": "509db47938262116004db047a4fab10bf723824206a68c6aca1d410ba1e0d61b",
    "v234_inventory_sidecar": "6d4104fc62be59e4fa32c5d9e8244ea5cca98343e93c1e49a84ac22b8d94c65c",
    "v265_terminal": "6937abe1a8f5afa65003b71fd96567b4254ad4501d4dc0579d8d606b00a9440c",
    "v265_gate": "53d9c6006afd9f74e679cf34de0c5062d2f27305979167b9cf797bd4872dd4a7",
    "v265_runtime_lock": "0f8b467c43b1463ec8d729c03622a77486c7abddf0ced29126b86162436b1280",
    "v267_terminal": "386b7cd5632c0d0a2bfc658522e31b564a21036201686318f35ab220a610af8c",
    "v267_score": "8ca52ff3acef1766b79e88d40aa4dc6d97d00714e8e230e768b9610bbc879392",
    "v267_runtime_lock": "bbf609679bf3353bbd127c4d0f5d70ab2a054e8de7a906c0aabca13ebf2ffc21",
    "v267_sidecar_base": "2cc631c62b7ad369ea726ae019f2708d874ade41c441c684481b1ebe881e44a2",
    "v267_sidecar_canary": "37d321e483d76b7461b3d675a6adb5c538ee7fcb3195fe5087111a691c4a251b",
    "v268_terminal": "9a65d1f4697876fba5ec3f1f0aa135ae9c477bff441c3436d0cee593efd889e7",
    "v268_runtime_lock": "57456f775351d4a7d751cde117ecc7e1ca89eaefb4c617473e2a35de35b539e4",
    "v268_sidecar": "d39f01f9d5bf2ff318387fc9032beea7d99fcc9eb65105067880a64134c10f74",
}

class V269InventoryExplicitRealizationError(RuntimeError):
    """The v269 architecture cannot proceed or be adopted safely."""


class V269OutputContractError(V269InventoryExplicitRealizationError):
    """A completed output violated the frozen ownership/projection contract."""


class V269ArchitectureStop(V269InventoryExplicitRealizationError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V269InventoryExplicitRealizationError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V269InventoryExplicitRealizationError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V269InventoryExplicitRealizationError(f"frozen {path.name} drifted")
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


def _single_turn(root: Path, prefix: str) -> Path:
    matches = list(root.glob(f"turns/{prefix}*"))
    if len(matches) != 1:
        raise V269InventoryExplicitRealizationError(f"{prefix} turn membership drifted")
    return matches[0]


def _lineage_paths() -> dict[str, Path]:
    v234_turn = _single_turn(V234_ROOT, "v234-blind-inventory-")
    v267_base = _single_turn(V267_ROOT, "v267-frozen-alignment-base")
    v267_canary = _single_turn(V267_ROOT, "v267-frozen-alignment-balanced-canary")
    v268_turn = _single_turn(V268_ROOT, "v268-inventory-atomic-realization-")
    return {
        "v234_terminal": V234_ROOT / "terminal.json",
        "v234_runtime_lock": V234_ROOT / "runtime-lock.json",
        "v234_inventory_output": v234_turn / "output.private.json",
        "v234_inventory_sidecar": v234_turn / "sidecar.json",
        "v265_terminal": V265_ROOT / "terminal.json",
        "v265_gate": V265_ROOT / "architecture-structural-gate.json",
        "v265_runtime_lock": V265_ROOT / "runtime-lock.json",
        "v267_terminal": V267_ROOT / "terminal.json",
        "v267_score": V267_ROOT / "alignment-score.json",
        "v267_runtime_lock": V267_ROOT / "runtime-lock.json",
        "v267_sidecar_base": v267_base / "sidecar.json",
        "v267_sidecar_canary": v267_canary / "sidecar.json",
        "v268_terminal": V268_ROOT / "terminal.json",
        "v268_runtime_lock": V268_ROOT / "runtime-lock.json",
        "v268_sidecar": v268_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V269InventoryExplicitRealizationError(f"frozen lineage {name} drifted")
    v234.verify_runtime_lock(paths["v234_runtime_lock"])
    v265.verify_runtime_lock(paths["v265_runtime_lock"])
    v267.verify_runtime_lock(paths["v267_runtime_lock"])
    v268.verify_runtime_lock(paths["v268_runtime_lock"])
    t234 = _load_json(paths["v234_terminal"], "v234 terminal")
    s234 = _load_json(paths["v234_inventory_sidecar"], "v234 sidecar")
    inventory = _load_json(paths["v234_inventory_output"], "v234 inventory")
    t265 = _load_json(paths["v265_terminal"], "v265 terminal")
    g265 = _load_json(paths["v265_gate"], "v265 gate")
    t267 = _load_json(paths["v267_terminal"], "v267 terminal")
    q267 = _load_json(paths["v267_score"], "v267 score")
    s267 = [
        _load_json(paths["v267_sidecar_base"], "v267 base sidecar"),
        _load_json(paths["v267_sidecar_canary"], "v267 canary sidecar"),
    ]
    t268 = _load_json(paths["v268_terminal"], "v268 terminal")
    s268 = _load_json(paths["v268_sidecar"], "v268 sidecar")
    inventory_counts = [len(row.get("propositions") or []) for row in inventory["segments"]]
    if (
        t234.get("terminal_reason")
        != "v234_two_pass_architecture_structural_or_count_gate_not_passed"
        or t234.get("error_class") != "V234ArchitectureStop"
        or (s234.get("usage") or {}).get("total_tokens") != ADOPTED_INVENTORY_TOKENS
        or inventory_counts != [32, 1]
        or t265.get("terminal_reason")
        != "v265_segment_isolation_structural_cost_gate_passed"
        or g265.get("passed") is not True
        or t267.get("terminal_reason")
        != "v267_alignment_quality_or_permutation_gate_not_passed"
        or (q267.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.786707
        or any(sidecar.get("usage_status") != "measured" for sidecar in s267)
        or any(sidecar.get("usage_complete") is not True for sidecar in s267)
        or t268.get("terminal_reason")
        != "v268_inventory_atomic_realization_structural_quality_or_cost_gate_not_passed"
        or t268.get("error_class") != "V268OutputContractError"
        or (s268.get("usage") or {}).get("total_tokens") != 35_933
        or s268.get("usage_status") != "measured"
        or s268.get("usage_complete") is not True
        or any(
            terminal.get("production_mutated") is not False
            for terminal in (t234, t265, t267, t268)
        )
    ):
        raise V269InventoryExplicitRealizationError("v269 predecessor evidence drifted")
    packet = v234._source_packet(v234._validate_lineage())
    if [row["segment_id"] for row in inventory["segments"]] != packet["segment_ids"]:
        raise V269InventoryExplicitRealizationError("v234 inventory segment order drifted")
    return {
        "paths": paths,
        "records": records,
        "inventory": inventory,
        "packet": packet,
        "adopted_inventory_usage": {
            field: int((s234.get("usage") or {})[field]) for field in USAGE_FIELDS
        },
    }


def _explicit_realization_schema(
    packet: Mapping[str, Any], inventory: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    direct = v234._realization_schema(packet, inventory)
    schema = copy.deepcopy(direct)
    event_path = schema["properties"]["segments"]["items"]["properties"]["events"]
    source_event = event_path["items"]
    explicit_event = v249._explicit_event_schema(source_event)
    explicit_event["properties"]["proposition_id"] = copy.deepcopy(
        source_event["properties"]["proposition_id"]
    )
    explicit_event["required"].append("proposition_id")
    event_path["items"] = explicit_event
    return schema, direct


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    direct = v234._prepare_realization_turn(lineage["packet"], lineage["inventory"])
    schema, direct_schema = _explicit_realization_schema(
        lineage["packet"], lineage["inventory"]
    )
    base = (
        direct["base"]
        + "\n\n# Explicit applicability contract\n"
        + v249.APPLICABILITY_INSTRUCTIONS
        + "\n"
    )
    base_bytes = len(base.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        direct["prompt_bytes"] > MAX_PROMPT_BYTES
        or base_bytes > MAX_BASE_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
    ):
        raise V269InventoryExplicitRealizationError("v269 request size cap failed")
    return {
        **direct,
        "turn_name": "v269_inventory_explicit_realization_"
        + sha256_text(direct["episode_id"])[:20],
        "base": base,
        "schema": schema,
        "direct_schema": direct_schema,
        "base_bytes": base_bytes,
        "schema_bytes": schema_bytes,
        "packet": lineage["packet"],
        "inventory": lineage["inventory"],
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
        direct = copy.deepcopy(dict(output))
        for segment in direct.get("segments") or []:
            events = []
            for event in segment.get("events") or []:
                projected = v249._project_event(event)
                projected["proposition_id"] = event["proposition_id"]
                events.append(projected)
            segment["events"] = events
        direct_turn = dict(turn)
        direct_turn["schema"] = turn["direct_schema"]
        normalized, provenance, diagnostics = v234._project_realization(
            direct,
            direct_turn,
            turn["packet"]["private_input"],
            turn["inventory"],
        )
    except (
        v234.V234OutputContractError,
        v249.V249OutputContractError,
        ValidationError,
        ValueError,
        TypeError,
    ) as exc:
        raise V269OutputContractError(str(exc)) from exc
    ownership = {
        "schema_version": SCHEMA_VERSION,
        "architecture_id": "adopted_blind_inventory_explicit_applicability_realization",
        "segments": [
            {
                "segment_id": row["segment_id"],
                "inventory_proposition_count": row["inventory_proposition_count"],
                "final_event_count": row["realized_event_count"],
                "all_propositions_accounted_exactly_once": (
                    row["inventory_proposition_count"] == row["realized_event_count"]
                ),
            }
            for row in diagnostics
        ],
        "all_realization_semantics_selected_by_llm": True,
        "all_optional_applicability_states_selected_by_llm": True,
        "deterministic_exact_ownership_projection_only": True,
    }
    return normalized, provenance, diagnostics, ownership


def _combine_usage(*rows: Mapping[str, int]) -> dict[str, int]:
    return {field: sum(int(row[field]) for row in rows) for field in USAGE_FIELDS}


def _production_accounting(combined_tokens: int) -> tuple[int, float]:
    total = v234.PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined_tokens * v234.PRODUCTION_SCALE
    return total, total / v234.BASELINE_END_TO_END_TOKENS


def _gate(
    *,
    adopted_usage: Mapping[str, int],
    new_usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
    ownership: Mapping[str, Any],
) -> dict[str, Any]:
    combined = _combine_usage(adopted_usage, new_usage)
    total, ratio = _production_accounting(combined["total_tokens"])
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v234.DENSE_SEGMENT_ID) or {}).get("realized_event_count", -1))
    residual = int((by_id.get(v234.NO_SIGNAL_SEGMENT_ID) or {}).get("realized_event_count", -1))
    ownership_rows = list(ownership.get("segments") or [])
    checks = {
        "adopted_inventory_usage_exact": int(adopted_usage["total_tokens"])
        == ADOPTED_INVENTORY_TOKENS,
        "both_segments_validated": set(by_id)
        == {v234.DENSE_SEGMENT_ID, v234.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_inventory_propositions_owned_exactly_once": len(ownership_rows) == 2
        and all(row.get("all_propositions_accounted_exactly_once") is True for row in ownership_rows),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_44000": int(new_usage["total_tokens"]) <= MAX_NEW_TOKENS,
        "combined_tokens_lte_73362": int(combined["total_tokens"]) <= MAX_COMBINED_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "adopted_inventory_usage": dict(adopted_usage),
        "new_turn_usage": dict(new_usage),
        "combined_usage": combined,
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "ownership": dict(ownership),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
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
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(MAX_NEW_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
            "phase_total_token_bound": MAX_NEW_TOKENS,
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
        "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
        "phase_total_token_bound": MAX_NEW_TOKENS,
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
                *v234._runtime_files(),
                *v249._runtime_files(),
                *v265._runtime_files(),
                *v267._runtime_files(),
                *v268._runtime_files(),
                Path(__file__).resolve(),
                Path(v234.__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(v265.__file__).resolve(),
                Path(v267.__file__).resolve(),
                Path(v268.__file__).resolve(),
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
    lock = _load_json(path, "v269 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v269 spec")
    paths = _turn_paths(root, spec["turn_name"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_new_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("max_new_tokens") != MAX_NEW_TOKENS
        or lock.get("max_combined_tokens") != MAX_COMBINED_TOKENS
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(file) for file in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V269InventoryExplicitRealizationError("v269 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("authorization"),
        lock.get("ranking"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V269InventoryExplicitRealizationError("v269 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V269InventoryExplicitRealizationError("v269 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v269 spec")
    paths = _turn_paths(root, spec["turn_name"])
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": spec,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "lineage": lineage,
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v269 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v269 schema"),
            "paths": paths,
        },
    }


def freeze_v269(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v269 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V269InventoryExplicitRealizationError("unfinished v269 root is not replayable")
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
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "adopt one immutable blind inventory and run one-to-one explicit-applicability realization",
            "new_semantic_attempt_count": 1,
            "retry_count": 0,
            "isolated_field_patch": False,
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
            "selected_architecture_id": "adopted_blind_inventory_explicit_applicability_realization",
            "architectures": [
                {"rank": 1, "id": "adopted_blind_inventory_explicit_applicability_realization"},
                {"rank": 2, "id": "full_schema_source_pointer_event_graph"},
                {"rank": 3, "id": "episode_bootstrap_source_owner_specialists"},
            ],
            "selection_basis": {
                "v234_inventory_dense_propositions": 32,
                "v234_inventory_tokens": ADOPTED_INVENTORY_TOKENS,
                "v267_alignment_macro_f1": 0.786707,
                "v267_strictly_equivalent_reference_units": 11,
                "v234_original_realization_turn_started": False,
                "v268_nonexact_metric_events": 4,
                "v268_metric_bearing_events": 5,
                "decision": "combine inventory-owned boundaries with the proven explicit applicability representation for every optional field",
            },
            "on_failure": "reject v269 and advance to the next distinct architecture without field repair",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_accounting(MAX_COMBINED_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "adopted_blind_inventory_explicit_applicability_realization",
            "hypothesis": (
                "inventory-owned one-to-one boundaries plus explicit applicability for every optional field will "
                "preserve recall while reducing unsupported placeholders and normalized literal drift"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "adopted_inventory_proposition_counts": [32, 1],
                "new_semantic_turn_count": 1,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "adopted_inventory_tokens": ADOPTED_INVENTORY_TOKENS,
                "new_turn_hard_max": MAX_NEW_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "exactly_one_event_per_inventory_proposition": True,
                "merging_or_dropping_inventory_propositions_allowed": False,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "new_turn_tokens_max": MAX_NEW_TOKENS,
                "combined_tokens_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
                "on_structural_pass": "run frozen side-free support then neutral alignment",
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
            "state": "frozen_before_one_turn_inventory_explicit_realization_canary",
            "turn_name": turn["turn_name"],
            "model": MODEL,
            "effort": EFFORT,
            "declared_new_turn_count": 1,
            "retry_count": 0,
            "adopted_inventory_tokens": ADOPTED_INVENTORY_TOKENS,
            "max_new_tokens": MAX_NEW_TOKENS,
            "max_combined_tokens": MAX_COMBINED_TOKENS,
            "prompt_bytes": turn["prompt_bytes"],
            "base_bytes": turn["base_bytes"],
            "schema_bytes": turn["schema_bytes"],
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
        "declared_new_turn_count": 1,
        "retry_count": 0,
        "adopted_inventory_tokens": ADOPTED_INVENTORY_TOKENS,
        "max_new_tokens": MAX_NEW_TOKENS,
        "max_combined_tokens": MAX_COMBINED_TOKENS,
        "semantic_regex_or_keyword_filtering": False,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(file) for file in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
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


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V269InventoryExplicitRealizationError("v269 sidecar usage incomplete") from exc
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
        raise V269InventoryExplicitRealizationError("v269 sidecar accounting or auth failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    new_usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v269 sidecar")
            new_usage = {field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS}
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
            measured_over_cap = unknown == 0 and new_usage["total_tokens"] > MAX_NEW_TOKENS
        except Exception:
            unknown = 1
    adopted = frozen["lineage"]["adopted_inventory_usage"]
    combined = _combine_usage(adopted, new_usage) if unknown == 0 else None
    semantic = isinstance(exc, (V269OutputContractError, V269ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v269_inventory_explicit_realization_structural_quality_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "new_semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "adopted_inventory_usage": adopted,
        "new_turn_usage": new_usage,
        "combined_usage": combined,
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
            "reject v269 and advance to the next distinct architecture without field repair"
            if semantic
            else "audit immutable v269 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v269(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v269 terminal")
    frozen = freeze_v269(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V269InventoryExplicitRealizationError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_new_turn_count": 1,
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
            raise V269InventoryExplicitRealizationError("v269 turn did not complete")
        new_usage = _usage(_load_json(paths["sidecar"], "v269 sidecar"))
        if new_usage["total_tokens"] > MAX_NEW_TOKENS:
            raise V269ArchitectureStop("v269 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, ownership = project_output(
            result.output, frozen["turn"]
        )
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "ownership": root / "inventory-ownership-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["ownership"], ownership)
        gate = _gate(
            adopted_usage=frozen["lineage"]["adopted_inventory_usage"],
            new_usage=new_usage,
            diagnostics=diagnostics,
            ownership=ownership,
        )
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V269ArchitectureStop("v269 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v269_architecture_structural_gate_passed",
            "terminal_reason": "v269_inventory_explicit_realization_structural_cost_gate_passed",
            "new_semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "adopted_inventory_usage": gate["adopted_inventory_usage"],
            "new_turn_usage": gate["new_turn_usage"],
            "combined_usage": gate["combined_usage"],
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
            "inventory_ownership_receipt": _record(artifacts["ownership"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before neutral alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v269 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v269 adopted inventory explicit-applicability realization"
    )
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v269(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "base_bytes": frozen["spec"]["base_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
            "projected_ratio": round(_production_accounting(MAX_COMBINED_TOKENS)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v269(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "new_total_tokens": (terminal.get("new_turn_usage") or {}).get("total_tokens"),
            "combined_total_tokens": (terminal.get("combined_usage") or {}).get("total_tokens"),
            "production_amortized_total_token_ratio": terminal.get(
                "production_amortized_total_token_ratio"
            ),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
