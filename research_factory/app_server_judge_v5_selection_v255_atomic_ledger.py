from __future__ import annotations

"""Run a one-turn atomic-proposition ledger plus full-event realization canary."""

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
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v255_atomic_ledger_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v255_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v255_terminal_v1"
PHASE_ID = "development_selection_v5_4_v255_atomic_ledger"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 55_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 24_000
MAX_SCHEMA_BYTES = 96_000
MAX_PROPOSITIONS_PER_SEGMENT = 32
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V254_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v254-local-owner-adjudication-v249"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v255-atomic-ledger"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1

EXPECTED_LINEAGE_HASHES = {
    "v249_terminal": "56497104fa40aee405a769f39fba5ba082418e3149909222708954e63bb06a5d",
    "v249_runtime_lock": "5f20ee4aef5ca6a997786a5590fd78b7bd92fd3724d9a274ba273c3fdd53c796",
    "v254_terminal": "0f741e6b398402a48bb62480eacdd6563ce1ddc3fcf177c486a49b8e3c71b1c2",
    "v254_runtime_lock": "babd1ffb6598020c5d1c665517d1ef75f7ae117b16dace214102872ede43995c",
    "v254_score": "66951d4e84184302abd02909728e28cbf68739089b664ba7e5d663f708f4428b",
    "v254_sidecar": "b2aa637a231dc530cbedceab0231757ca3c886c9b7f1bd45e8f0ed58cf2e3be9",
}

ATOMIC_LEDGER_INSTRUCTIONS = """Before realizing any full events, build a complete atomic proposition ledger for each segment from the blind source packet. An atomic proposition is the smallest independently truth-evaluable source-supported claim that should become one event under the supplied extraction rules. Keep distinct actors, targets, stances, causal claims, metrics, and time horizons in separate propositions whenever combining them would change truth conditions. Do not split one proposition merely because it supports multiple metadata fields.

For each segment, list every atomic proposition in source order as P00, P01, and so on. Then realize exactly one full event for each proposition, in the same order, carrying the same proposition_id. Never add, drop, merge, or split propositions during event realization. The ledger and event evidence ranges are semantic selections made by you; choose exact source units and exact evidence under the existing evidence contract. A genuine no-signal segment must have an empty proposition ledger and an empty events array. Do not expose or infer a reference answer, target count, or prior event list. Return schema-valid JSON only."""


class V255AtomicLedgerError(RuntimeError):
    """The v255 architecture cannot proceed or be adopted safely."""


class V255OutputContractError(V255AtomicLedgerError):
    """A completed output violated the frozen atomic-ledger contract."""


class V255ArchitectureStop(V255AtomicLedgerError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V255AtomicLedgerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V255AtomicLedgerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V255AtomicLedgerError(f"frozen {path.name} drifted")
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


def _lineage_paths() -> dict[str, Path]:
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v254_terminal": V254_ROOT / "terminal.json",
        "v254_runtime_lock": V254_ROOT / "runtime-lock.json",
        "v254_score": V254_ROOT / "reconciled-alignment-score.json",
        "v254_sidecar": V254_ROOT
        / "turns/v254-local-side-free-owner-adjudication/sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V255AtomicLedgerError(f"frozen lineage {name} drifted")
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    t254 = _load_json(paths["v254_terminal"], "v254 terminal")
    q254 = _load_json(paths["v254_score"], "v254 score")
    s254 = _load_json(paths["v254_sidecar"], "v254 sidecar")
    if (
        t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or (t249.get("usage") or {}).get("total_tokens") != 44_474
        or t249.get("production_amortized_total_token_ratio") != 0.192218
        or t254.get("terminal_reason")
        != "v254_local_owner_adjudication_quality_gate_not_passed"
        or q254.get("failed_checks")
        != [
            "strict_full_field_macro_noninferior_margin_0_03",
            "no_material_source_macro_regression",
        ]
        or (q254.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.96
        or q254.get("mismatch_field_counts")
        != {"event_boundary": 2, "evidence": 2, "target": 1}
        or s254.get("usage_status") != "measured"
        or s254.get("usage_complete") is not True
        or (s254.get("usage") or {}).get("total_tokens") != 54_092
        or t249.get("production_mutated") is not False
        or t254.get("production_mutated") is not False
        or t254.get("holdout_authorized") is not False
    ):
        raise V255AtomicLedgerError("v255 predecessor evidence drifted")
    return {
        "paths": paths,
        "records": records,
        "source": v249._load_frozen(V249_ROOT)["turn"],
    }


def _ledger_schema(v249_schema: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(v249_schema))
    segment = schema["properties"]["segments"]["items"]
    segment_props = segment["properties"]
    events = segment_props["events"]
    event_item = events["items"]
    event_props = event_item["properties"]
    proposition_ids = [f"P{index:02d}" for index in range(MAX_PROPOSITIONS_PER_SEGMENT)]
    proposition_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "proposition_id",
            "evidence_start_unit_id",
            "evidence_end_unit_id",
            "proposition_text",
        ],
        "properties": {
            "proposition_id": {"type": "string", "enum": proposition_ids},
            "evidence_start_unit_id": copy.deepcopy(
                event_props["evidence_start_unit_id"]
            ),
            "evidence_end_unit_id": copy.deepcopy(event_props["evidence_end_unit_id"]),
            "proposition_text": {"type": "string", "minLength": 1, "maxLength": 500},
        },
    }
    proposition_ledger = {
        "type": "array",
        "minItems": 0,
        "maxItems": MAX_PROPOSITIONS_PER_SEGMENT,
        "items": proposition_schema,
    }
    event_item["properties"] = {
        "proposition_id": {"type": "string", "enum": proposition_ids},
        **event_props,
    }
    event_item["required"] = ["proposition_id", *event_item["required"]]
    new_segment_props: dict[str, Any] = {}
    for name, value in segment_props.items():
        if name == "events":
            new_segment_props["atomic_propositions"] = proposition_ledger
        new_segment_props[name] = value
    segment["properties"] = new_segment_props
    required = list(segment["required"])
    required.insert(required.index("events"), "atomic_propositions")
    segment["required"] = required
    return schema


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source"]
    schema = _ledger_schema(source["schema"])
    base = source["base"] + "\n\n# Atomic proposition ledger\n" + ATOMIC_LEDGER_INSTRUCTIONS + "\n"
    sizes = {
        "prompt_bytes": len(source["prompt"].encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V255AtomicLedgerError("v255 request size cap exceeded")
    return {
        "turn_name": "v255_atomic_ledger_" + sha256_text(source["episode_id"])[:20],
        "episode_id": source["episode_id"],
        "segment_ids": list(source["segment_ids"]),
        "private_input": copy.deepcopy(source["private_input"]),
        "prompt": source["prompt"],
        "base": base,
        "schema": schema,
        "v249_schema": copy.deepcopy(source["schema"]),
        "direct_schema": copy.deepcopy(source["direct_schema"]),
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V255OutputContractError("atomic-ledger output schema failed") from exc
    direct = copy.deepcopy(dict(output))
    ledger_rows: list[dict[str, Any]] = []
    for segment in direct.get("segments") or []:
        segment_id = str(segment["segment_id"])
        propositions = list(segment.pop("atomic_propositions"))
        events = list(segment.get("events") or [])
        expected_ids = [f"P{index:02d}" for index in range(len(propositions))]
        proposition_ids = [str(row["proposition_id"]) for row in propositions]
        event_ids = [str(row["proposition_id"]) for row in events]
        if proposition_ids != expected_ids or event_ids != expected_ids:
            raise V255OutputContractError(
                "atomic propositions and events must have ordered one-to-one IDs"
            )
        for event in events:
            event.pop("proposition_id")
        ledger_rows.append(
            {
                "segment_id": segment_id,
                "proposition_count": len(propositions),
                "event_count": len(events),
                "ordered_one_to_one": True,
                "proposition_id_sequence_sha256": sha256_text(
                    _canonical_json(proposition_ids)
                ),
            }
        )
    v249_turn = dict(turn)
    v249_turn["schema"] = turn["v249_schema"]
    normalized, provenance, diagnostics, applicability = v249.project_output(
        direct, v249_turn
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "segments": ledger_rows,
        "all_proposition_semantics_selected_by_llm": True,
        "one_to_one_structure_validated_deterministically": True,
        "applicability": applicability,
    }
    return normalized, provenance, diagnostics, receipt


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "v249_schema": turn_root / "v249-projection-schema.json",
        "direct_schema": turn_root / "direct-projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [
        _record(paths[name])
        for name in ("input", "prompt", "base", "schema", "v249_schema", "direct_schema")
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
                *v249._runtime_files(),
                Path(__file__).resolve(),
                Path(v249.__file__).resolve(),
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


def _production_ratio(tokens: int) -> tuple[int, float]:
    return v249._production_ratio(tokens)


def _gate(
    *,
    usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
    ledger_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v249.v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int(
        (by_id.get(v249.v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1)
    )
    production_total, ratio = _production_ratio(int(usage["total_tokens"]))
    ledger_rows = list(ledger_receipt.get("segments") or [])
    checks = {
        "both_segments_validated": set(by_id)
        == {v249.v239.DENSE_SEGMENT_ID, v249.v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(
            int(row["unresolved_count"]) == 0 for row in diagnostics
        ),
        "atomic_proposition_event_bijection": len(ledger_rows) == 2
        and all(
            row.get("ordered_one_to_one") is True
            and int(row["proposition_count"]) == int(row["event_count"])
            for row in ledger_rows
        ),
        "explicit_applicability_projection_passed": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "total_tokens_lte_55000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v255 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v255 spec")
    paths = _turn_paths(root, spec["turn_name"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("max_total_tokens") != MAX_TOTAL_TOKENS
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(file) for file in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V255AtomicLedgerError("v255 runtime lock contract drifted")
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
        raise V255AtomicLedgerError("v255 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V255AtomicLedgerError("v255 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v255 spec")
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
            "private_input": _load_json(paths["input"], "v255 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v255 schema"),
            "v249_schema": _load_json(paths["v249_schema"], "v249 projection schema"),
            "direct_schema": _load_json(paths["direct_schema"], "direct projection schema"),
            "paths": paths,
        },
    }


def freeze_v255(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v255 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V255AtomicLedgerError("unfinished v255 root is not replayable")
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
    _write_immutable(paths["v249_schema"], turn["v249_schema"])
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "one blind single-turn atomic-ledger development canary",
            "semantic_attempt_count": 1,
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
            "selected_architecture_id": "single_turn_atomic_ledger_one_to_one_realization",
            "architectures": [
                {
                    "rank": 1,
                    "id": "single_turn_atomic_ledger_one_to_one_realization",
                    "decision": "selected for smallest decision-changing canary",
                },
                {
                    "rank": 2,
                    "id": "overlapping_evidence_window_maps_then_global_llm_owner_reduce",
                    "decision": "defer because the extra semantic pass has unresolved cost risk",
                },
                {
                    "rank": 3,
                    "id": "episode_bootstrap_segment_specialists_then_global_llm_join",
                    "decision": "defer because repeated transcript consumption projects above target",
                },
            ],
            "measured_basis": {
                "v249_total_tokens": 44_474,
                "v249_structural_dense_events": 32,
                "v249_production_ratio": 0.192218,
                "v254_authoritative_macro_f1": 0.96,
                "v254_strict_reference_units": "23/27",
                "v254_remaining_mismatch_fields": {
                    "event_boundary": 2,
                    "evidence": 2,
                    "target": 1,
                },
                "v234_inventory_total_tokens": 29_362,
                "v238_specialist_total_tokens": 33_721,
                "segment_specialist_conservative_cost_conclusion": "above_0_28_before_join",
            },
            "on_failure": "freeze and reject atomic-ledger architecture; advance to rank 2 without a field patch",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(MAX_TOTAL_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "single_turn_atomic_ledger_one_to_one_realization",
            "hypothesis": (
                "forcing a complete atomic proposition ledger before one-to-one full-event realization "
                "in the same blind Sol-high turn will preserve v249 coverage and applicability quality "
                "while preventing the merged or mis-scoped boundaries that left v249 at 23/27 strict units"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "reference_visible_to_model": False,
                "prior_event_list_visible": False,
                "target_count_visible_to_model": False,
                "declared_semantic_turn_count": 1,
            },
            "production_cost_projection": {
                "turn_hard_max": MAX_TOTAL_TOKENS,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "atomic_proposition_event_bijection": True,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "total_tokens_max": MAX_TOTAL_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
                "on_structural_pass": "run frozen side-free support then alignment; do not open holdout",
                "on_failure": "reject architecture with no isolated field repair",
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
            "state": "frozen_before_one_turn_atomic_ledger_canary",
            "turn_name": turn["turn_name"],
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "max_total_tokens": MAX_TOTAL_TOKENS,
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
        "declared_turn_count": 1,
        "retry_count": 0,
        "max_total_tokens": MAX_TOTAL_TOKENS,
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
        raise V255AtomicLedgerError("v255 sidecar usage incomplete") from exc
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
        raise V255AtomicLedgerError("v255 sidecar accounting or auth failed")
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
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v255 sidecar")
            usage = {
                field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS
            }
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V255OutputContractError, V255ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v255_atomic_ledger_structural_quality_or_cost_gate_not_passed"
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
            "reject v255 and advance to ranked architecture 2 without a field patch"
            if semantic
            else "audit immutable v255 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v255(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v255 terminal")
    frozen = freeze_v255(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V255AtomicLedgerError("launch exists; replay prohibited")
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
            raise V255AtomicLedgerError("v255 turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v255 sidecar"))
        normalized, provenance, diagnostics, ledger_receipt = project_output(
            result.output, frozen["turn"]
        )
        normalized_path = root / "normalized-output.private.json"
        provenance_path = root / "evidence-provenance.private.json"
        diagnostics_path = root / "diagnostics.private.json"
        ledger_path = root / "atomic-ledger-receipt.json"
        _write_immutable(normalized_path, normalized)
        _write_immutable(provenance_path, provenance)
        _write_immutable(diagnostics_path, {"segments": diagnostics})
        _write_immutable(ledger_path, ledger_receipt)
        gate = _gate(
            usage=usage, diagnostics=diagnostics, ledger_receipt=ledger_receipt
        )
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V255ArchitectureStop("v255 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v255_architecture_structural_gate_passed",
            "terminal_reason": "v255_atomic_ledger_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
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
            "gate": _record(gate_path),
            "normalized_output": _record(normalized_path),
            "evidence_provenance": _record(provenance_path),
            "atomic_ledger_receipt": _record(ledger_path),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v255 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v255 atomic-ledger canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v255(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "base_bytes": frozen["spec"]["base_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
            "projected_ratio": round(_production_ratio(MAX_TOTAL_TOKENS)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v255(
                output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
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
