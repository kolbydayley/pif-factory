from __future__ import annotations

"""Run one blind dual-decomposition extraction and LLM reconciliation canary."""

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
from .util import now_iso


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v261_dual_decomposition_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v261_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v261_terminal_v1"
PHASE_ID = "development_selection_v5_4_v261_dual_decomposition"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 55_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 24_000
MAX_SCHEMA_BYTES = 100_000
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V254_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v254-local-owner-adjudication-v249"
).resolve()
V259_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v259-window-map-reduce"
).resolve()
V260_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v260-inventory-global-join"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v261-dual-decomposition"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1

EXPECTED_LINEAGE_HASHES = {
    "v249_terminal": "56497104fa40aee405a769f39fba5ba082418e3149909222708954e63bb06a5d",
    "v249_runtime_lock": "5f20ee4aef5ca6a997786a5590fd78b7bd92fd3724d9a274ba273c3fdd53c796",
    "v254_terminal": "0f741e6b398402a48bb62480eacdd6563ce1ddc3fcf177c486a49b8e3c71b1c2",
    "v254_score": "66951d4e84184302abd02909728e28cbf68739089b664ba7e5d663f708f4428b",
    "v259_terminal": "1abae1a58b56f94f1086024aab6a0b41cc52726f02d1bde83867c7527e8398aa",
    "v259_gate": "3518bb13cb57f197fb099d9f5e3788f949707df2ae5b7d6f04e79a6f81aa01d4",
    "v259_sidecar": "acd8eaef1a1c1e08a002325c5b96e26c1841b8363e053fc695ef0e6e3d422e68",
    "v260_terminal": "781a2692a360e27f63ea791158e1d68b5586519f82f461714772f2f3105f5bd3",
    "v260_runtime_lock": "5e1d715c7144d96b0877ed04f8f0b64f4d7e224b00b98887ad9ed9d5c8ee5c9d",
    "v260_sidecar": "d7d1e38310af00a36077374b3d20859cfe05f7446bca95ba22c9d842643ad5fd",
}

DUAL_DECOMPOSITION_INSTRUCTIONS = """Use one blind source packet to perform two complete, independent semantic decompositions before returning the final extraction. Do not expose either draft in the JSON output.

Pass A is chronological: read every source unit in order and identify each independently truth-valued, explicitly supported, research-useful event with its smallest sufficient evidence range. Pass B is relational: independently rebuild the event set by tracking speaker, actor, reported actor, target, attribution, stance, certainty, negation, temporal horizon, causal mechanism, main predicate, evidence, and metrics across the full segment. Pass B must inspect every unit from the source rather than copying Pass A.

Then perform one global semantic reconciliation. Compare the two decompositions against the source. Preserve a source-supported event found by either pass. Merge only genuine duplicate descriptions of the same truth-conditional event; keep events separate when any material actor, attribution, mechanism, stance, target, time, metric, evidence commitment, or event boundary differs. The LLM alone makes every discovery, eligibility, merge, split, boundary, field, and applicability decision.

Re-read the selected exact evidence range for every reconciled event before populating the final schema. Every material field must be supported by that range. Every nonempty metric string must be a literal contiguous substring of the selected evidence. Do not infer a reference answer, expected count, topic list, or quality hint. Never use keywords, regex, phrase rules, embeddings, or fixed-topic gates. Return only the final schema-valid JSON in source order."""


class V261DualDecompositionError(RuntimeError):
    """The v261 architecture cannot proceed or be adopted safely."""


class V261OutputContractError(V261DualDecompositionError):
    """A completed output violated the frozen exact projection contract."""


class V261ArchitectureStop(V261DualDecompositionError):
    """The measured architecture failed a predeclared gate."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V261DualDecompositionError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V261DualDecompositionError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V261DualDecompositionError(f"frozen {path.name} drifted")
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
        raise V261DualDecompositionError(f"{prefix} turn membership drifted")
    return matches[0]


def _lineage_paths() -> dict[str, Path]:
    v259_turn = _single_turn(V259_ROOT, "v259-window-map-reduce-")
    v260_turn = _single_turn(V260_ROOT, "v260-inventory-global-join-")
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v254_terminal": V254_ROOT / "terminal.json",
        "v254_score": V254_ROOT / "reconciled-alignment-score.json",
        "v259_terminal": V259_ROOT / "terminal.json",
        "v259_gate": V259_ROOT / "architecture-structural-gate.json",
        "v259_sidecar": v259_turn / "sidecar.json",
        "v260_terminal": V260_ROOT / "terminal.json",
        "v260_runtime_lock": V260_ROOT / "runtime-lock.json",
        "v260_sidecar": v260_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V261DualDecompositionError(f"frozen lineage {name} drifted")
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    t254 = _load_json(paths["v254_terminal"], "v254 terminal")
    q254 = _load_json(paths["v254_score"], "v254 score")
    t259 = _load_json(paths["v259_terminal"], "v259 terminal")
    g259 = _load_json(paths["v259_gate"], "v259 gate")
    s259 = _load_json(paths["v259_sidecar"], "v259 sidecar")
    t260 = _load_json(paths["v260_terminal"], "v260 terminal")
    s260 = _load_json(paths["v260_sidecar"], "v260 sidecar")
    if (
        t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or t254.get("terminal_reason")
        != "v254_local_owner_adjudication_quality_gate_not_passed"
        or (q254.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.96
        or q254.get("failed_checks")
        != ["strict_full_field_macro_noninferior_margin_0_03", "no_material_source_macro_regression"]
        or t259.get("terminal_reason")
        != "v259_window_map_reduce_structural_quality_or_cost_gate_not_passed"
        or g259.get("dense_event_count") != 26
        or (s259.get("usage") or {}).get("total_tokens") != 48_059
        or t260.get("terminal_reason")
        != "v260_inventory_global_join_structural_quality_or_cost_gate_not_passed"
        or t260.get("error_class") != "V260OutputContractError"
        or (s260.get("usage") or {}).get("total_tokens") != 35_369
        or any(
            terminal.get("production_mutated") is not False
            for terminal in (t249, t254, t259, t260)
        )
    ):
        raise V261DualDecompositionError("v261 predecessor evidence drifted")
    return {
        "paths": paths,
        "records": records,
        "source": v249._load_frozen(V249_ROOT)["turn"],
    }


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source"]
    base = (
        source["base"]
        + "\n\n# Independent dual decomposition and reconciliation contract\n"
        + DUAL_DECOMPOSITION_INSTRUCTIONS
        + "\n"
    )
    sizes = {
        "prompt_bytes": len(source["prompt"].encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(
            json.dumps(source["schema"], ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ),
    }
    if (
        sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V261DualDecompositionError("v261 request size cap failed")
    return {
        **copy.deepcopy(source),
        "turn_name": "v261_dual_decomposition_" + source["turn_name"].split("_")[-1],
        "base": base,
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        normalized, provenance, diagnostics, applicability = v249.project_output(output, turn)
    except v249.V249OutputContractError as exc:
        raise V261OutputContractError(str(exc)) from exc
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "all_semantic_extraction_and_reconciliation_owned_by_llm": True,
        "deterministic_exact_projection_only": True,
        "applicability": applicability,
    }
    return normalized, provenance, diagnostics, receipt


def _production_ratio(tokens: int) -> tuple[int, float]:
    return v249._production_ratio(tokens)


def _gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]], receipt: Mapping[str, Any]
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v249.v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int((by_id.get(v249.v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    total, ratio = _production_ratio(int(usage["total_tokens"]))
    checks = {
        "both_segments_validated": set(by_id)
        == {v249.v239.DENSE_SEGMENT_ID, v249.v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "explicit_applicability_projection_passed": bool(receipt.get("applicability")),
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
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "projection_receipt": dict(receipt),
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
        "direct_schema": turn_root / "direct-projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema", "direct_schema")]


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
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


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v261 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v261 spec")
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
        raise V261DualDecompositionError("v261 runtime lock contract drifted")
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
        raise V261DualDecompositionError("v261 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V261DualDecompositionError("v261 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v261 spec")
    paths = _turn_paths(root, spec["turn_name"])
    turn = prepare_turn(_validate_lineage())
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": spec,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v261 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v261 schema"),
            "direct_schema": _load_json(paths["direct_schema"], "v261 direct schema"),
            "paths": paths,
        },
    }


def freeze_v261(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v261 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V261DualDecompositionError("unfinished v261 root is not replayable")
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
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "one blind single-turn dual-decomposition extraction canary",
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
            "selected_architecture_id": "single_turn_dual_decomposition_llm_reconciliation",
            "architectures": [
                {"rank": 1, "id": "single_turn_dual_decomposition_llm_reconciliation"},
                {"rank": 2, "id": "episode_bootstrap_segment_specialists_global_llm_join"},
                {"rank": 3, "id": "independent_segment_threads_episode_level_llm_reducer"},
            ],
            "selection_basis": {
                "v249_macro_f1": 0.96,
                "v249_failed_mismatch_field_counts": {"event_boundary": 2, "evidence": 2, "target": 1},
                "v259_dense_events": 26,
                "v260_failure_class": "exact_metric_projection",
                "decision": "test independent full-source decompositions rather than another staged ledger or field repair",
            },
            "on_failure": "reject v261 and advance to a distinct architecture without isolated repair",
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
            "architecture_id": "single_turn_dual_decomposition_llm_reconciliation",
            "hypothesis": (
                "independent chronological and actor-relation decompositions followed by one source-grounded LLM "
                "reconciliation will preserve v249 recall while reducing boundary and full-field misses"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "declared_semantic_turn_count": 1,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
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
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "total_tokens_max": MAX_TOTAL_TOKENS,
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
            "state": "frozen_before_one_turn_dual_decomposition_canary",
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
        raise V261DualDecompositionError("v261 sidecar usage incomplete") from exc
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
        raise V261DualDecompositionError("v261 sidecar accounting or auth failed")
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
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v261 sidecar")
            usage = {field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS}
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
            measured_over_cap = unknown == 0 and usage["total_tokens"] > MAX_TOTAL_TOKENS
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V261OutputContractError, V261ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v261_dual_decomposition_structural_quality_or_cost_gate_not_passed"
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
            "reject v261 and advance to the next distinct architecture without field repair"
            if semantic
            else "audit immutable v261 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v261(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v261 terminal")
    frozen = freeze_v261(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V261DualDecompositionError("launch exists; replay prohibited")
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
            raise V261DualDecompositionError("v261 turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v261 sidecar"))
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V261ArchitectureStop("v261 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, receipt = project_output(result.output, frozen["turn"])
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "receipt": root / "dual-decomposition-projection-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["receipt"], receipt)
        gate = _gate(usage=usage, diagnostics=diagnostics, receipt=receipt)
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V261ArchitectureStop("v261 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v261_architecture_structural_gate_passed",
            "terminal_reason": "v261_dual_decomposition_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": gate["production_amortized_total_token_ratio"],
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
            "projection_receipt": _record(artifacts["receipt"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before neutral alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v261 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v261 dual-decomposition canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v261(output_dir=Path(args.output_dir))
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
            run_v261(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
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
