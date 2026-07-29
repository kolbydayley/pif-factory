from __future__ import annotations

"""Run the one-turn v211 source-grounded event-selector canary."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import app_server_dev_selection as selection_module
from . import app_server_judge_v5_calibration_v26_diagnostic as v26
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v201_residual_repair_score as v201
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v208_adaptive_router_canary as v208
from . import app_server_judge_v5_selection_v209_adaptive_router_schema_recovery as v209
from . import app_server_judge_v5_selection_v210_adaptive_router_nonacceptance as v210
from . import app_server_judge_v5_selection_v211_event_selector_design as v211
from . import app_server_llm_judge as llm_judge_module
from . import codex_app_server as codex_app_server_module
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_dev_selection import BASELINE_REPAIRED_SYSTEM
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .labels import ValidationError, _validate_schema
from .util import now_iso


V212_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V212_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V212_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v212_spec_v1"
V212_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v212_runtime_lock_v1"
V212_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v212_launch_v1"
V212_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v212_gate_v1"
V212_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_v212_score_private_v1"
V212_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v212_failure_v1"
V212_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v212_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v212_event_selector_canary"
TURN_NAME = "selection_event_selector_canary_00"
SYSTEM_ID = "event_selector_v212_existing_event_ids"
DEFAULT_OUTPUT_ROOT = (
    v211.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v212-event-selector-canary"
).resolve()


class JudgeV5SelectionV212Error(RuntimeError):
    """The event-selector canary cannot continue safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v211_authorization(
    design_root: Path = v211.DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    root = design_root.expanduser().resolve()
    paths = {
        "terminal": root / "terminal.json",
        "design": root / "event-selector-design.json",
        "spec": root / "event-selector-design-spec.json",
        "oracle": root / "event-selector-oracle-audit.json",
        "input": root / "selector-input.private.json",
        "provenance": root / "selector-provenance.private.json",
        "prompt": root / "selector-prompt.private.md",
        "schema": root / "selector-schema.json",
        "instructions": root / "selector-instructions.private.md",
    }
    values = {
        name: _load_json(path, f"v211 {name}")
        for name, path in paths.items()
        if name not in {"prompt", "instructions"}
    }
    prompt = paths["prompt"].read_text(encoding="utf-8")
    instructions = paths["instructions"].read_text(encoding="utf-8")
    terminal = values["terminal"]
    design = values["design"]
    oracle = values["oracle"]
    frozen = design.get("frozen_inputs") or {}
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v211_zero_extraction_event_selector_canary_authorized"
        or terminal.get("semantic_attempt_authorized") is not True
        or terminal.get("extraction_model_calls_authorized") != 0
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("design") != _record(paths["design"])
        or terminal.get("oracle") != _record(paths["oracle"])
        or terminal.get("spec") != _record(paths["spec"])
        or design.get("model") != v211.MODEL
        or design.get("reasoning_effort") != v211.EFFORT
        or design.get("declared_turn_count") != 1
        or design.get("selector_runtime_total_token_bound")
        != v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND
        or design.get("selector_promotion_total_token_gate")
        != v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE
        or design.get("extraction_model_calls_authorized") != 0
        or design.get("extraction_replay_allowed") is not False
        or design.get("batch_5_replay_allowed") is not False
        or design.get("semantic_output_scope")
        != "opaque_existing_event_id_verdicts_only_no_event_generation_or_rewrite"
        or design.get("candidate_event_count") != 79
        or design.get("holdout_authorized") is not False
        or oracle.get("perfect_selector_mean_f1") != 0.81506
        or oracle.get("mean_f1_regret_to_oracle") != 0.017481
        or oracle.get("maximum_dense_case_regret_to_oracle") != 0.124286
        or oracle.get("selected_extraction_cost_tokens") != 166_642.75
        or not all((oracle.get("checks") or {}).values())
        or any(frozen.get(name) != _record(paths[name]) for name in frozen)
        or llm_judge_module.validate_app_server_output_schema_subset(values["schema"])
    ):
        raise JudgeV5SelectionV212Error("v211 authorization drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV212Error("v211 artifact binding drifted")
    predecessor = v211._validate_v210_checkpoint()
    rows = v211._signal_rows(predecessor)
    packet, provenance = v211._selector_packet(
        rows=rows, sources=v192._all_composite_score()[0]
    )
    if (
        values["input"] != packet
        or values["provenance"] != provenance
        or prompt != v211._selector_prompt(packet)
        or values["schema"] != v211._selector_schema(packet)
        or instructions != v211.selector_instructions()
    ):
        raise JudgeV5SelectionV212Error("v211 selector request drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "design": design,
        "oracle": oracle,
        "input": values["input"],
        "provenance": values["provenance"],
        "schema": values["schema"],
        "prompt": prompt,
        "instructions": instructions,
        "predecessor": predecessor,
        "rows": rows,
    }


def validate_selector_output(
    output: Any, predecessor: Mapping[str, Any]
) -> list[str]:
    if not isinstance(output, dict):
        return ["output_not_object"]
    try:
        _validate_schema(dict(predecessor["schema"]), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    expected_cases = [str(row["case_id"]) for row in predecessor["input"]["cases"]]
    if [row.get("case_id") for row in output.get("cases") or []] != expected_cases:
        return ["case_order_or_coverage"]
    input_by_case = {
        str(row["case_id"]): row for row in predecessor["input"]["cases"]
    }
    for case in output["cases"]:
        expected_events = [
            str(row["event_id"])
            for row in input_by_case[str(case["case_id"])]["candidate_events"]
        ]
        decisions = case["decisions"]
        if [row.get("event_id") for row in decisions] != expected_events:
            return ["event_order_or_coverage"]
        kept = set()
        for decision in decisions:
            event_id = str(decision["event_id"])
            verdict = str(decision["verdict"])
            canonical = str(decision["canonical_event_id"])
            if verdict == "keep":
                if canonical != event_id:
                    return ["keep_canonical_link"]
                kept.add(event_id)
            elif verdict == "duplicate":
                if canonical not in kept:
                    return ["duplicate_canonical_link"]
            elif canonical:
                return ["nonselected_canonical_link"]
        if len(kept) > v211.MAX_EVENTS_PER_CASE:
            return ["selected_event_cap"]
    return []


def _selected_witness_ids(
    output: Mapping[str, Any], predecessor: Mapping[str, Any]
) -> dict[str, set[str]]:
    private_by_case = {
        str(row["case_id"]): row for row in predecessor["provenance"]["cases"]
    }
    selected: dict[str, set[str]] = {}
    for case in output["cases"]:
        case_id = str(case["case_id"])
        witness_by_event = {
            str(row["event_id"]): str(row["witness_id"])
            for row in private_by_case[case_id]["events"]
        }
        selected[case_id] = {
            witness_by_event[str(decision["event_id"])]
            for decision in case["decisions"]
            if decision["verdict"] == "keep"
        }
    return selected


def _selector_score_sources(
    output: Mapping[str, Any], predecessor: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    base_sources, _score, _combinations = v192._all_composite_score()
    mapping = copy.deepcopy(base_sources["mapping"])
    membership = copy.deepcopy(base_sources["membership"])
    selected_by_case = _selected_witness_ids(output, predecessor)
    case_to_segment = {
        str(row["case_id"]): str(row["segment_id"])
        for row in predecessor["rows"]
    }
    selected_by_segment = {
        case_to_segment[case_id]: witness_ids
        for case_id, witness_ids in selected_by_case.items()
    }
    for row in predecessor["rows"]:
        selected_by_segment.setdefault(str(row["segment_id"]), set())
    mapping_by_segment = {
        str(row["case_key"]): row for row in mapping["cases"]
    }
    membership["systems"][SYSTEM_ID] = {
        "kind": "llm_source_grounded_existing_event_id_selection",
        "semantic_rewrite": False,
    }
    membership["system_cases"][SYSTEM_ID] = {}
    for segment_id in membership["segment_order"]:
        selected_ids = selected_by_segment.get(str(segment_id), set())
        event_hashes = []
        exact_hashes = []
        for witness in mapping_by_segment[str(segment_id)]["witnesses"]:
            witness_id = str(witness["witness_id"])
            if witness_id not in selected_ids:
                continue
            provenance = witness["provenance"]
            event_hash = str(provenance["canonical_event_sha256"])
            exact = any(
                row.get("submitted_evidence_exact") is True
                for row in provenance.get("memberships") or []
            )
            event_hashes.append(event_hash)
            if exact:
                exact_hashes.append(event_hash)
            provenance["memberships"].append(
                {
                    "system_id": SYSTEM_ID,
                    "submitted_evidence_exact": exact,
                    "llm_selected_existing_event_id": True,
                }
            )
        if len(event_hashes) != len(selected_ids):
            raise JudgeV5SelectionV212Error("selected witness provenance drifted")
        membership["system_cases"][SYSTEM_ID][str(segment_id)] = {
            "event_hashes": sorted(event_hashes),
            "exact_evidence_event_count": len(set(exact_hashes)),
            "status": "coded" if event_hashes else "no_submitted_events",
            "submitted_event_count": len(event_hashes),
        }
    return {**base_sources, "mapping": mapping, "membership": membership}, {
        "selected_case_count": len(selected_by_case),
        "selected_event_count": sum(len(value) for value in selected_by_case.values()),
        "selected_by_case": {
            case_id: sorted(witness_ids)
            for case_id, witness_ids in sorted(selected_by_case.items())
        },
    }


def _score_selector(
    *,
    output: Mapping[str, Any],
    usage: Mapping[str, int],
    predecessor: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors = validate_selector_output(output, predecessor)
    if errors:
        raise JudgeV5SelectionV212Error("selector output invalid: " + ";".join(errors))
    sources, build_audit = _selector_score_sources(output, predecessor)
    consensus, consensus_record = v201._base_consensus()
    selected_case_ids = {str(row["case_id"]) for row in predecessor["rows"]}
    selected_consensus = {
        **consensus,
        "cases": [
            row
            for row in consensus["cases"]
            if str(row["case_id"]) in selected_case_ids
        ],
    }
    score = v186._score_subset(
        consensus=selected_consensus,
        selected_case_ids=selected_case_ids,
        sources=sources,
    )
    candidate = {
        str(row["segment_id"]): row
        for row in score["systems"][SYSTEM_ID]["cases"]
    }
    base = {
        str(row["segment_id"]): row
        for row in score["systems"][f"arm:{v211.BASE_ARM}:normalized"]["cases"]
    }
    route_by_segment = {
        str(row["segment_id"]): row for row in predecessor["rows"]
    }
    private_rows = []
    no_signal_passed = True
    dense_improvements = 0
    dense_regrets = []
    selected_total = 0.0
    max_events = 0
    for segment_id, route in route_by_segment.items():
        row = candidate[segment_id]
        f1 = float(row["f1"])
        regret = float(route["affordable_oracle_f1"]) - f1
        is_no_signal = route["density_stratum"] == "no_signal"
        if is_no_signal:
            no_signal_passed = no_signal_passed and f1 == 1.0
        else:
            dense_improvements += f1 > float(base[segment_id]["f1"])
            dense_regrets.append(regret)
        count = int(
            sources["membership"]["system_cases"][SYSTEM_ID][segment_id][
                "submitted_event_count"
            ]
        )
        max_events = max(max_events, count)
        selected_total += f1
        private_rows.append(
            {
                "case_id": route["case_id"],
                "segment_id": segment_id,
                "source_id": route["source_id"],
                "density_stratum": route["density_stratum"],
                "coverage": route["coverage"],
                "base_f1": base[segment_id]["f1"],
                "candidate_f1": round(f1, 6),
                "affordable_oracle_f1": route["affordable_oracle_f1"],
                "oracle_regret": round(regret, 6),
                "selected_event_count": count,
            }
        )
    candidate_mean = selected_total / len(private_rows)
    oracle_mean = float(predecessor["oracle"]["best_affordable_oracle_mean_f1"])
    mean_regret = oracle_mean - candidate_mean
    exact_count = sum(
        int(row["submitted_nonexact_evidence_events"])
        for row in candidate.values()
    )
    joint = (
        float(predecessor["oracle"]["selected_extraction_cost_tokens"])
        + v210.EXPECTED_USAGE["total_tokens"]
        + int(usage["total_tokens"])
    )
    scope_budget = float(predecessor["oracle"]["scope_budget_tokens"])
    checks = {
        "schema_order_coverage_and_canonical_links": True,
        "all_four_no_signal_cases_candidate_f1_1": no_signal_passed,
        "dense_cases_improved_over_base_min_3": dense_improvements >= 3,
        "mean_f1_regret_lte_0_05": mean_regret <= 0.05 + 1e-12,
        "maximum_dense_case_regret_lte_0_15": max(dense_regrets) <= 0.15 + 1e-12,
        "selected_event_cap_lte_32": max_events <= v211.MAX_EVENTS_PER_CASE,
        "selected_evidence_exact": exact_count == 0,
        "actual_selector_total_tokens_lte_35000": int(usage["total_tokens"])
        <= v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE,
        "actual_extraction_router_and_selector_within_scope_budget": joint
        <= scope_budget,
    }
    gate = {
        "schema_version": V212_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "case_count": len(private_rows),
        "signal_case_count": len(dense_regrets),
        "no_signal_case_count": len(private_rows) - len(dense_regrets),
        "dense_improvement_count": dense_improvements,
        "candidate_mean_f1": round(candidate_mean, 6),
        "best_affordable_oracle_mean_f1": round(oracle_mean, 6),
        "mean_f1_regret_to_oracle": round(mean_regret, 6),
        "maximum_dense_case_regret_to_oracle": round(max(dense_regrets), 6),
        "maximum_selected_event_count": max_events,
        "normalized_nonexact_evidence_events": exact_count,
        "selected_extraction_cost_tokens": predecessor["oracle"][
            "selected_extraction_cost_tokens"
        ],
        "measured_router_total_tokens": v210.EXPECTED_USAGE["total_tokens"],
        "actual_selector_usage": dict(usage),
        "actual_joint_canary_tokens": round(joint, 6),
        "scope_budget_tokens": scope_budget,
        "full_development_router_authorized": all(checks.values()),
        "full_development_selector_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    private_score = {
        "schema_version": V212_SCORE_VERSION,
        "cases": private_rows,
        "gate": gate,
        "score": score,
        "build_audit": {
            **build_audit,
            "base_consensus": consensus_record,
        },
    }
    return gate, private_score


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V212_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v211_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "terminal"
            ]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V212_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": bound,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(v211.__file__).resolve(),
                Path(v210.__file__).resolve(),
                Path(v209.__file__).resolve(),
                Path(v208.__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(v201.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v188.__file__).resolve(),
                Path(v186.__file__).resolve(),
                Path(v26.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(codex_app_server_module.__file__).resolve(),
                Path(selection_module.__file__).resolve(),
                Path(llm_judge_module.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    capacity: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V212_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v211_authorization": [
            predecessor["records"][name]
            for name in (
                "terminal",
                "design",
                "spec",
                "oracle",
                "input",
                "provenance",
                "prompt",
                "schema",
                "instructions",
            )
        ],
        "v210_checkpoint": list(predecessor["predecessor"]["records"].values()),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path, design_root=predecessor["root"])
    return path


def verify_runtime_lock(
    path: Path, *, design_root: Path = v211.DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    lock = _load_json(path, "v212 runtime lock")
    predecessor = _validate_v211_authorization(design_root)
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    expected_v211 = [
        predecessor["records"][name]
        for name in (
            "terminal",
            "design",
            "spec",
            "oracle",
            "input",
            "provenance",
            "prompt",
            "schema",
            "instructions",
        )
    ]
    if (
        lock.get("schema_version") != V212_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v211_authorization") != expected_v211
        or lock.get("v210_checkpoint")
        != list(predecessor["predecessor"]["records"].values())
    ):
        raise JudgeV5SelectionV212Error("v212 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v211_authorization") or []),
        *(lock.get("v210_checkpoint") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV212Error("v212 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV212Error("v212 policy/audit cross-link drifted")
    return lock


def freeze_v212(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    design_root: Path = v211.DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v211.TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v212 terminal")}
    if any(root.iterdir()):
        raise JudgeV5SelectionV212Error("v212 root is nonempty without a terminal")
    predecessor = _validate_v211_authorization(design_root)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["input"],
        prompt=predecessor["prompt"],
        schema=predecessor["schema"],
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V212_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "source_grounded_existing_event_id_selection_canary",
        "model": v211.MODEL,
        "reasoning_effort": v211.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
        "promotion_total_token_gate": v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE,
        "v209_router_turn_replayed": False,
        "v209_router_output_reused": True,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "full_development_router_authorized_before_canary_pass": False,
        "full_development_selector_authorized_before_route_budget": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "v211_authorization": predecessor["records"],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions": predecessor["records"]["instructions"],
        },
        "privacy": "private_source_prompts_outputs_sanitized_counts_hashes_metrics_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    runtime_lock = _freeze_runtime_lock(
        root=root,
        predecessor=predecessor,
        spec_path=spec_path,
        capacity=capacity,
    )
    return {
        "root": root,
        "predecessor": predecessor,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "capacity_audit": capacity["audit"],
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": runtime_lock,
    }


def _freeze_launch_receipt(frozen: Mapping[str, Any]) -> Path:
    path = frozen["root"] / "launch-receipt.json"
    if path.exists():
        receipt = _load_json(path, "v212 launch receipt")
        if (
            receipt.get("schema_version") != V212_LAUNCH_VERSION
            or receipt.get("runtime_lock") != _record(frozen["runtime_lock"])
            or receipt.get("attempt_spec") != _record(frozen["spec_path"])
            or receipt.get("capacity_policy") != _record(frozen["capacity_policy"])
            or receipt.get("capacity_checkpoint_exists_before_launch") is not False
            or receipt.get("sidecar_exists_before_launch") is not False
            or receipt.get("output_exists_before_launch") is not False
        ):
            raise JudgeV5SelectionV212Error("v212 launch receipt drifted")
        return path
    receipt = {
        "schema_version": V212_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "state": "semantic_attempt_not_started",
        "declared_turn_count": 1,
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "capacity_checkpoint_exists_before_launch": frozen["paths"][
            "capacity"
        ].exists(),
        "sidecar_exists_before_launch": frozen["paths"]["sidecar"].exists(),
        "output_exists_before_launch": frozen["paths"]["output"].exists(),
        "production_mutated": False,
    }
    if any(
        receipt[key]
        for key in (
            "capacity_checkpoint_exists_before_launch",
            "sidecar_exists_before_launch",
            "output_exists_before_launch",
        )
    ):
        raise JudgeV5SelectionV212Error("v212 semantic artifacts predate launch")
    _write_stable_time(path, receipt, "created_at")
    return path


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(
                _load_json(Path(sidecar_record["path"]), "v212 failed sidecar")
            )
        except Exception:
            unknown += 1
            continue
        known = _sum_usage(known, usage)
    complete = unknown == 0
    cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], known
    )
    failure = {
        "schema_version": V212_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V212_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "full_development_selector_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
        + unknown * v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v212(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    design_root: Path = v211.DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v211.TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v212 terminal")
    frozen = freeze_v212(
        output_dir=root,
        design_root=design_root,
        timeout_seconds=timeout_seconds,
    )
    verify_runtime_lock(frozen["runtime_lock"], design_root=design_root)
    launch_path = _freeze_launch_receipt(frozen)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["predecessor"]["prompt"],
                schema=frozen["predecessor"]["schema"],
                base_instructions=frozen["predecessor"]["instructions"],
                model=v211.MODEL,
                effort=v211.EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=4,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_selector_output(
                    value, frozen["predecessor"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        gate, private_score = _score_selector(
            output=output,
            usage=accounting["usage"],
            predecessor=frozen["predecessor"],
        )
        gate_path = root / "event-selector-canary-gate.json"
        score_path = root / "event-selector-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V212_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v212_event_selector_canary_passed_full_router_authorized"
                if passed
                else "v212_event_selector_canary_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v209_router_turn_replayed": False,
            "extraction_model_calls_started": 0,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(gate_path),
            "private_score": _record(score_path),
            "full_development_router_authorized": passed,
            "full_development_selector_authorized": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": frozen["predecessor"][
                "terminal"
            ]["cumulative_unknown_usage_turn_count"],
            "cumulative_conservative_unknown_usage_upper_bound": frozen[
                "predecessor"
            ]["terminal"]["cumulative_conservative_unknown_usage_upper_bound"],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v213-full-coverage-router"
                    / "terminal.json"
                )
                if passed
                else None
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v212 event-selector canary")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--design-root", default=str(v211.DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=v211.TIMEOUT_SECONDS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze_only:
        frozen = freeze_v212(
            output_dir=Path(args.output_dir),
            design_root=Path(args.design_root),
            timeout_seconds=args.timeout_seconds,
        )
        result = {
            "state": frozen["spec"]["state"],
            "runtime_lock": str(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutated": False,
        }
    else:
        terminal = asyncio.run(
            run_v212(
                output_dir=Path(args.output_dir),
                design_root=Path(args.design_root),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "full_development_router_authorized": terminal.get(
                "full_development_router_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
            "usage_status": terminal.get("usage_status"),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
