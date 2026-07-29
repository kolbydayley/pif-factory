from __future__ import annotations

"""Apply the frozen v174 support layer to the five-arm development pool."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v174_exact_evidence_continuation as v174
from .app_server_judge_v5 import build_pointwise_support_input
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
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
from .app_server_v5_reuse import verify_v5_reuse_contract
from .util import now_iso, sha256_text


V175_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_support_v175_spec_v1"
V175_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_support_v175_terminal_v1"
V175_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_support_v175_failure_v1"
V175_DEDUP_VERSION = "pif_app_server_selection_support_exact_claim_dedup_v1"
V175_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V175_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V175_PHASE_ID = "judge_v5_4_selection_v175_support"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_UNITS_PER_TURN = 128
MAX_PROMPT_BYTES = 240_000
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = 1200.0
DEFAULT_REUSE_CONTRACT = (
    v174.DEFAULT_OUTPUT_ROOT.parent / "reuse-contract-v4.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    v174.DEFAULT_OUTPUT_ROOT.parent / "development-selection-v5_4-v175-support"
).resolve()


class JudgeV5SelectionV175Error(RuntimeError):
    """The v175 selection-support evidence contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v174_selection_gate() -> dict[str, Any]:
    root = v174.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "exact-evidence-continuation-score.json",
        "protocol": root / "development-judge-protocol-v174.json",
        "spec": root / "exact-evidence-continuation-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
    }
    values = {name: _load_json(path, f"v174 {name}") for name, path in paths.items()}
    terminal, score, protocol, spec = (
        values["terminal"], values["score"], values["protocol"], values["spec"]
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v174_full_development_calibration_passed_selection_authorized"
        or terminal.get("development_judge_frozen") is not True
        or terminal.get("selection_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("turn_count") != 9
        or terminal.get("usage", {}).get("total_tokens") != 194205
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 5717699
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or protocol.get("schema_version") != v174.V174_PROTOCOL_VERSION
        or protocol.get("support_model") != MODEL
        or protocol.get("reasoning_effort") != EFFORT
        or protocol.get("selection_authorized") is not True
        or protocol.get("retry_count_per_turn") != 0
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5SelectionV175Error("v174 selection gate drifted")
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 9:
        raise JudgeV5SelectionV175Error("v174 attempt coverage drifted")
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            if not isinstance(attempt.get(key), Mapping):
                raise JudgeV5SelectionV175Error(f"v174 {key} record is missing")
            _verify_record(attempt[key])
        _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v174 sidecar"))
    for record in spec["runtime_files"]:
        _verify_record(record)
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV175Error("v174 selection artifact disappeared")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "cumulative_usage": {
            field: int(terminal["cumulative_calibration_usage"][field])
            for field in USAGE_FIELDS
        },
    }


def _validate_selection_sources(reuse_contract_path: Path) -> dict[str, Any]:
    contract = verify_v5_reuse_contract(reuse_contract_path.expanduser().resolve())
    policy = contract["policy"]
    if (
        Path(contract["target_pipeline_root"]).resolve()
        != v174.DEFAULT_OUTPUT_ROOT.parent.resolve()
        or len(contract.get("clean_arms") or []) != 5
        or policy.get("extraction_model_calls_allowed") is not False
        or policy.get("batch_5_same_thread_retry_allowed") is not False
        or policy.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV175Error("pipeline-v5 reuse contract drifted")
    inputs = contract["selection_inputs"]
    for record in inputs.values():
        _verify_record(record)
    for arm in contract["clean_arms"]:
        _verify_record(arm["report"])
    _verify_record(contract["interrupted_batch_5_same_thread"])
    for record in contract["verified_provenance"].values():
        _verify_record(record)
    pool_record = inputs["preassembled_witness_pool"]
    pool = _load_json(Path(pool_record["path"]), "five-arm shared witness pool")
    pointwise = build_pointwise_support_input(pool)
    if len(pool.get("cases") or []) != 32 or len(pointwise.get("units") or []) != 1960:
        raise JudgeV5SelectionV175Error("five-arm witness coverage drifted")
    return {
        "contract": contract,
        "contract_record": _record(reuse_contract_path),
        "pool": pool,
        "pool_record": pool_record,
        "pointwise": pointwise,
    }


def build_exact_claim_dedup(
    pointwise: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, list[str]], dict[str, Any]]:
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for unit in pointwise["units"]:
        key = (str(unit["case_id"]), str(unit["proposition"]["claim_text"]))
        by_key.setdefault(key, []).append(unit)
    representatives = []
    expansion = {}
    for members in by_key.values():
        ordered = sorted(members, key=lambda row: str(row["witness_id"]))
        representative = deepcopy(ordered[0])
        representative_id = str(representative["witness_id"])
        representatives.append(representative)
        expansion[representative_id] = [str(row["witness_id"]) for row in ordered]
    representatives.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    if (
        len(pointwise["units"]) != 1960
        or len(representatives) != 1566
        or sum(len(values) for values in expansion.values()) != 1960
        or len({item for values in expansion.values() for item in values}) != 1960
    ):
        raise JudgeV5SelectionV175Error("exact-claim dedup coverage drifted")
    audit = {
        "schema_version": V175_DEDUP_VERSION,
        "input_witness_count": 1960,
        "representative_count": 1566,
        "exact_duplicate_witness_count": 394,
        "identity_scope": "same_case_id_and_byte_identical_claim_text",
        "semantic_similarity_used": False,
        "embeddings_used": False,
        "regex_or_keyword_semantic_rules_used": False,
        "representative_selection": "lexicographically_smallest_opaque_witness_id",
        "expansion_is_exact_identity_only": True,
    }
    return representatives, expansion, audit


def _compact_packet(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    case_ids = sorted({str(row["case_id"]) for row in units})
    cases = []
    for case_id in case_ids:
        rows = [row for row in units if str(row["case_id"]) == case_id]
        sources = {str(row["source_excerpt"]) for row in rows}
        if len(sources) != 1:
            raise JudgeV5SelectionV175Error("support case source identity drifted")
        cases.append(
            {
                "case_id": case_id,
                "source_excerpt": next(iter(sources)),
                "witnesses": [
                    {
                        "witness_id": row["witness_id"],
                        "proposition": deepcopy(row["proposition"]),
                    }
                    for row in rows
                ],
            }
        )
    return {"cases": cases}


def compact_support_prompt(units: Sequence[Mapping[str, Any]]) -> str:
    return (
        "Return one independent support decision for every opaque witness_id. Preserve case_id and "
        "witness_id exactly. Each case supplies its source_excerpt once; every witness in that case "
        "uses that same source. Every evidence span must be an exact substring of its case's "
        "source_excerpt.\n\n"
        + json.dumps(_compact_packet(units), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def plan_support_shards(representatives: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    by_case: dict[str, list[dict[str, Any]]] = {}
    for unit in representatives:
        by_case.setdefault(str(unit["case_id"]), []).append(deepcopy(unit))
    shards: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for case_id in sorted(by_case):
        case_units = sorted(by_case[case_id], key=lambda row: str(row["witness_id"]))
        if len(case_units) > MAX_UNITS_PER_TURN:
            raise JudgeV5SelectionV175Error("one support case exceeds the unit cap")
        candidate = [*current, *case_units]
        if current and (
            len(candidate) > MAX_UNITS_PER_TURN
            or len(compact_support_prompt(candidate).encode("utf-8")) > MAX_PROMPT_BYTES
        ):
            shards.append(current)
            current = case_units
        else:
            current = candidate
    if current:
        shards.append(current)
    if (
        len(shards) != 15
        or sum(len(shard) for shard in shards) != 1566
        or max(len(shard) for shard in shards) > MAX_UNITS_PER_TURN
        or any(len(compact_support_prompt(shard).encode("utf-8")) > MAX_PROMPT_BYTES for shard in shards)
    ):
        raise JudgeV5SelectionV175Error("support shard plan drifted")
    return shards


def _support_value(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return v155._support_value(units)


def expand_support_output(
    *,
    representative_output: Mapping[str, Any],
    expansion: Mapping[str, Sequence[str]],
    full_value: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    for row in representative_output["units"]:
        representative_id = str(row["witness_id"])
        members = expansion.get(representative_id)
        if not members:
            raise JudgeV5SelectionV175Error("support expansion representative is unknown")
        for witness_id in members:
            rows.append({**deepcopy(row), "witness_id": witness_id})
    rows.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    result = {"units": rows}
    errors = v143.validate_support_output(result, full_value)
    if errors:
        raise JudgeV5SelectionV175Error("expanded support output is invalid")
    return result


def _build_capacity_policy(root: Path, gate: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = tuple(f"selection_support_{index:02d}" for index in range(15))
    bound = len(turn_names) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V175_CAPACITY_AUDIT_VERSION,
        "phase_id": V175_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v174_terminal": gate["records"]["terminal"],
        "v174_protocol": gate["records"]["protocol"],
        "measured_basis": {
            "v168_support_turn_count": 8,
            "v168_support_maximum_total_tokens": 25945,
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V175_CAPACITY_POLICY_VERSION,
        "phase_id": V175_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v175(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v175 terminal")}
    gate = _validate_v174_selection_gate()
    sources = _validate_selection_sources(reuse_contract_path)
    representatives, expansion, dedup_audit = build_exact_claim_dedup(sources["pointwise"])
    shards = plan_support_shards(representatives)
    mapping_path = root / "exact-claim-expansion.private.json"
    audit_path = root / "exact-claim-dedup-audit.json"
    _write_immutable(
        mapping_path,
        {
            "schema_version": V175_DEDUP_VERSION,
            "representative_to_witness_ids": expansion,
        },
    )
    _write_stable_time(audit_path, {**dedup_audit, "created_at": now_iso()}, "created_at")
    turns = []
    for index, units in enumerate(shards):
        turn_name = f"selection_support_{index:02d}"
        value = _support_value(units)
        prompt = compact_support_prompt(units)
        schema = v143.support_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    capacity = _build_capacity_policy(root, gate)
    spec = {
        "schema_version": V175_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V175_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "compact_case_grouped_support_with_exact_claim_identity_dedup",
        "input_witness_count": 1960,
        "representative_count": 1566,
        "turn_plan": [turn["turn_name"] for turn in turns],
        "retry_count_per_turn": 0,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "exact_identity_expansion_only": True,
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "v174_gate": gate["records"],
        "reuse_contract": sources["contract_record"],
        "clean_arm_reports": [item["report"] for item in sources["contract"]["clean_arms"]],
        "interrupted_arm": sources["contract"]["interrupted_batch_5_same_thread"],
        "runtime_files": [
            _record(Path(__file__)),
            *gate["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "support_base_sha256": sha256_text(v143.support_base_instructions_v143()),
            "compact_prompt_prefix_sha256": sha256_text(compact_support_prompt([])),
        },
        "frozen_inputs": {
            "pool": sources["pool_record"],
            "development_manifest": sources["contract"]["selection_inputs"]["development_manifest"],
            "run_spec": sources["contract"]["selection_inputs"]["extraction_run_spec"],
            "membership_index": sources["contract"]["selection_inputs"]["preassembled_membership_index"],
            "private_mapping": sources["contract"]["selection_inputs"]["preassembled_private_mapping"],
            "dedup_audit": _record(audit_path),
            "exact_claim_expansion": _record(mapping_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "selection-support-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "gate": gate,
        "sources": sources,
        "expansion": expansion,
    }


def _write_failure(
    root: Path, gate: Mapping[str, Any], turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = [
        row for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v175 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(gate["cumulative_usage"], usage)
    failure = {
        "schema_version": V175_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
        "predecessor_cumulative_usage": gate["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V175_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v175(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v175 terminal")
    frozen = freeze_v175(
        output_dir=root,
        reuse_contract_path=reuse_contract_path,
        timeout_seconds=timeout_seconds,
    )
    current_turn: Optional[str] = None
    outputs, sidecars = [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v143.support_base_instructions_v143(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"]["unit_count"],
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        representative_output = v155._merge_outputs(outputs, "units")
        full_value = _support_value(frozen["sources"]["pointwise"]["units"])
        expanded = expand_support_output(
            representative_output=representative_output,
            expansion=frozen["expansion"],
            full_value=full_value,
        )
        receipts = v155._support_receipts(expanded)
        output_path = root / "support-output-expanded.private.json"
        receipts_path = root / "support-receipts.private.json"
        _write_immutable(output_path, expanded)
        _write_immutable(receipts_path, receipts)
        status_counts = {status: 0 for status in ("supported", "unsupported", "abstain")}
        for row in expanded["units"]:
            status_counts[row["support_status"]] += 1
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(frozen["gate"]["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V175_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v175_selection_support_completed_alignment_authorized",
            "overall_evaluation_complete": False,
            "support_receipts_frozen": True,
            "alignment_authorized": True,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "input_witness_count": 1960,
            "representative_semantic_decision_count": 1566,
            "exact_identity_expanded_decision_count": 394,
            "support_status_counts": status_counts,
            "support_output": _record(output_path),
            "support_receipts": _record(receipts_path),
            "v174_protocol": frozen["gate"]["records"]["protocol"],
            "predecessor_cumulative_usage": frozen["gate"]["cumulative_usage"],
            "cumulative_evaluation_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["gate"], exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["gate"], current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v175 five-arm support phase")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--reuse-contract", default=str(DEFAULT_REUSE_CONTRACT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v175(
            output_dir=Path(args.output_dir),
            reuse_contract_path=Path(args.reuse_contract),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_receipts_frozen": terminal.get("support_receipts_frozen", False),
                "alignment_authorized": terminal.get("alignment_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
