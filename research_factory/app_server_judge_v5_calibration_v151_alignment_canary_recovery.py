from __future__ import annotations

"""Recover v150 by exact-span projection and run only its missing canaries."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic as v150
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
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
from .util import now_iso, sha256_text


V151_AUDIT_VERSION = "pif_app_server_judge_v5_4_v151_exact_span_projection_audit_v1"
V151_SPEC_VERSION = "pif_app_server_judge_v5_4_v151_spec_v1"
V151_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v151_alignment_protocol_v1"
V151_FAILURE_VERSION = "pif_app_server_judge_v5_4_v151_failure_v1"
V151_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v151_terminal_v1"
V151_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V151_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V151_PHASE_ID = "judge_v5_4_v151_alignment_canary_recovery"

MODEL = v150.MODEL
EFFORT = v150.EFFORT
TURN_NAMES = v150.CANARY_TURNS
TIMEOUT_SECONDS = v150.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v150.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v151-alignment-canary-recovery"
).resolve()


class JudgeV5CalibrationV151Error(RuntimeError):
    """The immutable v150 canary-recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v150_failure() -> dict[str, Any]:
    root = v150.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "fresh-alignment-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "truth": root / "fresh-alignment-truth.private.json",
        "selection": root / "selection-audit.json",
        "receipts": root / "neutralized-support-receipts.private.json",
    }
    values = {name: _load_json(path, f"v150 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 50978,
        "cached_input_tokens": 0,
        "output_tokens": 23146,
        "reasoning_output_tokens": 7620,
        "total_tokens": 74124,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "fresh_alignment_primary_01"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or failure.get("retry_allowed_in_this_version") is not False
        or spec.get("turn_plan") != list(v150.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV151Error("v150 failed-attempt contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5CalibrationV151Error("v150 failure record drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV151Error("v150 runtime record drifted")

    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    attempts = {}
    outputs = {}
    inputs = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in v150.PRIMARY_TURNS:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV151Error("v150 primary coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v150 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        outputs[turn_name] = _load_json(Path(records["output"]["path"]), "v150 output")
        input_record = frozen_turns[turn_name]["input"]
        if not _verify_record(input_record):
            raise JudgeV5CalibrationV151Error("v150 primary input drifted")
        inputs[turn_name] = _load_json(Path(input_record["path"]), "v150 input")
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV151Error("v150 usage aggregate drifted")
    if validate_neutral_alignment_output(outputs[v150.PRIMARY_TURNS[0]], inputs[v150.PRIMARY_TURNS[0]]) != []:
        raise JudgeV5CalibrationV151Error("v150 first primary is no longer valid")
    if validate_neutral_alignment_output(outputs[v150.PRIMARY_TURNS[1]], inputs[v150.PRIMARY_TURNS[1]]) != [
        "case_5_pair_0_checklist_10_invalid"
    ]:
        raise JudgeV5CalibrationV151Error("v150 failed-primary signature drifted")
    for turn_name in v150.CANARY_TURNS:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        if any((turn_root / name).exists() for name in ("capacity.json", "sidecar.json", "output.private.json")):
            raise JudgeV5CalibrationV151Error("v150 canary unexpectedly started")
    source = v150._validate_v149()
    v106 = v150._validate_v106_sources()
    rows, truth, selection, receipts = v150.build_v150_inputs(source, v106)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "outputs": outputs,
        "inputs": inputs,
        "frozen_turns": frozen_turns,
        "truth": truth,
        "selection": selection,
        "receipts": receipts,
        "source": source,
    }


def project_exact_source_spans(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = deepcopy(output)
    sources = {str(row["case_id"]): str(row["source_excerpt"]) for row in alignment_input["cases"]}
    dropped = []
    decision_count = 0
    for case in result["cases"]:
        source = sources[str(case["case_id"])]
        for pair_index, pair in enumerate(case["alignment_pairs"]):
            for checklist_index, row in enumerate(pair["checklist"]):
                decision_count += 1
                before = list(row["source_evidence_spans"])
                row["source_evidence_spans"] = [span for span in before if span in source]
                if len(row["source_evidence_spans"]) != len(before):
                    dropped.append(
                        {
                            "case_id": case["case_id"],
                            "pair_index": pair_index,
                            "checklist_index": checklist_index,
                            "field": row["field"],
                            "dropped_span_count": len(before) - len(row["source_evidence_spans"]),
                        }
                    )
    audit = {
        "schema_version": V151_AUDIT_VERSION,
        "decision_count": decision_count,
        "dropped_span_count": sum(row["dropped_span_count"] for row in dropped),
        "affected_checklist_count": len(dropped),
        "affected_rows": dropped,
        "semantic_decisions_changed": False,
        "witness_assignments_changed": False,
        "relations_changed": False,
        "deterministic_operation": "retain_only_exact_source_substrings",
    }
    errors = validate_neutral_alignment_output(result, alignment_input)
    if errors:
        raise JudgeV5CalibrationV151Error("v151 exact-span projection did not validate")
    return result, audit


def build_v151_inputs(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for turn_name in TURN_NAMES:
        frozen = source["frozen_turns"][turn_name]
        for key in ("input", "prompt", "schema"):
            if not _verify_record(frozen[key]):
                raise JudgeV5CalibrationV151Error("v150 canary request drifted")
        value = _load_json(Path(frozen["input"]["path"]), "v150 canary input")
        prompt = Path(frozen["prompt"]["path"]).read_text()
        schema = _load_json(Path(frozen["schema"]["path"]), "v150 canary schema")
        rows.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_canary",
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "case_ids": frozen["case_ids"],
            }
        )
    if len(rows) != 2 or any(len(row["case_ids"]) != 6 for row in rows):
        raise JudgeV5CalibrationV151Error("v151 canary coverage drifted")
    return rows


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {"schema_version": V151_CAPACITY_AUDIT_VERSION, "phase_id": V151_PHASE_ID, "created_at": now_iso(), "production_mutation_performed": False, "predecessor": predecessor, "measured_basis": {"declared_turn_count": len(TURN_NAMES), "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound}}
    _write_stable_time(audit_path, audit, "created_at")
    policy = {"schema_version": V151_CAPACITY_POLICY_VERSION, "phase_id": V151_PHASE_ID, "created_at": now_iso(), "managed_chatgpt_auth_only": True, "official_persistent_codex_app_server_only": True, "retry_count_per_turn": 0, "production_mutation_allowed": False, "rate_limit_reached_type_must_be_null": True, "unknown_usage_hard_stop": True, "ordered_turn_names": list(TURN_NAMES), "minimum_remaining_reserve_percent": 20, "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS, "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound, "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000), "semantic_output_root": str(root), "audit": _record(audit_path)}
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v151(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v151 terminal")}
    source = _validate_v150_failure()
    projected, audit = project_exact_source_spans(
        source["outputs"][v150.PRIMARY_TURNS[1]], source["inputs"][v150.PRIMARY_TURNS[1]]
    )
    projected_path = root / "projected-primary-01.private.json"
    audit_path = root / "exact-span-projection-audit.json"
    _write_immutable(projected_path, projected)
    _write_immutable(audit_path, audit)
    rows = build_v151_inputs(source)
    turns = []
    for row in rows:
        paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=row["prompt"], schema=row["schema"])
        turns.append({**row, "paths": paths})
    predecessor = {
        **{f"v150_{name}": record for name, record in source["records"].items()},
        "v150_attempts": source["attempts"],
        "v150_primary_inputs": [source["frozen_turns"][name]["input"] for name in v150.PRIMARY_TURNS],
        "v150_canary_requests": [
            {key: source["frozen_turns"][name][key] for key in ("input", "prompt", "schema")}
            for name in v150.CANARY_TURNS
        ],
        "v149_protocol": source["source"]["records"]["protocol"],
        "v148_reference": source["source"]["v148"]["records"]["reference"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V151_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "reuse_v150_primaries_after_exact_span_projection_run_only_missing_canaries",
        "reused_primary_turn_count": 2,
        "replayed_primary_turn_count": 0,
        "canary_turn_count": 2,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "deterministic_semantic_decision_changes": 0,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": source["records"]["truth"],
            "projected_primary": _record(projected_path),
            "projection_audit": _record(audit_path),
            "reference": source["source"]["v148"]["records"]["reference"],
            "field_protocol": source["source"]["records"]["protocol"],
            "turns": [
                {"turn_name": turn["turn_name"], "role": turn["turn_role"], "case_ids": turn["case_ids"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])}
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "alignment-canary-recovery-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {"root": root, "spec": spec, "spec_path": spec_path, "capacity_policy": capacity["policy"], "turns": turns, "source": source, "projected_primary": projected, "projection_audit": audit}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping): unknown += 1; continue
        try: measured = _validate_usage(_load_json(Path(record["path"]), "v151 sidecar"))
        except Exception: unknown += 1; continue
        for field in USAGE_FIELDS: usage[field] += measured[field]
    complete = unknown == 0
    failure = {"schema_version": V151_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None, "known_usage_lower_bound": usage, "unknown_usage_turn_count": unknown, "attempts": attempts}
    failure_path = root / "failure.json"; _write_immutable(failure_path, failure)
    terminal = {"schema_version": V151_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "alignment_protocol_frozen": False, "fresh_full_development_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}
    _write_immutable(root / "terminal.json", terminal); return terminal


async def run_v151(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS, client_factory: Optional[Callable[[Path], Any]] = None) -> dict[str, Any]:
    root = output_dir.expanduser().resolve(); terminal_path = root / "terminal.json"
    if terminal_path.exists(): return _load_json(terminal_path, "v151 terminal")
    frozen = freeze_v151(output_dir=root, timeout_seconds=timeout_seconds); current_turn: Optional[str] = None
    try:
        outputs = {}; sidecars = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(client=client, turn_name=current_turn, paths=turn["paths"], prompt=turn["prompt"], schema=turn["schema"], base_instructions=v130.alignment_instructions_v130(), model=MODEL, effort=EFFORT, timeout_seconds=timeout_seconds, batch_size=6, policy_path=frozen["capacity_policy"], output_validator=lambda candidate, item=turn["value"]: validate_neutral_alignment_output(candidate, item))
                outputs[current_turn] = output; sidecars.append(sidecar)
        primary_outputs = {
            v150.PRIMARY_TURNS[0]: frozen["source"]["outputs"][v150.PRIMARY_TURNS[0]],
            v150.PRIMARY_TURNS[1]: frozen["projected_primary"],
        }
        primary = v150._merge_normalized(primary_outputs, frozen["source"]["inputs"], v150.PRIMARY_TURNS)
        canary_inputs = {turn["turn_name"]: turn["value"] for turn in frozen["turns"]}
        canary = v150._merge_normalized(outputs, canary_inputs, TURN_NAMES)
        score = v150.score_v150(primary=primary, canary=canary, truth=frozen["source"]["truth"])
        paths = {"raw": root / "alignment-canary-raw.private.json", "primary": root / "reused-primary-normalized.private.json", "canary": root / "alignment-canary-normalized.private.json", "score": root / "fresh-alignment-score.json", "protocol": root / "alignment-judge-protocol-v151.json"}
        _write_immutable(paths["raw"], {"turns": outputs}); _write_immutable(paths["primary"], primary); _write_immutable(paths["canary"], canary); _write_immutable(paths["score"], score)
        passed = bool(score["passed"])
        if passed:
            protocol = {"schema_version": V151_PROTOCOL_VERSION, "frozen_at": now_iso(), "model": MODEL, "reasoning_effort": EFFORT, "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()), "reference": frozen["source"]["source"]["v148"]["records"]["reference"], "field_protocol": frozen["source"]["source"]["records"]["protocol"], "v150_primaries_reused": True, "v151_exact_span_projection": _record(root / "exact-span-projection-audit.json"), "quality_gates_unchanged": True, "selection_authorized": False, "holdout_authorized": False, "production_mutation_allowed": False}
            _write_immutable(paths["protocol"], protocol)
        accounting = _aggregate_usage(sidecars)
        terminal = {"schema_version": V151_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v151_alignment_protocol_frozen_full_development_calibration_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v151_alignment_canary_recovery_passed" if passed else "v151_alignment_quality_gate_not_passed", "overall_evaluation_complete": False, "alignment_protocol_frozen": passed, "fresh_full_development_calibration_authorized": passed, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "v150_primary_turns_replayed": False, "projection_audit": _record(root / "exact-span-projection-audit.json"), "score": _record(paths["score"]), "raw_outputs": _record(paths["raw"]), "primary_output": _record(paths["primary"]), "canary_output": _record(paths["canary"]), "protocol": _record(paths["protocol"]) if passed else None, "reference": frozen["source"]["source"]["v148"]["records"]["reference"], "field_protocol": frozen["source"]["source"]["records"]["protocol"], "failed_quality_gates": score["failed_checks"], "metrics": score["metrics"], "predecessor_v150_usage": frozen["source"]["usage"], **accounting}
        _write_immutable(terminal_path, terminal); return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc: return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc: return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v151 alignment canary recovery"); parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT)); parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS); args = parser.parse_args(argv)
    terminal = asyncio.run(run_v151(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "alignment_protocol_frozen": terminal.get("alignment_protocol_frozen", False), "fresh_full_development_calibration_authorized": terminal.get("fresh_full_development_calibration_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True)); return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__": raise SystemExit(main())
