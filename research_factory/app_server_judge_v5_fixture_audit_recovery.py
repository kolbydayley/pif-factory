from __future__ import annotations

"""Recover complete fixture-audit outputs after deterministic normalization failure."""

import argparse
import asyncio
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_judge_v5 import (
    ADJUDICATION_INPUT_VERSION,
    ALIGNMENT_OUTPUT_VERSION,
    CHECKLIST_FIELDS,
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    project_mismatch_fields,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import score_v5_calibration
from .app_server_judge_v5_calibration_runner import (
    validate_scoreable_calibration_alignment_output,
)
from .app_server_judge_v5_diagnostic import (
    JudgeV5DiagnosticAttemptFailed,
    _aggregate_usage,
    _attempt_records,
    _freeze_turn_request,
    _get_or_run_turn,
    _record,
    _sha256_file,
    _write_immutable_json,
)
from .util import now_iso


RECOVERY_SPEC_VERSION = "pif_app_server_fixture_truth_audit_recovery_v3_spec_v1"
RECOVERY_TERMINAL_VERSION = "pif_app_server_fixture_truth_audit_recovery_v3_terminal_v1"
RECOVERY_FAILURE_VERSION = "pif_app_server_fixture_truth_audit_recovery_v3_failure_v1"
ADOPTION_RECEIPT_VERSION = "pif_app_server_fixture_truth_audit_v2_adoption_receipt_v1"
DEFAULT_V2_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2"
).resolve()
DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3"
).resolve()


class FixtureAuditRecoveryError(RuntimeError):
    """The complete-output recovery contract is missing or unsafe."""


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FixtureAuditRecoveryError("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise FixtureAuditRecoveryError("%s is not an object" % purpose)
    return value


def _verify_record(record: Mapping[str, Any]) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise FixtureAuditRecoveryError("fixture-audit v2 record drifted")
    return path


def normalize_scoreable_alignment_output(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> dict[str, Any]:
    errors = validate_scoreable_calibration_alignment_output(output, alignment_input)
    if errors:
        raise FixtureAuditRecoveryError(
            "alignment output has non-scoreable errors: %s" % "; ".join(errors)
        )
    normalized_cases = []
    for row in output["cases"]:
        pairs = []
        for pair in row["alignment_pairs"]:
            ids = sorted((pair["witness_id_1"], pair["witness_id_2"]))
            checklist_by_field = {item["field"]: item for item in pair["checklist"]}
            pairs.append(
                {
                    "witness_ids": ids,
                    "relation": pair["relation"],
                    "mismatch_fields": project_mismatch_fields(pair["checklist"]),
                    "checklist_decisions": {
                        field: checklist_by_field[field]["decision"]
                        for field in CHECKLIST_FIELDS
                    },
                }
            )
        normalized_cases.append(
            {
                "case_id": row["case_id"],
                "equivalence_groups": sorted(
                    (sorted(group["witness_ids"]) for group in row["equivalence_groups"]),
                    key=lambda values: tuple(values),
                ),
                "alignment_pairs": sorted(
                    pairs, key=lambda item: tuple(item["witness_ids"])
                ),
                "unpaired_witness_ids": sorted(row["unpaired_witness_ids"]),
            }
        )
    return {
        "schema_version": ALIGNMENT_OUTPUT_VERSION,
        "cases": sorted(normalized_cases, key=lambda item: item["case_id"]),
        "mismatch_fields_projected_from_checklists": True,
        "origin_neutral": True,
        "scoreable_root_omissions_preserved_as_model_errors": True,
    }


def find_scoreable_alignment_disagreements(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    base = normalize_scoreable_alignment_output(base_output, base_input)
    canary = normalize_scoreable_alignment_output(canary_output, canary_input)
    base_by_case = {item["case_id"]: item for item in base["cases"]}
    canary_by_case = {item["case_id"]: item for item in canary["cases"]}
    support = {
        str(item["witness_id"]): item for item in support_receipts.get("units") or []
    }
    if not set(canary_by_case) <= set(base_by_case):
        raise FixtureAuditRecoveryError("canary cases are outside the base output")
    disagreements = []
    for case_id, canary_case in sorted(canary_by_case.items()):
        base_case = base_by_case[case_id]
        reasons = []
        if base_case != canary_case:
            reasons.append("permutation_output_changed")
        for pair in base_case["alignment_pairs"]:
            first, second = pair["witness_ids"]
            first_receipt = support[first]
            second_receipt = support[second]
            first_support = first_receipt["proposition_verdict"]
            second_support = second_receipt["proposition_verdict"]
            expected_unsupported = (
                "abstain"
                if "abstain" in {first_support, second_support}
                else "same"
                if first_support == second_support
                else "different"
            )
            if pair["checklist_decisions"]["unsupported_inference"] != expected_unsupported:
                reasons.append("support_alignment_unsupported_inference_conflict")
            if (
                first_receipt["structured_field_verdict"]
                != second_receipt["structured_field_verdict"]
                and pair["relation"] == "equivalent"
            ):
                reasons.append("support_alignment_structured_field_conflict")
        if reasons:
            disagreements.append(
                {"case_id": case_id, "reasons": sorted(set(reasons))}
            )
    return {
        "schema_version": "pif_app_server_fixture_truth_audit_v3_disagreements_v1",
        "disagreement_case_count": len(disagreements),
        "disagreements": disagreements,
        "adjudication_required": bool(disagreements),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
    }


def build_scoreable_adjudication_packet(
    *,
    base_input: Mapping[str, Any],
    base_output: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    disagreements = find_scoreable_alignment_disagreements(
        base_output=base_output,
        base_input=base_input,
        canary_output=canary_output,
        canary_input=canary_input,
        support_receipts=support_receipts,
    )
    case_ids = {item["case_id"] for item in disagreements["disagreements"]}
    normalized_base = {
        item["case_id"]: item
        for item in normalize_scoreable_alignment_output(base_output, base_input)["cases"]
    }
    normalized_canary = {
        item["case_id"]: item
        for item in normalize_scoreable_alignment_output(canary_output, canary_input)[
            "cases"
        ]
    }
    base_cases = {item["case_id"]: item for item in base_input["cases"]}
    return {
        "schema_version": ADJUDICATION_INPUT_VERSION,
        "cases": [
            {
                **deepcopy(base_cases[case_id]),
                "observed_disagreement_reasons": next(
                    item["reasons"]
                    for item in disagreements["disagreements"]
                    if item["case_id"] == case_id
                ),
                "anonymous_candidate_1": normalized_base[case_id],
                "anonymous_candidate_2": normalized_canary[case_id],
            }
            for case_id in sorted(case_ids)
        ],
        "adjudication_required": bool(case_ids),
        "call_cap": 1,
        "candidate_order_has_no_vote_meaning": True,
    }


def reconcile_scoreable_alignment(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    disagreements: Mapping[str, Any],
    adjudication_output: Optional[Mapping[str, Any]],
    adjudication_input: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    base = normalize_scoreable_alignment_output(base_output, base_input)
    disagreement_ids = {
        item["case_id"] for item in disagreements.get("disagreements") or []
    }
    adjudicated = {}
    if disagreement_ids:
        if adjudication_output is None or adjudication_input is None:
            raise FixtureAuditRecoveryError("required adjudication output is missing")
        strict_errors = validate_neutral_alignment_output(
            adjudication_output, adjudication_input
        )
        if strict_errors:
            raise FixtureAuditRecoveryError(
                "adjudication output is invalid: %s" % "; ".join(strict_errors)
            )
        normalized = normalize_scoreable_alignment_output(
            adjudication_output, adjudication_input
        )
        adjudicated = {item["case_id"]: item for item in normalized["cases"]}
        if set(adjudicated) != disagreement_ids:
            raise FixtureAuditRecoveryError("adjudication coverage drifted")
    cases = []
    for item in base["cases"]:
        case_id = item["case_id"]
        chosen = adjudicated.get(case_id, item)
        status = "adjudicated" if case_id in adjudicated else "accepted_base"
        cases.append({"case_id": case_id, "status": status, **chosen})
    return {
        "schema_version": "pif_app_server_fixture_truth_audit_v3_reconciled_v1",
        "cases": cases,
        "observable_disagreement_case_count": len(disagreement_ids),
        "adjudication_call_count": int(bool(adjudicated)),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
        "unresolved_cases_abstained": [],
        "scoreable_root_omissions_preserved_as_model_errors": True,
    }


def build_v2_adoption_receipt(*, v2_root: Path, output_path: Path) -> dict[str, Any]:
    source = v2_root.expanduser().resolve()
    terminal_path = source / "terminal.json"
    terminal = _load_json(terminal_path, "fixture-audit v2 terminal")
    failure = _load_json(source / "failure.json", "fixture-audit v2 failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or (terminal.get("usage") or {}).get("total_tokens") != 827876
        or terminal.get("fixture_truth_proposal_completed") is not False
        or terminal.get("selection_authorized") is not False
        or failure.get("error_class") != "JudgeV5ProtocolError"
        or failure.get("failed_turn_name") != "neutral_alignment_canary"
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("retry_allowed_in_this_version") is not False
    ):
        raise FixtureAuditRecoveryError("fixture-audit v2 terminal is not adoptable")
    attempts = failure.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 23:
        raise FixtureAuditRecoveryError("fixture-audit v2 attempt coverage drifted")
    for attempt in attempts:
        if (
            attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or attempt.get("error_class") is not None
        ):
            raise FixtureAuditRecoveryError("fixture-audit v2 has an incomplete attempt")
        for key in ("capacity", "output", "sidecar"):
            _verify_record(attempt[key])
    pool = _load_json(source / "shared-witness-pool.private.json", "v2 pool")
    receipts = _load_json(source / "support-receipts.private.json", "v2 receipts")
    truth = _load_json(source / "provisional-calibration-truth.private.json", "v2 truth")
    pointwise = _load_json(source / "pointwise-output-full.private.json", "v2 pointwise")
    base = _load_json(source / "base-alignment-full.private.json", "v2 base")
    canary = _load_json(
        source / "turns/neutral-alignment-canary/output.private.json", "v2 canary"
    )
    pointwise_input = _load_json(source / "pointwise-input-full.private.json", "v2 input")
    if validate_pointwise_support_output(pointwise, pointwise_input):
        raise FixtureAuditRecoveryError("fixture-audit v2 pointwise output is invalid")
    base_input = build_neutral_alignment_input(pool, receipts)
    canary_input = build_neutral_alignment_input(
        pool,
        receipts,
        case_ids=truth["canary_case_ids"],
        permutation="balanced_canary",
    )
    strict_base_errors = validate_neutral_alignment_output(base, base_input)
    if strict_base_errors != [
        "case_2_pair_0_unsupported_without_specific_root",
        "case_38_pair_0_unsupported_without_specific_root",
        "case_56_pair_0_unsupported_without_specific_root",
    ] or validate_scoreable_calibration_alignment_output(base, base_input):
        raise FixtureAuditRecoveryError("fixture-audit v2 base failure changed")
    if validate_neutral_alignment_output(canary, canary_input):
        raise FixtureAuditRecoveryError("fixture-audit v2 canary is invalid")
    disagreements = find_scoreable_alignment_disagreements(
        base_output=base,
        base_input=base_input,
        canary_output=canary,
        canary_input=canary_input,
        support_receipts=receipts,
    )
    if disagreements["disagreement_case_count"] != 1:
        raise FixtureAuditRecoveryError("fixture-audit v2 disagreement count drifted")
    payload = {
        "schema_version": ADOPTION_RECEIPT_VERSION,
        "status": "all_complete_outputs_adoptable_for_deterministic_recovery",
        "created_at": now_iso(),
        "source_terminal": _record(terminal_path),
        "source_failure": _record(source / "failure.json"),
        "source_spec": _record(source / "fixture-audit-spec.json"),
        "source_attempt_count": 23,
        "source_usage": terminal["usage"],
        "source_usage_status": "complete",
        "source_output_selection_performed": False,
        "all_completed_outputs_adopted": True,
        "source_semantic_turn_replay_allowed": False,
        "deterministic_normalization_only": True,
        "semantic_fields_modified": False,
        "scoreable_root_omission_count": 3,
        "observable_disagreement_case_count": 1,
        "new_semantic_work_allowed": "one_capped_side_free_permutation_adjudication",
        "proposal_can_authorize_selection": False,
        "production_mutation_performed": False,
    }
    _write_immutable_json(output_path.expanduser().resolve(), payload)
    return payload


async def run_fixture_audit_recovery(
    *,
    v2_root: Path = DEFAULT_V2_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    source = v2_root.expanduser().resolve()
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "fixture-audit recovery terminal")
    root.mkdir(parents=True, exist_ok=True)
    adoption_path = root / "v2-adoption-receipt.json"
    adoption = build_v2_adoption_receipt(v2_root=source, output_path=adoption_path)
    load = lambda name: _load_json(source / name, "fixture-audit v2 " + name)
    pool = load("shared-witness-pool.private.json")
    truth = load("provisional-calibration-truth.private.json")
    receipts = load("support-receipts.private.json")
    pointwise = load("pointwise-output-full.private.json")
    base = load("base-alignment-full.private.json")
    canary = _load_json(
        source / "turns/neutral-alignment-canary/output.private.json", "v2 canary"
    )
    base_input = build_neutral_alignment_input(pool, receipts)
    canary_input = build_neutral_alignment_input(
        pool,
        receipts,
        case_ids=truth["canary_case_ids"],
        permutation="balanced_canary",
    )
    disagreements = find_scoreable_alignment_disagreements(
        base_output=base,
        base_input=base_input,
        canary_output=canary,
        canary_input=canary_input,
        support_receipts=receipts,
    )
    _write_immutable_json(root / "observable-disagreements.private.json", disagreements)
    packet = build_scoreable_adjudication_packet(
        base_input=base_input,
        base_output=base,
        canary_output=canary,
        canary_input=canary_input,
        support_receipts=receipts,
    )
    adjudication_input = adjudication_alignment_input(
        base_input=base_input, adjudication_input=packet
    )
    prompt = build_disagreement_adjudication_prompt(
        adjudication_input=packet, adjudication_alignment=adjudication_input
    )
    schema = neutral_alignment_output_schema(adjudication_input)
    turn_name = "permutation_disagreement_adjudication"
    paths = _freeze_turn_request(
        root=root,
        turn_name=turn_name,
        input_value={"adjudication_packet": packet, "alignment_input": adjudication_input},
        prompt=prompt,
        schema=schema,
    )
    spec = {
        "schema_version": RECOVERY_SPEC_VERSION,
        "state": "frozen_before_single_recovery_adjudication",
        "created_at": now_iso(),
        "source_v2_adoption_receipt": _record(adoption_path),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "adopted_semantic_turn_count": 23,
        "new_semantic_turn_count": 1,
        "new_semantic_turn_retry_count": 0,
        "source_semantic_turn_replay_allowed": False,
        "all_source_outputs_adopted": True,
        "disagreement_case_count": 1,
        "side_free": True,
        "candidate_order_has_no_vote_meaning": True,
        "proposal_can_authorize_reference_freeze": False,
        "proposal_can_authorize_selection": False,
        "production_mutation_allowed": False,
        "turn_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
    }
    spec_path = root / "recovery-spec.json"
    _write_immutable_json(spec_path, spec)
    current_turn = turn_name
    try:
        async with client_factory() as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=turn_name,
                paths=paths,
                prompt=prompt,
                schema=schema,
                base_instructions=neutral_alignment_base_instructions(),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=1,
                output_validator=lambda value: validate_neutral_alignment_output(
                    value, adjudication_input
                ),
            )
        reconciled = reconcile_scoreable_alignment(
            base_output=base,
            base_input=base_input,
            disagreements=disagreements,
            adjudication_output=output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable_json(reconciled_path, reconciled)
        comparison = score_v5_calibration(
            pointwise_output=pointwise,
            reconciled_alignment=reconciled,
            expected=truth,
            observable_disagreements=disagreements,
        )
        comparison_path = root / "comparison-vs-provisional.json"
        _write_immutable_json(comparison_path, comparison)
        new_accounting = _aggregate_usage([sidecar])
        cumulative_usage = {
            field: adoption["source_usage"][field] + new_accounting["usage"][field]
            for field in adoption["source_usage"]
        }
        terminal = {
            "schema_version": RECOVERY_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "fixture_truth_proposal_completed_reference_adjudication_required"
            ),
            "spec_sha256": _sha256_file(spec_path),
            "fixture_truth_proposal_completed": True,
            "provisional_reference_comparison_passed": comparison["passed"],
            "calibration_passed": False,
            "reference_freeze_authorized": False,
            "selection_authorized": False,
            "source_v2_usage": adoption["source_usage"],
            "new_recovery_usage": new_accounting["usage"],
            "cumulative_proposal_usage": cumulative_usage,
            "accounting_complete": True,
            "usage_status": "complete",
            "semantic_retry_count": 0,
            "comparison": _record(comparison_path),
            "reconciled_alignment": _record(reconciled_path),
            "observable_disagreements": _record(
                root / "observable-disagreements.private.json"
            ),
            "attempts": _attempt_records(root),
            "production_mutated": False,
        }
        _write_immutable_json(terminal_path, terminal)
        return terminal
    except JudgeV5DiagnosticAttemptFailed as exc:
        error_class = exc.error_class
        current_turn = exc.turn_name
    except Exception as exc:
        error_class = type(exc).__name__
    attempts = _attempt_records(root)
    failure = {
        "schema_version": RECOVERY_FAILURE_VERSION,
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": current_turn,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "source_v2_usage": adoption["source_usage"],
        "attempts": attempts,
        "selection_authorized": False,
    }
    failure_path = root / "failure.json"
    _write_immutable_json(failure_path, failure)
    terminal = {
        "schema_version": RECOVERY_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "fixture_truth_proposal_completed": False,
        "calibration_passed": False,
        "reference_freeze_authorized": False,
        "selection_authorized": False,
        "failure": _record(failure_path),
        "production_mutated": False,
    }
    _write_immutable_json(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recover fixture-audit v2 postprocessing")
    parser.add_argument("--v2-root", default=str(DEFAULT_V2_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_fixture_audit_recovery(
            v2_root=Path(args.v2_root),
            output_dir=Path(args.output_dir),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fixture_truth_proposal_completed": terminal.get(
                    "fixture_truth_proposal_completed", False
                ),
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
