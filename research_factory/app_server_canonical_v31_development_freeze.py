from __future__ import annotations

"""Deterministically freeze one epoch-7 development winner.

This module performs no semantic work. It accepts only a fully verified live
four-turn quality terminal, applies the ranking policy frozen in the epoch-7
controller before extraction, and binds the exact winning configuration while
leaving holdout and production closed.
"""

import argparse
import copy
import json
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_epoch7_controller as controller
from . import app_server_canonical_v31_quality_runtime as quality_runtime


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FREEZE_VERSION = "pif_canonical_v31_development_winner_freeze_v1"
SELECTION_ANALYSIS_VERSION = "pif_canonical_v31_development_selection_analysis_v1"
WINNER_CONFIGURATION_VERSION = "pif_canonical_v31_winner_configuration_v1"
STATUS_VERSION = "pif_canonical_v31_development_freeze_status_v1"

SELECTION_ANALYSIS_FILENAME = "selection-analysis.json"
WINNER_CONFIGURATION_FILENAME = "winner-configuration.json"
FREEZE_RECEIPT_FILENAME = "development-winner-freeze-receipt.json"

_AUTHORIZATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")


class CanonicalV31DevelopmentFreezeError(controller.Epoch7ControllerError):
    """Development selection or immutable freeze failed closed."""


def _authorization_id(value: Any) -> str:
    if not isinstance(value, str) or _AUTHORIZATION_ID.fullmatch(value) is None:
        raise CanonicalV31DevelopmentFreezeError(
            "development freeze authorization ID is malformed"
        )
    return value


def _safe(path: Path, *, project_root: Path, label: str) -> Path:
    try:
        return controller._safe_path(path, project_root=project_root, label=label)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31DevelopmentFreezeError(str(exc)) from exc


def _load(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return controller._load_object(path, label=label)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31DevelopmentFreezeError(str(exc)) from exc


def _record(path: Path, *, allowed_root: Path | None = None) -> dict[str, Any]:
    try:
        return controller._record(path, allowed_root=allowed_root)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31DevelopmentFreezeError(str(exc)) from exc


def _decimal(value: Any, *, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanonicalV31DevelopmentFreezeError(f"{label} is malformed")
    normalized = Decimal(str(value))
    if not normalized.is_finite() or normalized < 0:
        raise CanonicalV31DevelopmentFreezeError(f"{label} is malformed")
    return normalized


def _quality_inputs(
    *,
    controller_root: Path,
    quality_root: Path,
    project_root: Path,
) -> dict[str, Any]:
    try:
        loaded = controller.load_epoch7_controller(
            controller_root, project_root=project_root
        )
        handoff = controller.verify_quality_handoff(
            controller_root, project_root=project_root
        )
        terminal = controller.verify_quality_terminal_receipt(
            controller_root, quality_root, project_root=project_root
        )
        runtime_terminal = quality_runtime.verify_quality_runtime(
            quality_root, project_root=project_root
        )
        extraction = controller._verify_extraction_receipt(loaded)
    except (
        controller.Epoch7ControllerError,
        quality_runtime.CanonicalV31QualityRuntimeError,
    ) as exc:
        raise CanonicalV31DevelopmentFreezeError(str(exc)) from exc
    receipt = terminal["receipt"]
    if (
        runtime_terminal.get("receipt") != receipt
        or receipt.get("state")
        != "passed_development_quality_checkpoint_selection_not_frozen"
        or receipt.get("quality_gate_passed") is not True
        or receipt.get("semantic_model_call_count") != 4
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("quality_selection_authorized") is not False
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31DevelopmentFreezeError(
            "development freeze requires one verified live passing quality terminal"
        )
    return {
        "loaded": loaded,
        "handoff": handoff,
        "terminal": terminal,
        "extraction": extraction,
        "quality_root": quality_root,
    }


def _selection_analysis(inputs: Mapping[str, Any]) -> dict[str, Any]:
    loaded = inputs["loaded"]
    terminal = inputs["terminal"]["receipt"]
    extraction = inputs["extraction"]["receipt"]
    policy = controller.development_selection_policy()
    if (
        loaded["contract"].get("development_selection_policy_sha256")
        != policy["policy_sha256"]
        or loaded["contract"].get("development_selection_policy")
        != _record(
            loaded["records"]["development_selection_policy"],
            allowed_root=loaded["controller_root"],
        )
        or terminal.get("development_selection_policy")
        != loaded["contract"]["development_selection_policy"]
        or terminal.get("development_selection_policy_sha256")
        != policy["policy_sha256"]
    ):
        raise CanonicalV31DevelopmentFreezeError(
            "development selection policy lineage drifted"
        )

    expected_ids = [str(row["variant_id"]) for row in extraction["arms"]]
    rows = terminal.get("arm_results")
    passing_ids = terminal.get("passing_arm_ids")
    if (
        len(expected_ids) != 6
        or len(expected_ids) != len(set(expected_ids))
        or not isinstance(rows, list)
        or [str(row.get("variant_id")) for row in rows] != expected_ids
        or not isinstance(passing_ids, list)
        or not passing_ids
        or len(passing_ids) != len(set(passing_ids))
        or any(variant_id not in expected_ids for variant_id in passing_ids)
    ):
        raise CanonicalV31DevelopmentFreezeError(
            "development quality arm coverage drifted"
        )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        variant_id = str(row["variant_id"])
        checks = row.get("checks")
        passed = row.get("passed")
        if (
            not isinstance(checks, Mapping)
            or not checks
            or any(not isinstance(value, bool) for value in checks.values())
            or not isinstance(passed, bool)
            or passed is not all(checks.values())
            or (variant_id in passing_ids) is not bool(passed)
        ):
            raise CanonicalV31DevelopmentFreezeError(
                f"development quality checks drifted for {variant_id}"
            )
        macro = _decimal(
            row.get("strict_full_field_macro"),
            label=f"{variant_id} strict macro",
        )
        ratio = _decimal(
            row.get("production_amortized_total_token_ratio"),
            label=f"{variant_id} production ratio",
        )
        normalized.append(
            {
                "variant_id": variant_id,
                "passed": bool(passed),
                "checks": copy.deepcopy(dict(checks)),
                "strict_full_field_macro": float(macro),
                "production_amortized_total_token_ratio": float(ratio),
            }
        )

    eligible = [row for row in normalized if row["passed"]]
    ranked = sorted(
        eligible,
        key=lambda row: (
            -Decimal(str(row["strict_full_field_macro"])),
            Decimal(str(row["production_amortized_total_token_ratio"])),
            row["variant_id"],
        ),
    )
    best = ranked[0]
    tied = [
        row
        for row in ranked
        if (
            row["strict_full_field_macro"],
            row["production_amortized_total_token_ratio"],
        )
        == (
            best["strict_full_field_macro"],
            best["production_amortized_total_token_ratio"],
        )
    ]
    if len(tied) != 1:
        raise CanonicalV31DevelopmentFreezeError(
            "development selection has no unique winner"
        )
    return {
        "schema_version": SELECTION_ANALYSIS_VERSION,
        "state": "unique_development_winner_selected_from_frozen_policy",
        "selection_policy": loaded["contract"]["development_selection_policy"],
        "selection_policy_sha256": policy["policy_sha256"],
        "quality_terminal": inputs["terminal"]["receipt_record"],
        "extraction_receipt": inputs["extraction"]["record"],
        "ranking_policy": (
            "highest_strict_full_field_macro_then_lowest_"
            "production_amortized_total_token_ratio"
        ),
        "exact_metric_tie_policy": "reject_no_unique_development_winner",
        "arms": normalized,
        "passing_arm_ids": [row["variant_id"] for row in eligible],
        "ranking": [row["variant_id"] for row in ranked],
        "winner_variant_id": best["variant_id"],
        "semantic_model_call_count_added": 0,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _winner_configuration(
    inputs: Mapping[str, Any], analysis: Mapping[str, Any]
) -> dict[str, Any]:
    loaded = inputs["loaded"]
    winner_id = str(analysis["winner_variant_id"])
    arm_by_id = {
        str(row["variant_id"]): row
        for row in loaded["precommit"]["precommit"]["arms"]
    }
    requests = loaded["preflight"]["requests_by_variant"].get(winner_id)
    arm = arm_by_id.get(winner_id)
    if arm is None or not isinstance(requests, list) or not requests:
        raise CanonicalV31DevelopmentFreezeError(
            "winning arm configuration is absent"
        )
    models = {str(request.get("model")) for request in requests}
    efforts = {str(request.get("effort")) for request in requests}
    packs = {str(request.get("canonical_label_pack")) for request in requests}
    candidate_ids = {str(request.get("candidate_system_id")) for request in requests}
    if len(models) != 1 or len(efforts) != 1 or len(packs) != 1 or len(candidate_ids) != 1:
        raise CanonicalV31DevelopmentFreezeError(
            "winning request configuration is not uniform"
        )
    winner_metrics = next(
        row for row in analysis["arms"] if row["variant_id"] == winner_id
    )
    receipt = inputs["terminal"]["receipt"]
    return {
        "schema_version": WINNER_CONFIGURATION_VERSION,
        "state": "frozen_development_configuration_holdout_closed",
        "candidate_system_id": next(iter(candidate_ids)),
        "winner_variant_id": winner_id,
        "canonical_label_pack": next(iter(packs)),
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "model": next(iter(models)),
        "effort": next(iter(efforts)),
        "request_count": arm["request_count"],
        "request_sha256s": copy.deepcopy(arm["request_sha256s"]),
        "request_set_sha256": arm["request_set_sha256"],
        "prompt_sha256s": copy.deepcopy(arm["prompt_sha256s"]),
        "base_instructions_sha256s": copy.deepcopy(
            arm["base_instructions_sha256s"]
        ),
        "output_schema_sha256s": copy.deepcopy(arm["output_schema_sha256s"]),
        "effective_batch_sizes": copy.deepcopy(arm["effective_batch_sizes"]),
        "thread_lifecycle": arm["thread_lifecycle"],
        "cache_regime": arm["cache_regime"],
        "canonical_adapter_module": loaded["preflight"]["receipt"]["adapter_module"],
        "runtime_binding_sha256": loaded["preflight"]["receipt"][
            "runtime_binding_sha256"
        ],
        "context_control_overlay_sha256": loaded["preflight"]["receipt"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": loaded["preflight"]["receipt"][
            "instruction_source_contract_sha256"
        ],
        "canonical_final_field_paths_sha256": loaded["preflight"]["receipt"][
            "canonical_final_field_paths_sha256"
        ],
        "model_semantic_field_paths_sha256": loaded["preflight"]["receipt"][
            "model_semantic_field_paths_sha256"
        ],
        "deterministic_provenance_field_paths_sha256": loaded["preflight"][
            "receipt"
        ]["deterministic_provenance_field_paths_sha256"],
        "shared_reference_seed": loaded["preflight"]["receipt"][
            "shared_reference_seed_record"
        ],
        "selection_policy": loaded["contract"]["development_selection_policy"],
        "controller_contract": _record(
            loaded["contract_path"], allowed_root=loaded["controller_root"]
        ),
        "controller_execution_receipt": inputs["handoff"]["handoff"][
            "controller_execution_receipt"
        ],
        "extraction_receipt": inputs["extraction"]["record"],
        "quality_handoff": inputs["handoff"]["handoff_record"],
        "quality_terminal": inputs["terminal"]["receipt_record"],
        "quality_evidence_receipt": receipt["quality_evidence_receipt"],
        "quality_recomputed_score": receipt["recomputed_quality_score"],
        "quality_cost_authority": receipt["quality_cost_authority"],
        "strict_full_field_macro": winner_metrics["strict_full_field_macro"],
        "baseline_strict_full_field_macro": receipt[
            "baseline_strict_full_field_macro"
        ],
        "production_amortized_total_token_ratio": winner_metrics[
            "production_amortized_total_token_ratio"
        ],
        "quality_evaluator_frozen": True,
        "shared_reference_frozen": True,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _freeze_receipt(
    *,
    inputs: Mapping[str, Any],
    freeze_root: Path,
    authorization_id: str,
    analysis_record: Mapping[str, Any],
    configuration_record: Mapping[str, Any],
) -> dict[str, Any]:
    loaded = inputs["loaded"]
    return {
        "schema_version": FREEZE_VERSION,
        "state": "development_winner_frozen_holdout_and_production_closed",
        "thread_id": controller.supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "authorized_by": "kolby",
        "operator_authorization_id": authorization_id,
        "freeze_runtime_source": _record(Path(__file__)),
        "controller_contract": _record(
            loaded["contract_path"], allowed_root=loaded["controller_root"]
        ),
        "quality_terminal": inputs["terminal"]["receipt_record"],
        "selection_policy": loaded["contract"]["development_selection_policy"],
        "selection_analysis": copy.deepcopy(dict(analysis_record)),
        "winner_configuration": copy.deepcopy(dict(configuration_record)),
        "winner_variant_id": _load(
            Path(str(configuration_record["path"])), label="winner configuration"
        )["winner_variant_id"],
        "development_winner_frozen": True,
        "quality_evaluator_frozen": True,
        "shared_reference_frozen": True,
        "semantic_model_call_count_added": 0,
        "semantic_retry_count": 0,
        "holdout_inspected": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "production_mutation_allowed": False,
        "freeze_root": str(freeze_root),
    }


def freeze_development_winner(
    *,
    controller_root: Path,
    quality_root: Path,
    freeze_root: Path,
    operator_authorization_id: str,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    controller_path = _safe(
        controller_root, project_root=project, label="controller root"
    )
    quality_path = _safe(quality_root, project_root=project, label="quality root")
    root = _safe(freeze_root, project_root=project, label="freeze root")
    if root in controller_path.parents or root in quality_path.parents:
        raise CanonicalV31DevelopmentFreezeError("freeze root contains an input root")
    if controller_path in root.parents or quality_path in root.parents:
        raise CanonicalV31DevelopmentFreezeError("freeze root is nested in an input root")
    authorization_id = _authorization_id(operator_authorization_id)
    receipt_path = root / FREEZE_RECEIPT_FILENAME
    if receipt_path.is_file():
        verified = verify_development_winner_freeze(root, project_root=project)
        if verified["receipt"]["operator_authorization_id"] != authorization_id:
            raise CanonicalV31DevelopmentFreezeError(
                "existing freeze authorization ID drifted"
            )
        return verified
    if root.exists() and any(root.iterdir()):
        raise CanonicalV31DevelopmentFreezeError(
            "development freeze root is partial or not fresh"
        )
    inputs = _quality_inputs(
        controller_root=controller_path,
        quality_root=quality_path,
        project_root=project,
    )
    analysis = _selection_analysis(inputs)
    configuration = _winner_configuration(inputs, analysis)
    root.mkdir(parents=True, exist_ok=True)
    analysis_record = controller._write_immutable_json(
        root / SELECTION_ANALYSIS_FILENAME, analysis
    )
    configuration_record = controller._write_immutable_json(
        root / WINNER_CONFIGURATION_FILENAME, configuration
    )
    payload = _freeze_receipt(
        inputs=inputs,
        freeze_root=root,
        authorization_id=authorization_id,
        analysis_record=analysis_record,
        configuration_record=configuration_record,
    )
    controller._write_controller_receipt(receipt_path, payload)
    return verify_development_winner_freeze(root, project_root=project)


def verify_development_winner_freeze(
    freeze_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe(freeze_root, project_root=project, label="freeze root")
    required = {
        SELECTION_ANALYSIS_FILENAME,
        WINNER_CONFIGURATION_FILENAME,
        FREEZE_RECEIPT_FILENAME,
    }
    if root.is_symlink() or not root.is_dir() or {p.name for p in root.iterdir()} != required:
        raise CanonicalV31DevelopmentFreezeError(
            "development freeze artifact set drifted"
        )
    receipt = _load(root / FREEZE_RECEIPT_FILENAME, label="freeze receipt")
    authorization_id = _authorization_id(receipt.get("operator_authorization_id"))
    controller_record = receipt.get("controller_contract")
    terminal_record = receipt.get("quality_terminal")
    if not isinstance(controller_record, Mapping) or not isinstance(terminal_record, Mapping):
        raise CanonicalV31DevelopmentFreezeError("freeze input records are malformed")
    controller_path = controller._verify_record(
        controller_record, label="controller contract", allowed_root=project
    ).parent
    quality_path = controller._verify_record(
        terminal_record, label="quality terminal", allowed_root=project
    ).parent
    inputs = _quality_inputs(
        controller_root=controller_path,
        quality_root=quality_path,
        project_root=project,
    )
    analysis = _selection_analysis(inputs)
    observed_analysis = _load(
        root / SELECTION_ANALYSIS_FILENAME, label="selection analysis"
    )
    if observed_analysis != analysis:
        raise CanonicalV31DevelopmentFreezeError("selection analysis drifted")
    analysis_record = _record(root / SELECTION_ANALYSIS_FILENAME, allowed_root=root)
    configuration = _winner_configuration(inputs, analysis)
    observed_configuration = _load(
        root / WINNER_CONFIGURATION_FILENAME, label="winner configuration"
    )
    if observed_configuration != configuration:
        raise CanonicalV31DevelopmentFreezeError("winner configuration drifted")
    configuration_record = _record(
        root / WINNER_CONFIGURATION_FILENAME, allowed_root=root
    )
    expected = _freeze_receipt(
        inputs=inputs,
        freeze_root=root,
        authorization_id=authorization_id,
        analysis_record=analysis_record,
        configuration_record=configuration_record,
    )
    expected["receipt_sha256"] = controller._receipt_checksum(expected)
    if receipt != expected:
        raise CanonicalV31DevelopmentFreezeError("development freeze receipt drifted")
    return {
        "receipt": receipt,
        "receipt_record": _record(root / FREEZE_RECEIPT_FILENAME, allowed_root=root),
        "selection_analysis": observed_analysis,
        "winner_configuration": observed_configuration,
        "development_winner_frozen": True,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def status_development_winner_freeze(
    freeze_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe(freeze_root, project_root=project, label="freeze root")
    if not root.exists():
        state = "not_frozen"
        winner = None
    else:
        verified = verify_development_winner_freeze(root, project_root=project)
        state = verified["receipt"]["state"]
        winner = verified["receipt"]["winner_variant_id"]
    return {
        "schema_version": STATUS_VERSION,
        "state": state,
        "winner_variant_id": winner,
        "semantic_model_call_count_added": 0,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m "
            "research_factory.app_server_canonical_v31_development_freeze"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--controller-root", type=Path, required=True)
    freeze.add_argument("--quality-root", type=Path, required=True)
    freeze.add_argument("--freeze-root", type=Path, required=True)
    freeze.add_argument("--operator-authorization-id", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--freeze-root", type=Path, required=True)
    status = commands.add_parser("status")
    status.add_argument("--freeze-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_development_winner(
                controller_root=args.controller_root,
                quality_root=args.quality_root,
                freeze_root=args.freeze_root,
                operator_authorization_id=args.operator_authorization_id,
            )
        elif args.command == "verify":
            result = verify_development_winner_freeze(args.freeze_root)
        else:
            result = status_development_winner_freeze(args.freeze_root)
    except controller.Epoch7ControllerError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, "result": result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
