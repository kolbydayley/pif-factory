from __future__ import annotations

"""Adapt verified epoch-7 development evidence to production lineage schemas.

This module performs no semantic work and does not inspect holdout data. It
reconstructs the canonical production matrix and quality lineage receipts from
the verified epoch-7 controller, four-turn quality terminal, and development
winner freeze. The resulting records are inputs to a later untouched-holdout
gate; they do not authorize holdout or production by themselves.
"""

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_freeze as development_freeze
from . import app_server_canonical_v31_epoch7_controller as controller
from . import app_server_canonical_v31_production_contract as production_contract


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROMOTION_LINEAGE_VERSION = "pif_canonical_v31_development_promotion_lineage_v1"
STATUS_VERSION = "pif_canonical_v31_development_promotion_lineage_status_v1"

MATRIX_LINEAGE_FILENAME = "canonical-matrix-lineage.json"
QUALITY_LINEAGE_FILENAME = "canonical-quality-lineage.json"
PROMOTION_LINEAGE_RECEIPT_FILENAME = "development-promotion-lineage-receipt.json"


class CanonicalV31PromotionLineageError(controller.Epoch7ControllerError):
    """Canonical development evidence cannot enter the promotion boundary."""


def _safe(path: Path, *, project_root: Path, label: str) -> Path:
    try:
        return controller._safe_path(path, project_root=project_root, label=label)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc


def _record(path: Path, *, allowed_root: Path | None = None) -> dict[str, Any]:
    try:
        return controller._record(path, allowed_root=allowed_root)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc


def _production_record(path: Path) -> dict[str, Any]:
    try:
        return production_contract._record(path)
    except (OSError, ValueError, production_contract.CanonicalV31ProductionContractError) as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc


def _sha256_json(value: Any) -> str:
    return controller._sha256_bytes(controller._canonical_json(value).encode("ascii"))


def _verified_inputs(
    development_freeze_root: Path,
    *,
    project_root: Path,
) -> dict[str, Any]:
    try:
        frozen = development_freeze.verify_development_winner_freeze(
            development_freeze_root,
            project_root=project_root,
        )
        freeze_receipt = frozen["receipt"]
        controller_root = controller._verify_record(
            freeze_receipt["controller_contract"],
            label="development freeze controller contract",
            allowed_root=project_root,
        ).parent
        quality_root = controller._verify_record(
            freeze_receipt["quality_terminal"],
            label="development freeze quality terminal",
            allowed_root=project_root,
        ).parent
        loaded = controller.load_epoch7_controller(
            controller_root,
            project_root=project_root,
        )
        extraction = controller._verify_extraction_receipt(loaded)
        quality = controller.verify_quality_terminal_receipt(
            controller_root,
            quality_root,
            project_root=project_root,
        )
    except (
        controller.Epoch7ControllerError,
        development_freeze.CanonicalV31DevelopmentFreezeError,
    ) as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc

    winner = frozen["winner_configuration"]
    terminal = quality["receipt"]
    if (
        freeze_receipt.get("development_winner_frozen") is not True
        or freeze_receipt.get("holdout_authorized") is not False
        or freeze_receipt.get("production_mutated") is not False
        or terminal.get("quality_gate_passed") is not True
        or terminal.get("winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or winner.get("winner_variant_id") != freeze_receipt.get("winner_variant_id")
        or winner.get("holdout_authorized") is not False
        or winner.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31PromotionLineageError(
            "development freeze or quality authority drifted"
        )
    return {
        "freeze": frozen,
        "freeze_root": development_freeze_root,
        "loaded": loaded,
        "extraction": extraction,
        "quality": quality,
        "winner": winner,
    }


def _matrix_lineage(inputs: Mapping[str, Any]) -> dict[str, Any]:
    receipt = inputs["extraction"]["receipt"]
    validation = receipt.get("full_output_validation")
    arms = receipt.get("arms")
    expected_ids = list(production_contract.EXPECTED_VARIANT_IDS)
    if (
        not isinstance(validation, Mapping)
        or validation.get("state")
        != "all_six_full_canonical_v31_outputs_validated"
        or validation.get("arm_count") != 6
        or not isinstance(validation.get("arms"), Mapping)
        or list(validation["arms"]) != expected_ids
        or not isinstance(arms, list)
        or [str(row.get("variant_id")) for row in arms] != expected_ids
    ):
        raise CanonicalV31PromotionLineageError(
            "epoch-7 extraction coverage cannot form production matrix lineage"
        )
    runtime = production_contract.canonical_runtime_binding()
    if receipt.get("adapter_runtime_binding_sha256") != runtime["matrix_binding_sha256"]:
        raise CanonicalV31PromotionLineageError(
            "epoch-7 adapter runtime differs from production canonical runtime"
        )
    arm_by_id = {str(row["variant_id"]): row for row in arms}
    validated = validation["arms"]
    payload = {
        "schema_version": production_contract.MATRIX_RECEIPT_VERSION,
        "state": "verified_full_six_arm_canonical_matrix",
        "manifest_sha256": receipt["manifest_sha256"],
        "context_set_sha256": receipt["context_set_sha256"],
        "runtime_binding_sha256": receipt["adapter_runtime_binding_sha256"],
        "context_control_overlay_sha256": receipt[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": receipt[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": receipt["capacity_policy_sha256"],
        "precommit_sha256": receipt["precommit_sha256"],
        "future_plan_binding_sha256": receipt["future_plan_binding_sha256"],
        "future_execution_directive_sha256": receipt["future_directive_sha256"],
        "arm_envelope_sha256s": {
            variant_id: str(arm_by_id[variant_id]["arm_envelope_sha256"])
            for variant_id in expected_ids
        },
        "full_output_validation_sha256": _sha256_json(validation),
        "opaque_case_order_sha256": validation["opaque_case_order_sha256"],
        "case_count": validation["case_count_per_arm"],
        "arm_count": 6,
        "per_arm_full_output_sha256s": {
            variant_id: str(validated[variant_id]["output_sha256"])
            for variant_id in expected_ids
        },
        "per_arm_canonical_labels_sha256s": {
            variant_id: str(validated[variant_id]["labels_sha256"])
            for variant_id in expected_ids
        },
        "managed_chatgpt_auth_only": True,
        "semantic_retry_count": 0,
        "semantic_pruning": False,
        "semantic_relabeling": False,
        "production_mutated": False,
    }
    try:
        return production_contract._verify_matrix_receipt(payload)
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc


def _quality_lineage(
    inputs: Mapping[str, Any],
    *,
    matrix_lineage: Mapping[str, Any],
    matrix_record: Mapping[str, Any],
) -> dict[str, Any]:
    terminal = inputs["quality"]["receipt"]
    winner = inputs["winner"]
    winner_id = str(winner["winner_variant_id"])
    row = next(
        (
            value
            for value in terminal["arm_results"]
            if str(value.get("variant_id")) == winner_id
        ),
        None,
    )
    if (
        not isinstance(row, Mapping)
        or row.get("passed") is not True
        or winner_id not in terminal.get("passing_arm_ids", [])
        or row.get("submitted_exact_evidence_rate") != 1.0
    ):
        raise CanonicalV31PromotionLineageError(
            "frozen winner did not pass exact canonical quality gates"
        )
    payload = {
        "schema_version": production_contract.QUALITY_RECEIPT_VERSION,
        "state": "canonical_development_quality_passed",
        "matrix_lineage_sha256": matrix_record["sha256"],
        "quality_result_sha256": terminal["recomputed_quality_score"]["sha256"],
        "selected_variant_id": winner_id,
        "selected_arm_envelope_sha256": matrix_lineage[
            "arm_envelope_sha256s"
        ][winner_id],
        "selected_full_output_sha256": matrix_lineage[
            "per_arm_full_output_sha256s"
        ][winner_id],
        "selected_canonical_labels_sha256": matrix_lineage[
            "per_arm_canonical_labels_sha256s"
        ][winner_id],
        "strict_full_field_macro": row["strict_full_field_macro"],
        "baseline_strict_full_field_macro": terminal[
            "baseline_strict_full_field_macro"
        ],
        "noninferiority_margin": terminal["noninferiority_margin"],
        "exact_evidence_rate": 1.0,
        "exact_offset_rate": 1.0,
        "exact_provenance_rate": 1.0,
        "production_amortized_total_token_ratio": row[
            "production_amortized_total_token_ratio"
        ],
        "usage_complete": True,
        "evaluation_mode": "llm_only",
        "embeddings_used": False,
        "deterministic_semantic_matching": False,
        "semantic_defaults": {},
        "semantic_pruning": False,
        "semantic_relabeling": False,
        "production_mutated": False,
    }
    try:
        return production_contract._verify_quality_receipt(
            payload,
            matrix=matrix_lineage,
        )
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc


def _receipt_payload(
    *,
    inputs: Mapping[str, Any],
    lineage_root: Path,
    matrix_record: Mapping[str, Any],
    quality_record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": PROMOTION_LINEAGE_VERSION,
        "state": "development_lineage_ready_untouched_holdout_closed",
        "thread_id": controller.supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "adapter_source": _record(Path(__file__)),
        "development_freeze_receipt": inputs["freeze"]["receipt_record"],
        "matrix_lineage": copy.deepcopy(dict(matrix_record)),
        "quality_lineage": copy.deepcopy(dict(quality_record)),
        "winner_variant_id": inputs["winner"]["winner_variant_id"],
        "semantic_model_call_count_added": 0,
        "semantic_retry_count": 0,
        "holdout_inspected": False,
        "holdout_authorized": False,
        "production_authorized": False,
        "production_mutated": False,
        "lineage_root": str(lineage_root),
    }


def freeze_development_promotion_lineage(
    *,
    development_freeze_root: Path,
    lineage_root: Path,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    source_root = _safe(
        development_freeze_root,
        project_root=project,
        label="development freeze root",
    )
    root = _safe(lineage_root, project_root=project, label="promotion lineage root")
    if root == source_root or root in source_root.parents or source_root in root.parents:
        raise CanonicalV31PromotionLineageError(
            "promotion lineage and development freeze roots must be disjoint"
        )
    receipt_path = root / PROMOTION_LINEAGE_RECEIPT_FILENAME
    if receipt_path.is_file():
        return verify_development_promotion_lineage(root, project_root=project)
    if root.exists() and any(root.iterdir()):
        raise CanonicalV31PromotionLineageError(
            "promotion lineage root is partial or not fresh"
        )

    inputs = _verified_inputs(source_root, project_root=project)
    matrix = _matrix_lineage(inputs)
    root.mkdir(parents=True, exist_ok=True)
    matrix_path = root / MATRIX_LINEAGE_FILENAME
    controller._write_immutable_json(matrix_path, matrix)
    matrix_record = _production_record(matrix_path)
    quality = _quality_lineage(
        inputs,
        matrix_lineage=matrix,
        matrix_record=matrix_record,
    )
    quality_path = root / QUALITY_LINEAGE_FILENAME
    controller._write_immutable_json(quality_path, quality)
    quality_record = _production_record(quality_path)
    controller._write_controller_receipt(
        receipt_path,
        _receipt_payload(
            inputs=inputs,
            lineage_root=root,
            matrix_record=matrix_record,
            quality_record=quality_record,
        ),
    )
    return verify_development_promotion_lineage(root, project_root=project)


def verify_development_promotion_lineage(
    lineage_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe(lineage_root, project_root=project, label="promotion lineage root")
    required = {
        MATRIX_LINEAGE_FILENAME,
        QUALITY_LINEAGE_FILENAME,
        PROMOTION_LINEAGE_RECEIPT_FILENAME,
    }
    if root.is_symlink() or not root.is_dir() or {path.name for path in root.iterdir()} != required:
        raise CanonicalV31PromotionLineageError(
            "promotion lineage artifact set drifted"
        )
    receipt_path = root / PROMOTION_LINEAGE_RECEIPT_FILENAME
    receipt = controller._load_object(receipt_path, label="promotion lineage receipt")
    freeze_receipt_path = controller._verify_record(
        receipt.get("development_freeze_receipt"),
        label="development freeze receipt",
        allowed_root=project,
    )
    inputs = _verified_inputs(freeze_receipt_path.parent, project_root=project)
    matrix = _matrix_lineage(inputs)
    matrix_path = root / MATRIX_LINEAGE_FILENAME
    observed_matrix = controller._load_object(matrix_path, label="matrix lineage")
    if observed_matrix != matrix:
        raise CanonicalV31PromotionLineageError("matrix lineage drifted")
    try:
        production_contract._verify_matrix_receipt(observed_matrix)
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc
    matrix_record = _production_record(matrix_path)
    quality = _quality_lineage(
        inputs,
        matrix_lineage=matrix,
        matrix_record=matrix_record,
    )
    quality_path = root / QUALITY_LINEAGE_FILENAME
    observed_quality = controller._load_object(quality_path, label="quality lineage")
    if observed_quality != quality:
        raise CanonicalV31PromotionLineageError("quality lineage drifted")
    try:
        production_contract._verify_quality_receipt(
            observed_quality,
            matrix=observed_matrix,
        )
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise CanonicalV31PromotionLineageError(str(exc)) from exc
    quality_record = _production_record(quality_path)
    expected = _receipt_payload(
        inputs=inputs,
        lineage_root=root,
        matrix_record=matrix_record,
        quality_record=quality_record,
    )
    expected["receipt_sha256"] = controller._receipt_checksum(expected)
    if receipt != expected:
        raise CanonicalV31PromotionLineageError(
            "promotion lineage receipt drifted"
        )
    return {
        "receipt": receipt,
        "receipt_record": _record(receipt_path, allowed_root=root),
        "matrix_lineage": observed_matrix,
        "matrix_record": matrix_record,
        "quality_lineage": observed_quality,
        "quality_record": quality_record,
        "holdout_authorized": False,
        "production_authorized": False,
        "production_mutated": False,
    }


def status_development_promotion_lineage(
    lineage_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe(lineage_root, project_root=project, label="promotion lineage root")
    if not root.exists():
        state = "not_frozen"
        winner = None
    else:
        verified = verify_development_promotion_lineage(root, project_root=project)
        state = verified["receipt"]["state"]
        winner = verified["receipt"]["winner_variant_id"]
    return {
        "schema_version": STATUS_VERSION,
        "state": state,
        "winner_variant_id": winner,
        "semantic_model_call_count_added": 0,
        "holdout_authorized": False,
        "production_authorized": False,
        "production_mutated": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m "
            "research_factory.app_server_canonical_v31_promotion_lineage"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--development-freeze-root", type=Path, required=True)
    freeze.add_argument("--lineage-root", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--lineage-root", type=Path, required=True)
    status = commands.add_parser("status")
    status.add_argument("--lineage-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_development_promotion_lineage(
                development_freeze_root=args.development_freeze_root,
                lineage_root=args.lineage_root,
            )
        elif args.command == "verify":
            result = verify_development_promotion_lineage(args.lineage_root)
        else:
            result = status_development_promotion_lineage(args.lineage_root)
    except controller.Epoch7ControllerError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, "result": result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
