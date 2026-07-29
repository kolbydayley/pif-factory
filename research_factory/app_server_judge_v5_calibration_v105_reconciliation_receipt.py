from __future__ import annotations

"""Freeze the v103/v104 reconciliation and authorize one full calibration."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_calibration_v103_unsupported_inference_protocol import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V103_ROOT,
)
from .app_server_judge_v5_calibration_v104_side_free_adjudication import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V104_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V105_AUDIT_VERSION = "pif_app_server_judge_v5_4_v105_reconciliation_audit_v1"
V105_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v105_protocol_receipt_v1"
V105_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v105_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V104_ROOT.parent / "judge-calibration-v5_4-v105-reconciliation-receipt"
).resolve()


class JudgeV5CalibrationV105Error(RuntimeError):
    """The v105 reconciliation cannot preserve its predecessor evidence."""


def _validate_predecessors() -> dict[str, Any]:
    paths = {
        "v103_terminal": DEFAULT_V103_ROOT / "terminal.json",
        "v103_truth": DEFAULT_V103_ROOT / "unsupported-protocol-truth.private.json",
        "v103_output": DEFAULT_V103_ROOT / "unsupported-protocol-output.private.json",
        "v103_canary": DEFAULT_V103_ROOT / "permutation-canary-output.private.json",
        "v103_score": DEFAULT_V103_ROOT / "unsupported-protocol-score.json",
        "v104_terminal": DEFAULT_V104_ROOT / "terminal.json",
        "v104_score": DEFAULT_V104_ROOT / "side-free-adjudication-score.json",
        "v104_proposal": DEFAULT_V104_ROOT / "adjudication-proposal.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t103, s103 = values["v103_terminal"], values["v103_score"]
    t104, s104, p104 = (
        values["v104_terminal"],
        values["v104_score"],
        values["v104_proposal"],
    )
    if (
        t103.get("state") != "inactive"
        or t103.get("development_terminal_reason")
        != "v103_unsupported_protocol_quality_gate_not_passed"
        or t103.get("usage_status") != "complete"
        or t103.get("accounting_complete") is not True
        or t103.get("production_mutated") is not False
        or not _record_matches(t103.get("score"), paths["v103_score"])
        or not _record_matches(t103.get("output"), paths["v103_output"])
        or not _record_matches(t103.get("canary_output"), paths["v103_canary"])
        or s103.get("passed") is not False
        or s103.get("metrics", {}).get("exact_count") != 6
        or s103.get("metrics", {}).get("incorrect_sensitivity") != 1.0
        or s103.get("metrics", {}).get("correct_specificity") != 1.0
        or s103.get("metrics", {}).get("permutation_canary_exact_count") != 1
        or s103.get("metrics", {}).get("evidence_complete_count") != 8
        or s103.get("metrics", {}).get("abstention_count") != 0
        or t104.get("state") != "completed"
        or t104.get("terminal_reason")
        != "v104_side_free_adjudication_passed_reconciliation_authorized"
        or t104.get("reconciliation_authorized") is not True
        or t104.get("reference_change_proposed") is not False
        or t104.get("usage_status") != "complete"
        or t104.get("accounting_complete") is not True
        or t104.get("production_mutated") is not False
        or len(t104.get("attempts") or []) != 1
        or not all(
            _verify_record(record)
            for attempt in t104.get("attempts") or []
            for record in (
                attempt.get("capacity"),
                attempt.get("sidecar"),
                attempt.get("output"),
            )
        )
        or not _record_matches(t104.get("score"), paths["v104_score"])
        or not _record_matches(t104.get("proposal"), paths["v104_proposal"])
        or s104.get("passed") is not True
        or s104.get("metrics", {}).get("control_exact_count") != 3
        or s104.get("metrics", {}).get("evidence_complete_count") != 4
        or s104.get("metrics", {}).get("abstention_count") != 0
        or s104.get("owner_status") != "incorrect"
        or s104.get("reference_change_proposed") is not False
        or p104.get("prior_reference_status") != "incorrect"
        or p104.get("owner_status") != "incorrect"
        or p104.get("reference_change_proposed") is not False
        or p104.get("reconciliation_authorized") is not True
    ):
        raise JudgeV5CalibrationV105Error("v103/v104 reconciliation contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_reconciliation_audit(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    p104 = predecessor["values"]["v104_proposal"]
    return {
        "schema_version": V105_AUDIT_VERSION,
        "created_at": now_iso(),
        "protocol_regression_primary_exact_count": 6,
        "protocol_regression_primary_task_count": 6,
        "raw_permutation_canary_exact_count": 1,
        "raw_permutation_canary_count": 2,
        "observable_disagreement_count": 1,
        "side_free_adjudication_call_count": 1,
        "side_free_control_exact_count": 3,
        "side_free_control_count": 3,
        "adjudicated_permutation_exact_count": 2,
        "adjudicated_permutation_count": 2,
        "field": p104["field"],
        "prior_reference_status": p104["prior_reference_status"],
        "owner_status": p104["owner_status"],
        "reference_change_applied": False,
        "majority_voting_used": False,
        "verbal_confidence_used": False,
        "semantic_decisions_from_llms_only": True,
        "deterministic_scope": "identity_mapping_metric_projection_and_gate_validation_only",
        "privacy": "opaque_identity_field_status_and_counts_only",
    }


def freeze_v105(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v105 terminal")
    predecessor = _validate_predecessors()
    audit = build_reconciliation_audit(predecessor)
    audit_path = root / "reconciliation-audit.json"
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V105_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "judge_model": "gpt-5.5",
        "adjudicator_model": "gpt-5.6-sol",
        "unsupported_inference_semantics": (
            "correct_means_no_unsupported_material_inference_incorrect_means_at_least_one"
        ),
        "reference_version": "fixture_reference_v8_v101_stance_inference_reconciled",
        "reference_change_applied": False,
        "observable_disagreement_policy": (
            "one_side_free_adjudication_call_for_observable_primary_canary_disagreement_only"
        ),
        "fresh_full_development_calibration_authorized": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "reconciliation_audit": _record(audit_path),
        "predecessor": predecessor["records"],
    }
    receipt_path = root / "protocol-authorization-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V105_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v105_reconciliation_frozen_full_calibration_authorized",
        "overall_evaluation_complete": False,
        "fresh_full_development_calibration_authorized": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "reconciliation_audit": _record(audit_path),
        "protocol_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v105 reconciliation receipt")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v105(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fresh_full_development_calibration_authorized": terminal[
                    "fresh_full_development_calibration_authorized"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
