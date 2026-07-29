from __future__ import annotations

"""Freeze the LLM-only reconcile-or-abstain protocol after v75/v76."""

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V75_ROOT,
)
from .app_server_judge_v5_calibration_v76_neutral_contested_adjudication import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V76_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _record, _sha256_file
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .util import now_iso


V77_DELTA_VERSION = "pif_app_server_judge_v5_4_v77_reconciled_delta_v1"
V77_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v77_reconciliation_receipt_v1"
V77_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v77_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V76_ROOT.parent / "judge-calibration-v5_4-v77-reconcile-or-abstain-design"
).resolve()


class JudgeV5CalibrationV77Error(RuntimeError):
    """The v77 reconciliation design cannot bind its semantic predecessors."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_predecessors(
    v75_root: Path, v76_root: Path
) -> dict[str, dict[str, Any]]:
    paths = {
        "v75_terminal": v75_root / "terminal.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v75_output": v75_root / "direct-field-output.private.json",
        "v75_score": v75_root / "direct-field-score.json",
        "v76_terminal": v76_root / "terminal.json",
        "v76_spec": v76_root / "neutral-adjudication-spec.json",
        "v76_truth": v76_root / "neutral-adjudication-truth.private.json",
        "v76_output": v76_root / "neutral-adjudication-output.private.json",
        "v76_score": v76_root / "neutral-adjudication-score.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v75_terminal = values["v75_terminal"]
    v76_terminal = values["v76_terminal"]
    if (
        v75_terminal.get("state") != "inactive"
        or v75_terminal.get("development_terminal_reason")
        != "v75_direct_field_audit_quality_gate_not_passed"
        or v75_terminal.get("usage_status") != "complete"
        or v75_terminal.get("accounting_complete") is not True
        or v75_terminal.get("production_mutated") is not False
        or not _record_matches(v75_terminal.get("output"), paths["v75_output"])
        or not _record_matches(v75_terminal.get("score"), paths["v75_score"])
        or v76_terminal.get("state") != "inactive"
        or v76_terminal.get("development_terminal_reason")
        != "v76_neutral_adjudication_quality_gate_not_passed"
        or v76_terminal.get("usage_status") != "complete"
        or v76_terminal.get("accounting_complete") is not True
        or v76_terminal.get("production_mutated") is not False
        or not _record_matches(v76_terminal.get("output"), paths["v76_output"])
        or not _record_matches(v76_terminal.get("score"), paths["v76_score"])
    ):
        raise JudgeV5CalibrationV77Error("v75/v76 predecessors are inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def build_reconciliation(
    v75_truth: Mapping[str, Any],
    v75_output: Mapping[str, Any],
    v76_truth: Mapping[str, Any],
    v76_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    base_truth = {row["task_id"]: row for row in v75_truth["tasks"]}
    primary = {row["task_id"]: row for row in v75_output["decisions"]}
    adjud_truth = {row["task_id"]: row for row in v76_truth["tasks"]}
    adjud = {row["task_id"]: row for row in v76_output["decisions"]}
    if set(adjud_truth) != set(adjud):
        raise JudgeV5CalibrationV77Error("v76 adjudication coverage drifted")
    rows = []
    for adjudication_id in sorted(adjud_truth):
        private = adjud_truth[adjudication_id]
        original_id = private["original_task_id"]
        if original_id not in base_truth or original_id not in primary:
            raise JudgeV5CalibrationV77Error("v77 original task binding drifted")
        primary_status = primary[original_id]["field_status"]
        adjudicated_status = adjud[adjudication_id]["field_status"]
        if primary_status in {"correct", "incorrect"} and primary_status == adjudicated_status:
            reconciled = primary_status
            basis = "independent_llm_agreement"
        elif primary_status == "abstain" and adjudicated_status in {"correct", "incorrect"}:
            reconciled = adjudicated_status
            basis = "capped_adjudicator_resolved_primary_abstention"
        else:
            reconciled = "abstain"
            basis = "independent_llm_disagreement_abstain"
        rows.append(
            {
                "original_task_id": original_id,
                "field": private["field"],
                "prior_status": base_truth[original_id]["expected_status"],
                "primary_status": primary_status,
                "adjudicated_status": adjudicated_status,
                "reconciled_status": reconciled,
                "reconciliation_basis": basis,
            }
        )
    changes = [
        row
        for row in rows
        if row["reconciled_status"] in {"correct", "incorrect"}
        and row["reconciled_status"] != row["prior_status"]
    ]
    abstentions = [row for row in rows if row["reconciled_status"] == "abstain"]
    if (
        len(rows) != 10
        or len(changes) != 4
        or Counter(row["field"] for row in changes) != Counter({"event_boundary": 1, "target": 3})
        or len(abstentions) != 3
        or Counter(row["field"] for row in abstentions)
        != Counter({"attribution": 1, "metric": 1, "stance": 1})
    ):
        raise JudgeV5CalibrationV77Error("v77 reconciliation outcome drifted")
    delta = {
        "schema_version": V77_DELTA_VERSION,
        "rows": rows,
        "change_count": len(changes),
        "abstention_count": len(abstentions),
        "semantic_decisions_from_llms_only": True,
        "deterministic_reconciliation_only": True,
    }
    receipt = {
        "schema_version": V77_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen_design_only",
        "settled_change_count": len(changes),
        "settled_change_field_counts": dict(sorted(Counter(row["field"] for row in changes).items())),
        "abstention_count": len(abstentions),
        "abstention_field_counts": dict(
            sorted(Counter(row["field"] for row in abstentions).items())
        ),
        "reconciliation_rule": "agree_nonabstain_else_capped_adjudicator_resolves_primary_abstain_else_abstain",
        "majority_voting_used": False,
        "reference_mutated": False,
        "fresh_15_development_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "counts_and_field_enums_only",
    }
    return delta, receipt


def freeze_v77(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    v76_root: Path = DEFAULT_V76_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v75_root.resolve(), v76_root.resolve())
    delta, receipt = build_reconciliation(
        _load_json(v75_root / "selected-truth.private.json", "v75 truth"),
        _load_json(v75_root / "direct-field-output.private.json", "v75 output"),
        _load_json(v76_root / "neutral-adjudication-truth.private.json", "v76 truth"),
        _load_json(v76_root / "neutral-adjudication-output.private.json", "v76 output"),
    )
    delta_path = root / "reconciled-delta.private.json"
    receipt_path = root / "reconciliation-receipt.json"
    if receipt_path.exists():
        prior = _load_json(receipt_path, "v77 receipt")
        receipt["created_at"] = prior.get("created_at")
    _write_immutable(delta_path, delta)
    _write_immutable(receipt_path, receipt)
    terminal_path = root / "terminal.json"
    terminal = {
        "schema_version": V77_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v77_reconcile_or_abstain_design_frozen_fresh_15_authorized",
        "overall_evaluation_complete": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "predecessors": predecessors,
        "reconciled_delta": _record(delta_path),
        "reconciliation_receipt": _record(receipt_path),
        "fresh_15_development_diagnostic_authorized": True,
        "reference_mutated": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
    }
    if terminal_path.exists():
        prior = _load_json(terminal_path, "v77 terminal")
        terminal["terminal_at"] = prior.get("terminal_at")
    _write_immutable(terminal_path, terminal)
    return terminal


def main() -> int:
    terminal = freeze_v77()
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fresh_15_development_diagnostic_authorized": terminal[
                    "fresh_15_development_diagnostic_authorized"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
