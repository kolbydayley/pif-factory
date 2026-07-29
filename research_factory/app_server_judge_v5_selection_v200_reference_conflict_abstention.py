from __future__ import annotations

"""Apply v199's predeclared abstention fallback without another model call."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v198_residual_repair_alignment as v198
from . import app_server_judge_v5_selection_v199_reference_conflict_adjudication as v199
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records
from .util import now_iso


V200_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v200_spec_v1"
V200_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v200_terminal_v1"
V200_AUDIT_VERSION = "pif_app_server_reference_conflict_abstention_audit_v1"
V200_RECONCILIATION_VERSION = "pif_app_server_reference_conflict_reconciliation_v2"
DEFAULT_OUTPUT_ROOT = (
    v199.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v200-reference-conflict-abstention"
).resolve()


class JudgeV5SelectionV200Error(RuntimeError):
    """The immutable v199 timeout cannot be reconciled safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v199_timeout() -> dict[str, Any]:
    root = v199.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "reference-conflict-adjudication-spec.json",
        "plan": root / "reference-conflict-adjudication-plan.private.json",
    }
    values = {name: _load_json(path, f"v199 {name}") for name, path in paths.items()}
    terminal, failure, spec, plan = (
        values["terminal"],
        values["failure"],
        values["spec"],
        values["plan"],
    )
    expected_cumulative = {
        "input_tokens": 7355828,
        "cached_input_tokens": 876032,
        "output_tokens": 1352413,
        "reasoning_output_tokens": 439201,
        "total_tokens": 8708241,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("scoring_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage_status") != "unknown"
        or terminal.get("usage") is not None
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 2
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 220000
        or terminal.get("failure") != _record(paths["failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v199.TURN_NAME
        or failure.get("error_class") != "AppServerTurnTimeout"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "unknown"
        or failure.get("unknown_usage_turn_count") != 1
        or failure.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or spec.get("turn_plan") != [v199.TURN_NAME]
        or spec.get("adjudication_call_cap") != 1
        or spec.get("retry_count_per_turn") != 0
        or spec.get("scoring_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or plan.get("repair_placement_dispute_count") != 3
        or plan.get("frozen_reference_only_conflict_count") != 4
        or plan.get("fallback")
        != "unresolved_repair_placement_abstains_its_original_case"
    ):
        raise JudgeV5SelectionV200Error("v199 timeout contract drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV200Error("v199 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV200Error("v199 runtime binding drifted")
    records = [
        spec["capacity_policy"],
        spec["capacity_audit"],
        *spec["predecessor"].values(),
        *spec["frozen_inputs"].values(),
    ]
    if any(not _verify_record(record) for record in records):
        raise JudgeV5SelectionV200Error("v199 frozen binding drifted")
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if (
        len(attempts) != 1
        or attempts[0].get("state") != "interrupted"
        or attempts[0].get("status") != "timeout"
        or attempts[0].get("usage_status") != "unknown"
        or attempts[0].get("output") is not None
    ):
        raise JudgeV5SelectionV200Error("v199 interrupted attempt drifted")
    for key in ("capacity", "sidecar"):
        if not isinstance(attempts[0].get(key), Mapping) or not _verify_record(
            attempts[0][key]
        ):
            raise JudgeV5SelectionV200Error(f"v199 {key} record drifted")
    source = v199._validate_v198_conflict()
    _value, recomputed_plan = v199.build_v199_input(source)
    if recomputed_plan != plan:
        raise JudgeV5SelectionV200Error("v199 dispute plan drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "spec": spec,
        "plan": plan,
        "attempt": attempts[0],
        "source": source,
    }


def build_abstention_reconciliation(
    predecessor: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = predecessor["source"]
    conflicts_by_case: dict[str, dict[int, Mapping[str, Any]]] = {}
    abstained_cases = set()
    for conflict in predecessor["plan"]["conflicts"]:
        case_id = str(conflict["original_case_id"])
        conflicts_by_case.setdefault(case_id, {})[
            int(conflict["original_group_index"])
        ] = conflict
        if conflict["repair_witness_ids"]:
            abstained_cases.add(case_id)
    if len(abstained_cases) != 2:
        raise JudgeV5SelectionV200Error("v200 abstention case coverage drifted")
    reconciled_cases = []
    for original in source["alignment"]["cases"]:
        case_id = str(original["case_id"])
        conflicts = conflicts_by_case.get(case_id, {})
        if not conflicts:
            reconciled_cases.append(deepcopy(original))
            continue
        groups = []
        conflicted_ids = set()
        for group_index, group in enumerate(original["equivalence_groups"]):
            conflict = conflicts.get(group_index)
            if conflict is None:
                groups.append([str(value) for value in group])
                continue
            ids = [
                *map(str, conflict["reference_witness_ids"]),
                *map(str, conflict["repair_witness_ids"]),
            ]
            conflicted_ids.update(ids)
            groups.extend([[value] for value in ids])
        pairs = [
            deepcopy(pair)
            for pair in original["alignment_pairs"]
            if not (set(map(str, pair["witness_ids"])) & conflicted_ids)
        ]
        all_ids = {str(value) for group in groups for value in group}
        paired_ids = {
            str(value) for pair in pairs for value in pair["witness_ids"]
        }
        reconciled_cases.append(
            {
                **deepcopy(original),
                "equivalence_groups": sorted(
                    [sorted(set(group)) for group in groups],
                    key=lambda group: tuple(group),
                ),
                "alignment_pairs": pairs,
                "unpaired_witness_ids": sorted(all_ids - paired_ids),
                "status": "partial_abstain" if case_id in abstained_cases else "accepted",
            }
        )
    reconciled_cases.sort(key=lambda row: str(row["case_id"]))
    conflict_audit = v198.build_reference_conflict_audit(
        reconciled_cases, source["mapping"]
    )
    if conflict_audit["frozen_reference_partition_conflict_count"] != 0:
        raise JudgeV5SelectionV200Error("v200 retained a frozen reference conflict")
    reconciliation = {
        "schema_version": V200_RECONCILIATION_VERSION,
        "cases": reconciled_cases,
        "adjudication_decisions": {},
        "abstained_original_case_ids": sorted(abstained_cases),
        "frozen_reference_partition_preserved": True,
        "failed_adjudication_output_used": False,
        "majority_voting_used": False,
    }
    audit = {
        "schema_version": V200_AUDIT_VERSION,
        "observable_v198_conflict_count": 7,
        "frozen_reference_only_conflict_count": 4,
        "unresolved_repair_placement_count": 3,
        "abstained_original_case_count": 2,
        "post_reconciliation_reference_conflict_count": 0,
        "fallback_basis": "predeclared_v199_unresolved_repair_placement_abstention",
        "v199_semantic_attempt_replayed": False,
        "new_semantic_model_call_count": 0,
        "failed_adjudication_output_used": False,
        "scoring_authorized": True,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "production_mutated": False,
    }
    return reconciliation, audit


def freeze_v200(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v200 terminal")}
    predecessor = _validate_v199_timeout()
    reconciliation, audit = build_abstention_reconciliation(predecessor)
    reconciliation_path = root / "residual-repair-alignment-reconciled.private.json"
    audit_path = root / "reference-conflict-abstention-audit.json"
    _write_immutable(reconciliation_path, reconciliation)
    _write_immutable(audit_path, audit)
    spec = {
        "schema_version": V200_SPEC_VERSION,
        "state": "zero_token_abstention_fallback_completed",
        "created_at": now_iso(),
        "strategy": "apply_predeclared_abstention_fallback_after_single_capped_owner_timeout",
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "v199_semantic_attempt_replayed": False,
        "failed_adjudication_output_used": False,
        "frozen_reference_partition_preserved": True,
        "scoring_authorized": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "runtime_files": [_record(Path(__file__)), _record(Path(v199.__file__)), _record(Path(v198.__file__))],
        "predecessor": predecessor["records"],
        "frozen_inputs": {"v199_attempt": predecessor["attempt"]},
        "reconciliation": _record(reconciliation_path),
        "audit": _record(audit_path),
        "privacy": "private_opaque_ids_sanitized_counts_hashes_no_source_or_event_text",
    }
    spec_path = root / "reference-conflict-abstention-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V200_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v200_predeclared_abstention_fallback_scoring_authorized",
        "terminal_classification": "active_development_recovery_required",
        "overall_evaluation_complete": False,
        "frozen_reference_partition_preserved": True,
        "unresolved_repair_placement_count": 3,
        "abstained_original_case_count": 2,
        "scoring_authorized": True,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": zero_usage,
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "reconciliation": _record(reconciliation_path),
        "reconciliation_audit": _record(audit_path),
        "spec": _record(spec_path),
        "required_next_artifact_path": str(
            root.parent / "development-selection-v5_4-v201-residual-repair-score" / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return {
        "root": root,
        "predecessor": predecessor,
        "reconciliation": reconciliation,
        "audit": audit,
        "spec": spec,
        "spec_path": spec_path,
        "terminal": terminal,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v200 abstention fallback")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v200(output_dir=Path(args.output_dir))["terminal"]
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "scoring_authorized": terminal["scoring_authorized"],
                "usage_status": terminal["usage_status"],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
