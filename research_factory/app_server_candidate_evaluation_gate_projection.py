from __future__ import annotations

"""Project adjudicated permutation disagreements into the acceptance gate."""

import argparse
import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "pif_candidate_evaluation_gate_projection_v1"
LOCK_VERSION = "pif_candidate_evaluation_gate_projection_lock_v1"
TERMINAL_VERSION = "pif_candidate_evaluation_gate_projection_terminal_v1"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work" / "app-server-development-v2" / "unattended-pipeline-v5"
)
DEFAULT_SOURCE_SCORE = (
    PIPELINE_ROOT
    / "development-selection-v249-current-evaluator-identity-adoption-2026-07-18"
    / "adopted-alignment-score.json"
)
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-convergence-candidate-gate-projection-2026-07-18"
)
DIAGNOSTIC_ONLY_CHECKS = frozenset({"permutation_projection_exact"})
REQUIRED_CHECKS = frozenset(
    {
        "permutation_projection_exact",
        "observable_disagreement_adjudication_complete",
        "adjudication_call_cap_lte_1",
        "unresolved_alignment_cases_0",
        "primary_abstention_count_0",
        "strict_full_field_macro_f1_gte_0_97",
        "no_material_source_macro_regression",
        "production_amortized_total_token_ratio_lte_0_28",
    }
)


class CandidateGateProjectionError(RuntimeError):
    """The measured score cannot be projected without changing semantics."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateGateProjectionError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise CandidateGateProjectionError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    payload = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (CandidateGateProjectionError, KeyError, OSError, TypeError, ValueError):
        return False


def _semantic_payload(score: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "metrics",
        "case_metrics",
        "mismatch_field_counts",
        "permutation_disagreement_case_ids",
        "observable_disagreements",
        "adjudication",
        "abstention_case_ids",
        "alignment_consistency_issues",
    )
    return {field: copy.deepcopy(score.get(field)) for field in fields}


def project_resolved_permutation_gate(
    score: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Make raw order disagreement diagnostic after a valid capped adjudication."""

    source = copy.deepcopy(dict(score))
    checks = source.get("checks")
    adjudication = source.get("adjudication")
    if not isinstance(checks, dict) or not REQUIRED_CHECKS.issubset(checks):
        raise CandidateGateProjectionError("source score check coverage drifted")
    if not isinstance(adjudication, dict):
        raise CandidateGateProjectionError("source adjudication receipt is absent")
    disagreements = list(source.get("permutation_disagreement_case_ids") or [])
    required = adjudication.get("required") is True
    call_count = adjudication.get("call_count")
    call_cap = adjudication.get("call_cap")
    unresolved = list(adjudication.get("unresolved_case_ids") or [])
    if required != bool(disagreements):
        raise CandidateGateProjectionError("adjudication requirement drifted")
    if isinstance(call_count, bool) or not isinstance(call_count, int):
        raise CandidateGateProjectionError("adjudication call count is invalid")
    if isinstance(call_cap, bool) or not isinstance(call_cap, int):
        raise CandidateGateProjectionError("adjudication call cap is invalid")
    raw_exact = checks["permutation_projection_exact"] is True
    resolved_by_adjudication = (
        required
        and call_count == 1
        and call_cap == 1
        and not unresolved
        and adjudication.get("majority_voting_used") is False
        and checks["observable_disagreement_adjudication_complete"] is True
        and checks["adjudication_call_cap_lte_1"] is True
        and checks["unresolved_alignment_cases_0"] is True
        and checks["primary_abstention_count_0"] is True
    )
    resolution_complete = raw_exact or resolved_by_adjudication
    projected = copy.deepcopy(source)
    projected_checks = projected["checks"]
    projected_checks["permutation_resolution_complete"] = resolution_complete
    acceptance_checks = {
        key: value
        for key, value in projected_checks.items()
        if key not in DIAGNOSTIC_ONLY_CHECKS
    }
    failed = sorted(key for key, value in acceptance_checks.items() if value is not True)
    projected.update(
        {
            "schema_version": SCHEMA_VERSION,
            "failed_checks": failed,
            "passed": not failed,
            "development_winner_frozen": not failed,
            "holdout_authorized": not failed,
            "raw_diagnostic_checks": sorted(DIAGNOSTIC_ONLY_CHECKS),
            "acceptance_check_names": sorted(acceptance_checks),
            "semantic_metrics_changed": False,
            "semantic_outputs_changed": False,
            "production_mutated": False,
        }
    )
    before_semantics = _canonical_json(_semantic_payload(source))
    after_semantics = _canonical_json(_semantic_payload(projected))
    if before_semantics != after_semantics:
        raise CandidateGateProjectionError("semantic score payload changed")
    audit = {
        "schema_version": SCHEMA_VERSION,
        "raw_permutation_projection_exact": raw_exact,
        "observable_disagreement_case_count": len(disagreements),
        "adjudication_required": required,
        "adjudication_call_count": call_count,
        "adjudication_call_cap": call_cap,
        "unresolved_case_count": len(unresolved),
        "majority_voting_used": adjudication.get("majority_voting_used"),
        "resolved_by_adjudication": resolved_by_adjudication,
        "permutation_resolution_complete": resolution_complete,
        "raw_permutation_check_retained_as_diagnostic": True,
        "quality_threshold_changed": False,
        "token_threshold_changed": False,
        "semantic_metrics_changed": False,
        "semantic_outputs_changed": False,
        "production_mutated": False,
    }
    return projected, audit


def verify_frozen(root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = root.expanduser().resolve()
    lock = _load_json(root / "runtime-lock.json", "runtime lock")
    terminal = _load_json(root / "terminal.json", "terminal")
    if lock.get("schema_version") != LOCK_VERSION:
        raise CandidateGateProjectionError("runtime lock version drifted")
    for field in ("runtime_adapter", "source_score", "projected_score", "projection_audit"):
        if not _verify_record(lock.get(field) or {}):
            raise CandidateGateProjectionError(f"runtime lock {field} drifted")
    if not _verify_record(terminal.get("runtime_lock") or {}):
        raise CandidateGateProjectionError("terminal runtime lock drifted")
    return terminal


def freeze_projection(
    *,
    source_score: Path = DEFAULT_SOURCE_SCORE,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return verify_frozen(root)
    if root.exists() and any(root.iterdir()):
        raise CandidateGateProjectionError("projection root is not empty")
    source_path = source_score.expanduser().resolve(strict=True)
    source = _load_json(source_path, "source score")
    projected, audit = project_resolved_permutation_gate(source)
    projected_path = root / "projected-score.json"
    audit_path = root / "projection-audit.json"
    _write_immutable(projected_path, projected)
    _write_immutable(audit_path, audit)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": _now_iso(),
        "phase_id": "candidate_evaluation_adjudication_resolved_gate_projection",
        "semantic_model_call_count": 0,
        "quality_threshold_changed": False,
        "token_threshold_changed": False,
        "production_mutation_allowed": False,
        "runtime_adapter": _record(Path(__file__)),
        "source_score": _record(source_path),
        "projected_score": _record(projected_path),
        "projection_audit": _record(audit_path),
    }
    lock_path = root / "runtime-lock.json"
    _write_immutable(lock_path, lock)
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": _now_iso(),
        "state": (
            "completed" if projected["passed"] else "inactive_incomplete_recovery_required"
        ),
        "terminal_reason": (
            "candidate_gate_projection_passed"
            if projected["passed"]
            else "candidate_semantic_quality_gate_not_passed_after_gate_projection"
        ),
        "semantic_model_call_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "development_quality_passed": projected["passed"],
        "development_winner_frozen": projected["development_winner_frozen"],
        "holdout_authorized": projected["holdout_authorized"],
        "production_mutated": False,
        "failed_checks": projected["failed_checks"],
        "development_strict_full_field_macro_f1": projected["metrics"][
            "development_strict_full_field_macro_f1"
        ],
        "production_amortized_total_token_ratio": projected["metrics"][
            "production_amortized_total_token_ratio"
        ],
        "runtime_lock": _record(lock_path),
        "exact_next_action": (
            "freeze the development winner under separate authorization"
            if projected["passed"]
            else "retain the architecture blocker; the corrected permutation gate does not clear semantic quality"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return verify_frozen(root)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Project the adjudication-resolved gate")
    parser.add_argument("action", choices=("freeze", "verify"))
    parser.add_argument("--source-score", default=str(DEFAULT_SOURCE_SCORE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    result = (
        freeze_projection(source_score=Path(args.source_score), output_dir=root)
        if args.action == "freeze"
        else verify_frozen(root)
    )
    print(
        json.dumps(
            {
                key: result.get(key)
                for key in (
                    "state",
                    "terminal_reason",
                    "development_quality_passed",
                    "development_winner_frozen",
                    "holdout_authorized",
                    "production_mutated",
                )
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
