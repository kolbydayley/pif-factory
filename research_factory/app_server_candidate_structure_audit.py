from __future__ import annotations

"""Freeze a sanitized, zero-token structural audit of an extractor candidate."""

import argparse
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from . import app_server_configured_experiment as configured
from . import app_server_flat_request_model_lane as flat
from .app_server_runtime_verifier import ContentHashCache
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


AUDIT_VERSION = "pif_candidate_structure_audit_v1"
LOCK_VERSION = "pif_candidate_structure_audit_runtime_lock_v1"
TERMINAL_VERSION = "pif_candidate_structure_audit_terminal_v1"
USAGE_ZERO = {
    "input_tokens": 0,
    "cached_input_tokens": 0,
    "output_tokens": 0,
    "reasoning_output_tokens": 0,
    "total_tokens": 0,
}
METRIC_FIELDS = (
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_raw_text",
)


class CandidateStructureAuditError(RuntimeError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateStructureAuditError(f"cannot read {label}") from exc


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return configured._record(path, cache=cache)  # noqa: SLF001


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise CandidateStructureAuditError("frozen structural-audit artifact drifted")


def _segment_ref(segment_id: str) -> str:
    return sha256_text(segment_id)[:16]


def audit_candidate_structure(
    *,
    source: Mapping[str, Any],
    output: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> dict[str, Any]:
    """Enumerate nonsemantic contract defects without changing candidate fields."""

    schema_valid = True
    schema_error_class: str | None = None
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        schema_valid = False
        schema_error_class = type(exc).__name__

    source_rows = list(source.get("segments") or [])
    output_rows = list(output.get("segments") or [])
    source_ids = [str(row.get("segment_id")) for row in source_rows]
    output_ids = [str(row.get("segment_id")) for row in output_rows]
    episode_segment_order_exact = (
        output.get("episode_id") == source.get("episode_id")
        and output_ids == source_ids
    )
    output_counts = Counter(output_ids)
    output_by_id = {
        str(row.get("segment_id")): row
        for row in output_rows
        if output_counts[str(row.get("segment_id"))] == 1
    }

    identity_seen: dict[str, dict[str, Any]] = {}
    identity_duplicates: list[dict[str, Any]] = []
    metric_applicability_errors: list[dict[str, Any]] = []
    metric_literal_errors: list[dict[str, Any]] = []
    evidence_range_errors: list[dict[str, Any]] = []
    source_order_errors: list[dict[str, Any]] = []
    segment_audits: list[dict[str, Any]] = []
    total_events = 0
    valid_evidence_ranges = 0

    for source_row in source_rows:
        segment_id = str(source_row.get("segment_id"))
        segment_ref = _segment_ref(segment_id)
        row = output_by_id.get(segment_id)
        units = list(source_row.get("units") or [])
        expected_ids = [str(unit.get("unit_id")) for unit in units]
        expected_counts = Counter(expected_ids)
        if row is None:
            segment_audits.append(
                {
                    "segment_ref": segment_ref,
                    "output_row_present_once": False,
                    "expected_unit_count": len(expected_ids),
                    "receipt_count": 0,
                    "receipt_order_exact": False,
                    "missing_receipt_count": len(expected_ids),
                    "extra_receipt_count": 0,
                    "duplicate_receipt_count": 0,
                    "coverage_audit_valid": False,
                    "unresolved_receipt_count": 0,
                    "receipt_event_total": 0,
                    "event_count": 0,
                    "receipt_event_total_matches": False,
                    "status_event_contract_valid": False,
                    "event_cap_valid": True,
                    "ownership_mismatch_unit_count": 0,
                }
            )
            continue

        receipts = list(row.get("unit_receipts") or [])
        actual_ids = [str(receipt.get("unit_id")) for receipt in receipts]
        actual_counts = Counter(actual_ids)
        missing_receipts = sum((expected_counts - actual_counts).values())
        extra_receipts = sum((actual_counts - expected_counts).values())
        duplicate_receipts = sum(count - 1 for count in actual_counts.values() if count > 1)
        receipt_event_total = sum(
            int(receipt.get("eligible_event_count") or 0) for receipt in receipts
        )
        unresolved_receipts = sum(
            1 for receipt in receipts if receipt.get("unresolved_count") != 0
        )
        coverage = row.get("coverage_audit") or {}
        events = list(row.get("events") or [])
        total_events += len(events)
        unit_index = {unit_id: index for index, unit_id in enumerate(expected_ids)}
        start_counts = {unit_id: 0 for unit_id in expected_ids}
        receipt_counts = {
            str(receipt.get("unit_id")): int(receipt.get("eligible_event_count") or 0)
            for receipt in receipts
            if actual_counts[str(receipt.get("unit_id"))] == 1
        }
        prior_start = -1

        for event_index, raw_event in enumerate(events):
            event = copy.deepcopy(dict(raw_event))
            start_id = str(event.get("evidence_start_unit_id"))
            end_id = str(event.get("evidence_end_unit_id"))
            evidence = ""
            try:
                evidence, _start_char, _end_char, _window_id = configured._evidence(  # noqa: SLF001
                    source_row, start_id, end_id
                )
                valid_evidence_ranges += 1
            except (
                configured.ConfiguredOutputContractError,
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                evidence_range_errors.append(
                    {
                        "segment_ref": segment_ref,
                        "event_index": event_index,
                        "error_class": type(exc).__name__,
                    }
                )
            if start_id in unit_index:
                if unit_index[start_id] < prior_start:
                    source_order_errors.append(
                        {"segment_ref": segment_ref, "event_index": event_index}
                    )
                prior_start = unit_index[start_id]
                start_counts[start_id] += 1

            metric_values = {
                field: str(event.get(field) or "") for field in METRIC_FIELDS
            }
            nonliteral_fields = [
                field
                for field, value in metric_values.items()
                if value and value not in evidence
            ]
            if nonliteral_fields:
                metric_literal_errors.append(
                    {
                        "segment_ref": segment_ref,
                        "event_index": event_index,
                        "fields": nonliteral_fields,
                    }
                )
            if any(metric_values.values()) == (
                event.get("metric_direction") == "not_applicable"
            ):
                metric_applicability_errors.append(
                    {
                        "segment_ref": segment_ref,
                        "event_index": event_index,
                        "metric_direction": event.get("metric_direction"),
                        "nonempty_metric_field_count": sum(
                            bool(value) for value in metric_values.values()
                        ),
                    }
                )

            identity = json.dumps(
                {field: event.get(field) for field in flat.IDENTITY_FIELDS},
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            identity_sha256 = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            location = {"segment_ref": segment_ref, "event_index": event_index}
            if identity_sha256 in identity_seen:
                identity_duplicates.append(
                    {
                        "identity_sha256": identity_sha256,
                        "first": identity_seen[identity_sha256],
                        "second": location,
                    }
                )
            else:
                identity_seen[identity_sha256] = location

        ownership_mismatches = sum(
            1
            for unit_id in expected_ids
            if start_counts.get(unit_id, 0) != receipt_counts.get(unit_id, 0)
        )
        segment_audits.append(
            {
                "segment_ref": segment_ref,
                "output_row_present_once": True,
                "expected_unit_count": len(expected_ids),
                "receipt_count": len(receipts),
                "receipt_order_exact": actual_ids == expected_ids,
                "missing_receipt_count": missing_receipts,
                "extra_receipt_count": extra_receipts,
                "duplicate_receipt_count": duplicate_receipts,
                "coverage_audit_valid": (
                    coverage.get("all_source_units_reviewed") is True
                    and coverage.get("unresolved_count") == 0
                ),
                "unresolved_receipt_count": unresolved_receipts,
                "receipt_event_total": receipt_event_total,
                "event_count": len(events),
                "receipt_event_total_matches": receipt_event_total == len(events),
                "status_event_contract_valid": (
                    (row.get("status") == "coded") == bool(events)
                ),
                "event_cap_valid": len(events) <= 32,
                "ownership_mismatch_unit_count": ownership_mismatches,
            }
        )

    checks = {
        "schema_valid": schema_valid,
        "episode_segment_order_exact": episode_segment_order_exact,
        "every_output_segment_present_once": (
            len(output_rows) == len(source_rows)
            and all(count == 1 for count in output_counts.values())
            and set(output_ids) == set(source_ids)
        ),
        "source_unit_receipt_order_exact": all(
            row["receipt_order_exact"] for row in segment_audits
        ),
        "source_unit_coverage_audit_valid": all(
            row["coverage_audit_valid"] for row in segment_audits
        ),
        "source_unit_receipts_resolved": all(
            row["unresolved_receipt_count"] == 0 for row in segment_audits
        ),
        "receipt_event_totals_match": all(
            row["receipt_event_total_matches"] for row in segment_audits
        ),
        "segment_status_and_event_cap_valid": all(
            row["status_event_contract_valid"] and row["event_cap_valid"]
            for row in segment_audits
        ),
        "exact_evidence_ranges_valid": not evidence_range_errors,
        "events_in_source_order": not source_order_errors,
        "metric_strings_are_exact_evidence_substrings": not metric_literal_errors,
        "metric_direction_applicability_consistent": not metric_applicability_errors,
        "exact_event_identities_unique": not identity_duplicates,
        "source_unit_event_ownership_matches": all(
            row["ownership_mismatch_unit_count"] == 0 for row in segment_audits
        ),
    }
    failed_checks = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": AUDIT_VERSION,
        "structural_gate_passed": not failed_checks,
        "checks": checks,
        "failed_checks": failed_checks,
        "schema_error_class": schema_error_class,
        "segment_audits": segment_audits,
        "totals": {
            "source_segment_count": len(source_rows),
            "output_segment_count": len(output_rows),
            "event_count": total_events,
            "valid_evidence_range_count": valid_evidence_ranges,
            "evidence_range_error_count": len(evidence_range_errors),
            "metric_literal_error_count": len(metric_literal_errors),
            "metric_applicability_error_count": len(metric_applicability_errors),
            "event_source_order_error_count": len(source_order_errors),
            "identity_duplicate_count": len(identity_duplicates),
        },
        "defects": {
            "evidence_range_errors": evidence_range_errors,
            "metric_literal_errors": metric_literal_errors,
            "metric_applicability_errors": metric_applicability_errors,
            "event_source_order_errors": source_order_errors,
            "identity_duplicates": identity_duplicates,
        },
        "deterministic_semantic_decision_made": False,
        "candidate_fields_changed": False,
        "private_content_included": False,
    }


def verify_frozen_audit(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    lock = _load_json(root / "runtime-lock.json", "audit runtime lock")
    if lock.get("schema_version") != LOCK_VERSION:
        raise CandidateStructureAuditError("structural-audit runtime lock drifted")
    cache = ContentHashCache()
    for group in ("runtime_files", "direct_lineage", "audit_artifacts"):
        records = lock.get(group)
        if not isinstance(records, list) or not records:
            raise CandidateStructureAuditError("structural-audit runtime lock is incomplete")
        for record in records:
            _verify_record(record, cache=cache)
    terminal = _load_json(root / "terminal.json", "audit terminal")
    if (
        terminal.get("schema_version") != TERMINAL_VERSION
        or terminal.get("usage") != USAGE_ZERO
        or terminal.get("semantic_model_call_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("goal_status_required") != "active"
    ):
        raise CandidateStructureAuditError("structural-audit terminal drifted")
    _verify_record(terminal.get("runtime_lock") or {}, cache=cache)
    return terminal


def freeze_structural_audit(
    *,
    root: Path,
    source_path: Path,
    output_path: Path,
    schema_path: Path,
    direct_lineage_paths: list[Path],
) -> dict[str, Any]:
    root = root.expanduser().resolve()
    if (root / "terminal.json").exists():
        return verify_frozen_audit(root)
    root.mkdir(parents=True, exist_ok=True)
    source_path = source_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    schema_path = schema_path.expanduser().resolve()
    direct_paths = [
        source_path,
        output_path,
        schema_path,
        *(path.expanduser().resolve() for path in direct_lineage_paths),
    ]
    if len(set(direct_paths)) != len(direct_paths):
        raise CandidateStructureAuditError("duplicate direct-lineage path")
    cache = ContentHashCache()
    direct_records = [_record(path, cache=cache) for path in direct_paths]
    audit = audit_candidate_structure(
        source=_load_json(source_path, "candidate source"),
        output=_load_json(output_path, "candidate output"),
        schema=_load_json(schema_path, "candidate schema"),
    )
    audit.update(
        {
            "created_at": now_iso(),
            "state": "zero_token_structural_audit_completed",
            "semantic_model_call_count": 0,
            "usage": dict(USAGE_ZERO),
            "production_mutated": False,
            "holdout_authorized": False,
            "direct_lineage": direct_records,
        }
    )
    audit_path = root / "event-structure-audit.json"
    configured._write_stable_time(audit_path, audit, "created_at")  # noqa: SLF001
    withdrawal_path = root / "prepared-next-decision-withdrawal.json"
    configured._write_stable_time(  # noqa: SLF001
        withdrawal_path,
        {
            "schema_version": "pif_candidate_structure_audit_decision_withdrawal_v1",
            "created_at": now_iso(),
            "state": "previous_semantic_diagnostic_withdrawn",
            "prior_support_alignment_diagnostic_authorized": False,
            "reason": (
                "The frozen candidate has independent metric-grounding, metric-applicability, "
                "receipt-resolution, receipt-total, and event-ownership defects. Ignoring only "
                "receipt coverage would not make it structurally eligible for semantic scoring."
            ),
            "candidate_extraction_replayed": False,
            "semantic_model_call_count": 0,
            "support_alignment_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "audit": _record(audit_path, cache=cache),
        },
        "created_at",
    )
    runtime_files = [
        Path(__file__).resolve(),
        Path(configured.__file__).resolve(),
        Path(flat.__file__).resolve(),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "semantic_model_call_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [_record(path, cache=cache) for path in runtime_files],
        "direct_lineage": direct_records,
        "audit_artifacts": [
            _record(audit_path, cache=cache),
            _record(withdrawal_path, cache=cache),
        ],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "development_candidate_structural_gate_not_passed",
        "blocker_class": "no_safe_semantic_evaluation_of_structurally_invalid_candidate",
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "development_winner_frozen": False,
        "support_alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "extraction_queue_mutated": False,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": dict(USAGE_ZERO),
        "failed_checks": audit["failed_checks"],
        "event_structure_audit": _record(audit_path, cache=cache),
        "prepared_next_decision_withdrawal": _record(withdrawal_path, cache=cache),
        "runtime_lock": _record(lock_path, cache=cache),
        "exact_next_action": (
            "Reject the router/Luna candidate without support, alignment, holdout, or repair. "
            "Retain the measured best cost/quality result and require a genuinely distinct "
            "LLM-only architecture or a direct acceptance-contract change."
        ),
        "privacy": "sanitized hashes counts enums and failure classes only",
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return verify_frozen_audit(root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--lineage", type=Path, action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    terminal = freeze_structural_audit(
        root=args.root,
        source_path=args.source,
        output_path=args.output,
        schema_path=args.schema,
        direct_lineage_paths=list(args.lineage),
    )
    print(
        json.dumps(
            {
                "ok": True,
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_model_call_count": terminal["semantic_model_call_count"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
