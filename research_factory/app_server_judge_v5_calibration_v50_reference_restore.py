from __future__ import annotations

"""Restore the pre-semantic canonical v5 calibration reference.

The v45/v47 reference lineage was derived from development judge proposals and
drifted from the fixture audit frozen before pipeline-v5 semantic calls.  This
module performs no semantic inference.  It regenerates the audited reference,
verifies the already-used witness pools, records a sanitized structural diff,
and rescales v49 only as non-promotional forensic evidence.
"""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration import (
    make_v5_calibration_pool,
    validate_v5_calibration_truth,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    DEFAULT_PIPELINE_ROOT,
    _load_json,
    _subset_truth,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v48_diagnostic import (
    TARGET_CASE_IDS,
    _score_v48,
)
from .app_server_judge_v5_diagnostic import _record, _sha256_file
from .app_server_judge_v5_fixture import DEFAULT_FIXTURE_AUDIT_PATH
from .util import now_iso


V50_AUDIT_VERSION = "pif_app_server_judge_v5_4_v50_reference_restoration_audit_v1"
V50_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v50_reference_restoration_receipt_v1"
V50_RESCORE_VERSION = "pif_app_server_judge_v5_4_v50_v49_canonical_rescore_v1"
V50_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v50_reference_restoration_terminal_v1"

DEFAULT_V45_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v45-reference-freeze-receipt"
).resolve()
DEFAULT_V46_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v46-full-development-diagnostic"
).resolve()
DEFAULT_V47_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v47-reference-consistency-freeze"
).resolve()
DEFAULT_V49_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v49-bipartite-targeted-diagnostic-recovery"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v50-canonical-reference-restoration"
).resolve()


class JudgeV5CalibrationV50ReferenceError(RuntimeError):
    """The canonical reference cannot be restored without breaking provenance."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _normalized_groups(value: Any) -> list[list[str]]:
    return sorted(
        (sorted(str(item) for item in group) for group in (value or [])),
        key=lambda group: tuple(group),
    )


def _pair_map(case: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    result: dict[tuple[str, str], Mapping[str, Any]] = {}
    for pair in case.get("pairs") or []:
        key = tuple(sorted(str(value) for value in pair.get("witness_ids") or []))
        if len(key) != 2 or key in result:
            raise JudgeV5CalibrationV50ReferenceError("reference pair topology is malformed")
        result[key] = pair
    return result


def audit_reference_drift(
    source_truth: Mapping[str, Any], canonical_truth: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare reference structures using only opaque IDs and frozen enums."""

    source_cases = source_truth.get("cases")
    canonical_cases = canonical_truth.get("cases")
    if not isinstance(source_cases, Mapping) or not isinstance(canonical_cases, Mapping):
        raise JudgeV5CalibrationV50ReferenceError("reference cases are malformed")
    if not set(source_cases) <= set(canonical_cases):
        raise JudgeV5CalibrationV50ReferenceError("source reference is outside canonical pool")

    category_counts: Counter[str] = Counter()
    cases: list[dict[str, Any]] = []
    for case_id in sorted(source_cases):
        source = source_cases[case_id]
        canonical = canonical_cases[case_id]
        if source.get("base_case_id") != canonical.get("base_case_id"):
            raise JudgeV5CalibrationV50ReferenceError("base case identity drifted")
        changes: list[dict[str, Any]] = []
        for field in ("proposition", "structured_fields", "field_issues"):
            before = source.get(field)
            after = canonical.get(field)
            if not isinstance(before, Mapping) or not isinstance(after, Mapping):
                raise JudgeV5CalibrationV50ReferenceError("pointwise reference is malformed")
            if set(before) != set(after):
                raise JudgeV5CalibrationV50ReferenceError("witness coverage drifted")
            for witness_id in sorted(before):
                if before[witness_id] != after[witness_id]:
                    changes.append(
                        {
                            "category": field,
                            "witness_id": str(witness_id),
                            "before": deepcopy(before[witness_id]),
                            "after": deepcopy(after[witness_id]),
                        }
                    )
                    category_counts[field] += 1

        before_pairs = _pair_map(source)
        after_pairs = _pair_map(canonical)
        for key in sorted(set(before_pairs) - set(after_pairs)):
            changes.append(
                {"category": "pair_removed", "witness_ids": list(key)}
            )
            category_counts["pair_removed"] += 1
        for key in sorted(set(after_pairs) - set(before_pairs)):
            changes.append(
                {"category": "pair_restored", "witness_ids": list(key)}
            )
            category_counts["pair_restored"] += 1
        for key in sorted(set(before_pairs) & set(after_pairs)):
            before_pair = before_pairs[key]
            after_pair = after_pairs[key]
            for field in ("relation", "mismatch_fields"):
                before_value = deepcopy(before_pair.get(field))
                after_value = deepcopy(after_pair.get(field))
                if field == "mismatch_fields":
                    before_value = sorted(str(value) for value in (before_value or []))
                    after_value = sorted(str(value) for value in (after_value or []))
                if before_value != after_value:
                    category = f"pair_{field}"
                    changes.append(
                        {
                            "category": category,
                            "witness_ids": list(key),
                            "before": before_value,
                            "after": after_value,
                        }
                    )
                    category_counts[category] += 1

        before_groups = _normalized_groups(source.get("equivalence_groups"))
        after_groups = _normalized_groups(canonical.get("equivalence_groups"))
        if before_groups != after_groups:
            changes.append(
                {
                    "category": "equivalence_partition",
                    "before": before_groups,
                    "after": after_groups,
                }
            )
            category_counts["equivalence_partition"] += 1
        before_unpaired = sorted(str(value) for value in source.get("unpaired_witness_ids") or [])
        after_unpaired = sorted(str(value) for value in canonical.get("unpaired_witness_ids") or [])
        if before_unpaired != after_unpaired:
            changes.append(
                {
                    "category": "unpaired_witness_ids",
                    "before": before_unpaired,
                    "after": after_unpaired,
                }
            )
            category_counts["unpaired_witness_ids"] += 1
        if changes:
            cases.append(
                {
                    "case_id": str(case_id),
                    "base_case_id": str(canonical.get("base_case_id")),
                    "change_count": len(changes),
                    "changes": changes,
                }
            )
    return {
        "source_case_count": len(source_cases),
        "affected_case_count": len(cases),
        "unchanged_case_count": len(source_cases) - len(cases),
        "category_counts": dict(sorted(category_counts.items())),
        "cases": cases,
        "privacy": "opaque_ids_and_frozen_enums_only_no_source_or_witness_text",
    }


def _pool_subset(pool: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    cases = [deepcopy(case) for case in pool.get("cases") or [] if case.get("case_id") in wanted]
    if {str(case.get("case_id")) for case in cases} != wanted:
        raise JudgeV5CalibrationV50ReferenceError("canonical pool subset is incomplete")
    return {
        **{key: deepcopy(value) for key, value in pool.items() if key != "cases"},
        "cases": cases,
    }


def _validate_predecessors(
    *, v45_root: Path, v46_root: Path, v47_root: Path, v49_root: Path
) -> dict[str, Any]:
    paths = {
        "v45_terminal": v45_root / "terminal.json",
        "v45_receipt": v45_root / "reference-freeze-receipt.json",
        "v45_truth": v45_root / "frozen-diagnostic-truth.private.json",
        "v46_terminal": v46_root / "terminal.json",
        "v46_pool": v46_root / "shared-witness-pool.private.json",
        "v47_terminal": v47_root / "terminal.json",
        "v47_receipt": v47_root / "reference-freeze-receipt.json",
        "v47_truth": v47_root / "frozen-diagnostic-truth.private.json",
        "v49_terminal": v49_root / "terminal.json",
        "v49_spec": v49_root / "bipartite-diagnostic-spec.json",
        "v49_score": v49_root / "diagnostic-score.json",
        "v49_pool": v49_root / "shared-witness-pool.private.json",
        "v49_pointwise": v49_root / "pointwise-output-full.private.json",
        "v49_alignment": v49_root / "reconciled-alignment.private.json",
        "v49_disagreements": v49_root / "observable-disagreements.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    if (
        values["v45_terminal"].get("state") != "completed"
        or values["v45_terminal"].get("production_mutated") is not False
        or not _record_matches(values["v45_terminal"].get("frozen_truth"), paths["v45_truth"])
        or not _record_matches(values["v45_terminal"].get("reference_freeze_receipt"), paths["v45_receipt"])
    ):
        raise JudgeV5CalibrationV50ReferenceError("v45 predecessor is not admissible")
    if (
        values["v47_terminal"].get("state") != "completed"
        or values["v47_terminal"].get("production_mutated") is not False
        or not _record_matches(values["v47_terminal"].get("frozen_truth"), paths["v47_truth"])
        or not _record_matches(values["v47_terminal"].get("reference_receipt"), paths["v47_receipt"])
    ):
        raise JudgeV5CalibrationV50ReferenceError("v47 predecessor is not admissible")
    v49 = values["v49_terminal"]
    if (
        v49.get("state") != "inactive"
        or v49.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v49.get("accounting_complete") is not True
        or v49.get("usage_status") != "complete"
        or v49.get("production_mutated") is not False
        or v49.get("semantic_retry_count") != 0
        or not _record_matches(v49.get("score"), paths["v49_score"])
        or not _record_matches(v49.get("reconciled_alignment"), paths["v49_alignment"])
        or not _record_matches(v49.get("observable_disagreements"), paths["v49_disagreements"])
    ):
        raise JudgeV5CalibrationV50ReferenceError("v49 predecessor is not admissible")
    return {name: _record(path) for name, path in paths.items()}


def freeze_v50_canonical_reference(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v45_root: Path = DEFAULT_V45_ROOT,
    v46_root: Path = DEFAULT_V46_ROOT,
    v47_root: Path = DEFAULT_V47_ROOT,
    v49_root: Path = DEFAULT_V49_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v50 terminal")
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(
        v45_root=v45_root, v46_root=v46_root, v47_root=v47_root, v49_root=v49_root
    )

    pool, mapping, canonical_truth = make_v5_calibration_pool()
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=canonical_truth)
    v46_pool = _load_json(v46_root / "shared-witness-pool.private.json", "v46 pool")
    v49_pool = _load_json(v49_root / "shared-witness-pool.private.json", "v49 pool")
    v46_ids = [str(case["case_id"]) for case in v46_pool.get("cases") or []]
    v49_ids = [str(case["case_id"]) for case in v49_pool.get("cases") or []]
    if v46_pool != _pool_subset(pool, v46_ids):
        raise JudgeV5CalibrationV50ReferenceError("canonical pool does not reproduce v46")
    if v49_pool != _pool_subset(pool, v49_ids):
        raise JudgeV5CalibrationV50ReferenceError("canonical pool does not reproduce v49")

    v45_truth = _load_json(v45_root / "frozen-diagnostic-truth.private.json", "v45 truth")
    v47_truth = _load_json(v47_root / "frozen-diagnostic-truth.private.json", "v47 truth")
    v45_drift = audit_reference_drift(v45_truth, canonical_truth)
    v47_drift = audit_reference_drift(v47_truth, canonical_truth)
    if v45_drift["affected_case_count"] == 0 or v47_drift["affected_case_count"] == 0:
        raise JudgeV5CalibrationV50ReferenceError("expected predecessor reference drift vanished")

    fixture_audit_path = DEFAULT_FIXTURE_AUDIT_PATH.resolve()
    fixture_audit = _load_json(fixture_audit_path, "fixture truth audit")
    source_relative = str((fixture_audit.get("source_fixture") or {}).get("path") or "")
    source_fixture_path = (fixture_audit_path.parents[2] / source_relative).resolve()
    if (
        not source_fixture_path.is_file()
        or (fixture_audit.get("source_fixture") or {}).get("sha256")
        != _sha256_file(source_fixture_path)
    ):
        raise JudgeV5CalibrationV50ReferenceError("canonical source fixture drifted")

    pool_path = root / "canonical-shared-witness-pool.private.json"
    mapping_path = root / "canonical-witness-mapping.private.json"
    truth_path = root / "canonical-calibration-truth.private.json"
    audit_path = root / "reference-restoration-audit.json"
    rescore_path = root / "v49-canonical-rescore.json"
    _write_immutable(pool_path, pool)
    _write_immutable(mapping_path, mapping)
    _write_immutable(truth_path, canonical_truth)

    audit = {
        "schema_version": V50_AUDIT_VERSION,
        "created_at": now_iso(),
        "restoration_basis": (
            "regeneration_from_fixture_and_truth_audit_frozen_before_pipeline_v5_semantic_calls"
        ),
        "canonical_source_fixture": _record(source_fixture_path),
        "canonical_fixture_truth_audit": _record(fixture_audit_path),
        "canonical_pool_reproduces_v46_exactly": True,
        "canonical_pool_reproduces_v49_exactly": True,
        "v45_reference_drift": v45_drift,
        "v47_reference_drift": v47_drift,
        "semantic_model_calls_performed": 0,
        "production_mutated": False,
        "privacy": "opaque_ids_and_frozen_enums_only_no_source_or_witness_text",
    }
    _write_immutable(audit_path, audit)

    expected_subset = _subset_truth(canonical_truth, TARGET_CASE_IDS)
    v49_rescore = _score_v48(
        pointwise_output=_load_json(v49_root / "pointwise-output-full.private.json", "v49 pointwise"),
        reconciled_alignment=_load_json(v49_root / "reconciled-alignment.private.json", "v49 alignment"),
        expected=expected_subset,
        observable_disagreements=_load_json(
            v49_root / "observable-disagreements.private.json", "v49 disagreements"
        ),
    )
    v49_rescore["schema_version"] = V50_RESCORE_VERSION
    v49_rescore["created_at"] = now_iso()
    v49_rescore["purpose"] = "forensic_only_not_promotion_evidence"
    v49_rescore["promotion_authorized"] = False
    v49_rescore["reason"] = (
        "v49_protocol_was_developed_against_the_superseded_v45_v47_reference_lineage"
    )
    v49_rescore["canonical_truth"] = _record(truth_path)
    v49_rescore["source_v49_terminal"] = predecessors["v49_terminal"]
    _write_immutable(rescore_path, v49_rescore)

    receipt = {
        "schema_version": V50_RECEIPT_VERSION,
        "created_at": now_iso(),
        "reference_version": "v50_canonical_restoration",
        "reference_frozen": True,
        "canonical_pool": _record(pool_path),
        "canonical_mapping": _record(mapping_path),
        "canonical_truth": _record(truth_path),
        "restoration_audit": _record(audit_path),
        "v49_forensic_rescore": _record(rescore_path),
        "predecessors": predecessors,
        "v45_reference_superseded": True,
        "v47_reference_superseded": True,
        "fresh_diagnostic_required": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt_path = root / "reference-restoration-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V50_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v50_canonical_reference_restoration_completed",
        "development_terminal_reason": (
            "v50_reference_restored_fresh_diagnostic_required"
        ),
        "overall_evaluation_complete": False,
        "inactive_incomplete_recovery_required": True,
        "reference_frozen": True,
        "fresh_diagnostic_required": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "not_applicable_no_semantic_turn_started",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "reference_receipt": _record(receipt_path),
        "canonical_truth": _record(truth_path),
        "restoration_audit": _record(audit_path),
        "v49_forensic_rescore": _record(rescore_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Restore the canonical v5 judge reference")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v45-root", default=str(DEFAULT_V45_ROOT))
    parser.add_argument("--v46-root", default=str(DEFAULT_V46_ROOT))
    parser.add_argument("--v47-root", default=str(DEFAULT_V47_ROOT))
    parser.add_argument("--v49-root", default=str(DEFAULT_V49_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v50_canonical_reference(
        output_dir=Path(args.output_dir),
        v45_root=Path(args.v45_root),
        v46_root=Path(args.v46_root),
        v47_root=Path(args.v47_root),
        v49_root=Path(args.v49_root),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "fresh_diagnostic_required": terminal["fresh_diagnostic_required"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
