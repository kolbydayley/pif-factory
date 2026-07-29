"""Auditable Task-4d option-2 measurement contract and checkpoint.

This module changes measurement semantics, never extraction data.  It records
the approved contract in the suite manifest, re-scores the frozen Phase-C
composition against intrinsic junk, and pairs that result with the blocking
relational-merge contamination measurement.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .true_north import (
    DEFAULT_PRIVATE_ROOT,
    SUITE_ID,
    TrueNorthError,
    _read_json,
    _score_phase_c_dispositions,
    _source_unchanged,
    _write_json,
    apply_phase_c_intrinsic_composition_rules,
    dumps_json,
    sha256_text,
    verify_suite,
)
from .true_north_relational_merge import verify_run


CONTRACT_VERSION = "pif_true_north_phase_c_option2_v2"
SUPERSEDED_CONTRACT_VERSION = "pif_true_north_phase_c_option2_v1"
SUPERSEDED_CONTRACT_SHA256 = (
    "dedab8c1cb837229da2d2b38730ee37e76e4eaa7e1b9bb6a4ba4681116ef754f"
)
DEFAULT_PHASE_C_RUN_ID = "phase-c-junk-verify-20260728-v1"
DEFAULT_CANONICAL_RUN_ID = "tnrun_62691fcee1b451600460cf53"


def _suite_root(
    output_root: str | Path | None,
    suite: str,
) -> Path:
    if suite != SUITE_ID:
        raise TrueNorthError(f"unknown true-north suite: {suite}")
    base = (
        Path(output_root).expanduser().resolve()
        if output_root
        else DEFAULT_PRIVATE_ROOT
    )
    return base / suite


def measurement_contract() -> dict[str, Any]:
    """Return the approved, deterministic option-2 contract."""

    body: dict[str, Any] = {
        "version": CONTRACT_VERSION,
        "approved_at": "2026-07-28",
        "supersedes_contract_sha256": SUPERSEDED_CONTRACT_SHA256,
        "decision": "task_4d_option_2_move_relational_junk_downstream",
        "task_4c_status": (
            "retired_no_fourth_screen_design"
        ),
        "old_gate": {
            "name": "phase_c_all_junk_zero_escape",
            "scope": "all_gold_junk",
            "acceptance": {
                "junk_escapes": "0/9",
                "false_rejects": "<=10",
                "retained_value_recall": ">=0.95",
            },
        },
        "new_gate": {
            "name": "phase_c_intrinsic_junk_zero_escape",
            "scope": [
                "chrome",
                "bare_mention",
                "fragment",
            ],
            "excluded_relational_reason_families": [
                "non_useful_repetition*",
                "nonasserted_question_frame",
            ],
            "acceptance": {
                "intrinsic_junk_escapes": 0,
                "false_rejects": "<=10",
                "retained_value_recall": ">=0.95",
            },
        },
        "blocking_certification": {
            "verifier": (
                "research_factory.true_north_relational_merge."
                "verify_relational_merges"
            ),
            "required_ledger_category": (
                "merged_duplicate_retained"
            ),
            "required_canonical_state": (
                "identifying canonical group with a corroborating "
                "non-junk peer"
            ),
            "required_contamination_count": 0,
        },
        "held_item_accounting": {
            "rule": (
                "A held_needs_review candidate with zero atomic claims "
                "did not enter the claim corpus."
            ),
            "junk_effect": (
                "exclude from intrinsic and relational escape numerators"
            ),
            "value_effect": (
                "exclude from the retained-value recall numerator while "
                "keeping the gold-value denominator unchanged"
            ),
        },
        "deterministic_intrinsic_rule": {
            "version": "bracket_link_chrome_only_v1",
            "when": (
                "bracketed link or chrome text is the candidate's only "
                "substantive object and no predicate is asserted about it"
            ),
            "composition_disposition": "reject",
            "model_calls": 0,
        },
        "evidence_trail": {
            "task_4b": {
                "run_id": DEFAULT_PHASE_C_RUN_ID,
                "result_sha256": (
                    "4b4d46878652394b327315469f6ef3818d236daa83a4f013cdb10fd339d8915d"
                ),
                "result": {
                    "junk_escapes": 3,
                    "false_rejects": 8,
                    "retained_value_recall": 0.966527,
                },
            },
            "task_4c": {
                "screen_sha256": (
                    "c263ec9e51fb3f0ed81ac8bdcdfb305b52747bd6592e0dfeb0f8b7b6f929db27"
                ),
                "screened_candidates": 222,
                "retained_candidates": 243,
                "first_validated_call_tokens": 47_323,
                "projected_total_tokens": 283_519,
                "token_ceiling": 150_000,
                "outcome": "mechanical_budget_preflight_failure",
            },
            "offline_parameter_sweep": {
                "source": "operator_review_thread_2026-07-28",
                "parameterizations": 36,
                "fixture_complete_selection_range": [
                    946,
                    1001,
                ],
                "retained_pool": 1056,
                "conclusion": (
                    "no selective operating point; relational junk "
                    "is not surface-detectable"
                ),
            },
        },
    }
    body["contract_sha256"] = sha256_text(dumps_json(body))
    return body


def apply_manifest_contract(
    *,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    """Version and apply the approved contract to the suite manifest."""

    suite_root = _suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = _read_json(manifest_path)
    existing = manifest.get("measurement_contract")
    contract = measurement_contract()
    if existing is not None and existing.get("version") == CONTRACT_VERSION:
        if existing != contract:
            raise TrueNorthError(
                "suite manifest carries a different measurement contract"
            )
        verification = verify_suite(
            output_root=output_root, suite=suite
        )
        return {
            "changed": False,
            "manifest_sha256": manifest["manifest_sha256"],
            "contract_sha256": contract["contract_sha256"],
            "verification": verification,
        }
    if existing is not None and existing.get(
        "version"
    ) != SUPERSEDED_CONTRACT_VERSION:
        raise TrueNorthError(
            "suite manifest carries an unsupported measurement contract"
        )
    old_manifest = dict(manifest)
    old_sha = str(old_manifest["manifest_sha256"])
    history_path = (
        suite_root / "manifest-history" / f"manifest-{old_sha}.json"
    )
    _write_json(history_path, old_manifest, immutable=True)
    updated = dict(manifest)
    updated["measurement_contract"] = contract
    updated.pop("manifest_sha256", None)
    updated["manifest_sha256"] = sha256_text(dumps_json(updated))
    shadow_path = Path(updated["shadow_database"])
    try:
        _write_json(manifest_path, updated, immutable=False)
        conn = sqlite3.connect(shadow_path)
        try:
            conn.execute(
                """
                UPDATE true_north_suites
                SET manifest_sha256 = ?
                WHERE suite_id = ?
                """,
                (updated["manifest_sha256"], suite),
            )
            conn.commit()
        finally:
            conn.close()
        verification = verify_suite(
            output_root=output_root, suite=suite
        )
        if not verification["ok"]:
            raise TrueNorthError(
                "suite verification failed after contract amendment: "
                + ", ".join(verification["errors"])
            )
        if not _source_unchanged(updated):
            raise TrueNorthError(
                "production source changed during contract amendment"
            )
    except Exception:
        _write_json(manifest_path, old_manifest, immutable=False)
        conn = sqlite3.connect(shadow_path)
        try:
            conn.execute(
                """
                UPDATE true_north_suites
                SET manifest_sha256 = ?
                WHERE suite_id = ?
                """,
                (old_sha, suite),
            )
            conn.commit()
        finally:
            conn.close()
        raise
    return {
        "changed": True,
        "old_manifest_sha256": old_sha,
        "manifest_sha256": updated["manifest_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "history_path": str(history_path),
        "production_source_unchanged": True,
        "holdout_opened": False,
        "verification": verification,
    }


def evaluate_checkpoint(
    *,
    phase_c_run_id: str = DEFAULT_PHASE_C_RUN_ID,
    canonical_run_id: str = DEFAULT_CANONICAL_RUN_ID,
    output_root: str | Path | None = None,
    suite: str = SUITE_ID,
) -> dict[str, Any]:
    """Report the intrinsic gate and contamination number together."""

    suite_root = _suite_root(output_root, suite)
    manifest = _read_json(suite_root / "manifest.json")
    contract = manifest.get("measurement_contract")
    if not isinstance(contract, Mapping) or contract.get(
        "version"
    ) != CONTRACT_VERSION:
        raise TrueNorthError(
            "option-2 measurement contract is not bound in the manifest"
        )
    phase_root = suite_root / "phase-c" / "runs" / phase_c_run_id
    combined = _read_json(
        phase_root / "outputs" / "combined.private.json"
    )
    raw_predictions = {
        str(row["candidate_id"]): row
        for row in combined["items"]
    }
    candidate_by_id: dict[str, dict[str, Any]] = {}
    for bundle_record in manifest["bundles"]:
        bundle = _read_json(Path(bundle_record["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in raw_predictions:
                candidate_by_id[candidate_id] = dict(candidate)
    intrinsic_rules = apply_phase_c_intrinsic_composition_rules(
        raw_predictions, candidate_by_id
    )
    predictions = intrinsic_rules["predictions"]
    gold_root = suite_root / "gold" / "development" / "final"
    baseline_disposition = _score_phase_c_dispositions(
        _read_json(gold_root / "consensus.private.json"),
        raw_predictions,
        _read_json(gold_root / "gold.private.json"),
    )
    initial_disposition = _score_phase_c_dispositions(
        _read_json(gold_root / "consensus.private.json"),
        predictions,
        _read_json(gold_root / "gold.private.json"),
    )
    merge_report, merge_inputs = verify_run(
        run_id=canonical_run_id,
        output_root=output_root,
        suite=suite,
        partition="development",
        escape_candidate_ids=(
            initial_disposition["all_junk_escape_candidate_ids"]
        ),
    )
    disposition = _score_phase_c_dispositions(
        _read_json(gold_root / "consensus.private.json"),
        predictions,
        _read_json(gold_root / "gold.private.json"),
        held_candidate_ids=merge_inputs.diagnostics[
            "held_candidate_ids"
        ],
    )
    derived_ledger_assignments = {
        entry.candidate_id: "merged_duplicate_retained"
        for entry in merge_report.escapes
        if entry.merged
    }
    contamination_ids = sorted(merge_report.unmerged)
    contamination_count = len(contamination_ids)
    checkpoint = {
        "schema_version": CONTRACT_VERSION,
        "suite_id": suite,
        "manifest_sha256": manifest["manifest_sha256"],
        "measurement_contract_sha256": contract[
            "contract_sha256"
        ],
        "phase_c_run_id": phase_c_run_id,
        "canonical_run_id": canonical_run_id,
        "disposition_gate": {
            "passed": disposition["passed"],
            "intrinsic_junk_escape_count": disposition[
                "intrinsic_junk_escape_count"
            ],
            "intrinsic_junk_escape_candidate_ids": disposition[
                "intrinsic_junk_escape_candidate_ids"
            ],
            "relational_junk_escape_count": disposition[
                "relational_junk_escape_count"
            ],
            "false_reject_count": disposition[
                "false_reject_count"
            ],
            "retained_value_recall": disposition[
                "retained_value_recall"
            ],
            "held_candidate_count": disposition[
                "held_candidate_count"
            ],
            "held_gold_value_count": disposition[
                "held_gold_value_count"
            ],
            "held_gold_junk_count": disposition[
                "held_gold_junk_count"
            ],
            "deterministic_rule_version": intrinsic_rules[
                "rule_version"
            ],
            "deterministic_rule_flipped_candidate_ids": (
                intrinsic_rules["flipped_candidate_ids"]
            ),
            "deterministic_rule_model_calls": intrinsic_rules[
                "model_calls_made"
            ],
            "deterministic_rule_false_reject_delta": (
                initial_disposition["false_reject_count"]
                - baseline_disposition["false_reject_count"]
            ),
            "pre_held_false_reject_count": (
                initial_disposition["false_reject_count"]
            ),
            "pre_held_retained_value_recall": (
                initial_disposition["retained_value_recall"]
            ),
            "held_false_reject_delta": (
                disposition["false_reject_count"]
                - initial_disposition["false_reject_count"]
            ),
            "held_retained_value_recall_delta": (
                disposition["retained_value_recall"]
                - initial_disposition["retained_value_recall"]
            ),
        },
        "relational_merge_certification": {
            "passed": contamination_count == 0,
            "relational_escape_count": (
                merge_report.relational_escapes
            ),
            "canonical_merge_count": merge_report.merged_count,
            "derived_merged_duplicate_retained_count": len(
                derived_ledger_assignments
            ),
            "source_merged_duplicate_retained_count": (
                merge_inputs.diagnostics[
                    "merged_duplicate_retained_ledger_rows"
                ]
            ),
            "derived_ledger_assignments": (
                derived_ledger_assignments
            ),
            "unmerged_candidate_ids": list(
                merge_report.unmerged
            ),
            "contamination_candidate_ids": contamination_ids,
            "contamination_count": contamination_count,
            "contamination_zero": contamination_count == 0,
            "held_relational_candidate_ids": list(
                merge_inputs.diagnostics[
                    "held_relational_candidates"
                ]
            ),
            "merge_report": merge_report.to_dict(),
        },
        "passed": (
            disposition["passed"] and contamination_count == 0
        ),
        "task_5_authorized": (
            disposition["passed"] and contamination_count == 0
        ),
        "holdout_opened": False,
        "production_mutation": False,
    }
    checkpoint["checkpoint_sha256"] = sha256_text(
        dumps_json(checkpoint)
    )
    output_path = (
        suite_root
        / "certification"
        / "phase-c-option2-development-v2-accounting.json"
    )
    _write_json(output_path, checkpoint, immutable=True)
    return {**checkpoint, "output_path": str(output_path)}
