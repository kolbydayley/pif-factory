"""Final owner-delegated True-North measurement contract v7."""

from __future__ import annotations

import copy
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from . import true_north
from .true_north_semantic_scoring import SCHEMA_VERSION as SCORER_VERSION


CONTRACT_VERSION = "pif_true_north_measurement_contract_v7"
SUPERSEDED_CONTRACT_VERSION = "pif_true_north_measurement_contract_v6"
OWNER_DECISION_ID = "owner-delegated-final-ruling-20260729-v1"
GATE_POLICY_FILENAME = "gate-policy-v4.json"
HALLUCINATION_CALIBRATION_RELATIVE = (
    "calibration/hallucination-gold-vs-gold-v1.json"
)
HALLUCINATION_MARGIN = 0.02
ORIGINAL_HALLUCINATION_ASPIRATION = 0.02


class FinalContractError(RuntimeError):
    """Raised when final ruling provenance or calibration drifts."""


def _hallucination_calibration(suite_root: Path) -> dict[str, Any]:
    path = suite_root / HALLUCINATION_CALIBRATION_RELATIVE
    document = true_north._read_json(path)
    body = copy.deepcopy(document)
    digest = str(body.pop("result_sha256", ""))
    if digest != true_north.sha256_text(true_north.dumps_json(body)):
        raise FinalContractError(
            "hallucination calibration hash does not verify"
        )
    matched_rate = float(
        document["matched_pair_only_diagnostic"]["rate"]
    )
    threshold = round(matched_rate + HALLUCINATION_MARGIN, 6)
    if (
        document["provider_calls"] != 0
        or document["holdout_opened"] is not False
        or threshold != 0.093684
    ):
        raise FinalContractError(
            "hallucination calibration differs from final ruling"
        )
    return {
        "path": str(path),
        "sha256": digest,
        "matched_pair_gold_vs_gold_rate": matched_rate,
        "margin": HALLUCINATION_MARGIN,
        "threshold": threshold,
        "live_coupled_gold_vs_gold_rate": float(
            document["live_proxy"]["rate"]
        ),
        "original_aspirational_target": (
            ORIGINAL_HALLUCINATION_ASPIRATION
        ),
    }


def gate_policy_document(
    *,
    calibration: Mapping[str, Any],
    previous_policy_sha256: str,
) -> dict[str, Any]:
    expected = float(
        true_north.APPROVED_GATE_POLICY[
            "hallucination_rate_proxy"
        ][1]
    )
    if float(calibration["threshold"]) != expected:
        raise FinalContractError(
            "artifact-derived hallucination threshold differs from policy"
        )
    body: dict[str, Any] = {
        "schema_version": true_north.APPROVED_GATE_POLICY_VERSION,
        "suite_id": true_north.SUITE_ID,
        "owner_decision_id": OWNER_DECISION_ID,
        "approved_at": "2026-07-29",
        "scoring_contract": SCORER_VERSION,
        "hallucination_gate": {
            "denominator": "matched_atomic_pairs_only",
            "calibration_rule": (
                "matched_pair_gold_vs_gold_rate_plus_0.02"
            ),
            "matched_pair_gold_vs_gold_rate": calibration[
                "matched_pair_gold_vs_gold_rate"
            ],
            "margin": calibration["margin"],
            "threshold": calibration["threshold"],
            "live_coupled_reading": "mandatory_diagnostic",
            "original_0_02_aspiration": "mandatory_diagnostic",
            "calibration_sha256": calibration["sha256"],
        },
        "atomicity_gate": {
            "threshold": 0.90,
            "re_referenced": False,
        },
        "supersedes_gate_policy_sha256": previous_policy_sha256,
        "rules": {
            metric: {
                "comparison": comparison,
                "threshold": threshold,
            }
            for metric, (
                comparison,
                threshold,
            ) in true_north.APPROVED_GATE_POLICY.items()
        },
    }
    body["gate_policy_sha256"] = true_north.sha256_text(
        true_north.dumps_json(body)
    )
    return body


def measurement_contract(
    *,
    previous_contract: Mapping[str, Any],
    calibration: Mapping[str, Any],
    gate_policy: Mapping[str, Any],
) -> dict[str, Any]:
    if previous_contract.get("version") != SUPERSEDED_CONTRACT_VERSION:
        raise FinalContractError(
            "final ruling requires measurement contract v6"
        )
    body: dict[str, Any] = {
        "version": CONTRACT_VERSION,
        "approved_at": "2026-07-29",
        "owner_decision_id": OWNER_DECISION_ID,
        "delegation": {
            "delegated_by": "Kolby Dayley",
            "delegated_to": "true_north_review_loop",
            "decision": "RULING_3_final",
            "explicit": True,
        },
        "provenance": {
            "source_kind": (
                "review_thread_directive_in_active_codex_thread"
            ),
            "directive_received_at": "2026-07-29",
            "directive_summary": (
                "Kolby delegated the final ruling to the review loop. "
                "Ruling 3 re-referenced hallucination to the measured "
                "matched-pair gold-vs-gold rate plus 0.02, retained the "
                "0.90 atomic gate, and certified a development-fold-only "
                "hybrid architecture with disclosed limitations."
            ),
        },
        "supersedes_contract_sha256": previous_contract[
            "contract_sha256"
        ],
        "previous_contract": copy.deepcopy(previous_contract),
        "hallucination": {
            **dict(calibration),
            "gate_denominator": "matched_atomic_pairs_only",
            "live_coupled_reading": "mandatory_diagnostic",
            "original_aspirational_target": (
                ORIGINAL_HALLUCINATION_ASPIRATION
            ),
            "gate_changed_by_owner_delegated_ruling": True,
        },
        "atomicity": {
            "gate": 0.90,
            "re_referenced": False,
            "process_property_finding": (
                "gold-level decomposition is a property of the "
                "independent A/B plus adjudication process, not any "
                "available measured single-pass model"
            ),
            "shortfall_treatment": (
                "certified_disclosed_limitation_not_recalibration"
            ),
        },
        "certification": {
            "scope": "development_fold_only",
            "architecture": "hybrid",
            "disposition": (
                "glm_5_2_with_certified_ensemble_composition"
            ),
            "actor": "deterministic_actor_span_rule_v1",
            "speaker": "prior_adoption",
            "compound_decomposition": "codex_class_lane",
            "sealed_transfer_episodes": "remain_closed",
            "decomposition_experiment_lane": "stopped",
            "future_consensus_recipe": (
                "identified_not_authorized_glm_plus_sol_independent_"
                "passes_plus_cheap_adjudicator"
            ),
        },
        "gate_policy": {
            "version": true_north.APPROVED_GATE_POLICY_VERSION,
            "sha256": gate_policy["gate_policy_sha256"],
            "supersedes_gate_policy_sha256": gate_policy[
                "supersedes_gate_policy_sha256"
            ],
        },
        "holdout_opened": False,
    }
    body["contract_sha256"] = true_north.sha256_text(
        true_north.dumps_json(body)
    )
    return body


def apply_manifest_contract(
    *,
    output_root: str | Path | None = None,
    suite: str = true_north.SUITE_ID,
) -> dict[str, Any]:
    """Version the manifest to v7 with transactional rollback."""

    suite_root = true_north._suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = true_north._read_json(manifest_path)
    existing = manifest.get("measurement_contract", {})
    calibration = _hallucination_calibration(suite_root)
    existing_policy = existing.get("gate_policy", {})
    previous_policy_sha = (
        existing_policy.get("supersedes_gate_policy_sha256")
        if existing.get("version") == CONTRACT_VERSION
        else existing_policy.get("sha256")
    )
    gate_policy = gate_policy_document(
        calibration=calibration,
        previous_policy_sha256=str(
            previous_policy_sha
            or manifest["frozen_interfaces"]["gate_policy_sha256"]
        ),
    )
    if existing.get("version") == CONTRACT_VERSION:
        reconstructed = measurement_contract(
            previous_contract=existing["previous_contract"],
            calibration=calibration,
            gate_policy=gate_policy,
        )
        if reconstructed != existing:
            raise FinalContractError(
                "current v7 manifest differs from reconstruction"
            )
        verification = true_north.verify_suite(
            output_root=output_root, suite=suite
        )
        return {
            "changed": False,
            "manifest_sha256": manifest["manifest_sha256"],
            "contract_sha256": existing["contract_sha256"],
            "verification": verification,
        }
    if existing.get("version") != SUPERSEDED_CONTRACT_VERSION:
        raise FinalContractError(
            "manifest does not carry superseded contract v6"
        )
    contract = measurement_contract(
        previous_contract=existing,
        calibration=calibration,
        gate_policy=gate_policy,
    )
    old_manifest = copy.deepcopy(manifest)
    old_sha = str(old_manifest["manifest_sha256"])
    history_path = (
        suite_root
        / "manifest-history"
        / f"manifest-{old_sha}.json"
    )
    policy_path = (
        suite_root / "diagnostics" / GATE_POLICY_FILENAME
    )
    true_north._write_json(
        history_path, old_manifest, immutable=True
    )
    true_north._write_json(
        policy_path, gate_policy, immutable=True
    )
    updated = copy.deepcopy(manifest)
    updated["measurement_contract"] = contract
    updated["frozen_interfaces"][
        "gate_policy_version"
    ] = true_north.APPROVED_GATE_POLICY_VERSION
    updated["frozen_interfaces"][
        "gate_policy_sha256"
    ] = gate_policy["gate_policy_sha256"]
    updated.pop("manifest_sha256", None)
    updated["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(updated)
    )
    shadow_path = Path(updated["shadow_database"])
    try:
        true_north._write_json(
            manifest_path, updated, immutable=False
        )
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
        verification = true_north.verify_suite(
            output_root=output_root, suite=suite
        )
        if not verification["ok"]:
            raise FinalContractError(
                "suite verification failed after v7 migration: "
                + ", ".join(verification["errors"])
            )
        if not true_north._source_unchanged(updated):
            raise FinalContractError(
                "production source changed during v7 migration"
            )
    except Exception:
        true_north._write_json(
            manifest_path, old_manifest, immutable=False
        )
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
        "gate_policy_path": str(policy_path),
        "hallucination_threshold": calibration["threshold"],
        "production_source_unchanged": True,
        "holdout_opened": False,
    }
