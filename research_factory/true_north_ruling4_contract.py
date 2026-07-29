"""Owner-delegated Ruling 4 measurement contract v8."""

from __future__ import annotations

import copy
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from . import true_north


CONTRACT_VERSION = "pif_true_north_measurement_contract_v8"
SUPERSEDED_CONTRACT_VERSION = "pif_true_north_measurement_contract_v7"
OWNER_DECISION_ID = "owner-delegated-ruling-4-20260729-v1"
GATE_POLICY_FILENAME = "gate-policy-v5.json"
CALIBRATION_RELATIVE = (
    "diagnostics/candidate-state-macro-f1-calibration-v1.json"
)
MEASURED_CEILING = 0.830664
MARGIN = 0.04
THRESHOLD = 0.790664
ORIGINAL_ASPIRATION = 0.90


class Ruling4ContractError(RuntimeError):
    """Raised when Ruling 4 provenance or calibration drifts."""


def _calibration(suite_root: Path) -> dict[str, Any]:
    path = suite_root / CALIBRATION_RELATIVE
    document = true_north._read_json(path)
    if (
        document.get("schema_version")
        != "pif_true_north_candidate_state_calibration_v1"
        or int(document.get("provider_calls", -1)) != 0
        or document.get("sealed_holdout_opened") is not False
        or float(document.get("measured_ceiling", -1)) != MEASURED_CEILING
        or float(
            document.get("calibration_rule", {}).get(
                "recommended_threshold", -1
            )
        )
        != THRESHOLD
    ):
        raise Ruling4ContractError(
            "candidate-state calibration differs from Ruling 4"
        )
    return {
        "path": str(path),
        "sha256": true_north._sha256_file(path),
        "calibration_sha256": document["calibration_sha256"],
        "measured_ceiling": MEASURED_CEILING,
        "margin": MARGIN,
        "threshold": THRESHOLD,
        "raw_disposition_exact_agreement": document["diagnostics"][
            "raw_disposition_exact_agreement"
        ],
        "value_state_exact_agreement": document["diagnostics"][
            "value_state_exact_agreement"
        ],
        "original_aspiration": ORIGINAL_ASPIRATION,
    }


def gate_policy_document(
    *,
    calibration: Mapping[str, Any],
    previous_policy_sha256: str,
) -> dict[str, Any]:
    if (
        true_north.APPROVED_GATE_POLICY[
            "consensus_candidate_state_macro_f1"
        ]
        != (">=", THRESHOLD)
    ):
        raise Ruling4ContractError(
            "code gate differs from Ruling 4 threshold"
        )
    body: dict[str, Any] = {
        "schema_version": true_north.APPROVED_GATE_POLICY_VERSION,
        "suite_id": true_north.SUITE_ID,
        "owner_decision_id": OWNER_DECISION_ID,
        "approved_at": "2026-07-29",
        "candidate_state_macro_f1_gate": {
            "calibration_rule": (
                "min(original_target, measured_gold_vs_gold_ceiling_minus_0.04)"
            ),
            **dict(calibration),
            "coupled_raw_agreement": "mandatory_diagnostic",
            "original_0_90_aspiration": "mandatory_diagnostic",
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
        raise Ruling4ContractError(
            "Ruling 4 requires measurement contract v7"
        )
    body: dict[str, Any] = {
        "version": CONTRACT_VERSION,
        "approved_at": "2026-07-29",
        "owner_decision_id": OWNER_DECISION_ID,
        "delegation": {
            "delegated_by": "Kolby Dayley",
            "delegated_to": "true_north_review_loop",
            "decision": "RULING_4",
            "explicit": True,
        },
        "provenance": {
            "source_kind": (
                "review_thread_directive_in_active_codex_thread"
            ),
            "directive_received_at": "2026-07-29",
            "directive_summary": (
                "Ruling 4 re-referenced candidate-state macro F1 to "
                "min(0.90, 0.830664 - 0.04) = 0.790664 and retained "
                "the 0.90 aspiration and raw agreement as diagnostics."
            ),
        },
        "supersedes_contract_sha256": previous_contract[
            "contract_sha256"
        ],
        "previous_contract": copy.deepcopy(previous_contract),
        "candidate_state_macro_f1": {
            **dict(calibration),
            "gate": THRESHOLD,
            "comparison": ">=",
            "original_aspiration": ORIGINAL_ASPIRATION,
            "raw_agreement_diagnostic_required": True,
        },
        "gate_policy": {
            "version": true_north.APPROVED_GATE_POLICY_VERSION,
            "sha256": gate_policy["gate_policy_sha256"],
            "supersedes_gate_policy_sha256": gate_policy[
                "supersedes_gate_policy_sha256"
            ],
        },
        "certification": copy.deepcopy(
            previous_contract["certification"]
        ),
        "atomicity": copy.deepcopy(previous_contract["atomicity"]),
        "hallucination": copy.deepcopy(
            previous_contract["hallucination"]
        ),
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
    """Version the suite manifest to v8 with rollback."""

    suite_root = true_north._suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = true_north._read_json(manifest_path)
    existing = manifest.get("measurement_contract", {})
    calibration = _calibration(suite_root)
    previous_policy = existing.get("gate_policy", {})
    previous_policy_sha = (
        previous_policy.get("supersedes_gate_policy_sha256")
        if existing.get("version") == CONTRACT_VERSION
        else previous_policy.get("sha256")
    )
    policy = gate_policy_document(
        calibration=calibration,
        previous_policy_sha256=str(previous_policy_sha),
    )
    if existing.get("version") == CONTRACT_VERSION:
        reconstructed = measurement_contract(
            previous_contract=existing["previous_contract"],
            calibration=calibration,
            gate_policy=policy,
        )
        if reconstructed != existing:
            raise Ruling4ContractError(
                "current v8 manifest differs from reconstruction"
            )
        return {
            "changed": False,
            "manifest_sha256": manifest["manifest_sha256"],
            "contract_sha256": existing["contract_sha256"],
            "verification": true_north.verify_suite(
                output_root=output_root, suite=suite
            ),
        }
    if existing.get("version") != SUPERSEDED_CONTRACT_VERSION:
        raise Ruling4ContractError(
            "manifest does not carry superseded contract v7"
        )
    contract = measurement_contract(
        previous_contract=existing,
        calibration=calibration,
        gate_policy=policy,
    )
    old_manifest = copy.deepcopy(manifest)
    old_sha = str(old_manifest["manifest_sha256"])
    history_path = (
        suite_root / "manifest-history" / f"manifest-{old_sha}.json"
    )
    policy_path = suite_root / "diagnostics" / GATE_POLICY_FILENAME
    true_north._write_json(history_path, old_manifest, immutable=True)
    true_north._write_json(policy_path, policy, immutable=True)
    updated = copy.deepcopy(manifest)
    updated["measurement_contract"] = contract
    updated["frozen_interfaces"][
        "gate_policy_version"
    ] = true_north.APPROVED_GATE_POLICY_VERSION
    updated["frozen_interfaces"][
        "gate_policy_sha256"
    ] = policy["gate_policy_sha256"]
    updated.pop("manifest_sha256", None)
    updated["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(updated)
    )
    shadow_path = Path(updated["shadow_database"])
    try:
        true_north._write_json(manifest_path, updated, immutable=False)
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
            raise Ruling4ContractError(
                "suite verification failed after v8 migration: "
                + ", ".join(verification["errors"])
            )
        if not true_north._source_unchanged(updated):
            raise Ruling4ContractError(
                "production source changed during v8 migration"
            )
    except Exception:
        true_north._write_json(manifest_path, old_manifest, immutable=False)
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
        "candidate_state_threshold": THRESHOLD,
        "production_source_unchanged": True,
        "holdout_opened": False,
    }
