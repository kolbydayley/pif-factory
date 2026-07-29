"""Owner-approved matched-pair measurement contract for True North."""

from __future__ import annotations

import copy
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from . import true_north
from . import true_north_gate_calibration as calibration
from .true_north_semantic_scoring import SCHEMA_VERSION as SCORER_VERSION


CONTRACT_VERSION = "pif_true_north_measurement_contract_v6"
SUPERSEDED_CONTRACT_VERSION = "pif_true_north_phase_c_option2_v5"
OWNER_DECISION_ID = "owner-decouple-fields-and-retain-atomic-gate-20260729-v1"
CALIBRATION_FILENAME = "gate-calibration-v2-decoupled.json"
GATE_POLICY_FILENAME = "gate-policy-v3.json"
ATOMIC_RECONCILIATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "TRUE_NORTH_ATOMIC_CEILING_RECONCILIATION.md"
)


class DecoupledContractError(RuntimeError):
    """Raised when the owner ruling cannot be applied exactly."""


def _atomic_range_ceiling(suite_root: Path) -> dict[str, Any]:
    base = suite_root / "gold" / "development"
    pass_a, pass_b = true_north._load_independent_atomic_gold(base)
    final = true_north._read_json(
        base / "final" / "gold.private.json"
    )
    pass_c = {
        str(row["candidate_id"]): row for row in final["items"]
    }
    if set(pass_a) != set(pass_b) or set(pass_a) != set(pass_c):
        raise DecoupledContractError(
            "atomic ceiling passes do not share one candidate scope"
        )
    accepted = 0
    for candidate_id in sorted(pass_a):
        low = min(
            len(pass_a[candidate_id]["atomic_claims"]),
            len(pass_b[candidate_id]["atomic_claims"]),
        )
        high = max(
            len(pass_a[candidate_id]["atomic_claims"]),
            len(pass_b[candidate_id]["atomic_claims"]),
        )
        count = len(pass_c[candidate_id]["atomic_claims"])
        accepted += int(low <= count <= high)
    total = len(pass_a)
    return {
        "rule": "pass_c_count_within_pass_a_pass_b_inclusive_range",
        "accepted": accepted,
        "total": total,
        "rate": round(accepted / total, 6),
        "gate": 0.90,
        "gate_fair": accepted / total >= 0.90,
        "reconciliation_document_sha256": true_north._sha256_file(
            ATOMIC_RECONCILIATION_PATH
        ),
    }


def _calibration_document(suite_root: Path) -> dict[str, Any]:
    base = suite_root / "gold" / "development"
    pass_a, pass_b = true_north._load_independent_atomic_gold(base)
    document = calibration.compute_ceiling_document(pass_a, pass_b)
    sources = sorted(
        [
            *(base / "pass-a" / "outputs").glob(
                "*/validated.private.json"
            ),
            *(base / "pass-b" / "outputs").glob(
                "*/validated.private.json"
            ),
        ]
    )
    document["suite_id"] = true_north.SUITE_ID
    document["partition"] = "development"
    document["source_artifacts"] = {
        "count": len(sources),
        "aggregate_sha256": true_north.sha256_text(
            true_north.dumps_json(
                [
                    {
                        "path": str(path.relative_to(suite_root)),
                        "sha256": true_north._sha256_file(path),
                    }
                    for path in sources
                ]
            )
        ),
    }
    document["calibration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    return document


def _derived_thresholds(
    calibration_document: Mapping[str, Any],
) -> dict[str, float]:
    mapping = {
        "speaker_exactness": "speaker_exactness",
        "reported_actor_exactness": "reported_actor_exactness",
        "claim_text_faithfulness_proxy": (
            "claim_text_faithfulness_proxy"
        ),
    }
    return {
        gate: float(
            calibration_document["ceilings"][ceiling][
                "recommended_threshold"
            ]
        )
        for gate, ceiling in mapping.items()
    }


def gate_policy_document(
    *,
    calibration_document: Mapping[str, Any],
    previous_policy_sha256: str,
) -> dict[str, Any]:
    thresholds = _derived_thresholds(calibration_document)
    expected = {
        metric: float(rule[1])
        for metric, rule in true_north.APPROVED_GATE_POLICY.items()
        if metric in thresholds
    }
    if thresholds != expected:
        raise DecoupledContractError(
            "artifact-derived thresholds differ from approved policy"
        )
    body: dict[str, Any] = {
        "schema_version": true_north.APPROVED_GATE_POLICY_VERSION,
        "suite_id": true_north.SUITE_ID,
        "owner_decision_id": OWNER_DECISION_ID,
        "approved_at": "2026-07-29",
        "scoring_contract": SCORER_VERSION,
        "field_denominator": "matched_atomic_pairs_only",
        "decomposition_gate": (
            "acceptable_atomic_count_rate_range_acceptance_only"
        ),
        "coupled_readings": (
            "required_diagnostics_not_gate_inputs"
        ),
        "calibration_sha256": calibration_document[
            "calibration_sha256"
        ],
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
    calibration_document: Mapping[str, Any],
    gate_policy: Mapping[str, Any],
    atomic_ceiling: Mapping[str, Any],
) -> dict[str, Any]:
    if previous_contract.get("version") != SUPERSEDED_CONTRACT_VERSION:
        raise DecoupledContractError(
            "unsupported prior measurement contract"
        )
    thresholds = _derived_thresholds(calibration_document)
    body: dict[str, Any] = {
        "version": CONTRACT_VERSION,
        "approved_at": "2026-07-29",
        "owner_decision_id": OWNER_DECISION_ID,
        "provenance": {
            "source_kind": (
                "review_thread_directive_in_active_codex_thread"
            ),
            "directive_received_at": "2026-07-29",
            "directive_summary": (
                "Kolby selected matched-pair field gates, retained the "
                "0.90 atomic range-acceptance gate, accepted the "
                "deterministic actor span baseline, and authorized one "
                "bounded Spark split-default probe."
            ),
        },
        "supersedes_contract_sha256": previous_contract[
            "contract_sha256"
        ],
        "disposition_contract": copy.deepcopy(previous_contract),
        "semantic_field_scoring": {
            "scorer_version": SCORER_VERSION,
            "gate_denominator": "matched_atomic_pairs_only",
            "unmatched_atomic_policy": (
                "excluded_from_speaker_reported_actor_and_faithfulness_"
                "gate_denominators"
            ),
            "coupled_readings": (
                "preserved_as_required_diagnostics"
            ),
            "calibration_rule": (
                "min(previous_target, matched_pair_ceiling_mean - 0.04)"
            ),
            "matched_pair_ceiling": {
                metric: calibration_document["ceilings"][metric]["mean"]
                for metric in (
                    "speaker_exactness",
                    "reported_actor_exactness",
                    "claim_text_faithfulness_proxy",
                )
            },
            "gate_thresholds": thresholds,
            "arithmetic_correction": (
                "The authoritative formula yields speaker=0.954615; "
                "the directive's displayed 0.9700 result was arithmetic, "
                "not the adopted rule."
            ),
            "calibration_sha256": calibration_document[
                "calibration_sha256"
            ],
        },
        "atomicity": copy.deepcopy(atomic_ceiling),
        "actor_stage_baseline": {
            "version": "deterministic_actor_span_rule_v1",
            "implementation": (
                "research_factory.true_north_actor_span_rule."
                "apply_actor_span_rule"
            ),
            "model_calls": 0,
            "accepted_as_contract_normative": True,
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
    """Version the manifest and gate policy with rollback on failure."""

    suite_root = true_north._suite_root(output_root, suite)
    manifest_path = suite_root / "manifest.json"
    manifest = true_north._read_json(manifest_path)
    existing = manifest.get("measurement_contract", {})
    calibration_document = _calibration_document(suite_root)
    previous_policy_sha = str(
        existing.get("gate_policy", {}).get(
            "supersedes_gate_policy_sha256"
        )
        or manifest["frozen_interfaces"]["gate_policy_sha256"]
    )
    gate_policy = gate_policy_document(
        calibration_document=calibration_document,
        previous_policy_sha256=previous_policy_sha,
    )
    if existing.get("version") == CONTRACT_VERSION:
        contract = measurement_contract(
            previous_contract=existing["disposition_contract"],
            calibration_document=calibration_document,
            gate_policy=gate_policy,
            atomic_ceiling=_atomic_range_ceiling(suite_root),
        )
        if existing != contract:
            raise DecoupledContractError(
                "current manifest contract differs from reconstruction"
            )
        verification = true_north.verify_suite(
            output_root=output_root, suite=suite
        )
        return {
            "changed": False,
            "manifest_sha256": manifest["manifest_sha256"],
            "contract_sha256": contract["contract_sha256"],
            "verification": verification,
        }
    if existing.get("version") != SUPERSEDED_CONTRACT_VERSION:
        raise DecoupledContractError(
            "manifest carries an unsupported measurement contract"
        )
    atomic_ceiling = _atomic_range_ceiling(suite_root)
    if (
        atomic_ceiling["accepted"] != 1132
        or atomic_ceiling["total"] != 1140
        or atomic_ceiling["rate"] != 0.992982
        or atomic_ceiling["gate_fair"] is not True
    ):
        raise DecoupledContractError(
            "atomic range ceiling differs from adopted reconciliation"
        )
    contract = measurement_contract(
        previous_contract=existing,
        calibration_document=calibration_document,
        gate_policy=gate_policy,
        atomic_ceiling=atomic_ceiling,
    )
    old_manifest = copy.deepcopy(manifest)
    old_sha = str(old_manifest["manifest_sha256"])
    history_path = (
        suite_root
        / "manifest-history"
        / f"manifest-{old_sha}.json"
    )
    calibration_path = suite_root / "diagnostics" / CALIBRATION_FILENAME
    policy_path = suite_root / "diagnostics" / GATE_POLICY_FILENAME
    true_north._write_json(history_path, old_manifest, immutable=True)
    true_north._write_json(
        calibration_path, calibration_document, immutable=True
    )
    true_north._write_json(policy_path, gate_policy, immutable=True)
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
            raise DecoupledContractError(
                "suite verification failed after contract update: "
                + ", ".join(verification["errors"])
            )
        if not true_north._source_unchanged(updated):
            raise DecoupledContractError(
                "production source changed during contract update"
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
        "calibration_path": str(calibration_path),
        "gate_policy_path": str(policy_path),
        "thresholds": _derived_thresholds(calibration_document),
        "atomic_range_ceiling": atomic_ceiling,
        "production_source_unchanged": True,
        "holdout_opened": False,
    }
