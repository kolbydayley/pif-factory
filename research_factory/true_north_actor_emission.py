"""Zero-call feasibility audit for Task 6 reported-actor emission."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from . import true_north
from . import true_north_actor_repair


SCHEMA_VERSION = "pif_true_north_actor_emission_feasibility_v1"


class ActorEmissionAuditError(RuntimeError):
    """Raised when actor feasibility inputs are incomplete or inconsistent."""


def _name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = value.get("name")
    text = " ".join(str(value or "").split())
    if not text or text.casefold() in {"none", "null", "n/a", "unknown"}:
        return None
    return text


def compute_actor_emission_feasibility(
    *,
    pass_a: Mapping[str, str | None],
    pass_b: Mapping[str, str | None],
    pass_c: Mapping[str, str | None],
    gold: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Measure the repaired ceiling and two perfect-emission upper bounds."""

    if not pass_a or set(pass_a) != set(pass_b):
        raise ActorEmissionAuditError(
            "actor pass A and pass B require the same non-empty scope"
        )
    disagreements = {
        atomic_id
        for atomic_id in pass_a
        if pass_a[atomic_id] != pass_b[atomic_id]
    }
    if not disagreements <= set(pass_c):
        raise ActorEmissionAuditError(
            "actor pass C does not cover every A/B disagreement"
        )
    final_values = {
        atomic_id: (
            pass_a[atomic_id]
            if pass_a[atomic_id] == pass_b[atomic_id]
            else pass_c[atomic_id]
        )
        for atomic_id in pass_a
    }
    agreement_count = len(pass_a) - len(disagreements)
    repaired_ceiling = agreement_count / len(pass_a)
    gate = min(0.95, max(0.0, repaired_ceiling - 0.02))

    atomic_count = 0
    gold_null = 0
    gold_non_null = 0
    candidate_prior_emitted = 0
    fallback_emitted = 0
    prior_exact_on_gold_non_null = 0
    fallback_exact_on_gold_non_null = 0
    observed_final: dict[str, str | None] = {}
    for item in gold["items"]:
        candidate_id = str(item["candidate_id"])
        if candidate_id not in candidates:
            raise ActorEmissionAuditError(
                f"gold candidate is missing from frozen bundles: {candidate_id}"
            )
        candidate = candidates[candidate_id]
        candidate_prior = _name(candidate.get("reported_actor"))
        fallback = candidate_prior or _name(candidate.get("actor_name"))
        for atomic_index, atomic in enumerate(item["atomic_claims"]):
            atomic_id = f"{candidate_id}:{atomic_index:02d}"
            gold_actor = _name(atomic.get("reported_actor"))
            observed_final[atomic_id] = gold_actor
            atomic_count += 1
            gold_null += int(gold_actor is None)
            gold_non_null += int(gold_actor is not None)
            candidate_prior_emitted += int(candidate_prior is not None)
            fallback_emitted += int(fallback is not None)
            if gold_actor is not None:
                prior_exact_on_gold_non_null += int(
                    candidate_prior is not None
                    and candidate_prior.casefold() == gold_actor.casefold()
                )
                fallback_exact_on_gold_non_null += int(
                    fallback is not None
                    and fallback.casefold() == gold_actor.casefold()
                )

    if set(observed_final) != set(final_values):
        raise ActorEmissionAuditError(
            "promoted gold atomic scope differs from actor repair scope"
        )
    if any(
        _name(final_values[key]) != observed_final[key]
        for key in observed_final
    ):
        raise ActorEmissionAuditError(
            "promoted gold does not match pass-C actor adjudication"
        )
    perfect_emission_existing_prior = (
        gold_null + prior_exact_on_gold_non_null
    ) / atomic_count
    perfect_emission_perfect_values = 1.0
    return {
        "schema_version": SCHEMA_VERSION,
        "post_repair_interannotator": {
            "atomic_count": len(pass_a),
            "agreement_count": agreement_count,
            "disagreement_count": len(disagreements),
            "ceiling": round(repaired_ceiling, 6),
            "gate_rule": "min(0.95, repaired_ceiling - 0.02)",
            "ceiling_referenced_gate": round(gate, 6),
        },
        "gold_prevalence": {
            "atomic_count": atomic_count,
            "reported_actor_non_null_count": gold_non_null,
            "reported_actor_non_null_rate": round(
                gold_non_null / atomic_count, 6
            ),
            "reported_actor_null_count": gold_null,
            "reported_actor_null_rate": round(
                gold_null / atomic_count, 6
            ),
        },
        "prior_diagnostic": {
            "candidate_reported_actor_emitted_count": (
                candidate_prior_emitted
            ),
            "candidate_reported_actor_emission_rate": round(
                candidate_prior_emitted / atomic_count, 6
            ),
            "reported_actor_or_actor_name_emitted_count": fallback_emitted,
            "reported_actor_or_actor_name_emission_rate": round(
                fallback_emitted / atomic_count, 6
            ),
            "existing_prior_correct_non_null_count": (
                prior_exact_on_gold_non_null
            ),
            "existing_prior_value_accuracy_given_gold_non_null": round(
                prior_exact_on_gold_non_null / gold_non_null, 6
            )
            if gold_non_null
            else None,
            "fallback_correct_non_null_count": (
                fallback_exact_on_gold_non_null
            ),
        },
        "upper_bounds": {
            "perfect_emission_existing_prior_values": round(
                perfect_emission_existing_prior, 6
            ),
            "perfect_emission_perfect_values": (
                perfect_emission_perfect_values
            ),
            "perfect_value_headroom": round(
                perfect_emission_perfect_values
                - perfect_emission_existing_prior,
                6,
            ),
        },
        "task_6": {
            "achievable_ceiling": round(
                perfect_emission_existing_prior, 6
            ),
            "required_gate": round(gate, 6),
            "authorized": perfect_emission_existing_prior >= gate,
            "decision": (
                "build_binary_emission"
                if perfect_emission_existing_prior >= gate
                else "stop_upper_bound_below_gate"
            ),
        },
        "holdout_opened": False,
        "model_calls": 0,
    }


def audit_actor_emission_feasibility(
    suite_root: str | Path,
) -> dict[str, Any]:
    """Load hash-bound development artifacts and write the audit result."""

    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise ActorEmissionAuditError(
            "suite verification failed before actor feasibility audit"
        )
    repair_root = (
        root / "gold" / "development" / "actor-repair"
    )
    audits: dict[str, dict[str, int]] = {
        "pass_a": {},
        "pass_b": {},
        "pass_c": {},
    }
    pass_a = true_north_actor_repair._load_actor_values(
        repair_root / "pass-a", audit=audits["pass_a"]
    )
    pass_b = true_north_actor_repair._load_actor_values(
        repair_root / "pass-b", audit=audits["pass_b"]
    )
    pass_c = true_north_actor_repair._load_actor_values(
        repair_root / "pass-c-adjudication", audit=audits["pass_c"]
    )
    manifest = true_north._read_json(root / "manifest.json")
    candidates: dict[str, dict[str, Any]] = {}
    bundle_hashes: dict[str, str] = {}
    for row in manifest["bundles"]:
        if row["partition"] != "development":
            continue
        bundle_path = Path(row["bundle_path"])
        bundle_hashes[str(row["episode_id"])] = (
            true_north._sha256_file(bundle_path)
        )
        bundle = true_north._read_json(bundle_path)
        for candidate in bundle["candidates"]:
            candidates[str(candidate["candidate_id"])] = dict(candidate)
    gold_path = (
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    gold = true_north._read_json(gold_path)
    document = compute_actor_emission_feasibility(
        pass_a=pass_a,
        pass_b=pass_b,
        pass_c=pass_c,
        gold=gold,
        candidates=candidates,
    )
    document["provenance"] = {
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "gold_path": str(gold_path),
        "gold_file_sha256": true_north._sha256_file(gold_path),
        "actor_repair_result_path": str(
            repair_root / "result-span-enforced.json"
        ),
        "actor_repair_result_file_sha256": true_north._sha256_file(
            repair_root / "result-span-enforced.json"
        ),
        "development_bundle_file_sha256": dict(
            sorted(bundle_hashes.items())
        ),
        "span_enforcement_audit": audits,
    }
    document["audit_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = (
        root
        / "diagnostics"
        / "task6-actor-emission-feasibility.json"
    )
    true_north._write_json(path, document, immutable=False)
    return {**document, "audit_path": str(path)}
