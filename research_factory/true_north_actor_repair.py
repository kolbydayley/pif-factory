"""Bounded A/B/C re-adjudication of the True North reported-actor field."""

from __future__ import annotations

import copy
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north


SCHEMA_VERSION = "pif_true_north_actor_repair_v1"
OUTPUT_SCHEMA_VERSION = "pif_true_north_actor_repair_output_v1"
MODEL = "openai/gpt-5.3-codex-spark"
BATCH_SIZE = 75
MAX_CALLS = 60
ACTOR_CONTRACT = (
    "`reported_actor` is the focal actor: the named person, organization, or "
    "collective whose action, decision, state, or outcome the claim describes, "
    "when that actor is explicitly named in the evidence and is not the direct "
    "speaker speaking in their own voice about themselves. It is not restricted "
    "to sources of reported speech. If no such actor is explicitly named, the "
    "field is null."
)
SYSTEM_PROMPT = (
    "You are independently adjudicating exactly one field in a private local "
    "podcast gold set. Do not use tools or outside knowledge. Apply this "
    f"definition exactly: {ACTOR_CONTRACT} Return only strict JSON. Never "
    "change or emit any field other than each supplied atomic_id and its "
    "reported_actor value."
)


class ActorRepairError(RuntimeError):
    """Raised when actor repair leaves its bounded field or budget."""


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _actor_output_schema(atomic_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": OUTPUT_SCHEMA_VERSION,
            },
            "items": {
                "type": "array",
                "minItems": len(atomic_ids),
                "maxItems": len(atomic_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["atomic_id", "reported_actor"],
                    "properties": {
                        "atomic_id": {
                            "type": "string",
                            "enum": list(atomic_ids),
                        },
                        "reported_actor": {
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        },
    }


def _validate_actor_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    true_north._validate_schema(
        packet["output_schema"], output, path="$"
    )
    expected = {
        str(row["atomic_id"]) for row in packet["input"]["items"]
    }
    seen = [str(row["atomic_id"]) for row in output["items"]]
    if len(seen) != len(set(seen)) or set(seen) != expected:
        raise ActorRepairError("actor output does not exactly cover packet")
    inputs = {
        str(row["atomic_id"]): row for row in packet["input"]["items"]
    }
    for row in output["items"]:
        value = row["reported_actor"]
        if isinstance(value, str):
            normalized = " ".join(value.split())
            if not normalized:
                raise ActorRepairError("reported_actor must be nonblank or null")
            source = inputs[str(row["atomic_id"])]
            evidence = " ".join(
                str(source["evidence_text"]).casefold().split()
            )
            speaker = " ".join(
                str(source["raw_speaker"]).casefold().split()
            )
            actor = normalized.casefold()
            row["reported_actor"] = (
                normalized
                if actor in evidence and actor != speaker
                else None
            )


def actor_repair_budget(
    atomic_count: int,
    *,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    if atomic_count <= 0 or batch_size <= 0:
        raise ActorRepairError("atomic_count and batch_size must be positive")
    per_pass = math.ceil(atomic_count / batch_size)
    maximum = per_pass * 3
    if maximum > MAX_CALLS:
        raise ActorRepairError(
            f"actor repair packet ceiling exceeds {MAX_CALLS}: {maximum}"
        )
    return {
        "atomic_count": atomic_count,
        "batch_size": batch_size,
        "pass_a_packet_count": per_pass,
        "pass_b_packet_count": per_pass,
        "pass_c_packet_ceiling": per_pass,
        "max_calls": MAX_CALLS,
        "maximum_packet_count": maximum,
    }


def _atomic_rows(gold: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in gold["items"]:
        for atomic_index, atomic in enumerate(item["atomic_claims"]):
            rows.append(
                {
                    "atomic_id": (
                        f"{item['candidate_id']}:{atomic_index:02d}"
                    ),
                    "candidate_id": str(item["candidate_id"]),
                    "episode_id": str(item["episode_id"]),
                    "atomic_index": atomic_index,
                    "claim_text": str(atomic["claim_text"]),
                    "raw_speaker": str(atomic["raw_speaker"]),
                    "current_reported_actor": atomic["reported_actor"],
                    "evidence_text": str(atomic["evidence_text"]),
                    "evidence_start": int(atomic["evidence_start"]),
                    "evidence_end": int(atomic["evidence_end"]),
                }
            )
    return rows


def _packet(
    rows: Sequence[Mapping[str, Any]],
    *,
    pass_name: str,
    batch_number: int,
) -> dict[str, Any]:
    atomic_ids = [str(row["atomic_id"]) for row in rows]
    instructions = [
        ACTOR_CONTRACT,
        "Use only each row's exact evidence, atomic claim, and direct speaker.",
        "Return one item for every atomic_id and no others.",
        "Copy the concise actor name as an exact span from the evidence or return null.",
        "Do not preserve the current value merely because it is present.",
    ]
    if pass_name == "pass-c-adjudication":
        instructions = [
            ACTOR_CONTRACT,
            "Resolve only the supplied pass-A/pass-B disagreement.",
            "Neither prior answer is authoritative; use the exact evidence.",
            "Return one item for every atomic_id and no others.",
        ]
    return {
        "schema_version": SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "partition": "development",
        "actor_repair_stage": pass_name,
        "multipass_stage": "actor-repair",
        "batch_number": batch_number,
        "field_scope": ["reported_actor"],
        "instructions": instructions,
        "output_schema": _actor_output_schema(atomic_ids),
        "input": {"items": list(rows)},
    }


def _repair_root(suite_root: Path) -> Path:
    return suite_root / "gold" / "development" / "actor-repair"


def prepare_actor_repair(
    suite_root: str | Path,
    *,
    partition: str = "development",
) -> dict[str, Any]:
    if partition != "development":
        raise ActorRepairError("actor repair is development-only")
    root = Path(suite_root).expanduser().resolve()
    true_north.verify_suite(output_root=root.parent, suite=root.name)
    gold_path = root / "gold" / "development" / "final" / "gold.private.json"
    gold = _read(gold_path)
    rows = _atomic_rows(gold)
    budget = actor_repair_budget(len(rows))
    repair_root = _repair_root(root)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "partition": "development",
        "field_scope": ["reported_actor"],
        "model": MODEL,
        "actor_contract": ACTOR_CONTRACT,
        "source_gold_path": str(gold_path),
        "source_gold_file_sha256": true_north._sha256_file(gold_path),
        "source_gold_sha256": gold["gold_sha256"],
        "budget": budget,
        "canonical_gold_mutation_allowed": False,
    }
    manifest["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(manifest)
    )
    true_north._write_json(
        repair_root / "manifest.json", manifest, immutable=True
    )
    for pass_name in ("pass-a", "pass-b"):
        for offset in range(0, len(rows), BATCH_SIZE):
            batch_number = offset // BATCH_SIZE
            job = _packet(
                rows[offset : offset + BATCH_SIZE],
                pass_name=pass_name,
                batch_number=batch_number,
            )
            true_north._write_json(
                repair_root
                / pass_name
                / "jobs"
                / f"actor-{batch_number:03d}.private.json",
                job,
                immutable=True,
            )
    return {
        "state": "prepared",
        "repair_root": str(repair_root),
        "atomic_count": len(rows),
        "budget": budget,
        "canonical_mutation": False,
    }


def _load_actor_values(
    pass_root: Path,
    *,
    audit: dict[str, int] | None = None,
) -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    jobs = sorted((pass_root / "jobs").glob("*.private.json"))
    if not jobs:
        raise ActorRepairError(f"actor pass has no jobs: {pass_root}")
    for job_path in jobs:
        name = job_path.name.removesuffix(".private.json")
        output_path = pass_root / "outputs" / name / "validated.private.json"
        if not output_path.is_file():
            raise ActorRepairError(f"actor output is missing: {name}")
        packet = _read(job_path)
        output = _read(output_path)
        raw_non_null = sum(
            row["reported_actor"] is not None for row in output["items"]
        )
        _validate_actor_output(output, packet)
        sanitized_non_null = sum(
            row["reported_actor"] is not None for row in output["items"]
        )
        if audit is not None:
            audit["raw_non_null"] = (
                audit.get("raw_non_null", 0) + raw_non_null
            )
            audit["sanitized_non_null"] = (
                audit.get("sanitized_non_null", 0) + sanitized_non_null
            )
            audit["mechanical_null_overrides"] = (
                audit.get("mechanical_null_overrides", 0)
                + raw_non_null
                - sanitized_non_null
            )
        for row in output["items"]:
            atomic_id = str(row["atomic_id"])
            if atomic_id in values:
                raise ActorRepairError(f"duplicate actor value: {atomic_id}")
            values[atomic_id] = row["reported_actor"]
    return values


def prepare_actor_adjudication(
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    repair_root = _repair_root(root)
    values_a = _load_actor_values(repair_root / "pass-a")
    values_b = _load_actor_values(repair_root / "pass-b")
    if set(values_a) != set(values_b):
        raise ActorRepairError("actor pass-A/pass-B scopes differ")
    source_gold = _read(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    source_rows = {
        str(row["atomic_id"]): row for row in _atomic_rows(source_gold)
    }
    disagreement_ids = [
        atomic_id
        for atomic_id in sorted(values_a)
        if values_a[atomic_id] != values_b[atomic_id]
    ]
    disagreement_rows = [
        {
            **source_rows[atomic_id],
            "pass_a_reported_actor": values_a[atomic_id],
            "pass_b_reported_actor": values_b[atomic_id],
        }
        for atomic_id in disagreement_ids
    ]
    pass_root = repair_root / "pass-c-adjudication"
    for offset in range(0, len(disagreement_rows), BATCH_SIZE):
        batch_number = offset // BATCH_SIZE
        job = _packet(
            disagreement_rows[offset : offset + BATCH_SIZE],
            pass_name="pass-c-adjudication",
            batch_number=batch_number,
        )
        true_north._write_json(
            pass_root
            / "jobs"
            / f"actor-{batch_number:03d}.private.json",
            job,
            immutable=True,
        )
    return {
        "state": "adjudication_prepared",
        "atomic_count": len(values_a),
        "agreement_count": len(values_a) - len(disagreement_ids),
        "disagreement_count": len(disagreement_ids),
        "packet_count": math.ceil(len(disagreement_ids) / BATCH_SIZE),
        "canonical_mutation": False,
    }


def _receipt_count(repair_root: Path) -> int:
    count = 0
    for path in repair_root.glob(
        "pass-*/outputs/*/receipt.private.json"
    ):
        count += len(_read(path).get("receipts", []))
    return count


def execute_actor_pass(
    suite_root: str | Path,
    *,
    pass_name: str,
    workers: int = 4,
    timeout_seconds: int = 1200,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
) -> dict[str, Any]:
    if pass_name not in {"pass-a", "pass-b", "pass-c"}:
        raise ActorRepairError("actor pass must be pass-a, pass-b, or pass-c")
    root = Path(suite_root).expanduser().resolve()
    repair_root = _repair_root(root)
    if not (repair_root / "manifest.json").is_file():
        prepare_actor_repair(root)
    directory = (
        "pass-c-adjudication" if pass_name == "pass-c" else pass_name
    )
    if pass_name == "pass-c":
        preparation = prepare_actor_adjudication(root)
    else:
        preparation = None
    pass_root = repair_root / directory
    jobs = sorted((pass_root / "jobs").glob("*.private.json"))
    prior_calls = _receipt_count(repair_root)
    if prior_calls + len(jobs) > MAX_CALLS:
        raise ActorRepairError("actor-repair call budget would be exceeded")

    def run_one(job_path: Path) -> dict[str, Any]:
        name = job_path.name.removesuffix(".private.json")
        output_dir = pass_root / "outputs" / name
        output, receipts, model = true_north._run_opencode_packet(
            packet_path=job_path,
            output_dir=output_dir,
            models=(MODEL,),
            stage=f"actor-repair-{pass_name}",
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            system_prompt=SYSTEM_PROMPT,
            validator=_validate_actor_output,
            _semantic_retry_remaining=0,
        )
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "packet_path": str(job_path),
            "packet_sha256": true_north._sha256_file(job_path),
            "provider_model": model,
            "output_sha256": true_north.sha256_text(
                true_north.dumps_json(output)
            ),
            "receipts": receipts,
        }
        true_north._write_json(
            output_dir / "receipt.private.json",
            receipt,
            immutable=False,
        )
        return receipt

    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(4, workers))) as pool:
        futures = {pool.submit(run_one, path): path for path in jobs}
        for future in as_completed(futures):
            receipts.append(future.result())
    actual_calls = _receipt_count(repair_root)
    if actual_calls > MAX_CALLS:
        raise ActorRepairError("actor-repair call budget exceeded")
    ledger = {
        "schema_version": SCHEMA_VERSION,
        "max_calls": MAX_CALLS,
        "actual_calls": actual_calls,
        "completed_pass": pass_name,
        "packet_count": len(jobs),
        "model": MODEL,
        "canonical_mutation": False,
    }
    true_north._write_json(
        repair_root / "call-ledger.json", ledger, immutable=False
    )
    return {
        "ok": True,
        "state": "actor_pass_completed",
        "pass": pass_name,
        "packet_count": len(jobs),
        "actual_calls": actual_calls,
        "max_calls": MAX_CALLS,
        "preparation": preparation,
        "canonical_mutation": False,
    }


def apply_actor_repairs(
    source_gold: Mapping[str, Any],
    repairs: Mapping[str, str | None],
) -> dict[str, Any]:
    """Apply exactly one actor value per existing atomic, changing no other field."""

    revised = copy.deepcopy(source_gold)
    expected: set[str] = set()
    for item in revised["items"]:
        for atomic_index, atomic in enumerate(item["atomic_claims"]):
            atomic_id = f"{item['candidate_id']}:{atomic_index:02d}"
            expected.add(atomic_id)
            if atomic_id not in repairs:
                raise ActorRepairError(f"missing actor repair: {atomic_id}")
            atomic["reported_actor"] = repairs[atomic_id]
    if set(repairs) != expected:
        raise ActorRepairError("actor repairs contain out-of-scope atomic IDs")

    comparison = copy.deepcopy(revised)
    for item in comparison["items"]:
        for atomic_index, atomic in enumerate(item["atomic_claims"]):
            source_item = next(
                row
                for row in source_gold["items"]
                if row["candidate_id"] == item["candidate_id"]
            )
            atomic["reported_actor"] = source_item["atomic_claims"][
                atomic_index
            ]["reported_actor"]
    if comparison != source_gold:
        raise ActorRepairError("actor repair changed a non-actor gold field")
    return revised


def compile_actor_repair(
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    repair_root = _repair_root(root)
    manifest = _read(repair_root / "manifest.json")
    source_path = Path(manifest["source_gold_path"])
    if true_north._sha256_file(source_path) != manifest[
        "source_gold_file_sha256"
    ]:
        raise ActorRepairError("source gold drifted during actor repair")
    enforcement_a: dict[str, int] = {}
    enforcement_b: dict[str, int] = {}
    enforcement_c: dict[str, int] = {}
    values_a = _load_actor_values(
        repair_root / "pass-a", audit=enforcement_a
    )
    values_b = _load_actor_values(
        repair_root / "pass-b", audit=enforcement_b
    )
    disagreements = {
        atomic_id
        for atomic_id in values_a
        if values_a[atomic_id] != values_b[atomic_id]
    }
    values_c = (
        _load_actor_values(
            repair_root / "pass-c-adjudication", audit=enforcement_c
        )
        if disagreements
        else {}
    )
    if not disagreements <= set(values_c):
        raise ActorRepairError("pass-C does not cover actor disagreements")
    final_values = {
        atomic_id: (
            values_a[atomic_id]
            if values_a[atomic_id] == values_b[atomic_id]
            else values_c[atomic_id]
        )
        for atomic_id in values_a
    }
    agreement = sum(
        values_a[key] == values_b[key] for key in values_a
    ) / len(values_a)
    repaired_ceiling = round(agreement, 6)
    proposed_threshold = round(
        min(0.95, max(0.0, repaired_ceiling - 0.02)), 6
    )
    source_gold = _read(source_path)
    revised = apply_actor_repairs(source_gold, final_values)
    revised.pop("gold_sha256", None)
    revised["actor_repair"] = {
        "schema_version": SCHEMA_VERSION,
        "source_gold_sha256": manifest["source_gold_sha256"],
        "contract": ACTOR_CONTRACT,
        "model": MODEL,
        "pass_a_b_agreement": repaired_ceiling,
        "disagreement_count": len(disagreements),
        "proposed_gate_threshold": proposed_threshold,
        "evidence_span_enforcement": {
            "pass_a": enforcement_a,
            "pass_b": enforcement_b,
            "pass_c": enforcement_c,
        },
        "canonical_switch_approved": False,
    }
    revised["gold_sha256"] = true_north.sha256_text(
        true_north.dumps_json(revised)
    )
    final_path = (
        repair_root / "final-span-enforced" / "gold.private.json"
    )
    true_north._write_json(final_path, revised, immutable=True)

    source_consensus = _read(
        root
        / "gold"
        / "development"
        / "final"
        / "consensus.private.json"
    )
    proposed_consensus = copy.deepcopy(source_consensus)
    proposed_consensus.pop("consensus_sha256", None)
    proposed_consensus["source_gold_sha256"] = revised["gold_sha256"]
    proposed_consensus["actor_repair"] = revised["actor_repair"]
    proposed_consensus["consensus_sha256"] = true_north.sha256_text(
        true_north.dumps_json(proposed_consensus)
    )
    consensus_path = (
        repair_root / "final-span-enforced" / "consensus.private.json"
    )
    true_north._write_json(consensus_path, proposed_consensus, immutable=True)
    result = {
        "schema_version": SCHEMA_VERSION,
        "atomic_count": len(values_a),
        "agreement_count": len(values_a) - len(disagreements),
        "disagreement_count": len(disagreements),
        "pass_c_adjudicated_count": len(values_c),
        "repaired_ceiling": repaired_ceiling,
        "proposed_gate_threshold": proposed_threshold,
        "actual_calls": _receipt_count(repair_root),
        "max_calls": MAX_CALLS,
        "proposed_gold_path": str(final_path),
        "proposed_gold_sha256": revised["gold_sha256"],
        "proposed_consensus_path": str(consensus_path),
        "proposed_consensus_sha256": proposed_consensus[
            "consensus_sha256"
        ],
        "canonical_gold_mutated": False,
        "live_gate_changed": False,
        "supersedes_unenforced_draft": str(
            repair_root / "final" / "gold.private.json"
        ),
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    true_north._write_json(
        repair_root / "result-span-enforced.json",
        result,
        immutable=True,
    )
    return result


def promote_actor_repair(
    suite_root: str | Path,
    *,
    approval_receipt: str,
) -> dict[str, Any]:
    """Version and promote the approved actor-only gold and gate policy."""

    if not approval_receipt.strip():
        raise ActorRepairError("approval receipt is required")
    root = Path(suite_root).expanduser().resolve()
    repair_root = _repair_root(root)
    result = _read(repair_root / "result-span-enforced.json")
    proposed_gold = _read(Path(result["proposed_gold_path"]))
    proposed_consensus = _read(Path(result["proposed_consensus_path"]))
    canonical_root = root / "gold" / "development" / "final"
    current_gold_path = canonical_root / "gold.private.json"
    current_consensus_path = canonical_root / "consensus.private.json"
    current_gold = _read(current_gold_path)
    current_consensus = _read(current_consensus_path)
    if (
        proposed_gold["actor_repair"]["source_gold_sha256"]
        != current_gold["gold_sha256"]
        or proposed_consensus["actor_repair"]["source_gold_sha256"]
        != current_gold["gold_sha256"]
    ):
        raise ActorRepairError(
            "proposed actor repair is not based on current canonical gold"
        )

    history = (
        root
        / "gold"
        / "development"
        / "history"
        / str(current_gold["gold_sha256"])
    )
    true_north._write_json(
        history / "gold.private.json", current_gold, immutable=True
    )
    true_north._write_json(
        history / "consensus.private.json",
        current_consensus,
        immutable=True,
    )

    promoted_gold = copy.deepcopy(proposed_gold)
    promoted_gold.pop("gold_sha256", None)
    promoted_gold["actor_repair"]["canonical_switch_approved"] = True
    promoted_gold["actor_repair"]["approval_receipt"] = approval_receipt
    promoted_gold["gold_sha256"] = true_north.sha256_text(
        true_north.dumps_json(promoted_gold)
    )
    promoted_consensus = copy.deepcopy(proposed_consensus)
    promoted_consensus.pop("consensus_sha256", None)
    promoted_consensus["source_gold_sha256"] = promoted_gold["gold_sha256"]
    promoted_consensus["actor_repair"] = promoted_gold["actor_repair"]
    promoted_consensus["consensus_sha256"] = true_north.sha256_text(
        true_north.dumps_json(promoted_consensus)
    )

    true_north._write_json(
        current_gold_path, promoted_gold, immutable=False
    )
    true_north._write_json(
        current_consensus_path, promoted_consensus, immutable=False
    )

    calibration_path = root / "diagnostics" / "gate-calibration.json"
    calibration = _read(calibration_path)
    gate_policy = {
        "schema_version": true_north.APPROVED_GATE_POLICY_VERSION,
        "suite_id": true_north.SUITE_ID,
        "approval_receipt": approval_receipt,
        "approval_scope": (
            "faithfulness_0.75_actor_0.903182_actor_gold_promotion_phase_c"
        ),
        "rules": {
            metric: {
                "comparison": comparison,
                "threshold": threshold,
            }
            for metric, (comparison, threshold) in sorted(
                true_north.APPROVED_GATE_POLICY.items()
            )
        },
        "faithfulness_calibration_sha256": calibration[
            "calibration_sha256"
        ],
        "actor_repair_result_sha256": result["result_sha256"],
        "promoted_gold_sha256": promoted_gold["gold_sha256"],
        "previous_gold_sha256": current_gold["gold_sha256"],
        "justification": (
            "User approved the v2 checkpoint after zero-call floor, "
            "inter-annotator ceiling calibration, and span-enforced "
            "independent actor re-adjudication."
        ),
    }
    gate_policy["gate_policy_sha256"] = true_north.sha256_text(
        true_north.dumps_json(gate_policy)
    )
    gate_policy_path = root / "diagnostics" / "gate-policy-v2.json"
    true_north._write_json(
        gate_policy_path, gate_policy, immutable=True
    )

    manifest_path = root / "manifest.json"
    manifest = _read(manifest_path)
    old_manifest_sha = str(manifest["manifest_sha256"])
    true_north._write_json(
        root / "history" / f"manifest-{old_manifest_sha}.json",
        manifest,
        immutable=True,
    )
    manifest.pop("manifest_sha256", None)
    manifest["frozen_interfaces"]["gate_policy_version"] = (
        true_north.APPROVED_GATE_POLICY_VERSION
    )
    manifest["frozen_interfaces"]["gate_policy_sha256"] = gate_policy[
        "gate_policy_sha256"
    ]
    manifest["frozen_interfaces"]["development_gold_sha256"] = promoted_gold[
        "gold_sha256"
    ]
    manifest["frozen_interfaces"][
        "development_consensus_sha256"
    ] = promoted_consensus["consensus_sha256"]
    manifest["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(manifest)
    )
    true_north._write_json(manifest_path, manifest, immutable=False)
    return {
        "ok": True,
        "approval_receipt": approval_receipt,
        "previous_gold_sha256": current_gold["gold_sha256"],
        "promoted_gold_sha256": promoted_gold["gold_sha256"],
        "promoted_consensus_sha256": promoted_consensus[
            "consensus_sha256"
        ],
        "gate_policy_sha256": gate_policy["gate_policy_sha256"],
        "previous_manifest_sha256": old_manifest_sha,
        "manifest_sha256": manifest["manifest_sha256"],
        "history_path": str(history),
        "production_mutation": False,
    }
