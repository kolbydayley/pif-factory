from __future__ import annotations

"""Independent side-free adjudication of provisional fixture-truth disagreements."""

import argparse
import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_judge_v5 import (
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_prompt,
    freeze_support_receipts,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration_runner import (
    validate_scoreable_calibration_alignment_output,
)
from .app_server_judge_v5_diagnostic import (
    JudgeV5DiagnosticAttemptFailed,
    _aggregate_usage,
    _attempt_records,
    _freeze_turn_request,
    _get_or_run_turn,
    _record,
    _sha256_file,
    _write_immutable_json,
)
from .app_server_judge_v5_fixture_audit_recovery import (
    normalize_scoreable_alignment_output,
)
from .util import now_iso


REFERENCE_ADJUDICATION_SPEC_VERSION = (
    "pif_app_server_fixture_reference_adjudication_v4_spec_v1"
)
REFERENCE_ADJUDICATION_TERMINAL_VERSION = (
    "pif_app_server_fixture_reference_adjudication_v4_terminal_v1"
)
REFERENCE_VERSION = "pif_app_server_judge_fixture_reference_v2"
REFERENCE_RECEIPT_VERSION = "pif_app_server_judge_fixture_reference_v2_receipt_v1"
DEFAULT_V2_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2"
).resolve()
DEFAULT_V3_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3"
).resolve()
DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-reference-adjudication-luna-v4"
).resolve()
CASES_PER_SHARD = 6


class ReferenceAdjudicationError(RuntimeError):
    """The fixture-reference adjudication cannot proceed safely."""


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReferenceAdjudicationError("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise ReferenceAdjudicationError("%s is not an object" % purpose)
    return value


def _chunks(values: Sequence[str], size: int = CASES_PER_SHARD) -> list[list[str]]:
    return [list(values[index : index + size]) for index in range(0, len(values), size)]


def _merge(outputs: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output[key]]
    ids = [(row["case_id"], row.get("witness_id")) for row in rows]
    if len(ids) != len(set(ids)):
        raise ReferenceAdjudicationError("adjudication shard outputs overlap")
    return {key: rows}


def build_reference_disagreement_manifest(
    *, v2_root: Path, v3_root: Path
) -> dict[str, Any]:
    v2 = v2_root.expanduser().resolve()
    v3 = v3_root.expanduser().resolve()
    v2_terminal = _load_json(v2 / "terminal.json", "fixture-audit v2 terminal")
    v3_terminal = _load_json(v3 / "terminal.json", "fixture-audit v3 terminal")
    if (
        v2_terminal.get("state") != "failed"
        or v2_terminal.get("usage_status") != "complete"
        or (v2_terminal.get("usage") or {}).get("total_tokens") != 827876
        or v3_terminal.get("state") != "completed"
        or v3_terminal.get("fixture_truth_proposal_completed") is not True
        or v3_terminal.get("reference_freeze_authorized") is not False
        or v3_terminal.get("selection_authorized") is not False
        or v3_terminal.get("usage_status") != "complete"
        or (v3_terminal.get("cumulative_proposal_usage") or {}).get("total_tokens")
        != 854550
    ):
        raise ReferenceAdjudicationError("fixture-audit predecessor terminal drifted")
    truth = _load_json(
        v2 / "provisional-calibration-truth.private.json", "provisional truth"
    )
    pointwise = _load_json(v2 / "pointwise-output-full.private.json", "proposal pointwise")
    alignment = _load_json(v3 / "reconciled-alignment.private.json", "proposal alignment")
    pointwise_by_id = {row["witness_id"]: row for row in pointwise["units"]}
    alignment_by_case = {row["case_id"]: row for row in alignment["cases"]}
    disputed_witness_ids = []
    pointwise_dimensions: dict[str, list[str]] = {}
    pointwise_case_ids = []
    alignment_case_ids = []
    alignment_dimensions: dict[str, list[str]] = {}
    for case_id, expected in truth["cases"].items():
        case_has_pointwise_dispute = False
        for witness_id, proposition in expected["proposition"].items():
            row = pointwise_by_id[witness_id]
            dimensions = []
            if row["proposition_verdict"] != proposition:
                dimensions.append("proposition_source_support")
            if row["structured_field_verdict"] != expected["structured_fields"][witness_id]:
                dimensions.append("structured_event_field_correctness")
            if set(row["field_issue_fields"]) != set(expected["field_issues"][witness_id]):
                dimensions.append("field_issue_fields")
            if dimensions:
                disputed_witness_ids.append(witness_id)
                pointwise_dimensions[witness_id] = dimensions
                case_has_pointwise_dispute = True
        if case_has_pointwise_dispute:
            pointwise_case_ids.append(case_id)
        observed = alignment_by_case[case_id]
        expected_pairs = {
            tuple(pair["witness_ids"]): (
                pair["relation"],
                tuple(pair["mismatch_fields"]),
            )
            for pair in expected["pairs"]
        }
        observed_pairs = {
            tuple(pair["witness_ids"]): (
                pair["relation"],
                tuple(pair["mismatch_fields"]),
            )
            for pair in observed["alignment_pairs"]
        }
        dimensions = []
        if expected_pairs != observed_pairs:
            dimensions.append("alignment_pairs")
        if {tuple(group) for group in expected["equivalence_groups"]} != {
            tuple(group) for group in observed["equivalence_groups"]
        }:
            dimensions.append("equivalence_partition")
        if set(expected["unpaired_witness_ids"]) != set(
            observed["unpaired_witness_ids"]
        ):
            dimensions.append("unpaired_witnesses")
        if dimensions:
            alignment_case_ids.append(case_id)
            alignment_dimensions[case_id] = dimensions
    if (
        len(disputed_witness_ids) != 102
        or len(pointwise_case_ids) != 49
        or len(alignment_case_ids) != 18
    ):
        raise ReferenceAdjudicationError("fixture-reference disagreement coverage drifted")
    pointwise_shards = _chunks(pointwise_case_ids)
    alignment_shards = _chunks(alignment_case_ids)
    if (
        len(pointwise_shards) != 9
        or len(alignment_shards) != 3
        or any(len(shard) > 6 for shard in pointwise_shards + alignment_shards)
    ):
        raise ReferenceAdjudicationError("fixture-reference shard layout drifted")
    return {
        "schema_version": "pif_app_server_fixture_reference_disagreements_v1",
        "pointwise_disputed_witness_ids": disputed_witness_ids,
        "pointwise_disputed_case_ids": pointwise_case_ids,
        "pointwise_dimensions_by_witness": pointwise_dimensions,
        "alignment_disputed_case_ids": alignment_case_ids,
        "alignment_dimensions_by_case": alignment_dimensions,
        "pointwise_shards": pointwise_shards,
        "alignment_shards": alignment_shards,
        "pointwise_disputed_witness_count": 102,
        "pointwise_disputed_case_count": 49,
        "alignment_disputed_case_count": 18,
        "cases_per_shard_maximum": 6,
        "candidate_labels_exposed_to_adjudicator": False,
        "model_identities_exposed_to_adjudicator": False,
        "majority_voting_allowed": False,
    }


def _pointwise_adjudication_input(
    pointwise_input: Mapping[str, Any], witness_ids: set[str]
) -> dict[str, Any]:
    units = [
        deepcopy(unit)
        for unit in pointwise_input["units"]
        if unit["witness_id"] in witness_ids
    ]
    if {unit["witness_id"] for unit in units} != witness_ids:
        raise ReferenceAdjudicationError("pointwise adjudication coverage drifted")
    return {
        "schema_version": pointwise_input["schema_version"],
        "units": units,
        "side_labels_present": False,
        "system_identity_present": False,
        "empty_event_fields_omitted_only": True,
        "candidate_labels_present": False,
        "adjudication_is_independent": True,
    }


def _final_reference(
    *,
    provisional: Mapping[str, Any],
    proposal_pointwise: Mapping[str, Any],
    adjudicated_pointwise: Mapping[str, Any],
    proposal_alignment: Mapping[str, Any],
    adjudicated_alignment: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    pointwise = {row["witness_id"]: deepcopy(row) for row in proposal_pointwise["units"]}
    disputed_witnesses = set(manifest["pointwise_disputed_witness_ids"])
    adjudicated_by_id = {
        row["witness_id"]: deepcopy(row) for row in adjudicated_pointwise["units"]
    }
    if set(adjudicated_by_id) != disputed_witnesses:
        raise ReferenceAdjudicationError("pointwise adjudication output coverage drifted")
    pointwise.update(adjudicated_by_id)
    proposal_cases = {row["case_id"]: row for row in proposal_alignment["cases"]}
    adjudicated_cases = {
        row["case_id"]: row for row in adjudicated_alignment["cases"]
    }
    disputed_cases = set(manifest["alignment_disputed_case_ids"])
    if set(adjudicated_cases) != disputed_cases:
        raise ReferenceAdjudicationError("alignment adjudication output coverage drifted")
    final_cases = {}
    for case_id, old in provisional["cases"].items():
        chosen = adjudicated_cases.get(case_id, proposal_cases[case_id])
        final_cases[case_id] = {
            "base_case_id": old["base_case_id"],
            "shape": old["shape"],
            "proposition": {
                witness_id: pointwise[witness_id]["proposition_verdict"]
                for witness_id in old["proposition"]
            },
            "structured_fields": {
                witness_id: pointwise[witness_id]["structured_field_verdict"]
                for witness_id in old["structured_fields"]
            },
            "field_issues": {
                witness_id: pointwise[witness_id]["field_issue_fields"]
                for witness_id in old["field_issues"]
            },
            "pairs": [
                {
                    "witness_ids": pair["witness_ids"],
                    "relation": pair["relation"],
                    "mismatch_fields": pair["mismatch_fields"],
                }
                for pair in chosen["alignment_pairs"]
            ],
            "equivalence_groups": chosen["equivalence_groups"],
            "unpaired_witness_ids": chosen["unpaired_witness_ids"],
        }
    unresolved = {
        "pointwise_abstain_witness_ids": sorted(
            witness_id
            for witness_id, row in pointwise.items()
            if row["proposition_verdict"] == "abstain"
            or row["structured_field_verdict"] == "abstain"
        ),
        "alignment_abstain_case_ids": sorted(
            case_id
            for case_id, row in adjudicated_cases.items()
            if any(
                pair["relation"] == "abstain"
                or "abstain" in pair["checklist_decisions"].values()
                for pair in row["alignment_pairs"]
            )
        ),
    }
    reference = {
        "schema_version": REFERENCE_VERSION,
        "cases": final_cases,
        "canary_case_ids": provisional["canary_case_ids"],
        "case_count": 66,
        "witness_count": 182,
        "proposition_and_structured_field_truth_separate": True,
        "legacy_joint_support_labels_used": False,
        "source_provisional_truth_schema_version": provisional["schema_version"],
        "pointwise_disputed_witness_count": len(disputed_witnesses),
        "alignment_disputed_case_count": len(disputed_cases),
    }
    return reference, unresolved


def _change_summary(
    *, provisional: Mapping[str, Any], reference: Mapping[str, Any]
) -> dict[str, Any]:
    counts = {
        "proposition_verdict_changes": 0,
        "structured_field_verdict_changes": 0,
        "field_issue_set_changes": 0,
        "alignment_pair_changes": 0,
        "equivalence_partition_changes": 0,
        "unpaired_witness_set_changes": 0,
    }
    changed_cases = set()
    for case_id, old in provisional["cases"].items():
        new = reference["cases"][case_id]
        for witness_id in old["proposition"]:
            if old["proposition"][witness_id] != new["proposition"][witness_id]:
                counts["proposition_verdict_changes"] += 1
                changed_cases.add(case_id)
            if old["structured_fields"][witness_id] != new["structured_fields"][witness_id]:
                counts["structured_field_verdict_changes"] += 1
                changed_cases.add(case_id)
            if set(old["field_issues"][witness_id]) != set(new["field_issues"][witness_id]):
                counts["field_issue_set_changes"] += 1
                changed_cases.add(case_id)
        old_pairs = {
            tuple(pair["witness_ids"]): (pair["relation"], tuple(pair["mismatch_fields"]))
            for pair in old["pairs"]
        }
        new_pairs = {
            tuple(pair["witness_ids"]): (pair["relation"], tuple(pair["mismatch_fields"]))
            for pair in new["pairs"]
        }
        if old_pairs != new_pairs:
            counts["alignment_pair_changes"] += 1
            changed_cases.add(case_id)
        if {tuple(group) for group in old["equivalence_groups"]} != {
            tuple(group) for group in new["equivalence_groups"]
        }:
            counts["equivalence_partition_changes"] += 1
            changed_cases.add(case_id)
        if set(old["unpaired_witness_ids"]) != set(new["unpaired_witness_ids"]):
            counts["unpaired_witness_set_changes"] += 1
            changed_cases.add(case_id)
    return {**counts, "changed_case_count": len(changed_cases)}


async def run_reference_adjudication(
    *,
    v2_root: Path = DEFAULT_V2_ROOT,
    v3_root: Path = DEFAULT_V3_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    v2 = v2_root.expanduser().resolve()
    v3 = v3_root.expanduser().resolve()
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "reference adjudication terminal")
    root.mkdir(parents=True, exist_ok=True)
    manifest = build_reference_disagreement_manifest(v2_root=v2, v3_root=v3)
    manifest_path = root / "disagreement-manifest.private.json"
    _write_immutable_json(manifest_path, manifest)
    pool = _load_json(v2 / "shared-witness-pool.private.json", "witness pool")
    pointwise_input = _load_json(v2 / "pointwise-input-full.private.json", "pointwise input")
    proposal_pointwise = _load_json(
        v2 / "pointwise-output-full.private.json", "proposal pointwise"
    )
    proposal_alignment = _load_json(
        v3 / "reconciled-alignment.private.json", "proposal alignment"
    )
    provisional = _load_json(
        v2 / "provisional-calibration-truth.private.json", "provisional truth"
    )
    disputed_witnesses = set(manifest["pointwise_disputed_witness_ids"])
    pointwise_requests = []
    for index, case_ids in enumerate(manifest["pointwise_shards"]):
        witness_ids = {
            unit["witness_id"]
            for unit in pointwise_input["units"]
            if unit["case_id"] in set(case_ids)
            and unit["witness_id"] in disputed_witnesses
        }
        value = _pointwise_adjudication_input(pointwise_input, witness_ids)
        prompt = build_pointwise_support_prompt(value)
        schema = pointwise_support_output_schema(value)
        turn_name = "reference_pointwise_shard_%02d" % index
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        pointwise_requests.append(
            {
                "turn_name": turn_name,
                "input": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
                "case_ids": case_ids,
            }
        )
    spec = {
        "schema_version": REFERENCE_ADJUDICATION_SPEC_VERSION,
        "state": "frozen_before_reference_adjudication_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "pointwise_disputed_witness_count": 102,
        "pointwise_disputed_case_count": 49,
        "alignment_disputed_case_count": 18,
        "cases_per_shard_maximum": 6,
        "pointwise_turn_count": 9,
        "alignment_turn_count": 3,
        "total_turn_count": 12,
        "retry_count_per_turn": 0,
        "candidate_labels_exposed_to_adjudicator": False,
        "model_identities_exposed_to_adjudicator": False,
        "majority_voting_allowed": False,
        "adjudication_passes_per_disputed_item": 1,
        "managed_chatgpt_auth_only": True,
        "semantic_turn_maximum_primary_used_percent": 20,
        "reference_freeze_requires_zero_abstentions": True,
        "selection_authorized": False,
        "production_mutation_allowed": False,
        "source_records": {
            "v2_terminal": _record(v2 / "terminal.json"),
            "v3_terminal": _record(v3 / "terminal.json"),
            "manifest": _record(manifest_path),
        },
        "pointwise_requests": [
            {
                "turn_name": request["turn_name"],
                "case_ids": request["case_ids"],
                "input": _record(request["paths"]["input"]),
                "prompt": _record(request["paths"]["prompt"]),
                "schema": _record(request["paths"]["schema"]),
            }
            for request in pointwise_requests
        ],
    }
    spec_path = root / "reference-adjudication-spec.json"
    _write_immutable_json(spec_path, spec)
    sidecars = []
    current_turn = None
    try:
        async with client_factory() as client:
            pointwise_outputs = []
            for request in pointwise_requests:
                current_turn = request["turn_name"]
                output, sidecar, _adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=request["paths"],
                    prompt=request["prompt"],
                    schema=request["schema"],
                    base_instructions=pointwise_support_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(request["input"]["units"]),
                    output_validator=lambda value, item=request: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
            adjudicated_pointwise = _merge(pointwise_outputs, "units")
            pointwise_path = root / "adjudicated-pointwise.private.json"
            _write_immutable_json(pointwise_path, adjudicated_pointwise)
            final_pointwise = {row["witness_id"]: deepcopy(row) for row in proposal_pointwise["units"]}
            final_pointwise.update(
                {row["witness_id"]: deepcopy(row) for row in adjudicated_pointwise["units"]}
            )
            full_pointwise = {"units": list(final_pointwise.values())}
            if validate_pointwise_support_output(full_pointwise, pointwise_input):
                raise ReferenceAdjudicationError("combined pointwise reference is invalid")
            support_receipts = freeze_support_receipts(full_pointwise, pointwise_input)
            support_path = root / "adjudicated-support-receipts.private.json"
            _write_immutable_json(support_path, support_receipts)
            alignment_outputs = []
            alignment_inputs = []
            for index, case_ids in enumerate(manifest["alignment_shards"]):
                alignment_input = build_neutral_alignment_input(
                    pool, support_receipts, case_ids=case_ids
                )
                prompt = build_neutral_alignment_prompt(alignment_input)
                schema = neutral_alignment_output_schema(alignment_input)
                current_turn = "reference_alignment_shard_%02d" % index
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=alignment_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, _adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(case_ids),
                    output_validator=lambda value, item=alignment_input: validate_scoreable_calibration_alignment_output(
                        value, item
                    ),
                )
                alignment_outputs.append(output)
                alignment_inputs.append(alignment_input)
                sidecars.append(sidecar)
        merged_alignment = _merge(alignment_outputs, "cases")
        normalized_cases = []
        strict_root_errors = []
        for output, alignment_input in zip(alignment_outputs, alignment_inputs):
            strict_root_errors.extend(
                error
                for error in validate_neutral_alignment_output(output, alignment_input)
                if error.endswith("_unsupported_without_specific_root")
            )
            normalized_cases.extend(
                normalize_scoreable_alignment_output(output, alignment_input)["cases"]
            )
        normalized_alignment = {"cases": normalized_cases}
        alignment_path = root / "adjudicated-alignment.private.json"
        _write_immutable_json(alignment_path, normalized_alignment)
        reference, unresolved = _final_reference(
            provisional=provisional,
            proposal_pointwise=proposal_pointwise,
            adjudicated_pointwise=adjudicated_pointwise,
            proposal_alignment=proposal_alignment,
            adjudicated_alignment=normalized_alignment,
            manifest=manifest,
        )
        unresolved["unsupported_without_specific_root_errors"] = strict_root_errors
        unresolved_count = sum(len(values) for values in unresolved.values())
        reference_path = root / "fixture-reference-v2.private.json"
        _write_immutable_json(reference_path, reference)
        unresolved_path = root / "reference-unresolved.private.json"
        _write_immutable_json(unresolved_path, unresolved)
        changes = _change_summary(provisional=provisional, reference=reference)
        changes_path = root / "reference-change-summary.json"
        _write_immutable_json(changes_path, changes)
        accounting = _aggregate_usage(sidecars)
        reference_frozen = unresolved_count == 0
        receipt = {
            "schema_version": REFERENCE_RECEIPT_VERSION,
            "status": (
                "fixture_reference_v2_frozen"
                if reference_frozen
                else "fixture_reference_v2_unresolved"
            ),
            "created_at": now_iso(),
            "reference": _record(reference_path),
            "disagreement_manifest": _record(manifest_path),
            "change_summary": _record(changes_path),
            "unresolved": _record(unresolved_path),
            "reference_frozen": reference_frozen,
            "fresh_calibration_authorized": reference_frozen,
            "selection_authorized": False,
            "semantic_turn_count": 12,
            "semantic_retry_count": 0,
            "candidate_labels_exposed_to_adjudicator": False,
            "model_identities_exposed_to_adjudicator": False,
            "majority_voting_used": False,
            "usage": accounting["usage"],
            "usage_status": accounting["usage_status"],
            "production_mutation_performed": False,
        }
        receipt_path = root / "fixture-reference-v2-receipt.json"
        _write_immutable_json(receipt_path, receipt)
        terminal = {
            "schema_version": REFERENCE_ADJUDICATION_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "fixture_reference_v2_frozen_fresh_calibration_authorized"
                if reference_frozen
                else "fixture_reference_v2_unresolved_fail_closed"
            ),
            "spec_sha256": _sha256_file(spec_path),
            "reference_frozen": reference_frozen,
            "fresh_calibration_authorized": reference_frozen,
            "selection_authorized": False,
            "receipt": _record(receipt_path),
            "change_summary": _record(changes_path),
            "unresolved": _record(unresolved_path),
            "attempts": _attempt_records(root),
            "semantic_retry_count": 0,
            "production_mutated": False,
            **accounting,
        }
        _write_immutable_json(terminal_path, terminal)
        return terminal
    except JudgeV5DiagnosticAttemptFailed as exc:
        current_turn = exc.turn_name
        error_class = exc.error_class
    except Exception as exc:
        error_class = type(exc).__name__
    terminal = {
        "schema_version": REFERENCE_ADJUDICATION_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": current_turn,
        "error_class": error_class,
        "reference_frozen": False,
        "fresh_calibration_authorized": False,
        "selection_authorized": False,
        "attempts": _attempt_records(root),
        "semantic_retry_count": 0,
        "production_mutated": False,
    }
    _write_immutable_json(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Adjudicate fixture-reference disputes")
    parser.add_argument("--v2-root", default=str(DEFAULT_V2_ROOT))
    parser.add_argument("--v3-root", default=str(DEFAULT_V3_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_reference_adjudication(
            v2_root=Path(args.v2_root),
            v3_root=Path(args.v3_root),
            output_dir=Path(args.output_dir),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_calibration_authorized": terminal.get(
                    "fresh_calibration_authorized", False
                ),
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
