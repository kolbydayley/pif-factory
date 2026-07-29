from __future__ import annotations

"""Epoch-25 exact-token-ID canary for unit-reviewed canonical extraction."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_owned_ordinal_canary_runtime as epoch24
from . import app_server_canonical_v31_unit_owned_token_id_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch25-unit-owned-token-id-canary-v1"
)
PREDECESSOR_ROOT = epoch24.DEFAULT_ROOT
EPOCH23_ROOT = epoch24.PREDECESSOR_ROOT
DIRECTIVE_PATH = PROJECT_ROOT / "automation/pif-evaluation-epoch25-unit-owned-token-id-canary-v25.json"
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v25.json"
DIRECTIVE_SHA256 = "1e4078d726da495254d190d511440f380bc2a13c31f325c4db53cfe8ef031124"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 25
STEP_ID = "canonical_v31_epoch25_unit_owned_token_id_canary_v25"
TURN_NAME = "epoch25_unit_owned_token_id_two_case_canary"
AUTHORIZATION_STATEMENT = "You have my full permission to continue. No need to seek out my approval anymore."
AUTHORIZATION_ID_DEFAULT = "kolby-epoch25-unit-owned-token-id-20260719"
SELECTED_SEGMENT_IDS = epoch24.SELECTED_SEGMENT_IDS
MAXIMUM_TOTAL_TOKENS = 32_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 23_614,
    "schema_bytes": 5_841,
    "request_bytes": 312_589,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
FULL_VISIBLE_REQUEST_BYTES = 71_669
CANARY_VISIBLE_REQUEST_BYTES = 43_732
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926
PREDECESSOR_RUNTIME_LOCK_SHA256 = "8d472a3edce4f3b7e4c7bcf43b632afed8131f70fc25252226430a997a4f9138"
PREDECESSOR_RECEIPT_SHA256 = "e9641bf69b1e21e341e71a930696a2589a277da362294b6e6ec0a1cb64a285f8"
PREDECESSOR_SIDECAR_SHA256 = "9870680d1ec1203bf45baeb4a6a49f530c1867ee668858efb14f789e6931adcf"
PREDECESSOR_OUTPUT_SHA256 = "874f6e10e8015cbd06a0e6f8296051d064c5c06ea27406c604710a47388588eb"
EPOCH23_RECEIPT_SHA256 = "d4ab5d93e68d7767cd73c95693caf287112591e2ca6bcb543b465ef1aaeb90bc"
EPOCH23_SIDECAR_SHA256 = "0e16d394060a1188328b1f63e4fad3e26cd19a45d5f58225dd8ee7a9846bf78b"

UnitOwnedTokenIdCanaryError = one_turn.OneTurnCanaryError
UnitOwnedTokenIdCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return {
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_bytes": len(request["base_instructions"].encode("utf-8")),
        "schema_bytes": len(
            one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
        ),
        "request_bytes": len(one_turn._canonical_json(request).encode("utf-8")),  # noqa: SLF001
        "source_unit_count": sum(
            len(segment["units"])
            for segment in request["private_input"]["segments"]
        ),
        "evidence_span_count": sum(
            len(segment["evidence_spans"])
            for segment in request["private_input"]["segments"]
        ),
        "literal_token_count": sum(
            len(unit["literal_tokens"])
            for segment in request["private_input"]["segments"]
            for unit in segment["units"]
        ),
    }


def _build_request() -> dict[str, Any]:
    requests = adapter.prepare_episode_batches(
        epoch24.epoch23._source_episode(),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(requests) != 1:
        raise UnitOwnedTokenIdCanaryError("epoch-25 request count drifted")
    request = requests[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("semantic_integrity", {}).get("token_id_protocol_version")
        != adapter.TOKEN_ID_PROTOCOL_VERSION
        or request.get("semantic_integrity", {}).get(
            "deterministic_owner_container_normalization_only"
        )
        is not True
        or _request_shape(request) != EXPECTED_SHAPE
    ):
        raise UnitOwnedTokenIdCanaryError("epoch-25 frozen request shape drifted")
    epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
    adapter.validate_prepared_request(request)
    return request


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("epoch24_runtime_lock", PREDECESSOR_ROOT / "runtime-lock.json"),
    ("epoch24_receipt", PREDECESSOR_ROOT / "plan-step-receipt.json"),
    ("epoch24_terminal", PREDECESSOR_ROOT / "terminal.json"),
    ("epoch24_authorization", PREDECESSOR_ROOT / "operator-authorization.json"),
    ("epoch24_attempt", PREDECESSOR_ROOT / "turn/semantic-attempt.json"),
    ("epoch24_sidecar", PREDECESSOR_ROOT / "turn/sidecar.json"),
    ("epoch24_output", PREDECESSOR_ROOT / "turn/output.private.json"),
    ("epoch24_rejection", PREDECESSOR_ROOT / "semantic-rejection.json"),
    ("epoch24_request", PREDECESSOR_ROOT / "prepared-turn/request.private.json"),
    ("epoch23_receipt", EPOCH23_ROOT / "plan-step-receipt.json"),
    ("epoch23_sidecar", EPOCH23_ROOT / "turn/sidecar.json"),
)


def _frozen_predecessor_evidence() -> list[dict[str, Any]]:
    return [{"role": role, **_record(path)} for role, path in _PREDECESSOR_FILES]


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch24_measured_structural_rejection": {
            role.removeprefix("epoch24_"): _record(path)
            for role, path in _PREDECESSOR_FILES
            if role.startswith("epoch24_")
        },
        "epoch23_interrupted_unknown_usage_attempt": {
            role.removeprefix("epoch23_"): _record(path)
            for role, path in _PREDECESSOR_FILES
            if role.startswith("epoch23_")
        },
    }


def _epoch24_diagnostic() -> dict[str, int]:
    ordinal = adapter.ordinal
    positional = ordinal.positional
    request = _load(PREDECESSOR_ROOT / "prepared-turn/request.private.json", "epoch-24 request")
    output = _load(PREDECESSOR_ROOT / "turn/output.private.json", "epoch-24 output")
    wire = ordinal._positional_wire(request, output)  # noqa: SLF001
    normalized = copy.deepcopy(wire)
    canonical = ordinal._parent_request(request)  # noqa: SLF001
    segment_schema = canonical["output_schema"]["properties"]["segments"]["items"]
    event_schema = segment_schema["properties"]["discourse_events"]["items"]
    concept_schema = segment_schema["properties"]["concept_candidates"]["items"]
    event_count = concept_count = event_mismatches = concept_mismatches = 0
    for segment_index, (source, row) in enumerate(
        zip(request["private_input"]["segments"], wire["s"])
    ):
        spans = source["evidence_spans"]
        units = source["units"]
        owner = {unit["unit_id"]: index for index, unit in enumerate(units)}
        new_rows = [[[], []] for _ in units]
        for unit_index, unit_row in enumerate(row[2]):
            for kind, (items, schema) in enumerate(
                ((unit_row[0], event_schema), (unit_row[1], concept_schema))
            ):
                for item in items:
                    decoded = positional._decode_value(item, schema, "diagnostic")  # noqa: SLF001
                    pointer = positional._pointer_index(  # noqa: SLF001
                        decoded["evidence_span_id"], spans, "diagnostic"
                    )
                    target = owner[spans[pointer]["evidence_start_unit_id"]]
                    new_rows[target][kind].append(copy.deepcopy(item))
                    if kind == 0:
                        event_count += 1
                        event_mismatches += int(target != unit_index)
                    else:
                        concept_count += 1
                        concept_mismatches += int(target != unit_index)
        normalized["s"][segment_index][2] = new_rows
    raw = positional._decode_output(request, normalized)  # noqa: SLF001
    nonnull_ranges = invalid_ranges = 0
    for source, segment in zip(request["private_input"]["segments"], raw["segments"]):
        units = source["units"]
        spans = {span["evidence_span_id"]: span for span in source["evidence_spans"]}
        for event in segment["discourse_events"]:
            span = spans[event["evidence_span_id"]]
            for field in adapter.METRIC_RANGE_FIELDS:
                value = event["metric"][field]
                if value is None:
                    continue
                nonnull_ranges += 1
                invalid = (
                    not isinstance(value, list)
                    or len(value) != 4
                    or any(isinstance(item, bool) or not isinstance(item, int) for item in value)
                )
                if not invalid:
                    start_unit, start_index, end_unit, end_index = value
                    invalid = not (
                        0 <= start_unit < len(units)
                        and 0 <= end_unit < len(units)
                        and 0 <= start_index < len(units[start_unit]["literal_tokens"])
                        and 0 <= end_index < len(units[end_unit]["literal_tokens"])
                    )
                    if not invalid:
                        start = units[start_unit]["literal_tokens"][start_index]["start_char"]
                        end = units[end_unit]["literal_tokens"][end_index]["end_char"]
                        invalid = start < span["start_char"] or end > span["end_char"]
                invalid_ranges += int(invalid)
    input_projection = math.ceil(
        14_405 * FULL_VISIBLE_REQUEST_BYTES / CANARY_VISIBLE_REQUEST_BYTES
    )
    return {
        "event_count": event_count,
        "concept_count": concept_count,
        "event_owner_container_mismatch_count": event_mismatches,
        "concept_owner_container_mismatch_count": concept_mismatches,
        "nonnull_metric_range_count": nonnull_ranges,
        "invalid_numeric_metric_range_count": invalid_ranges,
        "measured_total_tokens": 29_079,
        "case_count_scaled_full_run_token_projection": input_projection + 14_674 * FULL_CASE_SCALE,
    }


def _validate_predecessor(full_verify: bool) -> None:
    predecessor = (
        epoch24.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-24 receipt")
    )
    epoch23_receipt = _load(EPOCH23_ROOT / "plan-step-receipt.json", "epoch-23 receipt")
    if (
        _record(PREDECESSOR_ROOT / "runtime-lock.json")["sha256"]
        != PREDECESSOR_RUNTIME_LOCK_SHA256
        or _record(PREDECESSOR_ROOT / "plan-step-receipt.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "turn/sidecar.json")["sha256"]
        != PREDECESSOR_SIDECAR_SHA256
        or _record(PREDECESSOR_ROOT / "turn/output.private.json")["sha256"]
        != PREDECESSOR_OUTPUT_SHA256
        or predecessor.get("state") != "rejected"
        or predecessor.get("terminal_reason")
        != "epoch24_one_turn_canary_semantic_output_rejected"
        or predecessor.get("diagnostic", {}).get("diagnostic_path")
        != "concept is not owned by its evidence-start unit"
        or predecessor.get("new_semantic_model_call_count") != 1
        or predecessor.get("new_unknown_usage_turn_count") != 0
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("new_measured_usage", {}).get("total_tokens") != 29_079
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
        or _record(EPOCH23_ROOT / "plan-step-receipt.json")["sha256"]
        != EPOCH23_RECEIPT_SHA256
        or _record(EPOCH23_ROOT / "turn/sidecar.json")["sha256"]
        != EPOCH23_SIDECAR_SHA256
        or epoch23_receipt.get("state") != "waiting"
        or epoch23_receipt.get("new_semantic_model_call_count") != 1
        or epoch23_receipt.get("new_unknown_usage_turn_count") != 1
        or _epoch24_diagnostic()
        != json.loads(DIRECTIVE_PATH.read_text())["measured_predecessor_diagnostic"]
    ):
        raise UnitOwnedTokenIdCanaryError("epoch-25 predecessor evidence drifted")
    _build_request()


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.ordinal.__file__),
        Path(adapter.ordinal.positional.__file__),
        Path(adapter.ordinal.parent.__file__),
        Path(adapter.ordinal.parent.compact.__file__),
        Path(adapter.ordinal.parent.compact.local.__file__),
        Path(adapter.ordinal.parent.compact.local.bounded_span.__file__),
        Path(adapter.ordinal.parent.compact.local.pointer.__file__),
        Path(adapter.base.__file__),
        Path(one_turn.__file__),
        Path(epoch24.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    canary = expected.get("canary_contract", {})
    ranking = expected.get("architecture_ranking", [])
    if dict(value) != expected:
        raise UnitOwnedTokenIdCanaryError("epoch-25 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch25_unit_owned_token_id_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authorized_by") != "kolby"
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected.get("frozen_predecessor_evidence")
        != _frozen_predecessor_evidence()
        or expected.get("measured_predecessor_diagnostic") != _epoch24_diagnostic()
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or canary.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("epoch24_replay_allowed") is not False
        or canary.get("model_selects_exact_literal_token_ids") is not True
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
    ):
        raise UnitOwnedTokenIdCanaryError("epoch-25 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-25 semantic fidelity")
    input_projection = (
        math.ceil(
            int(usage["input_tokens"])
            * FULL_VISIBLE_REQUEST_BYTES
            / CANARY_VISIBLE_REQUEST_BYTES
        )
        if outcome["state"] != "waiting"
        else None
    )
    output_projection = (
        int(usage["output_tokens"]) * FULL_CASE_SCALE
        if outcome["state"] != "waiting"
        else None
    )
    full_projection = (
        input_projection + output_projection
        if input_projection is not None and output_projection is not None
        else None
    )
    return {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "event_owner_container_normalization_count": fidelity.get(
            "event_owner_container_normalization_count"
        ),
        "concept_owner_container_normalization_count": fidelity.get(
            "concept_owner_container_normalization_count"
        ),
        "exact_metric_token_pair_count": fidelity.get("exact_metric_token_pair_count"),
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_by_case_count": output_projection,
        "full_six_case_total_token_projection_by_case_count": full_projection,
        "full_six_case_cost_projection_pass": (
            full_projection <= FULL_PRODUCTION_TOKEN_CEILING
            if full_projection is not None
            else False
        ),
        "production_token_ceiling_for_future_full_run": FULL_PRODUCTION_TOKEN_CEILING,
        "quality_measured_by_this_step": False,
        "judge_required_before_full_run_promotion": True,
        "epoch23_predecessor_semantic_model_call_count": 1,
        "epoch23_predecessor_unknown_usage_turn_count": 1,
        "epoch24_predecessor_semantic_model_call_count": 1,
        "epoch24_predecessor_measured_total_tokens": 29_079,
        "aggregate_architecture_semantic_model_call_count": 3,
        "aggregate_architecture_unknown_usage_turn_count": 1,
        "aggregate_architecture_measured_total_tokens": 29_079 + total,
        "epoch23_replayed": False,
        "epoch24_replayed": False,
        "prior_semantic_output_reuse_count": 0,
        "deterministic_semantic_pruning": False,
    }


SPEC = one_turn.OneTurnCanarySpec(
    project_root=PROJECT_ROOT,
    default_root=DEFAULT_ROOT,
    directive_path=DIRECTIVE_PATH,
    plan_path=PLAN_PATH,
    directive_sha256=DIRECTIVE_SHA256,
    thread_id=THREAD_ID,
    plan_epoch=PLAN_EPOCH,
    step_id=STEP_ID,
    turn_name=TURN_NAME,
    stage="unit_owned_exact_token_id_two_case_extraction_canary",
    architecture_class="one_turn_unit_owned_exact_token_id_full_canonical_schema_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_two_case_token_id_canary",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_TOTAL_TOKENS,
    maximum_wall_seconds=MAXIMUM_WALL_SECONDS,
    minimum_remaining_reserve_percent=MINIMUM_REMAINING_RESERVE_PERCENT,
    capacity_safety_margin_percent=CAPACITY_SAFETY_MARGIN_PERCENT,
    quota_points_per_million_tokens=QUOTA_POINTS_PER_MILLION_TOKENS,
    authorization_window_seconds=AUTHORIZATION_WINDOW_SECONDS,
    adapter=adapter,
    validate_directive=_validate_directive,
    validate_predecessor=_validate_predecessor,
    build_request=_build_request,
    predecessor_records=_predecessor_records,
    runtime_module_paths=_runtime_module_paths,
    receipt_metadata=_receipt_metadata,
    pass_next_action="freeze_two_case_full_event_ab_ba_quality_if_cost_projection_passes",
    reject_next_action="reject_unit_owned_token_id_architecture_without_field_repair",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-25 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE:
        raise UnitOwnedTokenIdCanaryError("epoch-25 runtime request shape drifted")
    epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
    return {**copy.deepcopy(dict(status)), **shape}


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.verify_runtime(SPEC, root), root)


def authorize_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str, now: Any = None
) -> dict[str, Any]:
    return one_turn.authorize(
        SPEC,
        root=root,
        operator_authorization_id=operator_authorization_id,
        now=now,
    )


async def execute_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str
) -> dict[str, Any]:
    return await one_turn.execute(
        SPEC,
        root=root,
        operator_authorization_id=operator_authorization_id,
    )


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_receipt(SPEC, root)


def status_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.status(SPEC, root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare")
    authorize_parser = sub.add_parser("authorize")
    authorize_parser.add_argument("--operator-authorization-id", required=True)
    execute_parser = sub.add_parser("execute")
    execute_parser.add_argument("--operator-authorization-id", required=True)
    sub.add_parser("verify-runtime")
    sub.add_parser("verify-receipt")
    sub.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare":
        value = prepare_canary(args.root)
    elif args.command == "authorize":
        value = authorize_canary(
            root=args.root,
            operator_authorization_id=args.operator_authorization_id,
        )
    elif args.command == "execute":
        value = asyncio.run(
            execute_canary(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
            )
        )
    elif args.command == "verify-runtime":
        value = verify_runtime(args.root)
    elif args.command == "verify-receipt":
        value = verify_receipt(args.root)
    else:
        value = status_canary(args.root)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
