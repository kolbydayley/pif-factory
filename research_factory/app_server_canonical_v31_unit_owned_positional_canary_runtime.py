from __future__ import annotations

"""Epoch-23 two-segment canary for the unit-owned positional architecture."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_epoch22_quality_adoption as epoch22
from . import app_server_canonical_v31_single_message_compact_pointer_canary_runtime as epoch19
from . import app_server_canonical_v31_unit_owned_positional_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch23-unit-owned-positional-canary-v1"
)
SOURCE_ROOT = epoch19.DEFAULT_ROOT
PREDECESSOR_ROOT = epoch22.DEFAULT_ROOT
DIRECTIVE_PATH = PROJECT_ROOT / "automation/pif-evaluation-epoch23-unit-owned-positional-canary-v23.json"
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v23.json"
DIRECTIVE_SHA256 = "187ced5dc41d371a35c77347f3ec36d386f43b9a0a8e8ff4da66b841ea9d754b"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 23
STEP_ID = "canonical_v31_epoch23_unit_owned_positional_canary_v23"
TURN_NAME = "epoch23_unit_owned_positional_two_case_canary"
AUTHORIZATION_STATEMENT = "You have my full permission to continue. No need to seek out my approval anymore."
AUTHORIZATION_ID_DEFAULT = "kolby-epoch23-unit-owned-positional-20260719"
SELECTED_SEGMENT_IDS = (
    "seg_80fe585badf8f3d3597d0f96",
    "seg_7c027e01ed7e813b528c5c7f",
)
MAXIMUM_TOTAL_TOKENS = 32_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 10
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 22_771,
    "schema_bytes": 9_388,
    "request_bytes": 314_865,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
FULL_VISIBLE_REQUEST_BYTES = 92_601
CANARY_VISIBLE_REQUEST_BYTES = 46_436
FULL_SHARED_REFERENCE_UNIT_TARGET = 148
SOURCE_RUNTIME_LOCK_SHA256 = "c3149eef7cfbfd3de0e3bdad1229bf88ec5beca49899d71d80a23fc334fb4ffa"
SOURCE_RECEIPT_SHA256 = "9d3b0a3a2773d38b6814332c6245a54de0f357086c9d72ccf75ca3a2f3d724db"
SOURCE_REQUEST_SHA256 = "a40faa6a8e3cbfe4665ac088b87fea2e0382b34cc3e99ccd603553d4bb95685f"
PREDECESSOR_RUNTIME_LOCK_SHA256 = "ca65364267bbac099ce097f5e84153166aa7648eb3d69da5ea14087ca877a620"
PREDECESSOR_RECEIPT_SHA256 = "5125266047c804cc771856f75531972ae03426d8100ab29804c276fdea5b9927"

UnitOwnedPositionalCanaryError = one_turn.OneTurnCanaryError
UnitOwnedPositionalCanaryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return {
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_bytes": len(request["base_instructions"].encode("utf-8")),
        "schema_bytes": len(one_turn._canonical_json(request["output_schema"]).encode("utf-8")),  # noqa: SLF001
        "request_bytes": len(one_turn._canonical_json(request).encode("utf-8")),  # noqa: SLF001
        "source_unit_count": sum(len(segment["units"]) for segment in request["private_input"]["segments"]),
        "evidence_span_count": sum(len(segment["evidence_spans"]) for segment in request["private_input"]["segments"]),
        "literal_token_count": sum(
            len(unit["literal_tokens"])
            for segment in request["private_input"]["segments"]
            for unit in segment["units"]
        ),
    }


def _source_episode() -> dict[str, Any]:
    source = _load(SOURCE_ROOT / "prepared-turn/request.private.json", "epoch-19 source request")
    episode = copy.deepcopy(source["episode_context"])
    by_id = {segment["segment_id"]: segment for segment in source["private_input"]["segments"]}
    if not all(segment_id in by_id for segment_id in SELECTED_SEGMENT_IDS):
        raise UnitOwnedPositionalCanaryError("epoch-23 selected segment lineage drifted")
    episode["segments"] = [
        {
            key: copy.deepcopy(by_id[segment_id][key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment_id in SELECTED_SEGMENT_IDS
    ]
    return episode


def _build_request() -> dict[str, Any]:
    requests = adapter.prepare_episode_batches(
        _source_episode(), batch_size=3, thread_mode="new_thread"
    )
    if len(requests) != 1:
        raise UnitOwnedPositionalCanaryError("epoch-23 request count drifted")
    request = requests[0]
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or _request_shape(request) != EXPECTED_SHAPE
    ):
        raise UnitOwnedPositionalCanaryError("epoch-23 frozen request shape drifted")
    adapter.validate_prepared_request(request)
    return request


def _validate_predecessor(full_verify: bool) -> None:
    source_receipt = _load(SOURCE_ROOT / "plan-step-receipt.json", "epoch-19 receipt")
    predecessor = (
        epoch22.verify_receipt(PREDECESSOR_ROOT)
        if full_verify
        else _load(PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-22 receipt")
    )
    if (
        _record(SOURCE_ROOT / "runtime-lock.json")["sha256"] != SOURCE_RUNTIME_LOCK_SHA256
        or _record(SOURCE_ROOT / "plan-step-receipt.json")["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "terminal.json")["sha256"] != SOURCE_RECEIPT_SHA256
        or _record(SOURCE_ROOT / "prepared-turn/request.private.json")["sha256"] != SOURCE_REQUEST_SHA256
        or source_receipt.get("state") != "passed"
        or source_receipt.get("new_semantic_model_call_count") != 1
        or source_receipt.get("semantic_retry_count") != 0
        or source_receipt.get("production_mutated") is not False
        or source_receipt.get("holdout_authorized") is not False
        or _record(PREDECESSOR_ROOT / "runtime-lock.json")["sha256"]
        != PREDECESSOR_RUNTIME_LOCK_SHA256
        or _record(PREDECESSOR_ROOT / "plan-step-receipt.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or _record(PREDECESSOR_ROOT / "terminal.json")["sha256"]
        != PREDECESSOR_RECEIPT_SHA256
        or predecessor.get("state") != "rejected"
        or predecessor.get("new_semantic_model_call_count") != 0
        or predecessor.get("aggregate_semantic_model_call_count") != 3
        or predecessor.get("semantic_retry_count") != 0
        or predecessor.get("candidate_strict_full_field_macro_f1") != 0.344287
        or predecessor.get("production_mutated") is not False
        or predecessor.get("holdout_authorized") is not False
    ):
        raise UnitOwnedPositionalCanaryError("epoch-23 predecessor evidence drifted")
    _build_request()


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch19_source": {
            "runtime_lock": _record(SOURCE_ROOT / "runtime-lock.json"),
            "receipt": _record(SOURCE_ROOT / "plan-step-receipt.json"),
            "terminal": _record(SOURCE_ROOT / "terminal.json"),
            "prepared_request": _record(SOURCE_ROOT / "prepared-turn/request.private.json"),
        },
        "epoch22_quality_rejection": {
            "runtime_lock": _record(PREDECESSOR_ROOT / "runtime-lock.json"),
            "receipt": _record(PREDECESSOR_ROOT / "plan-step-receipt.json"),
            "terminal": _record(PREDECESSOR_ROOT / "terminal.json"),
            "score": _record(PREDECESSOR_ROOT / "shared-reference-score.json"),
        },
    }


def _runtime_module_paths() -> Sequence[Path]:
    return (
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.parent.__file__),
        Path(adapter.parent.compact.__file__),
        Path(adapter.parent.compact.local.__file__),
        Path(adapter.parent.compact.local.bounded_span.__file__),
        Path(adapter.parent.compact.local.pointer.__file__),
        Path(adapter.base.__file__),
        Path(one_turn.__file__),
        Path(epoch22.__file__),
    )


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    if dict(value) != expected:
        raise UnitOwnedPositionalCanaryError("epoch-23 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch23_unit_owned_positional_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authorized_by") != "kolby"
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("expected_receipt_path")
        != str((DEFAULT_ROOT / "plan-step-receipt.json").resolve())
        or expected.get("canary_contract", {}).get("selected_segment_ids")
        != list(SELECTED_SEGMENT_IDS)
        or expected.get("canary_contract", {}).get("semantic_model_call_cap") != 1
        or expected.get("canary_contract", {}).get("semantic_retry_cap") != 0
        or expected.get("canary_contract", {}).get("measured_total_token_ceiling")
        != MAXIMUM_TOTAL_TOKENS
        or expected.get("architecture_ranking", [])[0].get("selected") is not True
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed") is not False
    ):
        raise UnitOwnedPositionalCanaryError("epoch-23 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    event_count = None
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-23 semantic fidelity")
        event_count = fidelity.get("emitted_event_count")
    input_projection = math.ceil(
        int(usage["input_tokens"])
        * FULL_VISIBLE_REQUEST_BYTES
        / CANARY_VISIBLE_REQUEST_BYTES
    ) if outcome["state"] != "waiting" else None
    output_projection = None
    full_total_projection = None
    if isinstance(event_count, int) and event_count > 0:
        output_projection = math.ceil(
            int(usage["output_tokens"])
            * FULL_SHARED_REFERENCE_UNIT_TARGET
            / event_count
        )
        full_total_projection = input_projection + output_projection
    return {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "diagnostic_event_count": event_count,
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_at_148_units": output_projection,
        "full_six_case_total_token_projection_at_148_units": full_total_projection,
        "production_token_ceiling_for_future_full_run": 73_926,
        "quality_measured_by_this_step": False,
        "judge_required_before_full_run_promotion": True,
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
    stage="unit_owned_positional_two_case_extraction_canary",
    architecture_class="one_turn_unit_owned_positional_full_canonical_schema_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_two_case_extraction_canary",
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
    pass_next_action="freeze_two_case_full_event_ab_ba_quality_and_cost_projection",
    reject_next_action="reject_unit_owned_positional_architecture_without_field_repair",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-23 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE:
        raise UnitOwnedPositionalCanaryError("epoch-23 runtime request shape drifted")
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
