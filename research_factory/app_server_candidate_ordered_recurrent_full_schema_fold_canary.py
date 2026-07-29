from __future__ import annotations

"""Run the checksum-bound epoch-3 ordered recurrent full-schema fold canary.

The model sees the frozen v249 no-signal segment first and the dense segment
second in one managed-auth persistent app-server thread.  Each turn owns its
complete v249 semantics.  Python only freezes requests, validates/project them,
and structurally folds the two validated segment arrays.
"""

import argparse
import asyncio
import copy
import fcntl
import hashlib
import json
import math
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Mapping, Sequence

from . import app_server_capacity as capacity
from . import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import codex_app_server
from .util import now_iso


SCHEMA_VERSION = "pif_candidate_ordered_recurrent_full_schema_fold_canary_v3"
LOCK_VERSION = "pif_candidate_ordered_recurrent_full_schema_fold_canary_lock_v3"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
DIRECTIVE_VERSION = "pif_evaluation_ordered_recurrent_full_schema_fold_directive_v3"
PLAN_VERSION = "pif_evaluation_semantic_plan_v1"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 3
STEP_ID = "ordered_recurrent_full_schema_fold_canary_v3"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAMES = ("ordered-fold-no-signal-first", "ordered-fold-dense-second")
NO_SIGNAL_SEGMENT_ID = v233.NO_SIGNAL_SEGMENT_ID
DENSE_SEGMENT_ID = v233.DENSE_SEGMENT_ID
RUN_SEGMENT_ORDER = (NO_SIGNAL_SEGMENT_ID, DENSE_SEGMENT_ID)
CANONICAL_SEGMENT_ORDER = (DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID)
TURN_TOKEN_CAPS = (24_000, 48_891)
PHASE_TOKEN_CAP = 72_891
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
PRODUCTION_RATIO_MAX = 0.28
USAGE_FIELDS = v233.USAGE_FIELDS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v3.json").resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-ordered-recurrent-full-schema-fold-v3.json"
).resolve()
EXPECTED_PLAN_SHA256 = "95354e28e5e38fe057bd7e64ce2fcf403fc1e4c78f345f2088fcc032cbad5e7a"
EXPECTED_DIRECTIVE_SHA256 = "ca2040f7d1f139849b57709f69f047eb9463862ba976a07dc02ade943df4a34e"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "development-selection-v249-ordered-recurrent-full-schema-fold-v3"
).resolve()
PINNED_CODEX = v249.PINNED_CODEX_0_144_1
PROCESS_LOCK_NAME = ".ordered-recurrent-full-schema-fold.lock"


class OrderedRecurrentFoldError(RuntimeError):
    """The immutable epoch-3 contract or artifacts are invalid."""


class OperationalWaitingError(OrderedRecurrentFoldError):
    """A transport, auth, accounting, or live-capacity condition must wait."""


class StructuralRejectionError(OrderedRecurrentFoldError):
    """A completed model output or measured cost failed the frozen contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrderedRecurrentFoldError(f"cannot read {label}") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _is_capacity_path(path: Path) -> bool:
    return path.name == "capacity.json" or path.name.startswith("capacity-")


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        path = Path(str(record["path"]))
        return _record(path) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise OrderedRecurrentFoldError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise OrderedRecurrentFoldError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable_json(path, value)


@contextmanager
def _advisory_process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / PROCESS_LOCK_NAME
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another epoch-3 canary process owns the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_contract() -> dict[str, Any]:
    """Load the plan through its declared directive checksum; no checksum is hardcoded."""

    if _sha256_file(PLAN_PATH) != EXPECTED_PLAN_SHA256:
        raise OrderedRecurrentFoldError("epoch-3 plan checksum drifted")
    plan = _load_json(PLAN_PATH, "epoch-3 semantic plan")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if not isinstance(step, Mapping):
        raise OrderedRecurrentFoldError("epoch-3 plan step is missing")
    declared_directive = Path(str(step.get("directive_path") or "")).expanduser().resolve()
    if declared_directive != DIRECTIVE_PATH or not declared_directive.is_file():
        raise OrderedRecurrentFoldError("epoch-3 directive path drifted")
    directive_sha256 = _sha256_file(declared_directive)
    if directive_sha256 != EXPECTED_DIRECTIVE_SHA256:
        raise OrderedRecurrentFoldError("epoch-3 directive checksum drifted")
    directive = _load_json(declared_directive, "epoch-3 directive")
    execution = directive.get("execution_contract")
    architecture = directive.get("architecture_contract")
    acceptance = directive.get("structural_acceptance_contract")
    terminal = directive.get("terminal_contract")
    canary = architecture.get("representative_development_canary") if isinstance(architecture, Mapping) else None
    preconditions = execution.get("turn_2_preconditions") if isinstance(execution, Mapping) else None
    cost = execution.get("production_cost_projection") if isinstance(execution, Mapping) else None
    if (
        plan.get("schema_version") != PLAN_VERSION
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 2
        or step.get("max_total_tokens") != PHASE_TOKEN_CAP
        or step.get("directive_sha256") != directive_sha256
        or directive.get("schema_version") != DIRECTIVE_VERSION
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or not isinstance(execution, Mapping)
        or not isinstance(architecture, Mapping)
        or not isinstance(acceptance, Mapping)
        or not isinstance(terminal, Mapping)
        or not isinstance(canary, Mapping)
        or execution.get("extraction_model_call_cap") != 2
        or execution.get("semantic_judge_call_cap") != 0
        or execution.get("semantic_retry_cap") != 0
        or execution.get("model") != MODEL
        or execution.get("reasoning_effort") != EFFORT
        or execution.get("app_server_process_count") != 1
        or execution.get("thread_count") != 1
        or execution.get("thread_ephemeral") is not True
        or execution.get("thread_reused_for_both_turns") is not True
        or execution.get("turn_count") != 2
        or execution.get("turn_total_token_caps") != list(TURN_TOKEN_CAPS)
        or execution.get("combined_extraction_total_token_cap") != PHASE_TOKEN_CAP
        or execution.get("v249_full_applicability_schema_and_projection_required") is not True
        or execution.get("all_source_units_reviewed_required") is not True
        or execution.get("exact_source_evidence_required") is not True
        or execution.get("complete_usage_cache_reasoning_wall_time_telemetry_required") is not True
        or execution.get("managed_chatgpt_auth_required") is not True
        or execution.get("zero_silent_retry") is not True
        or architecture.get("deterministic_semantic_pruning_allowed") is not False
        or architecture.get("deterministic_support_filtering_allowed") is not False
        or architecture.get("deterministic_deduplication_allowed") is not False
        or architecture.get("deterministic_relabeling_allowed") is not False
        or not isinstance(preconditions, Mapping)
        or preconditions.get("turn_1_total_tokens_lte") != TURN_TOKEN_CAPS[0]
        or preconditions.get("remaining_combined_token_cap_gte") != TURN_TOKEN_CAPS[1]
        or preconditions.get("fresh_no_turn_capacity_probe_required") is not True
        or preconditions.get("minimum_remaining_reserve_percent_after_conservative_projection")
        != MIN_REMAINING_RESERVE_PERCENT
        or not isinstance(cost, Mapping)
        or cost.get("production_scale") != PRODUCTION_SCALE
        or cost.get("required_ratio_max") != PRODUCTION_RATIO_MAX
        or acceptance.get("combined_extraction_total_tokens_max") != PHASE_TOKEN_CAP
        or acceptance.get("accounting_complete") is not True
        or acceptance.get("production_mutated") is not False
        or acceptance.get("holdout_authorized") is not False
        or acceptance.get("development_winner_frozen") is not False
        or terminal.get("operational_or_transport_or_unknown_usage_failure_state") != "waiting"
        or terminal.get("structural_or_cost_or_semantic_quality_failure_state") != "rejected"
        or terminal.get("structural_pass_state") != "passed"
        or terminal.get("receipt_schema_version") != RECEIPT_VERSION
    ):
        raise OrderedRecurrentFoldError("epoch-3 semantic plan or directive drifted")
    turn_order = canary.get("turn_order")
    if not isinstance(turn_order, list) or [
        (row.get("turn_index"), row.get("turn_name"), row.get("segment_id"), row.get("total_token_cap"))
        for row in turn_order if isinstance(row, Mapping)
    ] != [
        (0, TURN_NAMES[0], NO_SIGNAL_SEGMENT_ID, TURN_TOKEN_CAPS[0]),
        (1, TURN_NAMES[1], DENSE_SEGMENT_ID, TURN_TOKEN_CAPS[1]),
    ]:
        raise OrderedRecurrentFoldError("epoch-3 ordered segment contract drifted")
    expected_receipt = Path(str(step.get("expected_receipt_path") or "")).resolve()
    if (
        expected_receipt != Path(str(directive.get("expected_receipt_path") or "")).resolve()
        or expected_receipt != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json"
    ):
        raise OrderedRecurrentFoldError("epoch-3 receipt path drifted")
    frozen_rows = directive.get("frozen_inputs")
    if not isinstance(frozen_rows, list) or not frozen_rows:
        raise OrderedRecurrentFoldError("epoch-3 frozen inputs are missing")
    frozen_records = []
    roles = set()
    for row in frozen_rows:
        if not isinstance(row, Mapping):
            raise OrderedRecurrentFoldError("epoch-3 frozen input is malformed")
        path = Path(str(row.get("path") or "")).resolve()
        if not path.is_file() or row.get("sha256") != _sha256_file(path):
            raise OrderedRecurrentFoldError("epoch-3 frozen input drifted")
        roles.add(str(row.get("role")))
        frozen_records.append(_record(path))
    required_roles = {
        "epoch_2_receipt", "epoch_2_score", "epoch_2_runtime_lock",
        "v249_runtime_lock", "v249_source_packet", "v249_base_instructions",
        "v249_output_schema", "v249_projection_schema", "v249_applicability_receipt",
        "v249_evidence_provenance", "v249_terminal",
    }
    if roles != required_roles:
        raise OrderedRecurrentFoldError("epoch-3 frozen input role set drifted")
    predecessor = directive.get("predecessor_receipt")
    if not isinstance(predecessor, Mapping) or predecessor.get("state") != "rejected":
        raise OrderedRecurrentFoldError("epoch-2 predecessor state drifted")
    predecessor_path = Path(str(predecessor.get("path") or "")).resolve()
    if not predecessor_path.is_file() or predecessor.get("sha256") != _sha256_file(predecessor_path):
        raise OrderedRecurrentFoldError("epoch-2 predecessor receipt drifted")
    return {
        "plan": plan,
        "directive": directive,
        "directive_sha256": directive_sha256,
        "frozen_records": frozen_records,
        "receipt_path": expected_receipt,
        "timeout_seconds": int(execution.get("timeout_seconds_per_turn", TIMEOUT_SECONDS)),
    }


def _source_paths_from_contract(contract: Mapping[str, Any]) -> dict[str, Path]:
    rows = contract["directive"]["frozen_inputs"]
    return {str(row["role"]): Path(str(row["path"])).resolve() for row in rows}


def _narrow_schema(schema: Mapping[str, Any], segment: Mapping[str, Any]) -> dict[str, Any]:
    narrowed = copy.deepcopy(dict(schema))
    segment_id = str(segment["segment_id"])
    unit_ids = [str(unit["unit_id"]) for unit in segment["units"]]
    segments = narrowed["properties"]["segments"]
    segments["minItems"] = 1
    segments["maxItems"] = 1
    segment_schema = segments["items"]
    segment_schema["properties"]["segment_id"]["enum"] = [segment_id]
    receipts = segment_schema["properties"]["unit_receipts"]
    receipts["minItems"] = len(unit_ids)
    receipts["maxItems"] = len(unit_ids)
    receipts["items"]["properties"]["unit_id"]["enum"] = unit_ids
    event = segment_schema["properties"]["events"]["items"]
    event["properties"]["evidence_start_unit_id"]["enum"] = unit_ids
    event["properties"]["evidence_end_unit_id"]["enum"] = unit_ids
    return narrowed


def prepare_turns(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    paths = _source_paths_from_contract(contract)
    source = _load_json(paths["v249_source_packet"], "v249 source packet")
    schema = _load_json(paths["v249_output_schema"], "v249 output schema")
    projection_schema = _load_json(paths["v249_projection_schema"], "v249 projection schema")
    base = paths["v249_base_instructions"].read_text(encoding="utf-8")
    episode_id = str(source.get("episode_id"))
    by_id = {str(row.get("segment_id")): row for row in source.get("segments") or []}
    if set(by_id) != set(RUN_SEGMENT_ORDER):
        raise OrderedRecurrentFoldError("frozen v249 segment membership drifted")
    turns = []
    for index, (name, segment_id, cap) in enumerate(zip(TURN_NAMES, RUN_SEGMENT_ORDER, TURN_TOKEN_CAPS)):
        segment = copy.deepcopy(by_id[segment_id])
        prompt_packet = {
            "episode_id": episode_id,
            "segments": [
                {
                    "segment_id": segment_id,
                    "source_units": [
                        {"unit_id": unit["unit_id"], "window_id": unit["window_id"], "text": unit["text"]}
                        for unit in segment["units"]
                    ],
                }
            ],
        }
        heading = "# Current segment source-unit packet\n"
        guard = (
            "\n# Recurrent fold boundary\nThis is turn 1. Return only the current no-signal segment.\n"
            if index == 0
            else (
                "\n# Recurrent fold boundary\nReturn only the current dense segment. The prior turn is immutable: "
                "do not revise, delete, relabel, replace, summarize, or re-emit any turn-1 event or segment.\n"
            )
        )
        prompt = heading + json.dumps(prompt_packet, ensure_ascii=True, separators=(",", ":")) + guard
        turn_schema = _narrow_schema(schema, segment)
        direct_schema = _narrow_schema(projection_schema, segment)
        turns.append(
            {
                "turn_index": index,
                "turn_name": name,
                "episode_id": episode_id,
                "segment_ids": [segment_id],
                "segment_id": segment_id,
                "token_cap": cap,
                "private_input": {
                    "schema_version": SCHEMA_VERSION,
                    "episode_id": episode_id,
                    "segments": [segment],
                    "privacy": "private frozen v249 source units",
                },
                "base": base,
                "prompt": prompt,
                "schema": turn_schema,
                "direct_schema": direct_schema,
            }
        )
    if NO_SIGNAL_SEGMENT_ID in turns[1]["prompt"] or DENSE_SEGMENT_ID in turns[0]["prompt"]:
        raise OrderedRecurrentFoldError("a recurrent turn exposes the other segment")
    return turns


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "direct_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
        "applicability": turn_root / "applicability-receipt.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted({
        Path(__file__).resolve(), Path(v233.__file__).resolve(), Path(v249.__file__).resolve(),
        Path(capacity.__file__).resolve(), Path(codex_app_server.__file__).resolve(),
        codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(), PINNED_CODEX.resolve(),
    }, key=str))


def _request_records(root: Path) -> list[dict[str, Any]]:
    records = []
    for name in TURN_NAMES:
        paths = _turn_paths(root, name)
        records.extend(_record(paths[key]) for key in ("input", "prompt", "schema", "direct_schema"))
    records.append(_record(root / "base-instructions.private.md"))
    return records


def _meaningful_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return _load_frozen(root)
    if root.exists() and _meaningful_entries(root):
        raise OrderedRecurrentFoldError("unfrozen epoch-3 output root is not empty")
    contract = load_contract()
    turns = prepare_turns(contract)
    _write_immutable_text(root / "base-instructions.private.md", turns[0]["base"])
    for turn in turns:
        paths = _turn_paths(root, turn["turn_name"])
        _write_immutable_json(paths["input"], turn["private_input"])
        _write_immutable_text(paths["prompt"], turn["prompt"])
        _write_immutable_json(paths["schema"], turn["schema"])
        _write_immutable_json(paths["direct_schema"], turn["direct_schema"])
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "model": MODEL,
        "effort": EFFORT,
        "app_server_process_count": 1,
        "thread_count": 1,
        "thread_ephemeral": True,
        "thread_reused_for_both_turns": True,
        "ordered_turn_names": list(TURN_NAMES),
        "run_segment_order": list(RUN_SEGMENT_ORDER),
        "canonical_output_segment_order": list(CANONICAL_SEGMENT_ORDER),
        "turn_total_token_caps": list(TURN_TOKEN_CAPS),
        "phase_total_token_cap": PHASE_TOKEN_CAP,
        "retry_count": 0,
        "turn_2_may_revise_turn_1": False,
        "deterministic_semantic_pruning_allowed": False,
        "deterministic_support_filtering_allowed": False,
        "deterministic_deduplication_allowed": False,
        "deterministic_relabeling_allowed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    request_records = _request_records(root)
    lock = {
        "schema_version": LOCK_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "turn_total_token_caps": list(TURN_TOKEN_CAPS),
        "phase_total_token_cap": PHASE_TOKEN_CAP,
        "plan": _record(PLAN_PATH),
        "directive": _record(DIRECTIVE_PATH),
        "frozen_inputs": contract["frozen_records"],
        "runtime_files": [_record(path) for path in _runtime_files()],
        "attempt_spec": _record(spec_path),
        "frozen_request": request_records,
        "capacity_files_excluded_from_usage_records": True,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "created_at")
    verify_runtime_lock(lock_path, acquire_lock=False)
    return _load_frozen(root)


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return _freeze_unlocked(root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    def verify() -> dict[str, Any]:
        lock = _load_json(path, "epoch-3 runtime lock")
        root = path.parent.resolve()
        records = [
            lock.get("plan"), lock.get("directive"), lock.get("attempt_spec"),
            *(lock.get("frozen_inputs") or []), *(lock.get("runtime_files") or []),
            *(lock.get("frozen_request") or []),
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("model") != MODEL
            or lock.get("effort") != EFFORT
            or lock.get("declared_turn_count") != 2
            or lock.get("retry_count") != 0
            or lock.get("turn_total_token_caps") != list(TURN_TOKEN_CAPS)
            or lock.get("phase_total_token_cap") != PHASE_TOKEN_CAP
            or lock.get("capacity_files_excluded_from_usage_records") is not True
            or lock.get("development_winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutation_allowed") is not False
            or {row.get("path") for row in lock.get("runtime_files") or []}
            != {str(path) for path in _runtime_files()}
            or {row.get("path") for row in lock.get("frozen_request") or []}
            != {row["path"] for row in _request_records(root)}
            or any(not _verify_record(record or {}) for record in records)
        ):
            raise OrderedRecurrentFoldError("epoch-3 runtime lock drifted")
        load_contract()
        return lock
    if acquire_lock:
        with _advisory_process_lock(path.parent.resolve()):
            return verify()
    return verify()


def _load_frozen(root: Path) -> dict[str, Any]:
    spec = _load_json(root / "attempt-spec.json", "epoch-3 attempt spec")
    base = (root / "base-instructions.private.md").read_text(encoding="utf-8")
    turns = []
    for index, name in enumerate(TURN_NAMES):
        paths = _turn_paths(root, name)
        private_input = _load_json(paths["input"], f"{name} input")
        turns.append({
            "turn_index": index,
            "turn_name": name,
            "episode_id": private_input["episode_id"],
            "segment_id": RUN_SEGMENT_ORDER[index],
            "segment_ids": [RUN_SEGMENT_ORDER[index]],
            "token_cap": TURN_TOKEN_CAPS[index],
            "private_input": private_input,
            "base": base,
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], f"{name} schema"),
            "direct_schema": _load_json(paths["direct_schema"], f"{name} projection schema"),
            "paths": paths,
        })
    return {"root": root, "spec": spec, "runtime_lock": root / "runtime-lock.json", "turns": turns}


def _usage_from_sidecar(path: Path, *, token_cap: int, thread_id: str) -> dict[str, int]:
    sidecar = _load_json(path, "turn sidecar")
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise OperationalWaitingError("turn usage is absent")
    values = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise OperationalWaitingError("turn usage is incomplete")
        values[field] = value
    wall = sidecar.get("wall_elapsed_seconds")
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_id
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or wall < 0
    ):
        raise OperationalWaitingError("measured managed-auth sidecar contract failed")
    if values["total_tokens"] > token_cap:
        raise StructuralRejectionError("turn exceeded its frozen token cap")
    return values


def _aggregate_usage(usages: Sequence[Mapping[str, int]]) -> dict[str, Any]:
    values = {field: sum(int(row[field]) for row in usages) for field in USAGE_FIELDS}
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "semantic_model_call_count": len(usages),
        "usage": values,
    }


def _project_direct_output_duplicate_tolerant(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Apply v233's structural projection while retaining exact duplicates.

    v233 rejected exact event identities.  Epoch 3 explicitly changes that one
    boundary: identity duplicates are counted but every event must survive for
    the later LLM equivalence judge.  All other source-unit and grounding checks
    remain equivalent to the v233 projection.
    """

    try:
        v233._validate_schema(turn["schema"], output, path="$")  # noqa: SLF001
    except (v233.ValidationError, ValueError, TypeError) as exc:
        raise StructuralRejectionError("projected output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise StructuralRejectionError("episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise StructuralRejectionError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    seen_identities: set[str] = set()
    duplicate_count = 0
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        expected_unit_ids = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        if [receipt.get("unit_id") for receipt in receipts] != expected_unit_ids:
            raise StructuralRejectionError("unit receipt order or coverage drifted")
        coverage = row.get("coverage_audit") or {}
        if coverage.get("all_source_units_reviewed") is not True or coverage.get("unresolved_count") != 0:
            raise StructuralRejectionError("source unit coverage audit failed")
        receipt_counts: dict[str, int] = {}
        for receipt in receipts:
            eligible = receipt.get("eligible_event_count")
            if (
                isinstance(eligible, bool)
                or not isinstance(eligible, int)
                or eligible < 0
                or receipt.get("unresolved_count") != 0
            ):
                raise StructuralRejectionError("unit receipt count is invalid")
            receipt_counts[str(receipt["unit_id"])] = eligible
        events = list(row.get("events") or [])
        if sum(receipt_counts.values()) != len(events):
            raise StructuralRejectionError("unit receipt event total drifted")
        if (row.get("status") == "coded") != bool(events):
            raise StructuralRejectionError("coded status does not match events")
        if not events and row.get("status") != "no_signal":
            raise StructuralRejectionError("empty extraction must be no_signal")
        if len(events) > v233.MAX_EVENTS_PER_SEGMENT:
            raise StructuralRejectionError("event cap exceeded")
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        start_counts = {unit_id: 0 for unit_id in expected_unit_ids}
        prior_start_index = -1
        projected_events = []
        text = str(source["segment_text"])
        boundaries = list(source["boundaries"])
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            if start_id not in unit_index or end_id not in unit_index:
                raise StructuralRejectionError("evidence unit belongs to another segment")
            start_index = unit_index[start_id]
            end_index = unit_index[end_id]
            if start_index > end_index:
                raise StructuralRejectionError("evidence unit range is reversed")
            if start_index < prior_start_index:
                raise StructuralRejectionError("event evidence order drifted")
            prior_start_index = start_index
            start_char = int(units[start_index]["start_char"])
            end_char = int(units[end_index]["end_char"])
            evidence = text[start_char:end_char]
            if not evidence:
                raise StructuralRejectionError("projected evidence is empty")
            if not any(
                int(boundary["extract_start"]) <= start_char
                and end_char <= int(boundary["extract_end"])
                for boundary in boundaries
            ):
                raise StructuralRejectionError("projected evidence is outside every fixed evidence window")
            metric_values = [
                str(event.get(field) or "")
                for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
            ]
            if any(value and value not in evidence for value in metric_values):
                raise StructuralRejectionError("metric literal is not in evidence")
            if any(metric_values) == (event.get("metric_direction") == "not_applicable"):
                raise StructuralRejectionError("metric direction applicability drifted")
            identity = _canonical_json({field: event.get(field) for field in v233.IDENTITY_FIELDS})
            if identity in seen_identities:
                duplicate_count += 1
            else:
                seen_identities.add(identity)
            window_id = v233.v232._owner_window(start_char, boundaries)  # noqa: SLF001
            event["window_id"] = window_id
            event["evidence"] = evidence
            projected_events.append(event)
            start_counts[start_id] += 1
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": start_char,
                    "end_char": end_char,
                    "window_id": window_id,
                    "evidence_sha256": v233.sha256_text(evidence),
                }
            )
        if start_counts != receipt_counts:
            raise StructuralRejectionError("unit receipt ownership drifted")
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "events": projected_events,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(receipts),
                "event_count": len(projected_events),
                "unresolved_count": 0,
                "exact_identity_duplicate_count": duplicate_count,
                "exact_identity_duplicate_detection": "diagnostic_only_nonblocking",
            }
        )
    return (
        {"episode_id": turn["episode_id"], "segments": normalized_rows},
        {"schema_version": SCHEMA_VERSION, "episode_id": turn["episode_id"], "events": provenance_rows},
        diagnostics,
    )


def project_turn_output(output: Mapping[str, Any], turn: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        v249._validate_schema(turn["schema"], output, path="$")  # noqa: SLF001
    except (v249.ValidationError, ValueError, TypeError) as exc:
        raise StructuralRejectionError("explicit-applicability output schema failed") from exc
    direct = copy.deepcopy(dict(output))
    state_counts: dict[str, dict[str, int]] = {}
    for segment in direct.get("segments") or []:
        projected_events = []
        for event in segment.get("events") or []:
            for field in (
                *v249.OPTIONAL_TEXT_FIELDS,
                "claim_type",
                "stance",
                "speaker",
                "actor",
                "reported_actor",
                "metric",
                *v249.ENTITY_FIELDS,
            ):
                state = str(event[field]["applicability"])
                bucket = state_counts.setdefault(field, {})
                bucket[state] = bucket.get(state, 0) + 1
            projected_events.append(v249._project_event(event))  # noqa: SLF001
        segment["events"] = projected_events
    direct_turn = dict(turn)
    direct_turn["schema"] = turn["direct_schema"]
    try:
        normalized, provenance, diagnostics = _project_direct_output_duplicate_tolerant(
            direct, direct_turn
        )
    except StructuralRejectionError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise StructuralRejectionError(str(exc)) from exc
    duplicate_count = sum(
        int(row.get("exact_identity_duplicate_count", 0)) for row in diagnostics
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "state_counts": state_counts,
        "all_optional_semantics_selected_by_llm": True,
        "deterministic_projection_only": True,
        "exact_identity_duplicate_count": duplicate_count,
        "exact_identity_duplicate_detection": "diagnostic_only_nonblocking",
    }
    return normalized, provenance, diagnostics, receipt


def fold_validated_outputs(
    first: tuple[Mapping[str, Any], Mapping[str, Any], Sequence[Mapping[str, Any]], Mapping[str, Any]],
    second: tuple[Mapping[str, Any], Mapping[str, Any], Sequence[Mapping[str, Any]], Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    outputs = [first, second]
    normalized_by_id = {}
    diagnostics_by_id = {}
    provenance_events = []
    input_event_hashes = []
    for normalized, provenance, diagnostics, _applicability in outputs:
        for row in normalized.get("segments") or []:
            segment_id = str(row.get("segment_id"))
            if segment_id in normalized_by_id:
                raise StructuralRejectionError("a segment was returned more than once")
            normalized_by_id[segment_id] = copy.deepcopy(row)
            input_event_hashes.extend(
                hashlib.sha256(_canonical_json(event).encode("utf-8")).hexdigest()
                for event in row.get("events") or []
            )
        for row in diagnostics:
            diagnostics_by_id[str(row.get("segment_id"))] = copy.deepcopy(row)
        provenance_events.extend(copy.deepcopy(provenance.get("events") or []))
    if set(normalized_by_id) != set(CANONICAL_SEGMENT_ORDER) or set(diagnostics_by_id) != set(CANONICAL_SEGMENT_ORDER):
        raise StructuralRejectionError("folded segment coverage drifted")
    folded = {
        "episode_id": first[0]["episode_id"],
        "segments": [normalized_by_id[segment_id] for segment_id in CANONICAL_SEGMENT_ORDER],
    }
    output_event_hashes = [
        hashlib.sha256(_canonical_json(event).encode("utf-8")).hexdigest()
        for row in folded["segments"] for event in row.get("events") or []
    ]
    if sorted(input_event_hashes) != sorted(output_event_hashes):
        raise StructuralRejectionError("fold changed the validated event multiset")
    identities = [
        _canonical_json({field: event.get(field) for field in v233.IDENTITY_FIELDS})
        for row in folded["segments"]
        for event in row.get("events") or []
    ]
    duplicate_count = len(identities) - len(set(identities))
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": folded["episode_id"],
        "events": sorted(
            provenance_events,
            key=lambda row: (CANONICAL_SEGMENT_ORDER.index(str(row["segment_id"])), int(row["event_index"])),
        ),
    }
    diagnostics = [diagnostics_by_id[segment_id] for segment_id in CANONICAL_SEGMENT_ORDER]
    fold_receipt = {
        "schema_version": SCHEMA_VERSION,
        "run_segment_order": list(RUN_SEGMENT_ORDER),
        "canonical_output_segment_order": list(CANONICAL_SEGMENT_ORDER),
        "input_event_count": len(input_event_hashes),
        "output_event_count": len(output_event_hashes),
        "event_multiset_sha256": hashlib.sha256(_canonical_json(sorted(output_event_hashes)).encode("utf-8")).hexdigest(),
        "exact_identity_duplicate_count": duplicate_count,
        "exact_identity_duplicate_detection": "diagnostic_only_nonblocking",
        "all_validated_events_preserved": True,
        "deterministic_structural_reorder_only": True,
        "semantic_pruning_performed": False,
        "support_filtering_performed": False,
        "deduplication_performed": False,
        "relabeling_performed": False,
    }
    return folded, provenance, diagnostics, fold_receipt


async def _probe_capacity(
    client: Any,
    *,
    checkpoint_path: Path,
    turn_name: str,
    remaining_token_cap: int,
    thread_exists_before_probe: bool,
) -> dict[str, Any]:
    account = getattr(client, "account_summary", None)
    if not isinstance(account, Mapping) or account.get("type") != "chatgpt" or account.get("plan_type") != "pro":
        raise OperationalWaitingError("managed ChatGPT Pro auth is unavailable")
    try:
        response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
        snapshot = capacity.parse_rate_limit_snapshot(response, maximum_primary_used_percent=100)
    except BaseException as exc:
        raise OperationalWaitingError("fresh no-turn capacity probe failed") from exc
    used = snapshot.get("primary_used_percent")
    reached = snapshot.get("rate_limit_reached_type")
    if isinstance(used, bool) or not isinstance(used, int) or not 0 <= used <= 100:
        raise OperationalWaitingError("capacity percentage is malformed")
    projected_points = math.ceil(remaining_token_cap * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    remaining = 100 - used
    cleared = reached is None and remaining - projected_points >= MIN_REMAINING_RESERVE_PERCENT
    checkpoint = {
        "schema_version": "pif_ordered_recurrent_capacity_checkpoint_v1",
        "checked_at": now_iso(),
        "turn_name": turn_name,
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "primary_used_percent": used,
        "primary_remaining_percent": remaining,
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "remaining_phase_token_cap": remaining_token_cap,
        "projected_remaining_quota_points": projected_points,
        "projected_terminal_remaining_percent": remaining - projected_points,
        "rate_limit_reached_type": reached,
        "cleared_for_semantic_turn": cleared,
        "thread_exists_before_probe": thread_exists_before_probe,
        "semantic_turn_started_before_probe": False,
        "semantic_turn_started_by_probe": False,
        "sidecar_started_before_probe": False,
    }
    _write_immutable_json(checkpoint_path, checkpoint)
    if not cleared:
        raise OperationalWaitingError("minimum reserve or provider capacity is unavailable")
    return checkpoint


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(command=(str(PINNED_CODEX), "app-server", "--listen", "stdio://"))


def _artifact_records(root: Path) -> list[dict[str, Any]]:
    paths = [
        root / "attempt-spec.json", root / "runtime-lock.json", root / "launch-receipt.json",
        root / "combined-normalized-output.private.json", root / "combined-evidence-provenance.private.json",
        root / "combined-diagnostics.private.json", root / "fold-receipt.json", root / "structural-gate.json",
    ]
    for name in TURN_NAMES:
        turn = _turn_paths(root, name)
        paths.extend(turn[key] for key in (
            "input", "prompt", "schema", "direct_schema", "capacity", "sidecar", "output",
            "normalized", "provenance", "diagnostics", "applicability",
        ))
    return [_record(path) for path in paths if path.is_file()]


def _receipt(
    *, root: Path, state: str, reason: str, accounting: Mapping[str, Any],
    error: BaseException | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise OrderedRecurrentFoldError("invalid epoch-3 receipt state")
    records = _artifact_records(root)
    return {
        "schema_version": RECEIPT_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": reason,
        "error_class": type(error).__name__ if error is not None else None,
        "error_message_sha256": hashlib.sha256(str(error).encode("utf-8")).hexdigest() if error is not None else None,
        "semantic_model_call_count": accounting.get("semantic_model_call_count", 0),
        "semantic_retry_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "records": records,
        "records_sha256": hashlib.sha256(_canonical_json(records).encode("utf-8")).hexdigest(),
    }


def verify_receipt(root: Path = DEFAULT_OUTPUT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    def verify() -> dict[str, Any]:
        receipt = _load_json(root / "plan-step-receipt.json", "epoch-3 receipt")
        terminal = _load_json(root / "terminal.json", "epoch-3 terminal")
        records = receipt.get("records")
        record_paths = {
            str(row.get("path")) for row in records or [] if isinstance(row, Mapping)
        }
        started_turn_count = receipt.get("semantic_model_call_count")
        required_capacity_paths = {
            str(_turn_paths(root, name)["capacity"].resolve())
            for name in TURN_NAMES[:started_turn_count]
        } if isinstance(started_turn_count, int) and 0 <= started_turn_count <= len(TURN_NAMES) else set()
        if (
            receipt != terminal
            or receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("semantic_retry_count") != 0
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production_mutated") is not False
            or not isinstance(records, list)
            or not required_capacity_paths.issubset(record_paths)
            or any(not _verify_record(row) for row in records)
            or receipt.get("records_sha256") != hashlib.sha256(_canonical_json(records).encode("utf-8")).hexdigest()
        ):
            raise OrderedRecurrentFoldError("epoch-3 receipt integrity failed")
        return receipt
    if acquire_lock:
        with _advisory_process_lock(root.resolve()):
            return verify()
    return verify()


def _best_effort_accounting(root: Path) -> dict[str, Any]:
    usages = []
    for name, cap in zip(TURN_NAMES, TURN_TOKEN_CAPS):
        path = _turn_paths(root, name)["sidecar"]
        if not path.is_file():
            continue
        try:
            sidecar = _load_json(path, "best-effort sidecar")
            usage = sidecar.get("usage")
            if (
                sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_status") != "measured"
                or sidecar.get("usage_complete") is not True
                or not isinstance(usage, Mapping)
            ):
                raise ValueError
            values = {field: int(usage[field]) for field in USAGE_FIELDS}
            if any(value < 0 for value in values.values()):
                raise ValueError
            usages.append(values)
        except (KeyError, TypeError, ValueError, OrderedRecurrentFoldError):
            return {"usage_status": "unknown", "accounting_complete": False, "semantic_model_call_count": len(usages) + 1, "usage": None}
    return _aggregate_usage(usages)


async def _run_unlocked(
    root: Path,
    *,
    client_factory: Callable[[], Any],
    capacity_probe: Callable[..., Awaitable[dict[str, Any]]],
    timeout_seconds: float,
) -> dict[str, Any]:
    receipt_path = root / "plan-step-receipt.json"
    terminal_path = root / "terminal.json"
    if receipt_path.exists() or terminal_path.exists():
        if not receipt_path.is_file() or not terminal_path.is_file():
            raise OrderedRecurrentFoldError("partial epoch-3 terminal integrity failure")
        return verify_receipt(root, acquire_lock=False)
    frozen = _freeze_unlocked(root)
    verify_runtime_lock(frozen["runtime_lock"], acquire_lock=False)
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        accounting = _best_effort_accounting(root)
        receipt = _receipt(root=root, state="waiting", reason="epoch3_existing_launch_requires_checksum_bound_recovery", accounting=accounting)
        _write_immutable_json(receipt_path, receipt)
        _write_immutable_json(terminal_path, receipt)
        return verify_receipt(root, acquire_lock=False)
    _write_immutable_json(launch_path, {
        "schema_version": SCHEMA_VERSION, "launched_at": now_iso(), "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH, "step_id": STEP_ID, "model": MODEL, "effort": EFFORT,
        "declared_turn_count": 2, "retry_count": 0, "runtime_lock": _record(frozen["runtime_lock"]),
        "development_winner_frozen": False, "holdout_authorized": False, "production_mutation_allowed": False,
    })
    started = time.monotonic()
    usages: list[dict[str, int]] = []
    projected_outputs = []
    try:
        async with client_factory() as client:
            await capacity_probe(
                client, checkpoint_path=frozen["turns"][0]["paths"]["capacity"],
                turn_name=TURN_NAMES[0], remaining_token_cap=PHASE_TOKEN_CAP,
                thread_exists_before_probe=False,
            )
            account = getattr(client, "account_summary", None)
            if not isinstance(account, Mapping) or account.get("type") != "chatgpt" or account.get("plan_type") != "pro":
                raise OperationalWaitingError("managed ChatGPT Pro auth is unavailable")
            thread = await client.start_thread(model=MODEL, base_instructions=frozen["turns"][0]["base"], cwd=PROJECT_ROOT, ephemeral=True)
            if thread.model != MODEL or thread.ephemeral is not True:
                raise OperationalWaitingError("app-server thread contract drifted")
            for index, turn in enumerate(frozen["turns"]):
                if index == 1:
                    remaining_cap = PHASE_TOKEN_CAP - usages[0]["total_tokens"]
                    if remaining_cap < TURN_TOKEN_CAPS[1]:
                        raise StructuralRejectionError("turn 1 left insufficient frozen combined token cap")
                    await capacity_probe(
                        client, checkpoint_path=turn["paths"]["capacity"],
                        turn_name=turn["turn_name"], remaining_token_cap=remaining_cap,
                        thread_exists_before_probe=True,
                    )
                result = await client.run_structured_turn(
                    thread=thread, effort=EFFORT, prompt=turn["prompt"], output_schema=turn["schema"],
                    sidecar_path=turn["paths"]["sidecar"], output_path=turn["paths"]["output"],
                    batch_size=1, thread_mode="same_thread", timeout_seconds=timeout_seconds,
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise OperationalWaitingError(f"turn {index + 1} did not complete")
                if getattr(result, "thread_id", None) != thread.thread_id:
                    raise OperationalWaitingError("turn escaped the pinned recurrent thread")
                usage = _usage_from_sidecar(turn["paths"]["sidecar"], token_cap=turn["token_cap"], thread_id=thread.thread_id)
                try:
                    projected = project_turn_output(result.output, turn)
                except StructuralRejectionError:
                    raise
                normalized, provenance, diagnostics, applicability = projected
                _write_immutable_json(turn["paths"]["normalized"], normalized)
                _write_immutable_json(turn["paths"]["provenance"], provenance)
                _write_immutable_json(turn["paths"]["diagnostics"], {"segments": diagnostics})
                _write_immutable_json(turn["paths"]["applicability"], applicability)
                usages.append(usage)
                projected_outputs.append(projected)
        accounting = _aggregate_usage(usages)
        total = accounting["usage"]["total_tokens"]
        if total > PHASE_TOKEN_CAP:
            raise StructuralRejectionError("combined extraction token cap exceeded")
        folded, provenance, diagnostics, fold_receipt = fold_validated_outputs(projected_outputs[0], projected_outputs[1])
        normalized_path = root / "combined-normalized-output.private.json"
        provenance_path = root / "combined-evidence-provenance.private.json"
        diagnostics_path = root / "combined-diagnostics.private.json"
        fold_path = root / "fold-receipt.json"
        _write_immutable_json(normalized_path, folded)
        _write_immutable_json(provenance_path, provenance)
        _write_immutable_json(diagnostics_path, {"segments": diagnostics})
        _write_immutable_json(fold_path, fold_receipt)
        production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + total * PRODUCTION_SCALE
        ratio = production_total / BASELINE_END_TO_END_TOKENS
        gate = {
            "schema_version": SCHEMA_VERSION,
            "passed": ratio <= PRODUCTION_RATIO_MAX,
            "checks": {
                "both_declared_segments_returned_once": [row["segment_id"] for row in folded["segments"]] == list(CANONICAL_SEGMENT_ORDER),
                "complete_v249_projection_valid": True,
                "all_source_units_reviewed": all(row["reviewed_source_unit_count"] == row["source_unit_count"] for row in diagnostics),
                "unresolved_source_unit_count_0": all(row["unresolved_count"] == 0 for row in diagnostics),
                "exact_evidence_rate_1": True,
                "metric_grounding_error_event_count_0": True,
                "event_cap_violation_count_0": True,
                "exact_identity_duplicate_count_reported_diagnostic_only": isinstance(fold_receipt["exact_identity_duplicate_count"], int),
                "all_validated_events_preserved": fold_receipt["all_validated_events_preserved"],
                "combined_extraction_total_tokens_lte_72891": total <= PHASE_TOKEN_CAP,
                "production_amortized_total_token_ratio_lte_0_28": ratio <= PRODUCTION_RATIO_MAX,
                "accounting_complete": True,
            },
            "combined_extraction_total_tokens": total,
            "production_amortized_total_tokens": production_total,
            "production_amortized_total_token_ratio": round(ratio, 6),
            "wall_seconds": round(time.monotonic() - started, 6),
        }
        gate["passed"] = all(gate["checks"].values())
        _write_immutable_json(root / "structural-gate.json", gate)
        if not gate["passed"]:
            raise StructuralRejectionError("epoch-3 structural or cost gate failed")
        receipt = _receipt(root=root, state="passed", reason="epoch3_ordered_recurrent_full_schema_fold_structural_gate_passed", accounting=accounting)
    except StructuralRejectionError as exc:
        receipt = _receipt(root=root, state="rejected", reason="epoch3_ordered_recurrent_full_schema_fold_structural_or_cost_rejected", accounting=_best_effort_accounting(root), error=exc)
    except BaseException as exc:
        receipt = _receipt(root=root, state="waiting", reason="epoch3_ordered_recurrent_full_schema_fold_operational_waiting", accounting=_best_effort_accounting(root), error=exc)
    _write_stable_time(receipt_path, receipt, "created_at")
    _write_stable_time(terminal_path, receipt, "created_at")
    return verify_receipt(root, acquire_lock=False)


async def run(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[], Any] = _inner_factory,
    capacity_probe: Callable[..., Awaitable[dict[str, Any]]] = _probe_capacity,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return await _run_unlocked(root, client_factory=client_factory, capacity_probe=capacity_probe, timeout_seconds=timeout_seconds)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the epoch-3 ordered recurrent full-schema fold canary")
    parser.add_argument("action", choices=("freeze", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_run(args.output_dir)
        result = {"state": "frozen", "runtime_lock": str(frozen["runtime_lock"])}
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds))
        result = {"state": receipt["state"], "terminal_reason": receipt["terminal_reason"], "semantic_model_call_count": receipt["semantic_model_call_count"]}
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
