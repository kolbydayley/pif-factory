from __future__ import annotations

"""Epoch-38 two-case full-event AB/BA quality gate for epoch 37.

The runtime consumes the immutable epoch-37 extraction output, places every
selected baseline and candidate event into one blinded witness pool, and runs
the mature support-first/full-field judge in AB then BA order.  It performs no
extraction, holdout work, production mutation, or deterministic semantic
repair.
"""

import argparse
import asyncio
import copy
import fcntl
import functools
import hashlib
import json
import math
import os
import stat
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_llm_judge as judge
from . import codex_app_server
from .util import now_iso


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
EPOCH37_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch37-dual-pass-omission-audit-ledger-canary-v1"
).resolve()
BASELINE_SEED_PATH = (
    PROJECT_ROOT / "work/app-server-development-v2/shared-reference-seed-v1.json"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch38-dual-pass-omission-audit-quality-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch38-dual-pass-omission-audit-quality-v38.json"
).resolve()
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v38.json").resolve()
SOURCE_CAPACITY_POLICY = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()
PINNED_CODEX = (
    PROJECT_ROOT
    / "work/app-server-development-v2/pinned-runtime/codex-0.144.1/bin/codex"
).resolve()
INSTRUCTION_SOURCE_PATH = Path("/Users/kolbydayley/.codex/AGENTS.md").resolve()

SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_quality_v38"
LOCK_VERSION = "pif_canonical_v31_dual_pass_omission_audit_quality_lock_v1"
LAUNCH_VERSION = "pif_canonical_v31_dual_pass_omission_audit_quality_launch_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 38
STEP_ID = "canonical_v31_epoch38_dual_pass_omission_audit_quality_v38"
MODEL = "gpt-5.5"
EFFORT = "high"
TRANSPORT = "official_codex_app_server_stdio_managed_chatgpt_auth"
TIMEOUT_SECONDS = 1200.0
MODEL_CALL_CAP = 2
SEMANTIC_RETRY_COUNT = 0
TOTAL_TOKEN_CAP = 200_000
MAXIMUM_TOTAL_TOKENS_PER_TURN = 100_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PHASE_TOTAL_TOKEN_BOUND = MODEL_CALL_CAP * MAXIMUM_TOTAL_TOKENS_PER_TURN
PROJECTED_PHASE_QUOTA_POINTS = math.ceil(
    PHASE_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
QUALITY_THRESHOLD = 0.97
PRODUCTION_RATIO_TARGET = 0.28
FULL_SIX_CASE_TOKEN_PROJECTION = 60_481
FULL_SIX_CASE_TOKEN_CEILING = 73_926
SYSTEM_BASELINE = "canonical_v31_frozen_baseline"
SYSTEM_CANDIDATE = "canonical_v31_epoch37_dual_pass_omission_audit_ledger_medium"
SOURCE_SEGMENT_ORDER = (
    "seg_80fe585badf8f3d3597d0f96",
    "seg_7c027e01ed7e813b528c5c7f",
)
BASELINE_EVENT_COUNTS = (23, 20)
CANDIDATE_EVENT_COUNTS = (17, 13)
EXPECTED_BASELINE_WITNESSES = sum(BASELINE_EVENT_COUNTS)
EXPECTED_CANDIDATE_WITNESSES = sum(CANDIDATE_EVENT_COUNTS)
EXPECTED_TOTAL_WITNESSES = EXPECTED_BASELINE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES
EXPECTED_WITNESS_COUNTS = {
    **{
        (segment_id, SYSTEM_BASELINE): count
        for segment_id, count in zip(SOURCE_SEGMENT_ORDER, BASELINE_EVENT_COUNTS)
    },
    **{
        (segment_id, SYSTEM_CANDIDATE): count
        for segment_id, count in zip(SOURCE_SEGMENT_ORDER, CANDIDATE_EVENT_COUNTS)
    },
}
CAPACITY_TURN_NAMES = ("epoch38_quality_ab", "epoch38_quality_ba")
EXPECTED_INSTRUCTION_SOURCES_COUNT = 1
EXPECTED_INSTRUCTION_SOURCES_SHA256 = (
    "a86aa3509517b418a3a54effc1a4f1fbde4af060b1664f753c06388334698aff"
)
PROCESS_LOCK_NAME = ".canonical-v31-epoch38-quality.lock"
_FORBIDDEN_AUTH_ENVIRONMENT = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "OPENAI_ACCESS_TOKEN",
    "OPENAI_AUTH_TOKEN",
    "CHATGPT_ACCESS_TOKEN",
    "OPENAI_SESSION_TOKEN",
    "CHATGPT_SESSION_TOKEN",
    "CODEX_SESSION_TOKEN",
)
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)

EPOCH37_EXPECTED = {
    "runtime_lock": "f49956812f1a80372e54a9870a8ce1062d7d943499a13a30a7dd81fc11d569e4",
    "receipt": "bd46613bc6bbaa87bad3fe9e4e3d70ac1f4082b65222ab48587dcb32797a04a4",
    "authorization": "bb005f613f052479204791575a711077480f494969fdd51ce364e0431f19c90e",
    "semantic_attempt": "97d30ee7676eac44b4347ae3aa326b97f89db18a41a092e08f884f0f5fb80572",
    "labels": "03674e03d8be3eaa5a922a3c54e2d888578df2c814b763b2d3a5cbb95f6bf9b4",
    "provenance": "b6506d0c179c7b0f10e67a122914e111c9fa737abb23140c24c04799911fe58e",
    "fidelity": "ec7cccd261ed5c2c11e2ecd5921f1cd7b826b67d6b4e0b29482deee1f03f7455",
    "output": "45ad201b6f440da5a951e6981cec9d43d5f9387a8b6973a47af6850538755ce5",
    "sidecar": "274311dfc2fd942b5ebda0318d456514ea6db07f80beb170dcd99af327d15172",
    "request": "c74cd58416c0a706c07ff2b6439bb935d33819b793c457d6677c7416774399da",
    "baseline_seed": "167bdfb40af5608b1dd23d3199d30b9d12bc7573f369038a4f642e85adaf19b0",
}


PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT = 15
PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT = 2
PREDECESSOR_MEASURED_TOTAL_TOKENS = 624_146


class Epoch38QualityError(RuntimeError):
    """The epoch-38 contract, lineage, or immutable evidence drifted."""


class Epoch38QualityWaiting(Epoch38QualityError):
    """A zero-retry operational attempt must stop without replay."""


@functools.lru_cache(maxsize=256)
def _cached_sha256(path_value: str, device: int, inode: int, size: int, mtime_ns: int) -> str:
    del device, inode, size, mtime_ns
    digest = hashlib.sha256()
    with Path(path_value).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_file(path: Path) -> str:
    target = path.expanduser().resolve(strict=True)
    info = target.stat()
    return _cached_sha256(str(target), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve(strict=True)
    info = target.stat()
    if not stat.S_ISREG(info.st_mode) or target.is_symlink():
        raise Epoch38QualityError(f"required artifact is not a real file: {target}")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": info.st_size,
    }


def _verify_record(value: Any) -> bool:
    try:
        return isinstance(value, Mapping) and set(value) == {
            "path",
            "sha256",
            "size_bytes",
        } and _record(Path(str(value["path"]))) == dict(value)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Epoch38QualityError(f"cannot read {label}") from exc


def _write_immutable_bytes(path: Path, payload: bytes) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.is_symlink() or target.read_bytes() != payload:
            raise Epoch38QualityError(f"immutable artifact drifted: {target}")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    _cached_sha256.cache_clear()


def _write_immutable_json(path: Path, value: Any) -> None:
    _write_immutable_bytes(path, _pretty_json(value).encode("utf-8"))


def _write_immutable_text(path: Path, value: str) -> None:
    _write_immutable_bytes(path, value.encode("utf-8"))


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_text(_canonical_json(list(records)))


@contextmanager
def _process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / PROCESS_LOCK_NAME
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Epoch38QualityWaiting("another epoch-38 evaluator owns the root") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _direct_source_paths() -> dict[str, Path]:
    return {
        "epoch37_runtime_lock": EPOCH37_ROOT / "runtime-lock.json",
        "epoch37_receipt": EPOCH37_ROOT / "plan-step-receipt.json",
        "epoch37_terminal": EPOCH37_ROOT / "terminal.json",
        "epoch37_authorization": EPOCH37_ROOT / "operator-authorization.json",
        "epoch37_semantic_attempt": EPOCH37_ROOT / "turn/semantic-attempt.json",
        "epoch37_labels": EPOCH37_ROOT / "turn/canonical-labels.private.json",
        "epoch37_provenance": EPOCH37_ROOT / "turn/evidence-provenance.private.json",
        "epoch37_fidelity": EPOCH37_ROOT / "turn/semantic-fidelity.json",
        "epoch37_output": EPOCH37_ROOT / "turn/output.private.json",
        "epoch37_sidecar": EPOCH37_ROOT / "turn/sidecar.json",
        "epoch37_request": EPOCH37_ROOT / "prepared-turn/request.private.json",
        "baseline_seed": BASELINE_SEED_PATH,
    }


def _runtime_paths() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(reserve.__file__).resolve(),
        Path(app_server_capacity.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
        PINNED_CODEX,
        INSTRUCTION_SOURCE_PATH,
    )


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "epoch-38 semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "epoch-38 quality directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    expected_plan_keys = {"schema_version", "thread_id", "plan_epoch", "state", "step"}
    expected_step_keys = {
        "step_id",
        "state",
        "max_model_calls",
        "max_total_tokens",
        "expected_receipt_path",
        "accepted_receipt_states",
        "directive_path",
        "directive_sha256",
    }
    if (
        not isinstance(step, Mapping)
        or set(plan) != expected_plan_keys
        or set(step) != expected_step_keys
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != MODEL_CALL_CAP
        or step.get("max_total_tokens") != TOTAL_TOKEN_CAP
        or step.get("expected_receipt_path") != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or Path(str(step.get("directive_path"))).resolve() != DIRECTIVE_PATH
        or step.get("directive_sha256") != _sha256_file(DIRECTIVE_PATH)
    ):
        raise Epoch38QualityError("epoch-38 semantic plan drifted")
    quality = directive.get("quality_contract")
    capacity = directive.get("capacity_contract")
    promotion = directive.get("promotion_contract")
    instruction = directive.get("instruction_contract")
    if (
        directive.get("schema_version")
        != "pif_evaluation_epoch38_dual_pass_omission_audit_quality_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authority") != "direct_user_instruction"
        or directive.get("authorized_by") != "kolby"
        or directive.get("authorization_statement")
        != "You have my full permission to continue. No need to seek out my approval anymore."
        or directive.get("state")
        != "authorized_for_zero_call_freeze_then_two_bounded_full_event_judge_turns"
        or directive.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or not isinstance(quality, Mapping)
        or quality.get("source_segment_order") != list(SOURCE_SEGMENT_ORDER)
        or quality.get("baseline_witness_count") != EXPECTED_BASELINE_WITNESSES
        or quality.get("candidate_witness_count") != EXPECTED_CANDIDATE_WITNESSES
        or quality.get("all_event_witness_count") != EXPECTED_TOTAL_WITNESSES
        or quality.get("judge_model") != MODEL
        or quality.get("judge_effort") != EFFORT
        or quality.get("semantic_model_call_cap") != MODEL_CALL_CAP
        or quality.get("semantic_retry_cap") != SEMANTIC_RETRY_COUNT
        or quality.get("all_events_direct_to_support_and_alignment") is not True
        or quality.get("semantic_prefilter_allowed") is not False
        or quality.get("deterministic_semantic_pruning_allowed") is not False
        or quality.get("reference_system_ids") is not None
        or quality.get("quality_threshold") != QUALITY_THRESHOLD
        or quality.get("candidate_noninferiority_required") is not True
        or quality.get("adjudication_call_cap") != 0
        or not isinstance(instruction, Mapping)
        or instruction.get("instruction_sources_count")
        != EXPECTED_INSTRUCTION_SOURCES_COUNT
        or instruction.get("instruction_sources_sha256")
        != EXPECTED_INSTRUCTION_SOURCES_SHA256
        or instruction.get("instruction_source_paths")
        != [str(INSTRUCTION_SOURCE_PATH)]
        or instruction.get("instruction_source_records")
        != [_record(INSTRUCTION_SOURCE_PATH)]
        or not isinstance(capacity, Mapping)
        or capacity.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or capacity.get("maximum_total_tokens_per_turn")
        != MAXIMUM_TOTAL_TOKENS_PER_TURN
        or capacity.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
        or capacity.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or not isinstance(promotion, Mapping)
        or promotion.get("full_six_case_token_projection")
        != FULL_SIX_CASE_TOKEN_PROJECTION
        or promotion.get("full_six_case_token_ceiling") != FULL_SIX_CASE_TOKEN_CEILING
        or promotion.get("production_amortized_ratio_max") != PRODUCTION_RATIO_TARGET
        or promotion.get("winner_frozen") is not False
        or promotion.get("holdout_authorized") is not False
        or promotion.get("production_mutation_allowed") is not False
        or directive.get("transport_contract")
        != {
            "api_key_billing_allowed": False,
            "managed_chatgpt_pro_auth_required": True,
            "raw_session_token_replay_allowed": False,
            "transport": TRANSPORT,
        }
    ):
        raise Epoch38QualityError("epoch-38 directive values drifted")
    frozen = directive.get("frozen_inputs")
    expected_sources = _direct_source_paths()
    if not isinstance(frozen, list) or [row.get("role") for row in frozen] != list(expected_sources):
        raise Epoch38QualityError("epoch-38 frozen input roles drifted")
    records: dict[str, dict[str, Any]] = {}
    for row in frozen:
        if not isinstance(row, Mapping) or set(row) != {"role", "path", "sha256", "size_bytes"}:
            raise Epoch38QualityError("epoch-38 frozen input record is malformed")
        role = str(row["role"])
        expected = _record(expected_sources[role])
        supplied = {key: row[key] for key in ("path", "sha256", "size_bytes")}
        if supplied != expected:
            raise Epoch38QualityError(f"epoch-38 frozen input drifted: {role}")
        records[role] = expected
    expected_hashes = {
        "epoch37_runtime_lock": EPOCH37_EXPECTED["runtime_lock"],
        "epoch37_receipt": EPOCH37_EXPECTED["receipt"],
        "epoch37_terminal": EPOCH37_EXPECTED["receipt"],
        "epoch37_authorization": EPOCH37_EXPECTED["authorization"],
        "epoch37_semantic_attempt": EPOCH37_EXPECTED["semantic_attempt"],
        "epoch37_labels": EPOCH37_EXPECTED["labels"],
        "epoch37_provenance": EPOCH37_EXPECTED["provenance"],
        "epoch37_fidelity": EPOCH37_EXPECTED["fidelity"],
        "epoch37_output": EPOCH37_EXPECTED["output"],
        "epoch37_sidecar": EPOCH37_EXPECTED["sidecar"],
        "epoch37_request": EPOCH37_EXPECTED["request"],
        "baseline_seed": EPOCH37_EXPECTED["baseline_seed"],
    }
    if any(records[role]["sha256"] != digest for role, digest in expected_hashes.items()):
        raise Epoch38QualityError("epoch-38 direct source hash drifted")
    receipt = _load_json(EPOCH37_ROOT / "plan-step-receipt.json", "epoch-37 receipt")
    terminal = _load_json(EPOCH37_ROOT / "terminal.json", "epoch-37 terminal")
    sidecar = _load_json(EPOCH37_ROOT / "turn/sidecar.json", "epoch-37 sidecar")
    fidelity = _load_json(EPOCH37_ROOT / "turn/semantic-fidelity.json", "epoch-37 fidelity")
    if (
        receipt != terminal
        or receipt.get("state") != "passed"
        or receipt.get("terminal_reason") != "epoch37_one_turn_canary_passed"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 27_857
        or receipt.get("new_wall_elapsed_seconds") != 219.321
        or receipt.get("aggregate_architecture_semantic_model_call_count")
        != PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT
        or receipt.get("aggregate_architecture_unknown_usage_turn_count")
        != PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT
        or receipt.get("aggregate_architecture_measured_total_tokens")
        != PREDECESSOR_MEASURED_TOTAL_TOKENS
        or receipt.get("full_six_case_total_token_projection_by_case_count")
        != FULL_SIX_CASE_TOKEN_PROJECTION
        or receipt.get("full_six_case_cost_projection_pass") is not True
        or receipt.get("structural_and_cost_pass") is not True
        or receipt.get("diagnostic_event_count") != EXPECTED_CANDIDATE_WITNESSES
        or receipt.get("primary_ledger_item_count") != 28
        or receipt.get("omission_audit_ledger_item_count") != 2
        or receipt.get("dual_ledger_item_count") != EXPECTED_CANDIDATE_WITNESSES
        or receipt.get("quality_measured_by_this_step") is not False
        or receipt.get("winner_frozen") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("holdout_authorized") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("effort") != "medium"
        or sidecar.get("usage", {}).get("total_tokens") != 27_857
        or sidecar.get("wall_elapsed_seconds") != 219.321
        or fidelity.get("emitted_event_count") != EXPECTED_CANDIDATE_WITNESSES
        or fidelity.get("primary_ledger_item_count") != 28
        or fidelity.get("omission_audit_ledger_item_count") != 2
        or fidelity.get("dual_ledger_item_count") != EXPECTED_CANDIDATE_WITNESSES
        or fidelity.get("all_emitted_semantic_values_preserved") is not True
        or fidelity.get("deterministic_semantic_pruning") is not False
        or fidelity.get("deterministic_support_filtering") is not False
        or fidelity.get("deterministic_deduplication") is not False
        or fidelity.get("deterministic_relabeling") is not False
    ):
        raise Epoch38QualityError("epoch-37 terminal evidence drifted")
    return {
        "plan": plan,
        "directive": directive,
        "source_records": records,
        "receipt_path": DEFAULT_ROOT / "plan-step-receipt.json",
    }


def _event_sha256(event: Mapping[str, Any]) -> str:
    return _sha256_text(_canonical_json(event))


def build_complete_container(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    records = contract["source_records"]
    request = _load_json(Path(records["epoch37_request"]["path"]), "epoch-37 request")
    labels = _load_json(Path(records["epoch37_labels"]["path"]), "epoch-37 labels")
    provenance = _load_json(
        Path(records["epoch37_provenance"]["path"]), "epoch-37 provenance"
    )
    baseline = _load_json(Path(records["baseline_seed"]["path"]), "baseline seed")
    segments = (request.get("private_input") or {}).get("segments")
    if (
        not isinstance(segments, list)
        or [row.get("segment_id") for row in segments] != list(SOURCE_SEGMENT_ORDER)
        or request.get("segment_ids") != list(SOURCE_SEGMENT_ORDER)
    ):
        raise Epoch38QualityError("epoch-37 source segment order drifted")
    if not isinstance(labels, list) or [row.get("segment_id") for row in labels] != list(
        SOURCE_SEGMENT_ORDER
    ):
        raise Epoch38QualityError("epoch-37 candidate segment order drifted")
    provenance_segments = provenance.get("segments") if isinstance(provenance, Mapping) else None
    if not isinstance(provenance_segments, list) or [
        row.get("segment_id") for row in provenance_segments
    ] != list(SOURCE_SEGMENT_ORDER):
        raise Epoch38QualityError("epoch-37 provenance segment order drifted")
    references = baseline.get("references") if isinstance(baseline, Mapping) else None
    if not isinstance(references, list):
        raise Epoch38QualityError("baseline references are missing")
    reference_by_id = {str(row.get("segment_id")): row for row in references if isinstance(row, Mapping)}
    if len(reference_by_id) != len(references):
        raise Epoch38QualityError("baseline segment identities are duplicated")

    raw_cases: list[dict[str, Any]] = []
    candidate_lineage: list[dict[str, Any]] = []
    expected_baseline_events: Counter[tuple[str, str]] = Counter()
    expected_candidate_events: Counter[tuple[str, str]] = Counter()
    for source_index, (segment, label, provenance_segment) in enumerate(
        zip(segments, labels, provenance_segments)
    ):
        segment_id = str(segment["segment_id"])
        source_text = segment.get("segment_text")
        baseline_events = (
            reference_by_id.get(segment_id, {}).get("golden_output") or {}
        ).get("discourse_events")
        candidate_events = label.get("discourse_events")
        provenance_events = provenance_segment.get("discourse_events")
        if (
            not isinstance(source_text, str)
            or not source_text
            or not isinstance(baseline_events, list)
            or not isinstance(candidate_events, list)
            or not isinstance(provenance_events, list)
            or len(candidate_events) != len(provenance_events)
            or len(baseline_events) != BASELINE_EVENT_COUNTS[source_index]
            or len(candidate_events) != CANDIDATE_EVENT_COUNTS[source_index]
        ):
            raise Epoch38QualityError("epoch-38 selected event counts drifted")
        baseline_wrapped = []
        for event_index, raw_event in enumerate(baseline_events):
            if not isinstance(raw_event, Mapping):
                raise Epoch38QualityError("baseline event is malformed")
            event = copy.deepcopy(dict(raw_event))
            evidence = event.get("evidence")
            start = event.get("evidence_start")
            end = event.get("evidence_end")
            if (
                not isinstance(evidence, str)
                or not evidence
                or isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or not 0 <= start < end <= len(source_text)
                or source_text[start:end] != evidence
            ):
                raise Epoch38QualityError("baseline exact-evidence lineage drifted")
            event_hash = _event_sha256(event)
            expected_baseline_events[(segment_id, event_hash)] += 1
            baseline_wrapped.append(
                {
                    "event": event,
                    "provenance": {
                        "system_id": SYSTEM_BASELINE,
                        "segment_id": segment_id,
                        "raw_event_index": event_index,
                        "raw_event_sha256": event_hash,
                    },
                }
            )
        candidate_wrapped = []
        for event_index, (raw_event, provenance_row) in enumerate(
            zip(candidate_events, provenance_events)
        ):
            if not isinstance(raw_event, Mapping) or not isinstance(provenance_row, Mapping):
                raise Epoch38QualityError("candidate event provenance is malformed")
            event = copy.deepcopy(dict(raw_event))
            evidence = event.get("evidence")
            start = provenance_row.get("evidence_start")
            end = provenance_row.get("evidence_end")
            if (
                not isinstance(evidence, str)
                or not evidence
                or isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or source_text[start:end] != evidence
                or event.get("evidence_start") != start
                or event.get("evidence_end") != end
                or provenance_row.get("evidence_sha256")
                != hashlib.sha256(evidence.encode("utf-8")).hexdigest()
                or provenance_row.get("item_index") != event_index
            ):
                raise Epoch38QualityError("candidate exact-evidence lineage drifted")
            event_hash = _event_sha256(event)
            lineage = {
                "segment_id": segment_id,
                "event_index": event_index,
                "event_sha256": event_hash,
                "evidence_sha256": provenance_row["evidence_sha256"],
                "evidence_start": start,
                "evidence_end": end,
            }
            candidate_lineage.append(lineage)
            expected_candidate_events[(segment_id, event_hash)] += 1
            candidate_wrapped.append(
                {
                    "event": event,
                    "provenance": {
                        "system_id": SYSTEM_CANDIDATE,
                        "segment_id": segment_id,
                        "raw_event_index": event_index,
                        **lineage,
                    },
                }
            )
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": source_text,
                "event_set_a": baseline_wrapped,
                "event_set_b": candidate_wrapped,
                "provenance": {
                    "segment_id": segment_id,
                    "source_index": source_index,
                    "density_stratum": segment.get("density_stratum"),
                },
            }
        )
    try:
        pool, mapping = judge.make_shared_witness_pool(raw_cases, seed=STEP_ID)
    except (TypeError, ValueError) as exc:
        raise Epoch38QualityError("epoch-38 shared witness pool construction failed") from exc
    if judge.validate_shared_witness_pool(pool):
        raise Epoch38QualityError("epoch-38 shared witness pool failed validation")
    observed_counts: Counter[tuple[str, str]] = Counter()
    observed_events = {SYSTEM_BASELINE: Counter(), SYSTEM_CANDIDATE: Counter()}
    observed_candidate_lineage: Counter[tuple[str, int, str]] = Counter()
    for mapped_case in mapping.get("cases") or []:
        for witness in mapped_case.get("witnesses") or []:
            provenance_row = witness.get("provenance") or {}
            segment_id = str(provenance_row.get("segment_id"))
            system_id = str(provenance_row.get("system_id"))
            original = provenance_row.get("original_event")
            if system_id not in observed_events or not isinstance(original, Mapping):
                raise Epoch38QualityError("private witness lineage drifted")
            event_hash = _event_sha256(original)
            observed_counts[(segment_id, system_id)] += 1
            observed_events[system_id][(segment_id, event_hash)] += 1
            if system_id == SYSTEM_CANDIDATE:
                raw_index = provenance_row.get("raw_event_index")
                if (
                    isinstance(raw_index, bool)
                    or not isinstance(raw_index, int)
                    or provenance_row.get("event_sha256") != event_hash
                ):
                    raise Epoch38QualityError("candidate witness lineage hash drifted")
                observed_candidate_lineage[(segment_id, raw_index, event_hash)] += 1
    expected_counts = Counter(EXPECTED_WITNESS_COUNTS)
    if observed_counts != expected_counts:
        raise Epoch38QualityError("43-baseline plus 30-candidate witness counts drifted")
    if observed_events[SYSTEM_BASELINE] != expected_baseline_events:
        raise Epoch38QualityError("baseline raw-to-witness event multiset drifted")
    if observed_events[SYSTEM_CANDIDATE] != expected_candidate_events:
        raise Epoch38QualityError("candidate raw-to-witness event multiset drifted")
    expected_lineage = Counter(
        (row["segment_id"], row["event_index"], row["event_sha256"])
        for row in candidate_lineage
    )
    if observed_candidate_lineage != expected_lineage:
        raise Epoch38QualityError("candidate provenance is not one-to-one with witnesses")
    total = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    if total != EXPECTED_TOTAL_WITNESSES:
        raise Epoch38QualityError("epoch-38 witness total drifted")
    variants = judge.build_judge_variants(pool)
    ab_cases = variants["ab"]["cases"]
    ba_cases = variants["ba"]["cases"]
    if [row["case_id"] for row in ab_cases] != [row["case_id"] for row in ba_cases]:
        raise Epoch38QualityError("AB/BA case order drifted")
    for ab_case, ba_case in zip(ab_cases, ba_cases):
        if (
            ab_case["event_set_a"] != ba_case["event_set_b"]
            or ab_case["event_set_b"] != ba_case["event_set_a"]
        ):
            raise Epoch38QualityError("AB/BA opaque witness order drifted")
    if SYSTEM_BASELINE in _canonical_json(pool) or SYSTEM_CANDIDATE in _canonical_json(pool):
        raise Epoch38QualityError("system origin leaked into the model-facing pool")
    return pool, mapping, candidate_lineage


def _capacity_policy(root: Path) -> Path:
    source = reserve.load_reserve_capacity_policy(SOURCE_CAPACITY_POLICY)
    policy = dict(source)
    policy.update(
        {
            "schema_version": reserve.RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": STEP_ID,
            "semantic_output_root": str(root.resolve()),
            "ordered_turn_names": list(CAPACITY_TURN_NAMES),
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
            "retry_count_per_turn": 0,
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "production_mutation_allowed": False,
            "unknown_usage_hard_stop": True,
            "rate_limit_reached_type_must_be_null": True,
        }
    )
    path = root / "capacity-policy.json"
    _write_immutable_json(path, policy)
    reserve.load_reserve_capacity_policy(path)
    return path


def _meaningful_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        return verify_runtime_lock(lock_path, acquire_lock=False)
    if root.exists() and _meaningful_entries(root):
        raise Epoch38QualityError("unfrozen epoch-38 root is not empty")
    contract = load_contract()
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise Epoch38QualityError("epoch-38 output root differs from the contract")
    policy_path = _capacity_policy(root)
    pool, mapping, lineage = build_complete_container(contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    lineage_path = root / "candidate-lineage.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    _write_immutable_json(lineage_path, {"event_count": len(lineage), "events": lineage})
    variants = judge.build_judge_variants(pool)
    request_records: list[dict[str, Any]] = []
    for name in ("ab", "ba"):
        prompt_path = root / "requests" / f"prompt-{name}.private.md"
        schema_path = root / "requests" / f"schema-{name}.json"
        _write_immutable_text(prompt_path, judge.build_judge_prompt(variants[name]))
        _write_immutable_json(schema_path, judge.semantic_judge_output_schema(variants[name]))
        request_records.extend((_record(prompt_path), _record(schema_path)))
    base_path = root / "requests/base-instructions.private.md"
    _write_immutable_text(base_path, judge.judge_base_instructions())
    request_records.append(_record(base_path))
    attempt_spec_path = root / "attempt-spec.json"
    _write_immutable_json(
        attempt_spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "thread_id": THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "model": MODEL,
            "effort": EFFORT,
            "transport": TRANSPORT,
            "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
            "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
            "instruction_source": _record(INSTRUCTION_SOURCE_PATH),
            "variant_order": ["ab", "ba"],
            "source_segment_order": list(SOURCE_SEGMENT_ORDER),
            "baseline_witness_count": EXPECTED_BASELINE_WITNESSES,
            "candidate_witness_count": EXPECTED_CANDIDATE_WITNESSES,
            "all_event_witness_count": EXPECTED_TOTAL_WITNESSES,
            "all_events_direct_to_support_and_full_field_alignment": True,
            "one_shared_augmented_reference": True,
            "reference_system_ids": None,
            "semantic_prefilter_applied": False,
            "deterministic_semantic_pruning_applied": False,
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "semantic_retry_count": SEMANTIC_RETRY_COUNT,
            "semantic_total_token_cap": TOTAL_TOKEN_CAP,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "candidate_quality_threshold": QUALITY_THRESHOLD,
            "candidate_noninferiority_required": True,
            "full_six_case_token_projection": FULL_SIX_CASE_TOKEN_PROJECTION,
            "full_six_case_token_ceiling": FULL_SIX_CASE_TOKEN_CEILING,
            "judge_tokens_included_in_production_formula": False,
            "winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
    )
    runtime_records = [_record(path) for path in _runtime_paths()]
    source_records = list(contract["source_records"].values()) + [
        _record(PLAN_PATH),
        _record(DIRECTIVE_PATH),
    ]
    direct_records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        _record(lineage_path),
        *request_records,
        _record(attempt_spec_path),
        _record(policy_path),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_files": runtime_records,
        "source_records": source_records,
        "pool": _record(pool_path),
        "mapping": _record(mapping_path),
        "candidate_lineage": _record(lineage_path),
        "request_records": request_records,
        "attempt_spec": _record(attempt_spec_path),
        "capacity_policy": _record(policy_path),
        "direct_record_digest": _record_digest(direct_records),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": SEMANTIC_RETRY_COUNT,
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
        "model": MODEL,
        "effort": EFFORT,
        "transport": TRANSPORT,
        "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "instruction_source": _record(INSTRUCTION_SOURCE_PATH),
        "all_event_witness_count": EXPECTED_TOTAL_WITNESSES,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_immutable_json(lock_path, lock)
    return verify_runtime_lock(lock_path, acquire_lock=False)


def freeze_run(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    with _process_lock(output_root):
        return _freeze_unlocked(output_root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()

    def verify() -> dict[str, Any]:
        lock = _load_json(lock_path, "epoch-38 runtime lock")
        records = [
            *(lock.get("runtime_files") or []),
            *(lock.get("source_records") or []),
            lock.get("pool") or {},
            lock.get("mapping") or {},
            lock.get("candidate_lineage") or {},
            *(lock.get("request_records") or []),
            lock.get("attempt_spec") or {},
            lock.get("capacity_policy") or {},
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("runtime_files") != [_record(item) for item in _runtime_paths()]
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("semantic_retry_count") != SEMANTIC_RETRY_COUNT
            or lock.get("semantic_total_token_cap") != TOTAL_TOKEN_CAP
            or lock.get("maximum_total_tokens_per_turn")
            != MAXIMUM_TOTAL_TOKENS_PER_TURN
            or lock.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or lock.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
            or lock.get("model") != MODEL
            or lock.get("effort") != EFFORT
            or lock.get("transport") != TRANSPORT
            or lock.get("instruction_sources_count")
            != EXPECTED_INSTRUCTION_SOURCES_COUNT
            or lock.get("instruction_sources_sha256")
            != EXPECTED_INSTRUCTION_SOURCES_SHA256
            or lock.get("instruction_source") != _record(INSTRUCTION_SOURCE_PATH)
            or lock.get("all_event_witness_count") != EXPECTED_TOTAL_WITNESSES
            or lock.get("winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutated") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(not _verify_record(record) for record in records)
        ):
            raise Epoch38QualityError("epoch-38 runtime lock drifted")
        contract = load_contract()
        source_records = list(contract["source_records"].values()) + [
            _record(PLAN_PATH),
            _record(DIRECTIVE_PATH),
        ]
        if lock.get("source_records") != source_records:
            raise Epoch38QualityError("epoch-38 source record set drifted")
        rebuilt_pool, rebuilt_mapping, rebuilt_lineage = build_complete_container(contract)
        if (
            _load_json(Path(lock["pool"]["path"]), "epoch-38 pool") != rebuilt_pool
            or _load_json(Path(lock["mapping"]["path"]), "epoch-38 mapping")
            != rebuilt_mapping
            or _load_json(Path(lock["candidate_lineage"]["path"]), "epoch-38 lineage")
            != {"event_count": len(rebuilt_lineage), "events": rebuilt_lineage}
        ):
            raise Epoch38QualityError("epoch-38 reconstructed witness lineage drifted")
        for name in ("ab", "ba"):
            _turn_material(lock_path.parent, name, rebuilt_pool)
        attempt_spec = _load_json(
            Path(lock["attempt_spec"]["path"]), "epoch-38 attempt spec"
        )
        if (
            attempt_spec.get("schema_version") != SCHEMA_VERSION
            or attempt_spec.get("thread_id") != THREAD_ID
            or attempt_spec.get("plan_epoch") != PLAN_EPOCH
            or attempt_spec.get("step_id") != STEP_ID
            or attempt_spec.get("model") != MODEL
            or attempt_spec.get("effort") != EFFORT
            or attempt_spec.get("transport") != TRANSPORT
            or attempt_spec.get("instruction_sources_count")
            != EXPECTED_INSTRUCTION_SOURCES_COUNT
            or attempt_spec.get("instruction_sources_sha256")
            != EXPECTED_INSTRUCTION_SOURCES_SHA256
            or attempt_spec.get("instruction_source") != _record(INSTRUCTION_SOURCE_PATH)
            or attempt_spec.get("variant_order") != ["ab", "ba"]
            or attempt_spec.get("source_segment_order") != list(SOURCE_SEGMENT_ORDER)
            or attempt_spec.get("baseline_witness_count") != EXPECTED_BASELINE_WITNESSES
            or attempt_spec.get("candidate_witness_count") != EXPECTED_CANDIDATE_WITNESSES
            or attempt_spec.get("all_event_witness_count") != EXPECTED_TOTAL_WITNESSES
            or attempt_spec.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or attempt_spec.get("semantic_retry_count") != 0
            or attempt_spec.get("semantic_total_token_cap") != TOTAL_TOKEN_CAP
            or attempt_spec.get("reference_system_ids") is not None
            or attempt_spec.get("semantic_prefilter_applied") is not False
            or attempt_spec.get("deterministic_semantic_pruning_applied") is not False
            or attempt_spec.get("winner_frozen") is not False
            or attempt_spec.get("holdout_authorized") is not False
            or attempt_spec.get("production_mutated") is not False
        ):
            raise Epoch38QualityError("epoch-38 attempt specification drifted")
        policy = reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
        if (
            policy.get("phase_id") != STEP_ID
            or policy.get("semantic_output_root") != str(lock_path.parent)
            or policy.get("ordered_turn_names") != list(CAPACITY_TURN_NAMES)
            or policy.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or policy.get("maximum_total_tokens_per_turn")
            != MAXIMUM_TOTAL_TOKENS_PER_TURN
            or policy.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
            or policy.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
        ):
            raise Epoch38QualityError("epoch-38 capacity policy drifted")
        return lock

    if acquire_lock:
        with _process_lock(lock_path.parent):
            return verify()
    return verify()


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


class Epoch28ReserveCapacityClient(reserve.ReserveCapacityGatedCodexAppServerClient):
    """Map the mature judge layout onto the frozen epoch-38 reserve policy."""

    def _paths_for_index(self, index: int) -> tuple[Path, Path, Path]:
        root = Path(self.policy["semantic_output_root"]).expanduser().resolve()
        name = ("ab", "ba")[index]
        return (
            root / f"judge/sidecars/{name}.capacity.json",
            root / f"judge/sidecars/{name}.json",
            root / f"judge/output-{name}.private.json",
        )

    def _turn_index(self, checkpoint_path: Any) -> tuple[int, Path]:
        if checkpoint_path is None:
            raise reserve.ReserveCapacityError("epoch-38 capacity checkpoint is required")
        checkpoint = Path(checkpoint_path).expanduser().resolve()
        for index in range(len(CAPACITY_TURN_NAMES)):
            if checkpoint == self._paths_for_index(index)[0]:
                return index, checkpoint
        raise reserve.ReserveCapacityError("capacity checkpoint is outside epoch 35")

    def _verify_prior_turns(self, turn_index: int) -> None:
        for index in range(turn_index):
            capacity_path, sidecar_path, output_path = self._paths_for_index(index)
            sidecar = _load_json(sidecar_path, "prior epoch-38 judge sidecar")
            capacity = _validate_capacity_checkpoint(capacity_path)
            usage = sidecar.get("usage") if isinstance(sidecar, Mapping) else None
            total = usage.get("total_tokens") if isinstance(usage, Mapping) else None
            if (
                sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
                or isinstance(total, bool)
                or not isinstance(total, int)
                or not 0 <= total <= MAXIMUM_TOTAL_TOKENS_PER_TURN
                or capacity.get("turn_ordinal") != index
                or capacity.get("turn_name") != CAPACITY_TURN_NAMES[index]
                or not output_path.is_file()
            ):
                raise reserve.ReserveCapacityError("prior epoch-38 judge turn drifted")


def _client_factory(root: Path) -> Epoch28ReserveCapacityClient:
    return Epoch28ReserveCapacityClient(
        policy_path=root / "capacity-policy.json",
        inner_factory=_inner_factory,
    )


def _validate_capacity_checkpoint(path: Path) -> dict[str, Any]:
    value = _load_json(path, "epoch-38 capacity checkpoint")
    policy_path = path.parents[2] / "capacity-policy.json"
    policy = reserve.load_reserve_capacity_policy(policy_path)
    expected_index = 0 if path.name == "ab.capacity.json" else 1
    required = {
        "schema_version": reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
        "policy_path": str(policy_path.resolve()),
        "policy_sha256": _sha256_file(policy_path),
        "phase_id": STEP_ID,
        "turn_name": CAPACITY_TURN_NAMES[expected_index],
        "turn_ordinal": expected_index,
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "rate_limit_reached_type": None,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "cleared_for_semantic_turn": True,
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
        "retry_checkpoint_reuse_allowed": False,
    }
    if not isinstance(value, Mapping) or any(value.get(key) != expected for key, expected in required.items()):
        raise Epoch38QualityError("epoch-38 capacity checkpoint drifted")
    used = value.get("primary_used_percent")
    remaining = value.get("primary_remaining_percent")
    remaining_turns = value.get("remaining_turn_count")
    evaluation = reserve.evaluate_reserve_capacity(
        {"primary_used_percent": used, "rate_limit_reached_type": None},
        policy=policy,
        remaining_turn_count=remaining_turns,
    )
    for key in (
        "primary_remaining_percent",
        "usable_percent_above_reserve",
        "remaining_turn_count",
        "projected_remaining_tokens",
        "projected_remaining_quota_points",
        "projected_terminal_remaining_percent",
    ):
        if value.get(key) != evaluation[key]:
            raise Epoch38QualityError("epoch-38 capacity arithmetic drifted")
    if remaining != 100 - used:
        raise Epoch38QualityError("epoch-38 capacity remaining percentage drifted")
    return dict(value)


def _launch_receipt(root: Path) -> dict[str, Any]:
    path = root / "launch-receipt.json"
    if path.is_file():
        return _validate_launch_receipt(root)
    value = {
        "schema_version": LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "pool": _record(root / "shared-witness-pool.private.json"),
        "mapping": _record(root / "private-witness-mapping.private.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "variant_order": ["ab", "ba"],
        "model": MODEL,
        "effort": EFFORT,
        "transport": TRANSPORT,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": SEMANTIC_RETRY_COUNT,
        "production_mutated": False,
        "holdout_authorized": False,
    }
    _write_immutable_json(path, value)
    return _validate_launch_receipt(root)


def _validate_launch_receipt(root: Path) -> dict[str, Any]:
    value = _load_json(root / "launch-receipt.json", "epoch-38 launch receipt")
    if (
        value.get("schema_version") != LAUNCH_VERSION
        or not isinstance(value.get("launched_at"), str)
        or not value.get("launched_at")
        or value.get("thread_id") != THREAD_ID
        or value.get("plan_epoch") != PLAN_EPOCH
        or value.get("step_id") != STEP_ID
        or value.get("runtime_lock") != _record(root / "runtime-lock.json")
        or value.get("pool") != _record(root / "shared-witness-pool.private.json")
        or value.get("mapping") != _record(root / "private-witness-mapping.private.json")
        or value.get("attempt_spec") != _record(root / "attempt-spec.json")
        or value.get("capacity_policy") != _record(root / "capacity-policy.json")
        or value.get("variant_order") != ["ab", "ba"]
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("transport") != TRANSPORT
        or value.get("semantic_model_call_cap") != MODEL_CALL_CAP
        or value.get("semantic_retry_count") != 0
        or value.get("production_mutated") is not False
        or value.get("holdout_authorized") is not False
    ):
        raise Epoch38QualityError("epoch-38 launch receipt drifted")
    return dict(value)


def _valid_usage(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise Epoch38QualityError("judge usage is missing")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise Epoch38QualityError("judge usage is malformed")
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"] != result["input_tokens"] + result["output_tokens"]
    ):
        raise Epoch38QualityError("judge usage arithmetic drifted")
    return result


def _turn_material(root: Path, name: str, pool: Mapping[str, Any]) -> tuple[str, dict[str, Any], str]:
    variant = judge.build_judge_variants(pool)[name]
    prompt = judge.build_judge_prompt(variant)
    schema = judge.semantic_judge_output_schema(variant)
    base = judge.judge_base_instructions()
    if (
        (root / f"requests/prompt-{name}.private.md").read_text(encoding="utf-8") != prompt
        or _load_json(root / f"requests/schema-{name}.json", f"frozen {name} schema") != schema
        or (root / "requests/base-instructions.private.md").read_text(encoding="utf-8") != base
    ):
        raise Epoch38QualityError("epoch-38 frozen judge request drifted")
    return prompt, schema, base


def _validate_completed_turn(
    root: Path, name: str, pool: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, int]]:
    prompt, schema, base = _turn_material(root, name, pool)
    output_path = root / f"judge/output-{name}.private.json"
    sidecar_path = root / f"judge/sidecars/{name}.json"
    try:
        output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
            raw_output_path=output_path,
            sidecar_path=sidecar_path,
            prompt=prompt,
            schema=schema,
            base_instructions=base,
            model=MODEL,
            reasoning_effort=EFFORT,
        )
    except (ValueError, judge.JudgeArtifactError) as exc:
        raise Epoch38QualityError(f"epoch-38 {name} checkpoint drifted") from exc
    required = {
        "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "state": "completed",
        "status": "completed",
        "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "protocol_schema_sha256": codex_app_server.PROTOCOL_SCHEMA_SHA256,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "synthetic_debug_errors": False,
        "recovery_reran_model": False,
        "model": MODEL,
        "effort": EFFORT,
        "batch_size": len(pool["cases"]),
        "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "usage_status": "measured",
        "usage_complete": True,
    }
    if any(sidecar.get(key) != expected for key, expected in required.items()):
        raise Epoch38QualityError(f"epoch-38 {name} managed sidecar contract drifted")
    usage = _valid_usage(sidecar.get("usage"))
    if sidecar.get("thread_total_usage") != usage:
        raise Epoch38QualityError(f"epoch-38 {name} thread usage drifted")
    if usage["total_tokens"] > MAXIMUM_TOTAL_TOKENS_PER_TURN:
        raise Epoch38QualityError(f"epoch-38 {name} exceeded the token ceiling")
    if judge.validate_judge_output(output, judge.build_judge_variants(pool)[name]):
        raise Epoch38QualityError(f"epoch-38 {name} judge output is invalid")
    _validate_capacity_checkpoint(root / f"judge/sidecars/{name}.capacity.json")
    return output, sidecar, usage


def _validate_partial_sidecar(
    root: Path,
    name: str,
    pool: Mapping[str, Any],
    sidecar: Mapping[str, Any],
) -> None:
    prompt, schema, base = _turn_material(root, name, pool)
    schema_text = _canonical_json(schema)
    required = {
        "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "protocol_schema_sha256": codex_app_server.PROTOCOL_SCHEMA_SHA256,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "synthetic_debug_errors": False,
        "recovery_reran_model": False,
        "model": MODEL,
        "effort": EFFORT,
        "batch_size": len(pool["cases"]),
        "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "prompt_sha256": _sha256_text(prompt),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_instructions_sha256": _sha256_text(base),
        "base_instructions_bytes": len(base.encode("utf-8")),
        "output_schema_sha256": _sha256_text(schema_text),
        "output_schema_bytes": len(schema_text.encode("utf-8")),
    }
    if any(sidecar.get(key) != expected for key, expected in required.items()):
        raise Epoch38QualityError(f"partial {name} sidecar lineage drifted")
    output_path = root / f"judge/output-{name}.private.json"
    if Path(str(sidecar.get("output_path") or "")).expanduser().resolve() != output_path:
        raise Epoch38QualityError(f"partial {name} output path drifted")
    thread_id = sidecar.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise Epoch38QualityError(f"partial {name} sidecar lacks thread identity")
    turn_id = sidecar.get("turn_id")
    if turn_id is not None and (not isinstance(turn_id, str) or not turn_id):
        raise Epoch38QualityError(f"partial {name} turn identity is malformed")


def _partial_accounting(root: Path) -> dict[str, Any]:
    measured_usage = {field: 0 for field in USAGE_FIELDS}
    measured_calls = 0
    unknown_calls = 0
    turns = []
    ab_evidence = False
    pool = _load_json(root / "shared-witness-pool.private.json", "epoch-38 pool")
    for name in ("ab", "ba"):
        sidecar_path = root / f"judge/sidecars/{name}.json"
        capacity_path = root / f"judge/sidecars/{name}.capacity.json"
        output_path = root / f"judge/output-{name}.private.json"
        present = [path.is_file() for path in (capacity_path, sidecar_path, output_path)]
        if name == "ba" and any(present) and not ab_evidence:
            raise Epoch38QualityError("BA evidence exists without AB lineage")
        if name == "ab" and any(present):
            ab_evidence = True
        if capacity_path.is_file():
            _validate_capacity_checkpoint(capacity_path)
        if output_path.is_file():
            output = _load_json(output_path, f"partial {name} output")
            if judge.validate_judge_output(output, judge.build_judge_variants(pool)[name]):
                raise Epoch38QualityError(f"partial {name} output is invalid")
        if sidecar_path.is_file():
            sidecar = _load_json(sidecar_path, f"partial {name} sidecar")
            if not isinstance(sidecar, Mapping):
                raise Epoch38QualityError(f"partial {name} sidecar is malformed")
            _validate_partial_sidecar(root, name, pool, sidecar)
            state = sidecar.get("state")
            if state == "completed" and output_path.is_file() and capacity_path.is_file():
                _output, checked, usage = _validate_completed_turn(root, name, pool)
                for field in USAGE_FIELDS:
                    measured_usage[field] += usage[field]
                measured_calls += 1
                turns.append(
                    {
                        "variant": name,
                        "usage_status": "measured",
                        "thread_id": checked["thread_id"],
                        "turn_id": checked["turn_id"],
                        "usage": usage,
                    }
                )
            elif state in {"started", "in_progress", "failed", "completed"}:
                unknown_calls += 1
                turns.append(
                    {
                        "variant": name,
                        "usage_status": "unknown",
                        "thread_id": sidecar.get("thread_id"),
                        "turn_id": sidecar.get("turn_id"),
                    }
                )
            else:
                raise Epoch38QualityError(f"partial {name} sidecar state drifted")
        elif capacity_path.is_file() or output_path.is_file():
            unknown_calls += 1
            turns.append({"variant": name, "usage_status": "unknown"})
    if measured_calls + unknown_calls > MODEL_CALL_CAP:
        raise Epoch38QualityError("epoch-38 partial call count exceeded the cap")
    return {
        "semantic_model_call_count": measured_calls + unknown_calls,
        "measured_model_call_count": measured_calls,
        "unknown_usage_turn_count": unknown_calls,
        "usage_status": (
            "unknown" if unknown_calls else "measured_partial" if measured_calls else "complete"
        ),
        "accounting_complete": unknown_calls == 0,
        "usage": measured_usage,
        "turns": turns,
    }


def _witness_systems(mapping: Mapping[str, Any]) -> dict[str, str]:
    systems: dict[str, str] = {}
    for case in mapping.get("cases") or []:
        for witness in case.get("witnesses") or []:
            witness_id = str(witness.get("witness_id"))
            system_id = str((witness.get("provenance") or {}).get("system_id"))
            if system_id not in {SYSTEM_BASELINE, SYSTEM_CANDIDATE} or witness_id in systems:
                raise Epoch38QualityError("private witness system lineage drifted")
            systems[witness_id] = system_id
    return systems


def _macro_scores(
    *, pool: Mapping[str, Any], mapping: Mapping[str, Any], consensus: Mapping[str, Any]
) -> dict[str, Any]:
    systems = _witness_systems(mapping)
    pool_cases = {str(row["case_id"]): row for row in pool.get("cases") or []}
    mapping_cases = {
        str(row["case_id"]): row.get("case_provenance") or {}
        for row in mapping.get("cases") or []
    }
    totals = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
    case_scores = []
    for row in consensus.get("cases") or []:
        case_id = str(row.get("case_id"))
        if case_id not in pool_cases or row.get("partition_abstained_witness_ids"):
            raise Epoch38QualityWaiting("AB/BA equivalence partition requires adjudication")
        events = {
            witness["witness_id"]: witness["event"]
            for side in ("a", "b")
            for witness in pool_cases[case_id][f"event_set_{side}"]
        }
        support = {
            str(item["witness_id"]): str(item["verdict"])
            for item in row.get("support_results") or []
        }
        groups = [frozenset(group) for group in row.get("equivalence_groups") or []]
        grouped = set().union(*groups) if groups else set()
        if grouped != set(events) or set(support) != set(events):
            raise Epoch38QualityError("shared-reference consensus omits a witness")
        group_by_witness = {
            witness_id: index for index, group in enumerate(groups) for witness_id in group
        }
        reference_units = {
            group_by_witness[witness_id]
            for witness_id, verdict in support.items()
            if verdict == "supported"
            and events[witness_id].get("submitted_evidence_exact") is True
        }
        rows = {}
        for system_id in (SYSTEM_BASELINE, SYSTEM_CANDIDATE):
            submitted_ids = {key for key in events if systems.get(key) == system_id}
            submitted_units = {group_by_witness[key] for key in submitted_ids}
            supported_units = {
                group_by_witness[key]
                for key in submitted_ids
                if support[key] == "supported"
                and events[key].get("submitted_evidence_exact") is True
            }
            precision = (
                len(supported_units) / len(submitted_units)
                if submitted_units
                else (1.0 if not reference_units else 0.0)
            )
            recall = (
                len(supported_units & reference_units) / len(reference_units)
                if reference_units
                else 1.0
            )
            f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
            totals[system_id].append(f1)
            rows[system_id] = {
                "submitted_unit_count": len(submitted_units),
                "supported_unit_count": len(supported_units),
                "shared_reference_unit_count": len(reference_units),
                "strict_precision": round(precision, 6),
                "strict_recall": round(recall, 6),
                "strict_f1": round(f1, 6),
            }
        case_scores.append(
            {
                "case_id": case_id,
                "source_index": mapping_cases.get(case_id, {}).get("source_index"),
                "systems": rows,
            }
        )
    if len(case_scores) != len(pool_cases) or not case_scores:
        raise Epoch38QualityError("macro score does not cover both cases")
    return {
        "systems": {
            system_id: {
                "strict_full_field_macro_f1": round(sum(values) / len(values), 6)
            }
            for system_id, values in totals.items()
        },
        "cases": case_scores,
    }


def _aggregate_completed_usage(
    root: Path, pool: Mapping[str, Any]
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    usage = {field: 0 for field in USAGE_FIELDS}
    turns = []
    identities = set()
    for name in ("ab", "ba"):
        _output, sidecar, per_turn = _validate_completed_turn(root, name, pool)
        identity = (sidecar["thread_id"], sidecar["turn_id"])
        if identity in identities:
            raise Epoch38QualityError("epoch-38 judge thread/turn identity was replayed")
        identities.add(identity)
        for field in USAGE_FIELDS:
            usage[field] += per_turn[field]
        turns.append(
            {
                "variant": name,
                "thread_id": sidecar["thread_id"],
                "turn_id": sidecar["turn_id"],
                "wall_elapsed_seconds": sidecar.get("wall_elapsed_seconds"),
                "usage": per_turn,
            }
        )
    if usage["total_tokens"] > TOTAL_TOKEN_CAP:
        raise Epoch38QualityError("epoch-38 judge token cap exceeded")
    return usage, turns


def _score(root: Path) -> dict[str, Any]:
    pool = _load_json(root / "shared-witness-pool.private.json", "epoch-38 pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "epoch-38 mapping")
    consensus = _load_json(root / "judge/consensus.private.json", "epoch-38 consensus")
    report = _load_json(root / "judge/report.json", "epoch-38 judge report")
    if (
        report.get("schema_version") != judge.JUDGE_RUN_VERSION
        or report.get("state") != "completed"
        or report.get("model") != MODEL
        or report.get("reasoning_effort") != EFFORT
        or report.get("transport") != TRANSPORT
        or report.get("variant_count") != 2
        or report.get("case_count") != len(pool["cases"])
        or report.get("witness_count") != EXPECTED_TOTAL_WITNESSES
        or report.get("spec_sha256") != _sha256_file(root / "judge/judge-spec.json")
        or report.get("pool_sha256")
        != _sha256_file(root / "shared-witness-pool.private.json")
        or report.get("consensus_sha256") != _sha256_file(root / "judge/consensus.private.json")
        or report.get("selection_admissible") is not True
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or report.get("validation_errors") != {"ab": [], "ba": []}
    ):
        raise Epoch38QualityError("epoch-38 judge report drifted")
    usage, turns = _aggregate_completed_usage(root, pool)
    if report.get("usage") != usage:
        raise Epoch38QualityError("epoch-38 report accounting drifted")
    try:
        shared = judge.score_named_systems_against_shared_reference(
            pool=pool,
            private_mapping=mapping,
            consensus=consensus,
            reference_system_ids=None,
        )
    except ValueError as exc:
        if "abstained semantic equivalence partition" in str(exc):
            raise Epoch38QualityWaiting(
                "AB/BA disagreement requires separately frozen adjudication"
            ) from exc
        raise Epoch38QualityError("epoch-38 shared-reference score failed") from exc
    if set(shared.get("reference_system_ids") or []) != {SYSTEM_BASELINE, SYSTEM_CANDIDATE}:
        raise Epoch38QualityError("epoch-38 shared reference is not symmetric")
    macro = _macro_scores(pool=pool, mapping=mapping, consensus=consensus)
    baseline_macro = macro["systems"][SYSTEM_BASELINE]["strict_full_field_macro_f1"]
    candidate_macro = macro["systems"][SYSTEM_CANDIDATE]["strict_full_field_macro_f1"]
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    checks = {
        "candidate_strict_full_field_macro_f1_gte_0_97": (
            candidate_macro >= QUALITY_THRESHOLD
        ),
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "full_six_case_token_projection_lte_frozen_ceiling": (
            FULL_SIX_CASE_TOKEN_PROJECTION <= FULL_SIX_CASE_TOKEN_CEILING
        ),
        "exact_evidence_rate_1": exact_count == witness_count == EXPECTED_TOTAL_WITNESSES,
        "judge_accounting_complete_under_cap": usage["total_tokens"] <= TOTAL_TOKEN_CAP,
    }
    failed = sorted(key for key, passed in checks.items() if passed is not True)
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "candidate_strict_full_field_macro_f1": candidate_macro,
        "baseline_strict_full_field_macro_f1": baseline_macro,
        "candidate_noninferiority_delta": round(candidate_macro - baseline_macro, 6),
        "shared_reference_unit_count": shared["reference_unit_count"],
        "reference_system_ids": shared["reference_system_ids"],
        "systems": shared["systems"],
        "macro": macro,
        "exact_evidence_rate": round(exact_count / witness_count, 6),
        "semantic_model_call_count": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "judge_usage": usage,
        "judge_turns": turns,
        "full_six_case_token_projection": FULL_SIX_CASE_TOKEN_PROJECTION,
        "full_six_case_token_ceiling": FULL_SIX_CASE_TOKEN_CEILING,
        "judge_tokens_included_in_production_formula": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _optional_record(path: Path) -> dict[str, Any] | None:
    return _record(path) if path.is_file() else None


def _receipt_records(root: Path) -> dict[str, Any]:
    records = {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "pool": _record(root / "shared-witness-pool.private.json"),
        "mapping": _record(root / "private-witness-mapping.private.json"),
        "candidate_lineage": _record(root / "candidate-lineage.json"),
        "launch": _optional_record(root / "launch-receipt.json"),
        "judge_spec": _optional_record(root / "judge/judge-spec.json"),
        "output_ab": _optional_record(root / "judge/output-ab.private.json"),
        "output_ba": _optional_record(root / "judge/output-ba.private.json"),
        "sidecar_ab": _optional_record(root / "judge/sidecars/ab.json"),
        "sidecar_ba": _optional_record(root / "judge/sidecars/ba.json"),
        "capacity_ab": _optional_record(root / "judge/sidecars/ab.capacity.json"),
        "capacity_ba": _optional_record(root / "judge/sidecars/ba.capacity.json"),
        "consensus": _optional_record(root / "judge/consensus.private.json"),
        "judge_report": _optional_record(root / "judge/report.json"),
        "score": _optional_record(root / "shared-reference-score.json"),
    }
    return records


def _receipt(
    root: Path,
    *,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    score: Mapping[str, Any] | None = None,
    diagnostic: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    usage = accounting.get("usage") or {field: 0 for field in USAGE_FIELDS}
    current_calls = int(accounting.get("semantic_model_call_count") or 0)
    unknown = int(accounting.get("unknown_usage_turn_count") or 0)
    measured_total = int(usage.get("total_tokens") or 0)
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": terminal_reason,
        "created_at": now_iso(),
        "output_root": str(root),
        "semantic_model_call_count": current_calls,
        "semantic_retry_count": 0,
        "measured_model_call_count": int(accounting.get("measured_model_call_count") or 0),
        "unknown_usage_turn_count": unknown,
        "usage_status": accounting.get("usage_status"),
        "accounting_complete": accounting.get("accounting_complete") is True,
        "usage": usage,
        "turns": accounting.get("turns") or [],
        "aggregate_architecture_semantic_model_call_count": (
            PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT + current_calls
        ),
        "aggregate_architecture_unknown_usage_turn_count": (
            PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT + unknown
        ),
        "aggregate_architecture_measured_total_tokens": (
            PREDECESSOR_MEASURED_TOTAL_TOKENS + measured_total
        ),
        "predecessor_semantic_model_call_count": PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT,
        "predecessor_unknown_usage_turn_count": PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT,
        "predecessor_measured_total_tokens": PREDECESSOR_MEASURED_TOTAL_TOKENS,
        "score": copy.deepcopy(dict(score)) if score is not None else None,
        "failed_checks": list(score.get("failed_checks") or []) if score else [],
        "diagnostic": copy.deepcopy(dict(diagnostic)) if diagnostic else None,
        "quality_measured_by_this_step": score is not None,
        "quality_passed": bool(score and score.get("passed") is True),
        "next_authorized_action": (
            "freeze_epoch37_dual_pass_omission_audit_candidate_for_next_frozen_gate"
            if state == "passed"
            else "reject_dual_pass_omission_audit_architecture_without_field_repair"
            if state == "rejected"
            else "versioned_no_replay_recovery_or_adjudication_only"
        ),
        "full_six_case_token_projection": FULL_SIX_CASE_TOKEN_PROJECTION,
        "full_six_case_token_ceiling": FULL_SIX_CASE_TOKEN_CEILING,
        "judge_tokens_included_in_production_formula": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "artifact_records": _receipt_records(root),
    }


def _write_terminal(root: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    _write_immutable_json(root / "plan-step-receipt.json", value)
    _write_immutable_json(root / "terminal.json", value)
    return verify_receipt(root, acquire_lock=False)


def _recover_terminal_mirror(root: Path) -> dict[str, Any] | None:
    receipt_path = root / "plan-step-receipt.json"
    terminal_path = root / "terminal.json"
    if not receipt_path.is_file() and not terminal_path.is_file():
        return None
    if receipt_path.is_file() and terminal_path.is_file():
        return verify_receipt(root, acquire_lock=False)
    existing = receipt_path if receipt_path.is_file() else terminal_path
    missing = terminal_path if existing == receipt_path else receipt_path
    value = _load_json(existing, "single epoch-38 terminal mirror")
    _validate_receipt_payload(root, value)
    _write_immutable_json(missing, value)
    return verify_receipt(root, acquire_lock=False)


def _validate_receipt_payload(root: Path, receipt: Mapping[str, Any]) -> None:
    if (
        receipt.get("schema_version") != RECEIPT_VERSION
        or receipt.get("thread_id") != THREAD_ID
        or receipt.get("plan_epoch") != PLAN_EPOCH
        or receipt.get("step_id") != STEP_ID
        or receipt.get("state") not in {"passed", "rejected", "waiting"}
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("full_six_case_token_projection") != FULL_SIX_CASE_TOKEN_PROJECTION
        or receipt.get("full_six_case_token_ceiling") != FULL_SIX_CASE_TOKEN_CEILING
        or receipt.get("judge_tokens_included_in_production_formula") is not False
        or receipt.get("artifact_records") != _receipt_records(root)
    ):
        raise Epoch38QualityError("epoch-38 terminal receipt drifted")
    current_calls = receipt.get("semantic_model_call_count")
    measured_calls = receipt.get("measured_model_call_count")
    unknown = receipt.get("unknown_usage_turn_count")
    if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in (
        current_calls, measured_calls, unknown
    )) or current_calls != measured_calls + unknown or current_calls > MODEL_CALL_CAP:
        raise Epoch38QualityError("epoch-38 receipt call accounting drifted")
    usage = _valid_usage(receipt.get("usage"))
    if (
        receipt.get("predecessor_semantic_model_call_count")
        != PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT
        or receipt.get("predecessor_unknown_usage_turn_count")
        != PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT
        or receipt.get("predecessor_measured_total_tokens")
        != PREDECESSOR_MEASURED_TOTAL_TOKENS
        or receipt.get("aggregate_architecture_semantic_model_call_count")
        != PREDECESSOR_SEMANTIC_MODEL_CALL_COUNT + current_calls
        or receipt.get("aggregate_architecture_unknown_usage_turn_count")
        != PREDECESSOR_UNKNOWN_USAGE_TURN_COUNT + unknown
        or receipt.get("aggregate_architecture_measured_total_tokens")
        != PREDECESSOR_MEASURED_TOTAL_TOKENS + usage["total_tokens"]
    ):
        raise Epoch38QualityError("epoch-38 aggregate accounting drifted")
    quality_terminal = receipt.get("score") is not None
    if receipt.get("state") == "passed" or quality_terminal:
        score = _score(root)
        if (
            receipt.get("semantic_model_call_count") != MODEL_CALL_CAP
            or receipt.get("measured_model_call_count") != MODEL_CALL_CAP
            or receipt.get("unknown_usage_turn_count") != 0
            or receipt.get("accounting_complete") is not True
            or receipt.get("usage") != score["judge_usage"]
            or receipt.get("score") != score
            or receipt.get("quality_measured_by_this_step") is not True
            or receipt.get("quality_passed") is not (score["passed"] is True)
            or receipt.get("state") != ("passed" if score["passed"] else "rejected")
        ):
            raise Epoch38QualityError("epoch-38 quality receipt drifted")
    else:
        partial = _partial_accounting(root)
        if (
            receipt.get("semantic_model_call_count") != partial["semantic_model_call_count"]
            or receipt.get("measured_model_call_count") != partial["measured_model_call_count"]
            or receipt.get("unknown_usage_turn_count") != partial["unknown_usage_turn_count"]
            or receipt.get("usage") != partial["usage"]
            or receipt.get("quality_measured_by_this_step") is not False
            or receipt.get("quality_passed") is not False
            or receipt.get("score") is not None
        ):
            raise Epoch38QualityError("epoch-38 waiting receipt drifted")


def verify_receipt(root: Path = DEFAULT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    output_root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(output_root / "runtime-lock.json", acquire_lock=False)
        receipt = _load_json(output_root / "plan-step-receipt.json", "epoch-38 receipt")
        terminal = _load_json(output_root / "terminal.json", "epoch-38 terminal")
        if receipt != terminal:
            raise Epoch38QualityError("epoch-38 terminal mirrors differ")
        _validate_receipt_payload(output_root, receipt)
        return dict(receipt)

    if acquire_lock:
        with _process_lock(output_root):
            return verify()
    return verify()


def _semantic_artifacts_exist(root: Path) -> bool:
    judge_root = root / "judge"
    return judge_root.exists() and any(path.is_file() for path in judge_root.rglob("*"))


def _reject_external_auth_material() -> None:
    present = sorted(name for name in _FORBIDDEN_AUTH_ENVIRONMENT if os.environ.get(name))
    if present:
        raise Epoch38QualityError(
            "managed ChatGPT execution rejects API-key/raw-session auth: "
            + ", ".join(present)
        )


async def _run_unlocked(
    root: Path,
    *,
    judge_runner: Callable[..., Any],
    client_factory: Callable[[], Any],
) -> dict[str, Any]:
    recovered = _recover_terminal_mirror(root)
    if recovered is not None:
        return recovered
    verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
    launch_exists = (root / "launch-receipt.json").is_file()
    artifacts_exist = _semantic_artifacts_exist(root)
    if artifacts_exist and not launch_exists:
        raise Epoch38QualityError("semantic artifacts exist without an epoch-38 launch")
    if launch_exists:
        _validate_launch_receipt(root)
        report_exists = (root / "judge/report.json").is_file()
        if report_exists:
            try:
                score = _score(root)
            except Epoch38QualityWaiting as exc:
                accounting = _partial_accounting(root)
                return _write_terminal(
                    root,
                    _receipt(
                        root,
                        state="waiting",
                        terminal_reason="epoch38_ab_ba_disagreement_requires_versioned_adjudication",
                        accounting=accounting,
                        diagnostic={"class": "semantic_disagreement", "message": str(exc)},
                    ),
                )
            _write_immutable_json(root / "shared-reference-score.json", score)
            accounting = {
                "semantic_model_call_count": MODEL_CALL_CAP,
                "measured_model_call_count": MODEL_CALL_CAP,
                "unknown_usage_turn_count": 0,
                "usage_status": "complete",
                "accounting_complete": True,
                "usage": score["judge_usage"],
                "turns": score["judge_turns"],
            }
            return _write_terminal(
                root,
                _receipt(
                    root,
                    state="passed" if score["passed"] else "rejected",
                    terminal_reason=(
                        "epoch38_two_case_quality_gate_passed"
                        if score["passed"]
                        else "epoch38_two_case_quality_gate_rejected"
                    ),
                    accounting=accounting,
                    score=score,
                ),
            )
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="waiting",
                terminal_reason="epoch38_partial_or_interrupted_ab_ba_preserved_without_replay",
                accounting=accounting,
                diagnostic={"class": "partial_semantic_attempt", "replay_allowed": False},
            ),
        )

    _launch_receipt(root)
    try:
        await judge_runner(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=MODEL,
            reasoning_effort=EFFORT,
            timeout_seconds=TIMEOUT_SECONDS,
            client_factory=client_factory,
        )
    except codex_app_server.AppServerStructuredOutputError as exc:
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="rejected",
                terminal_reason="epoch38_judge_structured_output_rejected",
                accounting=accounting,
                diagnostic={"class": type(exc).__name__, "semantic_retry_allowed": False},
            ),
        )
    except judge.JudgeAttemptFailed as exc:
        accounting = _partial_accounting(root)
        rejected = exc.error_class == "structured_output_invalid"
        return _write_terminal(
            root,
            _receipt(
                root,
                state="rejected" if rejected else "waiting",
                terminal_reason=(
                    "epoch38_judge_structured_output_rejected"
                    if rejected
                    else "epoch38_judge_operational_attempt_waiting_no_replay"
                ),
                accounting=accounting,
                diagnostic={"class": exc.error_class, "variant": exc.variant},
            ),
        )
    except (
        reserve.ReserveCapacityError,
        codex_app_server.AppServerTurnTimeout,
        codex_app_server.AppServerProcessDied,
        codex_app_server.AppServerRPCError,
        codex_app_server.AppServerAuthError,
        codex_app_server.AppServerRecoveryRequired,
        OSError,
        asyncio.TimeoutError,
    ) as exc:
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="waiting",
                terminal_reason="epoch38_judge_operational_attempt_waiting_no_replay",
                accounting=accounting,
                diagnostic={"class": type(exc).__name__},
            ),
        )
    except judge.JudgeArtifactError as exc:
        if "semantic judge output failed exact validation" not in str(exc):
            raise
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="rejected",
                terminal_reason="epoch38_judge_exact_validation_rejected",
                accounting=accounting,
                diagnostic={"class": type(exc).__name__},
            ),
        )
    return await _run_unlocked(
        root, judge_runner=judge_runner, client_factory=client_factory
    )


async def run(
    root: Path = DEFAULT_ROOT,
    *,
    judge_runner: Callable[..., Any] = judge.run_app_server_semantic_judge,
    client_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if output_root == DEFAULT_ROOT and (
        judge_runner is not judge.run_app_server_semantic_judge or client_factory is not None
    ):
        raise Epoch38QualityError(
            "the live epoch-38 root forbids injected judge runners or clients"
        )
    _reject_external_auth_material()
    with _process_lock(output_root):
        return await _run_unlocked(
            output_root,
            judge_runner=judge_runner,
            client_factory=client_factory or (lambda: _client_factory(output_root)),
        )


def status(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if (output_root / "plan-step-receipt.json").is_file():
        receipt = verify_receipt(output_root)
        return {
            "state": receipt["state"],
            "terminal_reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["semantic_model_call_count"],
            "usage": receipt["usage"],
        }
    if (output_root / "launch-receipt.json").is_file():
        return {"state": "in_progress_or_interrupted", **_partial_accounting(output_root)}
    if (output_root / "runtime-lock.json").is_file():
        verify_runtime_lock(output_root / "runtime-lock.json")
        return {"state": "frozen_zero_call", "semantic_model_call_count": 0}
    return {"state": "absent", "semantic_model_call_count": 0}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("freeze", "verify-runtime", "run", "verify-receipt", "status"):
        commands.add_parser(command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_run(args.root)
        elif args.command == "verify-runtime":
            result = verify_runtime_lock(args.root / "runtime-lock.json")
        elif args.command == "run":
            result = asyncio.run(run(args.root))
        elif args.command == "verify-receipt":
            result = verify_receipt(args.root)
        else:
            result = status(args.root)
    except Epoch38QualityWaiting as exc:
        print(json.dumps({"state": "waiting", "error": str(exc)}, sort_keys=True))
        return 75
    except Epoch38QualityError as exc:
        print(json.dumps({"state": "invalid", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
