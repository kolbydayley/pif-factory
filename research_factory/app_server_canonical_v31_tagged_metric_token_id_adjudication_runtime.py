from __future__ import annotations

"""Epoch-29 one-call origin-neutral adjudication of epoch-28 disagreement."""

import argparse
import asyncio
import copy
import fcntl
import hashlib
import json
import math
import os
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_canonical_v31_tagged_metric_token_id_quality_runtime as epoch28
from . import app_server_llm_judge as judge
from . import codex_app_server
from .util import now_iso


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
EPOCH28_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch28-tagged-metric-token-id-quality-v1"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT
    / "canonical-v31-epoch29-tagged-metric-token-id-adjudication-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch29-tagged-metric-token-id-adjudication-v29.json"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v29.json"
).resolve()
SOURCE_CAPACITY_POLICY = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()
PINNED_CODEX = (
    PROJECT_ROOT
    / "work/app-server-development-v2/pinned-runtime/codex-0.144.1/bin/codex"
).resolve()
INSTRUCTION_SOURCE_PATH = Path("/Users/kolbydayley/.codex/AGENTS.md").resolve()

SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_adjudication_v29"
LOCK_VERSION = "pif_canonical_v31_tagged_metric_token_id_adjudication_lock_v1"
LAUNCH_VERSION = "pif_canonical_v31_tagged_metric_token_id_adjudication_launch_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = epoch28.THREAD_ID
PLAN_EPOCH = 29
STEP_ID = "canonical_v31_epoch29_tagged_metric_token_id_adjudication_v29"
MODEL = epoch28.MODEL
EFFORT = epoch28.EFFORT
TRANSPORT = epoch28.TRANSPORT
TIMEOUT_SECONDS = 1200.0
MODEL_CALL_CAP = 1
SEMANTIC_RETRY_COUNT = 0
NEW_TOTAL_TOKEN_CAP = 100_000
QUALITY_TOTAL_TOKEN_CAP = 300_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PROJECTED_PHASE_QUOTA_POINTS = math.ceil(
    NEW_TOTAL_TOKEN_CAP * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
QUALITY_THRESHOLD = epoch28.QUALITY_THRESHOLD
EXPECTED_TOTAL_WITNESSES = epoch28.EXPECTED_TOTAL_WITNESSES
EXPECTED_BASELINE_WITNESSES = epoch28.EXPECTED_BASELINE_WITNESSES
EXPECTED_CANDIDATE_WITNESSES = epoch28.EXPECTED_CANDIDATE_WITNESSES
EXPECTED_DISAGREEMENT_CASE_IDS = ("jcase_42e27c0a64b9807fa07f378d",)
CAPACITY_TURN_NAMES = ("epoch29_quality_adjudication",)
TURN_DIRECTORY_NAME = "epoch29-quality-adjudication"
EXPECTED_INSTRUCTION_SOURCES_COUNT = epoch28.EXPECTED_INSTRUCTION_SOURCES_COUNT
EXPECTED_INSTRUCTION_SOURCES_SHA256 = epoch28.EXPECTED_INSTRUCTION_SOURCES_SHA256
PREDECESSOR_USAGE = {
    "input_tokens": 107_482,
    "cached_input_tokens": 2_816,
    "output_tokens": 34_580,
    "reasoning_output_tokens": 13_206,
    "total_tokens": 142_062,
}
PREDECESSOR_AGGREGATE_MODEL_CALLS = 7
PREDECESSOR_AGGREGATE_UNKNOWN_USAGE = 2
PREDECESSOR_AGGREGATE_MEASURED_TOKENS = 216_388
PROCESS_LOCK_NAME = ".canonical-v31-epoch29-adjudication.lock"
USAGE_FIELDS = epoch28.USAGE_FIELDS
_FORBIDDEN_AUTH_ENVIRONMENT = epoch28._FORBIDDEN_AUTH_ENVIRONMENT  # noqa: SLF001


class Epoch29AdjudicationError(RuntimeError):
    """Epoch-29 contract, lineage, or immutable evidence drifted."""


class Epoch29AdjudicationWaiting(Epoch29AdjudicationError):
    """The zero-retry adjudication stopped without a quality verdict."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Epoch29AdjudicationError(f"cannot read {label}") from exc


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_text(_canonical_json([dict(row) for row in records]))


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise Epoch29AdjudicationError(f"frozen {path.name} drifted")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def _write_immutable_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise Epoch29AdjudicationError(f"frozen {path.name} drifted")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


@contextmanager
def _process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / PROCESS_LOCK_NAME
    with path.open("a+", encoding="ascii") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Epoch29AdjudicationWaiting(
                "another epoch-29 adjudication process owns this root"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _runtime_paths() -> list[Path]:
    return [
        Path(__file__).resolve(),
        Path(epoch28.__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(reserve.__file__).resolve(),
        Path(app_server_capacity.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
        PINNED_CODEX,
        INSTRUCTION_SOURCE_PATH,
    ]


def _expected_predecessor_records() -> list[dict[str, Any]]:
    receipt = epoch28.verify_receipt(EPOCH28_ROOT)
    records = [
        _record(EPOCH28_ROOT / "runtime-lock.json"),
        _record(EPOCH28_ROOT / "plan-step-receipt.json"),
        _record(EPOCH28_ROOT / "terminal.json"),
    ]
    artifact_records = receipt.get("artifact_records")
    if not isinstance(artifact_records, Mapping):
        raise Epoch29AdjudicationError("epoch-28 artifact closure is missing")
    for role in sorted(artifact_records):
        row = artifact_records[role]
        if row is not None:
            if not isinstance(row, Mapping) or not _verify_record(row):
                raise Epoch29AdjudicationError(
                    f"epoch-28 artifact closure drifted: {role}"
                )
            records.append(dict(row))
    deduplicated: dict[tuple[str, str, int], dict[str, Any]] = {}
    for row in records:
        key = (str(row["path"]), str(row["sha256"]), int(row["size_bytes"]))
        deduplicated[key] = row
    return list(deduplicated.values())


def _validate_epoch28_terminal() -> dict[str, Any]:
    receipt = epoch28.verify_receipt(EPOCH28_ROOT)
    if (
        receipt.get("state") != "waiting"
        or receipt.get("terminal_reason")
        != "epoch28_ab_ba_disagreement_requires_versioned_adjudication"
        or receipt.get("semantic_model_call_count") != 2
        or receipt.get("measured_model_call_count") != 2
        or receipt.get("unknown_usage_turn_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("usage") != PREDECESSOR_USAGE
        or receipt.get("quality_measured_by_this_step") is not False
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise Epoch29AdjudicationError("epoch-28 terminal contract drifted")
    return receipt


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "epoch-29 semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "epoch-29 directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if (
        set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
        or not isinstance(step, Mapping)
        or set(step)
        != {
            "step_id",
            "state",
            "max_model_calls",
            "max_total_tokens",
            "expected_receipt_path",
            "accepted_receipt_states",
            "directive_path",
            "directive_sha256",
        }
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != MODEL_CALL_CAP
        or step.get("max_total_tokens") != NEW_TOTAL_TOKEN_CAP
        or step.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or step.get("directive_path") != str(DIRECTIVE_PATH)
        or step.get("directive_sha256") != _sha256_file(DIRECTIVE_PATH)
    ):
        raise Epoch29AdjudicationError("epoch-29 semantic plan drifted")
    adjudication = directive.get("adjudication_contract")
    capacity = directive.get("capacity_contract")
    instruction = directive.get("instruction_contract")
    predecessor = directive.get("predecessor_contract")
    promotion = directive.get("promotion_contract")
    quality = directive.get("quality_contract")
    if (
        directive.get("schema_version")
        != "pif_evaluation_epoch29_tagged_metric_token_id_adjudication_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authority") != "direct_user_instruction"
        or directive.get("authorized_by") != "kolby"
        or directive.get("authorization_statement")
        != "You have my full permission to continue. No need to seek out my approval anymore."
        or directive.get("state")
        != "authorized_for_zero_call_freeze_then_one_bounded_origin_neutral_adjudication"
        or directive.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or not isinstance(adjudication, Mapping)
        or adjudication.get("observable_disagreement_case_ids")
        != list(EXPECTED_DISAGREEMENT_CASE_IDS)
        or adjudication.get("origin_neutral") is not True
        or adjudication.get("prior_ab_ba_replay_allowed") is not False
        or adjudication.get("model") != MODEL
        or adjudication.get("effort") != EFFORT
        or adjudication.get("adjudication_call_cap") != MODEL_CALL_CAP
        or adjudication.get("semantic_retry_cap") != 0
        or adjudication.get("adjudication_total_token_cap") != NEW_TOTAL_TOKEN_CAP
        or not isinstance(capacity, Mapping)
        or capacity.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or capacity.get("maximum_total_tokens_per_turn") != NEW_TOTAL_TOKEN_CAP
        or capacity.get("phase_total_token_bound") != NEW_TOTAL_TOKEN_CAP
        or capacity.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or capacity.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or not isinstance(instruction, Mapping)
        or instruction.get("instruction_sources_count")
        != EXPECTED_INSTRUCTION_SOURCES_COUNT
        or instruction.get("instruction_sources_sha256")
        != EXPECTED_INSTRUCTION_SOURCES_SHA256
        or instruction.get("instruction_source_paths") != [str(INSTRUCTION_SOURCE_PATH)]
        or instruction.get("instruction_source_records")
        != [_record(INSTRUCTION_SOURCE_PATH)]
        or not isinstance(predecessor, Mapping)
        or predecessor.get("state") != "waiting"
        or predecessor.get("terminal_reason")
        != "epoch28_ab_ba_disagreement_requires_versioned_adjudication"
        or predecessor.get("measured_model_call_count") != 2
        or predecessor.get("measured_total_tokens") != PREDECESSOR_USAGE["total_tokens"]
        or predecessor.get("quality_measured") is not False
        or predecessor.get("runtime_lock") != _record(EPOCH28_ROOT / "runtime-lock.json")
        or predecessor.get("receipt")
        != _record(EPOCH28_ROOT / "plan-step-receipt.json")
        or predecessor.get("terminal") != _record(EPOCH28_ROOT / "terminal.json")
        or not isinstance(quality, Mapping)
        or quality.get("all_event_witness_count") != EXPECTED_TOTAL_WITNESSES
        or quality.get("baseline_witness_count") != EXPECTED_BASELINE_WITNESSES
        or quality.get("candidate_witness_count") != EXPECTED_CANDIDATE_WITNESSES
        or quality.get("quality_threshold") != QUALITY_THRESHOLD
        or quality.get("candidate_noninferiority_required") is not True
        or quality.get("reference_system_ids") is not None
        or quality.get("exact_evidence_rate_required") != 1.0
        or quality.get("semantic_prefilter_allowed") is not False
        or quality.get("deterministic_semantic_pruning_allowed") is not False
        or quality.get("judge_total_token_cap_including_epoch28")
        != QUALITY_TOTAL_TOKEN_CAP
        or not isinstance(promotion, Mapping)
        or promotion.get("full_six_case_token_projection")
        != epoch28.FULL_SIX_CASE_TOKEN_PROJECTION
        or promotion.get("full_six_case_token_ceiling")
        != epoch28.FULL_SIX_CASE_TOKEN_CEILING
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
        raise Epoch29AdjudicationError("epoch-29 directive values drifted")
    receipt = _validate_epoch28_terminal()
    return {
        "plan": plan,
        "directive": directive,
        "predecessor_receipt": receipt,
        "receipt_path": Path(str(directive["expected_receipt_path"])).resolve(),
    }


def _base_bundle() -> dict[str, Any]:
    _validate_epoch28_terminal()
    pool = _load_json(EPOCH28_ROOT / "shared-witness-pool.private.json", "epoch-28 pool")
    mapping = _load_json(
        EPOCH28_ROOT / "private-witness-mapping.private.json", "epoch-28 mapping"
    )
    outputs = {
        name: _load_json(
            EPOCH28_ROOT / f"judge/output-{name}.private.json", f"epoch-28 {name} output"
        )
        for name in ("ab", "ba")
    }
    variants = judge.build_judge_variants(pool)
    for name in ("ab", "ba"):
        if judge.validate_judge_output(outputs[name], variants[name]):
            raise Epoch29AdjudicationError(f"epoch-28 {name} output drifted")
    consensus = judge.combine_judge_consensus(pool, outputs)
    if consensus != _load_json(
        EPOCH28_ROOT / "judge/consensus.private.json", "epoch-28 consensus"
    ):
        raise Epoch29AdjudicationError("epoch-28 consensus reconstruction drifted")
    return {
        "pool": pool,
        "mapping": mapping,
        "outputs": outputs,
        "consensus": consensus,
    }


def observable_disagreement_case_ids(
    pool: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    variants = judge.build_judge_variants(pool)
    for name in ("ab", "ba"):
        if judge.validate_judge_output(outputs[name], variants[name]):
            raise Epoch29AdjudicationError("base judge output failed exact validation")
    left = judge._normalized_case_output(outputs["ab"], orientation="ab")  # noqa: SLF001
    right = judge._normalized_case_output(outputs["ba"], orientation="ba")  # noqa: SLF001
    disagreement = []
    for case_id in left:
        support_changed = any(
            left[case_id]["support"][witness_id]["verdict"]
            != right[case_id]["support"][witness_id]["verdict"]
            for witness_id in left[case_id]["support"]
        )
        partition_changed = (
            left[case_id]["equivalence_partition"]
            != right[case_id]["equivalence_partition"]
        )
        if support_changed or partition_changed:
            disagreement.append(case_id)
    if sorted(disagreement) != sorted(EXPECTED_DISAGREEMENT_CASE_IDS):
        raise Epoch29AdjudicationError("observable disagreement case set drifted")
    return sorted(disagreement)


def build_adjudication_variant(
    pool: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    selected = set(case_ids)
    cases = []
    for case in pool["cases"]:
        if case["case_id"] not in selected:
            continue
        witnesses = sorted(
            [*case["event_set_a"], *case["event_set_b"]],
            key=lambda row: str(row["witness_id"]),
        )
        cases.append(
            {
                "case_id": case["case_id"],
                "source_excerpt": case["source_excerpt"],
                "event_set_a": copy.deepcopy(witnesses),
                "event_set_b": [],
            }
        )
    if not cases or {row["case_id"] for row in cases} != selected:
        raise Epoch29AdjudicationError("adjudication case coverage drifted")
    variant = {
        "schema_version": judge.JUDGE_VARIANT_VERSION,
        "variant": "side_free_adjudication",
        "orientation": "ab",
        "cases": cases,
    }
    serialized = _canonical_json(variant)
    if epoch28.SYSTEM_BASELINE in serialized or epoch28.SYSTEM_CANDIDATE in serialized:
        raise Epoch29AdjudicationError("system origin leaked into adjudication")
    return variant


def _request_material() -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    bundle = _base_bundle()
    case_ids = observable_disagreement_case_ids(bundle["pool"], bundle["outputs"])
    variant = build_adjudication_variant(bundle["pool"], case_ids)
    base = judge.judge_base_instructions()
    prompt = (
        "This is the sole origin-neutral adjudication for an observed AB/BA disagreement. "
        "All witnesses are pooled on one presentation side. Independently decide complete-"
        "event source support and the full exact-evidence semantic-equivalence partition. "
        "Do not vote between, infer, or reference prior judge outputs. Abstain when the "
        "source cannot resolve a decision. The measured total-token acceptance ceiling for "
        "this turn is 100000.\n\n"
        + judge.build_judge_prompt(variant)
    )
    schema = judge.semantic_judge_output_schema(variant)
    return variant, base, prompt, schema


def _capacity_policy(root: Path) -> Path:
    policy = dict(reserve.load_reserve_capacity_policy(SOURCE_CAPACITY_POLICY))
    policy.update(
        {
            "schema_version": reserve.RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": STEP_ID,
            "semantic_output_root": str(root.resolve()),
            "ordered_turn_names": list(CAPACITY_TURN_NAMES),
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "maximum_total_tokens_per_turn": NEW_TOTAL_TOKEN_CAP,
            "phase_total_token_bound": NEW_TOTAL_TOKEN_CAP,
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
        raise Epoch29AdjudicationError("unfrozen epoch-29 root is not empty")
    contract = load_contract()
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise Epoch29AdjudicationError("epoch-29 output root differs from contract")
    variant, base, prompt, schema = _request_material()
    variant_path = root / "requests/variant.private.json"
    base_path = root / "requests/base-instructions.private.md"
    prompt_path = root / "requests/prompt.private.md"
    schema_path = root / "requests/schema.json"
    _write_immutable_json(variant_path, variant)
    _write_immutable_text(base_path, base)
    _write_immutable_text(prompt_path, prompt)
    _write_immutable_json(schema_path, schema)
    policy_path = _capacity_policy(root)
    attempt_path = root / "attempt-spec.json"
    _write_immutable_json(
        attempt_path,
        {
            "schema_version": SCHEMA_VERSION,
            "thread_id": THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "predecessor_receipt": _record(EPOCH28_ROOT / "plan-step-receipt.json"),
            "observable_disagreement_case_ids": list(EXPECTED_DISAGREEMENT_CASE_IDS),
            "origin_neutral": True,
            "prior_ab_ba_replay_allowed": False,
            "model": MODEL,
            "effort": EFFORT,
            "transport": TRANSPORT,
            "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
            "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
            "instruction_source": _record(INSTRUCTION_SOURCE_PATH),
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "semantic_retry_count": 0,
            "semantic_total_token_cap": NEW_TOTAL_TOKEN_CAP,
            "quality_total_token_cap_including_epoch28": QUALITY_TOTAL_TOKEN_CAP,
            "winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
    )
    runtime_records = [_record(path) for path in _runtime_paths()]
    source_records = [_record(PLAN_PATH), _record(DIRECTIVE_PATH)]
    predecessor_records = _expected_predecessor_records()
    request_records = [
        _record(variant_path),
        _record(base_path),
        _record(prompt_path),
        _record(schema_path),
    ]
    direct_records = [
        *runtime_records,
        *source_records,
        *predecessor_records,
        *request_records,
        _record(policy_path),
        _record(attempt_path),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_files": runtime_records,
        "source_records": source_records,
        "predecessor_records": predecessor_records,
        "request_records": request_records,
        "capacity_policy": _record(policy_path),
        "attempt_spec": _record(attempt_path),
        "direct_record_digest": _record_digest(direct_records),
        "model": MODEL,
        "effort": EFFORT,
        "transport": TRANSPORT,
        "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "instruction_source": _record(INSTRUCTION_SOURCE_PATH),
        "observable_disagreement_case_ids": list(EXPECTED_DISAGREEMENT_CASE_IDS),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "semantic_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "quality_total_token_cap_including_epoch28": QUALITY_TOTAL_TOKEN_CAP,
        "prior_ab_ba_replay_allowed": False,
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
        lock = _load_json(lock_path, "epoch-29 runtime lock")
        records = [
            *(lock.get("runtime_files") or []),
            *(lock.get("source_records") or []),
            *(lock.get("predecessor_records") or []),
            *(lock.get("request_records") or []),
            lock.get("capacity_policy") or {},
            lock.get("attempt_spec") or {},
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("runtime_files") != [_record(path) for path in _runtime_paths()]
            or lock.get("source_records") != [_record(PLAN_PATH), _record(DIRECTIVE_PATH)]
            or lock.get("predecessor_records") != _expected_predecessor_records()
            or lock.get("model") != MODEL
            or lock.get("effort") != EFFORT
            or lock.get("transport") != TRANSPORT
            or lock.get("instruction_sources_count")
            != EXPECTED_INSTRUCTION_SOURCES_COUNT
            or lock.get("instruction_sources_sha256")
            != EXPECTED_INSTRUCTION_SOURCES_SHA256
            or lock.get("instruction_source") != _record(INSTRUCTION_SOURCE_PATH)
            or lock.get("observable_disagreement_case_ids")
            != list(EXPECTED_DISAGREEMENT_CASE_IDS)
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("semantic_retry_count") != 0
            or lock.get("semantic_total_token_cap") != NEW_TOTAL_TOKEN_CAP
            or lock.get("quality_total_token_cap_including_epoch28")
            != QUALITY_TOTAL_TOKEN_CAP
            or lock.get("prior_ab_ba_replay_allowed") is not False
            or lock.get("winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutated") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(not isinstance(row, Mapping) or not _verify_record(row) for row in records)
        ):
            raise Epoch29AdjudicationError("epoch-29 runtime lock drifted")
        load_contract()
        variant, base, prompt, schema = _request_material()
        expected_requests = [
            _record(lock_path.parent / "requests/variant.private.json"),
            _record(lock_path.parent / "requests/base-instructions.private.md"),
            _record(lock_path.parent / "requests/prompt.private.md"),
            _record(lock_path.parent / "requests/schema.json"),
        ]
        if (
            lock.get("request_records") != expected_requests
            or _load_json(Path(str(expected_requests[0]["path"])), "adjudication variant")
            != variant
            or Path(expected_requests[1]["path"]).read_text(encoding="utf-8") != base
            or Path(expected_requests[2]["path"]).read_text(encoding="utf-8") != prompt
            or _load_json(Path(str(expected_requests[3]["path"])), "adjudication schema")
            != schema
        ):
            raise Epoch29AdjudicationError("epoch-29 request artifacts drifted")
        attempt = _load_json(
            Path(str(lock["attempt_spec"]["path"])), "epoch-29 attempt spec"
        )
        if (
            attempt.get("schema_version") != SCHEMA_VERSION
            or attempt.get("thread_id") != THREAD_ID
            or attempt.get("plan_epoch") != PLAN_EPOCH
            or attempt.get("step_id") != STEP_ID
            or attempt.get("predecessor_receipt")
            != _record(EPOCH28_ROOT / "plan-step-receipt.json")
            or attempt.get("observable_disagreement_case_ids")
            != list(EXPECTED_DISAGREEMENT_CASE_IDS)
            or attempt.get("origin_neutral") is not True
            or attempt.get("prior_ab_ba_replay_allowed") is not False
            or attempt.get("model") != MODEL
            or attempt.get("effort") != EFFORT
            or attempt.get("transport") != TRANSPORT
            or attempt.get("instruction_sources_count")
            != EXPECTED_INSTRUCTION_SOURCES_COUNT
            or attempt.get("instruction_sources_sha256")
            != EXPECTED_INSTRUCTION_SOURCES_SHA256
            or attempt.get("instruction_source") != _record(INSTRUCTION_SOURCE_PATH)
            or attempt.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or attempt.get("semantic_retry_count") != 0
            or attempt.get("semantic_total_token_cap") != NEW_TOTAL_TOKEN_CAP
            or attempt.get("quality_total_token_cap_including_epoch28")
            != QUALITY_TOTAL_TOKEN_CAP
            or attempt.get("winner_frozen") is not False
            or attempt.get("holdout_authorized") is not False
            or attempt.get("production_mutated") is not False
        ):
            raise Epoch29AdjudicationError("epoch-29 attempt specification drifted")
        policy = reserve.load_reserve_capacity_policy(
            Path(str(lock["capacity_policy"]["path"]))
        )
        if (
            policy.get("phase_id") != STEP_ID
            or policy.get("semantic_output_root") != str(lock_path.parent)
            or policy.get("ordered_turn_names") != list(CAPACITY_TURN_NAMES)
            or policy.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or policy.get("maximum_total_tokens_per_turn") != NEW_TOTAL_TOKEN_CAP
            or policy.get("phase_total_token_bound") != NEW_TOTAL_TOKEN_CAP
            or policy.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
        ):
            raise Epoch29AdjudicationError("epoch-29 capacity policy drifted")
        return lock

    if acquire_lock:
        with _process_lock(lock_path.parent):
            return verify()
    return verify()


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(root: Path) -> reserve.ReserveCapacityGatedCodexAppServerClient:
    return reserve.ReserveCapacityGatedCodexAppServerClient(
        policy_path=root / "capacity-policy.json", inner_factory=_inner_factory
    )


def _turn_root(root: Path) -> Path:
    return root / "turns" / TURN_DIRECTORY_NAME


def _validate_capacity_checkpoint(root: Path) -> dict[str, Any]:
    path = _turn_root(root) / "capacity.json"
    value = _load_json(path, "epoch-29 capacity checkpoint")
    policy_path = root / "capacity-policy.json"
    policy = reserve.load_reserve_capacity_policy(policy_path)
    required = {
        "schema_version": reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
        "policy_path": str(policy_path.resolve()),
        "policy_sha256": _sha256_file(policy_path),
        "phase_id": STEP_ID,
        "turn_name": CAPACITY_TURN_NAMES[0],
        "turn_ordinal": 0,
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
    if not isinstance(value, Mapping) or any(
        value.get(key) != expected for key, expected in required.items()
    ):
        raise Epoch29AdjudicationError("epoch-29 capacity checkpoint drifted")
    evaluation = reserve.evaluate_reserve_capacity(
        {
            "primary_used_percent": value.get("primary_used_percent"),
            "rate_limit_reached_type": None,
        },
        policy=policy,
        remaining_turn_count=1,
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
            raise Epoch29AdjudicationError("epoch-29 capacity arithmetic drifted")
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
        "predecessor_receipt": _record(EPOCH28_ROOT / "plan-step-receipt.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "variant": _record(root / "requests/variant.private.json"),
        "base_instructions": _record(root / "requests/base-instructions.private.md"),
        "prompt": _record(root / "requests/prompt.private.md"),
        "schema": _record(root / "requests/schema.json"),
        "observable_disagreement_case_ids": list(EXPECTED_DISAGREEMENT_CASE_IDS),
        "model": MODEL,
        "effort": EFFORT,
        "transport": TRANSPORT,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "prior_ab_ba_replay_allowed": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_immutable_json(path, value)
    return _validate_launch_receipt(root)


def _validate_launch_receipt(root: Path) -> dict[str, Any]:
    value = _load_json(root / "launch-receipt.json", "epoch-29 launch receipt")
    expected = {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "predecessor_receipt": _record(EPOCH28_ROOT / "plan-step-receipt.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "variant": _record(root / "requests/variant.private.json"),
        "base_instructions": _record(root / "requests/base-instructions.private.md"),
        "prompt": _record(root / "requests/prompt.private.md"),
        "schema": _record(root / "requests/schema.json"),
    }
    if (
        value.get("schema_version") != LAUNCH_VERSION
        or not isinstance(value.get("launched_at"), str)
        or not value.get("launched_at")
        or value.get("thread_id") != THREAD_ID
        or value.get("plan_epoch") != PLAN_EPOCH
        or value.get("step_id") != STEP_ID
        or any(value.get(key) != record for key, record in expected.items())
        or value.get("observable_disagreement_case_ids")
        != list(EXPECTED_DISAGREEMENT_CASE_IDS)
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("transport") != TRANSPORT
        or value.get("semantic_model_call_cap") != MODEL_CALL_CAP
        or value.get("semantic_retry_count") != 0
        or value.get("prior_ab_ba_replay_allowed") is not False
        or value.get("winner_frozen") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutated") is not False
    ):
        raise Epoch29AdjudicationError("epoch-29 launch receipt drifted")
    return dict(value)


def _valid_usage(value: Any) -> dict[str, int]:
    try:
        return epoch28._valid_usage(value)  # noqa: SLF001
    except epoch28.Epoch28QualityError as exc:
        raise Epoch29AdjudicationError(str(exc)) from exc


def _validate_partial_sidecar(root: Path, sidecar: Mapping[str, Any]) -> None:
    variant, base, prompt, schema = _request_material()
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
        "batch_size": len(variant["cases"]),
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
        raise Epoch29AdjudicationError("partial adjudication sidecar lineage drifted")
    thread_id = sidecar.get("thread_id")
    turn_id = sidecar.get("turn_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise Epoch29AdjudicationError("partial adjudication lacks thread identity")
    if turn_id is not None and (not isinstance(turn_id, str) or not turn_id):
        raise Epoch29AdjudicationError("partial adjudication turn identity drifted")
    output_path = _turn_root(root) / "output.private.json"
    if Path(str(sidecar.get("output_path") or "")).expanduser().resolve() != output_path:
        raise Epoch29AdjudicationError("partial adjudication output path drifted")


def _validate_completed_turn(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, int]]:
    variant, base, prompt, schema = _request_material()
    turn_root = _turn_root(root)
    try:
        output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
            raw_output_path=turn_root / "output.private.json",
            sidecar_path=turn_root / "sidecar.json",
            prompt=prompt,
            schema=schema,
            base_instructions=base,
            model=MODEL,
            reasoning_effort=EFFORT,
        )
    except (ValueError, judge.JudgeArtifactError) as exc:
        raise Epoch29AdjudicationError("completed adjudication checkpoint drifted") from exc
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
        "batch_size": len(variant["cases"]),
        "instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "usage_status": "measured",
        "usage_complete": True,
    }
    if any(sidecar.get(key) != expected for key, expected in required.items()):
        raise Epoch29AdjudicationError("managed adjudication sidecar contract drifted")
    usage = _valid_usage(sidecar.get("usage"))
    if sidecar.get("thread_total_usage") != usage:
        raise Epoch29AdjudicationError("adjudication thread usage drifted")
    if usage["total_tokens"] > NEW_TOTAL_TOKEN_CAP:
        raise Epoch29AdjudicationError("adjudication exceeded its token ceiling")
    if judge.validate_judge_output(output, variant):
        raise Epoch29AdjudicationError("adjudication output failed exact validation")
    _validate_capacity_checkpoint(root)
    predecessor = _validate_epoch28_terminal()
    prior_ids = {
        (str(row["thread_id"]), str(row["turn_id"]))
        for row in predecessor.get("turns") or []
    }
    identity = (str(sidecar.get("thread_id")), str(sidecar.get("turn_id")))
    if identity in prior_ids or len(identity[0]) == 0 or len(identity[1]) == 0:
        raise Epoch29AdjudicationError("adjudication reused a prior thread or turn")
    return output, sidecar, usage


def merge_adjudication(
    consensus: Mapping[str, Any],
    output: Mapping[str, Any],
    case_ids: Sequence[str],
) -> dict[str, Any]:
    selected = set(case_ids)
    adjudicated = {str(row["case_id"]): row for row in output["cases"]}
    if set(adjudicated) != selected:
        raise Epoch29AdjudicationError("adjudication output case coverage drifted")
    cases = []
    for prior_case in consensus["cases"]:
        case_id = str(prior_case["case_id"])
        if case_id not in selected:
            cases.append(copy.deepcopy(prior_case))
            continue
        row = adjudicated[case_id]
        all_ids = sorted(item["witness_id"] for item in row["support_results"])
        cases.append(
            {
                "case_id": case_id,
                "status": "adjudicated",
                "support_results": [
                    {
                        "witness_id": item["witness_id"],
                        "verdict": item["verdict"],
                        "evidence_spans": sorted(set(item["evidence_spans"])),
                    }
                    for item in row["support_results"]
                ],
                "equivalence_groups": sorted(
                    [sorted(group["witness_ids"]) for group in row["equivalence_groups"]]
                ),
                "partition_abstained_witness_ids": [],
                "alignment_results": [],
                "unaligned_a_witness_ids": all_ids,
                "unaligned_b_witness_ids": [],
                "alignment_abstained_witness_ids": [],
            }
        )
    merged = copy.deepcopy(dict(consensus))
    merged["cases"] = cases
    merged["selection_admissible"] = True
    merged["adjudication"] = {
        "call_count": 1,
        "call_cap": 1,
        "case_ids": sorted(selected),
        "side_free": True,
        "majority_vote_used": False,
    }
    return merged


def _score(root: Path) -> dict[str, Any]:
    bundle = _base_bundle()
    case_ids = observable_disagreement_case_ids(bundle["pool"], bundle["outputs"])
    output, sidecar, usage = _validate_completed_turn(root)
    consensus = merge_adjudication(bundle["consensus"], output, case_ids)
    try:
        shared = judge.score_named_systems_against_shared_reference(
            pool=bundle["pool"],
            private_mapping=bundle["mapping"],
            consensus=consensus,
            reference_system_ids=None,
        )
        macro = epoch28._macro_scores(  # noqa: SLF001
            pool=bundle["pool"], mapping=bundle["mapping"], consensus=consensus
        )
    except (ValueError, epoch28.Epoch28QualityError) as exc:
        raise Epoch29AdjudicationError("adjudicated shared-reference score failed") from exc
    baseline_macro = macro["systems"][epoch28.SYSTEM_BASELINE][
        "strict_full_field_macro_f1"
    ]
    candidate_macro = macro["systems"][epoch28.SYSTEM_CANDIDATE][
        "strict_full_field_macro_f1"
    ]
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in bundle["pool"]["cases"]
    )
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in bundle["pool"]["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    quality_total = PREDECESSOR_USAGE["total_tokens"] + usage["total_tokens"]
    checks = {
        "candidate_strict_full_field_macro_f1_gte_0_97": (
            candidate_macro >= QUALITY_THRESHOLD
        ),
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "full_six_case_token_projection_lte_frozen_ceiling": (
            epoch28.FULL_SIX_CASE_TOKEN_PROJECTION
            <= epoch28.FULL_SIX_CASE_TOKEN_CEILING
        ),
        "exact_evidence_rate_1": exact_count == witness_count == EXPECTED_TOTAL_WITNESSES,
        "adjudication_accounting_complete_under_cap": (
            usage["total_tokens"] <= NEW_TOTAL_TOKEN_CAP
            and quality_total <= QUALITY_TOTAL_TOKEN_CAP
        ),
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
        "semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "adjudication_usage": usage,
        "quality_total_tokens_including_epoch28": quality_total,
        "adjudication_turn": {
            "thread_id": sidecar["thread_id"],
            "turn_id": sidecar["turn_id"],
            "wall_elapsed_seconds": sidecar.get("wall_elapsed_seconds"),
            "usage": usage,
        },
        "full_six_case_token_projection": epoch28.FULL_SIX_CASE_TOKEN_PROJECTION,
        "full_six_case_token_ceiling": epoch28.FULL_SIX_CASE_TOKEN_CEILING,
        "judge_tokens_included_in_production_formula": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "consensus": consensus,
    }


def _optional_record(path: Path) -> dict[str, Any] | None:
    return _record(path) if path.is_file() else None


def _receipt_records(root: Path) -> dict[str, Any]:
    turn_root = _turn_root(root)
    return {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "variant": _record(root / "requests/variant.private.json"),
        "base_instructions": _record(root / "requests/base-instructions.private.md"),
        "prompt": _record(root / "requests/prompt.private.md"),
        "schema": _record(root / "requests/schema.json"),
        "predecessor_receipt": _record(EPOCH28_ROOT / "plan-step-receipt.json"),
        "launch": _optional_record(root / "launch-receipt.json"),
        "capacity": _optional_record(turn_root / "capacity.json"),
        "sidecar": _optional_record(turn_root / "sidecar.json"),
        "output": _optional_record(turn_root / "output.private.json"),
        "consensus": _optional_record(root / "consensus.private.json"),
        "score": _optional_record(root / "shared-reference-score.json"),
    }


def _partial_accounting(root: Path) -> dict[str, Any]:
    turn_root = _turn_root(root)
    sidecar_path = turn_root / "sidecar.json"
    launch = (root / "launch-receipt.json").is_file()
    if not launch:
        return {
            "semantic_model_call_count": 0,
            "measured_model_call_count": 0,
            "unknown_usage_turn_count": 0,
            "usage_status": "zero_call",
            "accounting_complete": True,
            "usage": {field: 0 for field in USAGE_FIELDS},
            "turns": [],
        }
    usage = {field: 0 for field in USAGE_FIELDS}
    measured = 0
    unknown = 1
    turns: list[dict[str, Any]] = []
    if sidecar_path.is_file():
        sidecar = _load_json(sidecar_path, "partial adjudication sidecar")
        _validate_partial_sidecar(root, sidecar)
        raw_usage = sidecar.get("usage")
        if raw_usage is not None:
            usage = _valid_usage(raw_usage)
            measured = 1
            unknown = 0
        turns.append(
            {
                "thread_id": sidecar.get("thread_id"),
                "turn_id": sidecar.get("turn_id"),
                "usage_status": "measured" if measured else "unknown",
                "usage": usage if measured else None,
            }
        )
    capacity_path = turn_root / "capacity.json"
    if capacity_path.is_file():
        _validate_capacity_checkpoint(root)
    return {
        "semantic_model_call_count": 1,
        "measured_model_call_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage_status": "measured" if measured else "unknown",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "turns": turns,
    }


def _receipt(
    root: Path,
    *,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    score: Mapping[str, Any] | None = None,
    diagnostic: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise Epoch29AdjudicationError("epoch-29 receipt state is invalid")
    usage = accounting.get("usage") or {field: 0 for field in USAGE_FIELDS}
    calls = int(accounting.get("semantic_model_call_count") or 0)
    measured = int(accounting.get("measured_model_call_count") or 0)
    unknown = int(accounting.get("unknown_usage_turn_count") or 0)
    total = int(usage.get("total_tokens") or 0)
    public_score = None
    if score is not None:
        public_score = {
            key: copy.deepcopy(value)
            for key, value in score.items()
            if key != "consensus"
        }
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": terminal_reason,
        "created_at": now_iso(),
        "output_root": str(root),
        "semantic_model_call_count": calls,
        "measured_model_call_count": measured,
        "unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": accounting.get("usage_status"),
        "accounting_complete": accounting.get("accounting_complete") is True,
        "usage": usage,
        "turns": accounting.get("turns") or [],
        "predecessor_semantic_model_call_count": 2,
        "predecessor_measured_total_tokens": PREDECESSOR_USAGE["total_tokens"],
        "quality_total_tokens_including_epoch28": PREDECESSOR_USAGE["total_tokens"]
        + total,
        "aggregate_architecture_semantic_model_call_count": (
            PREDECESSOR_AGGREGATE_MODEL_CALLS + calls
        ),
        "aggregate_architecture_unknown_usage_turn_count": (
            PREDECESSOR_AGGREGATE_UNKNOWN_USAGE + unknown
        ),
        "aggregate_architecture_measured_total_tokens": (
            PREDECESSOR_AGGREGATE_MEASURED_TOKENS + total
        ),
        "score": public_score,
        "failed_checks": list(score.get("failed_checks") or []) if score else [],
        "diagnostic": copy.deepcopy(dict(diagnostic)) if diagnostic else None,
        "quality_measured_by_this_step": score is not None,
        "quality_passed": bool(score and score.get("passed") is True),
        "next_authorized_action": (
            "freeze_full_six_case_tagged_metric_extraction_measurement"
            if state == "passed"
            else "reject_tagged_metric_architecture_without_field_repair"
            if state == "rejected"
            else "capacity_or_transport_recovery_without_semantic_replay"
        ),
        "full_six_case_token_projection": epoch28.FULL_SIX_CASE_TOKEN_PROJECTION,
        "full_six_case_token_ceiling": epoch28.FULL_SIX_CASE_TOKEN_CEILING,
        "judge_tokens_included_in_production_formula": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "artifact_records": _receipt_records(root),
    }


def _write_terminal(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _write_immutable_json(root / "plan-step-receipt.json", receipt)
    _write_immutable_json(root / "terminal.json", receipt)
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
    value = _load_json(existing, "single epoch-29 terminal mirror")
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
        or receipt.get("full_six_case_token_projection")
        != epoch28.FULL_SIX_CASE_TOKEN_PROJECTION
        or receipt.get("full_six_case_token_ceiling")
        != epoch28.FULL_SIX_CASE_TOKEN_CEILING
        or receipt.get("judge_tokens_included_in_production_formula") is not False
        or receipt.get("artifact_records") != _receipt_records(root)
    ):
        raise Epoch29AdjudicationError("epoch-29 terminal receipt drifted")
    calls = receipt.get("semantic_model_call_count")
    measured = receipt.get("measured_model_call_count")
    unknown = receipt.get("unknown_usage_turn_count")
    if (
        any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in (calls, measured, unknown)
        )
        or calls != measured + unknown
        or calls > MODEL_CALL_CAP
    ):
        raise Epoch29AdjudicationError("epoch-29 call accounting drifted")
    usage = _valid_usage(receipt.get("usage"))
    if (
        receipt.get("quality_total_tokens_including_epoch28")
        != PREDECESSOR_USAGE["total_tokens"] + usage["total_tokens"]
        or receipt.get("aggregate_architecture_semantic_model_call_count")
        != PREDECESSOR_AGGREGATE_MODEL_CALLS + calls
        or receipt.get("aggregate_architecture_unknown_usage_turn_count")
        != PREDECESSOR_AGGREGATE_UNKNOWN_USAGE + unknown
        or receipt.get("aggregate_architecture_measured_total_tokens")
        != PREDECESSOR_AGGREGATE_MEASURED_TOKENS + usage["total_tokens"]
    ):
        raise Epoch29AdjudicationError("epoch-29 aggregate accounting drifted")
    if receipt.get("score") is not None or receipt.get("state") == "passed":
        score = _score(root)
        public_score = {key: value for key, value in score.items() if key != "consensus"}
        if (
            receipt.get("semantic_model_call_count") != 1
            or receipt.get("measured_model_call_count") != 1
            or receipt.get("unknown_usage_turn_count") != 0
            or receipt.get("accounting_complete") is not True
            or receipt.get("usage") != score["adjudication_usage"]
            or receipt.get("score") != public_score
            or receipt.get("quality_measured_by_this_step") is not True
            or receipt.get("quality_passed") is not (score["passed"] is True)
            or receipt.get("state") != ("passed" if score["passed"] else "rejected")
            or _load_json(root / "consensus.private.json", "epoch-29 consensus")
            != score["consensus"]
        ):
            raise Epoch29AdjudicationError("epoch-29 quality receipt drifted")
    else:
        partial = _partial_accounting(root)
        if (
            receipt.get("state") not in {"waiting", "rejected"}
            or (
                receipt.get("state") == "rejected"
                and receipt.get("terminal_reason")
                != "epoch29_adjudication_structured_output_rejected"
            )
            or receipt.get("semantic_model_call_count")
            != partial["semantic_model_call_count"]
            or receipt.get("measured_model_call_count")
            != partial["measured_model_call_count"]
            or receipt.get("unknown_usage_turn_count")
            != partial["unknown_usage_turn_count"]
            or receipt.get("usage") != partial["usage"]
            or receipt.get("quality_measured_by_this_step") is not False
            or receipt.get("quality_passed") is not False
        ):
            raise Epoch29AdjudicationError("epoch-29 waiting receipt drifted")


def verify_receipt(root: Path = DEFAULT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    output_root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(output_root / "runtime-lock.json", acquire_lock=False)
        receipt = _load_json(output_root / "plan-step-receipt.json", "epoch-29 receipt")
        terminal = _load_json(output_root / "terminal.json", "epoch-29 terminal")
        if receipt != terminal:
            raise Epoch29AdjudicationError("epoch-29 terminal mirrors differ")
        _validate_receipt_payload(output_root, receipt)
        return dict(receipt)

    if acquire_lock:
        with _process_lock(output_root):
            return verify()
    return verify()


def _semantic_artifacts_exist(root: Path) -> bool:
    turn_root = _turn_root(root)
    return turn_root.exists() and any(path.is_file() for path in turn_root.rglob("*"))


def _reject_external_auth_material() -> None:
    present = sorted(name for name in _FORBIDDEN_AUTH_ENVIRONMENT if os.environ.get(name))
    if present:
        raise Epoch29AdjudicationError(
            "managed ChatGPT execution rejects API-key/raw-session auth: "
            + ", ".join(present)
        )


def _finalize_completed(root: Path) -> dict[str, Any]:
    score = _score(root)
    _write_immutable_json(root / "consensus.private.json", score["consensus"])
    public_score = {key: value for key, value in score.items() if key != "consensus"}
    _write_immutable_json(root / "shared-reference-score.json", public_score)
    accounting = {
        "semantic_model_call_count": 1,
        "measured_model_call_count": 1,
        "unknown_usage_turn_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": score["adjudication_usage"],
        "turns": [score["adjudication_turn"]],
    }
    return _write_terminal(
        root,
        _receipt(
            root,
            state="passed" if score["passed"] else "rejected",
            terminal_reason=(
                "epoch29_adjudicated_quality_gate_passed"
                if score["passed"]
                else "epoch29_adjudicated_quality_gate_rejected"
            ),
            accounting=accounting,
            score=score,
        ),
    )


async def _run_unlocked(
    root: Path, *, client_factory: Callable[[], Any]
) -> dict[str, Any]:
    recovered = _recover_terminal_mirror(root)
    if recovered is not None:
        return recovered
    verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
    launch_exists = (root / "launch-receipt.json").is_file()
    artifacts_exist = _semantic_artifacts_exist(root)
    if artifacts_exist and not launch_exists:
        raise Epoch29AdjudicationError("semantic artifacts exist without epoch-29 launch")
    if launch_exists:
        _validate_launch_receipt(root)
        turn_root = _turn_root(root)
        if (
            (turn_root / "output.private.json").is_file()
            and (turn_root / "sidecar.json").is_file()
            and (turn_root / "capacity.json").is_file()
        ):
            sidecar = _load_json(turn_root / "sidecar.json", "adjudication sidecar")
            if sidecar.get("state") == "completed" and sidecar.get("status") == "completed":
                return _finalize_completed(root)
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="waiting",
                terminal_reason="epoch29_partial_or_interrupted_adjudication_preserved_without_replay",
                accounting=accounting,
                diagnostic={"class": "partial_semantic_attempt", "replay_allowed": False},
            ),
        )
    _launch_receipt(root)
    variant, base, prompt, schema = _request_material()
    turn_root = _turn_root(root)
    try:
        async with client_factory() as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=base,
                prompt=prompt,
                output_schema=schema,
                cwd=PROJECT_ROOT,
                sidecar_path=turn_root / "sidecar.json",
                output_path=turn_root / "output.private.json",
                capacity_checkpoint_path=turn_root / "capacity.json",
                batch_size=len(variant["cases"]),
                thread_mode="new_thread",
                timeout_seconds=TIMEOUT_SECONDS,
            )
        if not result.status_ok or not isinstance(result.output, Mapping):
            raise Epoch29AdjudicationWaiting("adjudication turn did not complete")
    except codex_app_server.AppServerStructuredOutputError as exc:
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="rejected",
                terminal_reason="epoch29_adjudication_structured_output_rejected",
                accounting=accounting,
                diagnostic={"class": type(exc).__name__, "semantic_retry_allowed": False},
            ),
        )
    except (
        reserve.ReserveCapacityError,
        codex_app_server.AppServerTurnTimeout,
        codex_app_server.AppServerProcessDied,
        codex_app_server.AppServerRPCError,
        codex_app_server.AppServerAuthError,
        codex_app_server.AppServerRecoveryRequired,
        Epoch29AdjudicationWaiting,
        OSError,
        asyncio.TimeoutError,
    ) as exc:
        accounting = _partial_accounting(root)
        return _write_terminal(
            root,
            _receipt(
                root,
                state="waiting",
                terminal_reason="epoch29_adjudication_operational_waiting_no_replay",
                accounting=accounting,
                diagnostic={"class": type(exc).__name__},
            ),
        )
    return _finalize_completed(root)


async def run(
    root: Path = DEFAULT_ROOT,
    *,
    client_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if output_root == DEFAULT_ROOT and client_factory is not None:
        raise Epoch29AdjudicationError("the live epoch-29 root forbids injected clients")
    _reject_external_auth_material()
    with _process_lock(output_root):
        return await _run_unlocked(
            output_root,
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
    except Epoch29AdjudicationWaiting as exc:
        print(json.dumps({"state": "waiting", "error": str(exc)}, sort_keys=True))
        return 75
    except Epoch29AdjudicationError as exc:
        print(json.dumps({"state": "invalid", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
