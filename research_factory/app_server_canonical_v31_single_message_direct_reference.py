from __future__ import annotations

"""Direct full-event evaluation of the passed epoch-19 canonical canary.

This adapter performs no extraction. It binds the immutable one-turn epoch-19
artifacts, places all 120 canonical baseline events and all 31 candidate events
into one blinded witness pool, and prepares the mature full-event AB/BA judge.
Semantic decisions remain LLM-owned; deterministic work is limited to artifact
integrity, exact lineage, blinding, accounting, lifecycle, and scoring.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
import math
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_candidate_shared_reference_repair as shared_repair
from . import app_server_capacity_reserve as reserve
from . import app_server_canonical_v31_single_message_compact_pointer_canary_runtime as epoch19
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_canonical_v31_single_message_direct_reference_v20"
LOCK_VERSION = "pif_canonical_v31_single_message_direct_reference_lock_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 20
STEP_ID = "canonical_v31_epoch20_single_message_direct_reference_v20"
MODEL = "gpt-5.5"
EFFORT = "high"
JUDGE_TRANSPORT = "official_codex_app_server_stdio_managed_chatgpt_auth"
JUDGE_TIMEOUT_SECONDS = 1200.0
ADJUDICATION_LAUNCH_VERSION = (
    "pif_canonical_v31_single_message_direct_reference_adjudication_launch_v1"
)
LAUNCH_VERSION = "pif_canonical_v31_single_message_direct_reference_launch_v1"
MODEL_CALL_CAP = 3
BASE_JUDGE_CALL_COUNT = 2
ADJUDICATION_CALL_CAP = 1
TOTAL_TOKEN_CAP = 400_000
ADJUDICATION_MAX_TOTAL_TOKENS = 100_000
MAXIMUM_TOTAL_TOKENS_PER_TURN = 150_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PHASE_TOTAL_TOKEN_BOUND = MODEL_CALL_CAP * MAXIMUM_TOTAL_TOKENS_PER_TURN
PROJECTED_PHASE_QUOTA_POINTS = math.ceil(
    PHASE_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_TARGET = 0.28
SYSTEM_BASELINE = "canonical_v31_frozen_baseline"
SYSTEM_CANDIDATE = "canonical_v31_epoch19_single_message_candidate"
SOURCE_SEGMENT_ORDER = tuple(epoch19.EXPECTED_SEGMENT_IDS)
BASELINE_EVENT_COUNTS = (20, 19, 23, 19, 20, 19)
CANDIDATE_EVENT_COUNTS = (6, 6, 5, 5, 3, 6)
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
EXPECTED_BASELINE_WITNESSES = sum(BASELINE_EVENT_COUNTS)
EXPECTED_CANDIDATE_WITNESSES = sum(CANDIDATE_EVENT_COUNTS)
EXPECTED_TOTAL_WITNESSES = EXPECTED_BASELINE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES
EXPECTED_EXTRACTION_USAGE = {
    "input_tokens": 27_332,
    "cached_input_tokens": 0,
    "output_tokens": 21_027,
    "reasoning_output_tokens": 3_399,
    "total_tokens": 48_359,
}
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
EXPECTED_PRODUCTION_TOTAL_TOKENS = 2_051_308
EXPECTED_PRODUCTION_TOKEN_RATIO = 0.2037974349024075
EXPECTED_WITNESS_CONTRACT = {
    "source_segment_order": list(SOURCE_SEGMENT_ORDER),
    "baseline_event_counts_by_segment": dict(zip(SOURCE_SEGMENT_ORDER, BASELINE_EVENT_COUNTS)),
    "candidate_event_counts_by_segment": dict(zip(SOURCE_SEGMENT_ORDER, CANDIDATE_EVENT_COUNTS)),
    "baseline_total_event_count": EXPECTED_BASELINE_WITNESSES,
    "candidate_total_event_count": EXPECTED_CANDIDATE_WITNESSES,
    "shared_witness_total": EXPECTED_TOTAL_WITNESSES,
    "identical_ab_ba_case_order_required": True,
    "opaque_witness_ids_required": True,
    "one_shared_augmented_reference_required": True,
    "all_candidate_events_preserved": True,
}
EXPECTED_ACCEPTANCE_CONTRACT = {
    "candidate_strict_full_field_macro_f1_min": QUALITY_THRESHOLD,
    "candidate_must_be_noninferior_to_baseline": True,
    "production_amortized_total_token_ratio_max": TOKEN_RATIO_TARGET,
    "exact_evidence_rate": 1.0,
    "quality_failure_is_rejected": True,
    "operational_failure_is_waiting": True,
    "winner_frozen_by_this_step": False,
    "holdout_authorized_by_this_step": False,
    "production_mutation_allowed": False,
}
EXPECTED_TERMINAL_CONTRACT = {
    "receipt_schema_version": RECEIPT_VERSION,
    "passed_state": "passed",
    "quality_failure_state": "rejected",
    "operational_failure_state": "waiting",
    "zero_retry": True,
    "replay_safe": True,
    "development_winner_frozen": False,
    "holdout_authorized": False,
    "production_mutated": False,
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch20-single-message-direct-reference-v20.json"
).resolve()
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v20.json").resolve()
EPOCH19_ROOT = (
    PIPELINE_ROOT
    / "canonical-v31-epoch19-single-message-compact-pointer-canary-v1"
).resolve()
BASELINE_SEED_PATH = (
    PROJECT_ROOT / "work/app-server-development-v2/shared-reference-seed-v1.json"
).resolve()
SOURCE_PACKET_PATH = (
    EPOCH19_ROOT / "prepared-turn/request.private.json"
).resolve()
MATURE_LOGIC_PATH = (
    PROJECT_ROOT / "research_factory/app_server_candidate_recurrent_fold_direct_reference.py"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "canonical-v31-epoch20-single-message-direct-reference-v1"
).resolve()
PINNED_CODEX = shared_repair.PINNED_CODEX
USAGE_FIELDS = shared_repair.USAGE_FIELDS
PROCESS_LOCK_NAME = ".canonical-v31-single-message-direct-reference.lock"
SOURCE_POLICY = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()
CAPACITY_POLICY_PATH = DEFAULT_OUTPUT_ROOT / "capacity-policy.json"
CAPACITY_TURN_NAMES = ("epoch20_quality_ab", "epoch20_quality_ba", "epoch20_quality_adjudication")

EXPECTED_FROZEN_PATHS = {
    "baseline_complete_output": BASELINE_SEED_PATH,
    "blind_source_packet": SOURCE_PACKET_PATH,
    "epoch19_terminal": EPOCH19_ROOT / "terminal.json",
    "epoch19_plan_step_receipt": EPOCH19_ROOT / "plan-step-receipt.json",
    "epoch19_runtime_lock": EPOCH19_ROOT / "runtime-lock.json",
    "epoch19_canonical_labels": EPOCH19_ROOT / "turn/canonical-labels.private.json",
    "epoch19_evidence_provenance": EPOCH19_ROOT / "turn/evidence-provenance.private.json",
    "epoch19_semantic_fidelity": EPOCH19_ROOT / "turn/semantic-fidelity.json",
    "epoch19_sidecar": EPOCH19_ROOT / "turn/sidecar.json",
    "epoch19_raw_output": EPOCH19_ROOT / "turn/output.private.json",
    "epoch19_request": EPOCH19_ROOT / "prepared-turn/request.private.json",
    "mature_direct_reference_logic": MATURE_LOGIC_PATH,
}

_HASH_CACHE: dict[tuple[str, int, int, int, int], str] = {}


class ExpandedCapDirectReferenceError(RuntimeError):
    """The epoch-20 contract, lineage, or evaluator artifact is invalid."""


class OperationalWaitingError(ExpandedCapDirectReferenceError):
    """An operational attempt must stop without replay or quality authorization."""


class PartialAttemptWaitingError(OperationalWaitingError):
    """An immutable partial attempt must be preserved without starting another turn."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpandedCapDirectReferenceError(f"cannot read {label}") from exc


def _sha256_file(path: Path) -> str:
    resolved = path.expanduser().resolve(strict=True)
    stat = resolved.stat()
    key = (str(resolved), stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    cached = _HASH_CACHE.get(key)
    if cached is not None:
        return cached
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    value = digest.hexdigest()
    _HASH_CACHE[key] = value
    return value


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


def _validated_record(row: Mapping[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise ExpandedCapDirectReferenceError(f"{label} checksum or size drifted")
    frozen = {
        "path": row.get("path"),
        "sha256": row.get("sha256"),
        "size_bytes": row.get("size_bytes"),
    }
    if not _verify_record(frozen):
        raise ExpandedCapDirectReferenceError(f"{label} checksum or size drifted")
    return _record(Path(str(frozen["path"])))


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise ExpandedCapDirectReferenceError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise ExpandedCapDirectReferenceError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


@contextmanager
def _advisory_process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / PROCESS_LOCK_NAME).open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another epoch-20 evaluator owns the process lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_contract(
    *, plan_path: Path | None = None, directive_path: Path | None = None
) -> dict[str, Any]:
    resolved_plan = (plan_path or PLAN_PATH).expanduser().resolve()
    plan = _load_json(resolved_plan, "epoch-20 semantic plan")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if not isinstance(step, Mapping):
        raise ExpandedCapDirectReferenceError("epoch-20 plan step is missing")
    resolved_directive = (
        directive_path.expanduser().resolve()
        if directive_path is not None
        else Path(str(step.get("directive_path") or DIRECTIVE_PATH)).expanduser().resolve()
    )
    directive = _load_json(resolved_directive, "epoch-20 direct-reference directive")
    execution = directive.get("execution_contract")
    witness = directive.get("witness_contract")
    acceptance = directive.get("acceptance_contract")
    terminal = directive.get("terminal_contract")
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
        set(plan) != expected_plan_keys
        or set(step) != expected_step_keys
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != MODEL_CALL_CAP
        or step.get("max_total_tokens") != TOTAL_TOKEN_CAP
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or Path(str(step.get("directive_path"))).expanduser().resolve() != resolved_directive
        or step.get("directive_sha256") != _sha256_file(resolved_directive)
        or directive.get("schema_version")
        != "pif_evaluation_epoch20_single_message_direct_reference_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or not isinstance(execution, Mapping)
        or not isinstance(witness, Mapping)
        or not isinstance(acceptance, Mapping)
        or not isinstance(terminal, Mapping)
        or execution.get("extraction_model_call_cap") != 0
        or execution.get("semantic_judge_call_cap") != MODEL_CALL_CAP
        or execution.get("required_ab_ba_call_count") != BASE_JUDGE_CALL_COUNT
        or execution.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
        or execution.get("semantic_retry_cap") != 0
        or execution.get("predecessor_extraction_model_call_count") != 1
        or execution.get("judge_model") != MODEL
        or execution.get("judge_reasoning_effort") != EFFORT
        or execution.get("judge_transport") != JUDGE_TRANSPORT
        or execution.get("judge_total_token_cap") != TOTAL_TOKEN_CAP
        or execution.get("adjudication_max_total_tokens")
        != ADJUDICATION_MAX_TOTAL_TOKENS
        or execution.get("variants") != ["ab", "ba"]
        or execution.get("all_events_direct_to_full_event_judge") is not True
        or execution.get("semantic_prefilter_allowed") is not False
        or execution.get("semantic_pruning_allowed") is not False
        or execution.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or execution.get("maximum_total_tokens_per_turn")
        != MAXIMUM_TOTAL_TOKENS_PER_TURN
        or execution.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
        or execution.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or execution.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or dict(witness) != EXPECTED_WITNESS_CONTRACT
        or dict(acceptance) != EXPECTED_ACCEPTANCE_CONTRACT
        or dict(terminal) != EXPECTED_TERMINAL_CONTRACT
    ):
        raise ExpandedCapDirectReferenceError("epoch-20 direct-reference contract drifted")
    expected_receipt = Path(str(step.get("expected_receipt_path"))).expanduser().resolve()
    if (
        expected_receipt != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json"
        or expected_receipt
        != Path(str(directive.get("expected_receipt_path"))).expanduser().resolve()
    ):
        raise ExpandedCapDirectReferenceError("epoch-20 receipt path drifted")

    raw_rows = directive.get("frozen_inputs")
    if not isinstance(raw_rows, list) or any(not isinstance(row, Mapping) for row in raw_rows):
        raise ExpandedCapDirectReferenceError("epoch-20 frozen inputs are malformed")
    by_role = {str(row.get("role")): dict(row) for row in raw_rows}
    if len(by_role) != len(raw_rows) or set(by_role) != set(EXPECTED_FROZEN_PATHS):
        raise ExpandedCapDirectReferenceError("epoch-20 frozen input roles drifted")
    frozen_records: dict[str, dict[str, Any]] = {}
    for role, expected_path in EXPECTED_FROZEN_PATHS.items():
        row = by_role[role]
        if Path(str(row.get("path"))).expanduser().resolve() != expected_path.resolve():
            raise ExpandedCapDirectReferenceError(f"{role} path drifted")
        frozen_records[role] = _validated_record(row, role)

    terminal = _load_json(
        Path(frozen_records["epoch19_terminal"]["path"]), "epoch-19 terminal"
    )
    receipt = _load_json(
        Path(frozen_records["epoch19_plan_step_receipt"]["path"]),
        "epoch-19 plan-step receipt",
    )
    if terminal != receipt:
        raise ExpandedCapDirectReferenceError("epoch-19 terminal mirrors differ")
    try:
        epoch19.verify_receipt(EPOCH19_ROOT)
    except epoch19.SingleMessageCompactCanaryError as exc:
        raise ExpandedCapDirectReferenceError("epoch-19 receipt verification failed") from exc
    artifact_records = terminal.get("artifact_records")
    if not isinstance(artifact_records, Mapping):
        raise ExpandedCapDirectReferenceError("epoch-19 artifact records are absent")
    terminal_roles = {
        "epoch19_runtime_lock": "lock",
        "epoch19_canonical_labels": "labels",
        "epoch19_evidence_provenance": "provenance",
        "epoch19_semantic_fidelity": "fidelity",
        "epoch19_sidecar": "sidecar",
        "epoch19_raw_output": "output",
        "epoch19_request": "request",
    }
    for role, terminal_role in terminal_roles.items():
        if artifact_records.get(terminal_role) != frozen_records[role]:
            raise ExpandedCapDirectReferenceError(
                f"epoch-19 terminal does not bind {role}"
            )
    usage = terminal.get("new_measured_usage")
    if (
        terminal.get("state") != "passed"
        or terminal.get("terminal_reason")
        != "epoch19_one_turn_canary_passed"
        or terminal.get("new_semantic_model_call_count") != 1
        or terminal.get("new_unknown_usage_turn_count") != 0
        or usage != EXPECTED_EXTRACTION_USAGE
        or terminal.get("winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
    ):
        raise ExpandedCapDirectReferenceError("epoch-19 passed accounting contract drifted")
    fidelity = _load_json(
        Path(frozen_records["epoch19_semantic_fidelity"]["path"]),
        "epoch-19 semantic fidelity",
    )
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + EXPECTED_EXTRACTION_USAGE["total_tokens"] * PRODUCTION_SCALE
    )
    production_ratio = production_total / BASELINE_END_TO_END_TOKENS
    if (
        production_total != EXPECTED_PRODUCTION_TOTAL_TOKENS
        or production_ratio != EXPECTED_PRODUCTION_TOKEN_RATIO
        or terminal.get("production_amortized_ratio") != production_ratio
        or terminal.get("failed_checks") != []
        or terminal.get("next_authorized_action")
        != "freeze_full_event_ab_ba_shared_reference_quality_evaluation"
        or fidelity.get("emitted_event_count") != EXPECTED_CANDIDATE_WITNESSES
        or fidelity.get("projected_event_count") != EXPECTED_CANDIDATE_WITNESSES
        or fidelity.get("semantic_postprocessing") is not False
        or fidelity.get("deterministic_semantic_pruning") is not False
        or fidelity.get("deterministic_relabeling") is not False
    ):
        raise ExpandedCapDirectReferenceError("epoch-19 structural or cost gate drifted")
    cost = execution.get("production_cost_projection")
    if not isinstance(cost, Mapping) or cost != {
        "baseline_end_to_end_tokens": BASELINE_END_TO_END_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "extraction_total_tokens": EXPECTED_EXTRACTION_USAGE["total_tokens"],
        "production_amortized_total_tokens": EXPECTED_PRODUCTION_TOTAL_TOKENS,
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "judge_tokens_included_in_production_formula": False,
    }:
        raise ExpandedCapDirectReferenceError("production-cost projection drifted")
    return {
        "plan": plan,
        "directive": directive,
        "plan_record": _record(resolved_plan),
        "directive_record": _record(resolved_directive),
        "frozen_records": frozen_records,
        "receipt_path": expected_receipt,
        "extraction_accounting": {
            "semantic_model_call_count": 1,
            "usage_status": "measured",
            "accounting_complete": True,
            "usage": dict(EXPECTED_EXTRACTION_USAGE),
        },
        "production_token_ratio": production_ratio,
    }


def _event_sha256(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(event).encode("utf-8")).hexdigest()


def _source_cases(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    private_input = source.get("private_input") if isinstance(source, Mapping) else None
    segments = private_input.get("segments") if isinstance(private_input, Mapping) else None
    if not isinstance(segments, list) or [row.get("segment_id") for row in segments] != list(
        SOURCE_SEGMENT_ORDER
    ) or source.get("segment_ids") != list(SOURCE_SEGMENT_ORDER):
        raise ExpandedCapDirectReferenceError("blind source segment order drifted")
    result = []
    for index, row in enumerate(segments):
        text = row.get("segment_text")
        if not isinstance(text, str) or not text:
            raise ExpandedCapDirectReferenceError("blind source segment text is missing")
        result.append(
            {
                "segment_id": str(row["segment_id"]),
                "source_excerpt": text,
                "density_stratum": row.get("density_stratum"),
                "source_index": index,
            }
        )
    return result


def _selected_baseline_events(
    baseline: Mapping[str, Any], source_by_id: Mapping[str, str]
) -> dict[str, list[dict[str, Any]]]:
    references = baseline.get("references") if isinstance(baseline, Mapping) else None
    if not isinstance(references, list):
        raise ExpandedCapDirectReferenceError("baseline references are missing")
    by_id: dict[str, Mapping[str, Any]] = {}
    for row in references:
        if not isinstance(row, Mapping):
            raise ExpandedCapDirectReferenceError("baseline reference is malformed")
        segment_id = str(row.get("segment_id") or "")
        if segment_id in by_id:
            raise ExpandedCapDirectReferenceError("baseline segment identity is duplicated")
        by_id[segment_id] = row
    result: dict[str, list[dict[str, Any]]] = {}
    for segment_id, source_text in source_by_id.items():
        row = by_id.get(segment_id)
        events = (row.get("golden_output") or {}).get("discourse_events") if row else None
        if not isinstance(events, list) or any(not isinstance(event, Mapping) for event in events):
            raise ExpandedCapDirectReferenceError("selected baseline events are malformed")
        copied = [deepcopy(dict(event)) for event in events]
        for event in copied:
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
                raise ExpandedCapDirectReferenceError(
                    "selected baseline exact-evidence lineage drifted"
                )
        result[segment_id] = copied
    return result


def _candidate_events_and_lineage(
    contract: Mapping[str, Any], cases: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    records = contract["frozen_records"]
    source = _load_json(Path(records["blind_source_packet"]["path"]), "blind source packet")
    epoch19_request = _load_json(
        Path(records["epoch19_request"]["path"]), "epoch-19 request"
    )
    if source != epoch19_request:
        raise ExpandedCapDirectReferenceError(
            "epoch-19 request differs from the blind source packet"
        )
    labels = _load_json(
        Path(records["epoch19_canonical_labels"]["path"]),
        "epoch-19 canonical labels",
    )
    provenance = _load_json(
        Path(records["epoch19_evidence_provenance"]["path"]),
        "epoch-19 evidence provenance",
    )
    provenance_segments = (
        provenance.get("segments") if isinstance(provenance, Mapping) else None
    )
    if (
        not isinstance(labels, list)
        or [row.get("segment_id") for row in labels] != list(SOURCE_SEGMENT_ORDER)
        or not isinstance(provenance_segments, list)
        or [row.get("segment_id") for row in provenance_segments]
        != list(SOURCE_SEGMENT_ORDER)
    ):
        raise ExpandedCapDirectReferenceError("epoch-19 candidate coverage drifted")
    source_by_id = {str(row["segment_id"]): str(row["source_excerpt"]) for row in cases}
    provenance_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for segment in provenance_segments:
        segment_id = str(segment.get("segment_id") or "")
        rows = segment.get("discourse_events")
        if not isinstance(rows, list):
            raise ExpandedCapDirectReferenceError(
                "epoch-19 provenance events are malformed"
            )
        for row in rows:
            if not isinstance(row, Mapping):
                raise ExpandedCapDirectReferenceError(
                    "epoch-19 provenance row is malformed"
                )
            item_index = row.get("item_index")
            if isinstance(item_index, bool) or not isinstance(item_index, int):
                raise ExpandedCapDirectReferenceError(
                    "epoch-19 provenance item index is malformed"
                )
            key = (segment_id, item_index)
            if key in provenance_by_key:
                raise ExpandedCapDirectReferenceError(
                    "epoch-19 provenance identity is duplicated"
                )
            provenance_by_key[key] = row
    result: dict[str, list[dict[str, Any]]] = {}
    lineage: list[dict[str, Any]] = []
    consumed = set()
    for segment in labels:
        segment_id = str(segment.get("segment_id") or "")
        events = segment.get("discourse_events")
        if not isinstance(events, list) or any(not isinstance(event, Mapping) for event in events):
            raise ExpandedCapDirectReferenceError("epoch-19 candidate events are malformed")
        result[segment_id] = []
        for index, raw_event in enumerate(events):
            event = deepcopy(dict(raw_event))
            row = provenance_by_key.get((segment_id, index))
            evidence = event.get("evidence")
            if not isinstance(row, Mapping) or not isinstance(evidence, str) or not evidence:
                raise ExpandedCapDirectReferenceError("candidate event provenance is incomplete")
            start_char = row.get("evidence_start")
            end_char = row.get("evidence_end")
            text = source_by_id[segment_id]
            if (
                isinstance(start_char, bool)
                or not isinstance(start_char, int)
                or isinstance(end_char, bool)
                or not isinstance(end_char, int)
                or text[start_char:end_char] != evidence
                or event.get("evidence_start") != start_char
                or event.get("evidence_end") != end_char
                or row.get("evidence_sha256")
                != hashlib.sha256(evidence.encode("utf-8")).hexdigest()
            ):
                raise ExpandedCapDirectReferenceError("candidate exact-evidence lineage drifted")
            consumed.add((segment_id, index))
            event_hash = _event_sha256(event)
            result[segment_id].append(event)
            lineage.append(
                {
                    "segment_id": segment_id,
                    "event_index": index,
                    "event_sha256": event_hash,
                    "evidence_sha256": row["evidence_sha256"],
                    "start_char": start_char,
                    "end_char": end_char,
                }
            )
    if (
        consumed != set(provenance_by_key)
        or len(lineage) != EXPECTED_CANDIDATE_WITNESSES
    ):
        raise ExpandedCapDirectReferenceError("candidate event/provenance multiset drifted")
    return result, lineage


def build_complete_container(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    records = contract["frozen_records"]
    source = _load_json(Path(records["blind_source_packet"]["path"]), "blind source packet")
    baseline = _load_json(
        Path(records["baseline_complete_output"]["path"]), "baseline complete output"
    )
    cases = _source_cases(source)
    source_by_id = {str(row["segment_id"]): str(row["source_excerpt"]) for row in cases}
    baseline_by_segment = _selected_baseline_events(baseline, source_by_id)
    candidate_by_segment, candidate_lineage = _candidate_events_and_lineage(contract, cases)
    candidate_lineage_by_key = {
        (row["segment_id"], row["event_index"]): row for row in candidate_lineage
    }
    raw_cases = []
    for case in cases:
        segment_id = str(case["segment_id"])
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": case["source_excerpt"],
                "event_set_a": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": SYSTEM_BASELINE,
                            "segment_id": segment_id,
                            "raw_event_index": index,
                            "raw_event_sha256": _event_sha256(event),
                        },
                    }
                    for index, event in enumerate(baseline_by_segment[segment_id])
                ],
                "event_set_b": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": SYSTEM_CANDIDATE,
                            "segment_id": segment_id,
                            "raw_event_index": index,
                            **candidate_lineage_by_key[(segment_id, index)],
                        },
                    }
                    for index, event in enumerate(candidate_by_segment[segment_id])
                ],
                "provenance": {
                    "segment_id": segment_id,
                    "density_stratum": case["density_stratum"],
                    "source_index": case["source_index"],
                },
            }
        )
    try:
        pool, mapping = judge.make_shared_witness_pool(raw_cases, seed=STEP_ID)
    except (TypeError, ValueError) as exc:
        raise ExpandedCapDirectReferenceError("151-witness pool construction failed") from exc
    if judge.validate_shared_witness_pool(pool):
        raise ExpandedCapDirectReferenceError("151-witness pool failed validation")
    observed: Counter[tuple[str, str]] = Counter()
    witness_event_multisets = {
        SYSTEM_BASELINE: Counter(),
        SYSTEM_CANDIDATE: Counter(),
    }
    candidate_witness_lineage: Counter[tuple[str, int, str]] = Counter()
    for case in mapping.get("cases") or []:
        for witness in case.get("witnesses") or []:
            provenance = witness.get("provenance") or {}
            segment_id = str(provenance.get("segment_id"))
            system_id = str(provenance.get("system_id"))
            original_event = provenance.get("original_event")
            if system_id not in witness_event_multisets or not isinstance(
                original_event, Mapping
            ):
                raise ExpandedCapDirectReferenceError(
                    "private all-event witness lineage drifted"
                )
            event_hash = _event_sha256(original_event)
            observed[(segment_id, system_id)] += 1
            witness_event_multisets[system_id][(segment_id, event_hash)] += 1
            if system_id == SYSTEM_CANDIDATE:
                raw_index = provenance.get("raw_event_index")
                bound_hash = provenance.get("event_sha256")
                if (
                    isinstance(raw_index, bool)
                    or not isinstance(raw_index, int)
                    or bound_hash != event_hash
                ):
                    raise ExpandedCapDirectReferenceError(
                        "candidate witness provenance hash drifted"
                    )
                candidate_witness_lineage[(segment_id, raw_index, event_hash)] += 1
    expected_observed = Counter(
        {key: count for key, count in EXPECTED_WITNESS_COUNTS.items() if count}
    )
    if observed != expected_observed:
        raise ExpandedCapDirectReferenceError(
            "120-baseline plus 31-candidate witness counts drifted"
        )
    expected_baseline_events = Counter(
        (segment_id, _event_sha256(event))
        for segment_id, events in baseline_by_segment.items()
        for event in events
    )
    expected_candidate_events = Counter(
        (segment_id, _event_sha256(event))
        for segment_id, events in candidate_by_segment.items()
        for event in events
    )
    expected_candidate_lineage = Counter(
        (str(row["segment_id"]), int(row["event_index"]), str(row["event_sha256"]))
        for row in candidate_lineage
    )
    if witness_event_multisets[SYSTEM_BASELINE] != expected_baseline_events:
        raise ExpandedCapDirectReferenceError(
            "baseline raw/witness event-hash multiset drifted"
        )
    if witness_event_multisets[SYSTEM_CANDIDATE] != expected_candidate_events:
        raise ExpandedCapDirectReferenceError(
            "candidate raw/witness event-hash multiset drifted"
        )
    if candidate_witness_lineage != expected_candidate_lineage:
        raise ExpandedCapDirectReferenceError(
            "candidate provenance events are not one-to-one with witnesses"
        )
    total = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    if total != EXPECTED_TOTAL_WITNESSES:
        raise ExpandedCapDirectReferenceError("shared witness total drifted")
    variants = judge.build_judge_variants(pool)
    ab_cases = variants["ab"]["cases"]
    ba_cases = variants["ba"]["cases"]
    if [row["case_id"] for row in ab_cases] != [row["case_id"] for row in ba_cases]:
        raise ExpandedCapDirectReferenceError("AB/BA source case order drifted")
    for ab, ba in zip(ab_cases, ba_cases):
        if ab["event_set_a"] != ba["event_set_b"] or ab["event_set_b"] != ba["event_set_a"]:
            raise ExpandedCapDirectReferenceError("AB/BA opaque witness order drifted")
    return pool, mapping, candidate_lineage


build_shared_pool = build_complete_container


def _runtime_files() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        Path(epoch19.__file__).resolve(),
        Path(shared_repair.__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(reserve.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
        PINNED_CODEX.resolve(),
    )


def _meaningful_root_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _build_capacity_policy(root: Path) -> Path:
    source = reserve.load_reserve_capacity_policy(SOURCE_POLICY)
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


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return {"root": root, "runtime_lock": lock_path}
    if root.exists() and _meaningful_root_entries(root):
        raise ExpandedCapDirectReferenceError("unfrozen epoch-20 root is not empty")
    contract = load_contract()
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise ExpandedCapDirectReferenceError("output root differs from epoch-20 contract")
    capacity_policy_path = _build_capacity_policy(root)
    pool, mapping, lineage = build_complete_container(contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    lineage_path = root / "candidate-lineage.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    _write_immutable_json(lineage_path, {"events": lineage, "event_count": len(lineage)})
    variants = judge.build_judge_variants(pool)
    request_records = []
    for name in ("ab", "ba"):
        prompt_path = root / "requests" / f"prompt-{name}.private.md"
        schema_path = root / "requests" / f"schema-{name}.json"
        _write_immutable_text(prompt_path, judge.build_judge_prompt(variants[name]))
        _write_immutable_json(schema_path, judge.semantic_judge_output_schema(variants[name]))
        request_records.extend((_record(prompt_path), _record(schema_path)))
    base_path = root / "requests/base-instructions.private.md"
    _write_immutable_text(base_path, judge.judge_base_instructions())
    request_records.append(_record(base_path))
    spec_path = root / "attempt-spec.json"
    _write_immutable_json(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "thread_id": THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "model": MODEL,
            "effort": EFFORT,
            "judge_transport": JUDGE_TRANSPORT,
            "managed_chatgpt_auth_required": True,
            "official_persistent_app_server_required": True,
            "variant_order": ["ab", "ba"],
            "source_segment_order": list(SOURCE_SEGMENT_ORDER),
            "baseline_witness_count": EXPECTED_BASELINE_WITNESSES,
            "candidate_witness_count": EXPECTED_CANDIDATE_WITNESSES,
            "all_event_witness_count": EXPECTED_TOTAL_WITNESSES,
            "all_events_direct_to_full_event_judge": True,
            "semantic_prefilter_applied": False,
            "deterministic_semantic_pruning_applied": False,
            "reference_system_ids": None,
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "required_ab_ba_call_count": BASE_JUDGE_CALL_COUNT,
            "adjudication_call_cap": ADJUDICATION_CALL_CAP,
            "semantic_total_token_cap": TOTAL_TOKEN_CAP,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "semantic_retry_count": 0,
            "timeout_seconds": JUDGE_TIMEOUT_SECONDS,
            "extraction_model_call_count": 0,
            "predecessor_extraction_model_call_count": 1,
            "predecessor_extraction_total_tokens": EXPECTED_EXTRACTION_USAGE[
                "total_tokens"
            ],
            "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
            "judge_tokens_included_in_production_formula": False,
            "winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
    )
    runtime_records = [_record(path) for path in _runtime_files()]
    source_records = [
        contract["plan_record"],
        contract["directive_record"],
        *contract["frozen_records"].values(),
    ]
    records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        _record(lineage_path),
        *request_records,
        _record(spec_path),
        _record(capacity_policy_path),
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
        "attempt_spec": _record(spec_path),
        "capacity_policy": _record(capacity_policy_path),
        "direct_record_digest": _record_digest(records),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "judge_transport": JUDGE_TRANSPORT,
        "managed_chatgpt_auth_required": True,
        "official_persistent_app_server_required": True,
        "required_ab_ba_call_count": BASE_JUDGE_CALL_COUNT,
        "adjudication_call_cap": ADJUDICATION_CALL_CAP,
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
        "timeout_seconds": JUDGE_TIMEOUT_SECONDS,
        "semantic_retry_count": 0,
        "extraction_model_call_cap": 0,
        "predecessor_extraction_model_call_count": 1,
        "all_event_witness_count": EXPECTED_TOTAL_WITNESSES,
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "judge_tokens_included_in_production_formula": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_immutable_json(lock_path, lock)
    verify_runtime_lock(lock_path, acquire_lock=False)
    return {"root": root, "runtime_lock": lock_path}


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return _freeze_unlocked(root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()

    def verify() -> dict[str, Any]:
        lock = _load_json(lock_path, "epoch-20 runtime lock")
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
        expected_runtime_paths = [str(path.resolve()) for path in _runtime_files()]
        actual_runtime_paths = [str(row.get("path")) for row in lock.get("runtime_files") or []]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or actual_runtime_paths != expected_runtime_paths
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("judge_transport") != JUDGE_TRANSPORT
            or lock.get("managed_chatgpt_auth_required") is not True
            or lock.get("official_persistent_app_server_required") is not True
            or lock.get("required_ab_ba_call_count") != BASE_JUDGE_CALL_COUNT
            or lock.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
            or lock.get("semantic_total_token_cap") != TOTAL_TOKEN_CAP
            or lock.get("maximum_total_tokens_per_turn")
            != MAXIMUM_TOTAL_TOKENS_PER_TURN
            or lock.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or lock.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
            or lock.get("timeout_seconds") != JUDGE_TIMEOUT_SECONDS
            or lock.get("semantic_retry_count") != 0
            or lock.get("extraction_model_call_cap") != 0
            or lock.get("predecessor_extraction_model_call_count") != 1
            or lock.get("all_event_witness_count") != EXPECTED_TOTAL_WITNESSES
            or lock.get("production_amortized_total_token_ratio")
            != EXPECTED_PRODUCTION_TOKEN_RATIO
            or lock.get("judge_tokens_included_in_production_formula") is not False
            or lock.get("winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutated") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(not isinstance(row, Mapping) or not _verify_record(row) for row in records)
        ):
            raise ExpandedCapDirectReferenceError("epoch-20 runtime lock drifted")
        attempt_spec_record = lock.get("attempt_spec")
        if not isinstance(attempt_spec_record, Mapping):
            raise ExpandedCapDirectReferenceError("epoch-20 attempt spec binding drifted")
        attempt_spec = _load_json(
            Path(str(attempt_spec_record.get("path"))), "epoch-20 attempt spec"
        )
        if (
            attempt_spec.get("schema_version") != SCHEMA_VERSION
            or attempt_spec.get("thread_id") != THREAD_ID
            or attempt_spec.get("plan_epoch") != PLAN_EPOCH
            or attempt_spec.get("step_id") != STEP_ID
            or attempt_spec.get("model") != MODEL
            or attempt_spec.get("effort") != EFFORT
            or attempt_spec.get("judge_transport") != JUDGE_TRANSPORT
            or attempt_spec.get("managed_chatgpt_auth_required") is not True
            or attempt_spec.get("official_persistent_app_server_required") is not True
            or attempt_spec.get("semantic_retry_count") != 0
            or attempt_spec.get("baseline_witness_count")
            != EXPECTED_BASELINE_WITNESSES
            or attempt_spec.get("candidate_witness_count")
            != EXPECTED_CANDIDATE_WITNESSES
            or attempt_spec.get("all_event_witness_count")
            != EXPECTED_TOTAL_WITNESSES
            or attempt_spec.get("maximum_total_tokens_per_turn")
            != MAXIMUM_TOTAL_TOKENS_PER_TURN
            or attempt_spec.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or attempt_spec.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
            or attempt_spec.get("timeout_seconds") != JUDGE_TIMEOUT_SECONDS
            or attempt_spec.get("winner_frozen") is not False
            or attempt_spec.get("holdout_authorized") is not False
            or attempt_spec.get("production_mutated") is not False
        ):
            raise ExpandedCapDirectReferenceError("epoch-20 attempt spec binding drifted")
        capacity_policy_record = lock.get("capacity_policy")
        if not isinstance(capacity_policy_record, Mapping):
            raise ExpandedCapDirectReferenceError(
                "epoch-20 capacity policy binding drifted"
            )
        capacity_policy_path = Path(str(capacity_policy_record.get("path")))
        policy = reserve.load_reserve_capacity_policy(capacity_policy_path)
        if (
            capacity_policy_path != lock_path.parent / "capacity-policy.json"
            or policy.get("phase_id") != STEP_ID
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
            raise ExpandedCapDirectReferenceError(
                "epoch-20 capacity policy contract drifted"
            )
        load_contract()
        return lock

    if acquire_lock:
        with _advisory_process_lock(lock_path.parent):
            return verify()
    return verify()


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


class Epoch20ReserveCapacityClient(ReserveCapacityGatedCodexAppServerClient):
    """Map the mature judge's artifact layout onto the audited reserve gate."""

    def _paths_for_index(self, turn_index: int) -> tuple[Path, Path, Path]:
        root = Path(self.policy["semantic_output_root"]).expanduser().resolve()
        rows = (
            (
                root / "judge/sidecars/ab.capacity.json",
                root / "judge/sidecars/ab.json",
                root / "judge/output-ab.private.json",
            ),
            (
                root / "judge/sidecars/ba.capacity.json",
                root / "judge/sidecars/ba.json",
                root / "judge/output-ba.private.json",
            ),
            (
                root / "adjudication/capacity.json",
                root / "adjudication/sidecar.json",
                root / "adjudication/output.private.json",
            ),
        )
        return rows[turn_index]

    def _turn_index(self, checkpoint_path: Any) -> tuple[int, Path]:
        if checkpoint_path is None:
            raise reserve.ReserveCapacityError(
                "reserve policy requires a capacity checkpoint"
            )
        checkpoint = Path(checkpoint_path).expanduser().resolve()
        for index in range(len(CAPACITY_TURN_NAMES)):
            if checkpoint == self._paths_for_index(index)[0]:
                return index, checkpoint
        raise reserve.ReserveCapacityError(
            "capacity checkpoint is outside the frozen epoch-20 phase"
        )

    def _verify_prior_turns(self, turn_index: int) -> None:
        maximum = int(self.policy["maximum_total_tokens_per_turn"])
        for index in range(turn_index):
            capacity_path, sidecar_path, output_path = self._paths_for_index(index)
            try:
                sidecar = _load_json(sidecar_path, "prior quality sidecar")
                capacity = _validate_capacity_checkpoint(capacity_path)
            except ExpandedCapDirectReferenceError as exc:
                raise reserve.ReserveCapacityError(
                    "a prior quality turn is not terminal and measured"
                ) from exc
            usage = sidecar.get("usage") if isinstance(sidecar, Mapping) else None
            total = usage.get("total_tokens") if isinstance(usage, Mapping) else None
            if (
                sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
                or isinstance(total, bool)
                or not isinstance(total, int)
                or not 0 <= total <= maximum
                or capacity.get("turn_ordinal") != index
                or capacity.get("turn_name") != CAPACITY_TURN_NAMES[index]
                or not output_path.is_file()
            ):
                raise reserve.ReserveCapacityError(
                    "a prior quality turn contract drifted"
                )


def _client_factory() -> Epoch20ReserveCapacityClient:
    return Epoch20ReserveCapacityClient(
        policy_path=CAPACITY_POLICY_PATH,
        inner_factory=_inner_factory,
    )


def _zero_accounting() -> dict[str, Any]:
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 0,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "turns": [],
    }


def _aggregate_usage(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    try:
        return shared_repair._aggregate_usage(sidecar_paths)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise ExpandedCapDirectReferenceError(str(exc)) from exc


def _fresh_attempt_accounting(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    try:
        return _aggregate_usage(sidecar_paths)
    except ExpandedCapDirectReferenceError as exc:
        raise OperationalWaitingError("fresh judge usage accounting is incomplete") from exc


def _best_effort_accounting(root: Path) -> dict[str, Any]:
    sidecars = [
        path
        for path in (
            root / "judge/sidecars/ab.json",
            root / "judge/sidecars/ba.json",
            root / "adjudication/sidecar.json",
        )
        if path.is_file()
    ]
    if not sidecars:
        return _zero_accounting() if not (root / "launch-receipt.json").is_file() else {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": 0,
            "usage": None,
            "turns": [],
        }
    try:
        return _aggregate_usage(sidecars)
    except ExpandedCapDirectReferenceError:
        return {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": len(sidecars),
            "usage": None,
            "turns": [],
        }


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _judge_variant_material(
    root: Path, pool: Mapping[str, Any], name: str
) -> tuple[dict[str, Any], str, dict[str, Any], str]:
    variants = judge.build_judge_variants(pool)
    if name not in variants:
        raise judge.JudgeArtifactError(f"unknown judge orientation: {name}")
    variant = variants[name]
    prompt = judge.build_judge_prompt(variant)
    schema = judge.semantic_judge_output_schema(variant)
    base = judge.judge_base_instructions()
    frozen_prompt = root / "requests" / f"prompt-{name}.private.md"
    frozen_schema = root / "requests" / f"schema-{name}.json"
    frozen_base = root / "requests" / "base-instructions.private.md"
    try:
        if frozen_prompt.read_text(encoding="utf-8") != prompt:
            raise judge.JudgeArtifactError("frozen judge prompt drifted")
        if _load_json(frozen_schema, "frozen judge schema") != schema:
            raise judge.JudgeArtifactError("frozen judge schema drifted")
        if frozen_base.read_text(encoding="utf-8") != base:
            raise judge.JudgeArtifactError("frozen judge base instructions drifted")
    except OSError as exc:
        raise judge.JudgeArtifactError("frozen judge request is unreadable") from exc
    return variant, prompt, schema, base


def _expected_judge_spec(root: Path, pool: Mapping[str, Any]) -> dict[str, Any]:
    prompt_hashes: dict[str, str] = {}
    schema_hashes: dict[str, str] = {}
    for name in ("ab", "ba"):
        _variant, prompt, schema, base = _judge_variant_material(root, pool, name)
        prompt_hashes[name] = _sha256_text(prompt)
        schema_hashes[name] = _sha256_text(_canonical_json(schema))
    return {
        "schema_version": judge.JUDGE_RUN_VERSION,
        "state": "frozen_before_model_calls",
        "pool_path": str((root / "shared-witness-pool.private.json").resolve()),
        "pool_file_sha256": _sha256_file(root / "shared-witness-pool.private.json"),
        "pool_content_sha256": _sha256_text(_canonical_json(pool)),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": JUDGE_TIMEOUT_SECONDS,
        "app_server_client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "transport": JUDGE_TRANSPORT,
        "capacity_gate": "managed_chatgpt_remaining_reserve_before_each_turn",
        "retry_count": 0,
        "base_instructions_sha256": _sha256_text(base),
        "variant_prompt_sha256": prompt_hashes,
        "variant_schema_sha256": schema_hashes,
        "case_ids": [case["case_id"] for case in pool["cases"]],
        "privacy": "private_analysis_only_prompts_and_outputs_are_private",
    }


def _validate_capacity_checkpoint(path: Path) -> dict[str, Any]:
    payload = _load_json(path, "managed-auth capacity checkpoint")
    expected_keys = {
        "schema_version",
        "checked_at",
        "policy_path",
        "policy_sha256",
        "phase_id",
        "turn_name",
        "turn_ordinal",
        "managed_chatgpt_auth_verified",
        "plan_type",
        "primary_used_percent",
        "primary_remaining_percent",
        "primary_resets_at",
        "rate_limit_reached_type",
        "minimum_remaining_reserve_percent",
        "usable_percent_above_reserve",
        "remaining_turn_count",
        "projected_remaining_tokens",
        "quota_points_per_million_tokens",
        "projected_remaining_quota_points",
        "projected_terminal_remaining_percent",
        "cleared_for_semantic_turn",
        "thread_started",
        "turn_started",
        "sidecar_started",
        "retry_checkpoint_reuse_allowed",
        "privacy",
    }
    used = payload.get("primary_used_percent") if isinstance(payload, Mapping) else None
    reset = payload.get("primary_resets_at") if isinstance(payload, Mapping) else None
    ordinal = payload.get("turn_ordinal") if isinstance(payload, Mapping) else None
    remaining_turn_count = (
        payload.get("remaining_turn_count") if isinstance(payload, Mapping) else None
    )
    projected_tokens = (
        payload.get("projected_remaining_tokens")
        if isinstance(payload, Mapping)
        else None
    )
    projected_points = (
        payload.get("projected_remaining_quota_points")
        if isinstance(payload, Mapping)
        else None
    )
    policy_path = (
        Path(str(payload.get("policy_path"))).expanduser().resolve()
        if isinstance(payload, Mapping) and isinstance(payload.get("policy_path"), str)
        else None
    )
    if (
        not isinstance(payload, Mapping)
        or set(payload) != expected_keys
        or payload.get("schema_version")
        != reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION
        or not isinstance(payload.get("checked_at"), str)
        or not payload.get("checked_at")
        or policy_path is None
        or not policy_path.is_file()
        or payload.get("policy_sha256") != _sha256_file(policy_path)
        or payload.get("phase_id") != STEP_ID
        or isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or not 0 <= ordinal < MODEL_CALL_CAP
        or payload.get("turn_name") != CAPACITY_TURN_NAMES[ordinal]
        or isinstance(used, bool)
        or not isinstance(used, int)
        or not 0 <= used <= 100
        or payload.get("primary_remaining_percent") != 100 - used
        or (
            reset is not None
            and (isinstance(reset, bool) or not isinstance(reset, int))
        )
        or payload.get("rate_limit_reached_type") is not None
        or payload.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or payload.get("usable_percent_above_reserve")
        != max(0, 100 - used - MINIMUM_REMAINING_RESERVE_PERCENT)
        or remaining_turn_count != MODEL_CALL_CAP - ordinal
        or projected_tokens
        != remaining_turn_count * MAXIMUM_TOTAL_TOKENS_PER_TURN
        or payload.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or projected_points
        != math.ceil(
            projected_tokens * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        )
        or payload.get("projected_terminal_remaining_percent")
        != 100 - used - projected_points
        or payload.get("cleared_for_semantic_turn") is not True
        or payload.get("managed_chatgpt_auth_verified") is not True
        or payload.get("plan_type") != "pro"
        or payload.get("thread_started") is not False
        or payload.get("turn_started") is not False
        or payload.get("sidecar_started") is not False
        or payload.get("retry_checkpoint_reuse_allowed") is not False
        or payload.get("privacy")
        != "capacity_status_policy_hash_and_counts_no_prompt_output_email_credentials_or_thread_ids"
    ):
        raise judge.JudgeArtifactError("managed-auth capacity checkpoint drifted")
    try:
        policy = reserve.load_reserve_capacity_policy(policy_path)
    except reserve.ReserveCapacityError as exc:
        raise judge.JudgeArtifactError("managed-auth capacity policy drifted") from exc
    semantic_root = Path(policy["semantic_output_root"]).expanduser().resolve()
    expected_paths = (
        semantic_root / "judge/sidecars/ab.capacity.json",
        semantic_root / "judge/sidecars/ba.capacity.json",
        semantic_root / "adjudication/capacity.json",
    )
    if (
        path.expanduser().resolve() != expected_paths[ordinal]
        or policy.get("phase_id") != STEP_ID
        or policy.get("ordered_turn_names") != list(CAPACITY_TURN_NAMES)
        or policy.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or policy.get("maximum_total_tokens_per_turn")
        != MAXIMUM_TOTAL_TOKENS_PER_TURN
        or policy.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
        or policy.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
    ):
        raise judge.JudgeArtifactError("managed-auth capacity lineage drifted")
    return dict(payload)


def _validate_completed_turn_checkpoint(
    *,
    output_path: Path,
    sidecar_path: Path,
    capacity_path: Path,
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    variant: Mapping[str, Any],
    batch_size: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
        raw_output_path=output_path,
        sidecar_path=sidecar_path,
        prompt=prompt,
        schema=schema,
        base_instructions=base_instructions,
        model=MODEL,
        reasoning_effort=EFFORT,
    )
    usage = sidecar.get("usage")
    thread_id = sidecar.get("thread_id")
    turn_id = sidecar.get("turn_id")
    if (
        sidecar.get("schema_version") != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or sidecar.get("cli_version") != codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("client_version") != codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("protocol_schema_sha256")
        != codex_app_server.PROTOCOL_SCHEMA_SHA256
        or sidecar.get("transport") != "stdio"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_mode") != "new_thread"
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("recovery_reran_model") is not False
        or sidecar.get("batch_size") != batch_size
        or not isinstance(thread_id, str)
        or not thread_id
        or not isinstance(turn_id, str)
        or not turn_id
        or sidecar.get("output_path") != str(output_path.resolve())
        or sidecar.get("prompt_bytes") != len(prompt.encode("utf-8"))
        or sidecar.get("base_instructions_bytes")
        != len(base_instructions.encode("utf-8"))
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(schema).encode("utf-8"))
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or not isinstance(usage, Mapping)
        or sidecar.get("thread_total_usage") != usage
    ):
        raise judge.JudgeArtifactError("completed app-server sidecar contract drifted")
    try:
        shared_repair._sidecar_usage(sidecar)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise judge.JudgeArtifactError("completed sidecar usage drifted") from exc
    if judge.validate_judge_output(output, variant):
        raise judge.JudgeArtifactError("completed judge output failed exact validation")
    _validate_capacity_checkpoint(capacity_path)
    return dict(output), dict(sidecar)


def _validate_judge_prepared_artifacts(
    root: Path, pool: Mapping[str, Any], *, require_spec: bool
) -> None:
    for name in ("ab", "ba"):
        _variant, prompt, schema, _base = _judge_variant_material(root, pool, name)
        prompt_path = root / "judge" / f"prompt-{name}.private.md"
        schema_path = root / "judge" / f"schema-{name}.json"
        if prompt_path.exists() and prompt_path.read_text(encoding="utf-8") != prompt:
            raise judge.JudgeArtifactError("judge working prompt differs from frozen request")
        if schema_path.exists() and _load_json(schema_path, "judge working schema") != schema:
            raise judge.JudgeArtifactError("judge working schema differs from frozen request")
    spec_path = root / "judge" / "judge-spec.json"
    if require_spec and not spec_path.is_file():
        raise judge.JudgeArtifactError("completed AB/BA bundle lacks judge spec")
    if spec_path.exists() and _load_json(spec_path, "judge spec") != _expected_judge_spec(
        root, pool
    ):
        raise judge.JudgeArtifactError("judge spec differs from the frozen evaluator")


def _validate_base_judge_bundle(
    root: Path, pool: Mapping[str, Any]
) -> dict[str, Any]:
    required = [
        root / "judge" / "judge-spec.json",
        root / "judge" / "report.json",
        root / "judge" / "consensus.private.json",
        *[
            root / "judge" / suffix
            for name in ("ab", "ba")
            for suffix in (
                f"prompt-{name}.private.md",
                f"schema-{name}.json",
                f"output-{name}.private.json",
                f"sidecars/{name}.json",
                f"sidecars/{name}.capacity.json",
            )
        ],
    ]
    if any(not path.is_file() for path in required):
        raise OperationalWaitingError("AB/BA bundle is incomplete")
    _validate_judge_prepared_artifacts(root, pool, require_spec=True)
    variants = judge.build_judge_variants(pool)
    outputs: dict[str, dict[str, Any]] = {}
    sidecars: dict[str, dict[str, Any]] = {}
    for name in ("ab", "ba"):
        _variant, prompt, schema, base = _judge_variant_material(root, pool, name)
        output, sidecar = _validate_completed_turn_checkpoint(
            output_path=root / "judge" / f"output-{name}.private.json",
            sidecar_path=root / "judge" / f"sidecars/{name}.json",
            capacity_path=root / "judge" / f"sidecars/{name}.capacity.json",
            prompt=prompt,
            schema=schema,
            base_instructions=base,
            variant=variants[name],
            batch_size=len(pool["cases"]),
        )
        outputs[name] = output
        sidecars[name] = sidecar
    thread_ids = [sidecars[name]["thread_id"] for name in ("ab", "ba")]
    turn_ids = [sidecars[name]["turn_id"] for name in ("ab", "ba")]
    if len(set(thread_ids)) != 2 or len(set(turn_ids)) != 2:
        raise judge.JudgeArtifactError("AB/BA thread or turn IDs are not unique")
    try:
        consensus = judge.combine_judge_consensus(pool, outputs)
    except (KeyError, TypeError, ValueError) as exc:
        raise judge.JudgeArtifactError("AB/BA consensus cannot be reconstructed") from exc
    stored_consensus_path = root / "judge" / "consensus.private.json"
    if _load_json(stored_consensus_path, "judge consensus") != consensus:
        raise judge.JudgeArtifactError("judge consensus differs from AB/BA outputs")
    accounting = _aggregate_usage(
        [root / "judge" / f"sidecars/{name}.json" for name in ("ab", "ba")]
    )
    report = _load_json(root / "judge" / "report.json", "judge report")
    expected_report_fields = {
        "schema_version": judge.JUDGE_RUN_VERSION,
        "state": "completed",
        "completed_at": max(str(sidecars[name].get("finished_at") or "") for name in ("ab", "ba")),
        "spec_sha256": _sha256_file(root / "judge" / "judge-spec.json"),
        "pool_sha256": _sha256_file(root / "shared-witness-pool.private.json"),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "transport": JUDGE_TRANSPORT,
        "variant_count": 2,
        "case_count": len(pool["cases"]),
        "witness_count": EXPECTED_TOTAL_WITNESSES,
        "validation_errors": {"ab": [], "ba": []},
        "consensus_sha256": _sha256_file(stored_consensus_path),
        "abstentions": consensus["abstentions"],
        "abstention_gate_applied": False,
        "selection_admissible": bool(
            consensus["selection_admissible"] and accounting["accounting_complete"]
        ),
        "sidecar_paths": [
            str((root / "judge" / f"sidecars/{name}.json").resolve())
            for name in ("ab", "ba")
        ],
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": accounting["usage"],
    }
    if not isinstance(report, Mapping) or any(
        report.get(key) != value for key, value in expected_report_fields.items()
    ):
        raise judge.JudgeArtifactError("judge report lineage or accounting drifted")
    return {
        "outputs": outputs,
        "sidecars": sidecars,
        "consensus": consensus,
        "accounting": accounting,
        "thread_ids": thread_ids,
        "turn_ids": turn_ids,
    }


def _validate_partial_base_artifacts(root: Path, pool: Mapping[str, Any]) -> None:
    _validate_judge_prepared_artifacts(
        root, pool, require_spec=(root / "judge/judge-spec.json").exists()
    )
    variants = judge.build_judge_variants(pool)
    for name in ("ab", "ba"):
        output_path = root / "judge" / f"output-{name}.private.json"
        sidecar_path = root / "judge" / f"sidecars/{name}.json"
        capacity_path = root / "judge" / f"sidecars/{name}.capacity.json"
        if output_path.is_file() and sidecar_path.is_file() and capacity_path.is_file():
            _variant, prompt, schema, base = _judge_variant_material(root, pool, name)
            _validate_completed_turn_checkpoint(
                output_path=output_path,
                sidecar_path=sidecar_path,
                capacity_path=capacity_path,
                prompt=prompt,
                schema=schema,
                base_instructions=base,
                variant=variants[name],
                batch_size=len(pool["cases"]),
            )
            continue
        if capacity_path.is_file():
            _validate_capacity_checkpoint(capacity_path)
        if output_path.is_file():
            output = _load_json(output_path, f"partial {name} output")
            if judge.validate_judge_output(output, variants[name]):
                raise judge.JudgeArtifactError("partial judge output is invalid")
        if sidecar_path.is_file():
            sidecar = _load_json(sidecar_path, f"partial {name} sidecar")
            if not isinstance(sidecar, Mapping):
                raise judge.JudgeArtifactError("partial judge sidecar is malformed")
            for key, expected in {
                "transport": "stdio",
                "auth_type": "chatgpt",
                "thread_mode": "new_thread",
                "model": MODEL,
                "effort": EFFORT,
            }.items():
                if key in sidecar and sidecar.get(key) != expected:
                    raise judge.JudgeArtifactError(
                        f"partial judge sidecar mismatch: {key}"
                    )


def _preflight_adjudication(accounting: Mapping[str, Any]) -> None:
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    used = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if (
        accounting.get("accounting_complete") is not True
        or accounting.get("measured_model_call_count") != BASE_JUDGE_CALL_COUNT
        or isinstance(used, bool)
        or not isinstance(used, int)
        or used < 0
        or used + ADJUDICATION_MAX_TOTAL_TOKENS > TOTAL_TOKEN_CAP
    ):
        raise OperationalWaitingError("adjudication lacks a measured bounded AB/BA preflight")


def _enforce_adjudication_usage(
    accounting: Mapping[str, Any], *, measured_ab_ba_total: int
) -> None:
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    total = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if (
        accounting.get("accounting_complete") is not True
        or accounting.get("measured_model_call_count") != 1
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
        or total > ADJUDICATION_MAX_TOTAL_TOKENS
        or measured_ab_ba_total + total > TOTAL_TOKEN_CAP
    ):
        raise OperationalWaitingError("adjudication exceeded its measured token envelope")


def _normalized_consensus_metadata(consensus: Mapping[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(dict(consensus))
    support_total = 0
    support_abstain = 0
    alignment_total = 0
    alignment_abstain = 0
    topology_abstain = 0
    partition_abstain = 0
    for case in normalized.get("cases") or []:
        supports = case.get("support_results") or []
        alignments = case.get("alignment_results") or []
        support_total += len(supports)
        support_abstain += sum(row.get("verdict") == "abstain" for row in supports)
        alignment_total += len(alignments)
        alignment_abstain += sum(row.get("relation") == "abstain" for row in alignments)
        topology_abstain += bool(case.get("alignment_abstained_witness_ids"))
        partition_abstain += bool(case.get("partition_abstained_witness_ids"))

    def rate(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 6) if denominator else 0.0

    case_count = len(normalized.get("cases") or [])
    normalized["abstentions"] = {
        "support": {
            "numerator": support_abstain,
            "denominator": support_total,
            "rate": rate(support_abstain, support_total),
        },
        "alignment_labels": {
            "numerator": alignment_abstain,
            "denominator": alignment_total,
            "rate": rate(alignment_abstain, alignment_total),
        },
        "alignment_topology": {
            "numerator": topology_abstain,
            "denominator": case_count,
            "rate": rate(topology_abstain, case_count),
        },
        "equivalence_partition": {
            "numerator": partition_abstain,
            "denominator": case_count,
            "rate": rate(partition_abstain, case_count),
        },
    }
    normalized["selection_admissible"] = True
    normalized["abstention_gate_applied"] = False
    return normalized


def _witness_systems(mapping: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for case in mapping.get("cases") or []:
        for witness in case.get("witnesses") or []:
            witness_id = str(witness.get("witness_id"))
            system_id = str((witness.get("provenance") or {}).get("system_id"))
            if system_id not in {SYSTEM_BASELINE, SYSTEM_CANDIDATE} or witness_id in result:
                raise ExpandedCapDirectReferenceError("private witness system lineage drifted")
            result[witness_id] = system_id
    return result


def _macro_scores(
    *, pool: Mapping[str, Any], mapping: Mapping[str, Any], consensus: Mapping[str, Any]
) -> dict[str, Any]:
    systems = _witness_systems(mapping)
    provenance = {
        str(row["case_id"]): row.get("case_provenance") or {}
        for row in mapping.get("cases") or []
    }
    pool_cases = {str(row["case_id"]): row for row in pool.get("cases") or []}
    totals = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
    case_scores = []
    for row in consensus.get("cases") or []:
        case_id = str(row.get("case_id"))
        if case_id not in pool_cases or row.get("partition_abstained_witness_ids"):
            raise ExpandedCapDirectReferenceError("cannot score an abstained or unknown partition")
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
            raise ExpandedCapDirectReferenceError("shared-reference partition omits a witness")
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
                "density_stratum": provenance.get(case_id, {}).get("density_stratum"),
                "systems": rows,
            }
        )
    if len(case_scores) != len(pool_cases) or not case_scores:
        raise ExpandedCapDirectReferenceError("macro score does not cover every source case")
    return {
        "systems": {
            system_id: {
                "strict_full_field_macro_f1": round(sum(values) / len(values), 6)
            }
            for system_id, values in totals.items()
        },
        "cases": case_scores,
    }


def score_shared_reference(
    *,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    consensus: Mapping[str, Any],
    accounting: Mapping[str, Any],
    adjudication_call_count: int,
) -> dict[str, Any]:
    shared = judge.score_named_systems_against_shared_reference(
        pool=pool,
        private_mapping=mapping,
        consensus=consensus,
        reference_system_ids=None,
    )
    reference_system_ids = shared.get("reference_system_ids")
    if (
        not isinstance(reference_system_ids, list)
        or len(reference_system_ids) != 2
        or len(set(reference_system_ids)) != 2
        or set(reference_system_ids) != {SYSTEM_BASELINE, SYSTEM_CANDIDATE}
    ):
        raise ExpandedCapDirectReferenceError("shared union is not symmetric across both systems")
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
    usage = accounting.get("usage") or {}
    calls = accounting.get("measured_model_call_count")
    total_tokens = usage.get("total_tokens")
    if (
        accounting.get("accounting_complete") is not True
        or isinstance(calls, bool)
        or not isinstance(calls, int)
        or calls not in {BASE_JUDGE_CALL_COUNT, MODEL_CALL_CAP}
        or isinstance(total_tokens, bool)
        or not isinstance(total_tokens, int)
        or total_tokens > TOTAL_TOKEN_CAP
        or adjudication_call_count not in {0, 1}
        or calls != BASE_JUDGE_CALL_COUNT + adjudication_call_count
    ):
        raise OperationalWaitingError("judge accounting or semantic call count is inadmissible")
    checks = {
        "candidate_strict_full_field_macro_f1_gte_0_97": candidate_macro >= QUALITY_THRESHOLD,
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "production_amortized_total_token_ratio_lte_0_28": (
            EXPECTED_PRODUCTION_TOKEN_RATIO <= TOKEN_RATIO_TARGET
        ),
        "exact_evidence_rate_1": witness_count == EXPECTED_TOTAL_WITNESSES
        and exact_count == witness_count,
    }
    failed = sorted(name for name, passed in checks.items() if passed is not True)
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "systems": shared["systems"],
        "macro": macro,
        "reference_system_ids": shared["reference_system_ids"],
        "shared_reference_unit_count": shared["reference_unit_count"],
        "candidate_strict_full_field_macro_f1": candidate_macro,
        "baseline_strict_full_field_macro_f1": baseline_macro,
        "candidate_noninferiority_delta": round(candidate_macro - baseline_macro, 6),
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "judge_tokens_included_in_production_formula": False,
        "exact_evidence_rate": round(exact_count / witness_count, 6),
        "semantic_model_call_count": calls,
        "semantic_total_tokens": total_tokens,
        "adjudication_call_count": adjudication_call_count,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _observable_disagreement_case_ids(
    pool: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    try:
        return shared_repair._observable_disagreement_case_ids(pool, outputs)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise ExpandedCapDirectReferenceError(str(exc)) from exc


def _adjudication_material(
    pool: Mapping[str, Any], case_ids: Sequence[str]
) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    try:
        variant = shared_repair._build_adjudication_variant(pool, case_ids)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise ExpandedCapDirectReferenceError(str(exc)) from exc
    instructions = judge.judge_base_instructions()
    prompt = (
        "This is the sole origin-neutral adjudication. Independently judge complete-event "
        "source support and the full exact-evidence semantic-equivalence partition. Do not "
        "vote between or reference prior outputs. Abstain when the source cannot resolve the "
        "question. The measured total-token acceptance ceiling for this turn is 100000.\n\n"
        + judge.build_judge_prompt(variant)
    )
    return variant, instructions, prompt, judge.semantic_judge_output_schema(variant)


def _prepare_adjudication_artifacts(
    root: Path, pool: Mapping[str, Any], case_ids: Sequence[str]
) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    phase = root / "adjudication"
    variant, instructions, prompt, schema = _adjudication_material(pool, case_ids)
    _write_immutable_json(phase / "variant.private.json", variant)
    _write_immutable_text(phase / "base-instructions.private.md", instructions)
    _write_immutable_text(phase / "prompt.private.md", prompt)
    _write_immutable_json(phase / "schema.json", schema)
    return variant, instructions, prompt, schema


def _validate_adjudication_prepared_artifacts(
    root: Path, pool: Mapping[str, Any], case_ids: Sequence[str]
) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    phase = root / "adjudication"
    variant, instructions, prompt, schema = _adjudication_material(pool, case_ids)
    if (
        _load_json(phase / "variant.private.json", "adjudication variant") != variant
        or phase.joinpath("base-instructions.private.md").read_text(encoding="utf-8")
        != instructions
        or phase.joinpath("prompt.private.md").read_text(encoding="utf-8") != prompt
        or _load_json(phase / "schema.json", "adjudication schema") != schema
    ):
        raise judge.JudgeArtifactError("adjudication request artifacts drifted")
    return variant, instructions, prompt, schema


def _adjudication_launch_receipt(
    *, root: Path, case_ids: Sequence[str], base_accounting: Mapping[str, Any]
) -> dict[str, Any]:
    path = root / "adjudication" / "adjudication-launch-receipt.json"
    phase = root / "adjudication"
    payload = {
        "schema_version": ADJUDICATION_LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "base_launch_receipt": _record(root / "launch-receipt.json"),
        "judge_report": _record(root / "judge/report.json"),
        "judge_consensus": _record(root / "judge/consensus.private.json"),
        "variant": _record(phase / "variant.private.json"),
        "base_instructions": _record(phase / "base-instructions.private.md"),
        "prompt": _record(phase / "prompt.private.md"),
        "schema": _record(phase / "schema.json"),
        "observable_disagreement_case_ids": list(case_ids),
        "observable_disagreement_case_count": len(case_ids),
        "model": MODEL,
        "effort": EFFORT,
        "judge_transport": JUDGE_TRANSPORT,
        "managed_chatgpt_auth_required": True,
        "semantic_model_call_count_before_adjudication": BASE_JUDGE_CALL_COUNT,
        "semantic_retry_count": 0,
        "adjudication_total_token_cap": ADJUDICATION_MAX_TOTAL_TOKENS,
        "base_usage": base_accounting.get("usage"),
        "production_mutated": False,
        "holdout_authorized": False,
    }
    _write_immutable_json(path, payload)
    return payload


def _validate_adjudication_launch_receipt(
    *, root: Path, case_ids: Sequence[str], base_accounting: Mapping[str, Any]
) -> dict[str, Any]:
    phase = root / "adjudication"
    payload = _load_json(
        phase / "adjudication-launch-receipt.json", "adjudication launch receipt"
    )
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != ADJUDICATION_LAUNCH_VERSION
        or not isinstance(payload.get("launched_at"), str)
        or not payload.get("launched_at")
        or payload.get("thread_id") != THREAD_ID
        or payload.get("plan_epoch") != PLAN_EPOCH
        or payload.get("step_id") != STEP_ID
        or payload.get("runtime_lock") != _record(root / "runtime-lock.json")
        or payload.get("base_launch_receipt") != _record(root / "launch-receipt.json")
        or payload.get("judge_report") != _record(root / "judge/report.json")
        or payload.get("judge_consensus")
        != _record(root / "judge/consensus.private.json")
        or payload.get("variant") != _record(phase / "variant.private.json")
        or payload.get("base_instructions")
        != _record(phase / "base-instructions.private.md")
        or payload.get("prompt") != _record(phase / "prompt.private.md")
        or payload.get("schema") != _record(phase / "schema.json")
        or payload.get("observable_disagreement_case_ids") != list(case_ids)
        or payload.get("observable_disagreement_case_count") != len(case_ids)
        or payload.get("model") != MODEL
        or payload.get("effort") != EFFORT
        or payload.get("judge_transport") != JUDGE_TRANSPORT
        or payload.get("managed_chatgpt_auth_required") is not True
        or payload.get("semantic_model_call_count_before_adjudication")
        != BASE_JUDGE_CALL_COUNT
        or payload.get("semantic_retry_count") != 0
        or payload.get("adjudication_total_token_cap")
        != ADJUDICATION_MAX_TOTAL_TOKENS
        or payload.get("base_usage") != base_accounting.get("usage")
        or payload.get("production_mutated") is not False
        or payload.get("holdout_authorized") is not False
    ):
        raise judge.JudgeArtifactError("adjudication launch receipt drifted")
    return dict(payload)


async def _run_adjudication(
    *,
    root: Path,
    pool: Mapping[str, Any],
    case_ids: Sequence[str],
    client_factory: Callable[[], Any],
    timeout_seconds: float,
    measured_ab_ba_total: int,
) -> tuple[dict[str, Any], Path]:
    phase = root / "adjudication"
    if any(
        (phase / name).exists()
        for name in (
            "adjudication-launch-receipt.json",
            "sidecar.json",
            "capacity.json",
            "output.private.json",
        )
    ):
        raise OperationalWaitingError("the sole adjudication already has an immutable attempt")
    variant, instructions, prompt, schema = _prepare_adjudication_artifacts(
        root, pool, case_ids
    )
    base_accounting = _aggregate_usage(
        [root / "judge/sidecars/ab.json", root / "judge/sidecars/ba.json"]
    )
    _adjudication_launch_receipt(
        root=root, case_ids=case_ids, base_accounting=base_accounting
    )
    sidecar = phase / "sidecar.json"
    output_path = phase / "output.private.json"
    async with client_factory() as client:
        result = await client.run_ephemeral_structured_turn(
            model=MODEL,
            effort=EFFORT,
            base_instructions=instructions,
            prompt=prompt,
            output_schema=schema,
            cwd=PROJECT_ROOT,
            sidecar_path=sidecar,
            output_path=output_path,
            capacity_checkpoint_path=phase / "capacity.json",
            batch_size=len(case_ids),
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    if not result.status_ok or not isinstance(result.output, Mapping):
        raise OperationalWaitingError("origin-neutral adjudication did not complete")
    output = _load_json(output_path, "origin-neutral adjudication output")
    if judge.validate_judge_output(output, variant):
        raise OperationalWaitingError("origin-neutral adjudication output is invalid")
    accounting = _fresh_attempt_accounting([sidecar])
    _enforce_adjudication_usage(accounting, measured_ab_ba_total=measured_ab_ba_total)
    return output, sidecar


def _validate_completed_adjudication(
    *,
    root: Path,
    pool: Mapping[str, Any],
    case_ids: Sequence[str],
    base_bundle: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    phase = root / "adjudication"
    variant, instructions, prompt, schema = _validate_adjudication_prepared_artifacts(
        root, pool, case_ids
    )
    _validate_adjudication_launch_receipt(
        root=root, case_ids=case_ids, base_accounting=base_bundle["accounting"]
    )
    output, sidecar = _validate_completed_turn_checkpoint(
        output_path=phase / "output.private.json",
        sidecar_path=phase / "sidecar.json",
        capacity_path=phase / "capacity.json",
        prompt=prompt,
        schema=schema,
        base_instructions=instructions,
        variant=variant,
        batch_size=len(case_ids),
    )
    if (
        sidecar["thread_id"] in set(base_bundle["thread_ids"])
        or sidecar["turn_id"] in set(base_bundle["turn_ids"])
    ):
        raise judge.JudgeArtifactError(
            "adjudication thread or turn ID reuses an AB/BA identity"
        )
    third_accounting = _aggregate_usage([phase / "sidecar.json"])
    _enforce_adjudication_usage(
        third_accounting,
        measured_ab_ba_total=base_bundle["accounting"]["usage"]["total_tokens"],
    )
    return output, sidecar


def _merge_adjudication(
    consensus: Mapping[str, Any], output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    try:
        merged = shared_repair._merge_adjudication(  # noqa: SLF001
            consensus=consensus,
            adjudication_output=output,
            adjudicated_case_ids=case_ids,
        )
    except shared_repair.SharedReferenceRepairError as exc:
        raise ExpandedCapDirectReferenceError(str(exc)) from exc
    return _normalized_consensus_metadata(merged)


def _reconstruct_bound_consensus(
    *, pool: Mapping[str, Any], records: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], int]:
    outputs: dict[str, Mapping[str, Any]] = {}
    for name in ("ab", "ba"):
        record = records.get(f"judge_output_{name}")
        if not isinstance(record, Mapping):
            raise ExpandedCapDirectReferenceError("quality receipt lacks both AB/BA outputs")
        output = _load_json(Path(str(record["path"])), f"receipt-bound {name} output")
        if not isinstance(output, Mapping):
            raise ExpandedCapDirectReferenceError("receipt-bound judge output is malformed")
        outputs[name] = output
    try:
        base_consensus = judge.combine_judge_consensus(pool, outputs)
    except (KeyError, TypeError, ValueError) as exc:
        raise ExpandedCapDirectReferenceError("AB/BA outputs cannot reconstruct consensus") from exc
    disagreements = _observable_disagreement_case_ids(pool, outputs)
    adjudication_record = records.get("adjudication_output")
    adjudication_sidecar = records.get("adjudication_sidecar")
    if disagreements:
        if not isinstance(adjudication_record, Mapping) or not isinstance(
            adjudication_sidecar, Mapping
        ):
            raise ExpandedCapDirectReferenceError("observable disagreement lacks adjudication")
        adjudication = _load_json(
            Path(str(adjudication_record["path"])), "receipt-bound adjudication"
        )
        try:
            variant = shared_repair._build_adjudication_variant(pool, disagreements)  # noqa: SLF001
        except shared_repair.SharedReferenceRepairError as exc:
            raise ExpandedCapDirectReferenceError(str(exc)) from exc
        if not isinstance(adjudication, Mapping) or judge.validate_judge_output(
            adjudication, variant
        ):
            raise ExpandedCapDirectReferenceError("receipt-bound adjudication is invalid")
        return _merge_adjudication(base_consensus, adjudication, disagreements), base_consensus, 1
    if adjudication_record is not None or adjudication_sidecar is not None:
        raise ExpandedCapDirectReferenceError("adjudication exists without observable disagreement")
    return _normalized_consensus_metadata(base_consensus), base_consensus, 0


def _launch_receipt(root: Path) -> dict[str, Any]:
    path = root / "launch-receipt.json"
    payload = {
        "schema_version": LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "pool": _record(root / "shared-witness-pool.private.json"),
        "model": MODEL,
        "effort": EFFORT,
        "judge_transport": JUDGE_TRANSPORT,
        "managed_chatgpt_auth_required": True,
        "variant_order": ["ab", "ba"],
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_count": 0,
        "production_mutated": False,
        "holdout_authorized": False,
    }
    _write_immutable_json(path, payload)
    return payload


def _validate_launch_receipt(root: Path) -> dict[str, Any]:
    launch = _load_json(root / "launch-receipt.json", "epoch-20 launch receipt")
    if (
        launch.get("schema_version")
        != LAUNCH_VERSION
        or launch.get("thread_id") != THREAD_ID
        or launch.get("plan_epoch") != PLAN_EPOCH
        or launch.get("step_id") != STEP_ID
        or launch.get("runtime_lock") != _record(root / "runtime-lock.json")
        or launch.get("pool") != _record(root / "shared-witness-pool.private.json")
        or launch.get("model") != MODEL
        or launch.get("effort") != EFFORT
        or launch.get("judge_transport") != JUDGE_TRANSPORT
        or launch.get("managed_chatgpt_auth_required") is not True
        or launch.get("variant_order") != ["ab", "ba"]
        or launch.get("semantic_model_call_cap") != MODEL_CALL_CAP
        or launch.get("semantic_retry_count") != 0
        or launch.get("extraction_model_call_count") != 0
        or launch.get("production_mutated") is not False
        or launch.get("holdout_authorized") is not False
    ):
        raise ExpandedCapDirectReferenceError("epoch-20 launch receipt drifted")
    return launch


def _optional_record(path: Path) -> dict[str, Any] | None:
    return _record(path) if path.is_file() else None


def _receipt_records(root: Path) -> dict[str, Any]:
    return {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "pool": _record(root / "shared-witness-pool.private.json"),
        "mapping": _record(root / "private-witness-mapping.private.json"),
        "candidate_lineage": _record(root / "candidate-lineage.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "launch_receipt": _optional_record(root / "launch-receipt.json"),
        "judge_spec": _optional_record(root / "judge/judge-spec.json"),
        "judge_prompt_ab": _optional_record(root / "judge/prompt-ab.private.md"),
        "judge_prompt_ba": _optional_record(root / "judge/prompt-ba.private.md"),
        "judge_schema_ab": _optional_record(root / "judge/schema-ab.json"),
        "judge_schema_ba": _optional_record(root / "judge/schema-ba.json"),
        "judge_report": _optional_record(root / "judge/report.json"),
        "judge_output_ab": _optional_record(root / "judge/output-ab.private.json"),
        "judge_output_ba": _optional_record(root / "judge/output-ba.private.json"),
        "judge_sidecar_ab": _optional_record(root / "judge/sidecars/ab.json"),
        "judge_sidecar_ba": _optional_record(root / "judge/sidecars/ba.json"),
        "judge_capacity_ab": _optional_record(root / "judge/sidecars/ab.capacity.json"),
        "judge_capacity_ba": _optional_record(root / "judge/sidecars/ba.capacity.json"),
        "adjudication_launch_receipt": _optional_record(
            root / "adjudication/adjudication-launch-receipt.json"
        ),
        "adjudication_variant": _optional_record(root / "adjudication/variant.private.json"),
        "adjudication_base_instructions": _optional_record(
            root / "adjudication/base-instructions.private.md"
        ),
        "adjudication_prompt": _optional_record(root / "adjudication/prompt.private.md"),
        "adjudication_schema": _optional_record(root / "adjudication/schema.json"),
        "adjudication_output": _optional_record(root / "adjudication/output.private.json"),
        "adjudication_sidecar": _optional_record(root / "adjudication/sidecar.json"),
        "adjudication_capacity": _optional_record(root / "adjudication/capacity.json"),
        "consensus": _optional_record(root / "consensus.private.json"),
        "score": _optional_record(root / "shared-reference-score.json"),
    }


def _receipt(
    *,
    root: Path,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    score: Mapping[str, Any] | None,
    error: Exception | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise ExpandedCapDirectReferenceError("invalid epoch-20 receipt state")
    records = _receipt_records(root)
    payload = {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_at": now_iso(),
        "terminal_reason": terminal_reason,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_model_call_count": accounting.get("measured_model_call_count", 0),
        "semantic_attempt_count": 1 if records["launch_receipt"] is not None else 0,
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_count": 0,
        "predecessor_extraction_model_call_count": 1,
        "predecessor_extraction_total_tokens": EXPECTED_EXTRACTION_USAGE[
            "total_tokens"
        ],
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "judge_usage_is_development_qa_overhead": True,
        "judge_tokens_included_in_production_formula": False,
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "development_quality_passed": bool(score and score.get("passed")),
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout": False,
        "holdout_authorized": False,
        "production": False,
        "production_mutated": False,
        "records": records,
        "next_action": (
            "freeze_epoch19_development_winner_and_prepare_untouched_holdout"
            if state == "passed"
            else "retain_epoch19_candidate_as_quality_rejected"
            if state == "rejected"
            else "operator_review_of_immutable_zero_retry_waiting_attempt"
        ),
        "privacy": "sanitized metrics counts hashes and failure class only",
    }
    if score is not None:
        payload.update(
            {
                "failed_checks": score.get("failed_checks"),
                "candidate_strict_full_field_macro_f1": score.get(
                    "candidate_strict_full_field_macro_f1"
                ),
                "baseline_strict_full_field_macro_f1": score.get(
                    "baseline_strict_full_field_macro_f1"
                ),
                "candidate_noninferiority_delta": score.get("candidate_noninferiority_delta"),
                "exact_evidence_rate": score.get("exact_evidence_rate"),
            }
        )
    if error is not None:
        message = str(error).encode("utf-8")
        payload.update(
            {
                "error_class": type(error).__name__,
                "error_message_sha256": hashlib.sha256(message).hexdigest(),
                "error_message_size_bytes": len(message),
            }
        )
    return payload


def _flatten_records(records: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [row for row in records.values() if isinstance(row, Mapping)]


def verify_receipt(
    root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    acquire_lock: bool = True,
    _allow_single_terminal_mirror: bool = False,
) -> dict[str, Any]:
    root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        lock = verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        receipt_path = root / "plan-step-receipt.json"
        terminal_path = root / "terminal.json"
        existing_mirrors = [path for path in (receipt_path, terminal_path) if path.is_file()]
        if len(existing_mirrors) == 1 and _allow_single_terminal_mirror:
            receipt = _load_json(existing_mirrors[0], "single epoch-20 terminal mirror")
            terminal = receipt
        elif len(existing_mirrors) == 2:
            receipt = _load_json(receipt_path, "epoch-20 receipt")
            terminal = _load_json(terminal_path, "epoch-20 terminal")
        else:
            raise ExpandedCapDirectReferenceError("epoch-20 terminal mirrors are incomplete")
        if receipt != terminal:
            raise ExpandedCapDirectReferenceError("epoch-20 terminal mirrors differ")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or receipt.get("semantic_retry_count") != 0
            or receipt.get("extraction_model_call_count") != 0
            or receipt.get("predecessor_extraction_model_call_count") != 1
            or receipt.get("predecessor_extraction_total_tokens")
            != EXPECTED_EXTRACTION_USAGE["total_tokens"]
            or receipt.get("judge_tokens_included_in_production_formula") is not False
            or receipt.get("production_amortized_total_token_ratio")
            != EXPECTED_PRODUCTION_TOKEN_RATIO
            or receipt.get("winner_frozen") is not False
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production") is not False
            or receipt.get("production_mutated") is not False
        ):
            raise ExpandedCapDirectReferenceError("epoch-20 receipt contract drifted")
        records = receipt.get("records")
        if not isinstance(records, Mapping):
            raise ExpandedCapDirectReferenceError("epoch-20 receipt records are missing")
        present = _flatten_records(records)
        if not present or any(not _verify_record(row) for row in present):
            raise ExpandedCapDirectReferenceError("epoch-20 receipt artifact integrity failed")
        if (
            records.get("runtime_lock") != _record(root / "runtime-lock.json")
            or records.get("pool") != lock.get("pool")
            or records.get("mapping") != lock.get("mapping")
            or records.get("candidate_lineage") != lock.get("candidate_lineage")
            or records.get("attempt_spec") != lock.get("attempt_spec")
        ):
            raise ExpandedCapDirectReferenceError("receipt/runtime lineage drifted")
        call_count = receipt.get("semantic_model_call_count")
        if isinstance(call_count, bool) or not isinstance(call_count, int) or not 0 <= call_count <= 3:
            raise ExpandedCapDirectReferenceError("receipt semantic call count drifted")
        if receipt.get("semantic_attempt_count") == 1:
            if records.get("launch_receipt") != _record(root / "launch-receipt.json"):
                raise ExpandedCapDirectReferenceError("attempt receipt lacks launch lineage")
            _validate_launch_receipt(root)
        elif receipt.get("semantic_attempt_count") != 0 or records.get("launch_receipt") is not None:
            raise ExpandedCapDirectReferenceError("semantic attempt count drifted")

        sidecar_records = [
            records.get("judge_sidecar_ab"),
            records.get("judge_sidecar_ba"),
            records.get("adjudication_sidecar"),
        ]
        sidecar_paths = [Path(str(row["path"])) for row in sidecar_records if isinstance(row, Mapping)]
        if receipt["state"] == "waiting":
            if records.get("score") is not None or receipt.get("development_quality_passed") is not False:
                raise ExpandedCapDirectReferenceError("waiting receipt cannot authorize quality")
            if receipt.get("accounting_complete") is True:
                accounting = _aggregate_usage(sidecar_paths) if sidecar_paths else _zero_accounting()
                if (
                    receipt.get("usage_status") != "complete"
                    or receipt.get("usage") != accounting["usage"]
                    or call_count != accounting["measured_model_call_count"]
                ):
                    raise ExpandedCapDirectReferenceError("waiting receipt accounting drifted")
            elif receipt.get("usage_status") != "unknown" or receipt.get("usage") is not None:
                raise ExpandedCapDirectReferenceError("unknown waiting accounting drifted")
            return receipt

        required = (
            "judge_spec",
            "judge_prompt_ab",
            "judge_prompt_ba",
            "judge_schema_ab",
            "judge_schema_ba",
            "judge_report",
            "judge_output_ab",
            "judge_output_ba",
            "judge_sidecar_ab",
            "judge_sidecar_ba",
            "judge_capacity_ab",
            "judge_capacity_ba",
            "consensus",
            "score",
        )
        if any(records.get(key) is None for key in required):
            raise ExpandedCapDirectReferenceError("quality receipt lacks required judge evidence")
        if receipt.get("semantic_attempt_count") != 1:
            raise ExpandedCapDirectReferenceError(
                "quality receipt requires exactly one launched base attempt"
            )
        pool = _load_json(root / "shared-witness-pool.private.json", "receipt pool")
        mapping = _load_json(root / "private-witness-mapping.private.json", "receipt mapping")
        try:
            base_bundle = _validate_base_judge_bundle(root, pool)
        except (judge.JudgeArtifactError, OperationalWaitingError) as exc:
            raise ExpandedCapDirectReferenceError(
                "quality receipt base judge lineage drifted"
            ) from exc
        consensus = _load_json(Path(str(records["consensus"]["path"])), "receipt consensus")
        score = _load_json(Path(str(records["score"]["path"])), "receipt score")
        report = _load_json(Path(str(records["judge_report"]["path"])), "receipt judge report")
        reconstructed, base_consensus, adjudication_calls = _reconstruct_bound_consensus(
            pool=pool, records=records
        )
        if consensus != reconstructed:
            raise ExpandedCapDirectReferenceError("final consensus reconstruction drifted")
        disagreements = _observable_disagreement_case_ids(pool, base_bundle["outputs"])
        adjudication_record_keys = (
            "adjudication_launch_receipt",
            "adjudication_variant",
            "adjudication_base_instructions",
            "adjudication_prompt",
            "adjudication_schema",
            "adjudication_output",
            "adjudication_sidecar",
            "adjudication_capacity",
        )
        if adjudication_calls:
            if any(records.get(key) is None for key in adjudication_record_keys):
                raise ExpandedCapDirectReferenceError(
                    "quality receipt lacks complete adjudication lineage"
                )
            try:
                _validate_completed_adjudication(
                    root=root,
                    pool=pool,
                    case_ids=disagreements,
                    base_bundle=base_bundle,
                )
            except (judge.JudgeArtifactError, OperationalWaitingError) as exc:
                raise ExpandedCapDirectReferenceError(
                    "quality receipt adjudication lineage drifted"
                ) from exc
        elif any(records.get(key) is not None for key in adjudication_record_keys):
            raise ExpandedCapDirectReferenceError(
                "quality receipt binds adjudication without a third call"
            )
        accounting = _aggregate_usage(sidecar_paths)
        ab_ba_accounting = _aggregate_usage(sidecar_paths[:2])
        judge_consensus_path = root / "judge/consensus.private.json"
        judge_consensus = _load_json(judge_consensus_path, "judge consensus")
        if (
            report.get("schema_version") != judge.JUDGE_RUN_VERSION
            or report.get("state") != "completed"
            or report.get("model") != MODEL
            or report.get("reasoning_effort") != EFFORT
            or report.get("variant_count") != 2
            or report.get("accounting_complete") is not True
            or report.get("usage_status") != "complete"
            or report.get("usage") != ab_ba_accounting["usage"]
            or report.get("consensus_sha256") != _sha256_file(judge_consensus_path)
            or judge_consensus != base_consensus
        ):
            raise ExpandedCapDirectReferenceError("judge report or base consensus drifted")
        if (
            receipt.get("usage_status") != "complete"
            or receipt.get("accounting_complete") is not True
            or receipt.get("usage") != accounting["usage"]
            or call_count != accounting["measured_model_call_count"]
        ):
            raise ExpandedCapDirectReferenceError("quality receipt accounting drifted")
        recomputed = score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=consensus,
            accounting=accounting,
            adjudication_call_count=adjudication_calls,
        )
        if score != recomputed:
            raise ExpandedCapDirectReferenceError("quality score does not recompute exactly")
        metric_fields = (
            "failed_checks",
            "candidate_strict_full_field_macro_f1",
            "baseline_strict_full_field_macro_f1",
            "candidate_noninferiority_delta",
            "exact_evidence_rate",
        )
        if any(receipt.get(field) != score.get(field) for field in metric_fields):
            raise ExpandedCapDirectReferenceError("receipt quality metrics differ from score")
        if (
            (receipt["state"] == "passed") != (score.get("passed") is True)
            or (receipt["state"] == "rejected") != (score.get("passed") is False)
            or receipt.get("development_quality_passed") != (score.get("passed") is True)
        ):
            raise ExpandedCapDirectReferenceError("receipt state is not the exact quality projection")
        return receipt

    if acquire_lock:
        with _advisory_process_lock(root):
            return verify()
    return verify()


def _semantic_attempt_artifacts(root: Path) -> list[Path]:
    return sorted(
        [path for phase in (root / "judge", root / "adjudication") if phase.exists() for path in phase.rglob("*") if path.is_file()]
    )


def _recover_terminal_mirror(root: Path) -> dict[str, Any] | None:
    receipt_path = root / "plan-step-receipt.json"
    terminal_path = root / "terminal.json"
    if not receipt_path.exists() and not terminal_path.exists():
        return None
    if receipt_path.exists() and terminal_path.exists():
        return verify_receipt(root, acquire_lock=False)
    existing = receipt_path if receipt_path.exists() else terminal_path
    missing = terminal_path if receipt_path.exists() else receipt_path
    payload = verify_receipt(
        root,
        acquire_lock=False,
        _allow_single_terminal_mirror=True,
    )
    _write_immutable_json(missing, payload)
    return verify_receipt(root, acquire_lock=False)


async def _run_reserve_semantic_judge(
    *,
    pool_path: Path,
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    client_factory: Callable[[], Any],
) -> dict[str, Any]:
    """Execute the mature combined support/equivalence judge with reserve metadata."""

    if model != MODEL or reasoning_effort != EFFORT or timeout_seconds != JUDGE_TIMEOUT_SECONDS:
        raise judge.JudgeArtifactError("epoch-20 judge invocation drifted")
    pool_file = pool_path.expanduser().resolve()
    root = output_dir.expanduser().resolve()
    experiment_root = root.parent
    pool = _load_json(pool_file, "epoch-20 shared witness pool")
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise judge.JudgeArtifactError("epoch-20 shared witness pool is invalid")
    if any(path.is_file() for path in root.rglob("*")):
        raise judge.JudgeArtifactError(
            "epoch-20 judge artifacts already exist; replay is prohibited"
        )
    root.mkdir(parents=True, exist_ok=True)
    variants = judge.build_judge_variants(pool)
    base = judge.judge_base_instructions()
    _write_immutable_text(root / "base-instructions.private.md", base)
    prepared: dict[str, dict[str, Any]] = {}
    for name in ("ab", "ba"):
        prompt = judge.build_judge_prompt(variants[name])
        schema = judge.semantic_judge_output_schema(variants[name])
        paths = {
            "prompt": root / f"prompt-{name}.private.md",
            "schema": root / f"schema-{name}.json",
            "output": root / f"output-{name}.private.json",
            "sidecar": root / "sidecars" / f"{name}.json",
            "capacity": root / "sidecars" / f"{name}.capacity.json",
        }
        _write_immutable_text(paths["prompt"], prompt)
        _write_immutable_json(paths["schema"], schema)
        prepared[name] = {"prompt": prompt, "schema": schema, "paths": paths}
    spec = _expected_judge_spec(experiment_root, pool)
    _write_immutable_json(root / "judge-spec.json", spec)

    outputs: dict[str, dict[str, Any]] = {}
    async with client_factory() as client:
        for name in ("ab", "ba"):
            item = prepared[name]
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=base,
                prompt=item["prompt"],
                output_schema=item["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=item["paths"]["sidecar"],
                capacity_checkpoint_path=item["paths"]["capacity"],
                output_path=item["paths"]["output"],
                batch_size=len(pool["cases"]),
                thread_mode="new_thread",
                timeout_seconds=JUDGE_TIMEOUT_SECONDS,
            )
            if not result.status_ok or not isinstance(result.output, Mapping):
                raise judge.JudgeAttemptFailed(
                    variant=name,
                    error_class=str(result.error_class or result.status or "turn_failed"),
                    sidecar_path=item["paths"]["sidecar"],
                )
            outputs[name] = dict(result.output)

    sidecars: dict[str, dict[str, Any]] = {}
    validation: dict[str, list[str]] = {}
    for name in ("ab", "ba"):
        item = prepared[name]
        output, sidecar = _validate_completed_turn_checkpoint(
            output_path=item["paths"]["output"],
            sidecar_path=item["paths"]["sidecar"],
            capacity_path=item["paths"]["capacity"],
            prompt=item["prompt"],
            schema=item["schema"],
            base_instructions=base,
            variant=variants[name],
            batch_size=len(pool["cases"]),
        )
        if _canonical_json(output) != _canonical_json(outputs[name]):
            raise judge.JudgeArtifactError(
                "returned and checkpointed epoch-20 outputs differ"
            )
        validation[name] = judge.validate_judge_output(output, variants[name])
        if validation[name]:
            raise judge.JudgeArtifactError(
                "epoch-20 semantic judge output failed exact validation"
            )
        sidecars[name] = sidecar

    consensus = judge.combine_judge_consensus(pool, outputs)
    consensus_path = root / "consensus.private.json"
    _write_immutable_json(consensus_path, consensus)
    accounting = _aggregate_usage(
        [prepared[name]["paths"]["sidecar"] for name in ("ab", "ba")]
    )
    report = {
        "schema_version": judge.JUDGE_RUN_VERSION,
        "state": "completed",
        "completed_at": max(
            str(sidecars[name].get("finished_at") or "") for name in ("ab", "ba")
        ),
        "spec_sha256": _sha256_file(root / "judge-spec.json"),
        "pool_sha256": _sha256_file(pool_file),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "transport": JUDGE_TRANSPORT,
        "variant_count": 2,
        "case_count": len(pool["cases"]),
        "witness_count": EXPECTED_TOTAL_WITNESSES,
        "validation_errors": validation,
        "consensus_sha256": _sha256_file(consensus_path),
        "abstentions": consensus["abstentions"],
        "abstention_gate_applied": False,
        "selection_admissible": bool(
            consensus["selection_admissible"] and accounting["accounting_complete"]
        ),
        "sidecar_paths": [
            str(prepared[name]["paths"]["sidecar"].resolve())
            for name in ("ab", "ba")
        ],
        "accounting_complete": accounting["accounting_complete"],
        "usage_status": accounting["usage_status"],
        "usage": accounting["usage"],
    }
    _write_immutable_json(root / "report.json", report)
    return report


def _write_terminal_receipt(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _write_immutable_json(root / "plan-step-receipt.json", receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return verify_receipt(root, acquire_lock=False)


async def _finalize_validated_base_bundle(
    *,
    root: Path,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    base_bundle: Mapping[str, Any],
    client_factory: Callable[[], Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    outputs = base_bundle["outputs"]
    base_consensus = base_bundle["consensus"]
    accounting = base_bundle["accounting"]
    sidecars = [root / "judge/sidecars/ab.json", root / "judge/sidecars/ba.json"]
    disagreements = _observable_disagreement_case_ids(pool, outputs)
    adjudication_calls = 0
    if disagreements:
        _preflight_adjudication(accounting)
        phase = root / "adjudication"
        marker = phase / "adjudication-launch-receipt.json"
        semantic_paths = [
            phase / "sidecar.json",
            phase / "capacity.json",
            phase / "output.private.json",
        ]
        if marker.is_file():
            _validate_adjudication_prepared_artifacts(root, pool, disagreements)
            _validate_adjudication_launch_receipt(
                root=root,
                case_ids=disagreements,
                base_accounting=accounting,
            )
            if any(not path.is_file() for path in semantic_paths):
                if (phase / "capacity.json").is_file():
                    _validate_capacity_checkpoint(phase / "capacity.json")
                raise PartialAttemptWaitingError(
                    "adjudication marker exists with a partial immutable attempt"
                )
            adjudicated, _adjudication_sidecar = _validate_completed_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreements,
                base_bundle=base_bundle,
            )
        else:
            if any(path.exists() for path in semantic_paths):
                raise judge.JudgeArtifactError(
                    "adjudication semantic artifacts exist without a launch marker"
                )
            adjudicated, _adjudication_sidecar = await _run_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreements,
                client_factory=client_factory,
                timeout_seconds=timeout_seconds,
                measured_ab_ba_total=accounting["usage"]["total_tokens"],
            )
            adjudicated, _adjudication_sidecar = _validate_completed_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreements,
                base_bundle=base_bundle,
            )
        sidecars.append(root / "adjudication/sidecar.json")
        accounting = _fresh_attempt_accounting(sidecars)
        if accounting["usage"]["total_tokens"] > TOTAL_TOKEN_CAP:
            raise OperationalWaitingError("final semantic usage exceeded the total cap")
        consensus = _merge_adjudication(base_consensus, adjudicated, disagreements)
        adjudication_calls = 1
    else:
        if any(path.is_file() for path in (root / "adjudication").glob("**/*")):
            raise judge.JudgeArtifactError(
                "adjudication artifacts exist without observable AB/BA disagreement"
            )
        consensus = _normalized_consensus_metadata(base_consensus)
    _write_immutable_json(root / "consensus.private.json", consensus)
    score = score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        accounting=accounting,
        adjudication_call_count=adjudication_calls,
    )
    _write_immutable_json(root / "shared-reference-score.json", score)
    state = "passed" if score["passed"] else "rejected"
    return _receipt(
        root=root,
        state=state,
        terminal_reason=(
            "epoch20_single_message_full_event_quality_passed"
            if score["passed"]
            else "epoch20_single_message_full_event_quality_rejected"
        ),
        accounting=accounting,
        score=score,
    )


async def _run_unlocked(
    *,
    root: Path,
    timeout_seconds: float,
    client_factory: Callable[[], Any],
    judge_runner: Callable[..., Any],
) -> dict[str, Any]:
    recovered = _recover_terminal_mirror(root)
    if recovered is not None:
        return recovered
    _freeze_unlocked(root)
    if timeout_seconds != JUDGE_TIMEOUT_SECONDS:
        raise ExpandedCapDirectReferenceError("epoch-20 timeout differs from the frozen spec")
    pool = _load_json(root / "shared-witness-pool.private.json", "epoch-20 pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "epoch-20 mapping")
    launch_exists = (root / "launch-receipt.json").is_file()
    artifacts_exist = bool(_semantic_attempt_artifacts(root))
    if artifacts_exist and not launch_exists:
        raise ExpandedCapDirectReferenceError(
            "semantic artifacts exist without a base launch receipt"
        )
    try:
        if launch_exists:
            _validate_launch_receipt(root)
        else:
            _launch_receipt(root)
            await judge_runner(
                pool_path=root / "shared-witness-pool.private.json",
                output_dir=root / "judge",
                model=MODEL,
                reasoning_effort=EFFORT,
                timeout_seconds=timeout_seconds,
                client_factory=client_factory,
            )
        try:
            base_bundle = _validate_base_judge_bundle(root, pool)
        except OperationalWaitingError as exc:
            _validate_partial_base_artifacts(root, pool)
            raise PartialAttemptWaitingError(
                "partial AB/BA attempt is immutable and cannot be resumed"
            ) from exc
        receipt = await _finalize_validated_base_bundle(
            root=root,
            pool=pool,
            mapping=mapping,
            base_bundle=base_bundle,
            client_factory=client_factory,
            timeout_seconds=timeout_seconds,
        )
    except (
        OperationalWaitingError,
        reserve.ReserveCapacityError,
        codex_app_server.AppServerError,
        judge.JudgeAttemptFailed,
        judge.JudgeArtifactError,
        asyncio.TimeoutError,
        OSError,
    ) as exc:
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason=(
                "epoch20_partial_or_interrupted_attempt_preserved_without_replay"
                if isinstance(exc, PartialAttemptWaitingError)
                else "epoch20_direct_reference_operational_or_capacity_waiting"
            ),
            accounting=_best_effort_accounting(root),
            score=None,
            error=exc,
        )
    return _write_terminal_receipt(root, receipt)


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = JUDGE_TIMEOUT_SECONDS,
    client_factory: Callable[[], Any] = _client_factory,
    judge_runner: Callable[..., Any] = _run_reserve_semantic_judge,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return await _run_unlocked(
            root=root,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
            judge_runner=judge_runner,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify-runtime", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=JUDGE_TIMEOUT_SECONDS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "prepare":
        frozen = freeze_run(args.output_dir)
        result = {"state": "prepared", "runtime_lock": str(frozen["runtime_lock"])}
    elif args.action == "verify-runtime":
        lock = verify_runtime_lock(args.output_dir / "runtime-lock.json")
        result = {"state": "prepared", "verified": True, "step_id": lock["step_id"]}
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds))
        result = {"state": receipt["state"], "terminal_reason": receipt["terminal_reason"]}
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
