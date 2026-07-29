from __future__ import annotations

"""Evaluate the frozen v249 true full output against one shared reference.

This epoch-2 adapter performs no extraction.  It blinds all sixty submissions
from the frozen candidate evaluator bundle, delegates full-event support and
equivalence decisions to the mature AB/BA app-server judge, permits at most one
origin-neutral adjudication, and scores both systems against the same supported
union.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_candidate_shared_reference_repair as v1
from . import app_server_capacity
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_candidate_true_full_output_shared_reference_v2"
LOCK_VERSION = "pif_candidate_true_full_output_shared_reference_lock_v2"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
DIRECTIVE_VERSION = "pif_evaluation_true_full_output_reference_directive_v2"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 2
STEP_ID = "true_full_output_shared_reference_v2"
MODEL = "gpt-5.5"
EFFORT = "high"
MODEL_CALL_CAP = 3
ADJUDICATION_CALL_CAP = 1
TOTAL_TOKEN_CAP = 400_000
DEFAULT_ADJUDICATION_TOKEN_RESERVE = 100_000
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_TARGET = 0.28
PRODUCTION_TOKEN_RATIO = 0.192218
SYSTEM_BASELINE = v1.SYSTEM_BASELINE
SYSTEM_CANDIDATE = v1.SYSTEM_CANDIDATE
EXPECTED_WITNESS_COUNTS = {
    ("dense", SYSTEM_BASELINE): 27,
    ("dense", SYSTEM_CANDIDATE): 32,
    ("no_signal", SYSTEM_BASELINE): 0,
    ("no_signal", SYSTEM_CANDIDATE): 1,
}
EXPECTED_TOTAL_WITNESSES = 60

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = (PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v1.json").resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation" / "pif-evaluation-true-full-output-reference-v2.json"
).resolve()
DEFAULT_BUNDLE_PATH = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-canary-v249-source-complete-columnar-owner-v2-semantic-evaluation-v1"
    / "support"
    / "support-bundle.private.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-selection-v249-true-full-output-shared-reference-v2"
).resolve()
PINNED_CODEX = v1.PINNED_CODEX
USAGE_FIELDS = v1.USAGE_FIELDS
PROCESS_LOCK_NAME = ".true-full-output-shared-reference.lock"


class TrueFullOutputRepairError(RuntimeError):
    """The epoch-2 contract or its immutable artifacts are invalid."""


class OperationalWaitingError(TrueFullOutputRepairError):
    """An operational or judge-budget condition requires a waiting receipt."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrueFullOutputRepairError(f"cannot read {label}") from exc


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


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        path = Path(str(record["path"]))
        if path.name.startswith("capacity") or path.name.endswith(".capacity.json"):
            return False
        return _record(path) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _write_immutable_json(path: Path, value: Any) -> None:
    try:
        v1._write_immutable_json(path, value)  # noqa: SLF001
    except v1.SharedReferenceRepairError as exc:
        raise TrueFullOutputRepairError(str(exc)) from exc


def _write_immutable_text(path: Path, value: str) -> None:
    try:
        v1._write_immutable_text(path, value)  # noqa: SLF001
    except v1.SharedReferenceRepairError as exc:
        raise TrueFullOutputRepairError(str(exc)) from exc


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


@contextmanager
def _advisory_process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / PROCESS_LOCK_NAME
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another epoch-2 evaluator process owns the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _execution_contract(directive: Mapping[str, Any]) -> Mapping[str, Any]:
    value = directive.get("execution_contract")
    if not isinstance(value, Mapping):
        raise TrueFullOutputRepairError("directive execution contract is missing")
    return value


def _acceptance_contract(directive: Mapping[str, Any]) -> Mapping[str, Any]:
    value = directive.get("acceptance_contract")
    if not isinstance(value, Mapping):
        raise TrueFullOutputRepairError("directive acceptance contract is missing")
    return value


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "true-full-output directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    execution = _execution_contract(directive)
    acceptance = _acceptance_contract(directive)
    scope = directive.get("scope_correction")
    predecessor = directive.get("predecessor_receipt")
    if (
        plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or not isinstance(step, Mapping)
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != MODEL_CALL_CAP
        or step.get("max_total_tokens") != TOTAL_TOKEN_CAP
        or Path(str(step.get("directive_path"))).resolve() != DIRECTIVE_PATH
        or step.get("directive_sha256") != _sha256_file(DIRECTIVE_PATH)
        or directive.get("schema_version") != DIRECTIVE_VERSION
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or execution.get("extraction_model_call_cap") != 0
        or execution.get("semantic_judge_call_cap") != MODEL_CALL_CAP
        or execution.get("semantic_retry_cap") != 0
        or execution.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
        or execution.get("judge_model") != MODEL
        or execution.get("judge_reasoning_effort") != EFFORT
        or execution.get("variants") != ["ab", "ba"]
        or execution.get("judge_transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or execution.get("support_scope")
        != "complete_event_all_populated_semantic_fields_and_exact_evidence"
        or execution.get("variant_case_order_and_opaque_ids_must_match") is not True
        or execution.get("process_claim_required") is not True
        or execution.get("existing_receipt_integrity_verification_required") is not True
        or execution.get("systems_scored_against_identical_reference")
        != [SYSTEM_BASELINE, SYSTEM_CANDIDATE]
        or execution.get("deterministic_semantic_pruning_allowed") is not False
        or acceptance.get("candidate_strict_full_field_macro_f1_min")
        != QUALITY_THRESHOLD
        or acceptance.get("candidate_must_be_noninferior_to_baseline") is not True
        or acceptance.get("production_amortized_total_token_ratio_max")
        != TOKEN_RATIO_TARGET
        or acceptance.get("exact_evidence_rate") != 1.0
        or acceptance.get("accounting_complete") is not True
        or acceptance.get("quality_failure_is_rejected") is not True
        or acceptance.get("operational_failure_is_waiting") is not True
        or not isinstance(scope, Mapping)
        or scope.get("reference_submission_count") != 27
        or scope.get("candidate_submission_count") != 33
        or scope.get("total_submission_count") != EXPECTED_TOTAL_WITNESSES
        or scope.get("dense_reference_submission_count") != 27
        or scope.get("dense_candidate_submission_count") != 32
        or scope.get("no_signal_reference_submission_count") != 0
        or scope.get("no_signal_candidate_submission_count") != 1
        or scope.get("all_submitted_events_must_reach_llm_support_and_equivalence_judgment")
        is not True
        or scope.get("prior_support_or_alignment_filtering_allowed") is not False
        or not isinstance(predecessor, Mapping)
        or predecessor.get("state") != "rejected"
        or predecessor.get("final_acceptance_evidence") is not False
    ):
        raise TrueFullOutputRepairError("epoch-2 semantic plan or directive drifted")
    expected_receipt = Path(str(step.get("expected_receipt_path"))).resolve()
    directive_receipt = Path(str(directive.get("expected_receipt_path"))).resolve()
    if expected_receipt != directive_receipt or expected_receipt != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json":
        raise TrueFullOutputRepairError("epoch-2 expected receipt path drifted")
    frozen_rows = directive.get("frozen_inputs")
    if not isinstance(frozen_rows, list) or not frozen_rows:
        raise TrueFullOutputRepairError("epoch-2 frozen inputs are missing")
    predecessor_path = Path(str(predecessor.get("path"))).resolve()
    if (
        not predecessor_path.is_file()
        or predecessor.get("sha256") != _sha256_file(predecessor_path)
    ):
        raise TrueFullOutputRepairError("epoch-1 predecessor receipt drifted")
    predecessor_record = _record(predecessor_path)
    frozen_records = []
    for row in frozen_rows:
        if not isinstance(row, Mapping):
            raise TrueFullOutputRepairError("epoch-2 frozen input is malformed")
        path = Path(str(row.get("path"))).resolve()
        if not path.is_file() or row.get("sha256") != _sha256_file(path):
            raise TrueFullOutputRepairError("epoch-2 frozen input drifted")
        frozen_records.append(_record(path))
    bundle_records = [row for row in frozen_records if Path(row["path"]).name == "support-bundle.private.json"]
    if len(bundle_records) != 1 or Path(bundle_records[0]["path"]) != DEFAULT_BUNDLE_PATH:
        raise TrueFullOutputRepairError("true full-output support bundle is not uniquely frozen")
    ratio = directive.get("production_amortized_total_token_ratio")
    if ratio is None:
        invalidated = directive.get("invalidated_gate") or {}
        ratio = invalidated.get("production_amortized_total_token_ratio")
    if ratio is None:
        ratio = PRODUCTION_TOKEN_RATIO
    if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or ratio < 0:
        raise TrueFullOutputRepairError("production token ratio is not frozen")
    reserve = execution.get("adjudication_token_reserve", DEFAULT_ADJUDICATION_TOKEN_RESERVE)
    if isinstance(reserve, bool) or not isinstance(reserve, int) or reserve <= 0:
        raise TrueFullOutputRepairError("adjudication token reserve is invalid")
    return {
        "plan": plan,
        "directive": directive,
        "execution": execution,
        "acceptance": acceptance,
        "receipt_path": expected_receipt,
        "frozen_records": frozen_records,
        "predecessor_record": predecessor_record,
        "bundle_path": DEFAULT_BUNDLE_PATH,
        "production_token_ratio": float(ratio),
        "adjudication_token_reserve": reserve,
    }


def build_shared_pool(contract: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    bundle = _load_json(Path(contract["bundle_path"]), "frozen true full-output bundle")
    if (
        bundle.get("schema_version") != "pif_candidate_semantic_evaluation_bundle_v1"
        or not isinstance(bundle.get("cases"), list)
        or not isinstance(bundle.get("origins"), list)
        or (bundle.get("counts") or {}).get("total_witness_count") != EXPECTED_TOTAL_WITNESSES
        or (bundle.get("counts") or {}).get("reference_witness_count") != 27
        or (bundle.get("counts") or {}).get("candidate_witness_count") != 33
    ):
        raise TrueFullOutputRepairError("frozen true full-output bundle shape drifted")
    origin_by_id = {str(row.get("witness_id")): row for row in bundle["origins"]}
    if len(origin_by_id) != EXPECTED_TOTAL_WITNESSES:
        raise TrueFullOutputRepairError("true full-output origins are not sixty unique rows")
    observed = {key: 0 for key in EXPECTED_WITNESS_COUNTS}
    raw_cases = []
    seen = set()
    for case in bundle["cases"]:
        density = str(case.get("density_stratum"))
        grouped = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
        for witness in case.get("witnesses") or []:
            legacy_id = str(witness.get("witness_id"))
            row = origin_by_id.get(legacy_id)
            if row is None or legacy_id in seen:
                raise TrueFullOutputRepairError("true full-output witness mapping drifted")
            seen.add(legacy_id)
            if (
                row.get("case_id") != case.get("case_id")
                or row.get("segment_id") != case.get("segment_id")
                or row.get("density_stratum") != density
            ):
                raise TrueFullOutputRepairError("true full-output case provenance drifted")
            if row.get("origin") == "reference":
                system_id = SYSTEM_BASELINE
            elif row.get("origin") == "candidate":
                system_id = SYSTEM_CANDIDATE
            else:
                raise TrueFullOutputRepairError("unknown true full-output origin")
            observed[(density, system_id)] += 1
            grouped[system_id].append(
                {
                    "event": deepcopy(witness.get("event")),
                    "provenance": {
                        "system_id": system_id,
                        "legacy_witness_id": legacy_id,
                        "event_sha256": row.get("event_sha256"),
                        "event_index": row.get("event_index"),
                    },
                }
            )
        raw_cases.append(
            {
                "case_key": str(case.get("case_id")),
                "source_excerpt": case.get("source_excerpt"),
                "event_set_a": grouped[SYSTEM_BASELINE],
                "event_set_b": grouped[SYSTEM_CANDIDATE],
                "provenance": {
                    "density_stratum": density,
                    "legacy_segment_id": case.get("segment_id"),
                },
            }
        )
    if seen != set(origin_by_id) or observed != EXPECTED_WITNESS_COUNTS:
        raise TrueFullOutputRepairError("true full-output 27+33 membership drifted")
    pool, mapping = judge.make_shared_witness_pool(raw_cases, seed=STEP_ID)
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise TrueFullOutputRepairError("generated true full-output pool is invalid")
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    if len(pool["cases"]) != 2 or witness_count != 60 or exact_count != 60:
        raise TrueFullOutputRepairError("true full-output exact-evidence invariant drifted")
    variants = judge.build_judge_variants(pool)
    if [row["case_id"] for row in variants["ab"]["cases"]] != [
        row["case_id"] for row in variants["ba"]["cases"]
    ]:
        raise TrueFullOutputRepairError("AB/BA case order drifted")
    return pool, mapping


def _runtime_files() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        Path(v1.__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(app_server_capacity.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        PINNED_CODEX,
    )


def _meaningful_root_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return {"root": root, "runtime_lock": lock_path}
    if root.exists() and _meaningful_root_entries(root):
        raise TrueFullOutputRepairError("unfrozen epoch-2 output root is not empty")
    contract = load_contract()
    pool, mapping = build_shared_pool(contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    variants = judge.build_judge_variants(pool)
    instructions = judge.judge_base_instructions()
    request_records = []
    request_sizes = {}
    for name in ("ab", "ba"):
        prompt = judge.build_judge_prompt(variants[name])
        schema = judge.semantic_judge_output_schema(variants[name])
        prompt_path = root / "requests" / f"prompt-{name}.private.md"
        schema_path = root / "requests" / f"schema-{name}.json"
        _write_immutable_text(prompt_path, prompt)
        _write_immutable_json(schema_path, schema)
        request_records.extend((_record(prompt_path), _record(schema_path)))
        request_sizes[name] = {
            "prompt_bytes": len(prompt.encode("utf-8")),
            "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        }
    instructions_path = root / "requests" / "base-instructions.private.md"
    _write_immutable_text(instructions_path, instructions)
    request_records.append(_record(instructions_path))
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
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "adjudication_call_cap": ADJUDICATION_CALL_CAP,
            "semantic_total_token_cap": TOTAL_TOKEN_CAP,
            "adjudication_token_reserve": contract["adjudication_token_reserve"],
            "extraction_model_call_cap": 0,
            "retry_count_per_turn": 0,
            "variant_order": ["ab", "ba"],
            "variant_case_order_and_opaque_ids_identical": True,
            "full_event_support": True,
            "claim_only_scoring": False,
            "reference_system_ids": None,
            "quality_threshold": QUALITY_THRESHOLD,
            "token_ratio_target": TOKEN_RATIO_TARGET,
            "production_amortized_total_token_ratio": contract["production_token_ratio"],
            "request_sizes": request_sizes,
            "managed_chatgpt_auth_only": True,
            "official_pinned_app_server_only": True,
            "production_mutation_allowed": False,
            "development_winner_freeze_allowed": False,
            "holdout_authorized": False,
        },
    )
    runtime_records = [_record(path) for path in _runtime_files()]
    source_records = [
        _record(PLAN_PATH),
        _record(DIRECTIVE_PATH),
        contract["predecessor_record"],
        *contract["frozen_records"],
    ]
    records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        *request_records,
        _record(spec_path),
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
        "request_records": request_records,
        "attempt_spec": _record(spec_path),
        "direct_record_digest": _record_digest(records),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "adjudication_call_cap": ADJUDICATION_CALL_CAP,
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_cap": 0,
        "capacity_checkpoint_records_excluded": True,
        "production_mutation_allowed": False,
        "development_winner_freeze_allowed": False,
        "holdout_authorized": False,
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
        lock = _load_json(lock_path, "epoch-2 runtime lock")
        records = [
            *(lock.get("runtime_files") or []),
            *(lock.get("source_records") or []),
            lock.get("pool") or {},
            lock.get("mapping") or {},
            *(lock.get("request_records") or []),
            lock.get("attempt_spec") or {},
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
            or lock.get("semantic_total_token_cap") != TOTAL_TOKEN_CAP
            or lock.get("semantic_retry_count") != 0
            or lock.get("extraction_model_call_cap") != 0
            or lock.get("capacity_checkpoint_records_excluded") is not True
            or lock.get("production_mutation_allowed") is not False
            or lock.get("development_winner_freeze_allowed") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(Path(str(row.get("path"))).name.startswith("capacity") for row in records)
            or any(not _verify_record(row) for row in records)
        ):
            raise TrueFullOutputRepairError("epoch-2 runtime lock or artifact drifted")
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


def _client_factory() -> CapacityGatedCodexAppServerClient:
    return CapacityGatedCodexAppServerClient(inner_factory=_inner_factory)


def _aggregate_usage(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    try:
        return v1._aggregate_usage(sidecar_paths)  # noqa: SLF001
    except v1.SharedReferenceRepairError as exc:
        raise OperationalWaitingError(str(exc)) from exc


def _preflight_adjudication(
    accounting: Mapping[str, Any], *, token_reserve: int
) -> int:
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    calls = accounting.get("measured_model_call_count")
    used = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if (
        accounting.get("accounting_complete") is not True
        or calls != 2
        or isinstance(used, bool)
        or not isinstance(used, int)
        or used < 0
        or isinstance(token_reserve, bool)
        or not isinstance(token_reserve, int)
        or token_reserve <= 0
    ):
        raise OperationalWaitingError("adjudication budget preflight lacks measured AB/BA accounting")
    remaining = TOTAL_TOKEN_CAP - used
    if remaining < token_reserve or calls + 1 > MODEL_CALL_CAP:
        raise OperationalWaitingError("remaining measured budget cannot cover the sole adjudication")
    return remaining


def _zero_accounting() -> dict[str, Any]:
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 0,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "turns": [],
    }


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

    def rate(num: int, den: int) -> float:
        return round(num / den, 6) if den else 0.0

    case_count = len(normalized.get("cases") or [])
    normalized["abstentions"] = {
        "support": {"numerator": support_abstain, "denominator": support_total, "rate": rate(support_abstain, support_total)},
        "alignment_labels": {"numerator": alignment_abstain, "denominator": alignment_total, "rate": rate(alignment_abstain, alignment_total)},
        "alignment_topology": {"numerator": topology_abstain, "denominator": case_count, "rate": rate(topology_abstain, case_count)},
        "equivalence_partition": {"numerator": partition_abstain, "denominator": case_count, "rate": rate(partition_abstain, case_count)},
    }
    normalized["selection_admissible"] = True
    normalized["abstention_gate_applied"] = False
    return normalized


def _build_adjudication_variant(pool: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    try:
        return v1._build_adjudication_variant(pool, case_ids)  # noqa: SLF001
    except v1.SharedReferenceRepairError as exc:
        raise TrueFullOutputRepairError(str(exc)) from exc


async def _run_adjudication(
    *,
    root: Path,
    pool: Mapping[str, Any],
    case_ids: Sequence[str],
    client_factory: Callable[[], Any],
    timeout_seconds: float,
) -> tuple[dict[str, Any], Path]:
    phase_root = root / "adjudication"
    if (phase_root / "sidecar.json").exists() or (phase_root / "output.private.json").exists():
        raise OperationalWaitingError("the sole adjudication already has an immutable attempt")
    variant = _build_adjudication_variant(pool, case_ids)
    instructions = judge.judge_base_instructions()
    prompt = (
        "This is the sole origin-neutral adjudication. All witnesses are pooled on one "
        "presentation side. Independently judge complete-event source support and the full "
        "semantic-equivalence partition. Do not vote between or reference prior outputs.\n\n"
        + judge.build_judge_prompt(variant)
    )
    schema = judge.semantic_judge_output_schema(variant)
    _write_immutable_json(phase_root / "variant.private.json", variant)
    _write_immutable_text(phase_root / "base-instructions.private.md", instructions)
    _write_immutable_text(phase_root / "prompt.private.md", prompt)
    _write_immutable_json(phase_root / "schema.json", schema)
    sidecar_path = phase_root / "sidecar.json"
    output_path = phase_root / "output.private.json"
    async with client_factory() as client:
        result = await client.run_ephemeral_structured_turn(
            model=MODEL,
            effort=EFFORT,
            base_instructions=instructions,
            prompt=prompt,
            output_schema=schema,
            cwd=PROJECT_ROOT,
            sidecar_path=sidecar_path,
            output_path=output_path,
            capacity_checkpoint_path=phase_root / "capacity.json",
            batch_size=len(case_ids),
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    if not result.status_ok or not isinstance(result.output, Mapping):
        raise OperationalWaitingError("the sole origin-neutral adjudication did not complete")
    output = _load_json(output_path, "adjudication output")
    if judge.validate_judge_output(output, variant):
        raise OperationalWaitingError("the sole origin-neutral adjudication output is invalid")
    _aggregate_usage([sidecar_path])
    return output, sidecar_path


def _merge_adjudication(
    consensus: Mapping[str, Any], output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    try:
        merged = v1._merge_adjudication(  # noqa: SLF001
            consensus=consensus,
            adjudication_output=output,
            adjudicated_case_ids=case_ids,
        )
    except v1.SharedReferenceRepairError as exc:
        raise TrueFullOutputRepairError(str(exc)) from exc
    return _normalized_consensus_metadata(merged)


def _witness_systems(mapping: Mapping[str, Any]) -> dict[str, str]:
    result = {}
    for case in mapping.get("cases") or []:
        case_provenance = case.get("case_provenance") or {}
        for witness in case.get("witnesses") or []:
            provenance = witness.get("provenance") or {}
            system_id = provenance.get("system_id") or case_provenance.get(
                f"system_{witness.get('canonical_side')}_id"
            )
            if system_id not in {SYSTEM_BASELINE, SYSTEM_CANDIDATE}:
                raise TrueFullOutputRepairError("private system membership drifted")
            witness_id = str(witness.get("witness_id"))
            if witness_id in result:
                raise TrueFullOutputRepairError("private witness membership is duplicated")
            result[witness_id] = str(system_id)
    return result


def _macro_scores(
    *,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    consensus: Mapping[str, Any],
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
            raise TrueFullOutputRepairError("cannot macro-score an abstained or unknown partition")
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
            raise TrueFullOutputRepairError("macro-score partition does not cover every witness")
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
            submitted_ids = {
                witness_id for witness_id in events if systems.get(witness_id) == system_id
            }
            submitted_units = {group_by_witness[item] for item in submitted_ids}
            supported_units = {
                group_by_witness[item]
                for item in submitted_ids
                if support[item] == "supported"
                and events[item].get("submitted_evidence_exact") is True
            }
            if submitted_units:
                precision = len(supported_units) / len(submitted_units)
            else:
                precision = 1.0 if not reference_units else 0.0
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
        raise TrueFullOutputRepairError("macro-score case coverage drifted")
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
    production_token_ratio: float,
    accounting: Mapping[str, Any],
    adjudication_call_count: int,
) -> dict[str, Any]:
    shared = judge.score_named_systems_against_shared_reference(
        pool=pool,
        private_mapping=mapping,
        consensus=consensus,
        reference_system_ids=None,
    )
    if shared.get("reference_system_ids") != [SYSTEM_BASELINE, SYSTEM_CANDIDATE]:
        raise TrueFullOutputRepairError("shared union did not symmetrically include both systems")
    macro = _macro_scores(pool=pool, mapping=mapping, consensus=consensus)
    baseline_macro = macro["systems"][SYSTEM_BASELINE]["strict_full_field_macro_f1"]
    candidate_macro = macro["systems"][SYSTEM_CANDIDATE]["strict_full_field_macro_f1"]
    witness_count = sum(len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"])
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
        or calls > MODEL_CALL_CAP
        or isinstance(total_tokens, bool)
        or not isinstance(total_tokens, int)
        or total_tokens > TOTAL_TOKEN_CAP
        or adjudication_call_count > ADJUDICATION_CALL_CAP
    ):
        raise OperationalWaitingError("judge accounting or semantic call cap is not admissible")
    checks = {
        "candidate_strict_full_field_macro_f1_gte_0_97": candidate_macro >= QUALITY_THRESHOLD,
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "production_amortized_total_token_ratio_lte_0_28": production_token_ratio <= TOKEN_RATIO_TARGET,
        "exact_evidence_rate_1": witness_count == EXPECTED_TOTAL_WITNESSES and exact_count == witness_count,
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
        "production_amortized_total_token_ratio": round(production_token_ratio, 6),
        "exact_evidence_rate": round(exact_count / witness_count, 6),
        "semantic_model_call_count": calls,
        "semantic_total_tokens": total_tokens,
        "adjudication_call_count": adjudication_call_count,
        "partial_alignment_is_diagnostic_only": True,
        "production_mutated": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "privacy": "sanitized metrics counts opaque ids and hashes only",
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
        raise TrueFullOutputRepairError("invalid epoch-2 receipt state")
    def optional(path: Path) -> dict[str, Any] | None:
        return _record(path) if path.is_file() else None

    records = {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "judge_report": optional(root / "judge" / "report.json"),
        "judge_output_ab": optional(root / "judge" / "output-ab.private.json"),
        "judge_output_ba": optional(root / "judge" / "output-ba.private.json"),
        "judge_sidecar_ab": optional(root / "judge" / "sidecars" / "ab.json"),
        "judge_sidecar_ba": optional(root / "judge" / "sidecars" / "ba.json"),
        "adjudication_output": optional(root / "adjudication" / "output.private.json"),
        "adjudication_sidecar": optional(root / "adjudication" / "sidecar.json"),
        "consensus": optional(root / "consensus.private.json"),
        "score": optional(root / "shared-reference-score.json"),
    }
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
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "development_quality_passed": bool(score and score.get("passed")),
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "records": records,
        "next_action": (
            "return_passed_step_to_epoch_2_supervisor_without_freezing_a_winner"
            if state == "passed"
            else "retain_true_full_output_architecture_as_quality_rejected"
            if state == "rejected"
            else "wait_for_an_explicit_versioned_operational_recovery"
        ),
        "privacy": "sanitized metrics counts hashes and failure class no source event prompt or output text",
    }
    if score is not None:
        payload.update(
            {
                "failed_checks": score.get("failed_checks"),
                "candidate_strict_full_field_macro_f1": score.get("candidate_strict_full_field_macro_f1"),
                "baseline_strict_full_field_macro_f1": score.get("baseline_strict_full_field_macro_f1"),
                "candidate_noninferiority_delta": score.get("candidate_noninferiority_delta"),
                "production_amortized_total_token_ratio": score.get("production_amortized_total_token_ratio"),
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


def verify_receipt(root: Path = DEFAULT_OUTPUT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        receipt = _load_json(root / "plan-step-receipt.json", "epoch-2 plan-step receipt")
        terminal = _load_json(root / "terminal.json", "epoch-2 terminal")
        if receipt != terminal:
            raise TrueFullOutputRepairError("epoch-2 terminal and receipt differ")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production_mutated") is not False
        ):
            raise TrueFullOutputRepairError("epoch-2 plan-step receipt drifted")
        records = receipt.get("records")
        if not isinstance(records, Mapping):
            raise TrueFullOutputRepairError("epoch-2 receipt records are absent")
        present = [row for row in records.values() if row is not None]
        if not present or any(not isinstance(row, Mapping) or not _verify_record(row) for row in present):
            raise TrueFullOutputRepairError("epoch-2 receipt artifact hash drifted")
        if receipt["state"] in {"passed", "rejected"} and (
            records.get("score") is None or records.get("consensus") is None or records.get("judge_report") is None
        ):
            raise TrueFullOutputRepairError("terminal quality receipt lacks scored evidence")
        return receipt

    if acquire_lock:
        with _advisory_process_lock(root):
            return verify()
    return verify()


def _best_effort_accounting(root: Path) -> dict[str, Any]:
    sidecars = sorted(root.glob("judge/sidecars/*.json")) + sorted(root.glob("adjudication/sidecar.json"))
    if not sidecars:
        return _zero_accounting()
    try:
        return _aggregate_usage(sidecars)
    except OperationalWaitingError:
        return {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": len(sidecars),
            "usage": None,
            "turns": [],
        }


async def _run_unlocked(
    *,
    root: Path,
    timeout_seconds: float,
    client_factory: Callable[[], Any],
    judge_runner: Callable[..., Any],
) -> dict[str, Any]:
    receipt_path = root / "plan-step-receipt.json"
    if receipt_path.is_file():
        return verify_receipt(root, acquire_lock=False)
    _freeze_unlocked(root)
    pool = _load_json(root / "shared-witness-pool.private.json", "true full-output pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "private mapping")
    score = None
    try:
        await judge_runner(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=MODEL,
            reasoning_effort=EFFORT,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
        )
        sidecars = [root / "judge" / "sidecars" / f"{name}.json" for name in ("ab", "ba")]
        accounting = _aggregate_usage(sidecars)
        if accounting["measured_model_call_count"] != 2:
            raise OperationalWaitingError("AB/BA measured call count drifted")
        outputs = {
            name: _load_json(root / "judge" / f"output-{name}.private.json", f"{name} output")
            for name in ("ab", "ba")
        }
        consensus = _load_json(root / "judge" / "consensus.private.json", "AB/BA consensus")
        try:
            disagreements = v1._observable_disagreement_case_ids(pool, outputs)  # noqa: SLF001
        except v1.SharedReferenceRepairError as exc:
            raise OperationalWaitingError(str(exc)) from exc
        adjudication_calls = 0
        if disagreements:
            reserve = load_contract()["adjudication_token_reserve"]
            _preflight_adjudication(accounting, token_reserve=reserve)
            adjudicated, sidecar = await _run_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreements,
                client_factory=client_factory,
                timeout_seconds=timeout_seconds,
            )
            sidecars.append(sidecar)
            accounting = _aggregate_usage(sidecars)
            adjudication_calls = 1
            consensus = _merge_adjudication(consensus, adjudicated, disagreements)
        else:
            consensus = _normalized_consensus_metadata(consensus)
        _write_immutable_json(root / "consensus.private.json", consensus)
        contract = load_contract()
        score = score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=consensus,
            production_token_ratio=contract["production_token_ratio"],
            accounting=accounting,
            adjudication_call_count=adjudication_calls,
        )
        _write_immutable_json(root / "shared-reference-score.json", score)
        state = "passed" if score["passed"] else "rejected"
        reason = "true_full_output_shared_reference_quality_passed" if score["passed"] else "true_full_output_shared_reference_quality_rejected"
        receipt = _receipt(root=root, state=state, terminal_reason=reason, accounting=accounting, score=score)
    except Exception as exc:
        accounting = _best_effort_accounting(root)
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason="true_full_output_operational_or_cap_waiting",
            accounting=accounting,
            score=None,
            error=exc,
        )
    _write_immutable_json(receipt_path, receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return verify_receipt(root, acquire_lock=False)


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = _client_factory,
    judge_runner: Callable[..., Any] = judge.run_app_server_semantic_judge,
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
    parser.add_argument("action", choices=("prepare", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "prepare":
        frozen = freeze_run(args.output_dir)
        result = {"state": "prepared", "runtime_lock": str(frozen["runtime_lock"])}
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds))
        result = {
            "state": receipt["state"],
            "terminal_reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["semantic_model_call_count"],
            "accounting_complete": receipt["accounting_complete"],
            "development_winner_frozen": receipt["development_winner_frozen"],
            "production_mutated": receipt["production_mutated"],
        }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
