from __future__ import annotations

"""Reserve-aware fresh successor for the immutable epoch-5 direct judge.

Epoch 5 froze the correct all-event AB/BA evaluation inputs but its legacy
capacity wrapper waits for an arbitrary ``usedPercent <= 20`` threshold.  This
module never resumes or writes into that attempt.  It binds the complete
pre-turn epoch-5 state, copies only the frozen private inputs into a new root,
and executes AB, BA, and (only when needed) one origin-neutral adjudication
through the audited remaining-reserve capacity gate.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
import math
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence

from . import app_server_candidate_expanded_cap_direct_reference as epoch5
from . import app_server_candidate_shared_reference_repair as shared_repair
from . import app_server_capacity_reserve as reserve
from . import app_server_interrupted_arm_recovery as capacity_probe
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_candidate_expanded_cap_direct_reference_v6"
LOCK_VERSION = "pif_candidate_expanded_cap_direct_reference_lock_v6"
PREFLIGHT_VERSION = "pif_candidate_expanded_cap_direct_reference_preflight_v6"
LAUNCH_VERSION = "pif_candidate_expanded_cap_direct_reference_launch_v6"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
PLAN_EPOCH = 6
STEP_ID = "expanded_cap_full_event_direct_reference_reserve_v6"
PHASE_ID = "epoch6_expanded_cap_full_event_direct_reference_reserve"
MODEL = epoch5.MODEL
EFFORT = epoch5.EFFORT
JUDGE_TRANSPORT = epoch5.JUDGE_TRANSPORT
TIMEOUT_SECONDS = epoch5.JUDGE_TIMEOUT_SECONDS
MODEL_CALL_CAP = 3
BASE_CALL_COUNT = 2
SEMANTIC_RETRY_COUNT = 0
MAXIMUM_TOTAL_TOKENS_PER_TURN = 102_000
PHASE_TOTAL_TOKEN_BOUND = MODEL_CALL_CAP * MAXIMUM_TOTAL_TOKENS_PER_TURN
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PROJECTED_PHASE_QUOTA_POINTS = math.ceil(
    PHASE_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
ADJUDICATION_TOTAL_TOKEN_CAP = epoch5.ADJUDICATION_MAX_TOTAL_TOKENS
TURN_NAMES = (
    "epoch6_direct_reference_ab",
    "epoch6_direct_reference_ba",
    "epoch6_direct_reference_adjudication",
)
BASE_VARIANTS = ("ab", "ba")
USAGE_FIELDS = epoch5.USAGE_FIELDS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
EPOCH5_ROOT = epoch5.DEFAULT_OUTPUT_ROOT.resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v249-expanded-cap-full-event-direct-reference-reserve-v6"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v6.json"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-expanded-cap-direct-reference-v6.json"
).resolve()
SOURCE_POLICY = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()
PINNED_CODEX = epoch5.PINNED_CODEX.resolve()
PROCESS_LOCK_NAME = ".expanded-cap-direct-reference-reserve-v6.lock"
PREDECESSOR_LOCK_NAME = epoch5.PROCESS_LOCK_NAME

EXPECTED_EPOCH5_PRETURN_FILES = {
    "judge/judge-spec.json",
    "judge/prompt-ab.private.md",
    "judge/prompt-ba.private.md",
    "judge/schema-ab.json",
    "judge/schema-ba.json",
}
EXPECTED_FROZEN_INPUT_ROLES = {
    "epoch5_attempt_spec",
    "epoch5_runtime_lock",
    "epoch5_launch_receipt",
    "epoch5_plan_step_receipt",
    "epoch5_terminal",
    "epoch5_pool",
    "epoch5_mapping",
    "epoch5_candidate_lineage",
    "epoch5_base_instructions",
    "epoch5_prompt_ab",
    "epoch5_schema_ab",
    "epoch5_prompt_ba",
    "epoch5_schema_ba",
    "epoch5_judge_spec",
    "epoch5_working_prompt_ab",
    "epoch5_working_schema_ab",
    "epoch5_working_prompt_ba",
    "epoch5_working_schema_ba",
    "audited_v20_capacity_policy",
}


class ExpandedCapDirectReferenceV6Error(RuntimeError):
    """The successor contract, lineage, or immutable evidence is invalid."""


class OperationalWaitingV6Error(ExpandedCapDirectReferenceV6Error):
    """A zero-retry successor attempt cannot safely continue."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve(strict=True).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve(strict=True)
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _verify_record(record: Any) -> bool:
    try:
        if not isinstance(record, Mapping):
            return False
        if set(record) - {"role", "path", "sha256", "size_bytes"}:
            return False
        frozen = {
            "path": record["path"],
            "sha256": record["sha256"],
            "size_bytes": record["size_bytes"],
        }
        return _record(Path(str(record["path"]))) == frozen
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpandedCapDirectReferenceV6Error(f"cannot read {label}") from exc


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    _write_immutable_bytes(path, payload.encode("utf-8"))


def _write_immutable_text(path: Path, value: str) -> None:
    _write_immutable_bytes(path, value.encode("utf-8"))


def _write_immutable_bytes(path: Path, value: bytes) -> None:
    target = path.expanduser().resolve()
    if target.exists():
        if not target.is_file() or target.read_bytes() != value:
            raise ExpandedCapDirectReferenceV6Error(
                f"immutable {target.name} drifted"
            )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("xb") as handle:
            handle.write(value)
            handle.flush()
    except FileExistsError as exc:
        raise ExpandedCapDirectReferenceV6Error(
            f"immutable {target.name} appeared concurrently"
        ) from exc


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        existing = _load_json(path, f"existing {path.name}")
        value[key] = existing.get(key)
    _write_immutable_json(path, value)


@contextmanager
def _exclusive_lock(path: Path, label: str) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingV6Error(f"another {label} process owns its lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _root_lock(root: Path) -> Iterator[None]:
    return _exclusive_lock(root / PROCESS_LOCK_NAME, "epoch-6 evaluator")


def _predecessor_lock() -> Iterator[None]:
    return _exclusive_lock(
        EPOCH5_ROOT / PREDECESSOR_LOCK_NAME, "epoch-5 predecessor"
    )


def _verify_epoch5_preturn(*, predecessor_lock_held: bool = False) -> dict[str, Any]:
    lock = epoch5.verify_runtime_lock(
        EPOCH5_ROOT / "runtime-lock.json",
        acquire_lock=not predecessor_lock_held,
    )
    launch = epoch5._validate_launch_receipt(EPOCH5_ROOT)  # noqa: SLF001
    terminal = epoch5.verify_receipt(
        EPOCH5_ROOT,
        acquire_lock=not predecessor_lock_held,
    )
    terminal_records = terminal.get("records") or {}
    if (
        terminal.get("state") != "waiting"
        or terminal.get("terminal_reason")
        != "epoch5_partial_or_interrupted_attempt_preserved_without_replay"
        or terminal.get("semantic_model_call_count") != 0
        or terminal.get("semantic_attempt_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage") is not None
        or terminal.get("development_quality_passed") is not False
        or terminal.get("winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or any(
            terminal_records.get(key) is not None
            for key in (
                "judge_output_ab",
                "judge_output_ba",
                "judge_sidecar_ab",
                "judge_sidecar_ba",
                "judge_capacity_ab",
                "judge_capacity_ba",
                "adjudication_output",
                "adjudication_sidecar",
                "adjudication_capacity",
                "score",
            )
        )
    ):
        raise ExpandedCapDirectReferenceV6Error(
            "epoch-5 terminal is not the exact zero-turn waiting predecessor"
        )
    actual = {
        str(path.relative_to(EPOCH5_ROOT))
        for phase in (EPOCH5_ROOT / "judge", EPOCH5_ROOT / "adjudication")
        if phase.exists()
        for path in phase.rglob("*")
        if path.is_file()
    }
    if actual != EXPECTED_EPOCH5_PRETURN_FILES:
        raise ExpandedCapDirectReferenceV6Error(
            "epoch-5 is not the exact immutable pre-turn attempt"
        )
    if list((EPOCH5_ROOT / "judge/sidecars").glob("*")):
        raise ExpandedCapDirectReferenceV6Error("epoch-5 contains a semantic checkpoint")
    return {
        "runtime_lock": lock,
        "launch_receipt": launch,
        "terminal": terminal,
    }


def load_contract(
    *, plan_path: Path = PLAN_PATH, directive_path: Optional[Path] = None
) -> dict[str, Any]:
    plan_file = plan_path.expanduser().resolve()
    plan = _load_json(plan_file, "epoch-6 semantic plan")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if not isinstance(step, Mapping):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 plan step is missing")
    directive_file = (
        directive_path.expanduser().resolve()
        if directive_path is not None
        else Path(str(step.get("directive_path") or "")).expanduser().resolve()
    )
    directive = _load_json(directive_file, "epoch-6 directive")
    execution = directive.get("execution_contract")
    capacity = directive.get("capacity_contract")
    predecessor = directive.get("predecessor_contract")
    witness = directive.get("witness_contract")
    acceptance = directive.get("acceptance_contract")
    terminal = directive.get("terminal_contract")
    if (
        set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
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
        or plan.get("thread_id") != epoch5.THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != MODEL_CALL_CAP
        or step.get("max_total_tokens") != PHASE_TOTAL_TOKEN_BOUND
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or Path(str(step.get("directive_path"))).expanduser().resolve()
        != directive_file
        or step.get("directive_sha256") != _sha256_file(directive_file)
        or directive.get("schema_version")
        != "pif_evaluation_expanded_cap_direct_reference_directive_v6"
        or directive.get("thread_id") != epoch5.THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or not isinstance(execution, Mapping)
        or execution.get("extraction_model_call_cap") != 0
        or execution.get("semantic_judge_call_cap") != MODEL_CALL_CAP
        or execution.get("required_ab_ba_call_count") != BASE_CALL_COUNT
        or execution.get("adjudication_call_cap") != 1
        or execution.get("semantic_retry_cap") != 0
        or execution.get("judge_model") != MODEL
        or execution.get("judge_reasoning_effort") != EFFORT
        or execution.get("judge_transport") != JUDGE_TRANSPORT
        or execution.get("ordered_turn_names") != list(TURN_NAMES)
        or execution.get("fresh_root_required") is not True
        or execution.get("all_events_direct_to_full_event_judge") is not True
        or execution.get("semantic_prefilter_allowed") is not False
        or execution.get("semantic_pruning_allowed") is not False
        or execution.get("support_or_alignment_prefilter_allowed") is not False
        or execution.get("reference_system_ids") is not None
        or execution.get("abstention_allowed") is not True
        or execution.get(
            "origin_neutral_adjudication_only_for_observable_disagreement"
        )
        is not True
        or (execution.get("production_cost_projection") or {}).get(
            "production_amortized_total_token_ratio"
        )
        != epoch5.EXPECTED_PRODUCTION_TOKEN_RATIO
        or (execution.get("production_cost_projection") or {}).get(
            "judge_tokens_included_in_production_formula"
        )
        is not False
        or not isinstance(capacity, Mapping)
        or capacity.get("policy_schema_version")
        != reserve.RESERVE_CAPACITY_POLICY_VERSION
        or capacity.get("source_policy_path") != str(SOURCE_POLICY)
        or capacity.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or capacity.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or capacity.get("maximum_total_tokens_per_turn")
        != MAXIMUM_TOTAL_TOKENS_PER_TURN
        or capacity.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
        or capacity.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or capacity.get("used_percent_threshold_allowed") is not False
        or capacity.get("no_turn_preflight_required") is not True
        or not isinstance(predecessor, Mapping)
        or predecessor.get("plan_epoch") != 5
        or predecessor.get("state") != "pre_turn_waiting_terminal_immutable"
        or predecessor.get("semantic_turn_count") != 0
        or predecessor.get("semantic_retry_allowed") is not False
        or predecessor.get("runtime_or_evidence_mutation_allowed") is not False
        or predecessor.get("predecessor_process_lock_must_be_held_by_successor")
        is not True
        or dict(witness or {}) != epoch5.EXPECTED_WITNESS_CONTRACT
        or dict(acceptance or {}) != epoch5.EXPECTED_ACCEPTANCE_CONTRACT
        or not isinstance(terminal, Mapping)
        or terminal.get("receipt_schema_version") != RECEIPT_VERSION
        or terminal.get("zero_retry") is not True
        or terminal.get("strict_thread_turn_call_accounting") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
    ):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 contract drifted")
    expected_receipt = Path(str(step.get("expected_receipt_path"))).expanduser().resolve()
    if expected_receipt != Path(
        str(directive.get("expected_receipt_path"))
    ).expanduser().resolve():
        raise ExpandedCapDirectReferenceV6Error("epoch-6 receipt path drifted")
    rows = directive.get("frozen_inputs")
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 frozen inputs are malformed")
    by_role = {str(row.get("role")): dict(row) for row in rows}
    if len(by_role) != len(rows) or set(by_role) != EXPECTED_FROZEN_INPUT_ROLES:
        raise ExpandedCapDirectReferenceV6Error("epoch-6 frozen input roles drifted")
    for role, row in by_role.items():
        frozen = {
            "path": row.get("path"),
            "sha256": row.get("sha256"),
            "size_bytes": row.get("size_bytes"),
        }
        if not _verify_record(frozen):
            raise ExpandedCapDirectReferenceV6Error(f"{role} checksum or size drifted")
    return {
        "plan": plan,
        "directive": directive,
        "plan_record": _record(plan_file),
        "directive_record": _record(directive_file),
        "frozen_records": by_role,
        "receipt_path": expected_receipt,
    }


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "base": turn_root / "base-instructions.private.md",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(epoch5.__file__).resolve(),
                Path(shared_repair.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(capacity_probe.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
                PINNED_CODEX,
            },
            key=str,
        )
    )


def _copy_immutable(source: Path, target: Path) -> None:
    _write_immutable_bytes(target, source.expanduser().resolve(strict=True).read_bytes())


def _build_capacity_policy(
    root: Path, *, source_policy_path: Path = SOURCE_POLICY
) -> Path:
    source_path = source_policy_path.expanduser().resolve()
    source = reserve.load_reserve_capacity_policy(source_path)
    if (
        source.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or source.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or source.get("maximum_total_tokens_per_turn")
        != MAXIMUM_TOTAL_TOKENS_PER_TURN
        or source.get("managed_chatgpt_auth_only") is not True
        or source.get("official_persistent_codex_app_server_only") is not True
        or source.get("retry_count_per_turn") != 0
        or source.get("production_mutation_allowed") is not False
        or source.get("unknown_usage_hard_stop") is not True
    ):
        raise ExpandedCapDirectReferenceV6Error(
            "audited v20 reserve policy constants drifted"
        )
    policy = {
        key: value
        for key, value in source.items()
        if key
        not in {
            "created_at",
            "phase_id",
            "phase_output_root",
            "semantic_output_root",
            "ordered_turn_names",
            "phase_total_token_bound",
            "projected_phase_quota_points",
            "authorization",
            "recovery",
        }
    }
    policy.update(
        {
            "schema_version": reserve.RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "phase_output_root": str(root),
            "semantic_output_root": str(root),
            "ordered_turn_names": list(TURN_NAMES),
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "recovery": {
                "schema_version": "pif_epoch6_capacity_successor_binding_v1",
                "source_policy": _record(source_path),
                "epoch5_runtime_lock": _record(EPOCH5_ROOT / "runtime-lock.json"),
                "epoch5_launch_receipt": _record(EPOCH5_ROOT / "launch-receipt.json"),
                "epoch5_plan_step_receipt": _record(
                    EPOCH5_ROOT / "plan-step-receipt.json"
                ),
                "epoch5_terminal": _record(EPOCH5_ROOT / "terminal.json"),
                "epoch5_retry_allowed": False,
                "epoch6_attempt_count": 1,
                "retry_count_per_turn": 0,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            },
        }
    )
    path = root / "capacity-policy.json"
    _write_stable_time(path, policy, "created_at")
    reserve.load_reserve_capacity_policy(path)
    return path


def _meaningful_root_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _source_request(role: str, contract: Mapping[str, Any]) -> Path:
    row = contract["frozen_records"][role]
    return Path(str(row["path"])).expanduser().resolve()


def _freeze_unlocked(
    root: Path,
    *,
    plan_path: Path,
    directive_path: Optional[Path],
    source_policy_path: Path,
) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_locks=False)
        return _load_frozen(root)
    if root.exists() and _meaningful_root_entries(root):
        raise ExpandedCapDirectReferenceV6Error("unfrozen epoch-6 root is not empty")
    contract = load_contract(plan_path=plan_path, directive_path=directive_path)
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise ExpandedCapDirectReferenceV6Error("output root differs from epoch-6 plan")
    _verify_epoch5_preturn(predecessor_lock_held=True)
    pool_source = _source_request("epoch5_pool", contract)
    mapping_source = _source_request("epoch5_mapping", contract)
    lineage_source = _source_request("epoch5_candidate_lineage", contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    lineage_path = root / "candidate-lineage.json"
    _copy_immutable(pool_source, pool_path)
    _copy_immutable(mapping_source, mapping_path)
    _copy_immutable(lineage_source, lineage_path)
    pool = _load_json(pool_path, "epoch-6 pool")
    variants = judge.build_judge_variants(pool)
    base_source = _source_request("epoch5_base_instructions", contract)
    turn_records: dict[str, dict[str, Any]] = {}
    for turn_name, variant_name in zip(TURN_NAMES[:2], BASE_VARIANTS):
        paths = _turn_paths(root, turn_name)
        _write_immutable_json(paths["input"], variants[variant_name])
        _copy_immutable(base_source, paths["base"])
        _copy_immutable(
            _source_request(f"epoch5_prompt_{variant_name}", contract),
            paths["prompt"],
        )
        _copy_immutable(
            _source_request(f"epoch5_schema_{variant_name}", contract),
            paths["schema"],
        )
        if (
            paths["prompt"].read_text(encoding="utf-8")
            != judge.build_judge_prompt(variants[variant_name])
            or _load_json(paths["schema"], f"epoch-6 {variant_name} schema")
            != judge.semantic_judge_output_schema(variants[variant_name])
            or paths["base"].read_text(encoding="utf-8")
            != judge.judge_base_instructions()
        ):
            raise ExpandedCapDirectReferenceV6Error(
                "copied epoch-5 request differs from reconstructed judge request"
            )
        turn_records[turn_name] = {
            name: _record(paths[name]) for name in ("input", "base", "prompt", "schema")
        }
    policy_path = _build_capacity_policy(root, source_policy_path=source_policy_path)
    attempt_path = root / "attempt-spec.json"
    _write_stable_time(
        attempt_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "thread_id": epoch5.THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "phase_id": PHASE_ID,
            "state": "frozen_before_no_turn_capacity_preflight",
            "model": MODEL,
            "effort": EFFORT,
            "judge_transport": JUDGE_TRANSPORT,
            "managed_chatgpt_auth_required": True,
            "official_persistent_app_server_required": True,
            "fresh_root": str(root),
            "ordered_turn_names": list(TURN_NAMES),
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "required_ab_ba_call_count": BASE_CALL_COUNT,
            "adjudication_call_cap": 1,
            "semantic_retry_count": 0,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "predecessor_epoch": 5,
            "predecessor_semantic_turn_count": 0,
            "predecessor_mutation_allowed": False,
            "extraction_model_call_count": 0,
            "winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
        "created_at",
    )
    runtime_records = [_record(path) for path in _runtime_files()]
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "thread_id": epoch5.THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "runtime_files": runtime_records,
        "plan": contract["plan_record"],
        "directive": contract["directive_record"],
        "frozen_inputs": list(contract["frozen_records"].values()),
        "source_policy": _record(source_policy_path.expanduser().resolve()),
        "capacity_policy": _record(policy_path),
        "attempt_spec": _record(attempt_path),
        "pool": _record(pool_path),
        "mapping": _record(mapping_path),
        "candidate_lineage": _record(lineage_path),
        "base_turn_requests": turn_records,
        "ordered_turn_names": list(TURN_NAMES),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "fresh_root_required": True,
        "predecessor_epoch5_immutable": True,
        "predecessor_semantic_turn_count": 0,
        "strict_thread_turn_call_accounting": True,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path, acquire_locks=False)
    return _load_frozen(root)


def freeze_run(
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    *,
    plan_path: Path = PLAN_PATH,
    directive_path: Optional[Path] = None,
    source_policy_path: Path = SOURCE_POLICY,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _predecessor_lock():
        with _root_lock(root):
            return _freeze_unlocked(
                root,
                plan_path=plan_path,
                directive_path=directive_path,
                source_policy_path=source_policy_path,
            )


def _request_record_paths(lock: Mapping[str, Any]) -> list[Path]:
    records = lock.get("base_turn_requests") or {}
    return [
        Path(str(record["path"]))
        for turn in TURN_NAMES[:2]
        for record in (records.get(turn) or {}).values()
        if isinstance(record, Mapping)
    ]


def verify_runtime_lock(
    path: Path, *, acquire_locks: bool = True
) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()
    root = lock_path.parent

    def verify() -> dict[str, Any]:
        lock = _load_json(lock_path, "epoch-6 runtime lock")
        if (
            not isinstance(lock, Mapping)
            or lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != epoch5.THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("phase_id") != PHASE_ID
            or lock.get("model") != MODEL
            or lock.get("effort") != EFFORT
            or lock.get("ordered_turn_names") != list(TURN_NAMES)
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("semantic_retry_count") != 0
            or lock.get("fresh_root_required") is not True
            or lock.get("predecessor_epoch5_immutable") is not True
            or lock.get("predecessor_semantic_turn_count") != 0
            or lock.get("strict_thread_turn_call_accounting") is not True
            or lock.get("winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutated") is not False
            or [str(Path(str(row.get("path"))).resolve()) for row in lock.get("runtime_files") or []]
            != [str(item) for item in _runtime_files()]
        ):
            raise ExpandedCapDirectReferenceV6Error("epoch-6 runtime lock drifted")
        direct_records = [
            *(lock.get("runtime_files") or []),
            lock.get("plan"),
            lock.get("directive"),
            *(lock.get("frozen_inputs") or []),
            lock.get("source_policy"),
            lock.get("capacity_policy"),
            lock.get("attempt_spec"),
            lock.get("pool"),
            lock.get("mapping"),
            lock.get("candidate_lineage"),
            *[
                row
                for turn in (lock.get("base_turn_requests") or {}).values()
                for row in (turn or {}).values()
            ],
        ]
        if any(not _verify_record(row) for row in direct_records):
            raise ExpandedCapDirectReferenceV6Error(
                "epoch-6 runtime lock record drifted"
            )
        plan_path = Path(str(lock["plan"]["path"]))
        directive_path = Path(str(lock["directive"]["path"]))
        contract = load_contract(plan_path=plan_path, directive_path=directive_path)
        if contract["receipt_path"] != root / "plan-step-receipt.json":
            raise ExpandedCapDirectReferenceV6Error("runtime root differs from plan")
        # ``verify`` is reached either under the locks acquired below or from
        # a caller that already owns both locks (the acquire_locks=False
        # internal contract).  Never recursively reacquire epoch 5's flock.
        _verify_epoch5_preturn(predecessor_lock_held=True)
        policy_path = Path(str(lock["capacity_policy"]["path"]))
        policy = reserve.load_reserve_capacity_policy(policy_path)
        recovery = policy.get("recovery")
        if (
            policy_path != root / "capacity-policy.json"
            or policy.get("phase_id") != PHASE_ID
            or policy.get("semantic_output_root") != str(root)
            or policy.get("phase_output_root") != str(root)
            or policy.get("ordered_turn_names") != list(TURN_NAMES)
            or policy.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or policy.get("quota_points_per_million_tokens")
            != QUOTA_POINTS_PER_MILLION_TOKENS
            or policy.get("maximum_total_tokens_per_turn")
            != MAXIMUM_TOTAL_TOKENS_PER_TURN
            or policy.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
            or policy.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
            or not isinstance(recovery, Mapping)
            or recovery.get("source_policy") != lock.get("source_policy")
            or recovery.get("epoch5_runtime_lock")
            != _record(EPOCH5_ROOT / "runtime-lock.json")
            or recovery.get("epoch5_launch_receipt")
            != _record(EPOCH5_ROOT / "launch-receipt.json")
            or recovery.get("epoch5_plan_step_receipt")
            != _record(EPOCH5_ROOT / "plan-step-receipt.json")
            or recovery.get("epoch5_terminal")
            != _record(EPOCH5_ROOT / "terminal.json")
            or recovery.get("epoch5_retry_allowed") is not False
            or recovery.get("epoch6_attempt_count") != 1
            or recovery.get("retry_count_per_turn") != 0
            or recovery.get("holdout_authorized") is not False
            or recovery.get("production_mutation_allowed") is not False
        ):
            raise ExpandedCapDirectReferenceV6Error("epoch-6 capacity policy drifted")
        for name, role in (
            ("shared-witness-pool.private.json", "epoch5_pool"),
            ("private-witness-mapping.private.json", "epoch5_mapping"),
            ("candidate-lineage.json", "epoch5_candidate_lineage"),
        ):
            if (root / name).read_bytes() != _source_request(role, contract).read_bytes():
                raise ExpandedCapDirectReferenceV6Error(
                    "epoch-6 copied predecessor evidence drifted"
                )
        pool = _load_json(root / "shared-witness-pool.private.json", "epoch-6 pool")
        variants = judge.build_judge_variants(pool)
        for turn_name, variant_name in zip(TURN_NAMES[:2], BASE_VARIANTS):
            paths = _turn_paths(root, turn_name)
            if (
                _load_json(paths["input"], f"{turn_name} input")
                != variants[variant_name]
                or paths["prompt"].read_text(encoding="utf-8")
                != judge.build_judge_prompt(variants[variant_name])
                or _load_json(paths["schema"], f"{turn_name} schema")
                != judge.semantic_judge_output_schema(variants[variant_name])
                or paths["base"].read_text(encoding="utf-8")
                != judge.judge_base_instructions()
            ):
                raise ExpandedCapDirectReferenceV6Error(
                    "epoch-6 frozen base request drifted"
                )
        return dict(lock)

    if not acquire_locks:
        return verify()
    with _predecessor_lock():
        with _root_lock(root):
            return verify()


def _load_frozen(root: Path) -> dict[str, Any]:
    return {
        "root": root,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "attempt_spec": root / "attempt-spec.json",
        "pool": _load_json(root / "shared-witness-pool.private.json", "epoch-6 pool"),
        "mapping": _load_json(
            root / "private-witness-mapping.private.json", "epoch-6 mapping"
        ),
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _verify_preflight(root: Path) -> dict[str, Any]:
    path = root / "capacity-preflight.json"
    payload = _load_json(path, "epoch-6 capacity preflight")
    lock = verify_runtime_lock(root / "runtime-lock.json", acquire_locks=False)
    snapshot = payload.get("live_capacity") if isinstance(payload, Mapping) else None
    evaluation = payload.get("reserve_evaluation") if isinstance(payload, Mapping) else None
    policy = reserve.load_reserve_capacity_policy(
        Path(str(lock["capacity_policy"]["path"]))
    )
    if not isinstance(snapshot, Mapping):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 preflight snapshot is malformed")
    recomputed = reserve.evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(TURN_NAMES),
    )
    if (
        payload.get("schema_version") != PREFLIGHT_VERSION
        or payload.get("thread_id") != epoch5.THREAD_ID
        or payload.get("plan_epoch") != PLAN_EPOCH
        or payload.get("step_id") != STEP_ID
        or payload.get("runtime_lock") != _record(root / "runtime-lock.json")
        or payload.get("capacity_policy") != _record(root / "capacity-policy.json")
        or snapshot.get("managed_chatgpt_auth_verified") is not True
        or snapshot.get("plan_type") != "pro"
        or snapshot.get("thread_started") is not False
        or snapshot.get("turn_started") is not False
        or snapshot.get("rate_limit_reached_type") is not None
        or evaluation != recomputed
        or recomputed.get("cleared_for_semantic_turn") is not True
        or payload.get("fresh_root_verified") is not True
        or payload.get("semantic_turn_count") != 0
        or payload.get("semantic_retry_count") != 0
        or payload.get("production_mutated") is not False
    ):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 capacity preflight drifted")
    return dict(payload)


async def preflight(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    rate_limit_probe: Callable[..., Any] = capacity_probe.probe_app_server_rate_limits,
    probe_client_factory: Callable[[], Any] = _inner_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if not (root / "runtime-lock.json").is_file():
        freeze_run(root)
    with _predecessor_lock():
        with _root_lock(root):
            verify_runtime_lock(root / "runtime-lock.json", acquire_locks=False)
            if (root / "capacity-preflight.json").is_file():
                return _verify_preflight(root)
            if any(
                (root / name).exists()
                for name in ("launch-receipt.json", "terminal.json", "plan-step-receipt.json")
            ) or _semantic_artifacts(root):
                raise ExpandedCapDirectReferenceV6Error(
                    "epoch-6 preflight requires a fresh zero-turn root"
                )
            snapshot = await rate_limit_probe(
                client_factory=probe_client_factory,
                maximum_primary_used_percent=100,
            )
            if (
                not isinstance(snapshot, Mapping)
                or snapshot.get("managed_chatgpt_auth_verified") is not True
                or snapshot.get("plan_type") != "pro"
                or snapshot.get("thread_started") is not False
                or snapshot.get("turn_started") is not False
            ):
                raise OperationalWaitingV6Error(
                    "no-turn preflight did not verify managed ChatGPT Pro auth"
                )
            policy = reserve.load_reserve_capacity_policy(root / "capacity-policy.json")
            evaluation = reserve.evaluate_reserve_capacity(
                snapshot,
                policy=policy,
                remaining_turn_count=len(TURN_NAMES),
            )
            if evaluation["cleared_for_semantic_turn"] is not True:
                raise OperationalWaitingV6Error(
                    "remaining capacity does not fit the audited epoch-6 phase bound"
                )
            payload = {
                "schema_version": PREFLIGHT_VERSION,
                "checked_at": now_iso(),
                "thread_id": epoch5.THREAD_ID,
                "plan_epoch": PLAN_EPOCH,
                "step_id": STEP_ID,
                "runtime_lock": _record(root / "runtime-lock.json"),
                "capacity_policy": _record(root / "capacity-policy.json"),
                "live_capacity": dict(snapshot),
                "reserve_evaluation": evaluation,
                "fresh_root_verified": True,
                "semantic_turn_count": 0,
                "semantic_retry_count": 0,
                "production_mutated": False,
            }
            _write_immutable_json(root / "capacity-preflight.json", payload)
            return _verify_preflight(root)


def _semantic_artifacts(root: Path) -> list[Path]:
    names = {"capacity.json", "sidecar.json", "output.private.json"}
    return sorted(
        path
        for path in (root / "turns").glob("*/**/*")
        if path.is_file() and path.name in names
    )


def _validate_reserve_checkpoint(
    path: Path, *, policy: Mapping[str, Any], turn_name: str
) -> dict[str, Any]:
    payload = _load_json(path, f"{turn_name} reserve capacity checkpoint")
    ordinal = TURN_NAMES.index(turn_name)
    snapshot = {
        "primary_used_percent": payload.get("primary_used_percent"),
        "rate_limit_reached_type": payload.get("rate_limit_reached_type"),
    }
    expected = reserve.evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(TURN_NAMES) - ordinal,
    )
    comparable = {
        key: payload.get(key)
        for key in (
            "primary_used_percent",
            "primary_remaining_percent",
            "minimum_remaining_reserve_percent",
            "usable_percent_above_reserve",
            "remaining_turn_count",
            "projected_remaining_tokens",
            "quota_points_per_million_tokens",
            "projected_remaining_quota_points",
            "projected_terminal_remaining_percent",
            "rate_limit_reached_type",
            "cleared_for_semantic_turn",
            "thread_started",
            "turn_started",
            "sidecar_started",
        )
    }
    if (
        payload.get("schema_version")
        != reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION
        or payload.get("policy_path") != str((path.parents[2] / "capacity-policy.json").resolve())
        or payload.get("policy_sha256") != _sha256_file(path.parents[2] / "capacity-policy.json")
        or payload.get("phase_id") != PHASE_ID
        or payload.get("turn_name") != turn_name
        or payload.get("turn_ordinal") != ordinal
        or payload.get("managed_chatgpt_auth_verified") is not True
        or payload.get("plan_type") != "pro"
        or payload.get("retry_checkpoint_reuse_allowed") is not False
        or comparable != {
            key: expected[key]
            for key in comparable
        }
        or expected.get("cleared_for_semantic_turn") is not True
    ):
        raise ExpandedCapDirectReferenceV6Error(
            f"{turn_name} reserve capacity checkpoint drifted"
        )
    return dict(payload)


def _validate_completed_turn(
    root: Path,
    *,
    turn_name: str,
    variant: Mapping[str, Any],
    batch_size: int,
) -> dict[str, Any]:
    paths = _turn_paths(root, turn_name)
    prompt = paths["prompt"].read_text(encoding="utf-8")
    schema = _load_json(paths["schema"], f"{turn_name} schema")
    base = paths["base"].read_text(encoding="utf-8")
    output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
        raw_output_path=paths["output"],
        sidecar_path=paths["sidecar"],
        prompt=prompt,
        schema=schema,
        base_instructions=base,
        model=MODEL,
        reasoning_effort=EFFORT,
    )
    usage = sidecar.get("usage")
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
        or not isinstance(sidecar.get("thread_id"), str)
        or not sidecar.get("thread_id")
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or not isinstance(usage, Mapping)
        or sidecar.get("thread_total_usage") != usage
    ):
        raise ExpandedCapDirectReferenceV6Error(
            f"{turn_name} sidecar transport or accounting drifted"
        )
    total = usage.get("total_tokens")
    if isinstance(total, bool) or not isinstance(total, int) or not 0 <= total <= MAXIMUM_TOTAL_TOKENS_PER_TURN:
        raise ExpandedCapDirectReferenceV6Error(f"{turn_name} usage exceeded its bound")
    if judge.validate_judge_output(output, variant):
        raise ExpandedCapDirectReferenceV6Error(f"{turn_name} output is invalid")
    policy = reserve.load_reserve_capacity_policy(root / "capacity-policy.json")
    capacity = _validate_reserve_checkpoint(
        paths["capacity"], policy=policy, turn_name=turn_name
    )
    return {"output": dict(output), "sidecar": dict(sidecar), "capacity": capacity}


async def _execute_turn(
    client: Any,
    *,
    root: Path,
    turn_name: str,
    variant: Mapping[str, Any],
    batch_size: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    paths = _turn_paths(root, turn_name)
    result = await client.run_ephemeral_structured_turn(
        model=MODEL,
        effort=EFFORT,
        base_instructions=paths["base"].read_text(encoding="utf-8"),
        prompt=paths["prompt"].read_text(encoding="utf-8"),
        output_schema=_load_json(paths["schema"], f"{turn_name} schema"),
        cwd=PROJECT_ROOT,
        sidecar_path=paths["sidecar"],
        output_path=paths["output"],
        capacity_checkpoint_path=paths["capacity"],
        batch_size=batch_size,
        thread_mode="new_thread",
        timeout_seconds=timeout_seconds,
    )
    if result.status_ok is not True or not isinstance(result.output, Mapping):
        raise OperationalWaitingV6Error(f"{turn_name} did not complete")
    validated = _validate_completed_turn(
        root, turn_name=turn_name, variant=variant, batch_size=batch_size
    )
    if _canonical_json(validated["output"]) != _canonical_json(result.output):
        raise ExpandedCapDirectReferenceV6Error(
            f"{turn_name} returned output differs from its checkpoint"
        )
    return validated


def _prepare_adjudication(
    root: Path, pool: Mapping[str, Any], case_ids: Sequence[str], base_usage: Mapping[str, Any]
) -> dict[str, Any]:
    turn_name = TURN_NAMES[2]
    paths = _turn_paths(root, turn_name)
    variant, instructions, prompt, schema = epoch5._adjudication_material(  # noqa: SLF001
        pool, case_ids
    )
    _write_immutable_json(paths["input"], variant)
    _write_immutable_text(paths["base"], instructions)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    receipt_path = paths["root"] / "adjudication-launch-receipt.json"
    _write_stable_time(
        receipt_path,
        {
            "schema_version": "pif_epoch6_adjudication_launch_v1",
            "launched_at": now_iso(),
            "thread_id": epoch5.THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "runtime_lock": _record(root / "runtime-lock.json"),
            "launch_receipt": _record(root / "launch-receipt.json"),
            "observable_disagreement_case_ids": list(case_ids),
            "observable_disagreement_case_count": len(case_ids),
            "base_usage": dict(base_usage),
            "model": MODEL,
            "effort": EFFORT,
            "semantic_retry_count": 0,
            "adjudication_total_token_cap": ADJUDICATION_TOTAL_TOKEN_CAP,
            "input": _record(paths["input"]),
            "base_instructions": _record(paths["base"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "holdout_authorized": False,
            "production_mutated": False,
        },
        "launched_at",
    )
    return {"variant": variant, "paths": paths, "launch": receipt_path}


def _aggregate_accounting(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    try:
        return epoch5._aggregate_usage(sidecar_paths)  # noqa: SLF001
    except epoch5.ExpandedCapDirectReferenceError as exc:
        raise ExpandedCapDirectReferenceV6Error(str(exc)) from exc


def _base_bundle(root: Path, pool: Mapping[str, Any]) -> dict[str, Any]:
    variants = judge.build_judge_variants(pool)
    turns = []
    for turn_name, variant_name in zip(TURN_NAMES[:2], BASE_VARIANTS):
        turns.append(
            _validate_completed_turn(
                root,
                turn_name=turn_name,
                variant=variants[variant_name],
                batch_size=len(pool["cases"]),
            )
        )
    thread_ids = [row["sidecar"]["thread_id"] for row in turns]
    turn_ids = [row["sidecar"]["turn_id"] for row in turns]
    if len(set(thread_ids)) != 2 or len(set(turn_ids)) != 2:
        raise ExpandedCapDirectReferenceV6Error(
            "epoch-6 AB/BA thread or turn IDs are not unique"
        )
    outputs = {name: row["output"] for name, row in zip(BASE_VARIANTS, turns)}
    consensus = judge.combine_judge_consensus(pool, outputs)
    accounting = _aggregate_accounting(
        [_turn_paths(root, name)["sidecar"] for name in TURN_NAMES[:2]]
    )
    return {
        "outputs": outputs,
        "consensus": consensus,
        "accounting": accounting,
        "thread_ids": thread_ids,
        "turn_ids": turn_ids,
    }


def _launch_receipt(root: Path) -> dict[str, Any]:
    payload = {
        "schema_version": LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": epoch5.THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "phase_id": PHASE_ID,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "capacity_preflight": _record(root / "capacity-preflight.json"),
        "fresh_root": str(root),
        "ordered_turn_names": list(TURN_NAMES),
        "model": MODEL,
        "effort": EFFORT,
        "judge_transport": JUDGE_TRANSPORT,
        "managed_chatgpt_auth_required": True,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "predecessor_epoch5_mutated": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_stable_time(root / "launch-receipt.json", payload, "launched_at")
    return payload


def _validate_launch(root: Path) -> dict[str, Any]:
    payload = _load_json(root / "launch-receipt.json", "epoch-6 launch receipt")
    if (
        payload.get("schema_version") != LAUNCH_VERSION
        or payload.get("thread_id") != epoch5.THREAD_ID
        or payload.get("plan_epoch") != PLAN_EPOCH
        or payload.get("step_id") != STEP_ID
        or payload.get("phase_id") != PHASE_ID
        or payload.get("runtime_lock") != _record(root / "runtime-lock.json")
        or payload.get("capacity_policy") != _record(root / "capacity-policy.json")
        or payload.get("capacity_preflight") != _record(root / "capacity-preflight.json")
        or payload.get("fresh_root") != str(root)
        or payload.get("ordered_turn_names") != list(TURN_NAMES)
        or payload.get("model") != MODEL
        or payload.get("effort") != EFFORT
        or payload.get("judge_transport") != JUDGE_TRANSPORT
        or payload.get("managed_chatgpt_auth_required") is not True
        or payload.get("semantic_model_call_cap") != MODEL_CALL_CAP
        or payload.get("semantic_retry_count") != 0
        or payload.get("predecessor_epoch5_mutated") is not False
        or payload.get("winner_frozen") is not False
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutated") is not False
    ):
        raise ExpandedCapDirectReferenceV6Error("epoch-6 launch receipt drifted")
    return dict(payload)


def _best_effort_accounting(root: Path) -> dict[str, Any]:
    complete_paths: list[Path] = []
    unknown = False
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        if not any(paths[key].exists() for key in ("capacity", "sidecar", "output")):
            continue
        if paths["capacity"].is_file() and paths["sidecar"].is_file() and paths["output"].is_file():
            try:
                variant = _load_json(paths["input"], f"{turn_name} input")
                _validate_completed_turn(
                    root,
                    turn_name=turn_name,
                    variant=variant,
                    batch_size=(
                        len(_load_json(root / "shared-witness-pool.private.json", "pool")["cases"])
                        if turn_name != TURN_NAMES[2]
                        else len(variant["cases"])
                    ),
                )
                complete_paths.append(paths["sidecar"])
                continue
            except Exception:
                pass
        unknown = True
    if unknown:
        return {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": None,
            "usage": None,
        }
    if not complete_paths:
        return {
            "usage_status": "complete",
            "accounting_complete": True,
            "measured_model_call_count": 0,
            "usage": {field: 0 for field in USAGE_FIELDS},
        }
    return _aggregate_accounting(complete_paths)


def _receipt_records(root: Path) -> dict[str, Any]:
    def optional(path: Path) -> Optional[dict[str, Any]]:
        return _record(path) if path.is_file() else None

    turns = {}
    for name in TURN_NAMES:
        paths = _turn_paths(root, name)
        turns[name] = {
            key: optional(paths[key])
            for key in ("input", "base", "prompt", "schema", "capacity", "sidecar", "output")
        }
        if name == TURN_NAMES[2]:
            turns[name]["launch_receipt"] = optional(
                paths["root"] / "adjudication-launch-receipt.json"
            )
    return {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "capacity_preflight": _record(root / "capacity-preflight.json"),
        "attempt_spec": _record(root / "attempt-spec.json"),
        "pool": _record(root / "shared-witness-pool.private.json"),
        "mapping": _record(root / "private-witness-mapping.private.json"),
        "candidate_lineage": _record(root / "candidate-lineage.json"),
        "launch_receipt": optional(root / "launch-receipt.json"),
        "turns": turns,
        "consensus": optional(root / "consensus.private.json"),
        "score": optional(root / "shared-reference-score.json"),
    }


def _receipt(
    *,
    root: Path,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    score: Optional[Mapping[str, Any]],
    error: Optional[BaseException] = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise ExpandedCapDirectReferenceV6Error("invalid epoch-6 receipt state")
    records = _receipt_records(root)
    payload: dict[str, Any] = {
        "schema_version": RECEIPT_VERSION,
        "thread_id": epoch5.THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_at": now_iso(),
        "terminal_reason": terminal_reason,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_model_call_count": accounting.get("measured_model_call_count"),
        "semantic_attempt_count": 1 if records["launch_receipt"] else 0,
        "semantic_total_token_cap": PHASE_TOTAL_TOKEN_BOUND,
        "semantic_retry_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "development_quality_passed": bool(score and score.get("passed")),
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout": False,
        "holdout_authorized": False,
        "production": False,
        "production_mutated": False,
        "predecessor_epoch5_mutated": False,
        "strict_thread_turn_call_accounting": True,
        "records": records,
        "next_action": (
            "independent_review_before_freezing_development_winner"
            if state == "passed"
            else "retain_expanded_cap_candidate_as_quality_rejected"
            if state == "rejected"
            else "audit_immutable_epoch6_attempt_and_create_no_retry_successor_if_needed"
        ),
        "privacy": "sanitized metrics counts hashes and failure class only",
    }
    if score is not None:
        for field in (
            "failed_checks",
            "candidate_strict_full_field_macro_f1",
            "baseline_strict_full_field_macro_f1",
            "candidate_noninferiority_delta",
            "exact_evidence_rate",
        ):
            payload[field] = score.get(field)
    if error is not None:
        message = str(error).encode("utf-8", errors="replace")
        payload.update(
            {
                "error_class": type(error).__name__,
                "error_message_sha256": hashlib.sha256(message).hexdigest(),
                "error_message_size_bytes": len(message),
            }
        )
    return payload


def _write_terminal(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _write_immutable_json(root / "plan-step-receipt.json", receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return verify_receipt(root, acquire_locks=False)


def _iter_records(value: Any) -> Iterator[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if set(value) == {"path", "sha256", "size_bytes"}:
            yield value
        else:
            for item in value.values():
                yield from _iter_records(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_records(item)


def _reconstruct_quality(root: Path) -> dict[str, Any]:
    pool = _load_json(root / "shared-witness-pool.private.json", "epoch-6 pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "epoch-6 mapping")
    base = _base_bundle(root, pool)
    disagreements = epoch5._observable_disagreement_case_ids(  # noqa: SLF001
        pool, base["outputs"]
    )
    sidecars = [_turn_paths(root, name)["sidecar"] for name in TURN_NAMES[:2]]
    if disagreements:
        turn_name = TURN_NAMES[2]
        paths = _turn_paths(root, turn_name)
        if not (paths["root"] / "adjudication-launch-receipt.json").is_file():
            raise ExpandedCapDirectReferenceV6Error(
                "observable disagreement lacks adjudication launch lineage"
            )
        variant = _load_json(paths["input"], "epoch-6 adjudication input")
        third = _validate_completed_turn(
            root,
            turn_name=turn_name,
            variant=variant,
            batch_size=len(disagreements),
        )
        if (
            third["sidecar"]["thread_id"] in set(base["thread_ids"])
            or third["sidecar"]["turn_id"] in set(base["turn_ids"])
        ):
            raise ExpandedCapDirectReferenceV6Error(
                "adjudication reused an AB/BA thread or turn ID"
            )
        third_total = third["sidecar"]["usage"]["total_tokens"]
        if third_total > ADJUDICATION_TOTAL_TOKEN_CAP:
            raise ExpandedCapDirectReferenceV6Error(
                "adjudication exceeded its stricter token bound"
            )
        consensus = epoch5._merge_adjudication(  # noqa: SLF001
            base["consensus"], third["output"], disagreements
        )
        sidecars.append(paths["sidecar"])
        adjudication_calls = 1
    else:
        third_root = _turn_paths(root, TURN_NAMES[2])["root"]
        if third_root.exists() and any(third_root.iterdir()):
            raise ExpandedCapDirectReferenceV6Error(
                "adjudication artifacts exist without observable disagreement"
            )
        consensus = epoch5._normalized_consensus_metadata(base["consensus"])  # noqa: SLF001
        adjudication_calls = 0
    stored_consensus = _load_json(root / "consensus.private.json", "epoch-6 consensus")
    if stored_consensus != consensus:
        raise ExpandedCapDirectReferenceV6Error("epoch-6 consensus drifted")
    accounting = _aggregate_accounting(sidecars)
    total = (accounting.get("usage") or {}).get("total_tokens")
    if (
        accounting.get("accounting_complete") is not True
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total > PHASE_TOTAL_TOKEN_BOUND
    ):
        raise ExpandedCapDirectReferenceV6Error(
            "epoch-6 aggregate accounting exceeded its phase bound"
        )
    score = epoch5.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        accounting=accounting,
        adjudication_call_count=adjudication_calls,
    )
    if _load_json(root / "shared-reference-score.json", "epoch-6 score") != score:
        raise ExpandedCapDirectReferenceV6Error("epoch-6 score does not recompute")
    return {"score": score, "accounting": accounting}


def verify_receipt(
    root: Path = DEFAULT_OUTPUT_ROOT, *, acquire_locks: bool = True
) -> dict[str, Any]:
    root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(root / "runtime-lock.json", acquire_locks=False)
        receipt = _load_json(root / "plan-step-receipt.json", "epoch-6 receipt")
        terminal = _load_json(root / "terminal.json", "epoch-6 terminal")
        if receipt != terminal:
            raise ExpandedCapDirectReferenceV6Error("epoch-6 terminal mirrors differ")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != epoch5.THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or receipt.get("semantic_total_token_cap") != PHASE_TOTAL_TOKEN_BOUND
            or receipt.get("semantic_retry_count") != 0
            or receipt.get("winner_frozen") is not False
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production") is not False
            or receipt.get("production_mutated") is not False
            or receipt.get("predecessor_epoch5_mutated") is not False
            or receipt.get("strict_thread_turn_call_accounting") is not True
        ):
            raise ExpandedCapDirectReferenceV6Error("epoch-6 receipt contract drifted")
        records = receipt.get("records")
        if not isinstance(records, Mapping):
            raise ExpandedCapDirectReferenceV6Error("epoch-6 receipt records are missing")
        record_rows = list(_iter_records(records))
        if not record_rows or any(not _verify_record(row) for row in record_rows):
            raise ExpandedCapDirectReferenceV6Error("epoch-6 receipt record drifted")
        if receipt.get("semantic_attempt_count") == 1:
            _validate_launch(root)
        elif receipt.get("semantic_attempt_count") != 0:
            raise ExpandedCapDirectReferenceV6Error("epoch-6 attempt count drifted")
        if receipt["state"] == "waiting":
            if (
                receipt.get("development_quality_passed") is not False
                or records.get("score") is not None
                or receipt.get("usage_status") not in {"complete", "unknown"}
                or (receipt.get("accounting_complete") is False and receipt.get("usage") is not None)
            ):
                raise ExpandedCapDirectReferenceV6Error(
                    "waiting epoch-6 receipt authorizes quality or false accounting"
                )
            return dict(receipt)
        quality = _reconstruct_quality(root)
        score = quality["score"]
        accounting = quality["accounting"]
        if (
            receipt.get("usage_status") != "complete"
            or receipt.get("accounting_complete") is not True
            or receipt.get("usage") != accounting["usage"]
            or receipt.get("semantic_model_call_count")
            != accounting["measured_model_call_count"]
            or (receipt["state"] == "passed") != (score.get("passed") is True)
            or (receipt["state"] == "rejected") != (score.get("passed") is False)
            or receipt.get("development_quality_passed") != (score.get("passed") is True)
        ):
            raise ExpandedCapDirectReferenceV6Error(
                "epoch-6 quality receipt or accounting drifted"
            )
        for field in (
            "failed_checks",
            "candidate_strict_full_field_macro_f1",
            "baseline_strict_full_field_macro_f1",
            "candidate_noninferiority_delta",
            "exact_evidence_rate",
        ):
            if receipt.get(field) != score.get(field):
                raise ExpandedCapDirectReferenceV6Error(
                    "epoch-6 receipt quality metric drifted"
                )
        return dict(receipt)

    if not acquire_locks:
        return verify()
    with _predecessor_lock():
        with _root_lock(root):
            return verify()


def _recover_terminal_mirror(root: Path) -> Optional[dict[str, Any]]:
    receipt_path = root / "plan-step-receipt.json"
    terminal_path = root / "terminal.json"
    if not receipt_path.exists() and not terminal_path.exists():
        return None
    if receipt_path.is_file() and terminal_path.is_file():
        return verify_receipt(root, acquire_locks=False)
    existing = receipt_path if receipt_path.is_file() else terminal_path
    missing = terminal_path if receipt_path.is_file() else receipt_path
    payload = _load_json(existing, "single epoch-6 terminal mirror")
    _write_immutable_json(missing, payload)
    return verify_receipt(root, acquire_locks=False)


async def _run_unlocked(
    *,
    root: Path,
    timeout_seconds: float,
    client_factory: Callable[[Path], Any],
) -> dict[str, Any]:
    recovered = _recover_terminal_mirror(root)
    if recovered is not None:
        return recovered
    frozen = _load_frozen(root)
    verify_runtime_lock(frozen["runtime_lock"], acquire_locks=False)
    _verify_preflight(root)
    if timeout_seconds != TIMEOUT_SECONDS:
        raise ExpandedCapDirectReferenceV6Error(
            "epoch-6 timeout differs from the frozen contract"
        )
    pool = frozen["pool"]
    mapping = frozen["mapping"]
    variants = judge.build_judge_variants(pool)
    launch_exists = (root / "launch-receipt.json").is_file()
    if launch_exists:
        _validate_launch(root)
    else:
        if _semantic_artifacts(root):
            raise ExpandedCapDirectReferenceV6Error(
                "epoch-6 semantic artifacts exist without a launch receipt"
            )
        _launch_receipt(root)
    try:
        base_complete = all(
            all(_turn_paths(root, name)[key].is_file() for key in ("capacity", "sidecar", "output"))
            for name in TURN_NAMES[:2]
        )
        if launch_exists and not base_complete:
            raise OperationalWaitingV6Error(
                "launched epoch-6 base attempt is incomplete; semantic replay is prohibited"
            )
        if base_complete:
            base = _base_bundle(root, pool)
        else:
            async with client_factory(frozen["capacity_policy"]) as client:
                for turn_name, variant_name in zip(TURN_NAMES[:2], BASE_VARIANTS):
                    await _execute_turn(
                        client,
                        root=root,
                        turn_name=turn_name,
                        variant=variants[variant_name],
                        batch_size=len(pool["cases"]),
                        timeout_seconds=timeout_seconds,
                    )
            base = _base_bundle(root, pool)
        disagreements = epoch5._observable_disagreement_case_ids(  # noqa: SLF001
            pool, base["outputs"]
        )
        sidecars = [_turn_paths(root, name)["sidecar"] for name in TURN_NAMES[:2]]
        if disagreements:
            paths = _turn_paths(root, TURN_NAMES[2])
            marker = paths["root"] / "adjudication-launch-receipt.json"
            complete = all(paths[key].is_file() for key in ("capacity", "sidecar", "output"))
            if marker.is_file() and not complete:
                raise OperationalWaitingV6Error(
                    "launched adjudication is incomplete; semantic replay is prohibited"
                )
            prepared = _prepare_adjudication(
                root, pool, disagreements, base["accounting"]["usage"]
            )
            if complete:
                third = _validate_completed_turn(
                    root,
                    turn_name=TURN_NAMES[2],
                    variant=prepared["variant"],
                    batch_size=len(disagreements),
                )
            else:
                async with client_factory(frozen["capacity_policy"]) as client:
                    third = await _execute_turn(
                        client,
                        root=root,
                        turn_name=TURN_NAMES[2],
                        variant=prepared["variant"],
                        batch_size=len(disagreements),
                        timeout_seconds=timeout_seconds,
                    )
            if (
                third["sidecar"]["thread_id"] in set(base["thread_ids"])
                or third["sidecar"]["turn_id"] in set(base["turn_ids"])
            ):
                raise ExpandedCapDirectReferenceV6Error(
                    "adjudication reused an AB/BA thread or turn ID"
                )
            if third["sidecar"]["usage"]["total_tokens"] > ADJUDICATION_TOTAL_TOKEN_CAP:
                raise OperationalWaitingV6Error(
                    "adjudication exceeded its stricter token cap"
                )
            consensus = epoch5._merge_adjudication(  # noqa: SLF001
                base["consensus"], third["output"], disagreements
            )
            sidecars.append(paths["sidecar"])
            adjudication_calls = 1
        else:
            third_root = _turn_paths(root, TURN_NAMES[2])["root"]
            if third_root.exists() and any(third_root.iterdir()):
                raise ExpandedCapDirectReferenceV6Error(
                    "adjudication artifacts exist without observable disagreement"
                )
            consensus = epoch5._normalized_consensus_metadata(base["consensus"])  # noqa: SLF001
            adjudication_calls = 0
        accounting = _aggregate_accounting(sidecars)
        if accounting["usage"]["total_tokens"] > PHASE_TOTAL_TOKEN_BOUND:
            raise OperationalWaitingV6Error("epoch-6 phase token bound was exceeded")
        _write_immutable_json(root / "consensus.private.json", consensus)
        score = epoch5.score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=consensus,
            accounting=accounting,
            adjudication_call_count=adjudication_calls,
        )
        _write_immutable_json(root / "shared-reference-score.json", score)
        state = "passed" if score["passed"] else "rejected"
        receipt = _receipt(
            root=root,
            state=state,
            terminal_reason=(
                "epoch6_reserve_aware_direct_reference_quality_passed"
                if state == "passed"
                else "epoch6_reserve_aware_direct_reference_quality_rejected"
            ),
            accounting=accounting,
            score=score,
        )
    except BaseException as exc:
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason="epoch6_reserve_aware_zero_retry_attempt_waiting",
            accounting=_best_effort_accounting(root),
            score=None,
            error=exc,
        )
    return _write_terminal(root, receipt)


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if not (root / "runtime-lock.json").is_file():
        freeze_run(root)
    with _predecessor_lock():
        with _root_lock(root):
            return await _run_unlocked(
                root=root,
                timeout_seconds=timeout_seconds,
                client_factory=client_factory,
            )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "preflight", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "prepare":
        frozen = freeze_run(args.output_dir)
        result = {
            "state": "prepared",
            "root": str(frozen["root"]),
            "runtime_lock": str(frozen["runtime_lock"]),
            "semantic_turn_count": 0,
        }
    elif args.action == "preflight":
        receipt = asyncio.run(preflight(output_dir=args.output_dir))
        result = {
            "state": "cleared",
            "primary_used_percent": receipt["live_capacity"]["primary_used_percent"],
            "projected_terminal_remaining_percent": receipt["reserve_evaluation"][
                "projected_terminal_remaining_percent"
            ],
            "semantic_turn_count": 0,
        }
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(
            run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": receipt["state"],
            "terminal_reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt.get("semantic_model_call_count"),
            "usage_status": receipt.get("usage_status"),
        }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
