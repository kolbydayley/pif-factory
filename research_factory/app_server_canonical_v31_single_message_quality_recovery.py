from __future__ import annotations

"""One-call origin-neutral recovery for the immutable epoch-20 judge failure."""

import argparse
import asyncio
import fcntl
import hashlib
import json
import math
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_candidate_shared_reference_repair as shared_repair
from . import app_server_capacity_reserve as reserve
from . import app_server_canonical_v31_single_message_direct_reference as epoch20
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_canonical_v31_single_message_quality_recovery_v21"
LOCK_VERSION = "pif_canonical_v31_single_message_quality_recovery_lock_v1"
LAUNCH_VERSION = "pif_canonical_v31_single_message_quality_recovery_launch_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = epoch20.THREAD_ID
PLAN_EPOCH = 21
STEP_ID = "canonical_v31_epoch21_single_message_quality_recovery_v21"
MODEL = epoch20.MODEL
EFFORT = epoch20.EFFORT
TRANSPORT = epoch20.JUDGE_TRANSPORT
TIMEOUT_SECONDS = epoch20.JUDGE_TIMEOUT_SECONDS
NEW_MODEL_CALL_CAP = 1
PREDECESSOR_MODEL_CALL_COUNT = 2
AGGREGATE_MODEL_CALL_CAP = 3
NEW_TOTAL_TOKEN_CAP = 150_000
AGGREGATE_TOTAL_TOKEN_CAP = 400_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PROJECTED_PHASE_QUOTA_POINTS = math.ceil(
    NEW_TOTAL_TOKEN_CAP * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
TURN_NAME = "epoch21_origin_neutral_quality_recovery"
EXPECTED_PREDECESSOR_USAGE = {
    "input_tokens": 192_312,
    "cached_input_tokens": 2_816,
    "output_tokens": 51_669,
    "reasoning_output_tokens": 6_579,
    "total_tokens": 243_981,
}
EXPECTED_AB_VALIDATION_ERRORS = (
    "case_0_support_0_evidence_not_exact",
    "case_5_alignment_0_invalid_or_duplicate_left",
)
EXPECTED_ERROR_MESSAGE_SHA256 = (
    "65d027b1c972b05fb60913ad11f748b5da08367004cd94ee5db0752b924332d4"
)
EXPECTED_PRODUCTION_TOKEN_RATIO = epoch20.EXPECTED_PRODUCTION_TOKEN_RATIO
USAGE_FIELDS = epoch20.USAGE_FIELDS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
EPOCH20_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch20-single-message-direct-reference-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch21-single-message-quality-recovery-v21.json"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v21.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch21-single-message-quality-recovery-v1"
).resolve()
PINNED_CODEX = epoch20.PINNED_CODEX
PROCESS_LOCK_NAME = ".canonical-v31-single-message-quality-recovery.lock"

EXPECTED_FROZEN_PATHS = {
    "epoch20_terminal": EPOCH20_ROOT / "terminal.json",
    "epoch20_plan_step_receipt": EPOCH20_ROOT / "plan-step-receipt.json",
    "epoch20_runtime_lock": EPOCH20_ROOT / "runtime-lock.json",
    "epoch20_launch_receipt": EPOCH20_ROOT / "launch-receipt.json",
    "shared_witness_pool": EPOCH20_ROOT / "shared-witness-pool.private.json",
    "private_witness_mapping": EPOCH20_ROOT / "private-witness-mapping.private.json",
    "candidate_lineage": EPOCH20_ROOT / "candidate-lineage.json",
    "judge_spec": EPOCH20_ROOT / "judge/judge-spec.json",
    "judge_ab_prompt": EPOCH20_ROOT / "judge/prompt-ab.private.md",
    "judge_ab_schema": EPOCH20_ROOT / "judge/schema-ab.json",
    "judge_ab_output": EPOCH20_ROOT / "judge/output-ab.private.json",
    "judge_ab_sidecar": EPOCH20_ROOT / "judge/sidecars/ab.json",
    "judge_ab_capacity": EPOCH20_ROOT / "judge/sidecars/ab.capacity.json",
    "judge_ba_prompt": EPOCH20_ROOT / "judge/prompt-ba.private.md",
    "judge_ba_schema": EPOCH20_ROOT / "judge/schema-ba.json",
    "judge_ba_output": EPOCH20_ROOT / "judge/output-ba.private.json",
    "judge_ba_sidecar": EPOCH20_ROOT / "judge/sidecars/ba.json",
    "judge_ba_capacity": EPOCH20_ROOT / "judge/sidecars/ba.capacity.json",
    "epoch20_adapter": Path(epoch20.__file__).resolve(),
}

EXPECTED_EXECUTION_CONTRACT = {
    "extraction_model_call_cap": 0,
    "predecessor_judge_model_call_count": PREDECESSOR_MODEL_CALL_COUNT,
    "new_judge_model_call_cap": NEW_MODEL_CALL_CAP,
    "aggregate_judge_model_call_cap": AGGREGATE_MODEL_CALL_CAP,
    "semantic_retry_cap": 0,
    "judge_model": MODEL,
    "judge_reasoning_effort": EFFORT,
    "judge_transport": TRANSPORT,
    "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
    "aggregate_total_token_cap": AGGREGATE_TOTAL_TOKEN_CAP,
    "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
    "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
    "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
    "origin_neutral_all_case_adjudication": True,
    "prior_ab_ba_replay_allowed": False,
    "prior_outputs_exposed_to_recovery_model": False,
    "semantic_prefilter_allowed": False,
    "semantic_pruning_allowed": False,
}
EXPECTED_WITNESS_CONTRACT = {
    "source_segment_order": list(epoch20.SOURCE_SEGMENT_ORDER),
    "baseline_total_event_count": epoch20.EXPECTED_BASELINE_WITNESSES,
    "candidate_total_event_count": epoch20.EXPECTED_CANDIDATE_WITNESSES,
    "shared_witness_total": epoch20.EXPECTED_TOTAL_WITNESSES,
    "all_six_cases_rejudged_once": True,
    "opaque_witness_ids_required": True,
    "one_shared_augmented_reference_required": True,
}
EXPECTED_ACCEPTANCE_CONTRACT = {
    "candidate_strict_full_field_macro_f1_min": epoch20.QUALITY_THRESHOLD,
    "candidate_must_be_noninferior_to_baseline": True,
    "production_amortized_total_token_ratio_max": epoch20.TOKEN_RATIO_TARGET,
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

_HASH_CACHE: dict[tuple[str, int, int, int, int], str] = {}


class QualityRecoveryError(RuntimeError):
    """The epoch-21 recovery contract or artifact lineage is invalid."""


class OperationalWaitingError(QualityRecoveryError):
    """The bounded recovery must stop without replay or quality authorization."""


class PartialAttemptWaitingError(OperationalWaitingError):
    """A partial immutable recovery attempt cannot be replayed."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualityRecoveryError(f"cannot read {label}") from exc


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


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise QualityRecoveryError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise QualityRecoveryError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_text(_canonical_json(list(records)))


@contextmanager
def _process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / PROCESS_LOCK_NAME).open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another epoch-21 recovery owns the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _validated_record(row: Mapping[str, Any], label: str) -> dict[str, Any]:
    frozen = {
        "path": row.get("path") if isinstance(row, Mapping) else None,
        "sha256": row.get("sha256") if isinstance(row, Mapping) else None,
        "size_bytes": row.get("size_bytes") if isinstance(row, Mapping) else None,
    }
    if not isinstance(row, Mapping) or not _verify_record(frozen):
        raise QualityRecoveryError(f"{label} checksum or size drifted")
    return _record(Path(str(frozen["path"])))


def _validate_predecessor_turn(
    *, root: Path, pool: Mapping[str, Any], name: str
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    variants = judge.build_judge_variants(pool)
    variant = variants[name]
    prompt = judge.build_judge_prompt(variant)
    schema = judge.semantic_judge_output_schema(variant)
    base = judge.judge_base_instructions()
    prompt_path = root / f"judge/prompt-{name}.private.md"
    schema_path = root / f"judge/schema-{name}.json"
    output_path = root / f"judge/output-{name}.private.json"
    sidecar_path = root / f"judge/sidecars/{name}.json"
    capacity_path = root / f"judge/sidecars/{name}.capacity.json"
    if (
        prompt_path.read_text(encoding="utf-8") != prompt
        or _load_json(schema_path, f"predecessor {name} schema") != schema
        or (root / "judge/base-instructions.private.md").read_text(encoding="utf-8")
        != base
    ):
        raise QualityRecoveryError(f"predecessor {name} request drifted")
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
        usage = shared_repair._sidecar_usage(sidecar)  # noqa: SLF001
        epoch20._validate_capacity_checkpoint(capacity_path)  # noqa: SLF001
    except (
        judge.JudgeArtifactError,
        shared_repair.SharedReferenceRepairError,
    ) as exc:
        raise QualityRecoveryError(f"predecessor {name} checkpoint drifted") from exc
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
        or sidecar.get("batch_size") != len(pool["cases"])
        or not isinstance(sidecar.get("thread_id"), str)
        or not sidecar.get("thread_id")
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("usage") != usage
        or sidecar.get("thread_total_usage") != usage
    ):
        raise QualityRecoveryError(f"predecessor {name} sidecar contract drifted")
    return dict(output), dict(sidecar), judge.validate_judge_output(output, variant)


def load_contract(
    *, plan_path: Path | None = None, directive_path: Path | None = None
) -> dict[str, Any]:
    resolved_plan = (plan_path or PLAN_PATH).expanduser().resolve()
    plan = _load_json(resolved_plan, "epoch-21 semantic plan")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if not isinstance(step, Mapping):
        raise QualityRecoveryError("epoch-21 plan step is missing")
    resolved_directive = (
        directive_path.expanduser().resolve()
        if directive_path is not None
        else Path(str(step.get("directive_path") or DIRECTIVE_PATH)).expanduser().resolve()
    )
    directive = _load_json(resolved_directive, "epoch-21 directive")
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
        or step.get("max_model_calls") != NEW_MODEL_CALL_CAP
        or step.get("max_total_tokens") != NEW_TOTAL_TOKEN_CAP
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or Path(str(step.get("directive_path"))).expanduser().resolve()
        != resolved_directive
        or step.get("directive_sha256") != _sha256_file(resolved_directive)
        or directive.get("schema_version")
        != "pif_evaluation_epoch21_single_message_quality_recovery_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authorized_by") != "kolby"
        or directive.get("authority") != "direct_user_instruction"
        or directive.get("execution_contract") != EXPECTED_EXECUTION_CONTRACT
        or directive.get("witness_contract") != EXPECTED_WITNESS_CONTRACT
        or directive.get("acceptance_contract") != EXPECTED_ACCEPTANCE_CONTRACT
        or directive.get("terminal_contract") != EXPECTED_TERMINAL_CONTRACT
    ):
        raise QualityRecoveryError("epoch-21 quality-recovery contract drifted")
    receipt_path = Path(str(step.get("expected_receipt_path"))).expanduser().resolve()
    if (
        receipt_path != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json"
        or receipt_path
        != Path(str(directive.get("expected_receipt_path"))).expanduser().resolve()
    ):
        raise QualityRecoveryError("epoch-21 receipt path drifted")
    rows = directive.get("frozen_inputs")
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise QualityRecoveryError("epoch-21 frozen inputs are malformed")
    by_role = {str(row.get("role")): dict(row) for row in rows}
    if len(by_role) != len(rows) or set(by_role) != set(EXPECTED_FROZEN_PATHS):
        raise QualityRecoveryError("epoch-21 frozen input roles drifted")
    frozen_records = {}
    for role, expected_path in EXPECTED_FROZEN_PATHS.items():
        row = by_role[role]
        if Path(str(row.get("path"))).expanduser().resolve() != expected_path:
            raise QualityRecoveryError(f"{role} path drifted")
        frozen_records[role] = _validated_record(row, role)
    terminal = _load_json(Path(frozen_records["epoch20_terminal"]["path"]), "epoch-20 terminal")
    mirror = _load_json(
        Path(frozen_records["epoch20_plan_step_receipt"]["path"]),
        "epoch-20 receipt",
    )
    if terminal != mirror:
        raise QualityRecoveryError("epoch-20 terminal mirrors differ")
    try:
        epoch20.verify_receipt(EPOCH20_ROOT)
    except epoch20.ExpandedCapDirectReferenceError as exc:
        raise QualityRecoveryError("epoch-20 receipt verification failed") from exc
    if (
        terminal.get("state") != "waiting"
        or terminal.get("terminal_reason")
        != "epoch20_direct_reference_operational_or_capacity_waiting"
        or terminal.get("error_class") != "JudgeArtifactError"
        or terminal.get("error_message_sha256") != EXPECTED_ERROR_MESSAGE_SHA256
        or terminal.get("error_message_size_bytes") != 46
        or terminal.get("semantic_model_call_count") != PREDECESSOR_MODEL_CALL_COUNT
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != EXPECTED_PREDECESSOR_USAGE
        or terminal.get("winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
    ):
        raise QualityRecoveryError("epoch-20 waiting checkpoint drifted")
    pool = _load_json(Path(frozen_records["shared_witness_pool"]["path"]), "shared pool")
    mapping = _load_json(
        Path(frozen_records["private_witness_mapping"]["path"]), "private mapping"
    )
    if (
        judge.validate_shared_witness_pool(pool)
        or len(pool.get("cases") or []) != len(epoch20.SOURCE_SEGMENT_ORDER)
        or sum(
            len(case["event_set_a"]) + len(case["event_set_b"])
            for case in pool["cases"]
        )
        != epoch20.EXPECTED_TOTAL_WITNESSES
    ):
        raise QualityRecoveryError("epoch-20 shared pool drifted")
    ab_output, ab_sidecar, ab_errors = _validate_predecessor_turn(
        root=EPOCH20_ROOT, pool=pool, name="ab"
    )
    ba_output, ba_sidecar, ba_errors = _validate_predecessor_turn(
        root=EPOCH20_ROOT, pool=pool, name="ba"
    )
    if tuple(ab_errors) != EXPECTED_AB_VALIDATION_ERRORS or ba_errors != []:
        raise QualityRecoveryError("epoch-20 bounded validation defect drifted")
    if (
        ab_sidecar["thread_id"] == ba_sidecar["thread_id"]
        or ab_sidecar["turn_id"] == ba_sidecar["turn_id"]
    ):
        raise QualityRecoveryError("epoch-20 AB/BA identities are not unique")
    accounting = epoch20._aggregate_usage(  # noqa: SLF001
        [EPOCH20_ROOT / "judge/sidecars/ab.json", EPOCH20_ROOT / "judge/sidecars/ba.json"]
    )
    if (
        accounting.get("measured_model_call_count") != PREDECESSOR_MODEL_CALL_COUNT
        or accounting.get("usage") != EXPECTED_PREDECESSOR_USAGE
    ):
        raise QualityRecoveryError("epoch-20 measured accounting drifted")
    return {
        "plan": plan,
        "directive": directive,
        "plan_record": _record(resolved_plan),
        "directive_record": _record(resolved_directive),
        "frozen_records": frozen_records,
        "receipt_path": receipt_path,
        "pool": pool,
        "mapping": mapping,
        "predecessor_outputs": {"ab": ab_output, "ba": ba_output},
        "predecessor_sidecars": {"ab": ab_sidecar, "ba": ba_sidecar},
        "predecessor_accounting": accounting,
    }


def _adjudication_material(pool: Mapping[str, Any]) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    case_ids = [str(case["case_id"]) for case in pool["cases"]]
    try:
        variant = shared_repair._build_adjudication_variant(pool, case_ids)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise QualityRecoveryError("origin-neutral variant construction failed") from exc
    base = judge.judge_base_instructions()
    prompt = (
        "This is one independently frozen, origin-neutral full-pool recovery after a prior "
        "permutation output failed exact structural validation. Prior judge outputs are not "
        "shown and must not be inferred. Independently decide source support and the complete "
        "full-event semantic-equivalence partition for every opaque witness in every case. "
        "Use only verbatim source substrings for evidence spans. All witnesses are pooled on "
        "one presentation side; alignment topology is diagnostic, while the full equivalence "
        "partition must cover every witness exactly once. Abstain when source evidence cannot "
        "resolve support. The measured total-token acceptance ceiling is 150000.\n\n"
        + judge.build_judge_prompt(variant)
    )
    schema = judge.semantic_judge_output_schema(variant)
    return variant, base, prompt, schema


def _runtime_files() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        Path(epoch20.__file__).resolve(),
        Path(shared_repair.__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(reserve.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
        PINNED_CODEX.resolve(),
    )


def _build_capacity_policy(root: Path) -> Path:
    predecessor = reserve.load_reserve_capacity_policy(EPOCH20_ROOT / "capacity-policy.json")
    policy = dict(predecessor)
    policy.update(
        {
            "schema_version": reserve.RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": STEP_ID,
            "semantic_output_root": str(root.resolve()),
            "ordered_turn_names": [TURN_NAME],
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


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "variant": turn_root / "variant.private.json",
        "base": turn_root / "base-instructions.private.md",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return {"root": root, "runtime_lock": lock_path}
    if root.exists() and any(path.name != PROCESS_LOCK_NAME for path in root.iterdir()):
        raise QualityRecoveryError("unfrozen epoch-21 root is not empty")
    contract = load_contract()
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise QualityRecoveryError("output root differs from epoch-21 contract")
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    _write_immutable_json(pool_path, contract["pool"])
    _write_immutable_json(mapping_path, contract["mapping"])
    paths = _turn_paths(root)
    variant, base, prompt, schema = _adjudication_material(contract["pool"])
    _write_immutable_json(paths["variant"], variant)
    _write_immutable_text(paths["base"], base)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    capacity_policy = _build_capacity_policy(root)
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
            "judge_transport": TRANSPORT,
            "managed_chatgpt_auth_required": True,
            "official_persistent_app_server_required": True,
            "predecessor_model_call_count": PREDECESSOR_MODEL_CALL_COUNT,
            "new_model_call_cap": NEW_MODEL_CALL_CAP,
            "aggregate_model_call_cap": AGGREGATE_MODEL_CALL_CAP,
            "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
            "aggregate_total_token_cap": AGGREGATE_TOTAL_TOKEN_CAP,
            "semantic_retry_count": 0,
            "case_ids": [case["case_id"] for case in contract["pool"]["cases"]],
            "all_event_witness_count": epoch20.EXPECTED_TOTAL_WITNESSES,
            "prior_outputs_exposed_to_recovery_model": False,
            "origin_neutral_all_case_adjudication": True,
            "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
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
    direct_records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        *[_record(paths[key]) for key in ("variant", "base", "prompt", "schema")],
        _record(capacity_policy),
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
        "variant": _record(paths["variant"]),
        "base_instructions": _record(paths["base"]),
        "prompt": _record(paths["prompt"]),
        "schema": _record(paths["schema"]),
        "capacity_policy": _record(capacity_policy),
        "attempt_spec": _record(spec_path),
        "direct_record_digest": _record_digest(direct_records),
        "predecessor_model_call_count": PREDECESSOR_MODEL_CALL_COUNT,
        "new_model_call_cap": NEW_MODEL_CALL_CAP,
        "aggregate_model_call_cap": AGGREGATE_MODEL_CALL_CAP,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "aggregate_total_token_cap": AGGREGATE_TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_immutable_json(lock_path, lock)
    verify_runtime_lock(lock_path, acquire_lock=False)
    return {"root": root, "runtime_lock": lock_path}


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _process_lock(root):
        return _freeze_unlocked(root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()

    def verify() -> dict[str, Any]:
        lock = _load_json(lock_path, "epoch-21 runtime lock")
        records = [
            *(lock.get("runtime_files") or []),
            *(lock.get("source_records") or []),
            lock.get("pool") or {},
            lock.get("mapping") or {},
            lock.get("variant") or {},
            lock.get("base_instructions") or {},
            lock.get("prompt") or {},
            lock.get("schema") or {},
            lock.get("capacity_policy") or {},
            lock.get("attempt_spec") or {},
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or [row.get("path") for row in lock.get("runtime_files") or []]
            != [str(path.resolve()) for path in _runtime_files()]
            or lock.get("predecessor_model_call_count") != PREDECESSOR_MODEL_CALL_COUNT
            or lock.get("new_model_call_cap") != NEW_MODEL_CALL_CAP
            or lock.get("aggregate_model_call_cap") != AGGREGATE_MODEL_CALL_CAP
            or lock.get("new_total_token_cap") != NEW_TOTAL_TOKEN_CAP
            or lock.get("aggregate_total_token_cap") != AGGREGATE_TOTAL_TOKEN_CAP
            or lock.get("semantic_retry_count") != 0
            or lock.get("production_amortized_total_token_ratio")
            != EXPECTED_PRODUCTION_TOKEN_RATIO
            or lock.get("winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutated") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(not isinstance(row, Mapping) or not _verify_record(row) for row in records)
        ):
            raise QualityRecoveryError("epoch-21 runtime lock drifted")
        pool = _load_json(Path(str(lock["pool"]["path"])), "frozen recovery pool")
        variant, base, prompt, schema = _adjudication_material(pool)
        if (
            _load_json(Path(str(lock["variant"]["path"])), "frozen recovery variant")
            != variant
            or Path(str(lock["base_instructions"]["path"])).read_text(encoding="utf-8")
            != base
            or Path(str(lock["prompt"]["path"])).read_text(encoding="utf-8") != prompt
            or _load_json(Path(str(lock["schema"]["path"])), "frozen recovery schema")
            != schema
        ):
            raise QualityRecoveryError("epoch-21 frozen request drifted")
        policy = reserve.load_reserve_capacity_policy(
            Path(str(lock["capacity_policy"]["path"]))
        )
        if (
            policy.get("phase_id") != STEP_ID
            or policy.get("semantic_output_root") != str(lock_path.parent)
            or policy.get("ordered_turn_names") != [TURN_NAME]
            or policy.get("minimum_remaining_reserve_percent")
            != MINIMUM_REMAINING_RESERVE_PERCENT
            or policy.get("maximum_total_tokens_per_turn") != NEW_TOTAL_TOKEN_CAP
            or policy.get("phase_total_token_bound") != NEW_TOTAL_TOKEN_CAP
            or policy.get("projected_phase_quota_points")
            != PROJECTED_PHASE_QUOTA_POINTS
        ):
            raise QualityRecoveryError("epoch-21 capacity policy drifted")
        load_contract()
        return lock

    if acquire_lock:
        with _process_lock(lock_path.parent):
            return verify()
    return verify()


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(root: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=root / "capacity-policy.json",
        inner_factory=_inner_factory,
    )


def _validate_capacity(path: Path, root: Path) -> dict[str, Any]:
    payload = _load_json(path, "epoch-21 capacity checkpoint")
    policy_path = root / "capacity-policy.json"
    expected = {
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
    if (
        not isinstance(payload, Mapping)
        or set(payload) != expected
        or path.resolve() != _turn_paths(root)["capacity"]
        or payload.get("schema_version")
        != reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION
        or payload.get("policy_path") != str(policy_path.resolve())
        or payload.get("policy_sha256") != _sha256_file(policy_path)
        or payload.get("phase_id") != STEP_ID
        or payload.get("turn_name") != TURN_NAME
        or payload.get("turn_ordinal") != 0
        or isinstance(used, bool)
        or not isinstance(used, int)
        or not 0 <= used <= 100
        or payload.get("primary_remaining_percent") != 100 - used
        or payload.get("rate_limit_reached_type") is not None
        or payload.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or payload.get("usable_percent_above_reserve")
        != max(0, 100 - used - MINIMUM_REMAINING_RESERVE_PERCENT)
        or payload.get("remaining_turn_count") != 1
        or payload.get("projected_remaining_tokens") != NEW_TOTAL_TOKEN_CAP
        or payload.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or payload.get("projected_remaining_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or payload.get("projected_terminal_remaining_percent")
        != 100 - used - PROJECTED_PHASE_QUOTA_POINTS
        or payload.get("cleared_for_semantic_turn") is not True
        or payload.get("managed_chatgpt_auth_verified") is not True
        or payload.get("plan_type") != "pro"
        or payload.get("thread_started") is not False
        or payload.get("turn_started") is not False
        or payload.get("sidecar_started") is not False
        or payload.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise judge.JudgeArtifactError("epoch-21 capacity checkpoint drifted")
    reserve.load_reserve_capacity_policy(policy_path)
    return dict(payload)


def _validate_completed_turn(
    root: Path, *, allow_invalid_output: bool = False
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    lock = verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
    pool = _load_json(Path(str(lock["pool"]["path"])), "epoch-21 pool")
    variant, base, prompt, schema = _adjudication_material(pool)
    paths = _turn_paths(root)
    try:
        output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
            raw_output_path=paths["output"],
            sidecar_path=paths["sidecar"],
            prompt=prompt,
            schema=schema,
            base_instructions=base,
            model=MODEL,
            reasoning_effort=EFFORT,
        )
        usage = shared_repair._sidecar_usage(sidecar)  # noqa: SLF001
    except (
        judge.JudgeArtifactError,
        shared_repair.SharedReferenceRepairError,
    ) as exc:
        raise judge.JudgeArtifactError("epoch-21 completed checkpoint drifted") from exc
    contract = load_contract()
    prior_ids = {
        value
        for name in ("ab", "ba")
        for value in (
            contract["predecessor_sidecars"][name]["thread_id"],
            contract["predecessor_sidecars"][name]["turn_id"],
        )
    }
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
        or sidecar.get("batch_size") != len(pool["cases"])
        or sidecar.get("thread_id") in prior_ids
        or sidecar.get("turn_id") in prior_ids
        or not isinstance(sidecar.get("thread_id"), str)
        or not sidecar.get("thread_id")
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("usage") != usage
        or sidecar.get("thread_total_usage") != usage
        or usage["total_tokens"] > NEW_TOTAL_TOKEN_CAP
    ):
        raise judge.JudgeArtifactError("epoch-21 sidecar or usage contract drifted")
    _validate_capacity(paths["capacity"], root)
    errors = judge.validate_judge_output(output, variant)
    if errors and not allow_invalid_output:
        raise judge.JudgeArtifactError("epoch-21 recovery output failed validation")
    return dict(output), dict(sidecar), errors


def _launch_receipt(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    payload = {
        "schema_version": LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "epoch20_terminal": _record(EPOCH20_ROOT / "terminal.json"),
        "epoch20_ab_output": _record(EPOCH20_ROOT / "judge/output-ab.private.json"),
        "epoch20_ba_output": _record(EPOCH20_ROOT / "judge/output-ba.private.json"),
        "variant": _record(paths["variant"]),
        "base_instructions": _record(paths["base"]),
        "prompt": _record(paths["prompt"]),
        "schema": _record(paths["schema"]),
        "capacity_policy": _record(root / "capacity-policy.json"),
        "model": MODEL,
        "effort": EFFORT,
        "judge_transport": TRANSPORT,
        "managed_chatgpt_auth_required": True,
        "prior_outputs_exposed_to_recovery_model": False,
        "semantic_retry_count": 0,
        "production_mutated": False,
        "holdout_authorized": False,
    }
    _write_immutable_json(root / "launch-receipt.json", payload)
    return payload


def _validate_launch(root: Path) -> dict[str, Any]:
    payload = _load_json(root / "launch-receipt.json", "epoch-21 launch receipt")
    paths = _turn_paths(root)
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != LAUNCH_VERSION
        or payload.get("thread_id") != THREAD_ID
        or payload.get("plan_epoch") != PLAN_EPOCH
        or payload.get("step_id") != STEP_ID
        or payload.get("runtime_lock") != _record(root / "runtime-lock.json")
        or payload.get("epoch20_terminal") != _record(EPOCH20_ROOT / "terminal.json")
        or payload.get("epoch20_ab_output")
        != _record(EPOCH20_ROOT / "judge/output-ab.private.json")
        or payload.get("epoch20_ba_output")
        != _record(EPOCH20_ROOT / "judge/output-ba.private.json")
        or payload.get("variant") != _record(paths["variant"])
        or payload.get("base_instructions") != _record(paths["base"])
        or payload.get("prompt") != _record(paths["prompt"])
        or payload.get("schema") != _record(paths["schema"])
        or payload.get("capacity_policy") != _record(root / "capacity-policy.json")
        or payload.get("model") != MODEL
        or payload.get("effort") != EFFORT
        or payload.get("judge_transport") != TRANSPORT
        or payload.get("managed_chatgpt_auth_required") is not True
        or payload.get("prior_outputs_exposed_to_recovery_model") is not False
        or payload.get("semantic_retry_count") != 0
        or payload.get("production_mutated") is not False
        or payload.get("holdout_authorized") is not False
    ):
        raise QualityRecoveryError("epoch-21 launch receipt drifted")
    return dict(payload)


def _zero_new_accounting() -> dict[str, Any]:
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 0,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "turns": [],
    }


def _new_accounting(root: Path) -> dict[str, Any]:
    try:
        return epoch20._aggregate_usage([_turn_paths(root)["sidecar"]])  # noqa: SLF001
    except epoch20.ExpandedCapDirectReferenceError as exc:
        raise OperationalWaitingError("epoch-21 usage accounting is incomplete") from exc


def _combined_accounting(new_accounting: Mapping[str, Any]) -> dict[str, Any]:
    usage = new_accounting.get("usage")
    if (
        new_accounting.get("accounting_complete") is not True
        or new_accounting.get("measured_model_call_count") != 1
        or not isinstance(usage, Mapping)
    ):
        raise OperationalWaitingError("epoch-21 measured usage is incomplete")
    combined = {
        field: EXPECTED_PREDECESSOR_USAGE[field] + int(usage[field])
        for field in USAGE_FIELDS
    }
    if combined["total_tokens"] > AGGREGATE_TOTAL_TOKEN_CAP:
        raise OperationalWaitingError("aggregate judge usage exceeded the frozen cap")
    root_value = new_accounting.get("root")
    if not isinstance(root_value, str) or not root_value:
        raise OperationalWaitingError("epoch-21 accounting root is missing")
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": AGGREGATE_MODEL_CALL_CAP,
        "usage": combined,
        "turns": [
            {"sidecar": _record(EPOCH20_ROOT / "judge/sidecars/ab.json")},
            {"sidecar": _record(EPOCH20_ROOT / "judge/sidecars/ba.json")},
            {"sidecar": _record(_turn_paths(Path(root_value))["sidecar"])},
        ],
    }


def _consensus_from_output(pool: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    case_ids = [str(case["case_id"]) for case in pool["cases"]]
    support_denominator = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    support_abstentions = sum(
        row.get("verdict") == "abstain"
        for case in output.get("cases") or []
        for row in case.get("support_results") or []
    )
    base = {
        "schema_version": judge.JUDGE_CONSENSUS_VERSION,
        "pool_sha256": _sha256_text(_canonical_json(pool)),
        "cases": [{"case_id": case_id} for case_id in case_ids],
        "abstentions": {
            "support": {
                "numerator": support_abstentions,
                "denominator": support_denominator,
                "rate": round(support_abstentions / support_denominator, 6),
            },
            "alignment_labels": {"numerator": 0, "denominator": 0, "rate": 0.0},
            "alignment_topology": {"numerator": 0, "denominator": len(case_ids), "rate": 0.0},
            "equivalence_partition": {"numerator": 0, "denominator": len(case_ids), "rate": 0.0},
        },
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }
    try:
        return shared_repair._merge_adjudication(  # noqa: SLF001
            consensus=base,
            adjudication_output=output,
            adjudicated_case_ids=case_ids,
        )
    except shared_repair.SharedReferenceRepairError as exc:
        raise QualityRecoveryError("epoch-21 consensus construction failed") from exc


def _artifact_records(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    values = {
        "runtime_lock": root / "runtime-lock.json",
        "attempt_spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "pool": root / "shared-witness-pool.private.json",
        "mapping": root / "private-witness-mapping.private.json",
        "launch_receipt": root / "launch-receipt.json",
        "variant": paths["variant"],
        "base_instructions": paths["base"],
        "prompt": paths["prompt"],
        "schema": paths["schema"],
        "capacity": paths["capacity"],
        "sidecar": paths["sidecar"],
        "output": paths["output"],
        "consensus": root / "consensus.private.json",
        "score": root / "shared-reference-score.json",
    }
    records = {}
    for role, path in values.items():
        records[role] = _record(path) if path.is_file() else None
    return records


def _receipt(
    *,
    root: Path,
    state: str,
    terminal_reason: str,
    new_accounting: Mapping[str, Any],
    combined_accounting: Mapping[str, Any] | None,
    score: Mapping[str, Any] | None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise QualityRecoveryError("invalid epoch-21 receipt state")
    new_calls = int(new_accounting.get("measured_model_call_count") or 0)
    unknown_new = 1 if new_accounting.get("accounting_complete") is not True else 0
    payload = {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": terminal_reason,
        "terminal_at": now_iso(),
        "predecessor_semantic_model_call_count": PREDECESSOR_MODEL_CALL_COUNT,
        "predecessor_usage": dict(EXPECTED_PREDECESSOR_USAGE),
        "new_semantic_model_call_cap": NEW_MODEL_CALL_CAP,
        "new_semantic_model_call_count": new_calls,
        "new_unknown_usage_turn_count": unknown_new,
        "aggregate_semantic_model_call_count": PREDECESSOR_MODEL_CALL_COUNT + new_calls,
        "semantic_retry_count": 0,
        "new_usage_status": new_accounting.get("usage_status"),
        "new_accounting_complete": new_accounting.get("accounting_complete"),
        "new_usage": new_accounting.get("usage"),
        "aggregate_usage_status": (
            combined_accounting.get("usage_status") if combined_accounting else "unknown"
        ),
        "aggregate_accounting_complete": bool(
            combined_accounting and combined_accounting.get("accounting_complete") is True
        ),
        "aggregate_usage": combined_accounting.get("usage") if combined_accounting else None,
        "production_amortized_total_token_ratio": EXPECTED_PRODUCTION_TOKEN_RATIO,
        "judge_tokens_included_in_production_formula": False,
        "development_quality_passed": bool(score and score.get("passed") is True),
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "records": _artifact_records(root),
        "next_action": (
            "freeze_epoch19_development_winner_and_prepare_untouched_holdout"
            if state == "passed"
            else "freeze_epoch19_candidate_as_semantic_quality_rejected"
            if state == "rejected"
            else "prepare_only_a_bounded_nonreplay_operational_recovery"
        ),
        "privacy": "sanitized metrics counts hashes and failure class only",
    }
    if score is not None:
        for key in (
            "failed_checks",
            "candidate_strict_full_field_macro_f1",
            "baseline_strict_full_field_macro_f1",
            "candidate_noninferiority_delta",
            "exact_evidence_rate",
        ):
            payload[key] = score.get(key)
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


def _finalize(root: Path) -> dict[str, Any]:
    output, _sidecar, errors = _validate_completed_turn(root)
    if errors:
        raise judge.JudgeArtifactError("epoch-21 recovery output failed validation")
    contract = load_contract()
    new_accounting = _new_accounting(root)
    new_accounting = {**new_accounting, "root": str(root)}
    combined = _combined_accounting(new_accounting)
    consensus = _consensus_from_output(contract["pool"], output)
    score = epoch20.score_shared_reference(
        pool=contract["pool"],
        mapping=contract["mapping"],
        consensus=consensus,
        accounting=combined,
        adjudication_call_count=1,
    )
    _write_immutable_json(root / "consensus.private.json", consensus)
    _write_immutable_json(root / "shared-reference-score.json", score)
    state = "passed" if score["passed"] else "rejected"
    return _receipt(
        root=root,
        state=state,
        terminal_reason=(
            "epoch21_origin_neutral_quality_recovery_passed"
            if state == "passed"
            else "epoch21_origin_neutral_quality_recovery_rejected"
        ),
        new_accounting=new_accounting,
        combined_accounting=combined,
        score=score,
    )


def _best_effort_new_accounting(root: Path) -> dict[str, Any]:
    sidecar = _turn_paths(root)["sidecar"]
    if not sidecar.is_file():
        return _zero_new_accounting()
    try:
        accounting = _new_accounting(root)
        return {**accounting, "root": str(root)}
    except OperationalWaitingError:
        return {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": 1,
            "usage": None,
            "turns": [],
        }


def _write_terminal(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    _write_immutable_json(root / "plan-step-receipt.json", receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return verify_receipt(root, acquire_lock=False)


async def _run_unlocked(
    *,
    root: Path,
    timeout_seconds: float,
    client_factory: Callable[[Path], Any],
) -> dict[str, Any]:
    if (root / "plan-step-receipt.json").is_file() or (root / "terminal.json").is_file():
        return verify_receipt(root, acquire_lock=False)
    _freeze_unlocked(root)
    if timeout_seconds != TIMEOUT_SECONDS:
        raise QualityRecoveryError("epoch-21 timeout differs from frozen contract")
    paths = _turn_paths(root)
    semantic_paths = (paths["capacity"], paths["sidecar"], paths["output"])
    launch_exists = (root / "launch-receipt.json").is_file()
    try:
        if launch_exists:
            _validate_launch(root)
            if not all(path.is_file() for path in semantic_paths):
                if paths["capacity"].is_file():
                    _validate_capacity(paths["capacity"], root)
                raise PartialAttemptWaitingError(
                    "epoch-21 partial attempt is immutable and cannot be replayed"
                )
        else:
            if any(path.exists() for path in semantic_paths):
                raise QualityRecoveryError(
                    "epoch-21 semantic artifacts exist without a launch receipt"
                )
            _launch_receipt(root)
            contract = load_contract()
            variant, base, prompt, schema = _adjudication_material(contract["pool"])
            async with client_factory(root) as client:
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=base,
                    prompt=prompt,
                    output_schema=schema,
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    capacity_checkpoint_path=paths["capacity"],
                    batch_size=len(variant["cases"]),
                    thread_mode="new_thread",
                    timeout_seconds=TIMEOUT_SECONDS,
                )
            if not result.status_ok or not isinstance(result.output, Mapping):
                raise OperationalWaitingError("epoch-21 semantic turn did not complete")
        receipt = _finalize(root)
    except (
        OperationalWaitingError,
        reserve.ReserveCapacityError,
        codex_app_server.AppServerError,
        judge.JudgeArtifactError,
        shared_repair.SharedReferenceRepairError,
        asyncio.TimeoutError,
        OSError,
    ) as exc:
        new_accounting = _best_effort_new_accounting(root)
        combined = None
        if new_accounting.get("accounting_complete") is True and new_accounting.get(
            "measured_model_call_count"
        ) == 1:
            try:
                combined = _combined_accounting(new_accounting)
            except OperationalWaitingError:
                combined = None
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason=(
                "epoch21_partial_attempt_preserved_without_replay"
                if isinstance(exc, PartialAttemptWaitingError)
                else "epoch21_quality_recovery_operational_or_capacity_waiting"
            ),
            new_accounting=new_accounting,
            combined_accounting=combined,
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
    with _process_lock(root):
        return await _run_unlocked(
            root=root,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
        )


def verify_receipt(
    root: Path = DEFAULT_OUTPUT_ROOT, *, acquire_lock: bool = True
) -> dict[str, Any]:
    root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        receipt = _load_json(root / "plan-step-receipt.json", "epoch-21 receipt")
        terminal = _load_json(root / "terminal.json", "epoch-21 terminal")
        if receipt != terminal:
            raise QualityRecoveryError("epoch-21 terminal mirrors differ")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("predecessor_semantic_model_call_count")
            != PREDECESSOR_MODEL_CALL_COUNT
            or receipt.get("predecessor_usage") != EXPECTED_PREDECESSOR_USAGE
            or receipt.get("new_semantic_model_call_cap") != NEW_MODEL_CALL_CAP
            or receipt.get("semantic_retry_count") != 0
            or receipt.get("production_amortized_total_token_ratio")
            != EXPECTED_PRODUCTION_TOKEN_RATIO
            or receipt.get("judge_tokens_included_in_production_formula") is not False
            or receipt.get("winner_frozen") is not False
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production_mutated") is not False
        ):
            raise QualityRecoveryError("epoch-21 receipt contract drifted")
        records = receipt.get("records")
        if not isinstance(records, Mapping):
            raise QualityRecoveryError("epoch-21 receipt records are missing")
        if any(
            not _verify_record(row)
            for row in records.values()
            if isinstance(row, Mapping)
        ):
            raise QualityRecoveryError("epoch-21 receipt artifact integrity failed")
        if records.get("runtime_lock") != _record(root / "runtime-lock.json"):
            raise QualityRecoveryError("epoch-21 receipt runtime lineage drifted")
        launch = records.get("launch_receipt")
        if launch is not None:
            if launch != _record(root / "launch-receipt.json"):
                raise QualityRecoveryError("epoch-21 launch record drifted")
            _validate_launch(root)
        if receipt["state"] == "waiting":
            if receipt.get("development_quality_passed") is not False:
                raise QualityRecoveryError("waiting receipt cannot authorize quality")
            if records.get("consensus") is not None or records.get("score") is not None:
                raise QualityRecoveryError("waiting receipt binds quality artifacts")
            if records.get("sidecar") is not None and records.get("output") is not None:
                _validate_completed_turn(root, allow_invalid_output=True)
            return receipt
        if any(records.get(role) is None for role in ("capacity", "sidecar", "output", "consensus", "score")):
            raise QualityRecoveryError("quality receipt lacks required artifacts")
        output, _sidecar, errors = _validate_completed_turn(root)
        if errors:
            raise QualityRecoveryError("quality receipt binds an invalid recovery output")
        contract = load_contract()
        new_accounting = {**_new_accounting(root), "root": str(root)}
        combined = _combined_accounting(new_accounting)
        consensus = _consensus_from_output(contract["pool"], output)
        stored_consensus = _load_json(root / "consensus.private.json", "epoch-21 consensus")
        if consensus != stored_consensus:
            raise QualityRecoveryError("epoch-21 consensus reconstruction drifted")
        score = epoch20.score_shared_reference(
            pool=contract["pool"],
            mapping=contract["mapping"],
            consensus=consensus,
            accounting=combined,
            adjudication_call_count=1,
        )
        if score != _load_json(root / "shared-reference-score.json", "epoch-21 score"):
            raise QualityRecoveryError("epoch-21 score reconstruction drifted")
        if (
            receipt.get("new_semantic_model_call_count") != 1
            or receipt.get("aggregate_semantic_model_call_count") != 3
            or receipt.get("new_usage") != new_accounting["usage"]
            or receipt.get("aggregate_usage") != combined["usage"]
            or receipt.get("aggregate_accounting_complete") is not True
            or (receipt["state"] == "passed") != (score.get("passed") is True)
            or receipt.get("development_quality_passed") != (score.get("passed") is True)
        ):
            raise QualityRecoveryError("epoch-21 receipt score or accounting drifted")
        for key in (
            "failed_checks",
            "candidate_strict_full_field_macro_f1",
            "baseline_strict_full_field_macro_f1",
            "candidate_noninferiority_delta",
            "exact_evidence_rate",
        ):
            if receipt.get(key) != score.get(key):
                raise QualityRecoveryError("epoch-21 receipt quality metrics drifted")
        return receipt

    if acquire_lock:
        with _process_lock(root):
            return verify()
    return verify()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify-runtime", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
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
        receipt = asyncio.run(
            run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds)
        )
        result = {"state": receipt["state"], "terminal_reason": receipt["terminal_reason"]}
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
