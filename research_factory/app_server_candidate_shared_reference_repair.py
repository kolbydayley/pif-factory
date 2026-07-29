from __future__ import annotations

"""Run the checksum-bound shared augmented reference correction.

This adapter intentionally performs no extraction. It reprojects the frozen
52-event development pool into the mature full-event judge, runs blinded AB/BA
turns, optionally resolves observable disagreement with one origin-neutral
turn, and scores both named systems against the same supported union.
"""

import argparse
import asyncio
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_capacity
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_candidate_shared_reference_repair_v1"
LOCK_VERSION = "pif_candidate_shared_reference_repair_lock_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 1
STEP_ID = "shared_augmented_reference_repair_v1"
MODEL = "gpt-5.5"
EFFORT = "high"
MODEL_CALL_CAP = 3
TOTAL_TOKEN_CAP = 400_000
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_TARGET = 0.28
SYSTEM_BASELINE = "baseline_reference_seed"
SYSTEM_CANDIDATE = "candidate_v249"
EXPECTED_WITNESS_COUNTS = {
    ("dense", SYSTEM_BASELINE): 27,
    ("dense", SYSTEM_CANDIDATE): 24,
    ("no_signal", SYSTEM_BASELINE): 0,
    ("no_signal", SYSTEM_CANDIDATE): 1,
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = (PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v1.json").resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation" / "pif-evaluation-shared-reference-repair-v1.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-selection-v249-shared-augmented-reference-correction-v1"
).resolve()
PINNED_CODEX = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "pinned-runtime"
    / "codex-0.144.1"
    / "bin"
    / "codex"
).resolve()
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


class SharedReferenceRepairError(RuntimeError):
    """The frozen repair contract or one of its artifacts is invalid."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SharedReferenceRepairError(f"cannot read {label}") from exc


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
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise SharedReferenceRepairError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise SharedReferenceRepairError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _direct_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "shared-reference directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    execution = directive.get("execution_contract") if isinstance(directive, Mapping) else None
    acceptance = directive.get("acceptance_contract") if isinstance(directive, Mapping) else None
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
        or directive.get("schema_version")
        != "pif_evaluation_shared_reference_repair_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or not isinstance(execution, Mapping)
        or not isinstance(acceptance, Mapping)
        or execution.get("extraction_model_call_cap") != 0
        or execution.get("semantic_judge_call_cap") != MODEL_CALL_CAP
        or execution.get("semantic_retry_cap") != 0
        or execution.get("judge_model") != MODEL
        or execution.get("judge_reasoning_effort") != EFFORT
        or execution.get("variants") != ["ab", "ba"]
        or execution.get("reference_policy")
        != "union_of_full_event_source_supported_exact_evidence_equivalence_groups_contributed_by_either_system"
        or acceptance.get("candidate_strict_full_field_macro_f1_min")
        != QUALITY_THRESHOLD
        or acceptance.get("candidate_must_be_noninferior_to_baseline") is not True
        or acceptance.get("production_amortized_total_token_ratio_max")
        != TOKEN_RATIO_TARGET
        or acceptance.get("exact_evidence_rate") != 1.0
        or acceptance.get("accounting_complete") is not True
    ):
        raise SharedReferenceRepairError("semantic plan or directive contract drifted")
    receipt_path = Path(str(step.get("expected_receipt_path"))).resolve()
    if (
        receipt_path != Path(str(directive.get("expected_receipt_path"))).resolve()
        or receipt_path != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json"
    ):
        raise SharedReferenceRepairError("expected receipt path drifted")
    frozen_records = []
    for row in directive.get("frozen_inputs") or []:
        if not isinstance(row, Mapping):
            raise SharedReferenceRepairError("directive frozen input is malformed")
        path = Path(str(row.get("path"))).resolve()
        if not path.is_file() or row.get("sha256") != _sha256_file(path):
            raise SharedReferenceRepairError("directive frozen input drifted")
        frozen_records.append(_record(path))
    if len(frozen_records) != 4:
        raise SharedReferenceRepairError("directive frozen input coverage drifted")
    return {
        "plan": plan,
        "directive": directive,
        "receipt_path": receipt_path,
        "frozen_records": frozen_records,
    }


def build_shared_pool(contract: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    frozen_paths = [Path(str(row["path"])) for row in contract["frozen_records"]]
    support_path = next(path for path in frozen_paths if path.name == "support-pool.private.json")
    origin_path = next(path for path in frozen_paths if path.name == "origin-map.private.json")
    support = _load_json(support_path, "frozen support pool")
    origin = _load_json(origin_path, "frozen origin map")
    if (
        support.get("schema_version") != "pif_adoption_semantic_evaluation_v1"
        or origin.get("schema_version") != "pif_adoption_semantic_evaluation_v1"
        or not isinstance(support.get("cases"), list)
        or not isinstance(origin.get("rows"), list)
    ):
        raise SharedReferenceRepairError("frozen 52-witness source shape drifted")
    origin_by_id = {str(row.get("witness_id")): row for row in origin["rows"]}
    if len(origin_by_id) != 52:
        raise SharedReferenceRepairError("frozen witness mapping is not 52 unique rows")
    raw_cases = []
    observed_counts = {key: 0 for key in EXPECTED_WITNESS_COUNTS}
    seen_ids = set()
    for case in support["cases"]:
        density = str(case.get("density_stratum"))
        grouped = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
        for witness in case.get("witnesses") or []:
            legacy_id = str(witness.get("witness_id"))
            if legacy_id in seen_ids or legacy_id not in origin_by_id:
                raise SharedReferenceRepairError("support pool witness mapping drifted")
            seen_ids.add(legacy_id)
            row = origin_by_id[legacy_id]
            if row.get("case_id") != case.get("case_id") or row.get("density_stratum") != density:
                raise SharedReferenceRepairError("support pool case provenance drifted")
            origin_name = row.get("origin")
            if origin_name == "reference":
                system_id = SYSTEM_BASELINE
            elif origin_name == "candidate":
                system_id = SYSTEM_CANDIDATE
            else:
                raise SharedReferenceRepairError("unknown source system membership")
            observed_counts[(density, system_id)] += 1
            grouped[system_id].append(
                {
                    "event": deepcopy(witness.get("event")),
                    "provenance": {
                        "system_id": system_id,
                        "legacy_witness_id": legacy_id,
                        "density_stratum": density,
                    },
                }
            )
        raw_cases.append(
            {
                "case_key": str(case.get("case_id")),
                "source_excerpt": case.get("source_excerpt"),
                "event_set_a": grouped[SYSTEM_BASELINE],
                "event_set_b": grouped[SYSTEM_CANDIDATE],
                "provenance": {"density_stratum": density},
            }
        )
    if seen_ids != set(origin_by_id) or observed_counts != EXPECTED_WITNESS_COUNTS:
        raise SharedReferenceRepairError("frozen 27+25 witness membership drifted")
    pool, mapping = judge.make_shared_witness_pool(
        raw_cases,
        seed="shared_augmented_reference_repair_v1",
    )
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise SharedReferenceRepairError("generated shared witness pool is invalid")
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    )
    if len(pool["cases"]) != 2 or witness_count != 52 or exact_count != 52:
        raise SharedReferenceRepairError("shared witness exact-evidence invariant drifted")
    return pool, mapping


def _runtime_files() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(app_server_capacity.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        PINNED_CODEX,
    )


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "runtime-lock.json").is_file():
        verify_runtime_lock(root / "runtime-lock.json")
        return {"root": root, "runtime_lock": root / "runtime-lock.json"}
    if root.exists() and any(root.iterdir()):
        raise SharedReferenceRepairError("unfrozen output root is not empty")
    contract = load_contract()
    pool, mapping = build_shared_pool(contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    variants = judge.build_judge_variants(pool)
    base_instructions = judge.judge_base_instructions()
    request_records = []
    request_sizes = {}
    for name in ("ab", "ba"):
        prompt = judge.build_judge_prompt(variants[name])
        schema = judge.semantic_judge_output_schema(variants[name])
        prompt_path = root / "judge" / f"prompt-{name}.private.md"
        schema_path = root / "judge" / f"schema-{name}.json"
        _write_immutable_text(prompt_path, prompt)
        _write_immutable_json(schema_path, schema)
        request_records.extend((_record(prompt_path), _record(schema_path)))
        request_sizes[name] = {
            "prompt_bytes": len(prompt.encode("utf-8")),
            "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        }
    instructions_path = root / "judge" / "base-instructions.private.md"
    _write_immutable_text(instructions_path, base_instructions)
    request_records.append(_record(instructions_path))
    spec_path = root / "attempt-spec.json"
    _write_immutable_json(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "frozen_at": now_iso(),
            "thread_id": THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "model": MODEL,
            "effort": EFFORT,
            "declared_model_call_cap": MODEL_CALL_CAP,
            "declared_total_token_cap": TOTAL_TOKEN_CAP,
            "extraction_model_call_cap": 0,
            "retry_count_per_turn": 0,
            "variant_order": ["ab", "ba"],
            "variant_case_order_and_opaque_ids_identical": True,
            "reference_system_ids": None,
            "quality_threshold": QUALITY_THRESHOLD,
            "token_ratio_target": TOKEN_RATIO_TARGET,
            "request_sizes": request_sizes,
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "production_mutation_allowed": False,
            "holdout_authorized": False,
        },
    )
    source_records = [
        _record(PLAN_PATH),
        _record(DIRECTIVE_PATH),
        *contract["frozen_records"],
    ]
    runtime_records = [_record(path) for path in _runtime_files()]
    frozen_records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        *request_records,
        _record(spec_path),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_files": runtime_records,
        "source_records": source_records,
        "pool": _record(pool_path),
        "mapping": _record(mapping_path),
        "request_records": request_records,
        "attempt_spec": _record(spec_path),
        "direct_record_digest": _direct_digest(frozen_records),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_cap": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_immutable_json(lock_path, lock)
    verify_runtime_lock(lock_path)
    return {"root": root, "runtime_lock": lock_path}


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()
    lock = _load_json(lock_path, "runtime lock")
    expected_runtime = {str(item.resolve()) for item in _runtime_files()}
    actual_runtime = {
        str(Path(str(row.get("path"))).resolve()) for row in lock.get("runtime_files") or []
    }
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
        or lock.get("semantic_retry_count") != 0
        or lock.get("extraction_model_call_cap") != 0
        or lock.get("production_mutation_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or actual_runtime != expected_runtime
        or lock.get("direct_record_digest") != _direct_digest(records)
    ):
        raise SharedReferenceRepairError("shared-reference runtime lock drifted")
    if not records or any(not _verify_record(row) for row in records):
        raise SharedReferenceRepairError("shared-reference runtime artifact drifted")
    load_contract()
    return lock


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory() -> CapacityGatedCodexAppServerClient:
    return CapacityGatedCodexAppServerClient(inner_factory=_inner_factory)


def _sidecar_usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage") if isinstance(sidecar, Mapping) else None
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or not isinstance(usage, Mapping)
    ):
        raise SharedReferenceRepairError("judge sidecar is not measured managed-auth output")
    normalized = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SharedReferenceRepairError("judge usage telemetry is incomplete")
        normalized[field] = value
    if (
        normalized["cached_input_tokens"] > normalized["input_tokens"]
        or normalized["reasoning_output_tokens"] > normalized["output_tokens"]
        or normalized["total_tokens"]
        != normalized["input_tokens"] + normalized["output_tokens"]
    ):
        raise SharedReferenceRepairError("judge usage accounting is inconsistent")
    return normalized


def _aggregate_usage(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    totals = {field: 0 for field in USAGE_FIELDS}
    rows = []
    for path in sidecar_paths:
        sidecar = _load_json(path, "judge sidecar")
        usage = _sidecar_usage(sidecar)
        for field in USAGE_FIELDS:
            totals[field] += usage[field]
        rows.append({"sidecar": _record(path), "usage": usage})
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": len(rows),
        "usage": totals,
        "turns": rows,
    }


def _observable_disagreement_case_ids(
    pool: Mapping[str, Any],
    outputs: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    variants = judge.build_judge_variants(pool)
    for name in ("ab", "ba"):
        errors = judge.validate_judge_output(outputs[name], variants[name])
        if errors:
            raise SharedReferenceRepairError("judge output failed validation")
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
    return sorted(disagreement)


def _build_adjudication_variant(
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
                "event_set_a": deepcopy(witnesses),
                "event_set_b": [],
            }
        )
    if not cases or {row["case_id"] for row in cases} != selected:
        raise SharedReferenceRepairError("adjudication disagreement coverage drifted")
    return {
        "schema_version": judge.JUDGE_VARIANT_VERSION,
        "variant": "side_free_adjudication",
        "orientation": "ab",
        "cases": cases,
    }


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
        raise SharedReferenceRepairError("adjudication attempt already exists; retry is forbidden")
    variant = _build_adjudication_variant(pool, case_ids)
    base_instructions = judge.judge_base_instructions()
    prompt = (
        "This is the sole capped, origin-neutral adjudication. All witnesses are pooled on one "
        "presentation side. Independently decide full-event source support and the full semantic "
        "equivalence partition. Do not vote between, infer, or reference prior judge outputs.\n\n"
        + judge.build_judge_prompt(variant)
    )
    schema = judge.semantic_judge_output_schema(variant)
    variant_path = phase_root / "variant.private.json"
    base_path = phase_root / "base-instructions.private.md"
    prompt_path = phase_root / "prompt.private.md"
    schema_path = phase_root / "schema.json"
    _write_immutable_json(variant_path, variant)
    _write_immutable_text(base_path, base_instructions)
    _write_immutable_text(prompt_path, prompt)
    _write_immutable_json(schema_path, schema)
    predecessor_records = [
        _record(root / "runtime-lock.json"),
        _record(root / "judge" / "report.json"),
        _record(root / "judge" / "output-ab.private.json"),
        _record(root / "judge" / "output-ba.private.json"),
        _record(root / "judge" / "sidecars" / "ab.json"),
        _record(root / "judge" / "sidecars" / "ba.json"),
        _record(variant_path),
        _record(base_path),
        _record(prompt_path),
        _record(schema_path),
    ]
    lock_path = phase_root / "runtime-lock.json"
    _write_immutable_json(
        lock_path,
        {
            "schema_version": LOCK_VERSION,
            "frozen_at": now_iso(),
            "phase": "sole_side_free_adjudication",
            "case_ids": list(case_ids),
            "model": MODEL,
            "effort": EFFORT,
            "model_call_cap": 1,
            "retry_count": 0,
            "direct_records": predecessor_records,
            "direct_record_digest": _direct_digest(predecessor_records),
            "production_mutation_allowed": False,
        },
    )
    if any(not _verify_record(row) for row in predecessor_records):
        raise SharedReferenceRepairError("adjudication predecessor drifted")
    sidecar_path = phase_root / "sidecar.json"
    output_path = phase_root / "output.private.json"
    capacity_path = phase_root / "capacity.json"
    async with client_factory() as client:
        result = await client.run_ephemeral_structured_turn(
            model=MODEL,
            effort=EFFORT,
            base_instructions=base_instructions,
            prompt=prompt,
            output_schema=schema,
            cwd=PROJECT_ROOT,
            sidecar_path=sidecar_path,
            output_path=output_path,
            capacity_checkpoint_path=capacity_path,
            batch_size=len(case_ids),
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    if not result.status_ok or not isinstance(result.output, Mapping):
        raise SharedReferenceRepairError("side-free adjudication turn failed")
    output = _load_json(output_path, "adjudication output")
    errors = judge.validate_judge_output(output, variant)
    if errors:
        raise SharedReferenceRepairError("side-free adjudication output is invalid")
    _sidecar_usage(_load_json(sidecar_path, "adjudication sidecar"))
    capacity = _load_json(capacity_path, "adjudication capacity")
    if (
        capacity.get("schema_version") != app_server_capacity.CAPACITY_CHECKPOINT_VERSION
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise SharedReferenceRepairError("adjudication capacity checkpoint is invalid")
    return output, sidecar_path


def _merge_adjudication(
    *,
    consensus: Mapping[str, Any],
    adjudication_output: Mapping[str, Any],
    adjudicated_case_ids: Sequence[str],
) -> dict[str, Any]:
    selected = set(adjudicated_case_ids)
    adjudicated = {str(row["case_id"]): row for row in adjudication_output["cases"]}
    if set(adjudicated) != selected:
        raise SharedReferenceRepairError("adjudication output case coverage drifted")
    cases = []
    for prior in consensus["cases"]:
        case_id = str(prior["case_id"])
        if case_id not in selected:
            cases.append(deepcopy(prior))
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
    merged = deepcopy(dict(consensus))
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


def _witness_systems(mapping: Mapping[str, Any]) -> dict[str, str]:
    result = {}
    for case in mapping["cases"]:
        case_provenance = case.get("case_provenance") or {}
        for witness in case["witnesses"]:
            provenance = witness.get("provenance") or {}
            system_id = provenance.get("system_id") or case_provenance.get(
                f"system_{witness['canonical_side']}_id"
            )
            if system_id not in {SYSTEM_BASELINE, SYSTEM_CANDIDATE}:
                raise SharedReferenceRepairError("private system membership drifted")
            result[str(witness["witness_id"])] = str(system_id)
    return result


def _macro_scores(
    *,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    consensus: Mapping[str, Any],
) -> dict[str, Any]:
    systems = _witness_systems(mapping)
    case_provenance = {
        str(row["case_id"]): row.get("case_provenance") or {}
        for row in mapping["cases"]
    }
    pool_by_case = {str(row["case_id"]): row for row in pool["cases"]}
    per_case = []
    totals = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
    for row in consensus["cases"]:
        case_id = str(row["case_id"])
        if row.get("partition_abstained_witness_ids"):
            raise SharedReferenceRepairError("cannot score an abstained shared partition")
        witness_event = {
            witness["witness_id"]: witness["event"]
            for side in ("a", "b")
            for witness in pool_by_case[case_id][f"event_set_{side}"]
        }
        support = {
            str(item["witness_id"]): str(item["verdict"])
            for item in row["support_results"]
        }
        groups = [frozenset(group) for group in row["equivalence_groups"]]
        if set().union(*groups) != set(witness_event):
            raise SharedReferenceRepairError("shared partition does not cover the case")
        group_by_witness = {
            witness_id: index for index, group in enumerate(groups) for witness_id in group
        }
        reference_units = {
            group_by_witness[witness_id]
            for witness_id, verdict in support.items()
            if verdict == "supported"
            and witness_event[witness_id].get("submitted_evidence_exact") is True
        }
        system_rows = {}
        for system_id in (SYSTEM_BASELINE, SYSTEM_CANDIDATE):
            submitted_ids = {
                witness_id for witness_id in witness_event if systems[witness_id] == system_id
            }
            submitted_units = {group_by_witness[item] for item in submitted_ids}
            supported_units = {
                group_by_witness[item]
                for item in submitted_ids
                if support[item] == "supported"
                and witness_event[item].get("submitted_evidence_exact") is True
            }
            precision = (
                len(supported_units) / len(submitted_units)
                if submitted_units
                else 1.0
            )
            recall = (
                len(supported_units & reference_units) / len(reference_units)
                if reference_units
                else 1.0
            )
            f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
            totals[system_id].append(f1)
            system_rows[system_id] = {
                "submitted_unit_count": len(submitted_units),
                "supported_unit_count": len(supported_units),
                "shared_reference_unit_count": len(reference_units),
                "strict_precision": round(precision, 6),
                "strict_recall": round(recall, 6),
                "strict_f1": round(f1, 6),
            }
        per_case.append(
            {
                "case_id": case_id,
                "density_stratum": case_provenance[case_id].get("density_stratum"),
                "systems": system_rows,
            }
        )
    return {
        "systems": {
            system_id: {
                "strict_full_field_macro_f1": round(sum(values) / len(values), 6)
            }
            for system_id, values in totals.items()
        },
        "cases": per_case,
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
        raise SharedReferenceRepairError("shared reference did not include both systems")
    macro = _macro_scores(pool=pool, mapping=mapping, consensus=consensus)
    baseline_macro = macro["systems"][SYSTEM_BASELINE]["strict_full_field_macro_f1"]
    candidate_macro = macro["systems"][SYSTEM_CANDIDATE]["strict_full_field_macro_f1"]
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    )
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    total_tokens = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    checks = {
        "full_52_witness_pool": witness_count == 52,
        "exact_evidence_rate_1": exact_count == witness_count,
        "both_systems_scored_against_same_union": shared.get("reference_system_ids")
        == [SYSTEM_BASELINE, SYSTEM_CANDIDATE],
        "candidate_strict_full_field_macro_f1_gte_0_97": candidate_macro
        >= QUALITY_THRESHOLD,
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "production_amortized_total_token_ratio_lte_0_28": production_token_ratio
        <= TOKEN_RATIO_TARGET,
        "accounting_complete": accounting.get("accounting_complete") is True,
        "semantic_model_call_cap_lte_3": accounting.get("measured_model_call_count", 99)
        <= MODEL_CALL_CAP,
        "semantic_total_token_cap_lte_400000": isinstance(total_tokens, int)
        and total_tokens <= TOTAL_TOKEN_CAP,
        "adjudication_call_cap_lte_1": adjudication_call_count <= 1,
        "extraction_model_call_count_0": True,
        "production_unchanged": True,
    }
    failed = sorted(name for name, value in checks.items() if value is not True)
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
        "adjudication_call_count": adjudication_call_count,
        "partial_alignment_is_diagnostic_only": True,
        "reference_policy": "union_of_full_event_source_supported_exact_evidence_equivalence_groups_contributed_by_either_system",
        "production_mutated": False,
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
    error: BaseException | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise SharedReferenceRepairError("invalid plan-step receipt state")
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
        "semantic_retry_count": 0,
        "extraction_model_call_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "development_quality_passed": bool(score and score.get("passed")),
        "development_winner_frozen": bool(score and score.get("passed")),
        "holdout_authorized": False,
        "production_mutated": False,
        "runtime_lock": _record(root / "runtime-lock.json"),
        "score": _record(root / "shared-reference-score.json")
        if (root / "shared-reference-score.json").is_file()
        else None,
        "consensus": _record(root / "consensus.private.json")
        if (root / "consensus.private.json").is_file()
        else None,
        "next_action": (
            "freeze_development_winner_then_open_untouched_holdout_under_separate_authorization"
            if state == "passed"
            else "run_one_ordered_recurrent_full_schema_fold_canary_with_predeclared_72891_extraction_token_cap"
            if state == "rejected"
            else "resume_only_the_frozen_shared_reference_step_after_external_capacity_or_transport_clearance"
        ),
        "privacy": "sanitized metrics counts hashes and failure class no source event prompt or output text",
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
                "production_amortized_total_token_ratio": score.get(
                    "production_amortized_total_token_ratio"
                ),
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


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    receipt_path = root / "plan-step-receipt.json"
    if receipt_path.is_file():
        receipt = _load_json(receipt_path, "plan-step receipt")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
        ):
            raise SharedReferenceRepairError("existing plan-step receipt drifted")
        return receipt
    frozen = freeze_run(root)
    verify_runtime_lock(frozen["runtime_lock"])
    pool = _load_json(root / "shared-witness-pool.private.json", "shared pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "private mapping")
    sidecar_paths: list[Path] = []
    try:
        await judge.run_app_server_semantic_judge(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=MODEL,
            reasoning_effort=EFFORT,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
        )
        for name in ("ab", "ba"):
            sidecar_paths.append(root / "judge" / "sidecars" / f"{name}.json")
        outputs = {
            name: _load_json(root / "judge" / f"output-{name}.private.json", f"{name} output")
            for name in ("ab", "ba")
        }
        consensus = _load_json(root / "judge" / "consensus.private.json", "judge consensus")
        disagreement_ids = _observable_disagreement_case_ids(pool, outputs)
        adjudication_call_count = 0
        if disagreement_ids:
            adjudication_output, adjudication_sidecar = await _run_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreement_ids,
                client_factory=client_factory,
                timeout_seconds=timeout_seconds,
            )
            sidecar_paths.append(adjudication_sidecar)
            adjudication_call_count = 1
            consensus = _merge_adjudication(
                consensus=consensus,
                adjudication_output=adjudication_output,
                adjudicated_case_ids=disagreement_ids,
            )
        _write_immutable_json(root / "consensus.private.json", consensus)
        accounting = _aggregate_usage(sidecar_paths)
        directive = load_contract()["directive"]
        ratio = float(directive["invalidated_gate"]["production_amortized_total_token_ratio"])
        score = score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=consensus,
            production_token_ratio=ratio,
            accounting=accounting,
            adjudication_call_count=adjudication_call_count,
        )
        _write_immutable_json(root / "shared-reference-score.json", score)
        state = "passed" if score["passed"] else "rejected"
        terminal_reason = (
            "shared_augmented_reference_quality_gate_passed"
            if score["passed"]
            else "shared_augmented_reference_quality_gate_not_passed"
        )
        receipt = _receipt(
            root=root,
            state=state,
            terminal_reason=terminal_reason,
            accounting=accounting,
            score=score,
        )
    except BaseException as exc:
        existing_sidecars = sorted(root.glob("judge/sidecars/*.json")) + sorted(
            root.glob("adjudication/sidecar.json")
        )
        try:
            accounting = _aggregate_usage(existing_sidecars) if existing_sidecars else {
                "usage_status": "complete",
                "accounting_complete": True,
                "measured_model_call_count": 0,
                "usage": {field: 0 for field in USAGE_FIELDS},
                "turns": [],
            }
        except SharedReferenceRepairError:
            accounting = {
                "usage_status": "unknown",
                "accounting_complete": False,
                "measured_model_call_count": len(existing_sidecars),
                "usage": None,
                "turns": [],
            }
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason="infrastructure_or_judge_attempt_failed",
            accounting=accounting,
            score=None,
            error=exc,
        )
    _write_immutable_json(receipt_path, receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the shared augmented reference repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--freeze-only", action="store_true")
    parser.add_argument("--verify", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = Path(args.output_dir).expanduser().resolve()
    if args.verify:
        verify_runtime_lock(root / "runtime-lock.json")
        print(json.dumps({"ok": True, "state": "verified"}, sort_keys=True))
        return 0
    if args.freeze_only:
        freeze_run(root)
        print(json.dumps({"ok": True, "state": "frozen"}, sort_keys=True))
        return 0
    receipt = asyncio.run(run(output_dir=root, timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "ok": receipt.get("state") in {"passed", "rejected", "waiting"},
                "state": receipt.get("state"),
                "terminal_reason": receipt.get("terminal_reason"),
                "semantic_model_call_count": receipt.get("semantic_model_call_count"),
                "accounting_complete": receipt.get("accounting_complete"),
                "development_quality_passed": receipt.get("development_quality_passed"),
                "production_mutated": receipt.get("production_mutated"),
            },
            sort_keys=True,
        )
    )
    return 0 if receipt.get("state") in {"passed", "rejected", "waiting"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
