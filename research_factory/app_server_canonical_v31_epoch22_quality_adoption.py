"""Zero-call adoption of the measured epoch-21 quality decision.

Epoch 21 completed one managed-auth judge turn but two evidence spans copied
line-wrapped speaker markers differently from the source packet.  This adapter
allows only a uniquely reversible formatting projection, validates that no
semantic field changes, and recomputes the frozen shared-reference score.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from . import app_server_canonical_v31_single_message_direct_reference as epoch20
from . import app_server_canonical_v31_single_message_quality_recovery as epoch21
from . import app_server_llm_judge as judge


PROJECT_ROOT = Path(__file__).resolve().parents[1]
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 22
STEP_ID = "canonical_v31_epoch22_quality_adoption_v22"
DIRECTIVE_PATH = PROJECT_ROOT / "automation/pif-evaluation-epoch22-quality-adoption-v22.json"
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v22.json"
EPOCH21_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch21-single-message-quality-recovery-v1"
)
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
    / "canonical-v31-epoch22-quality-adoption-v1"
)

RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
RUNTIME_LOCK_VERSION = "pif_epoch22_quality_adoption_runtime_lock_v1"
PROJECTION_AUDIT_VERSION = "pif_nonsemantic_exact_span_projection_audit_v1"
PROCESS_LOCK_NAME = ".canonical-v31-epoch22-quality-adoption.lock"
SOURCE_VALIDATION_ERRORS = [
    "case_5_support_11_evidence_not_exact",
    "case_5_support_22_evidence_not_exact",
]
EXPECTED_FAILED_CHECKS = [
    "candidate_noninferior_to_baseline",
    "candidate_strict_full_field_macro_f1_gte_0_97",
]
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


class Epoch22QualityAdoptionError(RuntimeError):
    """Raised when immutable lineage or deterministic projection drifts."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _record(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise Epoch22QualityAdoptionError(f"required artifact is missing: {resolved}")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Epoch22QualityAdoptionError(f"{label} is unreadable") from exc
    if not isinstance(value, dict):
        raise Epoch22QualityAdoptionError(f"{label} must be a JSON object")
    return value


def _write_immutable_json(path: Path, value: Any) -> None:
    data = _pretty_json(value).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise Epoch22QualityAdoptionError(f"immutable artifact drifted: {path}")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def _process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / PROCESS_LOCK_NAME
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Epoch22QualityAdoptionError("another epoch-22 adoption owns the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_contract() -> dict[str, Any]:
    plan = _load_json(PLAN_PATH, "epoch-22 semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "epoch-22 directive")
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
    step = plan.get("step")
    if (
        set(plan) != expected_plan_keys
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or not isinstance(step, Mapping)
        or set(step) != expected_step_keys
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 0
        or step.get("max_total_tokens") != 0
        or step.get("expected_receipt_path") != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or step.get("directive_path") != str(DIRECTIVE_PATH)
        or step.get("directive_sha256") != _sha256_file(DIRECTIVE_PATH)
    ):
        raise Epoch22QualityAdoptionError("epoch-22 semantic plan drifted")
    if (
        directive.get("schema_version")
        != "pif_evaluation_epoch22_quality_adoption_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authority") != "direct_user_instruction"
        or directive.get("authorized_by") != "kolby"
        or directive.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or directive.get("state") != "authorized_zero_call_deterministic_adoption"
        or directive.get("execution_contract", {}).get("semantic_model_call_cap") != 0
        or directive.get("execution_contract", {}).get("semantic_retry_cap") != 0
        or directive.get("execution_contract", {}).get("semantic_fields_mutable") is not False
        or directive.get("execution_contract", {}).get("exact_span_projection_only") is not True
        or directive.get("acceptance_contract", {}).get("quality_threshold") != 0.97
        or directive.get("acceptance_contract", {}).get("production_ratio_max") != 0.28
        or directive.get("acceptance_contract", {}).get("holdout_authorized") is not False
        or directive.get("acceptance_contract", {}).get("production_mutation_allowed") is not False
        or directive.get("projection_contract", {}).get("source_validation_errors")
        != SOURCE_VALIDATION_ERRORS
    ):
        raise Epoch22QualityAdoptionError("epoch-22 directive values drifted")
    frozen = directive.get("frozen_inputs")
    if not isinstance(frozen, list) or not frozen:
        raise Epoch22QualityAdoptionError("epoch-22 frozen inputs are missing")
    for item in frozen:
        if not isinstance(item, Mapping) or set(item) != {"role", "path", "sha256", "size_bytes"}:
            raise Epoch22QualityAdoptionError("epoch-22 frozen input shape drifted")
        if _record(Path(str(item["path"]))) != {
            "path": str(Path(str(item["path"])).resolve()),
            "sha256": item["sha256"],
            "size_bytes": item["size_bytes"],
        }:
            raise Epoch22QualityAdoptionError(f"epoch-22 frozen input drifted: {item['role']}")
    return {"plan": plan, "directive": directive}


def _canonical_speaker_stream(text: str) -> tuple[str, list[int]]:
    """Return a formatting-neutral stream and source index for each character."""

    chars: list[str] = []
    indexes: list[int] = []
    index = 0
    line_start = True
    pending_space = False
    while index < len(text):
        if line_start:
            prefix_start = index
            while prefix_start < len(text) and text[prefix_start] in " \t":
                prefix_start += 1
            if text.startswith("Speaker ", prefix_start):
                cursor = prefix_start + len("Speaker ")
                digit_start = cursor
                while cursor < len(text) and text[cursor].isdigit():
                    cursor += 1
                if cursor > digit_start and cursor < len(text) and text[cursor] == ":":
                    index = cursor + 1
                    while index < len(text) and text[index] in " \t":
                        index += 1
                    line_start = False
                    continue
        char = text[index]
        if char.isspace():
            pending_space = bool(chars)
            if char in "\r\n":
                line_start = True
            index += 1
            continue
        if pending_space:
            chars.append(" ")
            indexes.append(index)
            pending_space = False
        chars.append(char)
        indexes.append(index)
        line_start = False
        index += 1
    return "".join(chars), indexes


def _unique_exact_projection(span: str, source: str) -> str:
    if span in source:
        return span
    canonical_source, source_indexes = _canonical_speaker_stream(source)
    canonical_span, _ = _canonical_speaker_stream(span)
    if not canonical_span:
        raise Epoch22QualityAdoptionError("empty normalized evidence span")
    starts: list[int] = []
    offset = 0
    while True:
        found = canonical_source.find(canonical_span, offset)
        if found < 0:
            break
        starts.append(found)
        offset = found + 1
    if len(starts) != 1:
        raise Epoch22QualityAdoptionError("evidence span does not have one exact formatting projection")
    start = starts[0]
    projected = source[source_indexes[start] : source_indexes[start + len(canonical_span) - 1] + 1]
    if projected not in source or _canonical_speaker_stream(projected)[0] != canonical_span:
        raise Epoch22QualityAdoptionError("exact evidence projection failed reconstruction")
    return projected


def project_output(
    output: Mapping[str, Any], variant: Mapping[str, Any], directive: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected = copy.deepcopy(dict(output))
    cases = projected.get("cases")
    variant_cases = variant.get("cases")
    if not isinstance(cases, list) or not isinstance(variant_cases, list) or len(cases) != len(variant_cases):
        raise Epoch22QualityAdoptionError("epoch-21 case structure drifted")
    changes: list[dict[str, Any]] = []
    for case_index, (case, variant_case) in enumerate(zip(cases, variant_cases)):
        source = variant_case.get("source_excerpt") if isinstance(variant_case, Mapping) else None
        support = case.get("support_results") if isinstance(case, Mapping) else None
        if not isinstance(source, str) or not isinstance(support, list):
            raise Epoch22QualityAdoptionError("epoch-21 support structure drifted")
        for support_index, row in enumerate(support):
            spans = row.get("evidence_spans") if isinstance(row, Mapping) else None
            if not isinstance(spans, list):
                raise Epoch22QualityAdoptionError("epoch-21 evidence span structure drifted")
            replacements: list[str] = []
            for span_index, span in enumerate(spans):
                if not isinstance(span, str):
                    raise Epoch22QualityAdoptionError("epoch-21 evidence span is not text")
                exact = _unique_exact_projection(span, source)
                replacements.append(exact)
                if exact != span:
                    changes.append(
                        {
                            "case_index": case_index,
                            "support_index": support_index,
                            "span_index": span_index,
                            "before_sha256": _sha256_bytes(span.encode("utf-8")),
                            "before_size_bytes": len(span.encode("utf-8")),
                            "after_sha256": _sha256_bytes(exact.encode("utf-8")),
                            "after_size_bytes": len(exact.encode("utf-8")),
                            "unique_projection_count": 1,
                        }
                    )
            row["evidence_spans"] = replacements
    expected = directive.get("projection_contract", {}).get("expected_changes")
    if changes != expected:
        raise Epoch22QualityAdoptionError("exact-span projection set drifted")
    errors = judge.validate_judge_output(projected, variant)
    if errors:
        raise Epoch22QualityAdoptionError("projected judge output remains invalid")
    audit = {
        "schema_version": PROJECTION_AUDIT_VERSION,
        "source_output": _record(epoch21._turn_paths(EPOCH21_ROOT)["output"]),  # noqa: SLF001
        "source_errors": SOURCE_VALIDATION_ERRORS,
        "projection_count": len(changes),
        "changes": changes,
        "semantic_fields_changed": False,
        "verdicts_changed": False,
        "alignments_changed": False,
        "equivalence_groups_changed": False,
        "rationales_changed": False,
        "exact_validation_errors_after_projection": [],
        "privacy": "hashes indexes sizes and counts only",
    }
    return projected, audit


def _runtime_records(contract: Mapping[str, Any]) -> dict[str, Any]:
    frozen = contract["directive"]["frozen_inputs"]
    return {
        "adapter": _record(Path(__file__)),
        "directive": _record(DIRECTIVE_PATH),
        "plan": _record(PLAN_PATH),
        "judge_runtime": _record(Path(judge.__file__)),
        "epoch20_scorer": _record(Path(epoch20.__file__)),
        "epoch21_runtime": _record(Path(epoch21.__file__)),
        "frozen_inputs": {str(item["role"]): {k: item[k] for k in ("path", "sha256", "size_bytes")} for item in frozen},
    }


def _validated_epoch21_decision() -> tuple[
    dict[str, Any], dict[str, Any], list[str], dict[str, Any], dict[str, Any]
]:
    """Verify the direct predecessor closure without recursively locking history."""

    receipt = _load_json(EPOCH21_ROOT / "plan-step-receipt.json", "epoch-21 receipt")
    terminal = _load_json(EPOCH21_ROOT / "terminal.json", "epoch-21 terminal")
    lock = _load_json(EPOCH21_ROOT / "runtime-lock.json", "epoch-21 runtime lock")
    if receipt != terminal:
        raise Epoch22QualityAdoptionError("epoch-21 terminal mirrors differ")
    if (
        receipt.get("state") != "waiting"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("aggregate_semantic_model_call_count") != 3
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_accounting_complete") is not True
        or receipt.get("aggregate_accounting_complete") is not True
        or lock.get("schema_version")
        != "pif_canonical_v31_single_message_quality_recovery_lock_v1"
        or lock.get("thread_id") != THREAD_ID
        or lock.get("plan_epoch") != 21
        or lock.get("step_id") != "canonical_v31_epoch21_single_message_quality_recovery_v21"
        or lock.get("new_model_call_cap") != 1
        or lock.get("aggregate_model_call_cap") != 3
        or lock.get("semantic_retry_count") != 0
        or lock.get("production_mutated") is not False
        or lock.get("holdout_authorized") is not False
    ):
        raise Epoch22QualityAdoptionError("epoch-21 terminal or runtime contract drifted")
    paths = epoch21._turn_paths(EPOCH21_ROOT)  # noqa: SLF001
    required_lock_records = {
        "pool": EPOCH21_ROOT / "shared-witness-pool.private.json",
        "mapping": EPOCH21_ROOT / "private-witness-mapping.private.json",
        "variant": paths["variant"],
        "base_instructions": paths["base"],
        "prompt": paths["prompt"],
        "schema": paths["schema"],
        "capacity_policy": EPOCH21_ROOT / "capacity-policy.json",
        "attempt_spec": EPOCH21_ROOT / "attempt-spec.json",
    }
    for role, path in required_lock_records.items():
        if lock.get(role) != _record(path):
            raise Epoch22QualityAdoptionError(f"epoch-21 direct manifest drifted: {role}")
    records = receipt.get("records")
    if not isinstance(records, Mapping):
        raise Epoch22QualityAdoptionError("epoch-21 receipt records are missing")
    required_receipt_records = {
        "runtime_lock": EPOCH21_ROOT / "runtime-lock.json",
        "pool": required_lock_records["pool"],
        "mapping": required_lock_records["mapping"],
        "variant": paths["variant"],
        "base_instructions": paths["base"],
        "prompt": paths["prompt"],
        "schema": paths["schema"],
        "capacity_policy": required_lock_records["capacity_policy"],
        "attempt_spec": required_lock_records["attempt_spec"],
        "launch_receipt": EPOCH21_ROOT / "launch-receipt.json",
        "capacity": paths["capacity"],
        "sidecar": paths["sidecar"],
        "output": paths["output"],
    }
    for role, path in required_receipt_records.items():
        if records.get(role) != _record(path):
            raise Epoch22QualityAdoptionError(f"epoch-21 receipt lineage drifted: {role}")
    if records.get("consensus") is not None or records.get("score") is not None:
        raise Epoch22QualityAdoptionError("epoch-21 waiting receipt unexpectedly authorizes quality")
    epoch21._validate_launch(EPOCH21_ROOT)  # noqa: SLF001
    epoch21._validate_capacity(paths["capacity"], EPOCH21_ROOT)  # noqa: SLF001
    try:
        output, sidecar = judge._validate_completed_checkpoint(  # noqa: SLF001
            raw_output_path=paths["output"],
            sidecar_path=paths["sidecar"],
            prompt=paths["prompt"].read_text(encoding="utf-8"),
            schema=_load_json(paths["schema"], "epoch-21 schema"),
            base_instructions=paths["base"].read_text(encoding="utf-8"),
            model=epoch21.MODEL,
            reasoning_effort=epoch21.EFFORT,
        )
    except judge.JudgeArtifactError as exc:
        raise Epoch22QualityAdoptionError("epoch-21 completed checkpoint drifted") from exc
    usage = sidecar.get("usage")
    aggregate = receipt.get("aggregate_usage")
    if (
        usage != receipt.get("new_usage")
        or sidecar.get("thread_total_usage") != usage
        or not isinstance(usage, Mapping)
        or not isinstance(aggregate, Mapping)
        or set(usage) != set(USAGE_FIELDS)
        or set(aggregate) != set(USAGE_FIELDS)
        or any(
            aggregate[field]
            != epoch21.EXPECTED_PREDECESSOR_USAGE[field] + usage[field]
            for field in USAGE_FIELDS
        )
        or aggregate["total_tokens"] > epoch21.AGGREGATE_TOTAL_TOKEN_CAP
    ):
        raise Epoch22QualityAdoptionError("epoch-21 measured accounting drifted")
    variant = _load_json(paths["variant"], "epoch-21 variant")
    errors = judge.validate_judge_output(output, variant)
    pool = _load_json(required_lock_records["pool"], "epoch-21 pool")
    mapping = _load_json(required_lock_records["mapping"], "epoch-21 mapping")
    return dict(output), dict(sidecar), errors, pool, mapping


def freeze(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    root = root.resolve()
    if root != DEFAULT_ROOT.resolve():
        raise Epoch22QualityAdoptionError("epoch-22 output root differs from the frozen contract")
    with _process_lock(root):
        contract = load_contract()
        path = root / "runtime-lock.json"
        if path.is_file():
            return verify_runtime_lock(root)
        extras = [item for item in root.iterdir() if item.name != PROCESS_LOCK_NAME]
        if extras:
            raise Epoch22QualityAdoptionError("unfrozen epoch-22 root is not empty")
        payload = {
            "schema_version": RUNTIME_LOCK_VERSION,
            "thread_id": THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "frozen_at": _now_iso(),
            "semantic_model_call_cap": 0,
            "semantic_retry_cap": 0,
            "records": _runtime_records(contract),
            "production_mutation_allowed": False,
            "holdout_authorized": False,
        }
        _write_immutable_json(path, payload)
        return verify_runtime_lock(root)


def verify_runtime_lock(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    root = root.resolve()
    contract = load_contract()
    payload = _load_json(root / "runtime-lock.json", "epoch-22 runtime lock")
    if (
        payload.get("schema_version") != RUNTIME_LOCK_VERSION
        or payload.get("thread_id") != THREAD_ID
        or payload.get("plan_epoch") != PLAN_EPOCH
        or payload.get("step_id") != STEP_ID
        or payload.get("semantic_model_call_cap") != 0
        or payload.get("semantic_retry_cap") != 0
        or payload.get("records") != _runtime_records(contract)
        or payload.get("production_mutation_allowed") is not False
        or payload.get("holdout_authorized") is not False
    ):
        raise Epoch22QualityAdoptionError("epoch-22 runtime lock drifted")
    return payload


def _decision(root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    del root
    output, _sidecar, errors, pool, mapping = _validated_epoch21_decision()
    if errors != SOURCE_VALIDATION_ERRORS:
        raise Epoch22QualityAdoptionError("epoch-21 validation failure set drifted")
    contract = load_contract()
    variant = _load_json(epoch21._turn_paths(EPOCH21_ROOT)["variant"], "epoch-21 variant")  # noqa: SLF001
    projected, audit = project_output(output, variant, contract["directive"])
    consensus = epoch21._consensus_from_output(pool, projected)  # noqa: SLF001
    predecessor = _load_json(EPOCH21_ROOT / "terminal.json", "epoch-21 terminal")
    usage = predecessor["aggregate_usage"]
    accounting = {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 3,
        "usage": {field: int(usage[field]) for field in USAGE_FIELDS},
    }
    score = epoch20.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        accounting=accounting,
        adjudication_call_count=1,
    )
    if (
        score.get("passed") is not False
        or score.get("failed_checks") != EXPECTED_FAILED_CHECKS
        or score.get("candidate_strict_full_field_macro_f1") != 0.344287
        or score.get("baseline_strict_full_field_macro_f1") != 0.895374
        or score.get("candidate_noninferiority_delta") != -0.551087
        or score.get("exact_evidence_rate") != 1.0
        or score.get("production_amortized_total_token_ratio") != 0.2037974349024075
    ):
        raise Epoch22QualityAdoptionError("epoch-22 recomputed quality decision drifted")
    return projected, audit, consensus, score


def _artifact_records(root: Path) -> dict[str, Any]:
    paths = {
        "runtime_lock": root / "runtime-lock.json",
        "epoch21_terminal": EPOCH21_ROOT / "terminal.json",
        "epoch21_output": epoch21._turn_paths(EPOCH21_ROOT)["output"],  # noqa: SLF001
        "epoch21_sidecar": epoch21._turn_paths(EPOCH21_ROOT)["sidecar"],  # noqa: SLF001
        "projected_output": root / "projected-output.private.json",
        "projection_audit": root / "projection-audit.json",
        "consensus": root / "consensus.private.json",
        "score": root / "shared-reference-score.json",
    }
    return {role: _record(path) for role, path in paths.items()}


def _build_receipt(root: Path, score: Mapping[str, Any], predecessor: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": "rejected",
        "terminal_reason": "epoch22_quality_rejected_after_nonsemantic_exact_span_projection",
        "terminal_at": _now_iso(),
        "predecessor_semantic_model_call_count": 3,
        "new_semantic_model_call_cap": 0,
        "new_semantic_model_call_count": 0,
        "aggregate_semantic_model_call_count": 3,
        "semantic_retry_count": 0,
        "new_usage_status": "complete",
        "new_accounting_complete": True,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "aggregate_usage_status": "complete",
        "aggregate_accounting_complete": True,
        "aggregate_usage": predecessor["aggregate_usage"],
        "projection_count": 2,
        "semantic_fields_changed": False,
        "development_quality_passed": False,
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "judge_tokens_included_in_production_formula": False,
        "production_amortized_total_token_ratio": score["production_amortized_total_token_ratio"],
        "candidate_strict_full_field_macro_f1": score["candidate_strict_full_field_macro_f1"],
        "baseline_strict_full_field_macro_f1": score["baseline_strict_full_field_macro_f1"],
        "candidate_noninferiority_delta": score["candidate_noninferiority_delta"],
        "exact_evidence_rate": score["exact_evidence_rate"],
        "failed_checks": score["failed_checks"],
        "records": _artifact_records(root),
        "next_action": "prepare_one_materially_distinct_full_canonical_extraction_canary_no_field_repair",
        "privacy": "sanitized metrics counts hashes and failure class only",
    }


def run(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    root = root.resolve()
    with _process_lock(root):
        if (root / "plan-step-receipt.json").is_file() or (root / "terminal.json").is_file():
            return verify_receipt(root, acquire_lock=False)
        verify_runtime_lock(root)
        projected, audit, consensus, score = _decision(root)
        _write_immutable_json(root / "projected-output.private.json", projected)
        _write_immutable_json(root / "projection-audit.json", audit)
        _write_immutable_json(root / "consensus.private.json", consensus)
        _write_immutable_json(root / "shared-reference-score.json", score)
        predecessor = _load_json(EPOCH21_ROOT / "terminal.json", "epoch-21 terminal")
        receipt = _build_receipt(root, score, predecessor)
        _write_immutable_json(root / "plan-step-receipt.json", receipt)
        _write_immutable_json(root / "terminal.json", receipt)
        return verify_receipt(root, acquire_lock=False)


def verify_receipt(root: Path = DEFAULT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    root = root.resolve()

    def verify() -> dict[str, Any]:
        verify_runtime_lock(root)
        receipt = _load_json(root / "plan-step-receipt.json", "epoch-22 receipt")
        terminal = _load_json(root / "terminal.json", "epoch-22 terminal")
        if receipt != terminal:
            raise Epoch22QualityAdoptionError("epoch-22 terminal mirrors differ")
        projected, audit, consensus, score = _decision(root)
        expected_artifacts = {
            root / "projected-output.private.json": projected,
            root / "projection-audit.json": audit,
            root / "consensus.private.json": consensus,
            root / "shared-reference-score.json": score,
        }
        for path, expected in expected_artifacts.items():
            if _load_json(path, path.name) != expected:
                raise Epoch22QualityAdoptionError(f"epoch-22 artifact drifted: {path.name}")
        predecessor = _load_json(EPOCH21_ROOT / "terminal.json", "epoch-21 terminal")
        expected = _build_receipt(root, score, predecessor)
        expected["terminal_at"] = receipt.get("terminal_at")
        if receipt != expected:
            raise Epoch22QualityAdoptionError("epoch-22 receipt drifted")
        return receipt

    if acquire_lock:
        with _process_lock(root):
            return verify()
    return verify()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        result = freeze(args.output_dir)
        print(json.dumps({"runtime_lock_sha256": _sha256_file(args.output_dir / "runtime-lock.json"), "verified": bool(result)}))
    elif args.command == "run":
        freeze(args.output_dir)
        result = run(args.output_dir)
        print(json.dumps({"state": result["state"], "terminal_reason": result["terminal_reason"]}))
    else:
        result = verify_receipt(args.output_dir)
        print(json.dumps({"state": result["state"], "verified": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
