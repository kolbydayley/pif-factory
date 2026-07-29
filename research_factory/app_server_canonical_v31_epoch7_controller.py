from __future__ import annotations

import argparse
import asyncio
import copy
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
from datetime import datetime, timedelta, timezone
from math import gcd
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix as matrix
from . import app_server_canonical_v31_development_matrix_runtime as extraction_runtime
from . import app_server_thread_supervisor as supervisor


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_EPOCH = 7
STEP_ID = "canonical_v31_six_arm_extraction_runtime_epoch_7"

CONTROLLER_CONTRACT_VERSION = "pif_canonical_v31_epoch7_controller_contract_v1"
CONTROLLER_EXECUTION_VERSION = "pif_canonical_v31_epoch7_controller_execution_v1"
QUALITY_HANDOFF_VERSION = "pif_canonical_v31_epoch7_quality_handoff_v1"
QUALITY_INPUT_VERSION = "pif_canonical_v31_quality_runtime_input_v1"
QUALITY_EVIDENCE_CONTRACT_VERSION = (
    "pif_canonical_v31_quality_runtime_evidence_contract_v1"
)
QUALITY_TURN_REQUEST_VERSION = "pif_canonical_v31_quality_turn_request_v1"
QUALITY_TURN_DECISIONS_VERSION = "pif_canonical_v31_quality_turn_decisions_v1"
QUALITY_SUPPORT_CONSENSUS_VERSION = (
    "pif_canonical_v31_quality_support_consensus_v1"
)
QUALITY_ALIGNMENT_CONSENSUS_VERSION = (
    "pif_canonical_v31_quality_alignment_consensus_v1"
)
QUALITY_EVIDENCE_RECEIPT_VERSION = (
    "pif_canonical_v31_quality_runtime_evidence_receipt_v1"
)
QUALITY_SCORING_MANIFEST_VERSION = "pif_canonical_v31_quality_scoring_manifest_v1"
QUALITY_RECOMPUTED_SCORE_VERSION = "pif_canonical_v31_recomputed_quality_score_v1"
QUALITY_COST_AUTHORITY_VERSION = "pif_canonical_v31_quality_cost_authority_v1"
QUALITY_TERMINAL_RECEIPT_VERSION = "pif_canonical_v31_quality_terminal_receipt_v1"
DEVELOPMENT_SELECTION_POLICY_VERSION = (
    "pif_canonical_v31_development_selection_policy_v1"
)
QUALITY_PRODUCTION_COST_FORMULA = (
    "(ceil(measured_extraction_tokens*baseline_segments/development_segments)+"
    "production_amortized_context_tokens)/(baseline_measured_total_tokens+"
    "production_amortized_context_tokens)"
)

CONTRACT_FILENAME = "controller-contract.json"
PREFLIGHT_FILENAME = "dry-preflight.json"
PRECOMMIT_FILENAME = "precommit.json"
FUTURE_BINDING_FILENAME = "future-plan-binding.json"
DIRECTIVE_FILENAME = "extraction-directive.json"
SEMANTIC_PLAN_FILENAME = "semantic-plan.json"
EXECUTION_RECEIPT_FILENAME = "controller-execution-receipt.json"
QUALITY_HANDOFF_FILENAME = "quality-handoff.json"
QUALITY_EVIDENCE_RECEIPT_FILENAME = "quality-evidence-receipt.json"
QUALITY_COST_AUTHORITY_FILENAME = "quality-cost-authority.json"
QUALITY_SCORING_MANIFEST_FILENAME = "quality-scoring-manifest.json"
QUALITY_RECOMPUTED_SCORE_FILENAME = "quality-recomputed-score.json"
QUALITY_TERMINAL_RECEIPT_FILENAME = "quality-terminal-receipt.json"
SELECTION_POLICY_FILENAME = "development-selection-policy.json"
WRITER_LOCK_FILENAME = ".epoch7-controller-writer.lock"

_AUTHORIZATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
_TRUTH_CONDITIONAL_CHECKLIST_FIELDS = (
    "actor",
    "attribution",
    "causal_mechanism",
    "certainty",
    "event_boundary",
    "event_type",
    "evidence",
    "metric",
    "negation",
    "reported_actor",
    "speaker",
    "stance",
    "target",
    "temporal_horizon",
    "unsupported_inference",
)
_QUALITY_TURN_ORDER = (
    ("support_first", "ab"),
    ("support_first", "ba"),
    ("alignment", "ab"),
    ("alignment", "ba"),
)
_QUALITY_TURN_ARTIFACT_ROLES = (
    "capacity_request",
    "capacity_measurement",
    "capacity_admission",
    "request",
    "prompt",
    "base_instructions",
    "output_schema",
    "raw_output",
    "parsed_decisions",
    "sidecar",
    "terminal",
)


class Epoch7ControllerError(RuntimeError):
    pass


class Epoch7ControllerLockUnavailable(Epoch7ControllerError):
    """Another native controller invocation owns the immutable attempt root."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _safe_path(path: Path, *, project_root: Path, label: str) -> Path:
    project = project_root.expanduser().resolve()
    candidate = Path(os.path.abspath(os.path.expanduser(str(path))))
    try:
        candidate.relative_to(project)
    except ValueError as exc:
        raise Epoch7ControllerError(f"{label} is outside the project root") from exc
    cursor = candidate
    while cursor != project:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            mode = None
        except OSError as exc:
            raise Epoch7ControllerError(f"{label} path metadata is unavailable") from exc
        if mode is not None and stat.S_ISLNK(mode):
            raise Epoch7ControllerError(f"{label} traverses a symlink")
        cursor = cursor.parent
    return candidate


def _validate_controller_directory(root: Path) -> None:
    required = {
        CONTRACT_FILENAME,
        PREFLIGHT_FILENAME,
        PRECOMMIT_FILENAME,
        FUTURE_BINDING_FILENAME,
        DIRECTIVE_FILENAME,
        SEMANTIC_PLAN_FILENAME,
        SELECTION_POLICY_FILENAME,
    }
    allowed = required | {
        EXECUTION_RECEIPT_FILENAME,
        QUALITY_HANDOFF_FILENAME,
        WRITER_LOCK_FILENAME,
    }
    try:
        entries = list(root.iterdir())
    except OSError as exc:
        raise Epoch7ControllerError("controller root is unavailable") from exc
    names = {entry.name for entry in entries}
    if not required.issubset(names) or not names.issubset(allowed):
        raise Epoch7ControllerError("controller root artifact set drifted")
    for entry in entries:
        try:
            mode = entry.lstat().st_mode
        except OSError as exc:
            raise Epoch7ControllerError("controller artifact metadata is unavailable") from exc
        if not stat.S_ISREG(mode):
            raise Epoch7ControllerError("controller root contains a non-file artifact")


def _record(path: Path, *, allowed_root: Path | None = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if allowed_root is not None:
        try:
            resolved.relative_to(allowed_root.expanduser().resolve())
        except ValueError as exc:
            raise Epoch7ControllerError("artifact record escaped its allowed root") from exc
    try:
        info = resolved.stat()
    except OSError as exc:
        raise Epoch7ControllerError(f"required artifact is unavailable: {resolved}") from exc
    if not stat.S_ISREG(info.st_mode) or resolved.is_symlink():
        raise Epoch7ControllerError(f"required artifact is not a real file: {resolved}")
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
        "size": info.st_size,
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    allowed_root: Path | None = None,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size"}:
        raise Epoch7ControllerError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise Epoch7ControllerError(f"{label} record fields are malformed")
    path = Path(path_value).expanduser().resolve()
    if _record(path, allowed_root=allowed_root) != dict(value):
        raise Epoch7ControllerError(f"{label} record drifted")
    return path


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Epoch7ControllerError(f"{label} is malformed") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, dict):
        raise Epoch7ControllerError(f"{label} is not an object")
    return value


def _load_array(path: Path, *, label: str) -> list[Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, list):
        raise Epoch7ControllerError(f"{label} is not an array")
    return value


def _write_immutable_json(path: Path, value: Any) -> dict[str, Any]:
    raw = _canonical_json(value).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or path.read_bytes() != raw:
            raise Epoch7ControllerError(f"immutable artifact drifted: {path}")
        return _record(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return _record(path)


class _ControllerWriterLock:
    def __init__(self, root: Path) -> None:
        self.path = root / WRITER_LOCK_FILENAME
        self.handle: Any = None

    def __enter__(self) -> "_ControllerWriterLock":
        if self.path.exists():
            try:
                mode = self.path.lstat().st_mode
            except OSError as exc:
                raise Epoch7ControllerError(
                    "controller writer lock metadata is unavailable"
                ) from exc
            if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                raise Epoch7ControllerError("controller writer lock path is unsafe")
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise Epoch7ControllerError("controller writer lock is unavailable") from exc
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise Epoch7ControllerError("controller writer lock is not a file")
            self.handle = os.fdopen(descriptor, "r+", closefd=True)
            descriptor = -1
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            raise Epoch7ControllerLockUnavailable(
                "another epoch-7 controller invocation owns this root"
            ) from exc
        except Exception:
            if self.handle is not None:
                self.handle.close()
                self.handle = None
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        return self

    def __exit__(self, *_args: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise Epoch7ControllerError(f"{label} is absent")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Epoch7ControllerError(f"{label} is malformed") from exc
    if parsed.tzinfo is None:
        raise Epoch7ControllerError(f"{label} lacks a timezone")
    return parsed.astimezone(timezone.utc)


def _authorization_id(value: Any) -> str:
    if not isinstance(value, str) or _AUTHORIZATION_ID.fullmatch(value) is None:
        raise Epoch7ControllerError("operator authorization ID is malformed")
    return value


def _usage(value: Any, *, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != set(_USAGE_FIELDS):
        raise Epoch7ControllerError(f"{label} usage is malformed")
    normalized: dict[str, int] = {}
    for field in _USAGE_FIELDS:
        observed = value.get(field)
        if isinstance(observed, bool) or not isinstance(observed, int) or observed < 0:
            raise Epoch7ControllerError(f"{label} {field} is malformed")
        normalized[field] = observed
    if normalized["total_tokens"] != (
        normalized["input_tokens"] + normalized["output_tokens"]
    ):
        raise Epoch7ControllerError(f"{label} total token accounting drifted")
    if normalized["cached_input_tokens"] > normalized["input_tokens"]:
        raise Epoch7ControllerError(f"{label} cached input accounting drifted")
    return normalized


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(value[field]) for value in values) for field in _USAGE_FIELDS}


def development_selection_policy() -> dict[str, Any]:
    """Return the ranking rule frozen before extraction and quality work."""

    payload = {
        "schema_version": DEVELOPMENT_SELECTION_POLICY_VERSION,
        "state": "frozen_before_extraction_or_quality_semantic_turns",
        "quality_terminal_schema_version": QUALITY_TERMINAL_RECEIPT_VERSION,
        "eligible_terminal_state": (
            "passed_development_quality_checkpoint_selection_not_frozen"
        ),
        "eligible_arm_requirement": "every_frozen_quality_and_cost_check_passed",
        "ranking_keys": [
            {
                "field": "strict_full_field_macro",
                "direction": "descending",
            },
            {
                "field": "production_amortized_total_token_ratio",
                "direction": "ascending",
            },
        ],
        "quality_precedes_cost": True,
        "variant_id_is_not_a_semantic_or_tie_breaking_signal": True,
        "exact_metric_tie_policy": "reject_no_unique_development_winner",
        "all_six_arms_must_be_present": True,
        "winner_configuration_must_bind": [
            "batch_size",
            "thread_mode",
            "model",
            "effort",
            "prompt_hashes",
            "base_instruction_hashes",
            "output_schema_hashes",
            "canonical_adapter_hash",
            "context_control_overlay_hash",
            "instruction_source_contract_hash",
            "canonical_field_partition_hashes",
            "extraction_receipt",
            "quality_terminal",
            "quality_evidence",
            "quality_cost_authority",
            "shared_reference_seed",
        ],
        "reported_scalar_metrics_alone_sufficient": False,
        "quality_selection_is_deterministic_only": True,
        "semantic_model_calls_authorized": False,
        "holdout_inspection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "policy": payload,
        "policy_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
    }


def quality_runtime_evidence_contract() -> dict[str, Any]:
    """Return the exact non-forgeable boundary for the external quality runtime."""

    evaluator = matrix.quality_evaluator_contract()
    evaluator_sha = _sha256_bytes(_canonical_json(evaluator).encode("ascii"))
    support_schema = pointwise_support_decision_schema()
    alignment_schema = alignment_decision_schema()
    payload = {
        "schema_version": QUALITY_EVIDENCE_CONTRACT_VERSION,
        "quality_input_schema_version": QUALITY_INPUT_VERSION,
        "quality_evidence_receipt_schema_version": QUALITY_EVIDENCE_RECEIPT_VERSION,
        "quality_turn_request_schema_version": QUALITY_TURN_REQUEST_VERSION,
        "quality_evaluator_contract_sha256": evaluator_sha,
        "exact_semantic_turn_count": 4,
        "semantic_turn_order": [
            {"stage": stage, "orientation": orientation}
            for stage, orientation in _QUALITY_TURN_ORDER
        ],
        "one_shared_augmented_reference_for_all_six_arms": True,
        "identical_opaque_case_order_across_all_turns": True,
        "balanced_ab_ba_permutations_required": True,
        "abstention_enabled_per_turn": True,
        "support_stage_contract": {
            "pointwise_source_support_before_alignment": True,
            "exact_source_evidence_required_for_supported": True,
            "material_claim_entailment_required": True,
            "support_receipts_frozen_before_alignment": True,
            "decision_schema": support_schema,
            "decision_schema_sha256": _sha256_bytes(
                _canonical_json(support_schema).encode("ascii")
            ),
            "balanced_permutation_reconciliation": (
                "exact_verdict_entailment_and_evidence_agreement_else_abstain"
            ),
        },
        "alignment_uses_only_frozen_support_positive_witnesses": True,
        "alignment_decision_schema": alignment_schema,
        "alignment_decision_schema_sha256": _sha256_bytes(
            _canonical_json(alignment_schema).encode("ascii")
        ),
        "truth_conditional_checklist_fields": list(
            _TRUTH_CONDITIONAL_CHECKLIST_FIELDS
        ),
        "truth_conditional_checklist_fields_sha256": _sha256_bytes(
            _canonical_json(list(_TRUTH_CONDITIONAL_CHECKLIST_FIELDS)).encode(
                "ascii"
            )
        ),
        "truth_conditional_checklist_decisions": [
            "same",
            "different",
            "abstain",
        ],
        "all_15_checklist_rows_required_exactly_once_per_alignment_decision": True,
        "free_form_mismatch_field_generation_allowed": False,
        "unsupported_inference_must_bind_frozen_support_verdict": True,
        "ab_ba_consensus_rule": (
            "use_only_agreed_semantic_decisions; observable disagreement abstains"
        ),
        "confidence_routing_allowed": False,
        "majority_voting_allowed": False,
        "chain_of_thought_expansion_as_decision_rule_allowed": False,
        "adjudication_in_this_four_turn_runtime_authorized": False,
        "observable_disagreement_requires_abstention_or_separate_bounded_directive": (
            True
        ),
        "required_record_types_per_turn": list(_QUALITY_TURN_ARTIFACT_ROLES),
        "unique_nonempty_thread_and_turn_ids_required": True,
        "raw_llm_decisions_required": True,
        "deterministic_score_recomputation_from_raw_decisions_required": True,
        "quality_scoring_manifest_schema_version": QUALITY_SCORING_MANIFEST_VERSION,
        "recomputed_quality_score_schema_version": QUALITY_RECOMPUTED_SCORE_VERSION,
        "quality_cost_authority_schema_version": QUALITY_COST_AUTHORITY_VERSION,
        "quality_terminal_receipt_schema_version": QUALITY_TERMINAL_RECEIPT_VERSION,
        "every_support_positive_unordered_pair_within_case_required": True,
        "field_reference_units": (
            "union_find_over_llm_same_decisions_with_different_contradiction_rejection"
        ),
        "shared_reference_system_ids": None,
        "reported_scalar_metrics_never_sufficient_authority": True,
        "prompt_schema_base_and_output_hashes_reverified": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "capacity_admission_required_before_each_turn": True,
        "complete_input_cached_output_reasoning_wall_telemetry_required": True,
        "semantic_retry_count": 0,
        "caller_supplied_client_or_capacity_authority_allowed": False,
        "api_key_auth_allowed": False,
        "raw_session_token_auth_allowed": False,
        "codex_exec_allowed": False,
        "embeddings_used": False,
        "deterministic_semantic_matching": False,
        "semantic_defaults": {},
        "semantic_pruning": False,
        "semantic_relabeling": False,
        "production_cost_formula": QUALITY_PRODUCTION_COST_FORMULA,
        "judge_usage_is_separate_development_qa_overhead": True,
        "judge_usage_in_production_cost_formula": False,
        "strict_full_field_macro_threshold": matrix.QUALITY_THRESHOLD,
        "noninferiority_margin": matrix.NONINFERIORITY_MARGIN,
        "production_amortized_total_token_ratio_threshold": (
            matrix.TOKEN_RATIO_THRESHOLD
        ),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "contract": payload,
        "contract_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
        "quality_evaluator_contract": evaluator,
        "quality_evaluator_contract_sha256": evaluator_sha,
    }


def truth_conditional_checklist_schema() -> dict[str, Any]:
    """Return the app-server-compatible exact 15-row alignment schema."""

    return {
        "type": "array",
        "minItems": len(_TRUTH_CONDITIONAL_CHECKLIST_FIELDS),
        "maxItems": len(_TRUTH_CONDITIONAL_CHECKLIST_FIELDS),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["field", "decision", "rationale"],
            "properties": {
                "field": {
                    "type": "string",
                    "enum": list(_TRUTH_CONDITIONAL_CHECKLIST_FIELDS),
                },
                "decision": {
                    "type": "string",
                    "enum": ["same", "different", "abstain"],
                },
                "rationale": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 800,
                },
            },
        },
    }


def pointwise_support_decision_schema() -> dict[str, Any]:
    """Return the exact source-support decision schema for one orientation."""

    return {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "witness_id",
                "verdict",
                "material_claim_entailment",
                "evidence_spans",
                "rationale",
            ],
            "properties": {
                "witness_id": {"type": "string", "minLength": 1},
                "verdict": {
                    "type": "string",
                    "enum": ["supported", "unsupported", "abstain"],
                },
                "material_claim_entailment": {
                    "type": "string",
                    "enum": [
                        "entailed",
                        "contradicted",
                        "not_established",
                        "abstain",
                    ],
                },
                "evidence_spans": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["text", "start", "end"],
                        "properties": {
                            "text": {"type": "string", "minLength": 1},
                            "start": {"type": "integer", "minimum": 0},
                            "end": {"type": "integer", "minimum": 1},
                        },
                    },
                },
                "rationale": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 800,
                },
            },
        },
    }


def validate_pointwise_support_decisions(
    value: Any,
    *,
    expected_witness_ids: Sequence[str],
    source_by_witness: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Validate exact evidence and ordering without changing an LLM verdict."""

    expected = list(expected_witness_ids)
    if (
        not expected
        or len(expected) != len(set(expected))
        or any(not isinstance(witness_id, str) or not witness_id for witness_id in expected)
        or set(source_by_witness) != set(expected)
        or any(not isinstance(source_by_witness[witness_id], str) for witness_id in expected)
    ):
        raise Epoch7ControllerError("pointwise support witness contract is malformed")
    if not isinstance(value, list) or len(value) != len(expected):
        raise Epoch7ControllerError("pointwise support decision count drifted")

    normalized: list[dict[str, Any]] = []
    for index, (row, expected_id) in enumerate(zip(value, expected)):
        if not isinstance(row, Mapping) or set(row) != {
            "witness_id",
            "verdict",
            "material_claim_entailment",
            "evidence_spans",
            "rationale",
        }:
            raise Epoch7ControllerError(
                f"pointwise support decision {index} is malformed"
            )
        verdict = row.get("verdict")
        entailment = row.get("material_claim_entailment")
        rationale = row.get("rationale")
        spans = row.get("evidence_spans")
        if (
            row.get("witness_id") != expected_id
            or verdict not in {"supported", "unsupported", "abstain"}
            or entailment
            not in {"entailed", "contradicted", "not_established", "abstain"}
            or not isinstance(rationale, str)
            or not 1 <= len(rationale) <= 800
            or not isinstance(spans, list)
        ):
            raise Epoch7ControllerError(
                f"pointwise support decision {index} drifted"
            )
        if (
            (verdict == "supported" and entailment != "entailed")
            or (
                verdict == "unsupported"
                and entailment not in {"contradicted", "not_established"}
            )
            or (verdict == "abstain" and entailment != "abstain")
            or (verdict == "supported" and not spans)
        ):
            raise Epoch7ControllerError(
                f"pointwise support decision {index} entailment contract drifted"
            )

        source = source_by_witness[expected_id]
        normalized_spans: list[dict[str, Any]] = []
        observed_ranges: set[tuple[int, int]] = set()
        for span_index, span in enumerate(spans):
            if not isinstance(span, Mapping) or set(span) != {"text", "start", "end"}:
                raise Epoch7ControllerError(
                    f"pointwise support decision {index} evidence {span_index} is malformed"
                )
            text_value = span.get("text")
            start = span.get("start")
            end = span.get("end")
            if (
                not isinstance(text_value, str)
                or not text_value
                or isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or not 0 <= start < end <= len(source)
                or source[start:end] != text_value
                or (start, end) in observed_ranges
            ):
                raise Epoch7ControllerError(
                    f"pointwise support decision {index} evidence {span_index} is not exact"
                )
            observed_ranges.add((start, end))
            normalized_spans.append(
                {"text": text_value, "start": start, "end": end}
            )
        normalized.append(
            {
                "witness_id": expected_id,
                "verdict": verdict,
                "material_claim_entailment": entailment,
                "evidence_spans": normalized_spans,
                "rationale": rationale,
            }
        )
    return normalized


def reconcile_balanced_support_decisions(
    ab_value: Any,
    ba_value: Any,
    *,
    expected_witness_ids: Sequence[str],
    source_by_witness: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Project exact AB/BA agreement; every observable difference abstains."""

    ab = validate_pointwise_support_decisions(
        ab_value,
        expected_witness_ids=expected_witness_ids,
        source_by_witness=source_by_witness,
    )
    ba = validate_pointwise_support_decisions(
        ba_value,
        expected_witness_ids=expected_witness_ids,
        source_by_witness=source_by_witness,
    )
    reconciled: list[dict[str, Any]] = []
    for left, right in zip(ab, ba):
        agreed = (
            left["verdict"] == right["verdict"]
            and left["material_claim_entailment"]
            == right["material_claim_entailment"]
            and left["evidence_spans"] == right["evidence_spans"]
        )
        reconciled.append(
            {
                "witness_id": left["witness_id"],
                "verdict": left["verdict"] if agreed else "abstain",
                "material_claim_entailment": (
                    left["material_claim_entailment"] if agreed else "abstain"
                ),
                "evidence_spans": left["evidence_spans"] if agreed else [],
                "reconciliation": (
                    "balanced_permutations_agreed"
                    if agreed
                    else "observable_permutation_disagreement_abstained"
                ),
            }
        )
    return reconciled


def validate_truth_conditional_checklist(
    value: Any,
    *,
    left_support_verdict: str = "supported",
    right_support_verdict: str = "supported",
) -> list[dict[str, str]]:
    """Validate schema/order only; semantic checklist decisions remain LLM-owned."""

    support_verdicts = {"supported", "unsupported", "abstain"}
    if (
        left_support_verdict not in support_verdicts
        or right_support_verdict not in support_verdicts
    ):
        raise Epoch7ControllerError("checklist support verdict is malformed")
    if left_support_verdict != "supported" or right_support_verdict != "supported":
        raise Epoch7ControllerError(
            "alignment checklist requires two frozen support-positive witnesses"
        )
    if not isinstance(value, list) or len(value) != len(
        _TRUTH_CONDITIONAL_CHECKLIST_FIELDS
    ):
        raise Epoch7ControllerError("truth-conditional checklist row count drifted")
    normalized: list[dict[str, str]] = []
    for index, (row, expected_field) in enumerate(
        zip(value, _TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    ):
        if not isinstance(row, Mapping) or set(row) != {
            "field",
            "decision",
            "rationale",
        }:
            raise Epoch7ControllerError(
                f"truth-conditional checklist row {index} is malformed"
            )
        decision = row.get("decision")
        rationale = row.get("rationale")
        if (
            row.get("field") != expected_field
            or decision not in {"same", "different", "abstain"}
            or not isinstance(rationale, str)
            or not 1 <= len(rationale) <= 800
        ):
            raise Epoch7ControllerError(
                f"truth-conditional checklist row {index} drifted"
            )
        normalized.append(
            {
                "field": expected_field,
                "decision": str(decision),
                "rationale": rationale,
            }
        )
    return normalized


def reconcile_balanced_checklists(
    ab_value: Any,
    ba_value: Any,
) -> list[dict[str, str]]:
    """Project observable agreement only; permutation disagreement abstains."""

    ab = validate_truth_conditional_checklist(ab_value)
    ba = validate_truth_conditional_checklist(ba_value)
    reconciled: list[dict[str, str]] = []
    for left, right in zip(ab, ba):
        agreed = left["decision"] == right["decision"]
        reconciled.append(
            {
                "field": left["field"],
                "decision": left["decision"] if agreed else "abstain",
                "reconciliation": (
                    "balanced_permutations_agreed"
                    if agreed
                    else "observable_permutation_disagreement_abstained"
                ),
            }
        )
    return reconciled


def alignment_decision_schema() -> dict[str, Any]:
    """Return the exact pooled-ID full-field alignment decision schema."""

    return {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "pair_id",
                "left_witness_id",
                "right_witness_id",
                "relation",
                "checklist",
                "rationale",
            ],
            "properties": {
                "pair_id": {"type": "string", "minLength": 1},
                "left_witness_id": {"type": "string", "minLength": 1},
                "right_witness_id": {"type": "string", "minLength": 1},
                "relation": {
                    "type": "string",
                    "enum": ["equivalent", "not_equivalent", "abstain"],
                },
                "checklist": truth_conditional_checklist_schema(),
                "rationale": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 800,
                },
            },
        },
    }


def validate_alignment_decisions(
    value: Any,
    *,
    expected_pairs: Sequence[Mapping[str, str]],
    frozen_support_verdicts: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Validate the pooled-ID partition without making a semantic alignment."""

    normalized_pairs: list[dict[str, str]] = []
    pair_ids: set[str] = set()
    witness_pairs: set[tuple[str, str]] = set()
    for index, pair in enumerate(expected_pairs):
        if not isinstance(pair, Mapping) or set(pair) != {
            "pair_id",
            "left_witness_id",
            "right_witness_id",
        }:
            raise Epoch7ControllerError(f"alignment pair {index} is malformed")
        normalized = {key: str(pair[key]) for key in pair}
        if (
            any(not value for value in normalized.values())
            or normalized["left_witness_id"] == normalized["right_witness_id"]
            or normalized["pair_id"] in pair_ids
            or (
                normalized["left_witness_id"],
                normalized["right_witness_id"],
            )
            in witness_pairs
            or frozen_support_verdicts.get(normalized["left_witness_id"])
            != "supported"
            or frozen_support_verdicts.get(normalized["right_witness_id"])
            != "supported"
        ):
            raise Epoch7ControllerError(f"alignment pair {index} contract drifted")
        pair_ids.add(normalized["pair_id"])
        witness_pairs.add(
            (normalized["left_witness_id"], normalized["right_witness_id"])
        )
        normalized_pairs.append(normalized)

    if not isinstance(value, list) or len(value) != len(normalized_pairs):
        raise Epoch7ControllerError("alignment decision count drifted")
    decisions: list[dict[str, Any]] = []
    for index, (row, expected) in enumerate(zip(value, normalized_pairs)):
        if not isinstance(row, Mapping) or set(row) != {
            "pair_id",
            "left_witness_id",
            "right_witness_id",
            "relation",
            "checklist",
            "rationale",
        }:
            raise Epoch7ControllerError(f"alignment decision {index} is malformed")
        relation = row.get("relation")
        rationale = row.get("rationale")
        if (
            any(row.get(key) != expected[key] for key in expected)
            or relation not in {"equivalent", "not_equivalent", "abstain"}
            or not isinstance(rationale, str)
            or not 1 <= len(rationale) <= 800
        ):
            raise Epoch7ControllerError(f"alignment decision {index} drifted")
        checklist = validate_truth_conditional_checklist(
            row.get("checklist"),
            left_support_verdict=frozen_support_verdicts[
                expected["left_witness_id"]
            ],
            right_support_verdict=frozen_support_verdicts[
                expected["right_witness_id"]
            ],
        )
        checklist_values = {item["decision"] for item in checklist}
        if (
            (relation == "equivalent" and checklist_values != {"same"})
            or (relation == "not_equivalent" and "different" not in checklist_values)
        ):
            raise Epoch7ControllerError(
                f"alignment decision {index} relation/checklist drifted"
            )
        decisions.append(
            {
                **expected,
                "relation": relation,
                "checklist": checklist,
                "rationale": rationale,
            }
        )
    return decisions


def reconcile_balanced_alignment_decisions(
    ab_value: Any,
    ba_value: Any,
    *,
    expected_pairs: Sequence[Mapping[str, str]],
    frozen_support_verdicts: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Reconcile only exact observable agreement; otherwise abstain."""

    ab = validate_alignment_decisions(
        ab_value,
        expected_pairs=expected_pairs,
        frozen_support_verdicts=frozen_support_verdicts,
    )
    ba = validate_alignment_decisions(
        ba_value,
        expected_pairs=expected_pairs,
        frozen_support_verdicts=frozen_support_verdicts,
    )
    reconciled: list[dict[str, Any]] = []
    for left, right in zip(ab, ba):
        checklist = reconcile_balanced_checklists(
            left["checklist"], right["checklist"]
        )
        agreed = (
            left["relation"] == right["relation"]
            and all(
                row["reconciliation"] == "balanced_permutations_agreed"
                for row in checklist
            )
        )
        reconciled.append(
            {
                "pair_id": left["pair_id"],
                "left_witness_id": left["left_witness_id"],
                "right_witness_id": left["right_witness_id"],
                "relation": left["relation"] if agreed else "abstain",
                "checklist": checklist,
                "reconciliation": (
                    "balanced_permutations_agreed"
                    if agreed
                    else "observable_permutation_disagreement_abstained"
                ),
            }
        )
    return reconciled


def _verify_quality_turn_evidence(
    value: Any,
    *,
    expected_stage: str,
    expected_orientation: str,
    quality_root: Path,
    quality_handoff_record: Mapping[str, Any],
    quality_evidence_contract_sha256: str,
    scoring_manifest_record: Mapping[str, Any],
    support_consensus_sha256: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "stage",
        "orientation",
        "model",
        "effort",
        "thread_id",
        "turn_id",
        "artifacts",
        "usage",
        "wall_elapsed_seconds",
    }:
        raise Epoch7ControllerError("quality turn evidence is malformed")
    model = value.get("model")
    effort = value.get("effort")
    thread_id = value.get("thread_id")
    turn_id = value.get("turn_id")
    wall = value.get("wall_elapsed_seconds")
    if (
        value.get("stage") != expected_stage
        or value.get("orientation") != expected_orientation
        or not isinstance(model, str)
        or not model
        or not isinstance(effort, str)
        or not effort
        or not isinstance(thread_id, str)
        or not thread_id
        or not isinstance(turn_id, str)
        or not turn_id
        or isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or not 0 <= float(wall)
    ):
        raise Epoch7ControllerError("quality turn identity or wall telemetry drifted")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(
        _QUALITY_TURN_ARTIFACT_ROLES
    ):
        raise Epoch7ControllerError("quality turn artifact set drifted")
    paths = {
        role: _verify_record(
            artifacts[role],
            label=f"quality turn {expected_stage}.{expected_orientation}.{role}",
            allowed_root=quality_root,
        )
        for role in _QUALITY_TURN_ARTIFACT_ROLES
    }

    admission = _load_object(paths["capacity_admission"], label="quality capacity admission")
    measurement = _load_object(
        paths["capacity_measurement"], label="quality capacity measurement"
    )
    if (
        admission.get("cleared_for_semantic_turn") is not True
        or admission.get("managed_chatgpt_auth_verified") is not True
        or admission.get("rate_limit_reached_type") is not None
        or measurement.get("managed_chatgpt_auth_verified") is not True
        or measurement.get("rate_limit_reached_type") is not None
    ):
        raise Epoch7ControllerError("quality capacity admission drifted")

    prompt_bytes = paths["prompt"].read_bytes()
    base_bytes = paths["base_instructions"].read_bytes()
    schema = _load_json(paths["output_schema"], label="quality output schema")
    schema_bytes = _canonical_json(schema).encode("ascii")
    output_bytes = paths["raw_output"].read_bytes()
    sidecar = _load_object(paths["sidecar"], label="quality sidecar")
    usage = _usage(value.get("usage"), label="quality turn")
    thread_total_usage = _usage(
        sidecar.get("thread_total_usage"), label="quality thread total"
    )
    protocol_sha = _record(
        matrix.adapter.codex_app_server.PROTOCOL_SCHEMA_PATH
    )["sha256"]
    required_sidecar = {
        "schema_version": "pif_codex_app_server_turn_v2",
        "state": "completed",
        "status": "completed",
        "client_version": matrix.adapter.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": matrix.adapter.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": protocol_sha,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "model": model,
        "effort": effort,
        "thread_id": thread_id,
        "turn_id": turn_id,
        "prompt_sha256": _sha256_bytes(prompt_bytes),
        "prompt_bytes": len(prompt_bytes),
        "base_instructions_sha256": _sha256_bytes(base_bytes),
        "base_instructions_bytes": len(base_bytes),
        "output_schema_sha256": _sha256_bytes(schema_bytes),
        "output_schema_bytes": len(schema_bytes),
        "usage_status": "measured",
        "usage_complete": True,
        "synthetic_debug_errors": False,
        "recovery_reran_model": False,
    }
    if any(sidecar.get(key) != expected for key, expected in required_sidecar.items()):
        raise Epoch7ControllerError("quality sidecar contract drifted")
    if sidecar.get("usage") != usage or not isinstance(
        sidecar.get("app_server_user_agent"), str
    ) or not sidecar.get("app_server_user_agent"):
        raise Epoch7ControllerError("quality sidecar usage or agent drifted")
    if thread_total_usage["total_tokens"] < usage["total_tokens"]:
        raise Epoch7ControllerError("quality thread accounting regressed")
    sidecar_wall = sidecar.get("wall_elapsed_seconds")
    if (
        isinstance(sidecar_wall, bool)
        or not isinstance(sidecar_wall, (int, float))
        or abs(float(sidecar_wall) - float(wall)) > 1e-9
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != paths["raw_output"].resolve()
    ):
        raise Epoch7ControllerError("quality sidecar wall or output path drifted")
    output_hashes = {_sha256_bytes(output_bytes)}
    if output_bytes.endswith(b"\n"):
        output_hashes.add(_sha256_bytes(output_bytes[:-1]))
    if sidecar.get("output_sha256") not in output_hashes:
        raise Epoch7ControllerError("quality sidecar output hash drifted")

    terminal = _load_object(paths["terminal"], label="quality turn terminal")
    if (
        terminal.get("state") != "completed"
        or terminal.get("thread_id") != thread_id
        or terminal.get("turn_id") != turn_id
        or terminal.get("semantic_model_call_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
    ):
        raise Epoch7ControllerError("quality turn terminal drifted")

    parsed = _load_object(paths["parsed_decisions"], label="quality parsed decisions")
    common = {
        "schema_version",
        "stage",
        "orientation",
        "case_order_sha256",
        "decisions",
    }
    if expected_stage == "support_first":
        if set(parsed) != common | {"expected_witness_ids", "source_by_witness"}:
            raise Epoch7ControllerError("quality support decision artifact is malformed")
        expected_ids = parsed.get("expected_witness_ids")
        sources = parsed.get("source_by_witness")
        if (
            parsed.get("schema_version") != QUALITY_TURN_DECISIONS_VERSION
            or parsed.get("stage") != expected_stage
            or parsed.get("orientation") != expected_orientation
            or not isinstance(expected_ids, list)
            or not isinstance(sources, Mapping)
            or parsed.get("case_order_sha256")
            != _sha256_bytes(_canonical_json(expected_ids).encode("ascii"))
        ):
            raise Epoch7ControllerError("quality support decision lineage drifted")
        decisions = validate_pointwise_support_decisions(
            parsed.get("decisions"),
            expected_witness_ids=expected_ids,
            source_by_witness=sources,
        )
    else:
        if set(parsed) != common | {
            "expected_pairs",
            "frozen_support_verdicts",
            "support_consensus_sha256",
        }:
            raise Epoch7ControllerError("quality alignment decision artifact is malformed")
        expected_pairs = parsed.get("expected_pairs")
        verdicts = parsed.get("frozen_support_verdicts")
        if (
            parsed.get("schema_version") != QUALITY_TURN_DECISIONS_VERSION
            or parsed.get("stage") != expected_stage
            or parsed.get("orientation") != expected_orientation
            or not isinstance(expected_pairs, list)
            or not isinstance(verdicts, Mapping)
            or not _is_sha256(parsed.get("support_consensus_sha256"))
            or parsed.get("case_order_sha256")
            != _sha256_bytes(_canonical_json(expected_pairs).encode("ascii"))
        ):
            raise Epoch7ControllerError("quality alignment decision lineage drifted")
        decisions = validate_alignment_decisions(
            parsed.get("decisions"),
            expected_pairs=expected_pairs,
            frozen_support_verdicts=verdicts,
        )
    request = _load_object(paths["request"], label="quality turn request")
    if set(request) != {
        "schema_version",
        "stage",
        "orientation",
        "quality_handoff",
        "quality_runtime_evidence_contract_sha256",
        "quality_scoring_manifest",
        "decision_case_order_sha256",
        "frozen_support_consensus_sha256",
        "prompt_sha256",
        "base_instructions_sha256",
        "output_schema_sha256",
        "managed_chatgpt_auth_only",
        "official_persistent_codex_app_server_only",
        "semantic_retry_count",
        "deterministic_semantic_matching",
        "semantic_pruning",
        "holdout_authorized",
        "production_mutation_allowed",
    }:
        raise Epoch7ControllerError("quality turn request shape drifted")
    expected_support_sha = (
        support_consensus_sha256 if expected_stage == "alignment" else None
    )
    if (
        request.get("schema_version") != QUALITY_TURN_REQUEST_VERSION
        or request.get("stage") != expected_stage
        or request.get("orientation") != expected_orientation
        or request.get("quality_handoff") != dict(quality_handoff_record)
        or request.get("quality_runtime_evidence_contract_sha256")
        != quality_evidence_contract_sha256
        or request.get("quality_scoring_manifest")
        != dict(scoring_manifest_record)
        or request.get("decision_case_order_sha256")
        != parsed.get("case_order_sha256")
        or request.get("frozen_support_consensus_sha256") != expected_support_sha
        or request.get("prompt_sha256") != _sha256_bytes(prompt_bytes)
        or request.get("base_instructions_sha256") != _sha256_bytes(base_bytes)
        or request.get("output_schema_sha256") != _sha256_bytes(schema_bytes)
        or request.get("managed_chatgpt_auth_only") is not True
        or request.get("official_persistent_codex_app_server_only") is not True
        or request.get("semantic_retry_count") != 0
        or request.get("deterministic_semantic_matching") is not False
        or request.get("semantic_pruning") is not False
        or request.get("holdout_authorized") is not False
        or request.get("production_mutation_allowed") is not False
    ):
        raise Epoch7ControllerError("quality turn request lineage drifted")
    return {
        "stage": expected_stage,
        "orientation": expected_orientation,
        "thread_id": thread_id,
        "turn_id": turn_id,
        "usage": usage,
        "wall_elapsed_seconds": float(wall),
        "parsed": parsed,
        "decisions": decisions,
    }


def verify_quality_runtime_evidence_receipt(
    controller_root: Path,
    quality_receipt_path: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Verify four raw semantic turns; this receipt never authorizes quality."""

    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    receipt_path = _safe_path(
        quality_receipt_path,
        project_root=project,
        label="quality evidence receipt",
    )
    quality_root = receipt_path.parent
    handoff = verify_quality_handoff(controller_root, project_root=project)
    receipt = _load_object(receipt_path, label="quality evidence receipt")
    if set(receipt) != {
        "schema_version",
        "state",
        "thread_id",
        "plan_epoch",
        "quality_handoff",
        "quality_runtime_evidence_contract_sha256",
        "quality_scoring_manifest",
        "turn_order",
        "turns",
        "support_consensus",
        "alignment_consensus",
        "aggregate_usage",
        "aggregate_wall_elapsed_seconds",
        "semantic_model_call_count",
        "semantic_retry_count",
        "deterministic_score_recomputation_required",
        "quality_gate_authorized",
        "holdout_authorized",
        "production_mutated",
        "receipt_sha256",
    }:
        raise Epoch7ControllerError("quality evidence receipt shape drifted")
    expected_order = [
        {"stage": stage, "orientation": orientation}
        for stage, orientation in _QUALITY_TURN_ORDER
    ]
    turns = receipt.get("turns")
    if (
        receipt.get("schema_version") != QUALITY_EVIDENCE_RECEIPT_VERSION
        or receipt.get("state") != "completed_four_turn_evidence_quality_not_scored"
        or receipt.get("thread_id") != supervisor.TARGET_THREAD_ID
        or receipt.get("plan_epoch") != PLAN_EPOCH
        or receipt.get("quality_handoff") != handoff["handoff_record"]
        or receipt.get("quality_runtime_evidence_contract_sha256")
        != handoff["handoff"]["quality_runtime_evidence_contract_sha256"]
        or receipt.get("turn_order") != expected_order
        or not isinstance(turns, list)
        or len(turns) != 4
        or receipt.get("semantic_model_call_count") != 4
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("deterministic_score_recomputation_required") is not True
        or receipt.get("quality_gate_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("receipt_sha256") != _receipt_checksum(receipt)
    ):
        raise Epoch7ControllerError("quality evidence receipt contract drifted")

    scoring_path = _verify_record(
        receipt.get("quality_scoring_manifest"),
        label="quality scoring manifest",
        allowed_root=quality_root,
    )
    if scoring_path != (quality_root / QUALITY_SCORING_MANIFEST_FILENAME).resolve():
        raise Epoch7ControllerError("quality scoring manifest path drifted")
    scoring_manifest = _load_object(scoring_path, label="quality scoring manifest")
    scoring_witnesses = scoring_manifest.get("witnesses")
    if (
        scoring_manifest.get("schema_version") != QUALITY_SCORING_MANIFEST_VERSION
        or scoring_manifest.get("state") != "frozen_before_quality_turns"
        or not isinstance(scoring_witnesses, list)
    ):
        raise Epoch7ControllerError("quality scoring manifest contract drifted")
    support_consensus_path = _verify_record(
        receipt.get("support_consensus"),
        label="quality support consensus",
        allowed_root=quality_root,
    )

    validated = [
        _verify_quality_turn_evidence(
            turn,
            expected_stage=stage,
            expected_orientation=orientation,
            quality_root=quality_root,
            quality_handoff_record=handoff["handoff_record"],
            quality_evidence_contract_sha256=handoff["handoff"][
                "quality_runtime_evidence_contract_sha256"
            ],
            scoring_manifest_record=receipt["quality_scoring_manifest"],
            support_consensus_sha256=receipt["support_consensus"]["sha256"],
        )
        for turn, (stage, orientation) in zip(turns, _QUALITY_TURN_ORDER)
    ]
    thread_ids = [row["thread_id"] for row in validated]
    turn_ids = [row["turn_id"] for row in validated]
    if len(thread_ids) != len(set(thread_ids)) or len(turn_ids) != len(set(turn_ids)):
        raise Epoch7ControllerError("quality thread or turn identity was reused")

    support_ab, support_ba, alignment_ab, alignment_ba = validated
    if (
        support_ab["parsed"]["expected_witness_ids"]
        != support_ba["parsed"]["expected_witness_ids"]
        or support_ab["parsed"]["source_by_witness"]
        != support_ba["parsed"]["source_by_witness"]
        or [row.get("witness_id") for row in scoring_witnesses]
        != support_ab["parsed"]["expected_witness_ids"]
    ):
        raise Epoch7ControllerError("quality support AB/BA input drifted")
    support_decisions = reconcile_balanced_support_decisions(
        support_ab["decisions"],
        support_ba["decisions"],
        expected_witness_ids=support_ab["parsed"]["expected_witness_ids"],
        source_by_witness=support_ab["parsed"]["source_by_witness"],
    )
    support_payload = {
        "schema_version": QUALITY_SUPPORT_CONSENSUS_VERSION,
        "expected_witness_ids": support_ab["parsed"]["expected_witness_ids"],
        "source_by_witness_sha256": _sha256_bytes(
            _canonical_json(support_ab["parsed"]["source_by_witness"]).encode("ascii")
        ),
        "decisions": support_decisions,
    }
    if (
        _load_object(support_consensus_path, label="quality support consensus")
        != support_payload
    ):
        raise Epoch7ControllerError("quality support consensus drifted")
    support_sha = _sha256_bytes(_canonical_json(support_payload).encode("ascii"))
    support_verdicts = {
        row["witness_id"]: row["verdict"] for row in support_decisions
    }

    if (
        alignment_ab["parsed"]["expected_pairs"]
        != alignment_ba["parsed"]["expected_pairs"]
        or alignment_ab["parsed"]["frozen_support_verdicts"] != support_verdicts
        or alignment_ba["parsed"]["frozen_support_verdicts"] != support_verdicts
        or alignment_ab["parsed"]["support_consensus_sha256"] != support_sha
        or alignment_ba["parsed"]["support_consensus_sha256"] != support_sha
    ):
        raise Epoch7ControllerError("quality alignment frozen support lineage drifted")
    alignment_decisions = reconcile_balanced_alignment_decisions(
        alignment_ab["decisions"],
        alignment_ba["decisions"],
        expected_pairs=alignment_ab["parsed"]["expected_pairs"],
        frozen_support_verdicts=support_verdicts,
    )
    alignment_payload = {
        "schema_version": QUALITY_ALIGNMENT_CONSENSUS_VERSION,
        "support_consensus_sha256": support_sha,
        "expected_pairs": alignment_ab["parsed"]["expected_pairs"],
        "decisions": alignment_decisions,
    }
    alignment_path = _verify_record(
        receipt.get("alignment_consensus"),
        label="quality alignment consensus",
        allowed_root=quality_root,
    )
    if _load_object(alignment_path, label="quality alignment consensus") != alignment_payload:
        raise Epoch7ControllerError("quality alignment consensus drifted")

    aggregate_usage = _sum_usage([row["usage"] for row in validated])
    aggregate_wall = sum(row["wall_elapsed_seconds"] for row in validated)
    if (
        receipt.get("aggregate_usage") != aggregate_usage
        or isinstance(receipt.get("aggregate_wall_elapsed_seconds"), bool)
        or not isinstance(receipt.get("aggregate_wall_elapsed_seconds"), (int, float))
        or abs(float(receipt["aggregate_wall_elapsed_seconds"]) - aggregate_wall)
        > 1e-9
    ):
        raise Epoch7ControllerError("quality aggregate telemetry drifted")
    return {
        "receipt": receipt,
        "receipt_record": _record(receipt_path, allowed_root=quality_root),
        "support_consensus": support_payload,
        "alignment_consensus": alignment_payload,
        "aggregate_usage": aggregate_usage,
        "aggregate_wall_elapsed_seconds": aggregate_wall,
        "quality_gate_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def recompute_shared_reference_quality_score(
    *,
    scoring_manifest: Mapping[str, Any],
    support_consensus: Mapping[str, Any],
    alignment_consensus: Mapping[str, Any],
) -> dict[str, Any]:
    """Recompute strict per-field F1 from raw LLM-owned consensus only."""

    if not isinstance(scoring_manifest, Mapping) or set(scoring_manifest) != {
        "schema_version",
        "state",
        "case_order",
        "system_order",
        "witnesses",
        "one_shared_augmented_reference",
        "reference_system_ids",
        "semantic_fields",
        "semantic_fields_sha256",
        "deterministic_semantic_matching",
        "semantic_pruning",
    }:
        raise Epoch7ControllerError("quality scoring manifest is malformed")
    case_order = scoring_manifest.get("case_order")
    system_order = scoring_manifest.get("system_order")
    witness_rows = scoring_manifest.get("witnesses")
    semantic_fields = list(_TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    if (
        scoring_manifest.get("schema_version") != QUALITY_SCORING_MANIFEST_VERSION
        or scoring_manifest.get("state") != "frozen_before_quality_turns"
        or not isinstance(case_order, list)
        or not case_order
        or len(case_order) != len(set(case_order))
        or any(not isinstance(value, str) or not value for value in case_order)
        or not isinstance(system_order, list)
        or len(system_order) < 2
        or len(system_order) != len(set(system_order))
        or any(not isinstance(value, str) or not value for value in system_order)
        or not isinstance(witness_rows, list)
        or not witness_rows
        or scoring_manifest.get("one_shared_augmented_reference") is not True
        or scoring_manifest.get("reference_system_ids") is not None
        or scoring_manifest.get("semantic_fields") != semantic_fields
        or scoring_manifest.get("semantic_fields_sha256")
        != _sha256_bytes(_canonical_json(semantic_fields).encode("ascii"))
        or scoring_manifest.get("deterministic_semantic_matching") is not False
        or scoring_manifest.get("semantic_pruning") is not False
    ):
        raise Epoch7ControllerError("quality scoring manifest contract drifted")

    witnesses: dict[str, dict[str, Any]] = {}
    case_witnesses: dict[str, list[str]] = {case_id: [] for case_id in case_order}
    observed_systems: set[str] = set()
    for index, row in enumerate(witness_rows):
        if not isinstance(row, Mapping) or set(row) != {
            "witness_id",
            "case_id",
            "system_id",
            "submitted_evidence_exact",
        }:
            raise Epoch7ControllerError(f"quality scoring witness {index} is malformed")
        witness_id = row.get("witness_id")
        case_id = row.get("case_id")
        system_id = row.get("system_id")
        if (
            not isinstance(witness_id, str)
            or not witness_id
            or witness_id in witnesses
            or case_id not in case_witnesses
            or system_id not in system_order
            or not isinstance(row.get("submitted_evidence_exact"), bool)
        ):
            raise Epoch7ControllerError(
                f"quality scoring witness {index} lineage drifted"
            )
        normalized = {
            "witness_id": witness_id,
            "case_id": case_id,
            "system_id": system_id,
            "submitted_evidence_exact": row["submitted_evidence_exact"],
        }
        witnesses[witness_id] = normalized
        case_witnesses[str(case_id)].append(witness_id)
        observed_systems.add(str(system_id))
    if observed_systems != set(system_order) or any(
        not case_witnesses[case_id] for case_id in case_order
    ):
        raise Epoch7ControllerError("quality scoring system or case coverage drifted")

    if (
        not isinstance(support_consensus, Mapping)
        or support_consensus.get("schema_version")
        != QUALITY_SUPPORT_CONSENSUS_VERSION
        or support_consensus.get("expected_witness_ids") != list(witnesses)
        or not isinstance(support_consensus.get("decisions"), list)
    ):
        raise Epoch7ControllerError("quality support consensus scoring input drifted")
    support_rows = support_consensus["decisions"]
    if len(support_rows) != len(witnesses):
        raise Epoch7ControllerError("quality support consensus coverage drifted")
    support: dict[str, str] = {}
    for expected_id, row in zip(witnesses, support_rows):
        if (
            not isinstance(row, Mapping)
            or row.get("witness_id") != expected_id
            or row.get("verdict") not in {"supported", "unsupported", "abstain"}
        ):
            raise Epoch7ControllerError("quality support consensus row drifted")
        support[expected_id] = str(row["verdict"])

    if (
        not isinstance(alignment_consensus, Mapping)
        or alignment_consensus.get("schema_version")
        != QUALITY_ALIGNMENT_CONSENSUS_VERSION
        or not isinstance(alignment_consensus.get("expected_pairs"), list)
        or not isinstance(alignment_consensus.get("decisions"), list)
        or alignment_consensus.get("support_consensus_sha256")
        != _sha256_bytes(_canonical_json(support_consensus).encode("ascii"))
    ):
        raise Epoch7ControllerError("quality alignment consensus scoring input drifted")
    expected_pairs = alignment_consensus["expected_pairs"]
    decisions = alignment_consensus["decisions"]
    if len(expected_pairs) != len(decisions):
        raise Epoch7ControllerError("quality alignment consensus coverage drifted")

    required_pairs: set[tuple[str, str]] = set()
    for case_id in case_order:
        supported = sorted(
            witness_id
            for witness_id in case_witnesses[case_id]
            if support[witness_id] == "supported"
        )
        required_pairs.update(
            (left, right)
            for left_index, left in enumerate(supported)
            for right in supported[left_index + 1 :]
        )
    observed_pairs: set[tuple[str, str]] = set()
    normalized_decisions: list[dict[str, Any]] = []
    for index, (pair, decision) in enumerate(zip(expected_pairs, decisions)):
        if (
            not isinstance(pair, Mapping)
            or set(pair) != {"pair_id", "left_witness_id", "right_witness_id"}
            or not isinstance(decision, Mapping)
            or decision.get("pair_id") != pair.get("pair_id")
            or decision.get("left_witness_id") != pair.get("left_witness_id")
            or decision.get("right_witness_id") != pair.get("right_witness_id")
            or decision.get("relation")
            not in {"equivalent", "not_equivalent", "abstain"}
            or not isinstance(decision.get("checklist"), list)
        ):
            raise Epoch7ControllerError(
                f"quality alignment scoring decision {index} drifted"
            )
        left = str(pair["left_witness_id"])
        right = str(pair["right_witness_id"])
        if (
            left not in witnesses
            or right not in witnesses
            or witnesses[left]["case_id"] != witnesses[right]["case_id"]
            or support[left] != "supported"
            or support[right] != "supported"
        ):
            raise Epoch7ControllerError("quality alignment pair escaped support/case scope")
        identity = tuple(sorted((left, right)))
        if identity in observed_pairs:
            raise Epoch7ControllerError("quality alignment pair was duplicated")
        observed_pairs.add(identity)
        checklist = decision["checklist"]
        if len(checklist) != len(semantic_fields):
            raise Epoch7ControllerError("quality alignment checklist count drifted")
        normalized_rows: dict[str, str] = {}
        for expected_field, row in zip(semantic_fields, checklist):
            if (
                not isinstance(row, Mapping)
                or row.get("field") != expected_field
                or row.get("decision") not in {"same", "different", "abstain"}
            ):
                raise Epoch7ControllerError("quality alignment checklist row drifted")
            normalized_rows[expected_field] = str(row["decision"])
        normalized_decisions.append(
            {
                "left": left,
                "right": right,
                "relation": str(decision["relation"]),
                "fields": normalized_rows,
            }
        )
    if observed_pairs != required_pairs:
        raise Epoch7ControllerError("quality alignment pair coverage is incomplete")

    field_scores: dict[str, dict[str, dict[str, float | int]]] = {}
    for field in semantic_fields:
        parent = {witness_id: witness_id for witness_id in witnesses}

        def find(witness_id: str) -> str:
            cursor = witness_id
            while parent[cursor] != cursor:
                parent[cursor] = parent[parent[cursor]]
                cursor = parent[cursor]
            return cursor

        def union(left: str, right: str) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        for decision in normalized_decisions:
            if decision["relation"] != "abstain" and decision["fields"][field] == "same":
                union(decision["left"], decision["right"])
        for decision in normalized_decisions:
            if (
                decision["fields"][field] == "different"
                and find(decision["left"]) == find(decision["right"])
            ):
                raise Epoch7ControllerError(
                    "quality alignment consensus is transitively contradictory"
                )
        unit_by_witness = {
            witness_id: (witnesses[witness_id]["case_id"], find(witness_id))
            for witness_id in witnesses
        }
        reference_units = {
            unit_by_witness[witness_id]
            for witness_id, row in witnesses.items()
            if support[witness_id] == "supported"
            and row["submitted_evidence_exact"] is True
        }
        if not reference_units:
            raise Epoch7ControllerError("quality shared reference has no supported units")
        systems: dict[str, dict[str, float | int]] = {}
        for system_id in system_order:
            submitted_units = {
                unit_by_witness[witness_id]
                for witness_id, row in witnesses.items()
                if row["system_id"] == system_id
            }
            supported_units = {
                unit_by_witness[witness_id]
                for witness_id, row in witnesses.items()
                if row["system_id"] == system_id
                and support[witness_id] == "supported"
                and row["submitted_evidence_exact"] is True
            }
            precision = (
                len(supported_units) / len(submitted_units) if submitted_units else 0.0
            )
            covered = supported_units & reference_units
            recall = len(covered) / len(reference_units)
            f1 = (
                2 * precision * recall / (precision + recall)
                if precision + recall
                else 0.0
            )
            systems[system_id] = {
                "submitted_unit_count": len(submitted_units),
                "supported_unit_count": len(supported_units),
                "reference_unit_count": len(reference_units),
                "covered_reference_unit_count": len(covered),
                "precision": round(precision, 6),
                "recall": round(recall, 6),
                "f1": round(f1, 6),
            }
        field_scores[field] = systems

    systems = {}
    for system_id in system_order:
        values = [field_scores[field][system_id]["f1"] for field in semantic_fields]
        systems[system_id] = {
            "strict_full_field_macro": round(
                sum(float(value) for value in values) / len(values), 6
            ),
            "fields": {
                field: field_scores[field][system_id] for field in semantic_fields
            },
        }
    return {
        "schema_version": QUALITY_RECOMPUTED_SCORE_VERSION,
        "state": "deterministically_recomputed_from_raw_consensus",
        "scoring_manifest_sha256": _sha256_bytes(
            _canonical_json(scoring_manifest).encode("ascii")
        ),
        "support_consensus_sha256": _sha256_bytes(
            _canonical_json(support_consensus).encode("ascii")
        ),
        "alignment_consensus_sha256": _sha256_bytes(
            _canonical_json(alignment_consensus).encode("ascii")
        ),
        "semantic_fields": semantic_fields,
        "system_order": list(system_order),
        "systems": systems,
        "reported_scalar_metrics_used": False,
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _verified_historical_cost_chain(
    context_usage_recovery_path: Path,
    *,
    project_root: Path,
) -> dict[str, Any]:
    recovery_path = _safe_path(
        context_usage_recovery_path,
        project_root=project_root,
        label="historical context usage recovery",
    )
    recovery = _load_object(recovery_path, label="historical context usage recovery")
    requested_runs = recovery.get("requested_runs")
    recovered_runs = recovery.get("recovered_runs")
    sidecars = recovery.get("sidecars")
    empty_error_fields = (
        "missing_run_ids",
        "duplicate_matches",
        "invalid_matches",
        "artifact_errors",
        "expected_usage_mismatches",
    )
    if (
        recovery.get("schema_version")
        != "historical_episode_context_usage_recovery_v1"
        or recovery.get("ok") is not True
        or recovery.get("recovery_reran_model") is not False
        or isinstance(requested_runs, bool)
        or not isinstance(requested_runs, int)
        or requested_runs <= 0
        or recovered_runs != requested_runs
        or any(recovery.get(field) not in ([], {}) for field in empty_error_fields)
        or not isinstance(sidecars, list)
        or len(sidecars) != requested_runs
    ):
        raise Epoch7ControllerError("historical context recovery contract drifted")
    context_usage = _usage(recovery.get("usage"), label="historical context")
    expected_usage_raw = recovery.get("expected_usage")
    if isinstance(expected_usage_raw, Mapping) and set(expected_usage_raw) == {
        "input_tokens",
        "output_tokens",
        "total_tokens",
    }:
        expected_usage = dict(expected_usage_raw)
        if any(
            isinstance(expected_usage[field], bool)
            or not isinstance(expected_usage[field], int)
            or expected_usage[field] < 0
            for field in expected_usage
        ) or expected_usage["total_tokens"] != (
            expected_usage["input_tokens"] + expected_usage["output_tokens"]
        ):
            raise Epoch7ControllerError(
                "historical expected context usage is malformed"
            )
    else:
        expected_full_usage = _usage(
            expected_usage_raw, label="historical expected context"
        )
        expected_usage = {
            field: expected_full_usage[field]
            for field in ("input_tokens", "output_tokens", "total_tokens")
        }
    production_context_usage = _usage(
        recovery.get("production_amortized_usage"),
        label="historical production-amortized context",
    )
    if expected_usage != {
        field: context_usage[field]
        for field in ("input_tokens", "output_tokens", "total_tokens")
    }:
        raise Epoch7ControllerError("historical context expected usage drifted")

    sidecar_records: list[dict[str, Any]] = []
    run_ids: set[str] = set()
    sidecar_paths: set[str] = set()
    for index, row in enumerate(sidecars):
        if not isinstance(row, Mapping) or set(row) != {
            "run_id",
            "sidecar_path",
            "sidecar_sha256",
            "rollout_session_id",
            "rollout_sha256",
        }:
            raise Epoch7ControllerError(
                f"historical context sidecar {index} is malformed"
            )
        run_id = row.get("run_id")
        path_value = row.get("sidecar_path")
        if (
            not isinstance(run_id, str)
            or not run_id
            or run_id in run_ids
            or not isinstance(path_value, str)
            or path_value in sidecar_paths
            or not _is_sha256(row.get("sidecar_sha256"))
            or not isinstance(row.get("rollout_session_id"), str)
            or not row.get("rollout_session_id")
            or not _is_sha256(row.get("rollout_sha256"))
        ):
            raise Epoch7ControllerError(
                f"historical context sidecar {index} lineage drifted"
            )
        sidecar_path = _safe_path(
            Path(path_value),
            project_root=project_root,
            label=f"historical context sidecar {index}",
        )
        record = _record(sidecar_path, allowed_root=project_root)
        if record["sha256"] != row["sidecar_sha256"]:
            raise Epoch7ControllerError(
                f"historical context sidecar {index} hash drifted"
            )
        run_ids.add(run_id)
        sidecar_paths.add(path_value)
        sidecar_records.append(record)

    context_cost_path_value = recovery.get("context_cost_report_path")
    if not isinstance(context_cost_path_value, str):
        raise Epoch7ControllerError("historical context cost path is absent")
    context_cost_path = _safe_path(
        Path(context_cost_path_value),
        project_root=project_root,
        label="historical context cost report",
    )
    context_cost_record = _record(context_cost_path, allowed_root=project_root)
    if context_cost_record["sha256"] != recovery.get("context_cost_report_sha256"):
        raise Epoch7ControllerError("historical context cost record drifted")
    context_cost = _load_object(context_cost_path, label="historical context cost report")
    coverage = context_cost.get("usage_sidecar_coverage")
    phase_path_value = context_cost.get("phase_one_report_path")
    if (
        context_cost.get("schema_version") != "windowed_paired_context_cost_v1"
        or context_cost.get("exact_usage_available") is not True
        or context_cost.get("end_to_end_cost_evaluable") is not True
        or context_cost.get("fail_closed_reason") is not None
        or context_cost.get("exact_context_usage") != context_usage
        or context_cost.get("exact_context_usage_production_amortized")
        != production_context_usage
        or context_cost.get("exact_context_tokens") != context_usage["total_tokens"]
        or context_cost.get("exact_context_tokens_production_amortized")
        != production_context_usage["total_tokens"]
        or context_cost.get("unique_episodes") != requested_runs
        or context_cost.get("expected_unique_episodes") != requested_runs
        or not isinstance(coverage, Mapping)
        or coverage.get("required") != requested_runs
        or coverage.get("validated") != requested_runs
        or coverage.get("missing_or_invalid") != []
        or not isinstance(phase_path_value, str)
    ):
        raise Epoch7ControllerError("historical context cost contract drifted")

    phase_path = _safe_path(
        Path(phase_path_value),
        project_root=project_root,
        label="historical paired baseline phase-one report",
    )
    phase_record = _record(phase_path, allowed_root=project_root)
    phase = _load_object(phase_path, label="historical paired baseline phase-one report")
    baseline = phase.get("baseline")
    baseline_segments = phase.get("requested_segments")
    if (
        phase.get("schema_version") != "windowed_paired_phase_one_v1"
        or isinstance(baseline_segments, bool)
        or not isinstance(baseline_segments, int)
        or baseline_segments <= 0
        or context_cost.get("segments") != baseline_segments
        or not isinstance(baseline, Mapping)
        or baseline.get("accounting_complete") is not True
    ):
        raise Epoch7ControllerError("historical paired baseline contract drifted")
    baseline_usage = _usage(baseline.get("usage"), label="historical paired baseline")
    baseline_end_to_end = (
        baseline_usage["total_tokens"] + production_context_usage["total_tokens"]
    )
    if baseline_end_to_end <= 0:
        raise Epoch7ControllerError("historical production denominator is zero")
    return {
        "context_usage_recovery_record": _record(
            recovery_path, allowed_root=project_root
        ),
        "context_cost_report_record": context_cost_record,
        "baseline_phase_one_report_record": phase_record,
        "context_sidecar_count": len(sidecar_records),
        "context_sidecar_set_sha256": _sha256_bytes(
            _canonical_json(sidecar_records).encode("ascii")
        ),
        "baseline_segments": baseline_segments,
        "baseline_usage": baseline_usage,
        "historical_context_usage": context_usage,
        "production_amortized_context_usage": production_context_usage,
        "baseline_end_to_end_tokens": baseline_end_to_end,
    }


def _quality_cost_authority_payload(inputs: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": QUALITY_COST_AUTHORITY_VERSION,
        "state": "frozen_exact_historical_cost_authority",
        "formula": QUALITY_PRODUCTION_COST_FORMULA,
        "context_usage_recovery": copy.deepcopy(
            inputs["context_usage_recovery_record"]
        ),
        "context_cost_report": copy.deepcopy(inputs["context_cost_report_record"]),
        "baseline_phase_one_report": copy.deepcopy(
            inputs["baseline_phase_one_report_record"]
        ),
        "context_sidecar_count": inputs["context_sidecar_count"],
        "context_sidecar_set_sha256": inputs["context_sidecar_set_sha256"],
        "baseline_segments": inputs["baseline_segments"],
        "baseline_usage": copy.deepcopy(inputs["baseline_usage"]),
        "historical_context_usage": copy.deepcopy(
            inputs["historical_context_usage"]
        ),
        "production_amortized_context_usage": copy.deepcopy(
            inputs["production_amortized_context_usage"]
        ),
        "baseline_end_to_end_tokens": inputs["baseline_end_to_end_tokens"],
        "scalar_cost_claims_authoritative": False,
        "judge_usage_is_separate_development_qa_overhead": True,
        "judge_usage_in_production_cost_formula": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def freeze_quality_cost_authority(
    quality_root: Path,
    *,
    context_usage_recovery_path: Path,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Freeze the exact historical denominator without accepting scalar inputs."""

    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(quality_root, project_root=project, label="quality root")
    inputs = _verified_historical_cost_chain(
        context_usage_recovery_path,
        project_root=project,
    )
    path = root / QUALITY_COST_AUTHORITY_FILENAME
    _write_immutable_json(path, _quality_cost_authority_payload(inputs))
    return verify_quality_cost_authority(root, project_root=project)


def verify_quality_cost_authority(
    quality_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(quality_root, project_root=project, label="quality root")
    path = root / QUALITY_COST_AUTHORITY_FILENAME
    payload = _load_object(path, label="quality cost authority")
    recovery_path = _verify_record(
        payload.get("context_usage_recovery"),
        label="quality cost context recovery",
        allowed_root=project,
    )
    inputs = _verified_historical_cost_chain(
        recovery_path,
        project_root=project,
    )
    expected = _quality_cost_authority_payload(inputs)
    if payload != expected:
        raise Epoch7ControllerError("quality cost authority drifted")
    return {
        "authority": payload,
        "record": _record(path, allowed_root=root),
        **inputs,
    }


def _quality_terminal_inputs(
    controller_root: Path,
    quality_root: Path,
    *,
    project_root: Path,
) -> dict[str, Any]:
    root = _safe_path(quality_root, project_root=project_root, label="quality root")
    if root.is_symlink() or not root.is_dir():
        raise Epoch7ControllerError("quality root is not a real directory")
    loaded = load_epoch7_controller(controller_root, project_root=project_root)
    handoff = verify_quality_handoff(controller_root, project_root=project_root)
    evidence = verify_quality_runtime_evidence_receipt(
        controller_root,
        root / QUALITY_EVIDENCE_RECEIPT_FILENAME,
        project_root=project_root,
    )
    scoring_path = root / QUALITY_SCORING_MANIFEST_FILENAME
    scoring_manifest = _load_object(scoring_path, label="quality scoring manifest")
    scoring_record = _record(scoring_path, allowed_root=root)
    if evidence["receipt"].get("quality_scoring_manifest") != scoring_record:
        raise Epoch7ControllerError(
            "quality evidence did not bind the scoring manifest"
        )
    score = recompute_shared_reference_quality_score(
        scoring_manifest=scoring_manifest,
        support_consensus=evidence["support_consensus"],
        alignment_consensus=evidence["alignment_consensus"],
    )
    extraction = _verify_extraction_receipt(loaded)
    cost = verify_quality_cost_authority(root, project_root=project_root)
    arm_ids = [str(row["variant_id"]) for row in extraction["receipt"]["arms"]]
    system_order = scoring_manifest.get("system_order")
    baseline_ids = [
        system_id
        for system_id in system_order or []
        if isinstance(system_id, str) and system_id not in arm_ids
    ]
    if (
        scoring_manifest.get("case_order")
        != loaded["preflight"]["receipt"]["opaque_case_order"]
        or not isinstance(system_order, list)
        or len(baseline_ids) != 1
        or system_order != [baseline_ids[0], *arm_ids]
        or list(score["systems"]) != system_order
    ):
        raise Epoch7ControllerError("quality scoring system or case lineage drifted")
    return {
        "root": root,
        "loaded": loaded,
        "handoff": handoff,
        "evidence": evidence,
        "scoring_manifest": scoring_manifest,
        "scoring_record": scoring_record,
        "score": score,
        "extraction": extraction,
        "cost": cost,
        "arm_ids": arm_ids,
        "baseline_system_id": baseline_ids[0],
    }


def _quality_terminal_payload(
    inputs: Mapping[str, Any],
    *,
    recomputed_score_record: Mapping[str, Any],
) -> dict[str, Any]:
    loaded = inputs["loaded"]
    evidence = inputs["evidence"]
    extraction_receipt = inputs["extraction"]["receipt"]
    scoring_manifest = inputs["scoring_manifest"]
    score = inputs["score"]
    cost = inputs["cost"]
    baseline_id = inputs["baseline_system_id"]
    baseline_macro = float(score["systems"][baseline_id]["strict_full_field_macro"])
    development_segments = loaded["preflight"]["receipt"]["case_count"]
    baseline_segments = int(cost["baseline_segments"])
    context_tokens = int(cost["production_amortized_context_usage"]["total_tokens"])
    baseline_end_to_end = int(cost["baseline_end_to_end_tokens"])
    if development_segments <= 0:
        raise Epoch7ControllerError("quality development segment count is zero")

    witness_rows = scoring_manifest["witnesses"]
    arm_results: list[dict[str, Any]] = []
    for arm in extraction_receipt["arms"]:
        variant_id = str(arm["variant_id"])
        arm_usage = _usage(arm.get("usage"), label=f"quality cost {variant_id}")
        arm_macro = float(score["systems"][variant_id]["strict_full_field_macro"])
        submitted = [
            row for row in witness_rows if row.get("system_id") == variant_id
        ]
        exact_count = sum(
            row.get("submitted_evidence_exact") is True for row in submitted
        )
        exact_rate = exact_count / len(submitted) if submitted else 0.0
        scaled_numerator = arm_usage["total_tokens"] * baseline_segments
        scaled_extraction = (
            scaled_numerator + development_segments - 1
        ) // development_segments
        candidate_end_to_end = scaled_extraction + context_tokens
        divisor = gcd(candidate_end_to_end, baseline_end_to_end)
        ratio = candidate_end_to_end / baseline_end_to_end
        checks = {
            "strict_full_field_macro_gte_0_97": (
                arm_macro >= matrix.QUALITY_THRESHOLD
            ),
            "candidate_noninferior_to_same_reference_baseline": (
                arm_macro >= baseline_macro - matrix.NONINFERIORITY_MARGIN
            ),
            "submitted_exact_evidence_rate_1": exact_rate == 1.0,
            "production_amortized_total_token_ratio_lte_0_28": (
                candidate_end_to_end * 25 <= baseline_end_to_end * 7
            ),
            "complete_extraction_and_quality_telemetry": True,
            "full_canonical_output_validation_bound": True,
            "no_semantic_defaults_matching_or_pruning": True,
        }
        arm_results.append(
            {
                "variant_id": variant_id,
                "system_id": variant_id,
                "passed": all(checks.values()),
                "checks": checks,
                "strict_full_field_macro": arm_macro,
                "baseline_strict_full_field_macro": baseline_macro,
                "submitted_witness_count": len(submitted),
                "submitted_exact_evidence_count": exact_count,
                "submitted_exact_evidence_rate": round(exact_rate, 6),
                "measured_extraction_usage": arm_usage,
                "candidate_tokens_scaled_to_baseline_segment_scope": (
                    scaled_extraction
                ),
                "shared_production_amortized_context_tokens": context_tokens,
                "candidate_end_to_end_tokens": candidate_end_to_end,
                "baseline_end_to_end_tokens": baseline_end_to_end,
                "production_amortized_total_token_ratio_numerator": (
                    candidate_end_to_end // divisor
                ),
                "production_amortized_total_token_ratio_denominator": (
                    baseline_end_to_end // divisor
                ),
                "production_amortized_total_token_ratio": round(ratio, 6),
            }
        )
    passing = [row["variant_id"] for row in arm_results if row["passed"]]
    rejected = [row["variant_id"] for row in arm_results if not row["passed"]]
    return {
        "schema_version": QUALITY_TERMINAL_RECEIPT_VERSION,
        "state": (
            "passed_development_quality_checkpoint_selection_not_frozen"
            if passing
            else "rejected_development_quality_or_cost_gate"
        ),
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "controller_contract": _record(
            loaded["contract_path"], allowed_root=loaded["controller_root"]
        ),
        "quality_handoff": inputs["handoff"]["handoff_record"],
        "extraction_receipt": inputs["extraction"]["record"],
        "quality_evidence_receipt": evidence["receipt_record"],
        "quality_scoring_manifest": inputs["scoring_record"],
        "recomputed_quality_score": copy.deepcopy(dict(recomputed_score_record)),
        "quality_cost_authority": inputs["cost"]["record"],
        "development_selection_policy": loaded["contract"][
            "development_selection_policy"
        ],
        "development_selection_policy_sha256": loaded["contract"][
            "development_selection_policy_sha256"
        ],
        "baseline_system_id": baseline_id,
        "baseline_strict_full_field_macro": baseline_macro,
        "development_segment_count": development_segments,
        "baseline_segment_count": baseline_segments,
        "production_cost_formula": QUALITY_PRODUCTION_COST_FORMULA,
        "strict_full_field_macro_threshold": matrix.QUALITY_THRESHOLD,
        "noninferiority_margin": matrix.NONINFERIORITY_MARGIN,
        "production_amortized_total_token_ratio_threshold": matrix.TOKEN_RATIO_THRESHOLD,
        "quality_judge_usage": evidence["aggregate_usage"],
        "quality_judge_wall_elapsed_seconds": evidence[
            "aggregate_wall_elapsed_seconds"
        ],
        "quality_judge_usage_is_separate_development_qa_overhead": True,
        "quality_judge_usage_in_production_cost_formula": False,
        "semantic_model_call_count": 4,
        "semantic_retry_count": 0,
        "arm_results": arm_results,
        "passing_arm_ids": passing,
        "rejected_arm_ids": rejected,
        "quality_gate_passed": bool(passing),
        "reported_scalar_metrics_used": False,
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
        "quality_selection_authorized": False,
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def freeze_quality_terminal_receipt(
    controller_root: Path,
    quality_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Freeze a development-only terminal from raw evidence and exact cost lineage."""

    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    inputs = _quality_terminal_inputs(
        controller_root,
        quality_root,
        project_root=project,
    )
    score_path = inputs["root"] / QUALITY_RECOMPUTED_SCORE_FILENAME
    _write_immutable_json(score_path, inputs["score"])
    score_record = _record(score_path, allowed_root=inputs["root"])
    payload = _quality_terminal_payload(
        inputs,
        recomputed_score_record=score_record,
    )
    _write_controller_receipt(
        inputs["root"] / QUALITY_TERMINAL_RECEIPT_FILENAME,
        payload,
    )
    return verify_quality_terminal_receipt(
        controller_root,
        quality_root,
        project_root=project,
    )


def verify_quality_terminal_receipt(
    controller_root: Path,
    quality_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    inputs = _quality_terminal_inputs(
        controller_root,
        quality_root,
        project_root=project,
    )
    score_path = inputs["root"] / QUALITY_RECOMPUTED_SCORE_FILENAME
    observed_score = _load_object(score_path, label="recomputed quality score")
    if observed_score != inputs["score"]:
        raise Epoch7ControllerError("recomputed quality score drifted")
    score_record = _record(score_path, allowed_root=inputs["root"])
    expected = _quality_terminal_payload(
        inputs,
        recomputed_score_record=score_record,
    )
    expected["receipt_sha256"] = _receipt_checksum(expected)
    path = inputs["root"] / QUALITY_TERMINAL_RECEIPT_FILENAME
    observed = _load_object(path, label="quality terminal receipt")
    if observed != expected:
        raise Epoch7ControllerError("quality terminal receipt drifted")
    return {
        "receipt": observed,
        "receipt_record": _record(path, allowed_root=inputs["root"]),
        "recomputed_score": observed_score,
        "cost_authority": inputs["cost"]["authority"],
        "winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _directive_payload(
    *,
    future_binding: Mapping[str, Any],
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    semantic_plan_path: Path,
    plan_payload: Mapping[str, Any],
    extraction_root: Path,
    operator_authorization_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    binding = future_binding["binding"]
    limits = extraction_runtime._capacity_limits(preflight)
    runtime_binding = extraction_runtime.build_runtime_dependency_binding()
    runtime_payload = runtime_binding["binding"]
    plan_contract = extraction_runtime.semantic_plan_contract(plan_payload)
    return {
        "schema_version": matrix.FUTURE_DIRECTIVE_VERSION,
        "state": "explicit_live_development_matrix_authorization",
        "authorized_by": "kolby",
        "operator_authorization_id": operator_authorization_id,
        "authorization_scope": "one_epoch7_full_canonical_six_arm_extraction_only",
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "candidate_system_id": binding["candidate_system_id"],
        "future_plan_binding_sha256": future_binding["binding_sha256"],
        "precommit_sha256": binding["precommit_sha256"],
        "manifest_sha256": binding["manifest_sha256"],
        "context_set_sha256": binding["context_set_sha256"],
        "runtime_binding_sha256": binding["runtime_binding_sha256"],
        "context_control_overlay_sha256": binding[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": binding[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": binding["capacity_policy_sha256"],
        "quality_evaluator_contract_sha256": binding[
            "quality_evaluator_contract_sha256"
        ],
        "arms": copy.deepcopy(binding["arms"]),
        "live_semantic_dispatch_authorized": True,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "full_canonical_v31_outputs_required": True,
        "semantic_retry_count": 0,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "expected_receipt_path": str(
            (extraction_root / extraction_runtime.EXTRACTION_RECEIPT_FILENAME).resolve()
        ),
        "semantic_plan_path": str(semantic_plan_path.resolve()),
        "semantic_plan_contract_sha256": plan_contract["contract_sha256"],
        "max_model_calls": limits["exact_model_call_cap"],
        "max_total_tokens": limits["exact_total_token_cap"],
        "maximum_total_tokens_per_turn": limits[
            "maximum_total_tokens_per_turn"
        ],
        "maximum_wall_seconds_per_turn": limits[
            "maximum_wall_seconds_per_turn"
        ],
        "minimum_remaining_reserve_percent": limits[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": limits[
            "capacity_safety_margin_percent"
        ],
        "quota_points_per_million_tokens": limits[
            "quota_points_per_million_tokens"
        ],
        "quota_calibration_id": limits["quota_calibration_id"],
        "maximum_rate_limit_snapshot_age_seconds": limits[
            "maximum_rate_limit_snapshot_age_seconds"
        ],
        "operator_wall_deadline_safety_margin_seconds": limits[
            "operator_wall_deadline_safety_margin_seconds"
        ],
        "token_capacity_is_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_required": True,
        "postturn_measured_stop_required": True,
        "exact_request_count": limits["exact_model_call_cap"],
        "development_output_root": str(extraction_root.resolve()),
        "live_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "runtime_module_sha256": runtime_payload["runtime_module"]["sha256"],
        "offline_matrix_module_sha256": runtime_payload["exact_dependencies"][
            "offline_matrix"
        ]["sha256"],
        "canonical_adapter_module_sha256": runtime_payload["exact_dependencies"][
            "canonical_adapter"
        ]["sha256"],
        "supervisor_module_sha256": runtime_payload["exact_dependencies"][
            "supervisor_semantic_plan"
        ]["sha256"],
        "extraction_only": True,
        "quality_evaluation_authorized": False,
        "quality_selection_authorized": False,
        "api_key_auth_allowed": False,
        "raw_session_token_auth_allowed": False,
        "offline_test_mode_authorized": False,
        "live_pass_receipt_authorized": True,
        "canonical_arm_runner_implementation_sha256": runtime_payload[
            "canonical_arm_runner_implementation_sha256"
        ],
        "official_managed_auth_client_implementation_sha256": runtime_payload[
            "official_managed_auth_client_implementation_sha256"
        ],
        "trusted_capacity_implementation_state": runtime_payload[
            "trusted_capacity_implementation_state"
        ],
        "trusted_rate_limit_parser_implementation_sha256": runtime_payload[
            "trusted_rate_limit_parser_implementation_sha256"
        ],
        "trusted_capacity_measurement_implementation_sha256": runtime_payload[
            "trusted_capacity_measurement_implementation_sha256"
        ],
        "trusted_preturn_postturn_guard_implementation_sha256": runtime_payload[
            "trusted_preturn_postturn_guard_implementation_sha256"
        ],
        "borrowed_official_client_context_implementation_sha256": runtime_payload[
            "borrowed_official_client_context_implementation_sha256"
        ],
        "capacity_measurement_required_before_each_arm": True,
        "per_arm_capacity_admission_required": True,
        "new_empty_development_root_required": True,
    }


def freeze_epoch7_plan(
    *,
    controller_root: Path,
    extraction_root: Path,
    manifest_path: Path,
    episodes_path: Path,
    capacity_policy_path: Path,
    operator_authorization_id: str,
    issued_at: str,
    expires_at: str,
    project_root: Path | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(controller_root, project_root=project, label="controller root")
    extraction = _safe_path(
        extraction_root, project_root=project, label="extraction root"
    )
    if root == extraction or root in extraction.parents or extraction in root.parents:
        raise Epoch7ControllerError("controller and extraction roots must be disjoint")
    authorization_id = _authorization_id(operator_authorization_id)
    issued = _parse_timestamp(issued_at, label="issued_at")
    expires = _parse_timestamp(expires_at, label="expires_at")
    if expires <= issued:
        raise Epoch7ControllerError("directive expiry must follow issuance")
    if extraction.exists():
        raise Epoch7ControllerError("epoch-7 extraction root must be fresh and absent")

    manifest = _safe_path(manifest_path, project_root=project, label="manifest")
    episodes_file = _safe_path(episodes_path, project_root=project, label="episodes")
    capacity = _safe_path(
        capacity_policy_path, project_root=project, label="capacity policy"
    )
    manifest_record = _record(manifest, allowed_root=project)
    episodes_record = _record(episodes_file, allowed_root=project)
    capacity_record = _record(capacity, allowed_root=project)
    episodes = _load_array(episodes_file, label="epoch-7 episodes")

    preflight = matrix.build_matrix_dry_preflight(
        manifest_path=manifest,
        manifest_sha256=manifest_record["sha256"],
        episodes=episodes,
        capacity_policy_path=capacity,
        capacity_policy_sha256=capacity_record["sha256"],
        allowed_root=project,
    )
    precommit = matrix.build_matrix_precommit(preflight)
    future_binding = matrix.build_future_plan_binding(
        precommit, output_root=extraction
    )
    limits = extraction_runtime._capacity_limits(preflight)

    directive_path = root / DIRECTIVE_FILENAME
    semantic_plan_path = root / SEMANTIC_PLAN_FILENAME
    receipt_path = extraction / extraction_runtime.EXTRACTION_RECEIPT_FILENAME
    plan_payload = {
        "schema_version": supervisor.SEMANTIC_PLAN_SCHEMA_VERSION,
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "state": "executable",
        "step": {
            "step_id": STEP_ID,
            "state": "executable",
            "max_model_calls": limits["exact_model_call_cap"],
            "max_total_tokens": limits["exact_total_token_cap"],
            "expected_receipt_path": str(receipt_path.resolve()),
            "accepted_receipt_states": ["passed", "rejected", "waiting"],
            "directive_path": str(directive_path.resolve()),
            "directive_sha256": "0" * 64,
        },
    }
    directive = _directive_payload(
        future_binding=future_binding,
        preflight=preflight,
        precommit=precommit,
        semantic_plan_path=semantic_plan_path,
        plan_payload=plan_payload,
        extraction_root=extraction,
        operator_authorization_id=authorization_id,
        issued_at=issued,
        expires_at=expires,
    )
    directive_raw = _canonical_json(directive).encode("ascii")
    plan_payload["step"]["directive_sha256"] = _sha256_bytes(directive_raw)

    root.mkdir(parents=True, exist_ok=True)
    selection_policy = development_selection_policy()
    selection_policy_record = _write_immutable_json(
        root / SELECTION_POLICY_FILENAME, selection_policy["policy"]
    )
    preflight_record = _write_immutable_json(root / PREFLIGHT_FILENAME, preflight)
    precommit_record = _write_immutable_json(root / PRECOMMIT_FILENAME, precommit)
    future_record = _write_immutable_json(
        root / FUTURE_BINDING_FILENAME, future_binding
    )
    directive_record = _write_immutable_json(directive_path, directive)
    plan_record = _write_immutable_json(semantic_plan_path, plan_payload)
    runtime_binding = extraction_runtime.build_runtime_dependency_binding()
    quality_evidence = quality_runtime_evidence_contract()
    contract = {
        "schema_version": CONTROLLER_CONTRACT_VERSION,
        "state": "planned_zero_call",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "authorized_by": "kolby",
        "operator_authorization_id": authorization_id,
        "project_root": str(project),
        "controller_root": str(root),
        "extraction_root": str(extraction),
        "expected_extraction_receipt_path": str(receipt_path.resolve()),
        "controller_source": _record(Path(__file__)),
        "runtime_binding": copy.deepcopy(runtime_binding),
        "manifest": manifest_record,
        "episodes": episodes_record,
        "capacity_policy": capacity_record,
        "dry_preflight": preflight_record,
        "precommit": precommit_record,
        "future_plan_binding": future_record,
        "directive": directive_record,
        "semantic_plan": plan_record,
        "development_selection_policy": selection_policy_record,
        "development_selection_policy_sha256": selection_policy["policy_sha256"],
        "exact_model_call_cap": limits["exact_model_call_cap"],
        "exact_total_token_cap": limits["exact_total_token_cap"],
        "quality_handoff_path": str((root / QUALITY_HANDOFF_FILENAME).resolve()),
        "quality_input_schema_version": QUALITY_INPUT_VERSION,
        "quality_interface_state": "strict_typed_boundary_implementation_external",
        "quality_evaluator_contract_sha256": quality_evidence[
            "quality_evaluator_contract_sha256"
        ],
        "quality_runtime_evidence_contract": quality_evidence["contract"],
        "quality_runtime_evidence_contract_sha256": quality_evidence[
            "contract_sha256"
        ],
        "quality_model_calls_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "caller_capacity_admission_allowed": False,
        "caller_client_factory_allowed": False,
        "api_key_auth_allowed": False,
        "raw_session_token_auth_allowed": False,
        "codex_exec_allowed": False,
        "semantic_model_call_count": 0,
    }
    contract_record = _write_immutable_json(root / CONTRACT_FILENAME, contract)
    loaded = load_epoch7_controller(
        root,
        project_root=project,
        now=issued + ((expires - issued) / 2),
    )
    return {
        "contract": loaded["contract"],
        "contract_record": contract_record,
        "semantic_plan_record": plan_record,
        "directive_record": directive_record,
        "development_selection_policy_record": selection_policy_record,
        "exact_model_call_cap": limits["exact_model_call_cap"],
        "exact_total_token_cap": limits["exact_total_token_cap"],
        "semantic_model_call_count": 0,
    }


def load_epoch7_controller(
    controller_root: Path,
    *,
    project_root: Path | None = None,
    now: datetime | None = None,
    require_fresh: bool = False,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(controller_root, project_root=project, label="controller root")
    _validate_controller_directory(root)
    contract_path = root / CONTRACT_FILENAME
    contract = _load_object(contract_path, label="epoch-7 controller contract")
    expected_keys = {
        "schema_version",
        "state",
        "thread_id",
        "plan_epoch",
        "step_id",
        "authorized_by",
        "operator_authorization_id",
        "project_root",
        "controller_root",
        "extraction_root",
        "expected_extraction_receipt_path",
        "controller_source",
        "runtime_binding",
        "manifest",
        "episodes",
        "capacity_policy",
        "dry_preflight",
        "precommit",
        "future_plan_binding",
        "directive",
        "semantic_plan",
        "development_selection_policy",
        "development_selection_policy_sha256",
        "exact_model_call_cap",
        "exact_total_token_cap",
        "quality_handoff_path",
        "quality_input_schema_version",
        "quality_interface_state",
        "quality_evaluator_contract_sha256",
        "quality_runtime_evidence_contract",
        "quality_runtime_evidence_contract_sha256",
        "quality_model_calls_authorized",
        "holdout_authorized",
        "production_mutation_allowed",
        "caller_capacity_admission_allowed",
        "caller_client_factory_allowed",
        "api_key_auth_allowed",
        "raw_session_token_auth_allowed",
        "codex_exec_allowed",
        "semantic_model_call_count",
    }
    if set(contract) != expected_keys:
        raise Epoch7ControllerError("epoch-7 controller contract schema drifted")
    if (
        contract.get("schema_version") != CONTROLLER_CONTRACT_VERSION
        or contract.get("state") != "planned_zero_call"
        or contract.get("thread_id") != supervisor.TARGET_THREAD_ID
        or contract.get("plan_epoch") != PLAN_EPOCH
        or contract.get("step_id") != STEP_ID
        or contract.get("authorized_by") != "kolby"
        or contract.get("project_root") != str(project)
        or contract.get("controller_root") != str(root)
        or contract.get("quality_input_schema_version") != QUALITY_INPUT_VERSION
        or contract.get("quality_interface_state")
        != "strict_typed_boundary_implementation_external"
        or contract.get("quality_model_calls_authorized") is not False
        or contract.get("holdout_authorized") is not False
        or contract.get("production_mutation_allowed") is not False
        or contract.get("caller_capacity_admission_allowed") is not False
        or contract.get("caller_client_factory_allowed") is not False
        or contract.get("api_key_auth_allowed") is not False
        or contract.get("raw_session_token_auth_allowed") is not False
        or contract.get("codex_exec_allowed") is not False
        or contract.get("semantic_model_call_count") != 0
    ):
        raise Epoch7ControllerError("epoch-7 controller authority drifted")
    quality_evidence = quality_runtime_evidence_contract()
    if (
        contract.get("quality_evaluator_contract_sha256")
        != quality_evidence["quality_evaluator_contract_sha256"]
        or contract.get("quality_runtime_evidence_contract")
        != quality_evidence["contract"]
        or contract.get("quality_runtime_evidence_contract_sha256")
        != quality_evidence["contract_sha256"]
    ):
        raise Epoch7ControllerError("quality runtime evidence contract drifted")
    _authorization_id(contract.get("operator_authorization_id"))
    _verify_record(contract["controller_source"], label="controller source")
    if contract["controller_source"] != _record(Path(__file__)):
        raise Epoch7ControllerError("epoch-7 controller source drifted")

    records: dict[str, Path] = {}
    for key in (
        "manifest",
        "episodes",
        "capacity_policy",
        "dry_preflight",
        "precommit",
        "future_plan_binding",
        "directive",
        "semantic_plan",
        "development_selection_policy",
    ):
        records[key] = _verify_record(
            contract[key], label=key, allowed_root=project
        )
    selection_policy = development_selection_policy()
    if (
        _load_object(
            records["development_selection_policy"],
            label="development selection policy",
        )
        != selection_policy["policy"]
        or contract.get("development_selection_policy_sha256")
        != selection_policy["policy_sha256"]
    ):
        raise Epoch7ControllerError("development selection policy drifted")
    extraction_root = _safe_path(
        Path(str(contract["extraction_root"])),
        project_root=project,
        label="extraction root",
    )
    if contract.get("expected_extraction_receipt_path") != str(
        (extraction_root / extraction_runtime.EXTRACTION_RECEIPT_FILENAME).resolve()
    ):
        raise Epoch7ControllerError("expected extraction receipt path drifted")
    if contract.get("quality_handoff_path") != str(
        (root / QUALITY_HANDOFF_FILENAME).resolve()
    ):
        raise Epoch7ControllerError("quality handoff path drifted")

    episodes = _load_array(records["episodes"], label="epoch-7 episodes")
    preflight = matrix.build_matrix_dry_preflight(
        manifest_path=records["manifest"],
        manifest_sha256=contract["manifest"]["sha256"],
        episodes=episodes,
        capacity_policy_path=records["capacity_policy"],
        capacity_policy_sha256=contract["capacity_policy"]["sha256"],
        allowed_root=project,
    )
    if _load_object(records["dry_preflight"], label="dry preflight") != preflight:
        raise Epoch7ControllerError("dry preflight drifted")
    precommit = matrix.build_matrix_precommit(preflight)
    if _load_object(records["precommit"], label="precommit") != precommit:
        raise Epoch7ControllerError("precommit drifted")
    future_binding = matrix.build_future_plan_binding(
        precommit, output_root=extraction_root
    )
    if (
        _load_object(records["future_plan_binding"], label="future plan binding")
        != future_binding
    ):
        raise Epoch7ControllerError("future plan binding drifted")
    current_runtime_binding = extraction_runtime.build_runtime_dependency_binding()
    if contract.get("runtime_binding") != current_runtime_binding:
        raise Epoch7ControllerError("extraction runtime dependency binding drifted")
    limits = extraction_runtime._capacity_limits(preflight)
    if (
        contract.get("exact_model_call_cap") != limits["exact_model_call_cap"]
        or contract.get("exact_total_token_cap") != limits["exact_total_token_cap"]
    ):
        raise Epoch7ControllerError("controller call or token cap drifted")

    plan_payload = _load_object(records["semantic_plan"], label="semantic plan")
    directive = _load_object(records["directive"], label="semantic directive")
    try:
        plan = supervisor.read_semantic_plan(
            records["semantic_plan"],
            thread_id=supervisor.TARGET_THREAD_ID,
            project_root=project,
        )
    except Exception as exc:
        raise Epoch7ControllerError("semantic plan failed strict parsing") from exc
    issued = _parse_timestamp(directive.get("issued_at"), label="directive issued_at")
    validation_time = now or (issued + timedelta(seconds=1))
    if require_fresh:
        validation_time = now or datetime.now(timezone.utc)
    try:
        authority = extraction_runtime._validate_semantic_epoch_authority(
            semantic_plan_path=records["semantic_plan"],
            semantic_plan=plan,
            semantic_plan_payload=plan_payload,
            directive=directive,
            directive_sha256=contract["directive"]["sha256"],
            preflight=preflight,
            precommit=precommit,
            future_plan_binding=future_binding,
            runtime_binding=current_runtime_binding,
            output_root=extraction_root,
            limits=limits,
            now=validation_time,
            offline_test_mode=False,
        )
    except Exception as exc:
        raise Epoch7ControllerError("epoch-7 semantic authority failed validation") from exc
    if directive.get("operator_authorization_id") != contract[
        "operator_authorization_id"
    ]:
        raise Epoch7ControllerError("operator authorization cross-binding drifted")
    return {
        "contract": contract,
        "contract_path": contract_path,
        "records": records,
        "episodes": episodes,
        "preflight": preflight,
        "precommit": precommit,
        "future_binding": future_binding,
        "runtime_binding": current_runtime_binding,
        "limits": limits,
        "plan": plan,
        "plan_payload": plan_payload,
        "directive": directive,
        "authority": authority,
        "controller_root": root,
        "extraction_root": extraction_root,
        "project_root": project,
    }


def _artifact_manifest(root: Path) -> dict[str, Any]:
    if not root.exists():
        rows: list[dict[str, Any]] = []
    else:
        if root.is_symlink() or not root.is_dir():
            raise Epoch7ControllerError("extraction root is not a real directory")
        rows = []
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise Epoch7ControllerError("extraction root contains a symlink")
            if path.is_file():
                record = _record(path, allowed_root=root)
                rows.append(
                    {
                        "relative_path": str(path.resolve().relative_to(root.resolve())),
                        "sha256": record["sha256"],
                        "size": record["size"],
                    }
                )
    return {
        "files": rows,
        "file_count": len(rows),
        "manifest_sha256": _sha256_bytes(_canonical_json(rows).encode("ascii")),
    }


def _receipt_checksum(payload: Mapping[str, Any]) -> str:
    unhashed = dict(payload)
    unhashed.pop("receipt_sha256", None)
    return _sha256_bytes(_canonical_json(unhashed).encode("ascii"))


def _write_controller_receipt(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(payload))
    value["receipt_sha256"] = _receipt_checksum(value)
    _write_immutable_json(path, value)
    return value


def _revalidate_extraction_runtime_artifacts(
    loaded: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebuild the live extraction receipt from the exact runtime artifacts."""

    root = Path(loaded["extraction_root"])
    arms_root = root / "arms"
    capacity_root = root / "capacity"
    expected_root_entries = {
        "runtime-binding.json",
        "offline-preflight.json",
        "matrix-precommit.json",
        "arms",
        "capacity",
        extraction_runtime.EXTRACTION_RECEIPT_FILENAME,
    }
    root_entries = extraction_runtime._directory_entries(
        root, "epoch-7 extraction root"
    )
    if set(root_entries) != expected_root_entries:
        raise Epoch7ControllerError("extraction runtime root artifact set drifted")
    if (
        extraction_runtime._load_object(
            root / "runtime-binding.json", "epoch-7 runtime binding"
        )
        != loaded["runtime_binding"]["binding"]
        or extraction_runtime._load_object(
            root / "offline-preflight.json", "epoch-7 runtime preflight"
        )
        != loaded["preflight"]["receipt"]
        or extraction_runtime._load_object(
            root / "matrix-precommit.json", "epoch-7 runtime precommit"
        )
        != loaded["precommit"]["precommit"]
    ):
        raise Epoch7ControllerError("extraction runtime control plane drifted")

    arm_specs = loaded["precommit"]["precommit"]["arms"]
    if [
        (row.get("batch_size"), row.get("thread_mode")) for row in arm_specs
    ] != list(extraction_runtime.EXPECTED_ARMS):
        raise Epoch7ControllerError("extraction runtime arm order drifted")
    expected_variants = {str(row["variant_id"]) for row in arm_specs}
    arm_entries = extraction_runtime._directory_entries(
        arms_root, "epoch-7 extraction arms root"
    )
    if set(arm_entries) != expected_variants or any(
        not extraction_runtime._is_real_directory(path)
        for path in arm_entries.values()
    ):
        raise Epoch7ControllerError("extraction runtime arm membership drifted")
    extraction_runtime._validate_capacity_tree(capacity_root, expected_variants)

    limits = loaded["limits"]
    preflight = loaded["preflight"]
    precommit = loaded["precommit"]
    future_binding = loaded["future_binding"]
    semantic_authority = loaded["authority"]
    runtime_binding = loaded["runtime_binding"]
    all_turn_ids: set[str] = set()
    all_thread_ids: set[str] = set()
    all_measurement_ids: set[str] = set()
    all_admission_ids: set[str] = set()
    outputs_by_variant: dict[str, list[dict[str, Any]]] = {}
    usage_values: list[Mapping[str, Any]] = []
    total_turn_wall = 0.0
    used_calls = 0
    arm_receipts: list[dict[str, Any]] = []

    for arm in arm_specs:
        variant_id = str(arm["variant_id"])
        arm_dir = arms_root / variant_id
        requests = preflight["requests_by_variant"].get(variant_id)
        if not isinstance(requests, list) or len(requests) != arm["request_count"]:
            raise Epoch7ControllerError("extraction preflight request set drifted")
        extraction_runtime._validate_arm_directory_contents(
            arm_dir, requests, envelope_required=True
        )
        report_path = arm_dir / "report.json"
        envelope_path = arm_dir / "arm-envelope.json"
        report = extraction_runtime._load_object(
            report_path, f"{variant_id} report"
        )
        envelope = extraction_runtime._load_object(
            envelope_path, f"{variant_id} envelope"
        )
        (
            measurement,
            admission,
            prior_artifact_validation,
            capacity_records,
        ) = extraction_runtime._validate_envelope_runtime_extensions(
            envelope,
            semantic_authority=semantic_authority,
            runtime_binding=runtime_binding,
            arm_root=arm_dir,
            capacity_root=capacity_root,
        )
        probe_context = extraction_runtime._probe_context(
            arm=arm,
            preflight=preflight,
            precommit=precommit,
            future_plan_binding=future_binding,
            directive_info=semantic_authority["directive_info"],
            runtime_binding=runtime_binding,
            limits=limits,
            used_calls=used_calls,
            used_total_tokens=sum(
                int(value["total_tokens"]) for value in usage_values
            ),
            offline_test_mode=False,
        )
        extraction_runtime._validate_capacity_probe_request(
            capacity_records["request"],
            expected_context=probe_context,
            expected_sequence=0,
            expected_boundary="initial_arm_admission",
            expected_semantic_thread_started=False,
            expected_semantic_thread_resumed=False,
        )
        measurement_sha = _sha256_bytes(
            _canonical_json(measurement).encode("ascii")
        )
        measurement_info = extraction_runtime.validate_capacity_measurement(
            measurement,
            expected_context=probe_context,
            expected_sha256=measurement_sha,
            now=extraction_runtime._historical_validation_time(admission),
            historical=True,
            limits=limits,
        )
        admission_sha = _sha256_bytes(_canonical_json(admission).encode("ascii"))
        admission_id = admission.get("admission_id")
        if not isinstance(admission_id, str) or not admission_id:
            raise Epoch7ControllerError("extraction capacity admission ID is absent")
        admission_info = {
            "admission": admission,
            "admission_sha256": admission_sha,
            "admission_id": admission_id,
        }
        validated_admission = matrix.validate_capacity_admission(
            admission,
            expected_sha256=admission_sha,
            plan_binding=future_binding,
            directive_info=semantic_authority["directive_info"],
            precommit_bundle=precommit,
            variant_id=variant_id,
            now=extraction_runtime._historical_validation_time(admission),
        )
        admission_info["admission"] = validated_admission["admission"]
        artifact_validation = extraction_runtime.load_full_canonical_arm_outputs(
            arm_dir=arm_dir,
            report=report,
            requests=requests,
            request_sha256s=arm["request_sha256s"],
            manifest_info=preflight["manifest_info"],
        )
        if prior_artifact_validation != artifact_validation:
            raise Epoch7ControllerError(
                "extraction arm artifact validation receipt drifted"
            )
        reprobe_ids = extraction_runtime._validate_capacity_reprobes(
            capacity_records["reprobes"],
            limits=limits,
            offline_test_mode=False,
        )
        extraction_runtime._validate_capacity_reprobe_coverage(
            capacity_records["reprobes"],
            artifact_validation=artifact_validation,
            offline_test_mode=False,
        )
        envelope_sha = _sha256_bytes(_canonical_json(envelope).encode("ascii"))
        envelope_info = matrix.validate_arm_envelope(
            envelope,
            expected_sha256=envelope_sha,
            precommit_bundle=precommit,
            plan_binding=future_binding,
            directive_info=semantic_authority["directive_info"],
            admission_info=admission_info,
            manifest_info=preflight["manifest_info"],
        )

        arm_measurement_ids = {
            str(measurement_info["measurement_id"]),
            *reprobe_ids,
        }
        if (
            len(arm_measurement_ids) != 1 + len(reprobe_ids)
            or all_measurement_ids & arm_measurement_ids
            or admission_id in all_admission_ids
        ):
            raise Epoch7ControllerError(
                "extraction capacity identity was replayed across arms"
            )
        all_measurement_ids.update(arm_measurement_ids)
        all_admission_ids.add(admission_id)

        cap_accounting = extraction_runtime._validate_runtime_report_caps(
            report,
            arm=arm,
            limits=limits,
            artifact_validation=artifact_validation,
        )
        verified_turns = artifact_validation["verified_turns"]
        turn_ids = {str(row["turn_id"]) for row in verified_turns}
        thread_ids = {str(row["thread_id"]) for row in verified_turns}
        if all_turn_ids & turn_ids or all_thread_ids & thread_ids:
            raise Epoch7ControllerError(
                "extraction thread or turn identity was replayed across arms"
            )
        all_turn_ids.update(turn_ids)
        all_thread_ids.update(thread_ids)
        used_calls += int(artifact_validation["verified_attempt_count"])
        usage_values.append(cap_accounting["usage"])
        total_turn_wall += float(cap_accounting["turn_wall_elapsed_seconds"])
        if (
            used_calls > limits["exact_model_call_cap"]
            or sum(int(value["total_tokens"]) for value in usage_values)
            > limits["exact_total_token_cap"]
        ):
            raise Epoch7ControllerError("extraction global call or token cap exceeded")
        outputs_by_variant[variant_id] = copy.deepcopy(envelope["outputs"])
        arm_receipts.append(
            {
                "variant_id": variant_id,
                "batch_size": arm["batch_size"],
                "thread_mode": arm["thread_mode"],
                "request_count": arm["request_count"],
                "request_set_sha256": arm["request_set_sha256"],
                "capacity_request": copy.deepcopy(
                    dict(capacity_records["request_record"])
                ),
                "capacity_measurement": copy.deepcopy(
                    dict(capacity_records["measurement_record"])
                ),
                "capacity_admission": copy.deepcopy(
                    dict(capacity_records["admission_record"])
                ),
                "capacity_reprobes": copy.deepcopy(
                    list(capacity_records["reprobes"])
                ),
                "offline_test_mode": False,
                "report": extraction_runtime._record(report_path),
                "arm_envelope": extraction_runtime._record(envelope_path),
                "arm_envelope_sha256": envelope_info["envelope_sha256"],
                "outputs_sha256": artifact_validation["outputs_sha256"],
                "usage": cap_accounting["usage"],
                "verified_attempt_count": artifact_validation[
                    "verified_attempt_count"
                ],
                "verified_turn_ids": [
                    row["turn_id"] for row in verified_turns
                ],
            }
        )

    if used_calls != limits["exact_model_call_cap"]:
        raise Epoch7ControllerError("extraction exact call surface is incomplete")
    full_outputs = matrix.validate_full_v31_outputs(
        outputs_by_variant, manifest_info=preflight["manifest_info"]
    )
    total_usage = extraction_runtime._sum_usage(usage_values)

    lock_path, lock_binding = extraction_runtime._preauthority_lock_binding(
        {
            "output_root": root,
            "semantic_plan_path": loaded["records"]["semantic_plan"],
            "thread_id": supervisor.TARGET_THREAD_ID,
            "project_root": loaded["project_root"],
            "manifest_sha256": loaded["contract"]["manifest"]["sha256"],
            "capacity_policy_sha256": loaded["contract"]["capacity_policy"][
                "sha256"
            ],
            "future_plan_binding": future_binding,
        }
    )
    if extraction_runtime._load_object(
        lock_path, "epoch-7 persistent invocation lock"
    ) != lock_binding:
        raise Epoch7ControllerError("extraction invocation lock binding drifted")
    lock_info = {
        "binding": lock_binding,
        "binding_sha256": _sha256_bytes(
            _canonical_json(lock_binding).encode("ascii")
        ),
        "record": extraction_runtime._record(lock_path),
    }
    return {
        "arm_receipts": arm_receipts,
        "full_output_validation": full_outputs,
        "usage": total_usage,
        "wall_seconds": total_turn_wall,
        "verified_attempt_count": used_calls,
        "lock_info": lock_info,
    }


def _expected_live_extraction_receipt_static(
    loaded: Mapping[str, Any], rebuilt: Mapping[str, Any]
) -> dict[str, Any]:
    preflight = loaded["preflight"]
    authority = loaded["authority"]
    limits = loaded["limits"]
    usage = rebuilt["usage"]
    lock_info = rebuilt["lock_info"]
    zero_usage = {field: 0 for field in extraction_runtime.USAGE_FIELDS}
    return {
        "schema_version": supervisor.SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION,
        "receipt_contract_version": extraction_runtime.EXTRACTION_RECEIPT_VERSION,
        "thread_id": authority["thread_id"],
        "plan_epoch": loaded["plan"].plan_epoch,
        "step_id": loaded["plan"].step_id,
        "state": "passed",
        "waiting_reason": None,
        "candidate_system_id": matrix.adapter.CANDIDATE_SYSTEM_ID,
        "semantic_plan_sha256": authority["semantic_plan_sha256"],
        "semantic_plan_contract_sha256": authority["semantic_plan_contract"][
            "contract_sha256"
        ],
        "future_plan_binding_sha256": loaded["future_binding"]["binding_sha256"],
        "future_directive_sha256": authority["directive_sha256"],
        "precommit_sha256": loaded["precommit"]["precommit_sha256"],
        "manifest_sha256": preflight["receipt"]["manifest_sha256"],
        "context_set_sha256": preflight["receipt"]["context_set_sha256"],
        "adapter_runtime_binding_sha256": preflight["receipt"][
            "runtime_binding_sha256"
        ],
        "live_runtime_binding_sha256": loaded["runtime_binding"]["binding_sha256"],
        "invocation_lock_binding_sha256": lock_info["binding_sha256"],
        "invocation_lock_record": copy.deepcopy(dict(lock_info["record"])),
        "context_control_overlay_sha256": preflight["receipt"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": preflight["receipt"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": preflight["receipt"]["capacity_policy_sha256"],
        "exact_model_call_cap": limits["exact_model_call_cap"],
        "measured_model_calls": rebuilt["verified_attempt_count"],
        "verified_fixture_attempt_count": 0,
        "remaining_model_calls": limits["exact_model_call_cap"]
        - rebuilt["verified_attempt_count"],
        "exact_total_token_cap": limits["exact_total_token_cap"],
        "measured_usage": copy.deepcopy(dict(usage)),
        "verified_fixture_usage": zero_usage,
        "remaining_total_token_budget": limits["exact_total_token_cap"]
        - int(usage["total_tokens"]),
        "measured_turn_wall_elapsed_seconds": round(
            float(rebuilt["wall_seconds"]), 6
        ),
        "verified_fixture_turn_wall_elapsed_seconds": 0.0,
        "arm_count": len(extraction_runtime.EXPECTED_ARMS),
        "arms": copy.deepcopy(list(rebuilt["arm_receipts"])),
        "full_output_validation": copy.deepcopy(
            dict(rebuilt["full_output_validation"])
        ),
        "all_six_arms_complete": True,
        "opaque_case_order_preserved": True,
        "cross_arm_thread_reuse": False,
        "cross_arm_turn_reuse": False,
        "cross_arm_admission_reuse": False,
        "execution_mode": "live",
        "offline_test_mode": False,
        "promotable": True,
        "live_pass_authority": True,
        "live_dispatch_performed": True,
        "capacity_is_conservative_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_and_postturn_measured_stop": True,
        "trusted_capacity_implementation_state": (
            extraction_runtime.TRUSTED_CAPACITY_IMPLEMENTATION_STATE
        ),
        "extraction_only": True,
        "quality_evaluation_performed": False,
        "quality_evaluation_authorized": False,
        "quality_selection_authorized": False,
        "holdout_inspected": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "production_mutation_allowed": False,
    }


def _verify_extraction_receipt(loaded: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(str(loaded["contract"]["expected_extraction_receipt_path"]))
    receipt = _load_object(path, label="epoch-7 extraction receipt")
    try:
        rebuilt = _revalidate_extraction_runtime_artifacts(loaded)
        expected = _expected_live_extraction_receipt_static(loaded, rebuilt)
        receipt = extraction_runtime._validate_existing_receipt(
            receipt, static=expected
        )
    except Epoch7ControllerError:
        raise
    except Exception as exc:
        raise Epoch7ControllerError(
            "extraction runtime artifact reconstruction failed"
        ) from exc
    return {"receipt": receipt, "record": _record(path, allowed_root=loaded["extraction_root"])}


def _execution_receipt_path(loaded: Mapping[str, Any]) -> Path:
    return loaded["controller_root"] / EXECUTION_RECEIPT_FILENAME


def _verify_controller_execution_receipt(loaded: Mapping[str, Any]) -> dict[str, Any]:
    path = _execution_receipt_path(loaded)
    receipt = _load_object(path, label="epoch-7 controller execution receipt")
    if (
        receipt.get("schema_version") != CONTROLLER_EXECUTION_VERSION
        or receipt.get("state") not in {"passed", "waiting"}
        or receipt.get("thread_id") != supervisor.TARGET_THREAD_ID
        or receipt.get("plan_epoch") != PLAN_EPOCH
        or receipt.get("step_id") != STEP_ID
        or receipt.get("authorized_by") != "kolby"
        or receipt.get("operator_authorization_id")
        != loaded["contract"]["operator_authorization_id"]
        or receipt.get("controller_contract")
        != _record(loaded["contract_path"], allowed_root=loaded["controller_root"])
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("replay_authorized") is not False
        or receipt.get("quality_runtime_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("receipt_sha256") != _receipt_checksum(receipt)
    ):
        raise Epoch7ControllerError("controller execution receipt drifted")
    manifest = _artifact_manifest(loaded["extraction_root"])
    if receipt.get("extraction_artifact_manifest") != manifest:
        raise Epoch7ControllerError("controller extraction artifact manifest drifted")
    if receipt["state"] == "passed":
        extraction = _verify_extraction_receipt(loaded)
        if receipt.get("extraction_receipt") != extraction["record"]:
            raise Epoch7ControllerError("controller extraction receipt binding drifted")
    elif receipt.get("extraction_receipt") is not None:
        raise Epoch7ControllerError("waiting controller receipt claimed extraction pass")
    return receipt


async def _execute_epoch7_locked(
    loaded: Mapping[str, Any], authorization_id: str
) -> dict[str, Any]:
    extraction_runtime._reject_external_auth_material()
    execution_path = _execution_receipt_path(loaded)
    if execution_path.exists():
        return _verify_controller_execution_receipt(loaded)

    fresh_calls = 0
    cancelled = False
    try:
        if Path(loaded["contract"]["expected_extraction_receipt_path"]).exists():
            extraction = _verify_extraction_receipt(loaded)
            adopted = True
        else:
            result = await extraction_runtime.run_canonical_v31_development_matrix_runtime(
                output_root=loaded["extraction_root"],
                semantic_plan_path=loaded["records"]["semantic_plan"],
                thread_id=supervisor.TARGET_THREAD_ID,
                project_root=loaded["project_root"],
                manifest_path=loaded["records"]["manifest"],
                manifest_sha256=loaded["contract"]["manifest"]["sha256"],
                episodes=loaded["episodes"],
                capacity_policy_path=loaded["records"]["capacity_policy"],
                capacity_policy_sha256=loaded["contract"]["capacity_policy"]["sha256"],
                future_plan_binding=loaded["future_binding"],
            )
            if not isinstance(result, Mapping):
                raise Epoch7ControllerError("extraction runtime returned no result")
            fresh_calls = int(result.get("semantic_calls_started_by_this_invocation", 0))
            extraction = _verify_extraction_receipt(loaded)
            adopted = False
        payload = {
            "schema_version": CONTROLLER_EXECUTION_VERSION,
            "state": "passed",
            "terminal_reason": "epoch7_extraction_runtime_passed_quality_handoff_required",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "thread_id": supervisor.TARGET_THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "authorized_by": "kolby",
            "operator_authorization_id": authorization_id,
            "controller_contract": _record(
                loaded["contract_path"], allowed_root=loaded["controller_root"]
            ),
            "extraction_receipt": extraction["record"],
            "extraction_artifact_manifest": _artifact_manifest(
                loaded["extraction_root"]
            ),
            "extraction_receipt_adopted": adopted,
            "semantic_calls_started_by_controller": fresh_calls,
            "semantic_retry_count": 0,
            "replay_authorized": False,
            "quality_runtime_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
        }
    except (Exception, asyncio.CancelledError) as exc:
        cancelled = isinstance(exc, asyncio.CancelledError)
        payload = {
            "schema_version": CONTROLLER_EXECUTION_VERSION,
            "state": "waiting",
            "terminal_reason": "epoch7_extraction_runtime_waiting_no_replay",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "thread_id": supervisor.TARGET_THREAD_ID,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "authorized_by": "kolby",
            "operator_authorization_id": authorization_id,
            "controller_contract": _record(
                loaded["contract_path"], allowed_root=loaded["controller_root"]
            ),
            "extraction_receipt": None,
            "extraction_artifact_manifest": _artifact_manifest(
                loaded["extraction_root"]
            ),
            "extraction_receipt_adopted": False,
            "semantic_calls_started_by_controller": None,
            "usage_status": "unknown_nonpromotable",
            "blocker_class": type(exc).__name__,
            "blocker_detail": str(exc)[:500],
            "semantic_retry_count": 0,
            "replay_authorized": False,
            "quality_runtime_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
        }
    _write_controller_receipt(execution_path, payload)
    verified = _verify_controller_execution_receipt(loaded)
    if cancelled:
        raise asyncio.CancelledError
    return verified


async def execute_epoch7(
    *,
    controller_root: Path,
    operator_authorization_id: str,
    project_root: Path | None = None,
) -> dict[str, Any]:
    authorization_id = _authorization_id(operator_authorization_id)
    loaded = load_epoch7_controller(
        controller_root,
        project_root=project_root,
        now=datetime.now(timezone.utc),
        require_fresh=True,
    )
    if authorization_id != loaded["contract"]["operator_authorization_id"]:
        raise Epoch7ControllerError(
            "execute authorization ID does not match the frozen plan"
        )
    with _ControllerWriterLock(loaded["controller_root"]):
        return await _execute_epoch7_locked(loaded, authorization_id)


def status_epoch7(
    controller_root: Path,
    *,
    project_root: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    loaded = load_epoch7_controller(controller_root, project_root=project_root)
    execution_path = _execution_receipt_path(loaded)
    if execution_path.exists():
        execution = _verify_controller_execution_receipt(loaded)
        state = execution["state"]
        reason = execution["terminal_reason"]
    else:
        current = now or datetime.now(timezone.utc)
        expires = _parse_timestamp(
            loaded["directive"].get("expires_at"), label="directive expires_at"
        )
        state = "ready" if current < expires else "waiting"
        reason = (
            "ready_for_explicit_authorized_execute"
            if state == "ready"
            else "epoch7_directive_expired_fresh_plan_required"
        )
    return {
        "schema_version": "pif_canonical_v31_epoch7_controller_status_v1",
        "state": state,
        "reason": reason,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "controller_contract": _record(
            loaded["contract_path"], allowed_root=loaded["controller_root"]
        ),
        "extraction_receipt_exists": Path(
            loaded["contract"]["expected_extraction_receipt_path"]
        ).is_file(),
        "controller_execution_receipt_exists": execution_path.is_file(),
        "quality_handoff_exists": (loaded["controller_root"] / QUALITY_HANDOFF_FILENAME).is_file(),
        "semantic_model_call_count_added_by_status": 0,
    }


def _quality_handoff_payload(
    *,
    loaded: Mapping[str, Any],
    execution: Mapping[str, Any],
    extraction: Mapping[str, Any],
) -> dict[str, Any]:
    quality_evidence = quality_runtime_evidence_contract()
    return {
        "schema_version": QUALITY_HANDOFF_VERSION,
        "state": "ready_for_separately_checksum_bound_quality_runtime",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "candidate_system_id": matrix.adapter.CANDIDATE_SYSTEM_ID,
        "quality_input_schema_version": QUALITY_INPUT_VERSION,
        "quality_evaluator_contract": quality_evidence[
            "quality_evaluator_contract"
        ],
        "quality_evaluator_contract_sha256": quality_evidence[
            "quality_evaluator_contract_sha256"
        ],
        "quality_runtime_evidence_contract": quality_evidence["contract"],
        "quality_runtime_evidence_contract_sha256": quality_evidence[
            "contract_sha256"
        ],
        "development_selection_policy": loaded["contract"][
            "development_selection_policy"
        ],
        "development_selection_policy_sha256": loaded["contract"][
            "development_selection_policy_sha256"
        ],
        "controller_contract": _record(
            loaded["contract_path"], allowed_root=loaded["controller_root"]
        ),
        "controller_execution_receipt": _record(
            _execution_receipt_path(loaded), allowed_root=loaded["controller_root"]
        ),
        "extraction_receipt": extraction["record"],
        "extraction_receipt_contract_version": extraction["receipt"][
            "receipt_contract_version"
        ],
        "extraction_manifest_sha256": extraction["receipt"]["manifest_sha256"],
        "extraction_context_set_sha256": extraction["receipt"]["context_set_sha256"],
        "full_output_validation": copy.deepcopy(
            extraction["receipt"]["full_output_validation"]
        ),
        "measured_extraction_usage": copy.deepcopy(
            extraction["receipt"]["measured_usage"]
        ),
        "quality_interface_state": "external_runtime_must_bind_this_exact_handoff",
        "quality_runtime_authorized": False,
        "quality_model_call_count": 0,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def verify_quality_handoff(
    controller_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Rebuild the only extraction input a later quality runtime may consume."""

    loaded = load_epoch7_controller(controller_root, project_root=project_root)
    execution = _verify_controller_execution_receipt(loaded)
    if execution["state"] != "passed":
        raise Epoch7ControllerError(
            "quality handoff requires a passed extraction controller receipt"
        )
    extraction = _verify_extraction_receipt(loaded)
    expected = _quality_handoff_payload(
        loaded=loaded,
        execution=execution,
        extraction=extraction,
    )
    path = loaded["controller_root"] / QUALITY_HANDOFF_FILENAME
    observed = _load_object(path, label="quality handoff")
    if observed != expected:
        raise Epoch7ControllerError("quality handoff drifted")
    return {
        "handoff": observed,
        "handoff_record": _record(path, allowed_root=loaded["controller_root"]),
        "extraction_receipt": extraction["receipt"],
        "extraction_receipt_record": extraction["record"],
    }


def verify_epoch7(
    controller_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    loaded = load_epoch7_controller(controller_root, project_root=project_root)
    execution = _verify_controller_execution_receipt(loaded)
    if execution["state"] != "passed":
        return {
            "schema_version": QUALITY_HANDOFF_VERSION,
            "state": "waiting",
            "reason": execution["terminal_reason"],
            "quality_runtime_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
        }
    extraction = _verify_extraction_receipt(loaded)
    handoff = _quality_handoff_payload(
        loaded=loaded,
        execution=execution,
        extraction=extraction,
    )
    path = loaded["controller_root"] / QUALITY_HANDOFF_FILENAME
    _write_immutable_json(path, handoff)
    return verify_quality_handoff(
        controller_root,
        project_root=project_root,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m research_factory.app_server_canonical_v31_epoch7_controller"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--controller-root", type=Path, required=True)
    plan.add_argument("--extraction-root", type=Path, required=True)
    plan.add_argument("--manifest", type=Path, required=True)
    plan.add_argument("--episodes", type=Path, required=True)
    plan.add_argument("--capacity-policy", type=Path, required=True)
    plan.add_argument("--operator-authorization-id", required=True)
    plan.add_argument("--issued-at", required=True)
    plan.add_argument("--expires-at", required=True)
    status = commands.add_parser("status")
    status.add_argument("--controller-root", type=Path, required=True)
    execute = commands.add_parser("execute")
    execute.add_argument("--controller-root", type=Path, required=True)
    execute.add_argument("--operator-authorization-id", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--controller-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            result = freeze_epoch7_plan(
                controller_root=args.controller_root,
                extraction_root=args.extraction_root,
                manifest_path=args.manifest,
                episodes_path=args.episodes,
                capacity_policy_path=args.capacity_policy,
                operator_authorization_id=args.operator_authorization_id,
                issued_at=args.issued_at,
                expires_at=args.expires_at,
            )
        elif args.command == "status":
            result = status_epoch7(args.controller_root)
        elif args.command == "execute":
            result = asyncio.run(
                execute_epoch7(
                    controller_root=args.controller_root,
                    operator_authorization_id=args.operator_authorization_id,
                )
            )
        else:
            result = verify_epoch7(args.controller_root)
    except Epoch7ControllerError as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="", file=sys.stderr)
        return 2
    print(_pretty_json({"ok": True, "result": result}), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
