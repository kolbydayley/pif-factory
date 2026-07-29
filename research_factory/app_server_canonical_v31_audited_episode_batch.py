from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_evidence_episode_batch as bounded


PROJECT_ROOT = bounded.PROJECT_ROOT
MODEL = bounded.MODEL
EFFORT = bounded.EFFORT
PINNED_CODEX = bounded.PINNED_CODEX
CANONICAL_LABEL_PACK = bounded.CANONICAL_LABEL_PACK
CANONICAL_EVIDENCE_MAX_CHARS = bounded.CANONICAL_EVIDENCE_MAX_CHARS
CanonicalV31EpisodeBatchError = bounded.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = bounded.CanonicalV31OutputError
CanonicalV31TelemetryError = bounded.CanonicalV31TelemetryError
_client_factory = bounded._client_factory
expected_instruction_source_contract = bounded.expected_instruction_source_contract

CANONICAL_SELF_AUDIT_INSTRUCTION = (
    "\n# Canonical cross-field self-audit\n"
    "Before returning, audit every emitted segment, event, candidate, and unit "
    "receipt against the complete output schema and the selected exact evidence. "
    "For each metric, use the fully not-applicable form only when value, unit, "
    "comparator, and raw_text are all null and direction is not_applicable. Any "
    "other direction requires non-null raw_text that is an exact contiguous "
    "substring of the selected evidence, and every non-null value, unit, or "
    "comparator must occur in raw_text or that evidence. Recheck all enum, "
    "nullability, coded/no-signal, evidence-span, source-order, coverage-count, "
    "and receipt relationships before submission. Do not invent or deterministically "
    "repair semantics to satisfy a field; omit an unsupported item instead.\n"
)
CANONICAL_SELF_AUDIT_INSTRUCTION_SHA256 = hashlib.sha256(
    CANONICAL_SELF_AUDIT_INSTRUCTION.encode("ascii")
).hexdigest()


def _audited_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    original = value.get("base_instructions")
    if not isinstance(original, str) or not original:
        raise CanonicalV31EpisodeBatchError("base instructions are unavailable")
    if CANONICAL_SELF_AUDIT_INSTRUCTION.strip() in original:
        raise CanonicalV31EpisodeBatchError("canonical self-audit instruction was duplicated")
    value["base_instructions"] = original + CANONICAL_SELF_AUDIT_INSTRUCTION
    value["base_instructions_sha256"] = bounded.base.sha256_text(
        value["base_instructions"]
    )
    return value


def _bounded_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    audited = value.get("base_instructions")
    if (
        not isinstance(audited, str)
        or not audited.endswith(CANONICAL_SELF_AUDIT_INSTRUCTION)
        or audited.count(CANONICAL_SELF_AUDIT_INSTRUCTION) != 1
    ):
        raise CanonicalV31EpisodeBatchError("canonical self-audit instructions drifted")
    value["base_instructions"] = audited[: -len(CANONICAL_SELF_AUDIT_INSTRUCTION)]
    value["base_instructions_sha256"] = bounded.base.sha256_text(
        value["base_instructions"]
    )
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    requests = bounded.prepare_episode_batches(
        episode, batch_size=batch_size, thread_mode=thread_mode
    )
    audited = [_audited_request(request) for request in requests]
    for request in audited:
        validate_prepared_request(request)
    return audited


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded_request = _bounded_request(request)
    validated = bounded.validate_prepared_request(bounded_request)
    expected = _audited_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("audited canonical request drifted")
    return copy.deepcopy(dict(request))


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validate_prepared_request(request)
    return bounded.validate_and_project_output(_bounded_request(request), output)


def build_six_arm_matrix_binding() -> dict[str, Any]:
    return {
        "schema_version": "pif_canonical_v31_audited_adapter_binding_v1",
        "bounded_adapter_binding": bounded.build_six_arm_matrix_binding(),
        "bounded_adapter_module_sha256": hashlib.sha256(
            Path(bounded.__file__).read_bytes()
        ).hexdigest(),
        "audited_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "canonical_self_audit_instruction_sha256": (
            CANONICAL_SELF_AUDIT_INSTRUCTION_SHA256
        ),
        "canonical_evidence_max_chars": CANONICAL_EVIDENCE_MAX_CHARS,
        "semantic_postprocessing": False,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "CANONICAL_EVIDENCE_MAX_CHARS",
    "CANONICAL_LABEL_PACK",
    "CANONICAL_SELF_AUDIT_INSTRUCTION",
    "CANONICAL_SELF_AUDIT_INSTRUCTION_SHA256",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "EFFORT",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
)
