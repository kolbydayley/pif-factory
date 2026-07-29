from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_episode_batch as base


PROJECT_ROOT = base.PROJECT_ROOT
MODEL = base.MODEL
EFFORT = base.EFFORT
PINNED_CODEX = base.PINNED_CODEX
CANONICAL_LABEL_PACK = base.CANONICAL_LABEL_PACK
CANONICAL_EVIDENCE_MAX_CHARS = base.CANONICAL_EVIDENCE_MAX_CHARS
CanonicalV31EpisodeBatchError = base.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = base.CanonicalV31OutputError
CanonicalV31TelemetryError = base.CanonicalV31TelemetryError
_client_factory = base._client_factory
expected_instruction_source_contract = base.expected_instruction_source_contract

EVIDENCE_BOUND_INSTRUCTION = (
    "\n# Canonical evidence-span bound\n"
    "Every selected evidence_start_unit_id through evidence_end_unit_id range must "
    "reconstruct to at most 1000 source characters, including exact delimiters. "
    "Never select two or more units when their contiguous reconstructed span would "
    "exceed 1000 characters. Choose a narrower supporting range, split genuinely "
    "independent propositions into separate events, or omit a claim that cannot be "
    "fully grounded within one representable span.\n"
)
EVIDENCE_BOUND_INSTRUCTION_SHA256 = hashlib.sha256(
    EVIDENCE_BOUND_INSTRUCTION.encode("ascii")
).hexdigest()


def _bounded_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    original = value.get("base_instructions")
    if not isinstance(original, str) or not original:
        raise CanonicalV31EpisodeBatchError("base instructions are unavailable")
    if EVIDENCE_BOUND_INSTRUCTION.strip() in original:
        raise CanonicalV31EpisodeBatchError("evidence-bound instruction was duplicated")
    value["base_instructions"] = original + EVIDENCE_BOUND_INSTRUCTION
    value["base_instructions_sha256"] = base.sha256_text(
        value["base_instructions"]
    )
    return value


def _base_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    bounded = value.get("base_instructions")
    if (
        not isinstance(bounded, str)
        or not bounded.endswith(EVIDENCE_BOUND_INSTRUCTION)
        or bounded.count(EVIDENCE_BOUND_INSTRUCTION) != 1
    ):
        raise CanonicalV31EpisodeBatchError("evidence-bound instructions drifted")
    value["base_instructions"] = bounded[: -len(EVIDENCE_BOUND_INSTRUCTION)]
    value["base_instructions_sha256"] = base.sha256_text(
        value["base_instructions"]
    )
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    requests = base.prepare_episode_batches(
        episode, batch_size=batch_size, thread_mode=thread_mode
    )
    bounded = [_bounded_request(request) for request in requests]
    for request in bounded:
        validate_prepared_request(request)
    return bounded


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    base_request = _base_request(request)
    validated_base = base.validate_prepared_request(base_request)
    expected = _bounded_request(validated_base)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("bounded canonical request drifted")
    return copy.deepcopy(dict(request))


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validate_prepared_request(request)
    return base.validate_and_project_output(_base_request(request), output)


def build_six_arm_matrix_binding() -> dict[str, Any]:
    return {
        "schema_version": "pif_canonical_v31_bounded_evidence_adapter_binding_v1",
        "base_adapter_binding": base.build_six_arm_matrix_binding(),
        "base_adapter_module_sha256": hashlib.sha256(
            Path(base.__file__).read_bytes()
        ).hexdigest(),
        "bounded_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "evidence_bound_instruction_sha256": EVIDENCE_BOUND_INSTRUCTION_SHA256,
        "canonical_evidence_max_chars": CANONICAL_EVIDENCE_MAX_CHARS,
        "semantic_postprocessing": False,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "CANONICAL_EVIDENCE_MAX_CHARS",
    "CANONICAL_LABEL_PACK",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "EFFORT",
    "EVIDENCE_BOUND_INSTRUCTION",
    "EVIDENCE_BOUND_INSTRUCTION_SHA256",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
)
