from __future__ import annotations

import copy

import pytest

from research_factory import app_server_canonical_v31_audited_episode_batch as audited
from research_factory import app_server_canonical_v31_bounded_evidence_episode_batch as bounded
from tests.test_app_server_canonical_v31_bounded_evidence_episode_batch import (
    _all_no_signal_output,
    _episode,
)


def _request() -> dict:
    requests = audited.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )
    assert len(requests) == 1
    return requests[0]


def test_audited_request_contains_bounded_evidence_and_cross_field_contract() -> None:
    request = _request()

    assert bounded.EVIDENCE_BOUND_INSTRUCTION in request["base_instructions"]
    assert request["base_instructions"].endswith(
        audited.CANONICAL_SELF_AUDIT_INSTRUCTION
    )
    assert "direction is not_applicable" in request["base_instructions"]
    assert "exact contiguous substring" in request["base_instructions"]
    assert audited.validate_prepared_request(request) == request


def test_audited_request_tamper_is_rejected() -> None:
    request = _request()
    request["base_instructions"] = request["base_instructions"].replace(
        "direction is not_applicable", "direction is increase"
    )
    request["base_instructions_sha256"] = bounded.base.sha256_text(
        request["base_instructions"]
    )

    with pytest.raises(
        audited.CanonicalV31EpisodeBatchError,
        match="canonical self-audit instructions drifted",
    ):
        audited.validate_prepared_request(request)


def test_audited_request_projects_through_unchanged_canonical_validator() -> None:
    request = _request()
    output = _all_no_signal_output(request)
    projected = audited.validate_and_project_output(request, output)

    assert projected["labels"][0]["segment_id"] == "seg_bounded_fixture"
    assert projected["labels"][0]["extraction_status"] == "no_signal"


def test_audited_binding_forbids_deterministic_semantic_changes() -> None:
    binding = audited.build_six_arm_matrix_binding()

    assert binding["canonical_evidence_max_chars"] == 1000
    assert binding["semantic_postprocessing"] is False
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
    assert binding["bounded_adapter_binding"] == bounded.build_six_arm_matrix_binding()
