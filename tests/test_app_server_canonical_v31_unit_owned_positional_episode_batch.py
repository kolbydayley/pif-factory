from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_single_message_compact_pointer_episode_batch as parent
from research_factory import app_server_canonical_v31_unit_owned_positional_episode_batch as adapter


ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch19-single-message-compact-pointer-canary-v1"
)


@lru_cache(maxsize=1)
def _parent_request_cached() -> dict:
    return json.loads((ROOT / "prepared-turn/request.private.json").read_text())


def _parent_request() -> dict:
    return copy.deepcopy(_parent_request_cached())


@lru_cache(maxsize=1)
def _parent_output_cached() -> dict:
    return json.loads((ROOT / "turn/output.private.json").read_text())


def _parent_output() -> dict:
    return copy.deepcopy(_parent_output_cached())


def _episode(request: dict) -> dict:
    episode = copy.deepcopy(request["episode_context"])
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment in request["private_input"]["segments"]
    ]
    return episode


@lru_cache(maxsize=1)
def _request_cached() -> dict:
    values = adapter.prepare_episode_batches(
        _episode(_parent_request()), batch_size=8, thread_mode="new_thread"
    )
    assert len(values) == 1
    return values[0]


def _request() -> dict:
    return copy.deepcopy(_request_cached())


def test_request_is_full_canonical_unit_owned_and_positional() -> None:
    request = _request()
    assert adapter.validate_prepared_request(request) == request
    assert request["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert request["semantic_integrity"]["model_authors_all_canonical_semantic_fields"] is True
    assert request["semantic_integrity"]["deterministic_semantic_pruning"] is False
    assert request["output_schema"]["required"] == ["s"]
    assert len(request["output_schema"]["properties"]["s"]["prefixItems"]) == 6


def test_parent_output_round_trips_without_semantic_change() -> None:
    request = _request()
    raw = _parent_output()
    encoded = adapter.encode_parent_output_for_test(request, raw)
    projected = adapter.validate_and_project_output(request, encoded)
    expected = parent.validate_and_project_output(_parent_request(), raw)
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["emitted_event_count"] == 31
    assert projected["fidelity"]["deterministic_semantic_field_defaults"] is False


def test_positional_wire_reduces_observed_model_output_bytes() -> None:
    request = _request()
    raw = _parent_output()
    encoded = adapter.encode_parent_output_for_test(request, raw)
    raw_bytes = len(json.dumps(raw, separators=(",", ":")).encode())
    encoded_bytes = len(json.dumps(encoded, separators=(",", ":")).encode())
    assert encoded_bytes / raw_bytes < 0.62


def test_owner_mismatch_fails_before_projection() -> None:
    request = _request()
    encoded = adapter.encode_parent_output_for_test(request, _parent_output())
    segment = request["private_input"]["segments"][0]
    first_nonempty = next(
        index for index, unit_row in enumerate(encoded["s"][0][2]) if unit_row[0]
    )
    target = 0 if first_nonempty != 0 else 1
    encoded["s"][0][2][target][0].append(encoded["s"][0][2][first_nonempty][0].pop(0))
    with pytest.raises(adapter.CanonicalV31OutputError, match="owned"):
        adapter.validate_and_project_output(request, encoded)


def test_segment_or_unit_omission_fails_closed() -> None:
    request = _request()
    encoded = adapter.encode_parent_output_for_test(request, _parent_output())
    encoded["s"][0][2].pop()
    with pytest.raises(adapter.CanonicalV31OutputError, match="unit coverage"):
        adapter.validate_and_project_output(request, encoded)


def test_invalid_semantic_value_reaches_frozen_parent_validator() -> None:
    request = _request()
    encoded = adapter.encode_parent_output_for_test(request, _parent_output())
    encoded["s"][0][0] = "invented_status"
    with pytest.raises(adapter.CanonicalV31OutputError):
        adapter.validate_and_project_output(request, encoded)
