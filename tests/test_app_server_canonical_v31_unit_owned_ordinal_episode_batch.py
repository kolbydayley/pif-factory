from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_single_message_compact_pointer_episode_batch as parent
from research_factory import app_server_canonical_v31_unit_owned_ordinal_episode_batch as adapter


ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch19-single-message-compact-pointer-canary-v1"
)


@lru_cache(maxsize=1)
def _parent_request() -> dict:
    return json.loads((ROOT / "prepared-turn/request.private.json").read_text())


@lru_cache(maxsize=1)
def _parent_output() -> dict:
    return json.loads((ROOT / "turn/output.private.json").read_text())


@lru_cache(maxsize=1)
def _request() -> dict:
    source = _parent_request()
    episode = copy.deepcopy(source["episode_context"])
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
        for segment in source["private_input"]["segments"]
    ]
    values = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )
    assert len(values) == 1
    return values[0]


def test_schema_uses_only_provider_compatible_closed_objects_and_array_items() -> None:
    request = _request()
    schema = request["output_schema"]
    assert "positional_protocol_version" not in request["semantic_integrity"]
    assert request["semantic_integrity"]["ordinal_protocol_version"] == (
        adapter.ORDINAL_PROTOCOL_VERSION
    )

    def walk(value):
        if isinstance(value, dict):
            assert "prefixItems" not in value
            if value.get("type") == "object" or (
                isinstance(value.get("type"), list) and "object" in value["type"]
            ):
                assert value.get("additionalProperties") is False
                assert set(value.get("required", [])) == set(value.get("properties", {}))
            if value.get("type") == "array" or (
                isinstance(value.get("type"), list) and "array" in value["type"]
            ):
                assert isinstance(value.get("items"), dict)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(schema)


def test_parent_output_round_trips_with_no_semantic_defaults() -> None:
    request = _request()
    raw = _parent_output()
    encoded = adapter.encode_parent_output_for_test(request, raw)
    projected = adapter.validate_and_project_output(request, encoded)
    expected = parent.validate_and_project_output(_parent_request(), raw)
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["emitted_event_count"] == 31
    assert projected["fidelity"]["deterministic_semantic_field_defaults"] is False


def test_ordinal_wire_remains_materially_smaller_than_canonical_object_output() -> None:
    raw = _parent_output()
    encoded = adapter.encode_parent_output_for_test(_request(), raw)
    raw_bytes = len(json.dumps(raw, separators=(",", ":")).encode())
    encoded_bytes = len(json.dumps(encoded, separators=(",", ":")).encode())
    assert encoded_bytes / raw_bytes < 0.68


def test_missing_or_extra_ordinal_key_fails_closed() -> None:
    encoded = adapter.encode_parent_output_for_test(_request(), _parent_output())
    del encoded["s"][0]["0"]
    with pytest.raises(adapter.CanonicalV31OutputError, match="ordinal segment"):
        adapter.validate_and_project_output(_request(), encoded)


def test_owner_mismatch_fails_without_semantic_repair() -> None:
    request = _request()
    encoded = adapter.encode_parent_output_for_test(request, _parent_output())
    unit_rows = encoded["s"][0]["2"]
    source = next(index for index, row in enumerate(unit_rows) if row["0"])
    target = 0 if source != 0 else 1
    unit_rows[target]["0"].append(unit_rows[source]["0"].pop(0))
    with pytest.raises(adapter.CanonicalV31OutputError, match="owned"):
        adapter.validate_and_project_output(request, encoded)
