from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_single_message_compact_pointer_episode_batch as canonical_parent
from research_factory import app_server_canonical_v31_unit_owned_token_id_episode_batch as adapter


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


@lru_cache(maxsize=1)
def _encoded_fixture() -> dict:
    return adapter.encode_parent_output_for_test(_request(), _parent_output())


def _inject_valid_metric_pair(
    request: dict, output: dict
) -> tuple[dict, str, dict, dict]:
    layout = adapter._layout(request)  # noqa: SLF001
    raw_text_key = str(layout["metric_fields"]["raw_text_range"])
    for segment_index, segment in enumerate(output["s"]):
        source = request["private_input"]["segments"][segment_index]
        for unit in segment["2"]:
            for event in unit["0"]:
                metric = event[str(layout["event_metric"])]
                span = source["evidence_spans"][event[str(layout["event_evidence"])]]
                tokens = [
                    token
                    for source_unit in source["units"]
                    for token in source_unit["literal_tokens"]
                    if token["start_char"] >= span["start_char"]
                    and token["end_char"] <= span["end_char"]
                ]
                if tokens:
                    metric[raw_text_key] = {
                        "0": tokens[0]["literal_token_id"],
                        "1": tokens[-1]["literal_token_id"],
                    }
                    return metric, raw_text_key, source, span
    raise AssertionError("fixture has no event with evidence-local literal tokens")


def test_token_id_schema_is_closed_provider_compatible_and_bounded() -> None:
    request = _request()
    schema = request["output_schema"]
    assert request["semantic_integrity"]["model_selects_exact_literal_token_ids"] is True
    assert request["semantic_integrity"]["deterministic_metric_pointer_clamping"] is False

    def walk(value):
        if isinstance(value, dict):
            assert "prefixItems" not in value
            assert not isinstance(value.get("items"), bool)
            if value.get("type") == "object" or (
                isinstance(value.get("type"), list) and "object" in value["type"]
            ):
                assert value.get("additionalProperties") is False
                assert set(value.get("required", [])) == set(value.get("properties", {}))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(schema)
    layout = adapter._layout(request)  # noqa: SLF001
    unit_item = schema["properties"]["s"]["items"]["properties"]["2"]["items"]
    event = unit_item["properties"]["0"]["items"]
    metric = event["properties"][str(layout["event_metric"])]
    for index in layout["metric_fields"].values():
        pair = metric["properties"][str(index)]
        assert pair["properties"]["0"] == {
            "type": "string",
            "minLength": 11,
            "maxLength": 11,
        }


def test_parent_output_round_trips_without_semantic_defaults_or_pruning() -> None:
    request = _request()
    raw = _parent_output()
    encoded = copy.deepcopy(_encoded_fixture())
    projected = adapter.validate_and_project_output(request, encoded)
    expected = canonical_parent.validate_and_project_output(_parent_request(), raw)
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["emitted_event_count"] == 31
    assert projected["fidelity"]["deterministic_semantic_field_defaults"] is False
    assert projected["fidelity"]["deterministic_semantic_pruning"] is False


def test_model_authored_evidence_pointer_controls_owner_without_cardinality_change() -> None:
    request = _request()
    encoded = copy.deepcopy(_encoded_fixture())
    baseline = adapter.validate_and_project_output(request, encoded)
    rows = encoded["s"][0]["2"]
    source = next(index for index, row in enumerate(rows) if row["1"])
    target = 0 if source != 0 else 1
    rows[target]["1"].append(rows[source]["1"].pop(0))
    projected = adapter.validate_and_project_output(request, encoded)
    assert projected["labels"] == baseline["labels"]
    assert projected["fidelity"]["concept_owner_container_normalization_count"] == 1
    assert projected["fidelity"]["emitted_event_count"] == 31


def test_unknown_literal_token_id_fails_without_clamping() -> None:
    request = _request()
    encoded = copy.deepcopy(_encoded_fixture())
    metric, key, _, _ = _inject_valid_metric_pair(request, encoded)
    metric[key]["1"] = "X" * 11
    with pytest.raises(adapter.CanonicalV31OutputError, match="unknown"):
        adapter.validate_and_project_output(request, encoded)


def test_literal_token_outside_authored_evidence_fails_closed() -> None:
    request = _request()
    encoded = copy.deepcopy(_encoded_fixture())
    metric, key, source, span = _inject_valid_metric_pair(request, encoded)
    outside = next(
        token["literal_token_id"]
        for source_unit in source["units"]
        for token in source_unit["literal_tokens"]
        if token["end_char"] <= span["start_char"]
        or token["start_char"] >= span["end_char"]
    )
    metric[key] = {"0": outside, "1": outside}
    with pytest.raises(adapter.CanonicalV31OutputError, match="outside evidence"):
        adapter.validate_and_project_output(request, encoded)


def test_token_id_wire_remains_materially_smaller_than_canonical_objects() -> None:
    raw = _parent_output()
    encoded = copy.deepcopy(_encoded_fixture())
    raw_bytes = len(json.dumps(raw, separators=(",", ":")).encode())
    encoded_bytes = len(json.dumps(encoded, separators=(",", ":")).encode())
    assert encoded_bytes / raw_bytes < 0.75
