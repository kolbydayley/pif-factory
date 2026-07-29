from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_typed_event_set_experiment as lane
from research_factory.app_server_runtime_verifier import ContentHashCache
from research_factory.labels import ValidationError, _validate_schema


def _schema(value: object) -> dict:
    if isinstance(value, dict):
        return {
            "type": "object",
            "additionalProperties": False,
            "required": list(value),
            "properties": {key: _schema(item) for key, item in value.items()},
        }
    if isinstance(value, list):
        return {
            "type": "array",
            "minItems": 0,
            "items": _schema(value[0]) if value else {"type": "string"},
        }
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    return {"type": "string"}


def _source() -> dict:
    def segment(segment_id: str, text: str, density: str, unit_id: str) -> dict:
        return {
            "segment_id": segment_id,
            "segment_text": text,
            "density_stratum": density,
            "boundaries": [
                {
                    "window_id": 0,
                    "extract_start": 0,
                    "extract_end": len(text),
                    "owner_start": 0,
                    "owner_end": len(text),
                }
            ],
            "units": [
                {
                    "unit_id": unit_id,
                    "window_id": 0,
                    "text": text,
                    "start_char": 0,
                    "end_char": len(text),
                }
            ],
        }

    return {
        "episode_id": "episode_test",
        "segments": [
            segment("segment_dense", "Speaker reports 40 percent growth.", "dense", "S0U000"),
            segment("segment_empty", "No event here.", "no_signal", "S1U000"),
        ],
    }


def _direct_event() -> dict:
    return {
        "event_type": "capability_claim",
        "event_subtype": "test",
        "claim_type": "descriptive",
        "actor_name": "Speaker",
        "actor_type": "person",
        "speaker_name": "Speaker",
        "speaker_role": "guest",
        "reported_actor_name": "",
        "reported_actor_type": "none",
        "source_context_kind": "substantive_dialogue",
        "target_concept": "growth",
        "claim_text": "Speaker reports 40 percent growth.",
        "stance": "neutral",
        "certainty": "high",
        "temporal_horizon": "present",
        "causal_mechanism": "",
        "counterclaim": "",
        "metric_value": "40",
        "metric_unit": "percent",
        "metric_comparator": "",
        "metric_direction": "increase",
        "metric_raw_text": "40 percent",
        "signal_reason": "",
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
        "confidence": 0.9,
        "evidence_start_unit_id": "S0U000",
        "evidence_end_unit_id": "S0U000",
    }


def _direct_output() -> dict:
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "status": "coded",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "",
                "unit_receipts": [
                    {"unit_id": "S0U000", "eligible_event_count": 1, "unresolved_count": 0}
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [_direct_event()],
            },
            {
                "segment_id": "segment_empty",
                "status": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "unit_receipts": [
                    {"unit_id": "S1U000", "eligible_event_count": 0, "unresolved_count": 0}
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [],
            },
        ],
    }


def _typed_event() -> dict:
    direct = _direct_event()
    return {
        "event_type": direct["event_type"],
        "claim_text": direct["claim_text"],
        "certainty": direct["certainty"],
        "temporal_horizon": direct["temporal_horizon"],
        "source_context_kind": direct["source_context_kind"],
        "confidence": direct["confidence"],
        "evidence_start_unit_id": direct["evidence_start_unit_id"],
        "evidence_end_unit_id": direct["evidence_end_unit_id"],
        "event_subtype": [direct["event_subtype"]],
        "target_concept": [direct["target_concept"]],
        "causal_mechanism": [],
        "counterclaim": [],
        "signal_reason": [],
        "claim_type": [direct["claim_type"]],
        "stance": [direct["stance"]],
        "speaker": [{"name": "Speaker", "type": "guest"}],
        "actor": [{"name": "Speaker", "type": "person"}],
        "reported_actor": [],
        "metric": [
            {
                "value": "40",
                "unit": "percent",
                "comparator": "",
                "direction": "increase",
                "raw_text": "40 percent",
            }
        ],
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
        "source_order_index": 0,
        "segment_id": "segment_dense",
    }


def _typed_output() -> dict:
    return {
        "episode_id": "episode_test",
        "segments_by_id": {
            "segment_dense": {
                "status": "coded",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "unit_receipts_by_id": {
                    "S0U000": {"eligible_event_count": 1, "unresolved_count": 0}
                },
            },
            "segment_empty": {
                "status": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "unit_receipts_by_id": {
                    "S1U000": {"eligible_event_count": 0, "unresolved_count": 0}
                },
            },
        },
        "metric_events": [_typed_event()],
        "non_metric_events": [],
    }


def _frozen(tmp_path: Path) -> dict:
    projection = _schema(_direct_output())
    typed = lane.build_typed_schema(projection, _source())
    schema_path = tmp_path / "typed-schema.json"
    projection_path = tmp_path / "projection-schema.json"
    schema_path.write_text(json.dumps(typed), encoding="utf-8")
    projection_path.write_text(json.dumps(projection), encoding="utf-8")
    return {
        "turn": {"schema": schema_path, "projection_schema": projection_path},
        "source": _source(),
    }


def test_schema_compiles_segment_and_unit_ids_into_required_properties() -> None:
    schema = lane.build_typed_schema(_schema(_direct_output()), _source())
    segments = schema["properties"]["segments_by_id"]

    assert segments["required"] == ["segment_dense", "segment_empty"]
    dense = segments["properties"]["segment_dense"]["properties"]
    assert dense["unit_receipts_by_id"]["required"] == ["S0U000"]
    metric = schema["properties"]["metric_events"]["items"]
    nonmetric = schema["properties"]["non_metric_events"]["items"]
    assert metric["properties"]["metric"]["minItems"] == 1
    assert "metric" not in nonmetric["properties"]
    assert "identity_kind" not in metric["properties"]["speaker"]["items"]["properties"]


def test_valid_typed_output_projects_to_canonical_schema(tmp_path: Path) -> None:
    normalized, provenance, diagnostics = lane.validate_and_project_output(
        _typed_output(), _frozen(tmp_path)
    )

    assert normalized["segments"][0]["events"][0]["metric_value"] == "40"
    assert normalized["segments"][0]["events"][0]["speaker_name"] == "Speaker"
    assert normalized["segments"][1]["events"] == []
    assert len(provenance["events"]) == 1
    assert diagnostics["all_semantic_decisions_owned_by_llm"] is True


def test_duplicate_source_order_index_fails_closed(tmp_path: Path) -> None:
    output = _typed_output()
    second = copy.deepcopy(output["metric_events"][0])
    second["claim_text"] = "Another claim."
    output["metric_events"].append(second)
    output["segments_by_id"]["segment_dense"]["unit_receipts_by_id"]["S0U000"][
        "eligible_event_count"
    ] = 2

    with pytest.raises(lane.TypedEventSetOutputError, match="source order index"):
        lane.validate_and_project_output(output, _frozen(tmp_path))


def test_schema_rejects_missing_required_unit_receipt() -> None:
    schema = lane.build_typed_schema(_schema(_direct_output()), _source())
    output = _typed_output()
    del output["segments_by_id"]["segment_dense"]["unit_receipts_by_id"]["S0U000"]

    with pytest.raises(ValidationError):
        _validate_schema(schema, output, path="$")


def test_cost_gate_uses_raw_measured_tokens() -> None:
    diagnostics = {
        "diagnostics": [
            {
                "segment_id": "segment_dense",
                "source_unit_count": 1,
                "reviewed_source_unit_count": 1,
                "event_count": 1,
                "unresolved_count": 0,
            }
        ]
    }
    usage = {
        "input_tokens": 20_000,
        "cached_input_tokens": 10_000,
        "output_tokens": 20_000,
        "reasoning_output_tokens": 5_000,
        "total_tokens": 40_000,
    }

    gate = lane._gate(usage, diagnostics)  # noqa: SLF001
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_direct_runtime_receipt_does_not_recurse_but_rejects_direct_drift(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime.py"
    runtime.write_text("stable\n", encoding="utf-8")
    missing_child = tmp_path / "historical-child.json"
    predecessor = tmp_path / "predecessor-runtime-lock.json"
    predecessor.write_text(
        json.dumps(
            {
                "historical_child": {
                    "path": str(missing_child),
                    "sha256": "0" * 64,
                    "size_bytes": 1,
                }
            }
        ),
        encoding="utf-8",
    )
    cache = ContentHashCache()
    lock = tmp_path / "runtime-lock.json"
    lock.write_text(
        json.dumps(
            {
                "schema_version": lane.LOCK_VERSION,
                "experiment_id": "test",
                "runtime": cache.record(runtime),
                "predecessor": cache.record(predecessor),
            }
        ),
        encoding="utf-8",
    )
    receipt_path = tmp_path / "runtime-lock-closure.json"
    receipt_path.write_text(
        json.dumps(lane._direct_runtime_receipt(lock)),  # noqa: SLF001
        encoding="utf-8",
    )

    manifest = lane._verify_direct_runtime_lock(  # noqa: SLF001
        lock,
        receipt_path,
        required_fields={
            "schema_version": lane.LOCK_VERSION,
            "experiment_id": "test",
        },
        required_record_paths=[runtime, predecessor],
    )
    assert manifest["experiment_id"] == "test"

    runtime.write_text("changed\n", encoding="utf-8")
    with pytest.raises(lane.TypedEventSetError):
        lane._verify_direct_runtime_lock(  # noqa: SLF001
            lock,
            receipt_path,
            required_fields={"schema_version": lane.LOCK_VERSION},
            required_record_paths=[runtime, predecessor],
        )
