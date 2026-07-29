from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_minimal_typed_event_set_experiment as lane


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
            segment(
                "segment_dense",
                "Speaker reports 40 percent growth.",
                "dense",
                "S0U000",
            ),
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
                    {
                        "unit_id": "S0U000",
                        "eligible_event_count": 1,
                        "unresolved_count": 0,
                    }
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
                    {
                        "unit_id": "S1U000",
                        "eligible_event_count": 0,
                        "unresolved_count": 0,
                    }
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [],
            },
        ],
    }


def _minimal_event() -> dict:
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


def _minimal_output() -> dict:
    return {
        "episode_id": "episode_test",
        "segment_source_contexts_by_id": {
            "segment_dense": "substantive_dialogue",
            "segment_empty": "substantive_dialogue",
        },
        "metric_events": [_minimal_event()],
        "non_metric_events": [],
    }


def _frozen(tmp_path: Path) -> dict:
    projection = _schema(_direct_output())
    minimal = lane.build_minimal_schema(projection, _source())
    schema_path = tmp_path / "minimal-schema.json"
    projection_path = tmp_path / "projection-schema.json"
    schema_path.write_text(json.dumps(minimal), encoding="utf-8")
    projection_path.write_text(json.dumps(projection), encoding="utf-8")
    return {
        "turn": {"schema": schema_path, "projection_schema": projection_path},
        "source": _source(),
    }


def test_schema_omits_redundant_wrappers_and_provider_keywords() -> None:
    schema = lane.build_minimal_schema(_schema(_direct_output()), _source())
    serialized = json.dumps(schema, sort_keys=True)

    assert "segments_by_id" not in schema["properties"]
    assert "unit_receipts" not in serialized
    assert '"status"' not in serialized
    assert '"minLength"' not in serialized
    assert '"uniqueItems"' not in serialized
    assert schema["properties"]["segment_source_contexts_by_id"]["required"] == [
        "segment_dense",
        "segment_empty",
    ]
    assert schema["properties"]["metric_events"]["items"]["properties"]["metric"][
        "minItems"
    ] == 1
    assert (
        "metric"
        not in schema["properties"]["non_metric_events"]["items"]["properties"]
    )


def test_projection_derives_only_structural_segment_wrappers(tmp_path: Path) -> None:
    normalized, provenance, diagnostics = lane.validate_and_project_output(
        _minimal_output(), _frozen(tmp_path)
    )

    dense, empty = normalized["segments"]
    assert dense["status"] == "coded"
    assert dense["events"][0]["metric_value"] == "40"
    assert empty["status"] == "no_signal"
    assert empty["no_signal_reason"]
    assert len(provenance["events"]) == 1
    assert diagnostics["diagnostics"][0]["reviewed_source_unit_count"] == 1
    assert diagnostics["diagnostics"][1]["reviewed_source_unit_count"] == 1
    assert diagnostics["diagnostics"][0]["event_count"] == 1
    assert diagnostics["diagnostics"][1]["event_count"] == 0
    assert diagnostics["deterministic_event_semantics_changed"] is False


def test_duplicate_source_order_fails_closed(tmp_path: Path) -> None:
    output = _minimal_output()
    second = copy.deepcopy(output["metric_events"][0])
    second["claim_text"] = "Another claim."
    output["metric_events"].append(second)

    with pytest.raises(lane.MinimalTypedEventSetOutputError, match="source order"):
        lane.validate_and_project_output(output, _frozen(tmp_path))


def test_cost_gate_uses_exact_production_safe_ceiling() -> None:
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
        "input_tokens": lane.MAX_TOTAL_TOKENS,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": lane.MAX_TOTAL_TOKENS,
    }

    gate = lane._gate(usage, diagnostics)  # noqa: SLF001
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] <= 0.28

    usage["input_tokens"] += 1
    usage["total_tokens"] += 1
    gate = lane._gate(usage, diagnostics)  # noqa: SLF001
    assert gate["passed"] is False
    assert "total_tokens_within_production_safe_bound" in gate["failed_checks"]
