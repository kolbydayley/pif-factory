from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249
from research_factory import app_server_judge_v5_selection_v261_dual_decomposition as v261


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v261._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v261.prepare_turn(lineage)


def _text(value: object) -> dict:
    text = str(value or "")
    return {"applicability": "present" if text else "not_applicable", "value": text}


def _enum(value: object) -> dict:
    item = str(value)
    return {
        "applicability": "not_applicable" if item == "not_applicable" else "present",
        "value": item,
    }


def _party(name: object, kind: object, *, reported: bool = False) -> dict:
    text = str(name or "")
    item = str(kind)
    if reported and item == "none":
        return {"applicability": "not_applicable", "name": "", "type": "none"}
    if not text or item == "unknown":
        return {"applicability": "unknown", "name": "", "type": "unknown"}
    return {"applicability": "present", "name": text, "type": item}


def _array(values: object) -> dict:
    unique = list(dict.fromkeys(str(item) for item in (values or [])))
    return {
        "applicability": "present" if unique else "not_applicable",
        "value": unique,
    }


def _stateful_event(event: dict) -> dict:
    direct = {field: event[field] for field in v249.DIRECT_EVENT_FIELDS}
    metric_values = [
        str(event[field] or "")
        for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
    ]
    has_metric = any(metric_values)
    direction = str(event["metric_direction"])
    if has_metric and direction == "not_applicable":
        direction = "unknown"
    if not has_metric:
        direction = "not_applicable"
    return {
        **direct,
        **{field: _text(event[field]) for field in v249.OPTIONAL_TEXT_FIELDS},
        "claim_type": _enum(event["claim_type"]),
        "stance": _enum(event["stance"]),
        "speaker": _party(event["speaker_name"], event["speaker_role"]),
        "actor": _party(event["actor_name"], event["actor_type"]),
        "reported_actor": _party(
            event["reported_actor_name"], event["reported_actor_type"], reported=True
        ),
        "metric": {
            "applicability": "present" if has_metric else "not_applicable",
            "value": metric_values[0],
            "unit": metric_values[1],
            "comparator": metric_values[2],
            "raw_text": metric_values[3],
            "direction": direction,
        },
        **{field: _array(event[field]) for field in v249.ENTITY_FIELDS},
    }


def _synthetic_output(turn: dict) -> dict:
    old = json.loads(v249._lineage_paths()["v239_output"].read_text(encoding="utf-8"))
    output = json.loads(json.dumps(old))
    for segment in output["segments"]:
        segment["events"] = [_stateful_event(event) for event in segment["events"]]
    v261.v249._validate_schema(turn["schema"], output, path="$")
    return output


def test_design_is_blind_single_turn_and_below_cost_target(turn: dict) -> None:
    assert turn["segment_ids"] == [
        v249.v239.DENSE_SEGMENT_ID,
        v249.v239.NO_SIGNAL_SEGMENT_ID,
    ]
    assert turn["prompt_bytes"] <= v261.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v261.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v261.MAX_SCHEMA_BYTES
    assert round(v261._production_ratio(v261.MAX_TOTAL_TOKENS)[1], 6) == 0.223591
    assert v261.MODEL == "gpt-5.6-sol"
    assert v261.EFFORT == "high"
    assert "reference answer" in v261.DUAL_DECOMPOSITION_INSTRUCTIONS
    assert "Pass A" in v261.DUAL_DECOMPOSITION_INSTRUCTIONS
    assert "Pass B" in v261.DUAL_DECOMPOSITION_INSTRUCTIONS


def test_dual_decomposition_uses_unchanged_exact_projection(turn: dict) -> None:
    normalized, provenance, diagnostics, receipt = v261.project_output(
        _synthetic_output(turn), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 1]
    assert len(provenance["events"]) == 32
    assert [row["event_count"] for row in diagnostics] == [31, 1]
    assert receipt["all_semantic_extraction_and_reconciliation_owned_by_llm"] is True
    assert receipt["deterministic_exact_projection_only"] is True


class _FakeClient:
    def __init__(self, root: Path, output: dict, total_tokens: int = 20_000) -> None:
        self.root = root
        self.output = output
        self.total_tokens = total_tokens
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        assert "output_validator" not in kwargs
        assert (self.root / "launch-receipt.json").is_file()
        self.calls += 1
        Path(str(kwargs["capacity_checkpoint_path"])).write_text(
            json.dumps(
                {
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                    "rate_limit_reached_type": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        usage = {
            "input_tokens": self.total_tokens - 4_500,
            "cached_input_tokens": 1_000,
            "output_tokens": 4_000,
            "reasoning_output_tokens": 500,
            "total_tokens": self.total_tokens,
        }
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "usage": usage,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v261.MODEL,
                    "effort": v261.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_freeze_and_fake_run_are_hash_bound_single_turn(tmp_path: Path) -> None:
    root = tmp_path / "v261"
    frozen = v261.freeze_v261(output_dir=root)
    lock = v261.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["max_total_tokens"] == 55_000
    assert lock["semantic_regex_or_keyword_filtering"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))

    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(v261.run_v261(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 1
    assert terminal["state"] == "v261_architecture_structural_gate_passed"
    assert terminal["usage"]["total_tokens"] == 20_000
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
