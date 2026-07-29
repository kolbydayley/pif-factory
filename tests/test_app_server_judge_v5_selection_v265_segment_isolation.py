from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249
from research_factory import app_server_judge_v5_selection_v265_segment_isolation as v265


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v265._validate_lineage()


@pytest.fixture(scope="module")
def turns(lineage: dict) -> list[dict]:
    return v265.prepare_turns(lineage)


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


def _synthetic_outputs(turns: list[dict]) -> list[dict]:
    old = json.loads(v249._lineage_paths()["v239_output"].read_text(encoding="utf-8"))
    by_id = {str(row["segment_id"]): row for row in old["segments"]}
    outputs = []
    for turn in turns:
        segment = json.loads(json.dumps(by_id[turn["segment_ids"][0]]))
        segment["events"] = [_stateful_event(event) for event in segment["events"]]
        output = {"episode_id": turn["episode_id"], "segments": [segment]}
        v249._validate_schema(turn["schema"], output, path="$")
        outputs.append(output)
    return outputs


def _usage(total: int) -> dict[str, int]:
    return {
        "input_tokens": total - 4_500,
        "cached_input_tokens": 1_000,
        "output_tokens": 4_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_design_is_two_isolated_blind_turns_below_cost_target(turns: list[dict]) -> None:
    assert len(turns) == 2
    assert [turn["segment_ids"][0] for turn in turns] == [
        v249.v239.DENSE_SEGMENT_ID,
        v249.v239.NO_SIGNAL_SEGMENT_ID,
    ]
    for turn in turns:
        packet = json.loads(turn["prompt"].split("\n", 1)[1])
        assert len(packet["segments"]) == 1
        assert packet["segments"][0]["segment_id"] == turn["segment_ids"][0]
        segments_schema = turn["schema"]["properties"]["segments"]
        assert segments_schema["minItems"] == segments_schema["maxItems"] == 1
        assert segments_schema["items"]["properties"]["segment_id"]["enum"] == turn["segment_ids"]
        assert turn["prompt_bytes"] <= v265.MAX_PROMPT_BYTES
        assert turn["base_bytes"] <= v265.MAX_BASE_BYTES
        assert turn["schema_bytes"] <= v265.MAX_SCHEMA_BYTES
    assert round(v265._production_ratio(v265.PHASE_TOTAL_TOKEN_BOUND)[1], 6) == 0.27724
    assert v265.MODEL == "gpt-5.6-sol"
    assert v265.EFFORT == "high"


def test_exact_projection_and_episode_assembly_are_nonsemantic(turns: list[dict]) -> None:
    projected = [
        v265.project_output(output, turn)
        for output, turn in zip(_synthetic_outputs(turns), turns)
    ]
    normalized, provenance, diagnostics, receipt = v265.assemble_outputs(projected)
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 1]
    assert len(provenance["events"]) == 32
    assert [row["event_count"] for row in diagnostics] == [31, 1]
    assert receipt["all_semantic_extraction_owned_by_llm"] is True
    assert receipt["deterministic_episode_assembly_only"] is True
    gate = v265._gate(
        usage=_usage(40_000),
        per_turn_usages=[_usage(30_000), _usage(10_000)],
        diagnostics=diagnostics,
        receipt=receipt,
    )
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


class _FakeClient:
    def __init__(self, outputs: list[dict], *, fail_first: bool = False) -> None:
        self.outputs = outputs
        self.fail_first = fail_first
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        index = self.calls
        self.calls += 1
        capacity_path = Path(str(kwargs["capacity_checkpoint_path"]))
        assert capacity_path.parent.name.startswith(f"v265-segment-{index}-")
        capacity_path.write_text(
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
        total = 30_000 if index == 0 else 10_000
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "usage": _usage(total),
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v265.MODEL,
                    "effort": v265.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output = self.outputs[index]
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=not (self.fail_first and index == 0), output=output)


def test_freeze_and_fake_run_are_hash_bound_and_sequential(tmp_path: Path, turns: list[dict]) -> None:
    root = tmp_path / "v265"
    frozen = v265.freeze_v265(output_dir=root)
    lock = v265.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert lock["max_total_tokens_per_turn"] == 50_000
    assert lock["phase_total_token_bound"] == 73_000
    assert not list(root.rglob("capacity.json"))
    fake = _FakeClient(_synthetic_outputs(frozen["turns"]))
    terminal = asyncio.run(v265.run_v265(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 2
    assert terminal["state"] == "v265_architecture_structural_gate_passed"
    assert terminal["usage"]["total_tokens"] == 40_000
    assert len(terminal["sidecars"]) == 2
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_first_turn_failure_never_starts_second(tmp_path: Path, turns: list[dict]) -> None:
    root = tmp_path / "v265-failure"
    frozen = v265.freeze_v265(output_dir=root)
    fake = _FakeClient(_synthetic_outputs(frozen["turns"]), fail_first=True)
    terminal = asyncio.run(v265.run_v265(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 1
    assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
    assert terminal["semantic_retry_count"] == 0
    assert not frozen["turns"][1]["paths"]["capacity"].exists()
    assert not frozen["turns"][1]["paths"]["sidecar"].exists()
    assert terminal["production_mutated"] is False
