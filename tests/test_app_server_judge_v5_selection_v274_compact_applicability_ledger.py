from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v274_compact_applicability_ledger as v274


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v274._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v274.prepare_turn(lineage)


def _stateful_event(event: dict) -> dict:
    direct = json.loads(json.dumps(event))
    na: list[str] = []
    uk: list[str] = []
    for field in v274.OPTIONAL_TEXT_FIELDS:
        if not str(direct[field] or ""):
            na.append(v274.FIELD_CODES[field])
            direct[field] = ""
    for field in ("claim_type", "stance"):
        if direct[field] == "not_applicable":
            na.append(v274.FIELD_CODES[field])
    for field, name_key, type_key in (
        ("speaker", "speaker_name", "speaker_role"),
        ("actor", "actor_name", "actor_type"),
    ):
        if not str(direct[name_key] or "") or direct[type_key] == "unknown":
            uk.append(v274.FIELD_CODES[field])
            direct[name_key] = ""
            direct[type_key] = "unknown"
    if direct["reported_actor_type"] == "none":
        na.append("ra")
        direct["reported_actor_name"] = ""
    elif not str(direct["reported_actor_name"] or "") or direct["reported_actor_type"] == "unknown":
        uk.append("ra")
        direct["reported_actor_name"] = ""
        direct["reported_actor_type"] = "unknown"
    metric_values = [
        str(direct[field] or "")
        for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
    ]
    has_metric = any(metric_values)
    direction = str(direct["metric_direction"])
    if has_metric and direction == "not_applicable":
        direct["metric_direction"] = "unknown"
    if not has_metric:
        na.append("mt")
        direct["metric_direction"] = "not_applicable"
    for field in v274.ENTITY_FIELDS:
        direct[field] = list(dict.fromkeys(str(item) for item in (direct[field] or [])))
        if not direct[field]:
            na.append(v274.FIELD_CODES[field])
    direct["na"] = na
    direct["uk"] = uk
    return direct


def _synthetic_output(turn: dict) -> dict:
    old_path = v274._lineage_paths()["v239_output"]
    old = json.loads(old_path.read_text(encoding="utf-8"))
    output = json.loads(json.dumps(old))
    for segment in output["segments"]:
        segment["events"] = [_stateful_event(event) for event in segment["events"]]
    v274._validate_schema(turn["schema"], output, path="$")
    return output


def test_predeclared_canary_is_blind_bounded_and_below_cost_target(turn: dict) -> None:
    assert turn["segment_ids"] == [v274.v239.DENSE_SEGMENT_ID, v274.v239.NO_SIGNAL_SEGMENT_ID]
    assert turn["prompt_bytes"] <= v274.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v274.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v274.MAX_SCHEMA_BYTES
    assert v274.llm_judge.validate_app_server_output_schema_subset(turn["schema"]) == []
    assert round(v274._production_ratio(v274.MAX_TOTAL_TOKENS)[1], 6) == 0.268298
    assert v274.MODEL == "gpt-5.6-sol"
    assert v274.EFFORT == "high"
    assert v274.MAX_TOTAL_TOKENS == 70_000


def test_compact_applicability_projects_without_semantic_code(turn: dict) -> None:
    normalized, provenance, diagnostics, receipt = v274.project_output(
        _synthetic_output(turn), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 1]
    assert len(provenance["events"]) == 32
    assert [row["event_count"] for row in diagnostics] == [31, 1]
    assert receipt["all_optional_semantics_selected_by_llm"] is True
    assert receipt["compact_state_codes_losslessly_projected"] is True
    assert receipt["deterministic_projection_only"] is True


def test_metric_applicability_conflict_fails_closed(turn: dict) -> None:
    output = _synthetic_output(turn)
    event = output["segments"][0]["events"][0]
    event["metric_value"] = "1"
    if "mt" not in event["na"]:
        event["na"].append("mt")
    event["metric_direction"] = "not_applicable"
    with pytest.raises(v274.V274OutputContractError):
        v274.project_output(output, turn)


def test_duplicate_compact_state_code_fails_closed(turn: dict) -> None:
    output = _synthetic_output(turn)
    event = output["segments"][0]["events"][0]
    event["na"].append(event["na"][0])
    with pytest.raises(v274.V274OutputContractError):
        v274.project_output(output, turn)


def test_freeze_is_presemantic_and_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v274"
    frozen = v274.freeze_v274(output_dir=root)
    lock = v274.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["max_total_tokens"] == 70_000
    assert lock["semantic_regex_or_keyword_filtering"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


class _FakeClient:
    def __init__(self, root: Path, output: dict) -> None:
        self.root = root
        self.output = output
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
            "input_tokens": 16_000,
            "cached_input_tokens": 1_000,
            "output_tokens": 4_000,
            "reasoning_output_tokens": 500,
            "total_tokens": 20_000,
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
                    "model": v274.MODEL,
                    "effort": v274.EFFORT,
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


def test_fake_run_passes_and_has_one_measured_turn(tmp_path: Path) -> None:
    root = tmp_path / "v274"
    frozen = v274.freeze_v274(output_dir=root)
    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(v274.run_v274(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 1
    assert terminal["state"] == "v274_architecture_structural_gate_passed"
    assert terminal["usage"]["total_tokens"] == 20_000
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_schema_mutation(tmp_path: Path) -> None:
    root = tmp_path / "v274"
    frozen = v274.freeze_v274(output_dir=root)
    schema = next(root.glob("turns/*/schema.json"))
    schema.write_text(schema.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v274.V274ExplicitApplicabilityError):
        v274.verify_runtime_lock(frozen["runtime_lock"])
