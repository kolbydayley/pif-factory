from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from research_factory import (
    app_server_judge_v5_selection_v229_metric_grounding_repair as v229,
)


def test_lineage_is_exactly_the_immutable_v228_attempt():
    lineage = v229._validate_lineage()
    assert set(lineage["records"]) == set(v229.EXPECTED_V228_HASHES)
    assert lineage["gate"]["failed_checks"] == [
        "metric_grounding_error_events_0"
    ]
    assert lineage["gate"]["checks"]["all_exactness_pruned_events_0"] is True
    assert lineage["sidecar"]["usage"]["total_tokens"] == 21_530


def test_freeze_is_one_turn_metric_only_and_keeps_holdout_closed(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v229-metric-grounding-repair"
    frozen = v229.freeze_v229(output_dir=root)
    spec = frozen["spec"]
    assert spec["declared_turn_count"] == 1
    assert spec["retry_count"] == 0
    assert spec["v228_replayed"] is False
    assert spec["semantic_delta"] == "repair only the unsupported metric tuple"
    assert spec["production_protocol_additional_turns"] == 0
    assert spec["support_alignment_allowed_after_pass"] is True
    assert spec["holdout_authorized"] is False
    assert spec["production_mutation_allowed"] is False
    assert not (root / "launch-receipt.json").exists()
    turn_root = root / "turns/v229-metric-grounding-repair"
    assert frozen["paths"]["capacity"] == turn_root / "capacity.json"
    assert frozen["paths"]["sidecar"] == turn_root / "sidecar.json"
    v229.verify_runtime_lock(root / "runtime-lock.json")


def test_clear_metric_unit_is_a_minimal_semantic_patch(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v229-metric-grounding-repair"
    frozen = v229.freeze_v229(output_dir=root)
    keep = v229._project_action(
        {"repair_id": frozen["request"]["repair_id"], "action": "keep"},
        frozen,
    )
    _, keep_diagnostics = v229._normalize_repair(keep, frozen)
    assert keep_diagnostics[0]["metric_grounding_error_events"] == 1
    clear = v229._project_action(
        {
            "repair_id": frozen["request"]["repair_id"],
            "action": "clear_metric_unit",
        },
        frozen,
    )
    assert clear["event"]["metric_unit"] == ""
    for field in (
        "metric_value",
        "metric_comparator",
        "metric_raw_text",
    ):
        assert clear["event"][field] == frozen["event"][field]
    _, clear_diagnostics = v229._normalize_repair(clear, frozen)
    assert clear_diagnostics[0]["metric_grounding_error_events"] == 0
    assert clear_diagnostics[0]["exactness_pruned_events"] == 0


def test_clear_all_and_drop_are_explicit_llm_actions(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v229-metric-grounding-repair"
    frozen = v229.freeze_v229(output_dir=root)
    cleared = v229._project_action(
        {
            "repair_id": frozen["request"]["repair_id"],
            "action": "clear_all_metric_fields",
        },
        frozen,
    )
    assert all(cleared["event"][field] == "" for field in v229.METRIC_FIELDS)
    dropped = v229._project_action(
        {
            "repair_id": frozen["request"]["repair_id"],
            "action": "drop_event",
        },
        frozen,
    )
    assert dropped["event"] is None


def test_execution_runs_once_and_authorizes_only_support_alignment(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v229-metric-grounding-repair"
    calls: list[dict] = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def run_ephemeral_structured_turn(self, **kwargs):
            calls.append(kwargs)
            frozen = v229._load_frozen(root)
            output = {
                "repair_id": frozen["request"]["repair_id"],
                "action": "clear_metric_unit",
            }
            kwargs["capacity_checkpoint_path"].write_text("{}\n")
            kwargs["output_path"].write_text(json.dumps(output) + "\n")
            kwargs["sidecar_path"].write_text(
                json.dumps(
                    {
                        "state": "completed",
                        "usage_status": "measured",
                        "usage_complete": True,
                        "auth_type": "chatgpt",
                        "plan_type": "pro",
                        "model": v229.MODEL,
                        "effort": v229.EFFORT,
                        "error_class": None,
                        "usage": {
                            "input_tokens": 1_000,
                            "cached_input_tokens": 0,
                            "output_tokens": 100,
                            "reasoning_output_tokens": 0,
                            "total_tokens": 1_100,
                        },
                    }
                )
                + "\n"
            )
            return SimpleNamespace(status_ok=True, output=output)

    terminal = asyncio.run(
        v229.run_v229(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-5.6-luna"
    assert calls[0]["effort"] == "low"
    assert calls[0]["batch_size"] == 1
    assert terminal["structural_gate_passed"] is True
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    repeated = asyncio.run(
        v229.run_v229(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert repeated == terminal
    assert len(calls) == 1
