from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v228_exact_span_repair as v228,
)


def test_source_units_project_verbatim_contiguous_ranges():
    source = "first line\n\nsecond line\r\nthird line"
    units = v228._source_units(source)
    assert [row["unit_id"] for row in units] == ["U000", "U001", "U002"]
    assert source[units[0]["start_char"] : units[0]["end_char"]] == "first line"
    start = units[0]["start_char"]
    end = units[1]["end_char"]
    assert source[start:end] == "first line\n\nsecond line"


def test_freeze_uses_direct_predecessor_and_salvages_second_episode(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v228-exact-span-repair"
    frozen = v228.freeze_v228(output_dir=root)
    spec = frozen["spec"]
    assert spec["declared_turn_count"] == 1
    assert spec["retry_count"] == 0
    assert spec["predecessor_calls_replayed"] is False
    assert spec["predecessor_second_episode_reused"] is True
    assert spec["production_protocol_additional_turns"] == 0
    assert spec["support_alignment_allowed_after_pass"] is True
    assert spec["holdout_authorized"] is False
    assert spec["production_mutation_allowed"] is False
    assert set(spec["direct_lineage"]) == set(v228.EXPECTED_PREDECESSOR_HASHES)
    salvage = json.loads((root / "second-episode-salvage.json").read_text())
    assert salvage["schema_status_success"] is True
    assert salvage["dense_new_event_count"] >= 1
    assert salvage["exactness_pruned_events"] == 0
    assert salvage["metric_grounding_error_events"] == 0
    assert salvage["event_cap_violations"] == 0
    assert salvage["exact_existing_duplicate_events"] == 0
    assert not (root / "launch-receipt.json").exists()
    turn_root = root / "turns/v228-exact-span-repair"
    assert frozen["paths"]["capacity"] == turn_root / "capacity.json"
    assert frozen["paths"]["sidecar"] == turn_root / "sidecar.json"
    v228.verify_runtime_lock(root / "runtime-lock.json")


def test_patch_projection_uses_only_selected_source_units(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v228-exact-span-repair"
    frozen = v228.freeze_v228(output_dir=root)
    units = frozen["source_units"]
    selected = units[len(units) // 2]
    output = {
        "repair_id": frozen["request"]["repair_id"],
        "action": "patch",
        "start_unit_id": selected["unit_id"],
        "end_unit_id": selected["unit_id"],
    }
    projected = v228._project_repair(output, frozen)
    assert projected["evidence"] == frozen["source"][
        selected["start_char"] : selected["end_char"]
    ]
    assert projected["evidence"] in frozen["source"]


def test_patch_projection_rejects_reversed_range_and_drop_ids(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v228-exact-span-repair"
    frozen = v228.freeze_v228(output_dir=root)
    first, last = frozen["source_units"][0], frozen["source_units"][-1]
    reversed_output = {
        "repair_id": frozen["request"]["repair_id"],
        "action": "patch",
        "start_unit_id": last["unit_id"],
        "end_unit_id": first["unit_id"],
    }
    with pytest.raises(v228.V228ExactSpanError):
        v228._project_repair(reversed_output, frozen)
    invalid_drop = {
        "repair_id": frozen["request"]["repair_id"],
        "action": "drop",
        "start_unit_id": first["unit_id"],
        "end_unit_id": first["unit_id"],
    }
    with pytest.raises(v228.V228ExactSpanError):
        v228._project_repair(invalid_drop, frozen)


def test_empty_excluded_source_context_is_schema_valid_without_relabeling(
    tmp_path: Path,
):
    root = tmp_path / "development-selection-v5_4-v228-exact-span-repair"
    frozen = v228.freeze_v228(output_dir=root)
    assert v228._validate_second_episode(
        frozen["predecessor_output"], frozen
    ) == []
    invalid = copy.deepcopy(frozen["predecessor_output"])
    empty = next(row for row in invalid["segments"] if not row["events"])
    empty["status"] = "coded"
    assert v228._validate_second_episode(invalid, frozen) == [
        "coded_status_event_presence"
    ]


def test_execution_dispatches_one_turn_and_cannot_replay(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v228-exact-span-repair"
    calls: list[dict] = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def run_ephemeral_structured_turn(self, **kwargs):
            calls.append(kwargs)
            frozen = v228._load_frozen(root)
            output = {
                "repair_id": frozen["request"]["repair_id"],
                "action": "drop",
                "start_unit_id": "NONE",
                "end_unit_id": "NONE",
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
                        "model": v228.MODEL,
                        "effort": v228.EFFORT,
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
        v228.run_v228(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-5.6-luna"
    assert calls[0]["effort"] == "low"
    assert calls[0]["batch_size"] == 1
    assert calls[0]["thread_mode"] == "new_thread"
    assert terminal["structural_gate_passed"] is True
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    repeated = asyncio.run(
        v228.run_v228(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert repeated == terminal
    assert len(calls) == 1
