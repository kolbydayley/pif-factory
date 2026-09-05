from __future__ import annotations

import json
import asyncio
from pathlib import Path

from research_factory.signal_desk_gold_repair_v2 import (
    FULL_WINDOW_COUNT,
    PILOT_EVENT_FLOOR,
    REPRESENTATION_VARIANT_ID,
    build_repair_plan,
    select_powered_pilot_windows,
)
from research_factory.signal_desk_gold_representation import build_speaker_map_header


def test_header_uses_repeated_transcript_labels_and_rejects_page_chrome() -> None:
    text = (
        "Marc Andreessen: This is a substantive claim about markets.\n\n"
        "Balaji Srinivasan: I disagree with that claim.\n\n"
        "General\n\nBalaji Srinivasan: A second turn.\n\nMarc Andreessen: A third turn.\n"
    )
    header = build_speaker_map_header(
        metadata={"window_id": "w", "episode_id": "e", "transcript_structure": "paragraph"},
        window_text=text,
    )
    assert header["map_status"] == "supported"
    assert [row["speaker_id"] for row in header["speaker_map"]] == [
        "Balaji Srinivasan", "Marc Andreessen"
    ]
    assert all(row["basis"] == "transcript_label" for row in header["speaker_map"])
    assert "General" not in {row["speaker_id"] for row in header["speaker_map"]}


def test_header_quarantines_unresolved_identity_without_guessing() -> None:
    header = build_speaker_map_header(
        metadata={
            "window_id": "w", "episode_id": "e", "transcript_structure": "paragraph",
            "show_name": "A Show", "episode_title": "An Episode",
        },
        window_text="This paragraph has a claim but no supported speaker label.",
    )
    assert header["map_status"] == "unresolved"
    assert header["speaker_map"] == []
    assert "Never infer" in header["identity_policy"]


def test_header_can_use_explicit_metadata_only_when_surface_supported() -> None:
    header = build_speaker_map_header(
        metadata={
            "window_id": "w", "episode_id": "e", "transcript_structure": "flattened",
            "speaker_map": [{"speaker_id": "guest_1", "name": "Ada Lovelace"}],
        },
        window_text="Ada Lovelace argued that the system needed testing.",
    )
    assert header["speaker_map"] == [{
        "speaker_id": "guest_1", "surface_forms": ["Ada Lovelace"],
        "basis": "frozen_episode_metadata_surface_supported",
    }]


def test_powered_pilot_is_deterministic_and_has_event_floor(tmp_path: Path) -> None:
    rows = []
    root = tmp_path / "C"
    root.mkdir()
    for index in range(FULL_WINDOW_COUNT):
        window_id = f"w{index:03d}"
        rows.append({
            "window_id": window_id, "split": "development",
            "show_id": f"show-{index:02d}",
            "transcript_structure": ("speaker_turn", "paragraph", "flattened", "asr_diarized")[index % 4],
        })
        (root / f"{window_id}.json").write_text(json.dumps({"events": [{"speech_act": "assertion"}] * (30 if index < 100 else 1)}))
    manifest = {"windows": rows}
    first = select_powered_pilot_windows(manifest=manifest, baseline_c_root=root)
    second = select_powered_pilot_windows(manifest=manifest, baseline_c_root=root)
    assert first == second
    assert len(first) == 40
    assert sum(json.loads((root / f"{value}.json").read_text())["events"].__len__() for value in first) >= PILOT_EVENT_FLOOR


def test_real_plan_reuses_ab_and_only_schedules_c() -> None:
    plan = build_repair_plan(
        manifest_path=Path("work/signal-desk-rebuild/benchmark/partial-manifest.json"),
        project_root=Path.cwd(),
        baseline_root=Path("work/signal-desk-rebuild/gold-authoring-v2/results/development"),
        output_root=Path("/tmp/signal-desk-gold-v2-test"),
        pilot=True,
    )
    assert plan["variant_id"] == REPRESENTATION_VARIANT_ID
    assert plan["expected_calls"]["A"] == 0
    assert plan["expected_calls"]["B"] == 0
    assert plan["expected_calls"]["C"] == 40
    assert plan["one_change_invariant"]["frozen_baseline_ab_reused"] is True
    assert plan["reserved_token_ceiling"] == 40 * 57_000


def test_representation_registry_is_separate_from_prompt_registry() -> None:
    registry = json.loads(Path("config/signal_desk_gold_representation_variants.json").read_text())
    variant = registry["variants"][0]
    assert variant["family_type"] == "representation"
    assert variant["variant_id"] == REPRESENTATION_VARIANT_ID
    assert variant["prompt_variant_id"] == "gold-authoring-baseline-v2"
    assert variant["expected_calls"] == {"A": 0, "B": 0, "C": 189, "total": 189}


def test_runner_accepts_representation_family_and_window_subset(monkeypatch, tmp_path: Path) -> None:
    import research_factory.signal_desk_gold_runner as runner

    captured = {}

    async def fake_run(**kwargs):
        captured.update(kwargs)
        return {"complete": True}

    monkeypatch.setattr(runner, "_run_gold_split_phases", fake_run)
    result = asyncio.run(runner.run_gold_split_phase(
        manifest_path=Path("work/signal-desk-rebuild/benchmark/partial-manifest.json"),
        project_root=Path.cwd(), result_root=tmp_path / "results",
        dispatch_database=tmp_path / "dispatch.sqlite", budget_database=tmp_path / "budget.sqlite",
        grant_path=tmp_path / "grant.json", session_root=tmp_path / "sessions",
        budget_dir=tmp_path / "budget", split="development", turn_type="C",
        task_namespace="v2-test", representation_variant_id=REPRESENTATION_VARIANT_ID,
        target_window_ids=["sdw_test"],
    ))
    assert result == {"complete": True}
    assert captured["representation_variant_id"] == REPRESENTATION_VARIANT_ID
    assert captured["target_window_ids"] == ["sdw_test"]
