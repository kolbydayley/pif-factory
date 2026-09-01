from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.signal_desk_rebuild_authoring import (
    GoldAuthoringPlanError,
    build_authoring_plan,
    materialize_authoring_inputs,
    storage_class,
    verify_authoring_plan,
)


def _manifest() -> dict:
    path = Path("work/signal-desk-rebuild/benchmark/partial-manifest.json")
    return json.loads(path.read_text(encoding="utf-8"))


def test_complete_plan_sequences_gold_then_one_shared_a1_a2_run() -> None:
    plan = build_authoring_plan(_manifest(), concurrency=4)
    assert plan["calls"]["gold_total"] == 2493
    assert plan["calls"]["blind_audit"] == 81
    assert plan["a1_a2_shared_run"]["prediction_run_count"] == 1
    assert plan["budget"]["eligible_for_gpt_5_5_rebuild_grant"] is False
    verify_authoring_plan(plan)


def test_validation_holdout_and_audit_are_item_level_sealed() -> None:
    plan = build_authoring_plan(_manifest())
    assert plan["storage"]["development"] == "development_item_storage"
    assert plan["storage"]["validation"] == "sealed_item_storage"
    assert plan["storage"]["sealed_holdout"] == "sealed_item_storage"
    assert plan["storage"]["blind_audit"] == "sealed_item_storage"
    assert storage_class("development", blind_audit=True) == "sealed_item_storage"


def test_plan_rejects_bad_concurrency_and_tampering() -> None:
    with pytest.raises(GoldAuthoringPlanError, match="between 2 and 8"):
        build_authoring_plan(_manifest(), concurrency=9)
    plan = build_authoring_plan(_manifest())
    plan["budget"]["eligible_for_gpt_5_5_rebuild_grant"] = True
    with pytest.raises(GoldAuthoringPlanError, match="hash verification"):
        verify_authoring_plan(plan)


def test_materialized_inputs_never_mix_development_with_sealed(tmp_path: Path) -> None:
    manifest = _manifest()
    receipt = materialize_authoring_inputs(
        manifest,
        project_root=Path.cwd(),
        output_root=tmp_path,
    )
    assert receipt["total_model_calls"] == 2493
    assert len(list((tmp_path / "development_item_storage").rglob("*.json"))) == 567
    assert len(list((tmp_path / "sealed_item_storage" / "validation").rglob("*.json"))) == 1188
    assert len(list((tmp_path / "sealed_item_storage" / "sealed_holdout").rglob("*.json"))) == 657
    assert len(list((tmp_path / "sealed_item_storage" / "blind_audit").rglob("*.json"))) == 81
    assert not (tmp_path / "development_item_storage" / "validation").exists()
    assert receipt["contains_transcript_text"] is False
