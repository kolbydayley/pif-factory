from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration import (
    make_v5_calibration_pool,
    validate_v5_calibration_truth,
)
from research_factory.app_server_judge_v5_calibration_v119_full_reference_freeze import (
    _validate_sources,
    build_full_reference,
    freeze_v119,
)


def test_v119_projects_exactly_18_audited_cases_and_retains_48():
    sources = _validate_sources()
    full, audit = build_full_reference(
        sources["values"]["v101_truth"], sources["values"]["v118_reference"]
    )
    subset = sources["values"]["v118_reference"]["cases"]
    base = sources["values"]["v101_truth"]["cases"]
    assert len(full["cases"]) == 66
    assert sum(len(case["proposition"]) for case in full["cases"].values()) == 182
    assert len(full["canary_case_ids"]) == 12
    assert all(full["cases"][case_id] == case for case_id, case in subset.items())
    assert all(
        full["cases"][case_id] == case
        for case_id, case in base.items()
        if case_id not in subset
    )
    assert audit["audited_subset_case_count"] == 18
    assert audit["retained_v8_case_count"] == 48
    assert audit["changed_case_count"] == 18
    assert audit["semantic_decisions_by_deterministic_code"] is False


def test_v119_full_reference_passes_existing_fixture_validator():
    sources = _validate_sources()
    full, _ = build_full_reference(
        sources["values"]["v101_truth"], sources["values"]["v118_reference"]
    )
    pool, mapping, _ = make_v5_calibration_pool()
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=full)


def test_v119_freeze_is_idempotent_zero_token_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v119"
    first = freeze_v119(output_dir=root)
    second = freeze_v119(output_dir=root)
    assert first == second
    assert first["state"] == "completed"
    assert first["reference_frozen"] is True
    assert first["fresh_full_calibration_authorized"] is True
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["usage"]["total_tokens"] == 0
    truth = json.loads((root / "calibration-truth-v9-full.private.json").read_text())
    assert len(truth["cases"]) == 66
