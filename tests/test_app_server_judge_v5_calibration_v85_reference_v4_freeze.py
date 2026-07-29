from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v82_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as V82_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V84_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    build_v85_reference,
    freeze_v85,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _build() -> tuple[dict, dict]:
    return build_v85_reference(
        _load(V82_ROOT / "calibration-truth-v3.private.json"),
        _load(V84_ROOT / "reference-patch-proposal.json"),
    )


def test_v85_applies_exactly_two_authorized_metric_target_changes() -> None:
    reference, audit = _build()
    assert reference["reference_version"] == "fixture_reference_v4_v85_metric_target_reconciled"
    assert len(reference["cases"]) == 66
    assert sum(len(case["field_issues"]) for case in reference["cases"].values()) == 182
    assert audit["operation_count"] == 3
    assert audit["reference_change_count"] == 2
    assert audit["field_change_counts"] == {"metric": 1, "target": 1}
    assert sum(row["reference_changed"] for row in audit["operations"]) == 2
    assert audit["semantic_decisions_from_llms_only"] is True
    assert audit["deterministic_code_scope"] == "identity_mapping_and_field_set_projection_only"
    assert audit["majority_voting_used"] is False


def test_v85_only_touches_two_authorized_cells() -> None:
    base = _load(V82_ROOT / "calibration-truth-v3.private.json")
    reference, audit = _build()
    authorized = {
        (row["case_id"], row["witness_id"], row["field"])
        for row in audit["operations"]
        if row["reference_changed"]
    }
    observed = set()
    for case_id, case in base["cases"].items():
        for witness_id, fields in case["field_issues"].items():
            after = set(reference["cases"][case_id]["field_issues"][witness_id])
            for field in set(fields) ^ after:
                observed.add((case_id, witness_id, field))
    assert observed == authorized


def test_v85_freeze_is_zero_token_immutable_and_holdout_closed() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v85"
        terminal = freeze_v85(output_dir=root)
        assert freeze_v85(output_dir=root) == terminal
        assert terminal["state"] == "completed"
        assert terminal["reference_frozen"] is True
        assert terminal["fresh_enhanced_diagnostic_authorized"] is True
        assert terminal["full_calibration_authorized"] is False
        assert terminal["selection_authorized"] is False
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage"]["total_tokens"] == 0
        receipt = _load(root / "reference-receipt.json")
        assert receipt["reference_change_count"] == 2
        assert receipt["new_semantic_turn_count"] == 0
