from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
    V23_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v81_capped_disagreement import (
    DEFAULT_OUTPUT_ROOT as V81_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v82_reference_freeze import (
    build_v82_reference,
    freeze_v82,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _build() -> tuple[dict, dict]:
    return build_v82_reference(
        base_truth=_load(V23_ROOT / "calibration-truth.private.json"),
        reconciliation=_load(V81_ROOT / "reference-reconciliation.private.json"),
        v75_truth=_load(V75_ROOT / "selected-truth.private.json"),
        v78_truth=_load(V78_ROOT / "fresh-truth.private.json"),
    )


def test_v82_applies_exactly_eight_llm_authorized_changes() -> None:
    reference, audit = _build()
    assert reference["reference_version"] == "fixture_reference_v3_v82_llm_only_reconciled"
    assert len(reference["cases"]) == 66
    assert sum(len(case["field_issues"]) for case in reference["cases"].values()) == 182
    assert audit["operation_count"] == 10
    assert audit["adjudication_change_count"] == 8
    assert audit["full_reference_change_count"] == 3
    assert audit["already_present_in_full_reference_count"] == 5
    assert sum(row["adjudication_changed"] for row in audit["operations"]) == 8
    assert sum(row["reference_changed"] for row in audit["operations"]) == 3
    assert audit["field_change_counts"] == {
        "attribution": 1,
        "speaker": 1,
        "stance": 1,
    }
    assert audit["semantic_decisions_from_llms_only"] is True
    assert audit["deterministic_code_scope"] == "identity_mapping_and_field_set_projection_only"
    assert audit["majority_voting_used"] is False


def test_v82_only_touches_authorized_case_witness_field_cells() -> None:
    base = _load(V23_ROOT / "calibration-truth.private.json")
    reference, audit = _build()
    authorized = {
        (row["case_id"], row["witness_id"], row["field"])
        for row in audit["operations"]
        if row["reference_changed"]
    }
    observed = set()
    for case_id, case in base["cases"].items():
        for witness_id, fields in case["field_issues"].items():
            before = set(fields)
            after = set(reference["cases"][case_id]["field_issues"][witness_id])
            for field in before ^ after:
                observed.add((case_id, witness_id, field))
    assert observed == authorized


def test_v82_freeze_is_zero_token_immutable_and_holdout_closed() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v82"
        terminal = freeze_v82(output_dir=root)
        again = freeze_v82(output_dir=root)
        assert again == terminal
        assert terminal["state"] == "completed"
        assert terminal["reference_frozen"] is True
        assert terminal["fresh_primary_repair_diagnostic_authorized"] is True
        assert terminal["full_calibration_authorized"] is False
        assert terminal["selection_authorized"] is False
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage_status"] == "complete"
        assert terminal["usage"]["total_tokens"] == 0
        receipt = _load(root / "reference-receipt.json")
        assert receipt["reference_frozen"] is True
        assert receipt["case_count"] == 66
        assert receipt["witness_count"] == 182
        assert receipt["adjudication_change_count"] == 8
        assert receipt["full_reference_change_count"] == 3
        assert receipt["already_present_in_full_reference_count"] == 5
        assert receipt["new_semantic_turn_count"] == 0
