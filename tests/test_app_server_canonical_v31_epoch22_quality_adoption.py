from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_epoch22_quality_adoption as adoption
from research_factory import app_server_canonical_v31_single_message_quality_recovery as epoch21
from research_factory import app_server_llm_judge as judge


def _source_artifacts():
    paths = epoch21._turn_paths(adoption.EPOCH21_ROOT)  # noqa: SLF001
    output = json.loads(paths["output"].read_text(encoding="utf-8"))
    variant = json.loads(paths["variant"].read_text(encoding="utf-8"))
    return output, variant


def test_contract_binds_zero_call_exact_projection() -> None:
    contract = adoption.load_contract()
    assert contract["plan"]["step"]["max_model_calls"] == 0
    assert contract["plan"]["step"]["max_total_tokens"] == 0
    assert contract["directive"]["execution_contract"] == {
        "exact_span_projection_only": True,
        "semantic_fields_mutable": False,
        "semantic_model_call_cap": 0,
        "semantic_retry_cap": 0,
        "source_output_replay_allowed": False,
    }


def test_projection_is_exact_unique_and_limited_to_two_spans() -> None:
    output, variant = _source_artifacts()
    projected, audit = adoption.project_output(
        output, variant, adoption.load_contract()["directive"]
    )
    assert audit["projection_count"] == 2
    assert audit["semantic_fields_changed"] is False
    assert judge.validate_judge_output(projected, variant) == []


def test_projection_changes_only_declared_evidence_values() -> None:
    output, variant = _source_artifacts()
    projected, audit = adoption.project_output(
        output, variant, adoption.load_contract()["directive"]
    )
    restored = copy.deepcopy(projected)
    for change in audit["changes"]:
        source_span = output["cases"][change["case_index"]]["support_results"][
            change["support_index"]
        ]["evidence_spans"][change["span_index"]]
        restored["cases"][change["case_index"]]["support_results"][
            change["support_index"]
        ]["evidence_spans"][change["span_index"]] = source_span
    assert restored == output


def test_projection_rejects_ambiguous_or_nonmatching_text() -> None:
    output, variant = _source_artifacts()
    tampered = copy.deepcopy(output)
    tampered["cases"][5]["support_results"][11]["evidence_spans"][3] = "not present"
    with pytest.raises(adoption.Epoch22QualityAdoptionError, match="one exact"):
        adoption.project_output(
            tampered, variant, adoption.load_contract()["directive"]
        )


def test_recomputed_decision_is_a_measured_quality_rejection() -> None:
    projected, audit, consensus, score = adoption._decision(adoption.DEFAULT_ROOT)  # noqa: SLF001
    assert projected and audit and consensus
    assert score["passed"] is False
    assert score["failed_checks"] == adoption.EXPECTED_FAILED_CHECKS
    assert score["candidate_strict_full_field_macro_f1"] == 0.344287
    assert score["baseline_strict_full_field_macro_f1"] == 0.895374
    assert score["candidate_noninferiority_delta"] == -0.551087
    assert score["exact_evidence_rate"] == 1.0


def test_frozen_input_tamper_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    directive = json.loads(adoption.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    copied = tmp_path / "output.json"
    copied.write_bytes(adoption.EPOCH21_ROOT.joinpath(
        "turns/epoch21-origin-neutral-quality-recovery/output.private.json"
    ).read_bytes())
    directive["frozen_inputs"][-1] = {
        "role": "epoch21_output",
        "path": str(copied),
        "sha256": "0" * 64,
        "size_bytes": copied.stat().st_size,
    }
    directive_path = tmp_path / "directive.json"
    directive_path.write_text(json.dumps(directive), encoding="utf-8")
    plan = json.loads(adoption.PLAN_PATH.read_text(encoding="utf-8"))
    plan["step"]["directive_path"] = str(directive_path)
    plan["step"]["directive_sha256"] = hashlib.sha256(directive_path.read_bytes()).hexdigest()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    monkeypatch.setattr(adoption, "DIRECTIVE_PATH", directive_path)
    monkeypatch.setattr(adoption, "PLAN_PATH", plan_path)
    with pytest.raises(adoption.Epoch22QualityAdoptionError):
        adoption.load_contract()
