from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from research_factory import app_server_canonical_v31_epoch7_authority_bridge as bridge
from research_factory import app_server_canonical_v31_epoch7_input_authority_runtime as authority_runtime
from research_factory import app_server_canonical_v31_epoch7_input_package as inputs
from tests import test_app_server_canonical_v31_epoch7_input_package as input_fixtures


def _write(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="ascii",
    )
    return path


def _authority_preauthorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    input_root: Path,
    episode_count: int,
    invalid_reference_count: int,
) -> tuple[Path, dict[str, Any]]:
    root = tmp_path / "authority-runtime"
    authority_plan_path = _write(
        tmp_path / "authority-plan" / "authority-plan.json",
        {
            "schema_version": "fixture_authority_plan",
            "input_package_receipt": bridge._record(
                input_root / inputs.RECEIPT_FILENAME
            ),
            "exact_turn_count": episode_count,
            "invalid_reference_count": invalid_reference_count,
        },
    )
    contract_path = _write(
        root / authority_runtime.CONTRACT_FILENAME,
        {
            "schema_version": "fixture_authority_runtime_contract",
            "exact_turn_count": episode_count,
            "authority_plan": bridge._record(authority_plan_path),
        },
    )
    lock_path = _write(root / authority_runtime.RUNTIME_LOCK_FILENAME, {"ok": True})
    preauthorization = {
        "schema_version": "fixture_authority_preauthorization",
        "state": "passed_zero_call_preauthorization_only",
        "semantic_model_call_count": 0,
        "operator_authorization_present": False,
        "runtime_contract": bridge._record(contract_path),
        "runtime_lock": bridge._record(lock_path),
    }
    _write(root / authority_runtime.PREAUTHORIZATION_FILENAME, preauthorization)
    monkeypatch.setattr(
        bridge.authority_runtime,
        "verify_preauthorization",
        lambda observed_root: (
            copy.deepcopy(preauthorization)
            if Path(observed_root) == root
            else (_ for _ in ()).throw(AssertionError("unexpected authority root"))
        ),
    )
    return root, preauthorization


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    input_fixture = input_fixtures._fixture(
        tmp_path,
        monkeypatch,
        complete_context=False,
        valid_reference=False,
    )
    waiting_receipt = input_fixtures._freeze(input_fixture, tmp_path)
    assert waiting_receipt["state"] == "waiting"
    source = json.loads(
        Path(waiting_receipt["source_binding"]["path"]).read_text(encoding="utf-8")
    )
    invalid_count = sum(
        row["canonical_v31_valid"] is not True
        for row in source["canonical_reference_validation"]
    )
    authority_root, preauthorization = _authority_preauthorization(
        tmp_path,
        monkeypatch,
        input_root=input_fixture["root"],
        episode_count=source["episode_count"],
        invalid_reference_count=invalid_count,
    )
    return {
        "input": input_fixture,
        "source": source,
        "waiting_receipt": waiting_receipt,
        "authority_root": authority_root,
        "preauthorization": preauthorization,
        "plan_root": tmp_path / "bridge-plan",
        "materialization_root": tmp_path / "bridge-materialization",
        "package_root": tmp_path / "bridge-materialization" / "input-package",
        "controller_root": tmp_path / "controller",
        "extraction_root": tmp_path / "extraction",
    }


def _freeze_plan(bundle: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    return bridge.freeze_bridge_plan(
        plan_root=bundle["plan_root"],
        materialization_root=bundle["materialization_root"],
        future_input_package_root=bundle["package_root"],
        future_controller_root=bundle["controller_root"],
        future_extraction_root=bundle["extraction_root"],
        database_path=bundle["input"]["database_path"],
        input_root=bundle["input"]["root"],
        authority_root=bundle["authority_root"],
        project_root=tmp_path,
    )


def _passed_authority(
    bundle: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Any], dict[str, Any]]:
    episode_id = bundle["source"]["episode_ids"][0]
    segment_ids = bundle["source"]["segment_ids"]
    references = bundle["input"]["legacy_info"]["reference_by_segment"]
    repaired = copy.deepcopy(references[segment_ids[0]]["golden_output"])
    repaired["schema_version"] = "ai_discourse_v3_1"
    preserved = copy.deepcopy(references[segment_ids[1]]["golden_output"])
    labels = [repaired, preserved]
    context = copy.deepcopy(bundle["input"]["context"])
    context["excluded_source_context"] = ["neutral setup when unsupported"]
    merged_episode = {
        "episode_id": episode_id,
        "episode_context": context,
        "episode_context_sha256": hashlib.sha256(
            bridge._canonical_json(context).encode("ascii")
        ).hexdigest(),
        "reference_labels": labels,
        "reference_labels_sha256": hashlib.sha256(
            bridge._canonical_json(labels).encode("ascii")
        ).hexdigest(),
        "repaired_reference_segment_ids": [segment_ids[0]],
        "preserved_valid_reference_segment_ids": [segment_ids[1]],
    }
    merged = {
        "schema_version": authority_runtime.MERGED_OUTPUT_VERSION,
        "state": "complete_development_authority_only",
        "episode_count": 1,
        "reference_label_count": 2,
        "repaired_reference_count": 1,
        "preserved_reference_count": 1,
        "episodes": [merged_episode],
        "deterministic_semantic_mutation": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    merged_path = _write(
        bundle["authority_root"] / authority_runtime.MERGED_OUTPUT_FILENAME,
        merged,
    )
    receipt = {
        "schema_version": authority_runtime.EXECUTION_RECEIPT_VERSION,
        "state": "passed",
        "failed_checks": [],
        "unknown_usage_turn_count": 0,
        "semantic_retry_count": 0,
        "semantic_model_call_count": 1,
        "completed_validated_turn_count": 1,
        "extraction_plan_rebuild_required": True,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "merged_authority_output": bridge._record(merged_path),
        "measured_usage": {
            "input_tokens": 100,
            "cached_input_tokens": 25,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        },
    }
    _write(
        bundle["authority_root"] / authority_runtime.EXECUTION_RECEIPT_FILENAME,
        receipt,
    )
    monkeypatch.setattr(
        bridge.authority_runtime,
        "verify_execution_receipt",
        lambda observed_root: (
            copy.deepcopy(receipt)
            if Path(observed_root) == bundle["authority_root"]
            else (_ for _ in ()).throw(AssertionError("unexpected authority root"))
        ),
    )
    return receipt, merged


def test_plan_is_immutable_waiting_and_zero_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)

    receipt = _freeze_plan(bundle, tmp_path)
    status = bridge.status_bridge(bundle["plan_root"], project_root=tmp_path)

    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == "passed_authority_execution_receipt_required"
    assert receipt["semantic_model_call_count_started_by_bridge"] == 0
    assert receipt["extraction_authorized"] is False
    assert status["state"] == "waiting"
    assert status["reason"] == "passed_authority_execution_receipt_required"
    assert bundle["materialization_root"].exists() is False
    assert {path.name for path in bundle["plan_root"].iterdir()} == {
        bridge.PLAN_CONTRACT_FILENAME,
        bridge.PLAN_RECEIPT_FILENAME,
        bridge.PLAN_TERMINAL_FILENAME,
    }


def test_plan_source_or_runtime_tamper_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)
    _freeze_plan(bundle, tmp_path)
    contract_path = bundle["plan_root"] / bridge.PLAN_CONTRACT_FILENAME
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["case_count"] += 1
    contract_path.write_text(json.dumps(contract, sort_keys=True) + "\n", encoding="ascii")

    with pytest.raises(
        bridge.CanonicalV31Epoch7AuthorityBridgeError,
        match="drifted",
    ):
        bridge.verify_bridge_plan(bundle["plan_root"], project_root=tmp_path)


def test_materialization_preserves_valid_reference_and_repairs_only_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)
    _freeze_plan(bundle, tmp_path)
    authority_receipt, merged = _passed_authority(bundle, monkeypatch)

    receipt = bridge.materialize_authority_inputs(
        plan_root=bundle["plan_root"], project_root=tmp_path
    )
    verified = bridge.verify_materialization(
        bundle["plan_root"], project_root=tmp_path
    )

    assert receipt["state"] == "passed"
    assert receipt["semantic_model_call_count_started_by_bridge"] == 0
    assert receipt["predecessor_authority_semantic_model_call_count"] == 1
    assert receipt["predecessor_authority_measured_usage"] == authority_receipt[
        "measured_usage"
    ]
    assert receipt["controller_authorization_present"] is False
    assert verified["handoff"]["state"] == (
        "ready_for_separately_authorized_epoch7_controller_plan"
    )
    package = verified["input_package_receipt"]
    assert package["state"] == "passed"
    assert package["case_count"] == 2
    reference = json.loads(Path(package["shared_reference_seed"]["path"]).read_text())
    assert [row["label"] for row in reference["references"]] == merged["episodes"][0][
        "reference_labels"
    ]
    preserved = bundle["input"]["legacy_info"]["reference_by_segment"][
        bundle["source"]["segment_ids"][1]
    ]["golden_output"]
    assert reference["references"][1]["label"] == preserved
    context = json.loads(Path(package["context_artifacts"][0]["path"]).read_text())
    assert context["episode_context"]["excluded_source_context"] == [
        "neutral setup when unsupported"
    ]
    assert bundle["controller_root"].exists() is False
    assert bundle["extraction_root"].exists() is False


def test_same_count_valid_reference_substitution_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)
    _freeze_plan(bundle, tmp_path)
    receipt, merged = _passed_authority(bundle, monkeypatch)
    merged["episodes"][0]["reference_labels"][1]["no_signal_reason"] = (
        "A different but still structurally valid semantic answer."
    )
    labels = merged["episodes"][0]["reference_labels"]
    merged["episodes"][0]["reference_labels_sha256"] = hashlib.sha256(
        bridge._canonical_json(labels).encode("ascii")
    ).hexdigest()
    merged_path = bundle["authority_root"] / authority_runtime.MERGED_OUTPUT_FILENAME
    merged_path.write_text(
        json.dumps(merged, sort_keys=True, indent=2) + "\n", encoding="ascii"
    )
    receipt["merged_authority_output"] = bridge._record(merged_path)
    receipt_path = bundle["authority_root"] / authority_runtime.EXECUTION_RECEIPT_FILENAME
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="ascii"
    )
    monkeypatch.setattr(
        bridge.authority_runtime,
        "verify_execution_receipt",
        lambda _root: copy.deepcopy(receipt),
    )

    with pytest.raises(
        bridge.CanonicalV31Epoch7AuthorityBridgeError,
        match="previously valid reference",
    ):
        bridge.materialize_authority_inputs(
            plan_root=bundle["plan_root"], project_root=tmp_path
        )
    assert bundle["materialization_root"].exists() is False


def test_materialization_handoff_tamper_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)
    _freeze_plan(bundle, tmp_path)
    _passed_authority(bundle, monkeypatch)
    bridge.materialize_authority_inputs(
        plan_root=bundle["plan_root"], project_root=tmp_path
    )
    handoff_path = bundle["materialization_root"] / bridge.HANDOFF_FILENAME
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    handoff["extraction_authorized"] = True
    handoff_path.write_text(
        json.dumps(handoff, sort_keys=True, indent=2) + "\n", encoding="ascii"
    )

    with pytest.raises(
        bridge.CanonicalV31Epoch7AuthorityBridgeError,
        match="handoff drifted",
    ):
        bridge.verify_materialization(bundle["plan_root"], project_root=tmp_path)


def test_materialize_without_passed_authority_never_creates_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path, monkeypatch)
    _freeze_plan(bundle, tmp_path)

    with pytest.raises(
        bridge.CanonicalV31Epoch7AuthorityBridgeError,
        match="execution receipt failed verification",
    ):
        bridge.materialize_authority_inputs(
            plan_root=bundle["plan_root"], project_root=tmp_path
        )
    assert bundle["materialization_root"].exists() is False
