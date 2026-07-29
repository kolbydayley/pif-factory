from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_epoch12_canonical_consistency_authority_recovery as recovery,
)


def _freeze(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "epoch12-canonical-consistency"
    authorization_id = "kolby-epoch12-canonical-consistency-test"
    issued = datetime.now(timezone.utc).replace(microsecond=0)
    recovery.freeze_recovery(
        root=root,
        operator_authorization_id=authorization_id,
        issued_at=issued.isoformat(),
        expires_at=(issued + timedelta(hours=2)).isoformat(),
    )
    return root, authorization_id


def _all_no_signal_output(request: dict) -> dict:
    return {
        "episode_id": request["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "The fixture classifies this segment as no signal.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "The fixture emits no grounded discourse event.",
                "overall_confidence": 1.0,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in segment["units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
            for segment in request["private_input"]["segments"]
        ],
    }


def test_epoch12_plan_and_directive_are_checksum_bound() -> None:
    directive_raw = recovery.DIRECTIVE_PATH.read_bytes()
    directive = json.loads(directive_raw)
    plan_path = recovery.PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v12.json"
    plan = json.loads(plan_path.read_bytes())

    assert hashlib.sha256(directive_raw).hexdigest() == recovery.DIRECTIVE_SHA256
    assert plan["plan_epoch"] == 12
    assert plan["state"] == "executable"
    assert plan["step"] == {
        "step_id": recovery.STEP_ID,
        "state": "executable",
        "max_model_calls": 5,
        "max_total_tokens": 550_000,
        "expected_receipt_path": str(
            recovery.DEFAULT_ROOT / recovery.RECEIPT_FILENAME
        ),
        "accepted_receipt_states": ["passed", "rejected", "waiting"],
        "directive_path": str(recovery.DIRECTIVE_PATH),
        "directive_sha256": recovery.DIRECTIVE_SHA256,
    }
    assert directive["authorization_contract"][
        "additional_interactive_approval_required"
    ] is False


def test_epoch11_rejection_is_one_measured_canonical_consistency_repair() -> None:
    predecessor = recovery._validate_epoch11_predecessor()

    assert predecessor["measured_usage"]["total_tokens"] == 28_053
    assert predecessor["fresh_repair_segment_id"] == (
        "seg_75643e2d9d620af459f9dee3"
    )
    assert predecessor["sanitized_invalid_diagnostic"]["diagnostic_path"] == (
        "$.discourse_events[4].metric.raw_text must be an exact evidence substring"
    )
    assert predecessor["receipt"]["new_semantic_model_call_count"] == 1
    assert predecessor["receipt"]["semantic_retry_count"] == 0


def test_freeze_binds_five_turns_and_sanitized_repair_request(tmp_path: Path) -> None:
    root, authorization_id = _freeze(tmp_path)
    contract = recovery._load_object(
        root / recovery.CONTRACT_FILENAME, label="contract"
    )
    status = recovery.status_recovery(root)
    authorization = recovery.verify_authorization(
        root,
        expected_authorization_id=authorization_id,
        require_current=True,
    )

    assert status["state"] == "ready"
    assert status["turn_states"] == ["absent"] * 5
    assert [turn["stage"] for turn in contract["turns"]] == [
        "repair_canonical_labels",
        "context",
        "canonical_labels",
        "context",
        "canonical_labels",
    ]
    assert contract["adopted_development_qa_total_tokens"] == 795_124
    assert authorization["new_model_call_cap"] == 5
    repair = contract["turns"][0]
    io = recovery._io_binding(root, repair)
    request = recovery._load_object(
        recovery._verify_record(io["request"], label="repair request"),
        label="repair request",
    )
    model_visible = request["prompt"] + request["base_instructions"]
    assert request["segment_ids"] == ["seg_75643e2d9d620af459f9dee3"]
    assert request["retry_count"] == 0
    assert recovery.bounded_adapter.EVIDENCE_BOUND_INSTRUCTION in request[
        "base_instructions"
    ]
    assert request["base_instructions"].endswith(
        recovery.adapter.CANONICAL_SELF_AUDIT_INSTRUCTION
    )
    assert "at most 1000 source characters" in request["base_instructions"]
    assert "direction is not_applicable" in request["base_instructions"]
    assert "legacy_reference_label" not in model_visible
    assert "validation_diagnostic" not in model_visible
    assert "metric.raw_text must" not in model_visible
    assert recovery._load_object(
        recovery._verify_record(
            contract["adopted_evidence"]["epoch11_canonical_consistency_diagnostic"],
            label="diagnostic",
        ),
        label="diagnostic",
    ) == {
        "schema_version": recovery.REJECTION_VERSION,
        "segment_id": "seg_75643e2d9d620af459f9dee3",
        "error_class": "ValidationError",
        "diagnostic_path": (
            "$.discourse_events[4].metric.raw_text must be an exact evidence substring"
        ),
        "transcript_text_present": False,
        "legacy_label_text_present": False,
        "fresh_llm_repair_required": True,
        "llm_owned_canonical_cross_field_self_audit_required": True,
        "diagnostic_model_visible": False,
    }


def test_repair_request_projects_and_reconstructs_full_authority_output(
    tmp_path: Path,
) -> None:
    root, _ = _freeze(tmp_path)
    contract = recovery._load_object(
        root / recovery.CONTRACT_FILENAME, label="contract"
    )
    turn = contract["turns"][0]
    io = recovery._io_binding(root, turn)
    request = recovery._load_object(
        recovery._verify_record(io["request"], label="repair request"),
        label="repair request",
    )
    projected = recovery.adapter.validate_and_project_output(
        request, _all_no_signal_output(request)
    )
    combined = recovery._combine_repair_output(contract, turn, projected)

    assert len(projected["labels"]) == 1
    assert len(combined["repaired_reference_labels"]) == 7
    assert [
        row["segment_id"] for row in combined["repaired_reference_labels"]
    ] == recovery._load_object(
        recovery._verify_record(
            recovery._manifest(turn)["original_authority_input"],
            label="original input",
        ),
        label="original input",
    )["invalid_reference_segment_ids"]


def test_dynamic_label_request_is_derived_only_from_context_and_source(
    tmp_path: Path,
) -> None:
    root, _ = _freeze(tmp_path)
    contract = recovery._load_object(
        root / recovery.CONTRACT_FILENAME, label="contract"
    )
    context_turn = contract["turns"][1]
    label_turn = contract["turns"][2]
    context_paths = recovery._turn_paths(root, context_turn["turn_id"])
    context_paths["root"].mkdir(parents=True)
    context_record = recovery._write_json(
        context_paths["validated_output"],
        {
            "schema_version": recovery.epoch9.CONTEXT_OUTPUT_VERSION,
            "episode_id": context_turn["episode_id"],
            "excluded_source_context": [],
        },
    )
    recovery._write_json(
        context_paths["result"], {"validated_output": context_record}
    )

    io = recovery._materialize_dynamic_label_io(root, contract, label_turn)
    request = recovery._load_object(
        recovery._verify_record(io["request"], label="dynamic request"),
        label="dynamic request",
    )

    assert request["segment_ids"] == recovery._manifest(label_turn)["segment_ids"]
    assert request["batch_size_ceiling"] == 8
    assert request["effective_batch_size"] == 6
    assert request["episode_context"]["excluded_source_context"] == []
    assert request["base_instructions"].endswith(
        recovery.adapter.CANONICAL_SELF_AUDIT_INSTRUCTION
    )
    assert "legacy_reference_label" not in request["prompt"]
    assert "validation_diagnostic" not in request["base_instructions"]


def test_partial_attempt_terminalizes_waiting_before_client_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, authorization_id = _freeze(tmp_path)
    contract = recovery._load_object(
        root / recovery.CONTRACT_FILENAME, label="contract"
    )
    authorization = recovery.verify_authorization(
        root,
        expected_authorization_id=authorization_id,
        require_current=True,
    )
    first = contract["turns"][0]
    paths = recovery._turn_paths(root, first["turn_id"])
    paths["root"].mkdir(parents=True)
    recovery._write_json(
        paths["attempt"],
        recovery.epoch7._attempt_payload(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=first,
        ),
    )

    def forbidden_client():
        raise AssertionError("partial recovery must not create a client")

    monkeypatch.setattr(recovery.adapter, "_client_factory", forbidden_client)
    receipt = asyncio.run(
        recovery.execute_recovery(
            root=root,
            operator_authorization_id=authorization_id,
        )
    )

    assert receipt["schema_version"] == "pif_semantic_plan_step_receipt_v1"
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch12_partial_attempt_preserved_no_replay"
    )
    assert receipt["new_semantic_model_call_count"] == 0
    assert receipt["semantic_retry_count"] == 0
    assert recovery.verify_recovery_receipt(root) == receipt

    attempt = json.loads(paths["attempt"].read_bytes())
    attempt["state"] = "forged-after-terminal"
    paths["attempt"].write_text(recovery._pretty_json(attempt), encoding="ascii")
    with pytest.raises(recovery.CanonicalV31Epoch12Error, match="attempt drifted"):
        recovery.verify_recovery_receipt(root)


def test_runtime_lock_tamper_is_rejected(tmp_path: Path) -> None:
    root, _ = _freeze(tmp_path)
    lock_path = root / recovery.RUNTIME_LOCK_FILENAME
    value = json.loads(lock_path.read_bytes())
    value["model"] = "drifted-model"
    lock_path.write_text(recovery._pretty_json(value), encoding="ascii")

    with pytest.raises(recovery.CanonicalV31Epoch12Error, match="runtime lock drifted"):
        recovery.verify_preauthorization(root)


def test_api_key_environment_is_rejected_before_runtime_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, authorization_id = _freeze(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "forbidden-test-value")

    with pytest.raises(
        recovery.CanonicalV31Epoch12Error,
        match="rejects API-key/raw-session auth",
    ):
        asyncio.run(
            recovery.execute_recovery(
                root=root,
                operator_authorization_id=authorization_id,
            )
        )

    assert (root / recovery.RECEIPT_FILENAME).exists() is False
    assert (root / recovery.TURNS_DIRECTORY).exists() is False
