from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_epoch9_split_authority_recovery as recovery,
)


def _freeze(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "epoch9-split-authority-recovery"
    authorization_id = "kolby-epoch9-split-authority-test"
    issued = datetime.now(timezone.utc).replace(microsecond=0)
    recovery.freeze_recovery(
        root=root,
        operator_authorization_id=authorization_id,
        issued_at=issued.isoformat(),
        expires_at=(issued + timedelta(hours=2)).isoformat(),
    )
    return root, authorization_id


def test_epoch9_plan_and_directive_are_checksum_bound() -> None:
    directive_raw = recovery.DIRECTIVE_PATH.read_bytes()
    directive = json.loads(directive_raw)
    plan_path = recovery.PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v9.json"
    plan = json.loads(plan_path.read_bytes())

    assert hashlib.sha256(directive_raw).hexdigest() == recovery.DIRECTIVE_SHA256
    assert plan["plan_epoch"] == 9
    assert plan["state"] == "executable"
    assert plan["step"] == {
        "step_id": "canonical_v31_epoch9_split_authority_recovery_v9",
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
    assert directive["authorization_contract"]["authorized_by"] == "kolby"
    assert directive["authorization_contract"][
        "additional_interactive_approval_required"
    ] is False
    assert directive["expected_receipt_path"] == str(
        (recovery.DEFAULT_ROOT / recovery.RECEIPT_FILENAME).resolve()
    )


def test_epoch8_full_thread_usage_and_component_partition_are_exact() -> None:
    diagnostic = recovery._validate_epoch8_diagnostic()

    assert diagnostic["state"] == "rejected"
    assert diagnostic["last_inference_increment_usage"]["total_tokens"] == 92_732
    assert diagnostic["full_thread_total_usage"]["total_tokens"] == 580_618
    assert diagnostic["full_thread_total_usage_exceeded_ceiling"] is True
    assert len(diagnostic["valid_reference_labels"]) == 5
    assert len(diagnostic["invalid_reference_segment_ids"]) == 2
    assert [
        row["segment_id"] for row in diagnostic["invalid_reference_diagnostics"]
    ] == diagnostic["invalid_reference_segment_ids"]
    assert diagnostic["valid_context_output"]["episode_id"]
    assert diagnostic["whole_output_adoption_authorized"] is False
    assert diagnostic["completed_turn_replay_allowed"] is False


def test_freeze_prepares_one_repair_and_two_context_label_pairs(
    tmp_path: Path,
) -> None:
    root, authorization_id = _freeze(tmp_path)
    status = recovery.status_recovery(root)
    contract = recovery._load_object(root / recovery.CONTRACT_FILENAME, label="contract")
    authorization = recovery.verify_authorization(
        root,
        expected_authorization_id=authorization_id,
        require_current=True,
    )

    assert status["state"] == "ready"
    assert status["turn_states"] == ["absent"] * 5
    assert [turn["stage"] for turn in contract["turns"]] == [
        "repair_labels",
        "context",
        "labels",
        "context",
        "labels",
    ]
    assert contract["exact_turn_count"] == 5
    assert contract["phase_total_token_bound"] == 550_000
    assert contract["adopted_development_qa_total_tokens"] == 705_022
    assert authorization["new_model_call_cap"] == 5
    rejection = recovery._load_object(
        recovery._verify_record(
            contract["adopted_evidence"]["epoch8_accounting_rejection"],
            label="accounting rejection",
        ),
        label="accounting rejection",
    )
    assert rejection["state"] == "rejected"
    assert rejection["whole_output_adoption_authorized"] is False

    for turn in contract["turns"]:
        manifest = recovery._manifest(turn)
        assert manifest["model_visible_legacy_reference_label_count"] == 0
        assert manifest.get("model_visible_validation_diagnostic_count", 0) == 0
        if turn["stage"] == "context":
            prompt = recovery._verify_record(manifest["prompt"], label="context prompt")
            model_visible = prompt.read_text(encoding="ascii")
            assert "full_segmented_episode_text" in model_visible
        elif turn["stage"] == "labels":
            static_input = recovery._load_object(
                recovery._verify_record(manifest["static_input"], label="label input"),
                label="label input",
            )
            assert len(static_input["reference_regeneration"]) == 6
            assert "full_segmented_episode_text" not in static_input
            model_visible = recovery._canonical_json(static_input)
        else:
            repair_input = recovery._load_object(
                recovery._verify_record(manifest["input"], label="repair input"),
                label="repair input",
            )
            assert len(repair_input["reference_regeneration"]) == 2
            assert "full_segmented_episode_text" not in repair_input
            model_visible = recovery._canonical_json(repair_input)
        assert "legacy_reference_label" not in model_visible
        assert "validation_diagnostic" not in model_visible


def test_repair_partition_reconstructs_one_full_validator_clean_episode(
    tmp_path: Path,
) -> None:
    root, _ = _freeze(tmp_path)
    contract = recovery._load_object(root / recovery.CONTRACT_FILENAME, label="contract")
    turn = contract["turns"][0]
    manifest = recovery._manifest(turn)
    original = recovery._load_object(
        recovery._verify_record(
            manifest["original_authority_input"], label="original input"
        ),
        label="original input",
    )
    source_output = recovery._load_object(
        recovery.EPOCH8_ROOT
        / "turns"
        / manifest["source_epoch8_turn"]["turn_id"]
        / "output.private.json",
        label="epoch-8 raw output",
    )
    valid_template = copy.deepcopy(
        next(
            row
            for row in source_output["repaired_reference_labels"]
            if row["segment_id"] == "seg_04d1f3d721b8eb9f622eefda"
        )
    )
    repairs_by_id = {
        row["segment_id"]: row for row in original["reference_repairs"]
    }
    labels = []
    for segment_id in manifest["invalid_reference_segment_ids"]:
        label = copy.deepcopy(valid_template)
        label["segment_id"] = segment_id
        label["segment_quality"] = copy.deepcopy(
            repairs_by_id[segment_id]["legacy_reference_label"]["segment_quality"]
        )
        label["rejected_candidates"] = []
        labels.append(label)
    raw = {
        "schema_version": recovery.LABEL_OUTPUT_VERSION,
        "episode_id": turn["episode_id"],
        "repaired_reference_labels": labels,
    }
    validated, combined = recovery._validate_turn_output(
        root=root,
        contract=contract,
        turn=turn,
        raw_output=raw,
    )

    assert validated == raw
    assert combined is not None
    assert [row["segment_id"] for row in combined["repaired_reference_labels"]] == (
        original["invalid_reference_segment_ids"]
    )
    assert len(combined["repaired_reference_labels"]) == 7


def test_runtime_lock_tamper_is_rejected(tmp_path: Path) -> None:
    root, _ = _freeze(tmp_path)
    lock_path = root / recovery.RUNTIME_LOCK_FILENAME
    value = json.loads(lock_path.read_bytes())
    value["model"] = "drifted-model"
    lock_path.write_text(recovery._pretty_json(value), encoding="ascii")

    with pytest.raises(
        recovery.CanonicalV31Epoch9SplitRecoveryError,
        match="runtime lock drifted",
    ):
        recovery.verify_preauthorization(root)


def test_partial_attempt_terminalizes_waiting_without_client_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, authorization_id = _freeze(tmp_path)
    contract = recovery._load_object(root / recovery.CONTRACT_FILENAME, label="contract")
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
        raise AssertionError("partial recovery must not create an app-server client")

    monkeypatch.setattr(recovery.adapter, "_client_factory", forbidden_client)
    receipt = asyncio.run(
        recovery.execute_recovery(
            root=root,
            operator_authorization_id=authorization_id,
        )
    )

    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch9_split_authority_partial_attempt_preserved_no_replay"
    )
    assert receipt["new_semantic_model_call_count"] == 0
    assert receipt["semantic_retry_count"] == 0
    assert recovery.verify_recovery_receipt(root) == receipt


def test_api_key_environment_is_rejected_before_runtime_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, authorization_id = _freeze(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "forbidden-test-value")

    with pytest.raises(
        recovery.CanonicalV31Epoch9SplitRecoveryError,
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


def test_full_thread_total_not_last_increment_controls_cost_gate() -> None:
    completed = [
        {
            "thread_total_usage": {
                "input_tokens": 109_000,
                "cached_input_tokens": 100_000,
                "output_tokens": 2_000,
                "reasoning_output_tokens": 500,
                "total_tokens": 111_000,
            }
        }
    ]
    accounting = {
        "semantic_model_call_count": 1,
        "measured_usage": completed[0]["thread_total_usage"],
    }

    assert recovery._failed_checks(completed=completed, accounting=accounting) == [
        "new_per_turn_full_thread_total_token_cap"
    ]
