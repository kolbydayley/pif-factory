from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_epoch8_source_only_authority_recovery as recovery,
)


def _freeze(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "epoch8-source-only-recovery"
    authorization_id = "kolby-epoch8-source-only-test"
    issued = datetime.now(timezone.utc).replace(microsecond=0)
    recovery.freeze_recovery(
        root=root,
        operator_authorization_id=authorization_id,
        issued_at=issued.isoformat(),
        expires_at=(issued + timedelta(hours=2)).isoformat(),
    )
    return root, authorization_id


def test_epoch8_plan_and_directive_are_checksum_bound() -> None:
    directive_raw = recovery.DIRECTIVE_PATH.read_bytes()
    directive = json.loads(directive_raw)
    plan_path = recovery.PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v8.json"
    plan = json.loads(plan_path.read_bytes())

    assert hashlib.sha256(directive_raw).hexdigest() == recovery.DIRECTIVE_SHA256
    assert plan["plan_epoch"] == 8
    assert plan["state"] == "executable"
    assert plan["step"] == {
        "step_id": "canonical_v31_epoch8_source_only_authority_recovery_v8",
        "state": "executable",
        "max_model_calls": 3,
        "max_total_tokens": 283_596,
        "expected_receipt_path": str(
            recovery.DEFAULT_ROOT / recovery.RECEIPT_FILENAME
        ),
        "accepted_receipt_states": ["passed", "rejected", "waiting"],
        "directive_path": str(recovery.DIRECTIVE_PATH),
        "directive_sha256": recovery.DIRECTIVE_SHA256,
    }
    assert directive["predecessor"]["receipt"] == recovery._record(
        recovery.PREDECESSOR_ROOT / recovery.predecessor.EXECUTION_RECEIPT_FILENAME
    )
    assert directive["authorization_contract"][
        "operator_authorization_statement_sha256"
    ] == recovery.AUTHORIZATION_STATEMENT_SHA256


def test_freeze_adopts_one_turn_and_prepares_only_three_absent_turns(
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
    assert status["turn_states"] == ["absent", "absent", "absent"]
    assert status["adopted_predecessor_turn_count"] == 1
    assert contract["adopted_turn_count"] == 1
    assert contract["exact_turn_count"] == 3
    assert contract["cumulative_turn_count"] == 4
    assert contract["adopted_predecessor_total_tokens"] == 124_404
    assert contract["maximum_total_tokens_per_turn"] == 94_532
    assert contract["phase_total_token_bound"] == 283_596
    assert contract["cumulative_total_token_bound"] == 408_000
    assert authorization["new_model_call_cap"] == 3
    assert (root / recovery.predecessor.TURNS_DIRECTORY).exists() is False


def test_model_visible_prompts_remove_legacy_labels_and_materially_reduce_bytes(
    tmp_path: Path,
) -> None:
    root, _ = _freeze(tmp_path)
    contract = recovery._load_object(root / recovery.CONTRACT_FILENAME, label="contract")
    source_contract = recovery._load_object(
        recovery._verify_record(contract["source_runtime_contract"], label="source contract"),
        label="source contract",
    )

    for source_turn, compact_turn in zip(source_contract["turns"][1:], contract["turns"]):
        source_manifest = recovery._load_object(
            recovery._verify_record(source_turn["turn_manifest"], label="source manifest"),
            label="source manifest",
        )
        compact_manifest = recovery._load_object(
            recovery._verify_record(compact_turn["turn_manifest"], label="compact manifest"),
            label="compact manifest",
        )
        source_prompt = recovery._verify_record(source_manifest["prompt"], label="source prompt")
        compact_prompt = recovery._verify_record(compact_manifest["prompt"], label="compact prompt")
        compact_text = compact_prompt.read_text(encoding="ascii")
        compact_input = recovery._load_object(
            recovery._verify_record(compact_manifest["compact_input"], label="compact input"),
            label="compact input",
        )

        assert "legacy_reference_label" not in compact_text
        assert "validation_diagnostic_path" not in compact_text
        assert compact_manifest["model_visible_legacy_reference_label_count"] == 0
        assert compact_prompt.stat().st_size < source_prompt.stat().st_size * 0.61
        assert [row["segment_id"] for row in compact_input["reference_regeneration"]] == (
            compact_input["invalid_reference_segment_ids"]
        )
        assert all(
            set(row) == {
                "segment_id",
                "segment_text",
                "segment_text_sha256",
                "segment_quality",
            }
            for row in compact_input["reference_regeneration"]
        )
        assert compact_manifest["input"] == source_manifest["input"]
        assert recovery._load_object(
            recovery._verify_record(compact_manifest["output_schema"], label="compact schema"),
            label="compact schema",
        ) == recovery._load_object(
            recovery._verify_record(source_manifest["output_schema"], label="source schema"),
            label="source schema",
        )


def test_prompt_tamper_is_rejected_before_any_turn(tmp_path: Path) -> None:
    root, _ = _freeze(tmp_path)
    contract = recovery._load_object(root / recovery.CONTRACT_FILENAME, label="contract")
    manifest = recovery._load_object(
        recovery._verify_record(contract["turns"][0]["turn_manifest"], label="manifest"),
        label="manifest",
    )
    prompt = Path(manifest["prompt"]["path"])
    prompt.write_bytes(prompt.read_bytes() + b"tamper")

    with pytest.raises(
        recovery.CanonicalV31Epoch8SourceOnlyRecoveryError,
        match="prompt drifted",
    ):
        recovery.verify_preauthorization(root)
    assert (root / recovery.predecessor.TURNS_DIRECTORY).exists() is False


def test_runtime_lock_tamper_is_rejected(tmp_path: Path) -> None:
    root, _ = _freeze(tmp_path)
    lock_path = root / recovery.RUNTIME_LOCK_FILENAME
    value = json.loads(lock_path.read_bytes())
    value["model"] = "drifted-model"
    lock_path.write_text(recovery._pretty_json(value), encoding="ascii")

    with pytest.raises(
        recovery.CanonicalV31Epoch8SourceOnlyRecoveryError,
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
    paths = recovery.predecessor._turn_paths(root, first["turn_id"])
    paths["root"].mkdir(parents=True)
    recovery.predecessor._write_immutable_json(
        paths["attempt"],
        recovery.predecessor._attempt_payload(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=first,
        ),
    )

    def forbidden_client():
        raise AssertionError("partial recovery must not create an app-server client")

    monkeypatch.setattr(recovery.predecessor, "TRUSTED_CLIENT_FACTORY", forbidden_client)
    receipt = asyncio.run(
        recovery.execute_recovery(
            root=root,
            operator_authorization_id=authorization_id,
        )
    )

    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch8_source_only_authority_partial_attempt_preserved_no_replay"
    )
    assert receipt["new_semantic_model_call_count"] == 0
    assert receipt["cumulative_semantic_model_call_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert recovery.verify_recovery_receipt(root) == receipt


def test_api_key_environment_is_rejected_before_runtime_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, authorization_id = _freeze(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "forbidden-test-value")

    with pytest.raises(
        recovery.CanonicalV31Epoch8SourceOnlyRecoveryError,
        match="rejects API-key/raw-session auth",
    ):
        asyncio.run(
            recovery.execute_recovery(
                root=root,
                operator_authorization_id=authorization_id,
            )
        )

    assert (root / recovery.RECEIPT_FILENAME).exists() is False
    assert (root / recovery.predecessor.TURNS_DIRECTORY).exists() is False


def test_cost_failure_uses_cumulative_predecessor_accounting() -> None:
    predecessor_receipt = {
        "measured_usage": {
            "input_tokens": 102_733,
            "cached_input_tokens": 0,
            "output_tokens": 21_671,
            "reasoning_output_tokens": 2_484,
            "total_tokens": 124_404,
        }
    }
    new_usage = {
        "input_tokens": 250_000,
        "cached_input_tokens": 0,
        "output_tokens": 40_000,
        "reasoning_output_tokens": 5_000,
        "total_tokens": 290_000,
    }
    cumulative = recovery._cumulative_usage(predecessor_receipt, new_usage)
    checks = recovery._failed_checks(
        contract={"maximum_total_tokens_per_turn": 94_532},
        completed=[],
        accounting={
            "semantic_model_call_count": 3,
            "measured_usage": new_usage,
        },
        cumulative_usage=cumulative,
    )

    assert cumulative["total_tokens"] == 414_404
    assert checks == [
        "new_total_token_cap",
        "cumulative_authority_total_token_cap",
    ]
