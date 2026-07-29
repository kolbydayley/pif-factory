from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_single_message_quality_recovery as recovery
from research_factory import app_server_llm_judge as judge


def _run(coro):
    try:
        prior_loop = asyncio.get_event_loop()
    except RuntimeError:
        prior_loop = asyncio.new_event_loop()
    try:
        return asyncio.run(coro)
    finally:
        asyncio.set_event_loop(prior_loop)


def _frozen_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    contract = recovery.load_contract()
    root = tmp_path / "epoch-21"
    fixture_contract = copy.deepcopy(contract)
    fixture_contract["receipt_path"] = root / "plan-step-receipt.json"
    monkeypatch.setattr(
        recovery, "load_contract", lambda: copy.deepcopy(fixture_contract)
    )
    recovery.freeze_run(root)
    return root


def _valid_output(variant: dict) -> dict:
    cases = []
    for case in variant["cases"]:
        witnesses = case["event_set_a"] + case["event_set_b"]
        cases.append(
            {
                "case_id": case["case_id"],
                "support_results": [
                    {
                        "witness_id": witness["witness_id"],
                        "verdict": "supported",
                        "evidence_spans": [witness["event"]["evidence"]],
                        "rationale": "Synthetic exact source-support decision.",
                    }
                    for witness in witnesses
                ],
                "equivalence_groups": [
                    {
                        "witness_ids": [witness["witness_id"]],
                        "rationale": "Synthetic singleton full-field partition.",
                    }
                    for witness in witnesses
                ],
                "alignments": [],
                "unaligned_left_witness_ids": [
                    witness["witness_id"] for witness in case["event_set_a"]
                ],
                "unaligned_right_witness_ids": [],
            }
        )
    output = {"cases": cases}
    assert judge.validate_judge_output(output, variant) == []
    return output


class _CompletedClient:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.policy_path = root / "capacity-policy.json"
        self.policy = recovery.reserve.load_reserve_capacity_policy(self.policy_path)
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        assert not self.calls
        prompt_packet = json.loads(kwargs["prompt"].split("# Blinded cases\n", 1)[1])
        variant = {
            "schema_version": judge.JUDGE_VARIANT_VERSION,
            "variant": "side_free_adjudication",
            "orientation": "ab",
            "cases": prompt_packet["cases"],
        }
        output = _valid_output(variant)
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(kwargs["output_path"]).resolve()
        sidecar_path = Path(kwargs["sidecar_path"]).resolve()
        capacity_path = Path(kwargs["capacity_checkpoint_path"]).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        evaluated = recovery.reserve.evaluate_reserve_capacity(
            {"primary_used_percent": 18, "rate_limit_reached_type": None},
            policy=self.policy,
            remaining_turn_count=1,
        )
        capacity = {
            "schema_version": recovery.reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": "2026-07-19T23:30:00+00:00",
            "policy_path": str(self.policy_path.resolve()),
            "policy_sha256": recovery._sha256_file(self.policy_path),  # noqa: SLF001
            "phase_id": recovery.STEP_ID,
            "turn_name": recovery.TURN_NAME,
            "turn_ordinal": 0,
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "primary_used_percent": evaluated["primary_used_percent"],
            "primary_remaining_percent": evaluated["primary_remaining_percent"],
            "primary_resets_at": 1_800_000_000,
            "rate_limit_reached_type": None,
            "minimum_remaining_reserve_percent": evaluated[
                "minimum_remaining_reserve_percent"
            ],
            "usable_percent_above_reserve": evaluated[
                "usable_percent_above_reserve"
            ],
            "remaining_turn_count": 1,
            "projected_remaining_tokens": recovery.NEW_TOTAL_TOKEN_CAP,
            "quota_points_per_million_tokens": recovery.QUOTA_POINTS_PER_MILLION_TOKENS,
            "projected_remaining_quota_points": recovery.PROJECTED_PHASE_QUOTA_POINTS,
            "projected_terminal_remaining_percent": evaluated[
                "projected_terminal_remaining_percent"
            ],
            "cleared_for_semantic_turn": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": (
                "capacity_status_policy_hash_and_counts_no_prompt_output_email_"
                "credentials_or_thread_ids"
            ),
        }
        capacity_path.write_text(json.dumps(capacity, sort_keys=True) + "\n", encoding="utf-8")
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_text = recovery._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        sidecar = {
            "schema_version": recovery.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "cli_version": recovery.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "client_version": recovery.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_sha256": recovery.codex_app_server.PROTOCOL_SCHEMA_SHA256,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "batch_size": kwargs["batch_size"],
            "thread_id": "epoch21-fixture-thread",
            "turn_id": "epoch21-fixture-turn",
            "app_server_user_agent": "fixture-codex-app-server",
            "output_path": str(output_path),
            "prompt_sha256": recovery._sha256_text(kwargs["prompt"]),  # noqa: SLF001
            "base_instructions_sha256": recovery._sha256_text(  # noqa: SLF001
                kwargs["base_instructions"]
            ),
            "output_schema_sha256": recovery._sha256_text(schema_text),  # noqa: SLF001
            "output_sha256": recovery._sha256_text(output_text),  # noqa: SLF001
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_bytes": len(
                kwargs["base_instructions"].encode("utf-8")
            ),
            "output_schema_bytes": len(schema_text.encode("utf-8")),
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "finished_at": "2026-07-19T23:31:00+00:00",
        }
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
        self.calls.append(copy.deepcopy(kwargs))
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
            usage=SimpleNamespace(total_tokens=120),
        )


def test_contract_adopts_exact_epoch20_failure_without_replay() -> None:
    contract = recovery.load_contract()

    assert contract["predecessor_accounting"]["usage"] == recovery.EXPECTED_PREDECESSOR_USAGE
    assert recovery.EXPECTED_AB_VALIDATION_ERRORS == (
        "case_0_support_0_evidence_not_exact",
        "case_5_alignment_0_invalid_or_duplicate_left",
    )
    assert contract["directive"]["execution_contract"]["prior_ab_ba_replay_allowed"] is False
    assert contract["directive"]["execution_contract"]["prior_outputs_exposed_to_recovery_model"] is False
    assert len(contract["pool"]["cases"]) == 6


def test_freeze_is_zero_call_and_full_pool_origin_neutral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    lock = recovery.verify_runtime_lock(root / "runtime-lock.json")
    variant = json.loads(recovery._turn_paths(root)["variant"].read_text(encoding="utf-8"))
    prompt = recovery._turn_paths(root)["prompt"].read_text(encoding="utf-8")

    assert lock["new_model_call_cap"] == 1
    assert lock["aggregate_model_call_cap"] == 3
    assert len(variant["cases"]) == 6
    assert sum(len(case["event_set_a"]) for case in variant["cases"]) == 151
    assert all(case["event_set_b"] == [] for case in variant["cases"])
    assert "Prior judge outputs are not shown" in prompt
    assert not (root / "launch-receipt.json").exists()
    assert not recovery._turn_paths(root)["capacity"].exists()
    assert not recovery._turn_paths(root)["sidecar"].exists()
    assert not recovery._turn_paths(root)["output"].exists()


def test_completed_fixture_scores_once_with_aggregate_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root)

    receipt = _run(
        recovery.run(output_dir=root, client_factory=lambda _root: fake)
    )

    assert receipt["state"] in {"passed", "rejected"}
    assert receipt["new_semantic_model_call_count"] == 1
    assert receipt["aggregate_semantic_model_call_count"] == 3
    assert receipt["semantic_retry_count"] == 0
    assert receipt["new_usage"]["total_tokens"] == 120
    assert receipt["aggregate_usage"]["total_tokens"] == 244_101
    assert len(fake.calls) == 1
    assert recovery.verify_receipt(root)["state"] == receipt["state"]


def test_partial_attempt_waits_without_client_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    recovery._launch_receipt(root)  # noqa: SLF001
    sidecar = recovery._turn_paths(root)["sidecar"]
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text('{"state":"in_progress"}\n', encoding="utf-8")
    called = False

    def must_not_create(_root):
        nonlocal called
        called = True
        raise AssertionError("partial recovery replayed the turn")

    receipt = _run(recovery.run(output_dir=root, client_factory=must_not_create))

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == "epoch21_partial_attempt_preserved_without_replay"
    assert receipt["semantic_retry_count"] == 0
    assert receipt["development_quality_passed"] is False
    assert recovery.verify_receipt(root)["state"] == "waiting"


def test_frozen_request_tamper_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    prompt = recovery._turn_paths(root)["prompt"]
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift\n", encoding="utf-8")

    with pytest.raises(recovery.QualityRecoveryError, match="runtime lock drifted"):
        recovery.verify_runtime_lock(root / "runtime-lock.json")
