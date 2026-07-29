from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_candidate_expanded_cap_direct_reference_v6 as v6
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


def _temp_contract(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "epoch-6"
    directive = json.loads(v6.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    directive["expected_receipt_path"] = str((root / "plan-step-receipt.json").resolve())
    directive_path = tmp_path / "directive-v6.json"
    directive_path.write_text(
        json.dumps(directive, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    plan = json.loads(v6.PLAN_PATH.read_text(encoding="utf-8"))
    plan["step"]["expected_receipt_path"] = directive["expected_receipt_path"]
    plan["step"]["directive_path"] = str(directive_path.resolve())
    plan["step"]["directive_sha256"] = hashlib.sha256(
        directive_path.read_bytes()
    ).hexdigest()
    plan_path = tmp_path / "semantic-plan-v6.json"
    plan_path.write_text(
        json.dumps(plan, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return root, plan_path, directive_path


def _freeze(tmp_path: Path) -> Path:
    root, plan_path, directive_path = _temp_contract(tmp_path)
    v6.freeze_run(
        root,
        plan_path=plan_path,
        directive_path=directive_path,
    )
    return root


def _snapshot(*, used: int) -> dict:
    return {
        "schema_version": "pif_app_server_rate_limit_probe_v1",
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "limit_id": "codex",
        "primary_used_percent": used,
        "primary_resets_at": 1_800_000_000,
        "rate_limit_reached_type": None,
        "maximum_primary_used_percent": 100,
        "cleared_for_semantic_work": True,
        "thread_started": False,
        "turn_started": False,
        "privacy": "managed_auth_plan_limit_percent_reset_and_status_no_credentials",
    }


async def _cleared_probe(**kwargs):
    assert kwargs["maximum_primary_used_percent"] == 100
    return _snapshot(used=60)


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
                        "rationale": "Synthetic exact support for transport testing.",
                    }
                    for witness in witnesses
                ],
                "equivalence_groups": [
                    {
                        "witness_ids": [witness["witness_id"]],
                        "rationale": "Synthetic singleton partition.",
                    }
                    for witness in witnesses
                ],
                "alignments": [],
                "unaligned_left_witness_ids": [
                    witness["witness_id"] for witness in case["event_set_a"]
                ],
                "unaligned_right_witness_ids": [
                    witness["witness_id"] for witness in case["event_set_b"]
                ],
            }
        )
    output = {"cases": cases}
    assert judge.validate_judge_output(output, variant) == []
    return output


class _FakeReserveClient:
    def __init__(
        self,
        policy_path: Path,
        *,
        duplicate_ids: bool = False,
        fail_after_capacity: bool = False,
    ) -> None:
        self.policy_path = policy_path.resolve()
        self.policy = v6.reserve.load_reserve_capacity_policy(self.policy_path)
        self.duplicate_ids = duplicate_ids
        self.fail_after_capacity = fail_after_capacity
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def _capacity(self, path: Path, turn_name: str) -> None:
        ordinal = v6.TURN_NAMES.index(turn_name)
        evaluated = v6.reserve.evaluate_reserve_capacity(
            {
                "primary_used_percent": 60 + ordinal,
                "rate_limit_reached_type": None,
            },
            policy=self.policy,
            remaining_turn_count=len(v6.TURN_NAMES) - ordinal,
        )
        payload = {
            "schema_version": v6.reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": f"2026-07-18T21:00:0{ordinal}+00:00",
            "policy_path": str(self.policy_path),
            "policy_sha256": v6._sha256_file(self.policy_path),  # noqa: SLF001
            "phase_id": v6.PHASE_ID,
            "turn_name": turn_name,
            "turn_ordinal": ordinal,
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
            "remaining_turn_count": evaluated["remaining_turn_count"],
            "projected_remaining_tokens": evaluated["projected_remaining_tokens"],
            "quota_points_per_million_tokens": evaluated[
                "quota_points_per_million_tokens"
            ],
            "projected_remaining_quota_points": evaluated[
                "projected_remaining_quota_points"
            ],
            "projected_terminal_remaining_percent": evaluated[
                "projected_terminal_remaining_percent"
            ],
            "cleared_for_semantic_turn": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": "capacity_status_policy_hash_and_counts_no_prompt_output_email_credentials_or_thread_ids",
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    async def run_ephemeral_structured_turn(self, **kwargs):
        call_index = len(self.calls)
        capacity_path = Path(kwargs["capacity_checkpoint_path"]).resolve()
        turn_root = capacity_path.parent
        turn_name = next(
            name for name in v6.TURN_NAMES if name.replace("_", "-") == turn_root.name
        )
        self._capacity(capacity_path, turn_name)
        self.calls.append(copy.deepcopy(kwargs))
        if self.fail_after_capacity:
            raise RuntimeError("synthetic transport loss after capacity checkpoint")
        variant = json.loads((turn_root / "input.private.json").read_text(encoding="utf-8"))
        output = _valid_output(variant)
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(kwargs["output_path"]).resolve()
        sidecar_path = Path(kwargs["sidecar_path"]).resolve()
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_text = json.dumps(
            kwargs["output_schema"],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        identity = 0 if self.duplicate_ids else call_index
        sidecar = {
            "schema_version": v6.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "cli_version": v6.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "client_version": v6.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_sha256": v6.codex_app_server.PROTOCOL_SCHEMA_SHA256,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "batch_size": kwargs["batch_size"],
            "thread_id": f"thread-{identity}",
            "turn_id": f"turn-{identity}",
            "output_path": str(output_path),
            "prompt_sha256": v6._sha256_text(kwargs["prompt"]),  # noqa: SLF001
            "base_instructions_sha256": v6._sha256_text(  # noqa: SLF001
                kwargs["base_instructions"]
            ),
            "output_schema_sha256": v6._sha256_text(schema_text),  # noqa: SLF001
            "output_sha256": v6._sha256_text(output_text),  # noqa: SLF001
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_bytes": len(
                kwargs["base_instructions"].encode("utf-8")
            ),
            "output_schema_bytes": len(schema_text.encode("utf-8")),
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "finished_at": f"2026-07-18T21:00:0{call_index}+00:00",
        }
        sidecar_path.write_text(
            json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
            usage=SimpleNamespace(total_tokens=120),
        )


def test_repository_contract_binds_verified_zero_turn_epoch5_and_audited_phase_bound():
    contract = v6.load_contract()
    predecessor = v6._verify_epoch5_preturn()  # noqa: SLF001

    assert contract["plan"]["plan_epoch"] == 6
    assert predecessor["terminal"]["state"] == "waiting"
    assert predecessor["terminal"]["semantic_model_call_count"] == 0
    assert set(contract["frozen_records"]) == v6.EXPECTED_FROZEN_INPUT_ROLES
    assert contract["directive"]["witness_contract"] == v6.epoch5.EXPECTED_WITNESS_CONTRACT
    assert contract["directive"]["acceptance_contract"] == v6.epoch5.EXPECTED_ACCEPTANCE_CONTRACT
    capacity = contract["directive"]["capacity_contract"]
    assert capacity["used_percent_threshold_allowed"] is False
    assert capacity["phase_total_token_bound"] == 306_000
    assert capacity["projected_phase_quota_points"] == 6


def test_prepare_freezes_fresh_canonical_reserve_root_without_mutating_epoch5(
    tmp_path: Path,
):
    before = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in (
            v6.EPOCH5_ROOT / "runtime-lock.json",
            v6.EPOCH5_ROOT / "launch-receipt.json",
            v6.EPOCH5_ROOT / "terminal.json",
            v6.EPOCH5_ROOT / "plan-step-receipt.json",
        )
    }
    root = _freeze(tmp_path)
    lock = v6.verify_runtime_lock(root / "runtime-lock.json")
    policy = v6.reserve.load_reserve_capacity_policy(root / "capacity-policy.json")

    assert policy["ordered_turn_names"] == list(v6.TURN_NAMES)
    assert policy["semantic_output_root"] == str(root)
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["phase_total_token_bound"] == 306_000
    assert policy["projected_phase_quota_points"] == 6
    assert lock["predecessor_epoch5_immutable"] is True
    assert v6._semantic_artifacts(root) == []  # noqa: SLF001
    for turn_name in v6.TURN_NAMES[:2]:
        paths = v6._turn_paths(root, turn_name)  # noqa: SLF001
        assert paths["root"] == root / "turns" / turn_name.replace("_", "-")
        assert not paths["capacity"].exists()
    after = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in before
    }
    assert after == before


def test_no_turn_preflight_uses_remaining_reserve_instead_of_used_threshold(
    tmp_path: Path,
):
    root = _freeze(tmp_path)
    calls = []

    async def probe(**kwargs):
        calls.append(kwargs)
        return _snapshot(used=60)

    receipt = _run(v6.preflight(output_dir=root, rate_limit_probe=probe))

    assert len(calls) == 1
    assert calls[0]["maximum_primary_used_percent"] == 100
    assert receipt["live_capacity"]["primary_used_percent"] == 60
    assert receipt["reserve_evaluation"]["projected_remaining_quota_points"] == 6
    assert receipt["reserve_evaluation"]["projected_terminal_remaining_percent"] == 34
    assert receipt["semantic_turn_count"] == 0
    assert v6._semantic_artifacts(root) == []  # noqa: SLF001


def test_no_turn_preflight_fails_closed_when_phase_would_breach_reserve(
    tmp_path: Path,
):
    root = _freeze(tmp_path)

    async def probe(**_kwargs):
        return _snapshot(used=75)

    with pytest.raises(
        v6.OperationalWaitingV6Error,
        match="does not fit the audited epoch-6 phase bound",
    ):
        _run(v6.preflight(output_dir=root, rate_limit_probe=probe))
    assert not (root / "capacity-preflight.json").exists()
    assert v6._semantic_artifacts(root) == []  # noqa: SLF001


def test_complete_two_turn_attempt_has_strict_unique_accounting(
    tmp_path: Path,
):
    root = _freeze(tmp_path)
    _run(v6.preflight(output_dir=root, rate_limit_probe=_cleared_probe))
    fake = _FakeReserveClient(root / "capacity-policy.json")

    receipt = _run(v6.run(output_dir=root, client_factory=lambda _policy: fake))

    assert receipt["state"] in {"passed", "rejected"}
    assert receipt["semantic_model_call_count"] == 2
    assert receipt["semantic_retry_count"] == 0
    assert receipt["accounting_complete"] is True
    assert receipt["usage"]["total_tokens"] == 240
    assert receipt["strict_thread_turn_call_accounting"] is True
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert len(fake.calls) == 2
    for call, turn_name in zip(fake.calls, v6.TURN_NAMES[:2]):
        assert Path(call["capacity_checkpoint_path"]) == v6._turn_paths(  # noqa: SLF001
            root, turn_name
        )["capacity"]
    assert v6.verify_receipt(root)["state"] == receipt["state"]


def test_duplicate_thread_and_turn_identity_fails_closed_without_quality(
    tmp_path: Path,
):
    root = _freeze(tmp_path)
    _run(v6.preflight(output_dir=root, rate_limit_probe=_cleared_probe))
    fake = _FakeReserveClient(root / "capacity-policy.json", duplicate_ids=True)

    receipt = _run(v6.run(output_dir=root, client_factory=lambda _policy: fake))

    assert receipt["state"] == "waiting"
    assert receipt["development_quality_passed"] is False
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert receipt["error_class"] == v6.ExpandedCapDirectReferenceV6Error.__name__
    assert v6.verify_receipt(root)["state"] == "waiting"


def test_partial_turn_capacity_checkpoint_is_never_retried(
    tmp_path: Path,
):
    root = _freeze(tmp_path)
    _run(v6.preflight(output_dir=root, rate_limit_probe=_cleared_probe))
    fake = _FakeReserveClient(
        root / "capacity-policy.json", fail_after_capacity=True
    )

    first = _run(v6.run(output_dir=root, client_factory=lambda _policy: fake))
    assert first["state"] == "waiting"
    assert first["usage_status"] == "unknown"
    assert first["accounting_complete"] is False
    assert len(fake.calls) == 1

    called = False

    def must_not_create_client(_policy):
        nonlocal called
        called = True
        raise AssertionError("zero-retry terminal attempted another client")

    second = _run(v6.run(output_dir=root, client_factory=must_not_create_client))
    assert second == first
    assert called is False
    assert len(fake.calls) == 1
