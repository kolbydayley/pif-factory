from __future__ import annotations

import asyncio
import copy
import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_single_message_direct_reference as direct
from research_factory import app_server_llm_judge as judge


def _run_async(coro):
    try:
        prior_loop = asyncio.get_event_loop()
    except RuntimeError:
        prior_loop = asyncio.new_event_loop()
    try:
        return asyncio.run(coro)
    finally:
        asyncio.set_event_loop(prior_loop)


def _frozen_tmp_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    contract = direct.load_contract()
    root = tmp_path / "epoch-20"
    test_contract = copy.deepcopy(contract)
    test_contract["receipt_path"] = root / "plan-step-receipt.json"
    monkeypatch.setattr(direct, "load_contract", lambda: copy.deepcopy(test_contract))
    direct.freeze_run(root)
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
                        "rationale": "Synthetic exact support for transport testing.",
                    }
                    for witness in witnesses
                ],
                "equivalence_groups": [
                    {
                        "witness_ids": [witness["witness_id"]],
                        "rationale": "Synthetic singleton full-event partition.",
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


class _CompletedFixtureClient:
    def __init__(self, policy_path: Path) -> None:
        self.policy_path = policy_path.resolve()
        self.policy = direct.reserve.load_reserve_capacity_policy(self.policy_path)
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def _write_capacity(self, path: Path, ordinal: int) -> None:
        evaluated = direct.reserve.evaluate_reserve_capacity(
            {"primary_used_percent": 10 + ordinal, "rate_limit_reached_type": None},
            policy=self.policy,
            remaining_turn_count=len(direct.CAPACITY_TURN_NAMES) - ordinal,
        )
        payload = {
            "schema_version": direct.reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": f"2026-07-19T22:00:0{ordinal}+00:00",
            "policy_path": str(self.policy_path),
            "policy_sha256": direct._sha256_file(self.policy_path),  # noqa: SLF001
            "phase_id": direct.STEP_ID,
            "turn_name": direct.CAPACITY_TURN_NAMES[ordinal],
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
            "privacy": (
                "capacity_status_policy_hash_and_counts_no_prompt_output_email_"
                "credentials_or_thread_ids"
            ),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    async def run_ephemeral_structured_turn(self, **kwargs):
        call_index = len(self.calls)
        prompt_packets = json.loads(kwargs["prompt"].split("# Blinded cases\n", 1)[1])
        variant = {
            "schema_version": judge.JUDGE_VARIANT_VERSION,
            "variant": "opaque",
            "orientation": "opaque",
            "cases": prompt_packets["cases"],
        }
        output = _valid_output(variant)
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(kwargs["output_path"]).resolve()
        sidecar_path = Path(kwargs["sidecar_path"]).resolve()
        capacity_path = Path(kwargs["capacity_checkpoint_path"]).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_capacity(capacity_path, call_index)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_text = direct._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        sidecar = {
            "schema_version": direct.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "cli_version": direct.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "client_version": direct.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_sha256": direct.codex_app_server.PROTOCOL_SCHEMA_SHA256,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "batch_size": kwargs["batch_size"],
            "thread_id": f"fixture-thread-{call_index}",
            "turn_id": f"fixture-turn-{call_index}",
            "app_server_user_agent": "fixture-codex-app-server",
            "output_path": str(output_path),
            "prompt_sha256": direct._sha256_text(kwargs["prompt"]),  # noqa: SLF001
            "base_instructions_sha256": direct._sha256_text(  # noqa: SLF001
                kwargs["base_instructions"]
            ),
            "output_schema_sha256": direct._sha256_text(schema_text),  # noqa: SLF001
            "output_sha256": direct._sha256_text(output_text),  # noqa: SLF001
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_bytes": len(
                kwargs["base_instructions"].encode("utf-8")
            ),
            "output_schema_bytes": len(schema_text.encode("utf-8")),
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "finished_at": f"2026-07-19T22:00:0{call_index}+00:00",
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


def test_contract_binds_epoch19_cost_and_full_151_witness_lineage() -> None:
    contract = direct.load_contract()
    pool, mapping, lineage = direct.build_complete_container(contract)

    assert contract["extraction_accounting"] == {
        "semantic_model_call_count": 1,
        "usage_status": "measured",
        "accounting_complete": True,
        "usage": direct.EXPECTED_EXTRACTION_USAGE,
    }
    assert contract["production_token_ratio"] == direct.EXPECTED_PRODUCTION_TOKEN_RATIO
    assert len(lineage) == direct.EXPECTED_CANDIDATE_WITNESSES == 31
    assert len({(row["segment_id"], row["event_index"]) for row in lineage}) == 31
    assert [row["case_provenance"]["segment_id"] for row in mapping["cases"]] == list(
        direct.SOURCE_SEGMENT_ORDER
    )
    observed = Counter()
    for case in mapping["cases"]:
        for witness in case["witnesses"]:
            provenance = witness["provenance"]
            observed[(provenance["segment_id"], provenance["system_id"])] += 1
    assert observed == Counter(direct.EXPECTED_WITNESS_COUNTS)
    assert sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    ) == direct.EXPECTED_TOTAL_WITNESSES == 151


def test_ab_ba_are_exact_opaque_inversions_with_identical_case_order() -> None:
    pool, _mapping, _lineage = direct.build_complete_container(direct.load_contract())
    variants = judge.build_judge_variants(pool)
    ab_cases = variants["ab"]["cases"]
    ba_cases = variants["ba"]["cases"]

    assert [case["case_id"] for case in ab_cases] == [
        case["case_id"] for case in ba_cases
    ]
    for ab, ba in zip(ab_cases, ba_cases):
        assert ab["event_set_a"] == ba["event_set_b"]
        assert ab["event_set_b"] == ba["event_set_a"]
    public_pool = json.dumps(pool, ensure_ascii=True, sort_keys=True)
    assert direct.SYSTEM_BASELINE not in public_pool
    assert direct.SYSTEM_CANDIDATE not in public_pool


def test_freeze_is_zero_call_and_binds_transport_reserve_and_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    lock = direct.verify_runtime_lock(root / "runtime-lock.json")
    spec = json.loads((root / "attempt-spec.json").read_text(encoding="utf-8"))
    policy = json.loads((root / "capacity-policy.json").read_text(encoding="utf-8"))

    assert lock["plan_epoch"] == 20
    assert lock["semantic_model_call_cap"] == 3
    assert lock["judge_transport"] == direct.JUDGE_TRANSPORT
    assert spec["all_event_witness_count"] == 151
    assert spec["semantic_prefilter_applied"] is False
    assert spec["deterministic_semantic_pruning_applied"] is False
    assert policy["ordered_turn_names"] == list(direct.CAPACITY_TURN_NAMES)
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["projected_phase_quota_points"] == 8
    assert not (root / "launch-receipt.json").exists()
    assert not (root / "judge").exists()
    assert not (root / "plan-step-receipt.json").exists()


def test_frozen_artifact_tamper_is_rejected(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"state":"passed"}\n', encoding="utf-8")
    record = direct._record(artifact)  # noqa: SLF001
    artifact.write_text('{"state":"changed"}\n', encoding="utf-8")

    with pytest.raises(
        direct.ExpandedCapDirectReferenceError, match="checksum or size drifted"
    ):
        direct._validated_record(record, "synthetic artifact")  # noqa: SLF001


def test_same_count_witness_event_substitution_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = judge.make_shared_witness_pool

    def substituted(raw_cases, *, seed):
        pool, mapping = original(raw_cases, seed=seed)
        event = mapping["cases"][0]["witnesses"][0]["provenance"]["original_event"]
        event["claim_text"] = f"{event['claim_text']} substituted"
        return pool, mapping

    monkeypatch.setattr(judge, "make_shared_witness_pool", substituted)
    with pytest.raises(
        direct.ExpandedCapDirectReferenceError,
        match="raw/witness event-hash multiset drifted",
    ):
        direct.build_complete_container(direct.load_contract())


def test_partial_attempt_terminalizes_waiting_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    sidecar = root / "judge/sidecars/ab.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text('{"state":"started"}\n', encoding="utf-8")
    called = False

    async def must_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("partial recovery replayed the judge")

    receipt = _run_async(direct.run(output_dir=root, judge_runner=must_not_run))

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch20_partial_or_interrupted_attempt_preserved_without_replay"
    )
    assert receipt["usage_status"] == "unknown"
    assert receipt["accounting_complete"] is False
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert direct.verify_receipt(root)["state"] == "waiting"


def test_completed_ab_ba_fixture_has_exact_capacity_and_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    fake = _CompletedFixtureClient(root / "capacity-policy.json")

    receipt = _run_async(direct.run(output_dir=root, client_factory=lambda: fake))

    assert receipt["state"] in {"passed", "rejected"}
    assert receipt["semantic_model_call_count"] == 2
    assert receipt["semantic_retry_count"] == 0
    assert receipt["usage"]["total_tokens"] == 240
    assert receipt["accounting_complete"] is True
    assert len(fake.calls) == 2
    for ordinal, name in enumerate(("ab", "ba")):
        checkpoint = root / f"judge/sidecars/{name}.capacity.json"
        capacity = direct._validate_capacity_checkpoint(checkpoint)  # noqa: SLF001
        assert capacity["turn_ordinal"] == ordinal
        assert capacity["turn_name"] == direct.CAPACITY_TURN_NAMES[ordinal]
    assert direct.verify_receipt(root)["state"] == receipt["state"]

    capacity_path = root / "judge/sidecars/ab.capacity.json"
    capacity = json.loads(capacity_path.read_text(encoding="utf-8"))
    capacity["managed_chatgpt_auth_verified"] = False
    capacity_path.write_text(json.dumps(capacity, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(direct.ExpandedCapDirectReferenceError):
        direct.verify_receipt(root)
