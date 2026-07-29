from __future__ import annotations

import asyncio
import copy
import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_dual_pass_omission_audit_quality_runtime as quality
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
    contract = quality.load_contract()
    root = tmp_path / "epoch38"
    changed = copy.deepcopy(contract)
    changed["receipt_path"] = root / "plan-step-receipt.json"
    monkeypatch.setattr(quality, "load_contract", lambda: copy.deepcopy(changed))
    quality.freeze_run(root)
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


class _CompletedClient:
    def __init__(self, policy_path: Path) -> None:
        self.policy_path = policy_path.resolve()
        self.policy = quality.reserve.load_reserve_capacity_policy(self.policy_path)
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def _write_capacity(self, path: Path, ordinal: int) -> None:
        evaluation = quality.reserve.evaluate_reserve_capacity(
            {"primary_used_percent": 10 + ordinal, "rate_limit_reached_type": None},
            policy=self.policy,
            remaining_turn_count=len(quality.CAPACITY_TURN_NAMES) - ordinal,
        )
        value = {
            "schema_version": quality.reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": f"2026-07-19T23:00:0{ordinal}+00:00",
            "policy_path": str(self.policy_path),
            "policy_sha256": quality._sha256_file(self.policy_path),  # noqa: SLF001
            "phase_id": quality.STEP_ID,
            "turn_name": quality.CAPACITY_TURN_NAMES[ordinal],
            "turn_ordinal": ordinal,
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "primary_used_percent": evaluation["primary_used_percent"],
            "primary_remaining_percent": evaluation["primary_remaining_percent"],
            "primary_resets_at": 1_800_000_000,
            "rate_limit_reached_type": None,
            "minimum_remaining_reserve_percent": evaluation[
                "minimum_remaining_reserve_percent"
            ],
            "usable_percent_above_reserve": evaluation["usable_percent_above_reserve"],
            "remaining_turn_count": evaluation["remaining_turn_count"],
            "projected_remaining_tokens": evaluation["projected_remaining_tokens"],
            "quota_points_per_million_tokens": evaluation[
                "quota_points_per_million_tokens"
            ],
            "projected_remaining_quota_points": evaluation[
                "projected_remaining_quota_points"
            ],
            "projected_terminal_remaining_percent": evaluation[
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
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")

    async def run_ephemeral_structured_turn(self, **kwargs):
        ordinal = len(self.calls)
        packets = json.loads(kwargs["prompt"].split("# Blinded cases\n", 1)[1])
        variant = {
            "schema_version": judge.JUDGE_VARIANT_VERSION,
            "variant": "opaque",
            "orientation": "opaque",
            "cases": packets["cases"],
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
        self._write_capacity(capacity_path, ordinal)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_text = quality._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        sidecar = {
            "schema_version": quality.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "cli_version": quality.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "client_version": quality.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_sha256": quality.codex_app_server.PROTOCOL_SCHEMA_SHA256,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "batch_size": kwargs["batch_size"],
            "instruction_sources_count": quality.EXPECTED_INSTRUCTION_SOURCES_COUNT,
            "instruction_sources_sha256": quality.EXPECTED_INSTRUCTION_SOURCES_SHA256,
            "thread_id": f"fixture-thread-{ordinal}",
            "turn_id": f"fixture-turn-{ordinal}",
            "app_server_user_agent": "fixture-codex-app-server",
            "output_path": str(output_path),
            "prompt_sha256": quality._sha256_text(kwargs["prompt"]),  # noqa: SLF001
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_sha256": quality._sha256_text(  # noqa: SLF001
                kwargs["base_instructions"]
            ),
            "base_instructions_bytes": len(kwargs["base_instructions"].encode("utf-8")),
            "output_schema_sha256": quality._sha256_text(schema_text),  # noqa: SLF001
            "output_schema_bytes": len(schema_text.encode("utf-8")),
            "output_sha256": quality._sha256_text(output_text),  # noqa: SLF001
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "wall_elapsed_seconds": 1.25,
            "finished_at": f"2026-07-19T23:00:0{ordinal}+00:00",
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


def _started_sidecar(root: Path, name: str) -> dict:
    pool = json.loads((root / "shared-witness-pool.private.json").read_text(encoding="utf-8"))
    prompt, schema, base = quality._turn_material(root, name, pool)  # noqa: SLF001
    schema_text = quality._canonical_json(schema)  # noqa: SLF001
    return {
        "schema_version": quality.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "state": "started",
        "status": "in_progress",
        "cli_version": quality.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "client_version": quality.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "protocol_schema_sha256": quality.codex_app_server.PROTOCOL_SCHEMA_SHA256,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "synthetic_debug_errors": False,
        "recovery_reran_model": False,
        "model": quality.MODEL,
        "effort": quality.EFFORT,
        "batch_size": len(pool["cases"]),
        "instruction_sources_count": quality.EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": quality.EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "thread_id": "fixture-partial-thread",
        "turn_id": "fixture-partial-turn",
        "output_path": str((root / f"judge/output-{name}.private.json").resolve()),
        "prompt_sha256": quality._sha256_text(prompt),  # noqa: SLF001
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_instructions_sha256": quality._sha256_text(base),  # noqa: SLF001
        "base_instructions_bytes": len(base.encode("utf-8")),
        "output_schema_sha256": quality._sha256_text(schema_text),  # noqa: SLF001
        "output_schema_bytes": len(schema_text.encode("utf-8")),
    }


def test_contract_builds_exact_43_plus_30_full_event_pool() -> None:
    contract = quality.load_contract()
    pool, mapping, lineage = quality.build_complete_container(contract)

    observed = Counter()
    for case in mapping["cases"]:
        for witness in case["witnesses"]:
            provenance = witness["provenance"]
            observed[(provenance["segment_id"], provenance["system_id"])] += 1

    assert observed == Counter(quality.EXPECTED_WITNESS_COUNTS)
    assert len(lineage) == quality.EXPECTED_CANDIDATE_WITNESSES == 30
    assert sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    ) == quality.EXPECTED_TOTAL_WITNESSES == 73
    assert quality.SYSTEM_BASELINE not in json.dumps(pool, sort_keys=True)
    assert quality.SYSTEM_CANDIDATE not in json.dumps(pool, sort_keys=True)


def test_ab_ba_are_exact_opaque_inversions_with_identical_case_order() -> None:
    pool, _mapping, _lineage = quality.build_complete_container(quality.load_contract())
    variants = judge.build_judge_variants(pool)
    assert [row["case_id"] for row in variants["ab"]["cases"]] == [
        row["case_id"] for row in variants["ba"]["cases"]
    ]
    for ab_case, ba_case in zip(variants["ab"]["cases"], variants["ba"]["cases"]):
        assert ab_case["event_set_a"] == ba_case["event_set_b"]
        assert ab_case["event_set_b"] == ba_case["event_set_a"]


def test_freeze_is_zero_call_and_binds_reserve_transport_and_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    lock = quality.verify_runtime_lock(root / "runtime-lock.json")
    policy = json.loads((root / "capacity-policy.json").read_text(encoding="utf-8"))
    spec = json.loads((root / "attempt-spec.json").read_text(encoding="utf-8"))

    assert lock["plan_epoch"] == 38
    assert lock["semantic_model_call_cap"] == 2
    assert lock["transport"] == quality.TRANSPORT
    assert spec["all_event_witness_count"] == 73
    assert spec["semantic_prefilter_applied"] is False
    assert spec["deterministic_semantic_pruning_applied"] is False
    assert policy["ordered_turn_names"] == list(quality.CAPACITY_TURN_NAMES)
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["projected_phase_quota_points"] == 4
    assert not (root / "launch-receipt.json").exists()
    assert not (root / "judge").exists()
    assert not (root / "plan-step-receipt.json").exists()


def test_same_count_candidate_substitution_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = judge.make_shared_witness_pool

    def substituted(raw_cases, *, seed):
        pool, mapping = original(raw_cases, seed=seed)
        event = mapping["cases"][0]["witnesses"][-1]["provenance"]["original_event"]
        event["claim_text"] = f"{event['claim_text']} substituted"
        return pool, mapping

    monkeypatch.setattr(judge, "make_shared_witness_pool", substituted)
    with pytest.raises(
        quality.Epoch38QualityError,
        match="lineage hash drifted|event multiset drifted",
    ):
        quality.build_complete_container(quality.load_contract())


def test_partial_ab_attempt_terminalizes_waiting_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    quality._launch_receipt(root)  # noqa: SLF001
    sidecar_path = root / "judge/sidecars/ab.json"
    sidecar_path.parent.mkdir(parents=True)
    sidecar_path.write_text(
        json.dumps(_started_sidecar(root, "ab"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    called = False

    async def must_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("partial AB attempt was replayed")

    receipt = _run(
        quality.run(root, judge_runner=must_not_run, client_factory=lambda: object())
    )

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch38_partial_or_interrupted_ab_ba_preserved_without_replay"
    )
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["unknown_usage_turn_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert quality.verify_receipt(root)["state"] == "waiting"


def test_complete_ab_ba_fixture_terminalizes_with_exact_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root / "capacity-policy.json")

    receipt = _run(quality.run(root, client_factory=lambda: fake))

    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 2
    assert receipt["measured_model_call_count"] == 2
    assert receipt["unknown_usage_turn_count"] == 0
    assert receipt["usage"]["total_tokens"] == 240
    assert receipt["accounting_complete"] is True
    assert len(fake.calls) == 2
    assert quality.verify_receipt(root)["state"] == "rejected"


def test_complete_report_crash_finalizes_without_new_judge_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root / "capacity-policy.json")
    quality._launch_receipt(root)  # noqa: SLF001
    _run(
        judge.run_app_server_semantic_judge(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=quality.MODEL,
            reasoning_effort=quality.EFFORT,
            timeout_seconds=quality.TIMEOUT_SECONDS,
            client_factory=lambda: fake,
        )
    )
    called = False

    async def must_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("complete AB/BA bundle was replayed")

    receipt = _run(
        quality.run(root, judge_runner=must_not_run, client_factory=lambda: object())
    )

    assert called is False
    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 2
    assert quality.verify_receipt(root)["state"] == "rejected"


def test_single_valid_terminal_mirror_repairs_without_model_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root / "capacity-policy.json")
    receipt = _run(quality.run(root, client_factory=lambda: fake))
    (root / "terminal.json").unlink()
    called = False

    async def must_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("terminal mirror recovery replayed a model")

    recovered = _run(
        quality.run(root, judge_runner=must_not_run, client_factory=lambda: object())
    )
    assert called is False
    assert recovered == receipt
    assert (root / "terminal.json").is_file()


def test_live_default_root_rejects_injected_client_or_runner() -> None:
    async def fake_runner(**_kwargs):
        return {}

    with pytest.raises(quality.Epoch38QualityError, match="forbids injected"):
        _run(quality.run(quality.DEFAULT_ROOT, judge_runner=fake_runner))
    with pytest.raises(quality.Epoch38QualityError, match="forbids injected"):
        _run(quality.run(quality.DEFAULT_ROOT, client_factory=lambda: object()))


def test_external_api_key_or_session_auth_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "forbidden-fixture-value")
    monkeypatch.setenv("CHATGPT_SESSION_TOKEN", "forbidden-fixture-value")

    with pytest.raises(
        quality.Epoch38QualityError,
        match="managed ChatGPT execution rejects API-key/raw-session auth",
    ):
        quality._reject_external_auth_material()  # noqa: SLF001


def test_cli_exposes_all_bounded_phases() -> None:
    for command in ("freeze", "verify-runtime", "run", "verify-receipt", "status"):
        assert quality._parser().parse_args([command]).command == command  # noqa: SLF001
