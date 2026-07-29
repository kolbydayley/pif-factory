from __future__ import annotations

import asyncio
import copy
import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_candidate_expanded_cap_direct_reference as direct
from research_factory import app_server_llm_judge as judge


def _frozen_tmp_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    contract = direct.load_contract()
    root = tmp_path / "epoch-5"
    test_contract = copy.deepcopy(contract)
    test_contract["receipt_path"] = root / "plan-step-receipt.json"
    monkeypatch.setattr(direct, "load_contract", lambda: copy.deepcopy(test_contract))
    direct.freeze_run(root)
    return root


def _drifted_value(value):
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, float):
        return value + 0.01
    if isinstance(value, str):
        return f"{value}_drift"
    if isinstance(value, list):
        return list(reversed(value))
    raise AssertionError(f"unsupported synthetic drift value: {value!r}")


def _run_async(coro):
    try:
        prior_loop = asyncio.get_event_loop()
    except RuntimeError:
        prior_loop = asyncio.new_event_loop()
    try:
        return asyncio.run(coro)
    finally:
        asyncio.set_event_loop(prior_loop)


def _valid_output(variant: dict, *, disagree_support: bool = False) -> dict:
    cases = []
    for case_index, case in enumerate(variant["cases"]):
        witnesses = case["event_set_a"] + case["event_set_b"]
        support = []
        for witness_index, witness in enumerate(witnesses):
            unsupported = disagree_support and case_index == 0 and witness_index == 0
            support.append(
                {
                    "witness_id": witness["witness_id"],
                    "verdict": "unsupported" if unsupported else "supported",
                    "evidence_spans": [] if unsupported else [witness["event"]["evidence"]],
                    "rationale": "Synthetic full-event support decision.",
                }
            )
        cases.append(
            {
                "case_id": case["case_id"],
                "support_results": support,
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


def _capacity_payload() -> dict:
    return {
        "schema_version": "pif_app_server_capacity_checkpoint_v1",
        "checked_at": "2026-07-18T20:00:00+00:00",
        "limit_id": "codex",
        "primary_used_percent": 10,
        "primary_resets_at": 1_800_000_000,
        "rate_limit_reached_type": None,
        "maximum_primary_used_percent": 20,
        "cleared_for_semantic_turn": True,
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
        "retry_checkpoint_reuse_allowed": False,
        "privacy": "capacity_status_only_no_prompt_output_email_credentials_or_thread_ids",
    }


class _StrictFakeClient:
    def __init__(self, *, disagree_on_second: bool = False) -> None:
        self.calls: list[dict] = []
        self.disagree_on_second = disagree_on_second

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        call_index = len(self.calls)
        marker = "# Blinded cases\n"
        packets = json.loads(kwargs["prompt"].split(marker, 1)[1])
        variant = {
            "schema_version": judge.JUDGE_VARIANT_VERSION,
            "variant": "opaque",
            "orientation": "opaque",
            "cases": packets["cases"],
        }
        output = _valid_output(
            variant,
            disagree_support=self.disagree_on_second and call_index == 1,
        )
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(kwargs["output_path"]).resolve()
        sidecar_path = Path(kwargs["sidecar_path"]).resolve()
        capacity_path = Path(kwargs["capacity_checkpoint_path"]).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        capacity_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        schema_text = json.dumps(
            kwargs["output_schema"],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        sidecar = {
            "schema_version": "pif_codex_app_server_turn_v2",
            "state": "completed",
            "status": "completed",
            "cli_version": "0.144.1",
            "client_version": "pif-codex-app-server-v4",
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
            "thread_id": f"thread-{id(self)}-{call_index}",
            "turn_id": f"turn-{id(self)}-{call_index}",
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
            "finished_at": f"2026-07-18T20:00:0{call_index}+00:00",
        }
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
        capacity_path.write_text(
            json.dumps(_capacity_payload(), sort_keys=True) + "\n", encoding="utf-8"
        )
        self.calls.append(copy.deepcopy(kwargs))
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
        )


def _seed_complete_base(
    root: Path, *, disagree: bool = False
) -> _StrictFakeClient:
    fake = _StrictFakeClient(disagree_on_second=disagree)
    _run_async(
        judge.run_app_server_semantic_judge(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=direct.MODEL,
            reasoning_effort=direct.EFFORT,
            timeout_seconds=direct.JUDGE_TIMEOUT_SECONDS,
            client_factory=lambda: fake,
        )
    )
    assert len(fake.calls) == 2
    return fake


def test_exact_27_plus_40_witness_multiset_and_lineage() -> None:
    contract = direct.load_contract()
    pool, mapping, lineage = direct.build_complete_container(contract)

    assert len(lineage) == 40
    assert len({(row["segment_id"], row["event_index"]) for row in lineage}) == 40
    assert [row["case_provenance"]["segment_id"] for row in mapping["cases"]] == list(
        direct.SOURCE_SEGMENT_ORDER
    )
    observed = Counter()
    for case in mapping["cases"]:
        for witness in case["witnesses"]:
            provenance = witness["provenance"]
            observed[(provenance["segment_id"], provenance["system_id"])] += 1
    assert observed == Counter(
        {key: count for key, count in direct.EXPECTED_WITNESS_COUNTS.items() if count}
    )
    assert sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    ) == 67


def test_one_call_canary_accounting_and_production_formula_are_bound() -> None:
    contract = direct.load_contract()

    assert contract["extraction_accounting"] == {
        "semantic_model_call_count": 1,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": direct.EXPECTED_EXTRACTION_USAGE,
    }
    assert (
        direct.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + direct.EXPECTED_EXTRACTION_USAGE["total_tokens"] * direct.PRODUCTION_SCALE
    ) == direct.EXPECTED_PRODUCTION_TOTAL_TOKENS
    assert contract["production_token_ratio"] == 0.15692
    assert contract["directive"]["execution_contract"]["judge_total_token_cap"] == 400_000
    assert (
        contract["directive"]["execution_contract"]["production_cost_projection"]
        ["judge_tokens_included_in_production_formula"]
        is False
    )
    assert contract["directive"]["witness_contract"] == direct.EXPECTED_WITNESS_CONTRACT
    assert contract["directive"]["acceptance_contract"] == direct.EXPECTED_ACCEPTANCE_CONTRACT
    assert contract["directive"]["terminal_contract"] == direct.EXPECTED_TERMINAL_CONTRACT


def test_managed_transport_is_bound_in_attempt_spec_and_runtime_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    attempt_spec = json.loads((root / "attempt-spec.json").read_text(encoding="utf-8"))
    runtime_lock = direct.verify_runtime_lock(root / "runtime-lock.json")

    for artifact in (attempt_spec, runtime_lock):
        assert artifact["judge_transport"] == direct.JUDGE_TRANSPORT
        assert artifact["managed_chatgpt_auth_required"] is True
        assert artifact["official_persistent_app_server_required"] is True


def test_loader_rejects_each_witness_acceptance_terminal_and_transport_drift(
    tmp_path: Path,
) -> None:
    source_directive = json.loads(direct.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    source_plan = json.loads(direct.PLAN_PATH.read_text(encoding="utf-8"))
    fields = {
        "execution_contract": {"judge_transport": direct.JUDGE_TRANSPORT},
        "witness_contract": direct.EXPECTED_WITNESS_CONTRACT,
        "acceptance_contract": direct.EXPECTED_ACCEPTANCE_CONTRACT,
        "terminal_contract": direct.EXPECTED_TERMINAL_CONTRACT,
    }

    for section, expected in fields.items():
        for key, value in expected.items():
            case_root = tmp_path / section / key
            directive_path = case_root / "directive.json"
            plan_path = case_root / "plan.json"
            directive_path.parent.mkdir(parents=True, exist_ok=True)
            mutated_directive = copy.deepcopy(source_directive)
            mutated_directive[section][key] = _drifted_value(value)
            directive_path.write_text(
                json.dumps(mutated_directive, sort_keys=True) + "\n", encoding="utf-8"
            )
            mutated_plan = copy.deepcopy(source_plan)
            mutated_plan["step"]["directive_path"] = str(directive_path.resolve())
            mutated_plan["step"]["directive_sha256"] = direct._sha256_file(  # noqa: SLF001
                directive_path
            )
            plan_path.write_text(
                json.dumps(mutated_plan, sort_keys=True) + "\n", encoding="utf-8"
            )

            with pytest.raises(
                direct.ExpandedCapDirectReferenceError,
                match="epoch-5 direct-reference contract drifted",
            ):
                direct.load_contract(plan_path=plan_path, directive_path=directive_path)


def test_artifact_tamper_is_rejected(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"state":"passed"}\n', encoding="utf-8")
    record = direct._record(artifact)  # noqa: SLF001
    artifact.write_text('{"state":"changed"}\n', encoding="utf-8")

    with pytest.raises(direct.ExpandedCapDirectReferenceError, match="checksum or size drifted"):
        direct._validated_record(record, "synthetic artifact")  # noqa: SLF001


def test_ab_ba_share_case_and_opaque_witness_order() -> None:
    pool, _mapping, _lineage = direct.build_complete_container(direct.load_contract())
    variants = judge.build_judge_variants(pool)
    ab_cases = variants["ab"]["cases"]
    ba_cases = variants["ba"]["cases"]

    assert [row["case_id"] for row in ab_cases] == [row["case_id"] for row in ba_cases]
    for ab, ba in zip(ab_cases, ba_cases):
        assert ab["event_set_a"] == ba["event_set_b"]
        assert ab["event_set_b"] == ba["event_set_a"]
    serialized_pool = json.dumps(pool, sort_keys=True)
    assert direct.SYSTEM_BASELINE not in serialized_pool
    assert direct.SYSTEM_CANDIDATE not in serialized_pool


def test_partial_attempt_recovers_to_verified_waiting_without_replay(
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
        raise AssertionError("partial recovery replayed a judge")

    receipt = _run_async(direct.run(output_dir=root, judge_runner=must_not_run))

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == "epoch5_partial_or_interrupted_attempt_preserved_without_replay"
    assert receipt["usage_status"] == "unknown"
    assert receipt["accounting_complete"] is False
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert direct.verify_receipt(root)["state"] == "waiting"


def test_completed_ab_missing_ba_recovers_waiting_without_judge_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root)
    for path in (
        root / "judge/output-ba.private.json",
        root / "judge/sidecars/ba.json",
        root / "judge/sidecars/ba.capacity.json",
        root / "judge/report.json",
        root / "judge/consensus.private.json",
    ):
        path.unlink()
    called = False

    async def must_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("AB-complete recovery started the missing BA turn")

    receipt = _run_async(direct.run(output_dir=root, judge_runner=must_not_run))

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["accounting_complete"] is True
    assert receipt["usage_status"] == "complete"
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["usage"] == {
        "input_tokens": 100,
        "cached_input_tokens": 10,
        "output_tokens": 20,
        "reasoning_output_tokens": 5,
        "total_tokens": 120,
    }
    assert not (root / "judge/sidecars/ba.json").exists()
    assert not (root / "judge/output-ba.private.json").exists()
    assert direct.verify_receipt(root)["state"] == "waiting"


def test_complete_ab_ba_crash_finalizes_without_judge_or_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root)
    judge_called = False
    client_called = False

    async def must_not_run_judge(**_kwargs):
        nonlocal judge_called
        judge_called = True
        raise AssertionError("completed AB/BA was replayed")

    def must_not_create_client():
        nonlocal client_called
        client_called = True
        raise AssertionError("completed AB/BA recovery created a client")

    receipt = _run_async(
        direct.run(
            output_dir=root,
            judge_runner=must_not_run_judge,
            client_factory=must_not_create_client,
        )
    )

    assert judge_called is False
    assert client_called is False
    assert receipt["state"] in {"passed", "rejected"}
    assert receipt["semantic_model_call_count"] == 2
    assert direct.verify_receipt(root)["state"] == receipt["state"]


def test_base_disagreement_before_marker_launches_exactly_one_adjudication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root, disagree=True)
    pool = json.loads((root / "shared-witness-pool.private.json").read_text())
    base = direct._validate_base_judge_bundle(root, pool)  # noqa: SLF001
    disagreements = direct._observable_disagreement_case_ids(  # noqa: SLF001
        pool, base["outputs"]
    )
    direct._prepare_adjudication_artifacts(root, pool, disagreements)  # noqa: SLF001
    fake = _StrictFakeClient()
    judge_called = False

    async def must_not_run_judge(**_kwargs):
        nonlocal judge_called
        judge_called = True
        raise AssertionError("base judge was replayed")

    receipt = _run_async(
        direct.run(
            output_dir=root,
            judge_runner=must_not_run_judge,
            client_factory=lambda: fake,
        )
    )

    assert judge_called is False
    assert len(fake.calls) == 1
    assert (root / "adjudication/adjudication-launch-receipt.json").is_file()
    assert receipt["semantic_model_call_count"] == 3
    assert receipt["state"] in {"passed", "rejected"}


def test_after_adjudication_marker_crash_waits_without_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root, disagree=True)
    pool = json.loads((root / "shared-witness-pool.private.json").read_text())
    base = direct._validate_base_judge_bundle(root, pool)  # noqa: SLF001
    disagreements = direct._observable_disagreement_case_ids(  # noqa: SLF001
        pool, base["outputs"]
    )
    direct._prepare_adjudication_artifacts(root, pool, disagreements)  # noqa: SLF001
    direct._adjudication_launch_receipt(  # noqa: SLF001
        root=root, case_ids=disagreements, base_accounting=base["accounting"]
    )
    client_called = False

    def must_not_create_client():
        nonlocal client_called
        client_called = True
        raise AssertionError("marker recovery replayed adjudication")

    receipt = _run_async(
        direct.run(
            output_dir=root,
            client_factory=must_not_create_client,
            judge_runner=lambda **_kwargs: None,
        )
    )

    assert client_called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch5_partial_or_interrupted_attempt_preserved_without_replay"
    )
    assert receipt["semantic_model_call_count"] == 2


def test_complete_adjudication_crash_finalizes_without_any_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root, disagree=True)
    pool = json.loads((root / "shared-witness-pool.private.json").read_text())
    base = direct._validate_base_judge_bundle(root, pool)  # noqa: SLF001
    disagreements = direct._observable_disagreement_case_ids(  # noqa: SLF001
        pool, base["outputs"]
    )
    setup_client = _StrictFakeClient()
    _run_async(
        direct._run_adjudication(  # noqa: SLF001
            root=root,
            pool=pool,
            case_ids=disagreements,
            client_factory=lambda: setup_client,
            timeout_seconds=direct.JUDGE_TIMEOUT_SECONDS,
            measured_ab_ba_total=base["accounting"]["usage"]["total_tokens"],
        )
    )
    assert len(setup_client.calls) == 1
    client_called = False
    judge_called = False

    def must_not_create_client():
        nonlocal client_called
        client_called = True
        raise AssertionError("completed adjudication recovery created a client")

    async def must_not_run_judge(**_kwargs):
        nonlocal judge_called
        judge_called = True
        raise AssertionError("completed adjudication recovery replayed AB/BA")

    receipt = _run_async(
        direct.run(
            output_dir=root,
            client_factory=must_not_create_client,
            judge_runner=must_not_run_judge,
        )
    )

    assert client_called is False
    assert judge_called is False
    assert receipt["state"] in {"passed", "rejected"}
    assert receipt["semantic_model_call_count"] == 3


def test_terminal_single_mirror_is_validated_before_repair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    valid_root = _frozen_tmp_root(tmp_path / "valid", monkeypatch)
    valid = direct._receipt(  # noqa: SLF001
        root=valid_root,
        state="waiting",
        terminal_reason="synthetic_prelaunch_waiting",
        accounting=direct._zero_accounting(),  # noqa: SLF001
        score=None,
    )
    direct._write_immutable_json(  # noqa: SLF001
        valid_root / "plan-step-receipt.json", valid
    )
    repaired = _run_async(direct.run(output_dir=valid_root))
    assert repaired["state"] == "waiting"
    assert (valid_root / "terminal.json").is_file()

    invalid_root = tmp_path / "invalid" / "epoch-5"
    valid_contract = direct.load_contract()
    invalid_contract = copy.deepcopy(valid_contract)
    invalid_contract["receipt_path"] = invalid_root / "plan-step-receipt.json"
    monkeypatch.setattr(direct, "load_contract", lambda: copy.deepcopy(invalid_contract))
    direct.freeze_run(invalid_root)
    invalid = direct._receipt(  # noqa: SLF001
        root=invalid_root,
        state="waiting",
        terminal_reason="synthetic_invalid_waiting",
        accounting=direct._zero_accounting(),  # noqa: SLF001
        score=None,
    )
    invalid["production_mutated"] = True
    direct._write_immutable_json(  # noqa: SLF001
        invalid_root / "plan-step-receipt.json", invalid
    )

    with pytest.raises(direct.ExpandedCapDirectReferenceError):
        _run_async(direct.run(output_dir=invalid_root))
    assert not (invalid_root / "terminal.json").exists()


@pytest.mark.parametrize(
    "tamper_kind",
    ["output", "prompt", "sidecar_transport", "sidecar_auth", "capacity", "report"],
)
def test_completed_base_tamper_becomes_verified_waiting_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper_kind: str
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root)
    if tamper_kind == "output":
        path = root / "judge/output-ab.private.json"
        value = json.loads(path.read_text())
        value["cases"][0]["support_results"][0]["rationale"] = "Tampered rationale."
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    elif tamper_kind == "prompt":
        path = root / "judge/prompt-ab.private.md"
        path.write_text(path.read_text() + "\nTAMPER\n", encoding="utf-8")
    elif tamper_kind in {"sidecar_transport", "sidecar_auth"}:
        path = root / "judge/sidecars/ab.json"
        value = json.loads(path.read_text())
        value["transport" if tamper_kind == "sidecar_transport" else "auth_type"] = (
            "http" if tamper_kind == "sidecar_transport" else "api_key"
        )
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    elif tamper_kind == "capacity":
        path = root / "judge/sidecars/ab.capacity.json"
        value = json.loads(path.read_text())
        value["managed_chatgpt_auth_verified"] = False
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    else:
        path = root / "judge/report.json"
        value = json.loads(path.read_text())
        value["transport"] = "public_api"
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    judge_called = False
    client_called = False

    async def must_not_run_judge(**_kwargs):
        nonlocal judge_called
        judge_called = True
        raise AssertionError("tampered completed base replayed the judge")

    def must_not_create_client():
        nonlocal client_called
        client_called = True
        raise AssertionError("tampered completed base created a client")

    receipt = _run_async(
        direct.run(
            output_dir=root,
            judge_runner=must_not_run_judge,
            client_factory=must_not_create_client,
        )
    )

    assert judge_called is False
    assert client_called is False
    assert receipt["state"] == "waiting"
    assert receipt["development_quality_passed"] is False
    assert direct.verify_receipt(root)["state"] == "waiting"


def test_tampered_adjudication_capacity_waits_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    direct._launch_receipt(root)  # noqa: SLF001
    _seed_complete_base(root, disagree=True)
    pool = json.loads((root / "shared-witness-pool.private.json").read_text())
    base = direct._validate_base_judge_bundle(root, pool)  # noqa: SLF001
    disagreements = direct._observable_disagreement_case_ids(  # noqa: SLF001
        pool, base["outputs"]
    )
    setup = _StrictFakeClient()
    _run_async(
        direct._run_adjudication(  # noqa: SLF001
            root=root,
            pool=pool,
            case_ids=disagreements,
            client_factory=lambda: setup,
            timeout_seconds=direct.JUDGE_TIMEOUT_SECONDS,
            measured_ab_ba_total=base["accounting"]["usage"]["total_tokens"],
        )
    )
    capacity_path = root / "adjudication/capacity.json"
    capacity = json.loads(capacity_path.read_text())
    capacity["rate_limit_reached_type"] = "primary"
    capacity_path.write_text(json.dumps(capacity, sort_keys=True) + "\n", encoding="utf-8")
    receipt = _run_async(
        direct.run(
            output_dir=root,
            client_factory=lambda: (_ for _ in ()).throw(
                AssertionError("tampered adjudication was replayed")
            ),
        )
    )
    assert receipt["state"] == "waiting"
    assert receipt["semantic_model_call_count"] == 3


def test_quality_artifacts_without_base_launch_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_tmp_root(tmp_path, monkeypatch)
    _seed_complete_base(root)

    with pytest.raises(
        direct.ExpandedCapDirectReferenceError,
        match="without a base launch receipt",
    ):
        _run_async(direct.run(output_dir=root))
    assert not (root / "plan-step-receipt.json").exists()
    assert not (root / "terminal.json").exists()


def test_same_count_mapping_event_substitution_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_builder = judge.make_shared_witness_pool

    def substituted_builder(*args, **kwargs):
        pool, mapping = real_builder(*args, **kwargs)
        for case in mapping["cases"]:
            for witness in case["witnesses"]:
                if witness["provenance"].get("system_id") == direct.SYSTEM_CANDIDATE:
                    witness["provenance"]["original_event"] = copy.deepcopy(
                        witness["provenance"]["original_event"]
                    )
                    witness["provenance"]["original_event"][
                        "synthetic_same_count_substitution"
                    ] = True
                    return pool, mapping
        raise AssertionError("candidate witness not found")

    monkeypatch.setattr(judge, "make_shared_witness_pool", substituted_builder)
    with pytest.raises(
        direct.ExpandedCapDirectReferenceError,
        match="candidate witness provenance hash drifted",
    ):
        direct.build_complete_container(direct.load_contract())
