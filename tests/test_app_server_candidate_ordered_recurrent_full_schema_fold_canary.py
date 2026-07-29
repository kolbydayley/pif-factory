from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_candidate_ordered_recurrent_full_schema_fold_canary as canary
from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249


def _turns() -> list[dict]:
    return canary.prepare_turns(canary.load_contract())


def _frozen_v249_output() -> dict:
    path = next(v249.DEFAULT_OUTPUT_ROOT.glob("turns/*/output.private.json"))
    return json.loads(path.read_text(encoding="utf-8"))


def _outputs(turns: list[dict]) -> list[dict]:
    raw = _frozen_v249_output()
    by_id = {row["segment_id"]: row for row in raw["segments"]}
    return [
        {"episode_id": turn["episode_id"], "segments": [copy.deepcopy(by_id[turn["segment_id"]])]}
        for turn in turns
    ]


def _usage(total: int) -> dict[str, int]:
    return {
        "input_tokens": max(total - 3_000, 0),
        "cached_input_tokens": 500,
        "output_tokens": min(total, 2_500),
        "reasoning_output_tokens": min(total, 500),
        "total_tokens": total,
    }


class _FakeClient:
    def __init__(
        self,
        outputs: list[dict],
        *,
        totals: tuple[int, ...] = (10_000, 20_000),
        first_usage_status: str = "measured",
        fail_status_on: int | None = None,
    ) -> None:
        self.outputs = outputs
        self.totals = totals
        self.first_usage_status = first_usage_status
        self.fail_status_on = fail_status_on
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.started = 0
        self.calls: list[dict] = []
        self.thread = SimpleNamespace(
            thread_id="thread-recurrent-1", model=canary.MODEL, ephemeral=True
        )

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def start_thread(self, **kwargs: object) -> SimpleNamespace:
        self.started += 1
        assert kwargs["model"] == canary.MODEL
        assert kwargs["ephemeral"] is True
        return self.thread

    async def run_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        index = len(self.calls)
        self.calls.append(kwargs)
        assert kwargs["thread"] is self.thread
        assert kwargs["thread_mode"] == "same_thread"
        assert kwargs["batch_size"] == 1
        output = self.outputs[index]
        status_ok = index != self.fail_status_on
        sidecar = {
            "state": "completed" if status_ok else "failed",
            "status": "completed" if status_ok else "failed",
            "usage_status": self.first_usage_status if index == 0 else "measured",
            "usage_complete": (self.first_usage_status == "measured") if index == 0 else True,
            "usage": _usage(self.totals[index]),
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_id": self.thread.thread_id,
            "model": canary.MODEL,
            "effort": canary.EFFORT,
            "error_class": None if status_ok else "turn_failed",
            "wall_elapsed_seconds": 1.25,
        }
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(sidecar) + "\n", encoding="utf-8"
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(
            status_ok=status_ok,
            output=output if status_ok else None,
            thread_id=self.thread.thread_id,
        )


def _capacity_probe(fail_on: int | None = None):
    calls: list[dict] = []

    async def probe(_client: object, **kwargs: object) -> dict:
        index = len(calls)
        calls.append(dict(kwargs))
        if index == fail_on:
            raise canary.OperationalWaitingError("capacity unavailable")
        payload = {
            "schema_version": "test_capacity",
            "turn_name": kwargs["turn_name"],
            "remaining_phase_token_cap": kwargs["remaining_token_cap"],
            "cleared_for_semantic_turn": True,
        }
        Path(str(kwargs["checkpoint_path"])).write_text(
            json.dumps(payload) + "\n", encoding="utf-8"
        )
        return payload

    probe.calls = calls  # type: ignore[attr-defined]
    return probe


class _LiveCapacityClient:
    account_summary = {"type": "chatgpt", "plan_type": "pro"}

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict]] = []

    async def _request(self, method: str, params: dict) -> dict:
        self.requests.append((method, params))
        return {
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 10, "resetsAt": 1_800_000_000},
                    "rateLimitReachedType": None,
                }
            }
        }


def test_live_capacity_checkpoint_reports_existing_thread_without_starting_turn(
    tmp_path: Path,
) -> None:
    client = _LiveCapacityClient()
    checkpoint_path = tmp_path / "capacity.json"
    checkpoint = asyncio.run(
        canary._probe_capacity(  # noqa: SLF001
            client,
            checkpoint_path=checkpoint_path,
            turn_name=canary.TURN_NAMES[1],
            remaining_token_cap=60_000,
            thread_exists_before_probe=True,
        )
    )
    assert client.requests == [("account/rateLimits/read", {})]
    assert checkpoint["cleared_for_semantic_turn"] is True
    assert checkpoint["thread_exists_before_probe"] is True
    assert checkpoint["semantic_turn_started_before_probe"] is False
    assert checkpoint["semantic_turn_started_by_probe"] is False
    assert checkpoint["sidecar_started_before_probe"] is False
    assert json.loads(checkpoint_path.read_text(encoding="utf-8")) == checkpoint


def test_contract_binds_final_plan_order_caps_and_turn2_isolation() -> None:
    contract = canary.load_contract()
    turns = canary.prepare_turns(contract)
    assert contract["directive_sha256"] == canary.EXPECTED_DIRECTIVE_SHA256
    assert canary._sha256_file(canary.PLAN_PATH) == canary.EXPECTED_PLAN_SHA256  # noqa: SLF001
    assert [row["segment_id"] for row in turns] == [
        canary.NO_SIGNAL_SEGMENT_ID,
        canary.DENSE_SEGMENT_ID,
    ]
    assert [row["token_cap"] for row in turns] == [24_000, 48_891]
    assert sum(row["token_cap"] for row in turns) == 72_891
    assert canary.NO_SIGNAL_SEGMENT_ID not in turns[1]["prompt"]
    assert canary.DENSE_SEGMENT_ID not in turns[0]["prompt"]
    assert "do not revise, delete, relabel" in turns[1]["prompt"]
    assert turns[1]["schema"]["properties"]["segments"]["maxItems"] == 1


def test_pass_uses_one_ephemeral_thread_preserves_all_events_and_canonicalizes_order(
    tmp_path: Path,
) -> None:
    turns = _turns()
    outputs = _outputs(turns)
    expected_counts = {
        row["segments"][0]["segment_id"]: len(row["segments"][0]["events"])
        for row in outputs
    }
    client = _FakeClient(outputs)
    probe = _capacity_probe()
    root = tmp_path / "epoch3-pass"
    receipt = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=lambda: client,
            capacity_probe=probe,
        )
    )
    assert receipt["state"] == "passed"
    assert receipt["semantic_model_call_count"] == 2
    assert receipt["semantic_retry_count"] == 0
    assert client.started == 1
    assert len(client.calls) == 2
    assert client.calls[0]["thread"] is client.calls[1]["thread"] is client.thread
    assert [row["remaining_token_cap"] for row in probe.calls] == [72_891, 62_891]  # type: ignore[attr-defined]
    assert [row["thread_exists_before_probe"] for row in probe.calls] == [False, True]  # type: ignore[attr-defined]
    folded = json.loads((root / "combined-normalized-output.private.json").read_text())
    assert [row["segment_id"] for row in folded["segments"]] == list(
        canary.CANONICAL_SEGMENT_ORDER
    )
    assert {
        row["segment_id"]: len(row["events"]) for row in folded["segments"]
    } == expected_counts
    fold = json.loads((root / "fold-receipt.json").read_text())
    assert fold["all_validated_events_preserved"] is True
    assert fold["input_event_count"] == fold["output_event_count"]
    assert fold["exact_identity_duplicate_detection"] == "diagnostic_only_nonblocking"
    capacity_records = [
        row for row in receipt["records"] if Path(row["path"]).name == "capacity.json"
    ]
    assert len(capacity_records) == 2
    for row in capacity_records:
        assert canary._record(Path(row["path"])) == row  # noqa: SLF001


def test_exact_identity_duplicates_are_diagnostic_only_and_both_survive_fold(
    tmp_path: Path,
) -> None:
    turns = _turns()
    outputs = _outputs(turns)
    no_signal = outputs[0]["segments"][0]
    event = copy.deepcopy(no_signal["events"][0])
    no_signal["events"].append(copy.deepcopy(event))
    start_id = event["evidence_start_unit_id"]
    receipt = next(row for row in no_signal["unit_receipts"] if row["unit_id"] == start_id)
    receipt["eligible_event_count"] += 1
    client = _FakeClient(outputs)
    root = tmp_path / "epoch3-duplicates"
    terminal = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=lambda: client,
            capacity_probe=_capacity_probe(),
        )
    )
    assert terminal["state"] == "passed"
    folded = json.loads((root / "combined-normalized-output.private.json").read_text())
    folded_no_signal = next(
        row for row in folded["segments"] if row["segment_id"] == canary.NO_SIGNAL_SEGMENT_ID
    )
    assert len(folded_no_signal["events"]) == 2
    assert folded_no_signal["events"][0] == folded_no_signal["events"][1]
    fold_receipt = json.loads((root / "fold-receipt.json").read_text())
    assert fold_receipt["exact_identity_duplicate_count"] >= 1
    assert fold_receipt["exact_identity_duplicate_detection"] == "diagnostic_only_nonblocking"
    gate = json.loads((root / "structural-gate.json").read_text())
    assert gate["checks"]["exact_identity_duplicate_count_reported_diagnostic_only"] is True
    assert gate["passed"] is True


@pytest.mark.parametrize(
    ("mode", "expected_state"),
    [
        ("turn1_structural", "rejected"),
        ("turn1_accounting", "waiting"),
        ("turn1_cap", "rejected"),
        ("turn2_capacity", "waiting"),
        ("turn1_transport", "waiting"),
    ],
)
def test_turn2_is_blocked_on_turn1_validation_accounting_cap_or_operational_failure(
    tmp_path: Path, mode: str, expected_state: str
) -> None:
    turns = _turns()
    outputs = _outputs(turns)
    totals = (10_000, 20_000)
    usage_status = "measured"
    fail_status_on = None
    fail_probe = None
    if mode == "turn1_structural":
        outputs[0]["segments"][0]["unit_receipts"].pop()
    elif mode == "turn1_accounting":
        usage_status = "unknown"
    elif mode == "turn1_cap":
        totals = (24_001, 20_000)
    elif mode == "turn2_capacity":
        fail_probe = 1
    elif mode == "turn1_transport":
        fail_status_on = 0
    client = _FakeClient(
        outputs,
        totals=totals,
        first_usage_status=usage_status,
        fail_status_on=fail_status_on,
    )
    receipt = asyncio.run(
        canary.run(
            output_dir=tmp_path / mode,
            client_factory=lambda: client,
            capacity_probe=_capacity_probe(fail_on=fail_probe),
        )
    )
    assert receipt["state"] == expected_state
    assert receipt["semantic_retry_count"] == 0
    assert len(client.calls) == 1
    if mode == "turn1_accounting":
        assert receipt["accounting_complete"] is False
        assert receipt["usage_status"] == "unknown"


def test_process_lock_and_receipt_integrity_are_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "epoch3-lock"
    with canary._advisory_process_lock(root):  # noqa: SLF001
        with pytest.raises(canary.OperationalWaitingError, match="owns the lock"):
            canary.freeze_run(root)

    turns = _turns()
    client = _FakeClient(_outputs(turns))
    receipt = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=lambda: client,
            capacity_probe=_capacity_probe(),
        )
    )
    assert canary.verify_receipt(root) == receipt
    artifact = Path(receipt["records"][0]["path"])
    artifact.write_text(artifact.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(canary.OrderedRecurrentFoldError, match="integrity"):
        canary.verify_receipt(root)


def test_capacity_checkpoint_drift_invalidates_terminal_receipt(tmp_path: Path) -> None:
    root = tmp_path / "epoch3-capacity-integrity"
    turns = _turns()
    receipt = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=lambda: _FakeClient(_outputs(turns)),
            capacity_probe=_capacity_probe(),
        )
    )
    capacity_records = [
        row for row in receipt["records"] if Path(row["path"]).name == "capacity.json"
    ]
    assert len(capacity_records) == 2
    capacity_path = Path(capacity_records[1]["path"])
    capacity_path.write_text(
        capacity_path.read_text(encoding="utf-8") + "drift", encoding="utf-8"
    )
    with pytest.raises(canary.OrderedRecurrentFoldError, match="integrity"):
        canary.verify_receipt(root)


def test_freeze_hash_locks_requests_and_excludes_capacity_files(tmp_path: Path) -> None:
    root = tmp_path / "epoch3-freeze"
    frozen = canary.freeze_run(root)
    lock = canary.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert lock["turn_total_token_caps"] == [24_000, 48_891]
    assert lock["phase_total_token_cap"] == 72_891
    assert len(lock["frozen_request"]) == 9
    assert not list(root.rglob("capacity.json"))
    assert all(Path(row["path"]).name != "capacity.json" for row in lock["frozen_request"])
    schema = root / "turns" / canary.TURN_NAMES[1] / "schema.json"
    schema.write_text(schema.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(canary.OrderedRecurrentFoldError, match="runtime lock"):
        canary.verify_runtime_lock(frozen["runtime_lock"])
