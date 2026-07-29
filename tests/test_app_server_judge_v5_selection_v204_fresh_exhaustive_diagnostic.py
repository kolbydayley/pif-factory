from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory.app_server_capacity_reserve import ReserveCapacityError
from research_factory.app_server_judge_v5_selection_v204_fresh_exhaustive_diagnostic import (
    CoreArmReserveClient,
    JudgeV5SelectionV204Error,
    _validate_v203_authorization,
    evaluate_structural_gate,
    freeze_v204,
    run_v204,
    verify_runtime_lock,
)


def _write_normalized_outputs(root: Path, manifest: dict, *, ratio: float = 0.8) -> None:
    rows = []
    for episode in manifest["episodes"]:
        for segment in episode["segments"]:
            golden = int(segment["golden_event_count"])
            count = 0 if segment["density_stratum"] == "no_signal" else round(golden * ratio)
            rows.append(
                {
                    "segment_id": segment["segment_id"],
                    "events": [{"opaque": index} for index in range(count)],
                }
            )
    output = root / "normalized_outputs" / "all.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps({"episode_id": "synthetic", "segments": rows}) + "\n",
        encoding="utf-8",
    )


def _passing_report(total_tokens: int = 400_000) -> dict:
    return {
        "validated_segments": 16,
        "schema_status_success_rate": 1.0,
        "attempted_calls": 4,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage_measured_attempts": 4,
        "usage_unknown_attempts": 0,
        "normalized_exact_evidence_rate": 1.0,
        "no_signal_candidate_positive_segments_unadjudicated": 0,
        "metric_grounding_error_events": 0,
        "usage": {
            "input_tokens": total_tokens - 20_000,
            "cached_input_tokens": 10_000,
            "output_tokens": 20_000,
            "reasoning_output_tokens": 10_000,
            "total_tokens": total_tokens,
        },
    }


def test_v204_validates_and_freezes_v203_without_starting_semantics(tmp_path: Path):
    predecessor = _validate_v203_authorization()
    assert predecessor["terminal"]["semantic_attempt_authorized"] is True
    root = tmp_path / "v204"
    first = freeze_v204(output_dir=root)
    second = freeze_v204(output_dir=root)
    assert first["turn_names"] == second["turn_names"]
    assert len(first["turn_names"]) == 4
    assert verify_runtime_lock(first["runtime_lock"])["phase_id"].endswith(
        "fresh_exhaustive_diagnostic"
    )
    assert not (root / "launch-receipt.json").exists()
    assert not (root / "terminal.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    spec = json.loads((root / "attempt-spec.json").read_text())
    assert spec["semantic_model_calls_declared"] == 4
    assert spec["retry_count_per_turn"] == 0
    assert spec["holdout_authorized"] is False
    assert spec["production_mutation_allowed"] is False


def test_v204_runtime_lock_requires_exact_runtime_coverage(tmp_path: Path):
    frozen = freeze_v204(output_dir=tmp_path / "v204")
    lock_path = frozen["runtime_lock"]
    value = json.loads(lock_path.read_text())
    value["runtime_files"] = value["runtime_files"][:-1]
    lock_path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV204Error, match="runtime lock drifted"):
        verify_runtime_lock(lock_path)


class _FakeInner:
    def __init__(self, *, used_percent: int = 0):
        self.used_percent = used_percent
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.log: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def _request(self, method, params):
        assert method == "account/rateLimits/read"
        assert params == {}
        self.log.append("capacity")
        return {
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "primary": {"usedPercent": self.used_percent, "resetsAt": 1},
                    "rateLimitReachedType": None,
                }
            }
        }

    async def run_ephemeral_structured_turn(self, *args, **kwargs):
        self.log.append("semantic")
        sidecar_path = Path(kwargs["sidecar_path"])
        output_path = Path(kwargs["output_path"])
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        usage = {
            "input_tokens": 90,
            "cached_input_tokens": 10,
            "output_tokens": 10,
            "reasoning_output_tokens": 5,
            "total_tokens": 100,
        }
        sidecar_path.write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_complete": True,
                    "usage_status": "measured",
                    "usage": usage,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_path.write_text('{"ok":true}\n', encoding="utf-8")
        return SimpleNamespace(
            status_ok=True,
            usage=SimpleNamespace(total_tokens=100),
        )


def test_v204_adapter_probes_before_each_semantic_turn_and_sequences_checkpoints(
    tmp_path: Path,
):
    async def exercise():
        frozen = freeze_v204(output_dir=tmp_path / "v204")
        inner = _FakeInner()
        arm_root = frozen["root"] / "arm"
        client = CoreArmReserveClient(
            policy_path=frozen["capacity_policy"],
            arm_root=arm_root,
            inner_factory=lambda: inner,
        )
        async with client:
            for turn_name in frozen["turn_names"][:2]:
                await client.run_ephemeral_structured_turn(
                    sidecar_path=arm_root / "sidecars" / f"{turn_name}.json",
                    output_path=arm_root / "raw_outputs" / f"{turn_name}.json",
                )
        return frozen, inner, arm_root

    frozen, inner, arm_root = asyncio.run(exercise())
    assert inner.log == ["capacity", "semantic", "capacity", "semantic"]
    for turn_name in frozen["turn_names"][:2]:
        turn_root = frozen["root"] / "turns" / turn_name.replace("_", "-")
        checkpoint = json.loads((turn_root / "capacity.json").read_text())
        assert checkpoint["cleared_for_semantic_turn"] is True
        assert checkpoint["managed_chatgpt_auth_verified"] is True
        assert checkpoint["rate_limit_reached_type"] is None
        assert (turn_root / "sidecar.json").is_symlink()
        assert (turn_root / "output.private.json").is_symlink()


def test_v204_adapter_stops_before_semantic_thread_when_reserve_does_not_clear(
    tmp_path: Path,
):
    async def exercise():
        frozen = freeze_v204(output_dir=tmp_path / "v204")
        inner = _FakeInner(used_percent=75)
        arm_root = frozen["root"] / "arm"
        turn_name = frozen["turn_names"][0]
        client = CoreArmReserveClient(
            policy_path=frozen["capacity_policy"],
            arm_root=arm_root,
            inner_factory=lambda: inner,
        )
        async with client:
            with pytest.raises(ReserveCapacityError):
                await client.run_ephemeral_structured_turn(
                    sidecar_path=arm_root / "sidecars" / f"{turn_name}.json",
                    output_path=arm_root / "raw_outputs" / f"{turn_name}.json",
                )
        return frozen, inner, arm_root

    frozen, inner, arm_root = asyncio.run(exercise())
    assert inner.log == ["capacity"]
    assert not list(frozen["root"].rglob("capacity.json"))
    assert not list((arm_root / "sidecars").glob("*.json"))


def test_v204_structural_gate_enforces_density_and_cost(tmp_path: Path):
    predecessor = _validate_v203_authorization()
    arm_root = tmp_path / "arm"
    _write_normalized_outputs(arm_root, predecessor["manifest"], ratio=0.8)
    gate = evaluate_structural_gate(
        report=_passing_report(),
        manifest=predecessor["manifest"],
        arm_root=arm_root,
    )
    assert gate["passed"] is True
    assert gate["dense_median_candidate_to_reference_event_count_ratio"] >= 0.75
    assert gate["cost"]["production_amortized_total_token_ratio"] < 0.28
    failed_report = _passing_report(total_tokens=600_000)
    failed_report["no_signal_candidate_positive_segments_unadjudicated"] = 1
    failed = evaluate_structural_gate(
        report=failed_report,
        manifest=predecessor["manifest"],
        arm_root=arm_root,
    )
    assert failed["passed"] is False
    assert "no_signal_candidate_positive_segments_0" in failed["failed_checks"]
    assert "production_amortized_total_token_ratio_lte_0_28" in failed["failed_checks"]


def test_v204_launches_once_and_writes_judge_authorization_only_after_gate(
    tmp_path: Path,
):
    root = tmp_path / "v204"
    calls = 0

    async def arm_runner(conn, **kwargs):
        nonlocal calls
        calls += 1
        assert (root / "launch-receipt.json").is_file()
        assert (root / "runtime-lock.json").is_file()
        assert kwargs["model"] == "gpt-5.6-sol"
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["concurrency"] == 1
        predecessor = _validate_v203_authorization()
        _write_normalized_outputs(root / "arm", predecessor["manifest"], ratio=0.8)
        report = _passing_report()
        (root / "arm" / "report.json").write_text(
            json.dumps(report) + "\n", encoding="utf-8"
        )
        return report

    first = asyncio.run(
        run_v204(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    second = asyncio.run(
        run_v204(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    assert first == second
    assert calls == 1
    assert first["fresh_judge_authorized"] is True
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_retry_allowed"] is False


def test_v204_unknown_usage_is_immutable_infrastructure_failure(tmp_path: Path):
    root = tmp_path / "v204"
    calls = 0

    async def arm_runner(conn, **kwargs):
        nonlocal calls
        calls += 1
        sidecar = root / "arm" / "sidecars" / "failed.json"
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(
            json.dumps(
                {
                    "state": "failed",
                    "status": "failed",
                    "usage_complete": False,
                    "usage_status": "unknown",
                    "usage": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        raise RuntimeError("synthetic private failure")

    first = asyncio.run(
        run_v204(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    second = asyncio.run(
        run_v204(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    assert first == second
    assert calls == 1
    assert first["terminal_reason"] == "infrastructure_or_extraction_attempt_failed"
    assert first["terminal_classification"] == "inactive_incomplete_recovery_required"
    assert first["usage_status"] == "unknown"
    assert first["accounting_complete"] is False
    assert first["cumulative_unknown_usage_turn_count"] == 3
    assert first["cumulative_conservative_unknown_usage_upper_bound"] == 360_000
    assert first["semantic_retry_allowed"] is False
    assert first["holdout_authorized"] is False
    failure = json.loads((root / "failure.json").read_text())
    assert "synthetic private failure" not in json.dumps(failure)
    assert failure["error_message_bytes"] > 0
