from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_operator_authorized_v227_continuation as continuation,
)


def test_direct_lineage_is_narrow_and_frozen():
    lineage = continuation.validate_direct_lineage()
    assert set(lineage["records"]) == {
        "v227_terminal",
        "v227_gate",
        "v227_sidecar",
        "v227_runtime_lock",
        "operator_hold",
    }
    assert lineage["terminal"]["usage"]["total_tokens"] == 32_270
    assert lineage["gate"]["not_started_turn_count"] == 1
    assert lineage["hold"]["state"] == "operator_hold"


def test_freeze_builds_one_combined_non_reference_request(tmp_path: Path):
    root = tmp_path / "operator-authorized-v227-continuation-2026-07-16"
    frozen = continuation.freeze_authorized_continuation(output_dir=root)
    spec = frozen["spec"]
    assert spec["declared_turn_count"] == 1
    assert spec["retry_count"] == 0
    assert spec["completed_v227_turn_replay_allowed"] is False
    assert spec["support_or_alignment_allowed"] is False
    assert spec["holdout_authorized"] is False
    assert spec["production_mutation_allowed"] is False
    assert spec["prompt_bytes"] <= continuation.MAX_PROMPT_BYTES
    assert spec["schema_bytes"] <= continuation.MAX_SCHEMA_BYTES
    prompt = frozen["prompt"]
    assert "golden" not in prompt.lower()
    assert "reference" not in prompt.lower()
    assert "density_stratum" not in prompt
    assert "evidence_repair" in frozen["schema"]["required"]
    assert not (root / "launch-receipt.json").exists()
    assert not (root / "capacity.json").exists()
    assert not (root / "sidecar.json").exists()
    assert not (root / "output.private.json").exists()
    turn_root = root / "turns/operator-authorized-v227-combined-continuation"
    assert frozen["paths"]["capacity"] == turn_root / "capacity.json"
    assert frozen["paths"]["sidecar"] == turn_root / "sidecar.json"
    assert frozen["paths"]["output"] == turn_root / "output.private.json"
    continuation.verify_runtime_lock(root / "runtime-lock.json")


def test_patch_or_drop_validation_is_exact(tmp_path: Path):
    root = tmp_path / "operator-authorized-v227-continuation-2026-07-16"
    frozen = continuation.freeze_authorized_continuation(output_dir=root)
    request = frozen["input"]
    repair = request["repair"]
    source = repair["source_unit"]
    second = request["second_episode_input"]
    rows = [
        {
            "segment_id": row["segment_id"],
            "status": "no_signal",
            "segment_source_context": {
                "kind": "substantive_dialogue",
                "confidence": 1.0,
                "rationale": "The source unit is dialogue.",
            },
            "no_signal_reason": "No additional eligible event was found.",
            "events": [],
        }
        for row in second["normalization_segments"]
    ]
    valid_drop = {
        "evidence_repair": {
            "repair_id": repair["repair_id"],
            "action": "drop",
            "evidence": "",
        },
        "episode_id": second["episode_id"],
        "segments": rows,
    }
    assert continuation.validate_combined_output(valid_drop, frozen) == []
    invalid_patch = json.loads(json.dumps(valid_drop))
    invalid_patch["evidence_repair"] = {
        "repair_id": repair["repair_id"],
        "action": "patch",
        "evidence": source + "not exact",
    }
    assert continuation.validate_combined_output(invalid_patch, frozen) == [
        "repair_evidence_not_exact"
    ]


def test_freeze_rejects_nonempty_unlocked_root(tmp_path: Path):
    root = tmp_path / "operator-authorized-v227-continuation-2026-07-16"
    root.mkdir()
    (root / "partial.json").write_text("{}", encoding="utf-8")
    with pytest.raises(continuation.OperatorAuthorizedV227Error):
        continuation.freeze_authorized_continuation(output_dir=root)


def test_execution_dispatches_exactly_one_bounded_luna_turn(tmp_path: Path):
    root = tmp_path / "operator-authorized-v227-continuation-2026-07-16"
    calls = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def run_ephemeral_structured_turn(self, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("synthetic one-call stop")

    terminal = asyncio.run(
        continuation.run_authorized_continuation(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )

    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-5.6-luna"
    assert calls[0]["effort"] == "low"
    assert calls[0]["batch_size"] == 2
    assert calls[0]["thread_mode"] == "new_thread"
    turn_root = root / "turns/operator-authorized-v227-combined-continuation"
    assert calls[0]["capacity_checkpoint_path"] == turn_root / "capacity.json"
    assert calls[0]["sidecar_path"] == turn_root / "sidecar.json"
    assert calls[0]["output_path"] == turn_root / "output.private.json"
    assert terminal["semantic_attempt_count"] == 0
    assert terminal["semantic_retry_count"] == 0
    assert terminal["successor_authorized"] is False
