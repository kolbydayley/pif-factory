from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v213_event_selector_presemantic_recovery as v213,
)


@pytest.fixture(scope="module")
def frozen_v213(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v213") / "attempt"
    frozen = v213.freeze_v213(output_dir=root)
    return root, frozen


def test_v213_preserves_v212_as_zero_usage_presemantic_attempt():
    predecessor = v213._validate_v212_presemantic_failure()
    assert set(predecessor["records"]) == {
        "runtime_lock",
        "spec",
        "capacity_policy",
        "capacity_audit",
        "input",
        "prompt",
        "schema",
    }
    assert predecessor["spec"]["state"] == "frozen_before_model_call"
    assert predecessor["spec"]["retry_count_per_turn"] == 0
    assert predecessor["spec"]["extraction_model_calls_authorized"] == 0
    assert not (predecessor["root"] / "launch-receipt.json").exists()
    assert not (predecessor["root"] / "terminal.json").exists()
    assert not list(predecessor["root"].rglob("capacity.json"))
    assert not list(predecessor["root"].rglob("sidecar.json"))
    assert not list(predecessor["root"].rglob("output.private.json"))


def test_v213_freeze_only_to_run_handoff_is_idempotent(frozen_v213):
    root, first = frozen_v213
    second = v213.freeze_v213(output_dir=root)
    assert second["runtime_lock"] == first["runtime_lock"]
    assert second["spec"] == first["spec"]
    incident = json.loads((root / "v212-presemantic-incident.json").read_text())
    assert incident["classification"] == "presemantic_local_orchestration_failure"
    assert incident["semantic_attempt_started"] is False
    assert incident["capacity_probe_started"] is False
    assert incident["thread_started"] is False
    assert incident["turn_started"] is False
    assert incident["usage_status"] == "complete"
    assert incident["usage"]["total_tokens"] == 0
    assert incident["v212_artifacts_mutated"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v213_runtime_lock_has_exact_coverage(frozen_v213):
    _root, frozen = frozen_v213
    lock = v213.verify_runtime_lock(frozen["runtime_lock"])
    assert len(lock["runtime_files"]) == 19
    assert len(lock["v212_presemantic_attempt"]) == 7
    assert lock["managed_chatgpt_auth_only"] is True
    assert lock["production_mutation_allowed"] is False


def test_v213_runtime_lock_rejects_missing_runtime_file(frozen_v213):
    root, frozen = frozen_v213
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v213.JudgeV5SelectionV213Error, match="runtime lock drifted"):
        v213.verify_runtime_lock(mutated)


def test_v213_started_attempt_cannot_be_silently_resumed(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v213.freeze_v213(output_dir=root)
    receipt = v213._freeze_launch_receipt(frozen)
    assert receipt.is_file()
    with pytest.raises(
        v213.JudgeV5SelectionV213Error, match="cannot be silently resumed"
    ):
        v213.freeze_v213(output_dir=root)
    with pytest.raises(v213.JudgeV5SelectionV213Error, match="already exists"):
        v213._freeze_launch_receipt(frozen)
