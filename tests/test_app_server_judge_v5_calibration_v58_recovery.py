from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v57_repair import (
    DEFAULT_OUTPUT_ROOT as V57_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v58_recovery import (
    JudgeV5CalibrationV58RecoveryError,
    _validate_v57_predecessor,
    freeze_v58_recovery,
)
from research_factory.app_server_judge_v5_calibration_v56_structured import (
    DEFAULT_OUTPUT_ROOT as V56_ROOT,
)


def test_v58_accepts_only_zero_usage_presemantic_v57() -> None:
    records = _validate_v57_predecessor(V57_ROOT, V56_ROOT)
    terminal = json.loads((V57_ROOT / "terminal.json").read_text())
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert "v57_terminal" in records
    assert not list(V57_ROOT.glob("turns/*/sidecar.json"))


def test_v58_rejects_mutated_v57_terminal() -> None:
    with tempfile.TemporaryDirectory() as temp:
        copied = Path(temp) / "v57"
        shutil.copytree(V57_ROOT, copied)
        terminal = json.loads((copied / "terminal.json").read_text())
        terminal["semantic_attempt_started"] = True
        (copied / "terminal.json").write_text(json.dumps(terminal))
        with pytest.raises(JudgeV5CalibrationV58RecoveryError):
            _validate_v57_predecessor(copied, V56_ROOT)


def test_v58_refreeze_is_idempotent_and_reuses_exact_requests() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v58"
        frozen = freeze_v58_recovery(output_dir=root)
        again = freeze_v58_recovery(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["turn_plan"] == [
            "root_projection_shard_00",
            "root_projection_shard_01",
        ]
        assert frozen["spec"]["semantic_strategy_changed_from_v57"] is False
        for name in frozen["spec"]["turn_plan"]:
            source = V57_ROOT / "turns" / name.replace("_", "-")
            target = root / "turns" / name.replace("_", "-")
            for filename in ("input.private.json", "prompt.private.md", "schema.json"):
                assert (target / filename).read_bytes() == (source / filename).read_bytes()
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
