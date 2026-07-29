from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v73_direct_field_audit import (
    DEFAULT_OUTPUT_ROOT as V73_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v74_stable_direct_field_audit import (
    EFFORT,
    MODEL,
    TURN_NAMES,
    _validate_v73,
    freeze_v74,
    terminalize_v73_presemantic_failure,
)
from research_factory.app_server_judge_v5_diagnostic import USAGE_FIELDS


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v74_terminalizes_v73_only_as_zero_usage_presemantic_failure() -> None:
    with tempfile.TemporaryDirectory() as temp:
        copied = Path(temp) / "v73"
        shutil.copytree(V73_ROOT, copied)
        terminal = terminalize_v73_presemantic_failure(copied)
        assert terminal["state"] == "failed"
        assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage_status"] == "complete"
        assert terminal["usage"] == {field: 0 for field in USAGE_FIELDS}
        assert terminal["production_mutated"] is False
        assert terminalize_v73_presemantic_failure(copied) == terminal


def test_v74_actual_v73_predecessor_is_hash_bound_and_zero_usage() -> None:
    terminal = terminalize_v73_presemantic_failure(V73_ROOT)
    records = _validate_v73(V73_ROOT)
    assert records["v73_terminal"]["sha256"]
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"] == {field: 0 for field in USAGE_FIELDS}


def test_v74_freeze_reuses_every_v73_request_byte_for_byte() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v74"
        frozen = freeze_v74(output_dir=root)
        again = freeze_v74(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["v73_semantic_turn_count"] == 0
        assert frozen["spec"]["v73_turn_replayed"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        reuse = {row["turn_name"]: row for row in frozen["spec"]["exact_request_reuse"]}
        for turn in frozen["turns"]:
            turn_name = turn["turn_name"]
            source = reuse[turn_name]
            assert turn["paths"]["input"].read_bytes() == Path(source["v73_input"]["path"]).read_bytes()
            assert turn["paths"]["prompt"].read_bytes() == Path(source["v73_prompt"]["path"]).read_bytes()
            assert turn["paths"]["schema"].read_bytes() == Path(source["v73_schema"]["path"]).read_bytes()
            turn_root = root / "turns" / turn_name.replace("_", "-")
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
        assert not (root / "terminal.json").exists()
