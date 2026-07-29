from __future__ import annotations

import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v68_exact_span_recovery import (
    DEFAULT_OUTPUT_ROOT as V68_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v69_stable_recovery import (
    ROOT_EFFORT,
    ROOT_MODEL,
    TURN_NAME,
    _validate_v68,
    freeze_v69,
    terminalize_v68_presemantic_failure,
)
from research_factory.app_server_judge_v5_diagnostic import _sha256_file


def test_v69_terminalizes_v68_as_zero_usage_presemantic_failure() -> None:
    terminal = terminalize_v68_presemantic_failure()
    again = terminalize_v68_presemantic_failure()
    assert again == terminal
    assert terminal["state"] == "failed"
    assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage_status"] == "complete"
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert not (V68_ROOT / "turns/neutral-root-verification/capacity.json").exists()
    assert not (V68_ROOT / "turns/neutral-root-verification/sidecar.json").exists()


def test_v69_validates_the_immutable_v68_failure_and_request() -> None:
    records = _validate_v68(V68_ROOT)
    assert records["v68_terminal"]["sha256"]
    assert records["v68_root_input"]["sha256"]
    assert records["v68_root_prompt"]["sha256"]
    assert records["v68_root_schema"]["sha256"]


def test_v69_freeze_reuses_exact_v68_request_in_one_new_version() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v69"
        frozen = freeze_v69(output_dir=root)
        again = freeze_v69(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["turn_plan"] == [
            {"turn_name": TURN_NAME, "model": ROOT_MODEL, "effort": ROOT_EFFORT}
        ]
        assert frozen["spec"]["v67_pointwise_replayed"] is False
        assert frozen["spec"]["v68_semantic_turn_count"] == 0
        assert frozen["spec"]["retry_count_per_turn"] == 0
        source = V68_ROOT / "turns/neutral-root-verification"
        target = root / "turns/neutral-root-verification"
        assert _sha256_file(target / "input.private.json") == _sha256_file(
            source / "input.private.json"
        )
        assert _sha256_file(target / "prompt.private.md") == _sha256_file(
            source / "prompt.private.md"
        )
        assert _sha256_file(target / "schema.json") == _sha256_file(
            source / "schema.json"
        )
        assert not (target / "capacity.json").exists()
        assert not (target / "sidecar.json").exists()
        assert not (root / "terminal.json").exists()
