import json

import pytest

from research_factory.signal_desk_gold_audit import (
    GoldAuditError,
    evaluate_dev_audit,
    select_dev_audit_windows,
)


def _output(window_id, speaker="A"):
    return {
        "window_id": window_id,
        "events": [
            {
                "speaker_id": speaker,
                "attribution_type": "direct_speech",
                "issue_label": "jobs",
                "stance": "warning",
                "evidence_start": 0,
                "evidence_end": 10,
                "claim_text": "AI changes jobs",
            }
        ],
    }


def test_dev_audit_passes_identical_outputs(tmp_path):
    manifest = {
        "manifest_sha256": "a" * 64,
        "windows": [
            {
                "window_id": "w1",
                "show_id": "s1",
                "episode_id": "e1",
                "transcript_structure": "speaker_turn",
            }
        ],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
        (root / turn / "w1.json").write_text(json.dumps(_output("w1")))
    receipt = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1,
        agreement_minimum=0.0, critical_error_maximum=1.0,
    )
    assert receipt["passed"] is True
    assert receipt["agreement"]["point"] == 1.0
    assert receipt["item_outputs_exposed"] is False


def test_dev_audit_fails_closed_when_incomplete_or_critical(tmp_path):
    manifest = {
        "manifest_sha256": "a" * 64,
        "windows": [
            {
                "window_id": "w1",
                "show_id": "s1",
                "episode_id": "e1",
                "transcript_structure": "speaker_turn",
            }
        ],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    (root / "C").mkdir(parents=True)
    (root / "C" / "w1.json").write_text(json.dumps(_output("w1")))
    with pytest.raises(GoldAuditError, match="incomplete"):
        evaluate_dev_audit(
            manifest_path=path, result_root=root, expected_windows=1,
            initial_windows=1, minimum_events=1,
            agreement_minimum=0.0, critical_error_maximum=0.5,
        )
    (root / "AUDIT").mkdir()
    (root / "AUDIT" / "w1.json").write_text(json.dumps(_output("w1", speaker="B")))
    receipt = evaluate_dev_audit(
        manifest_path=path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1,
        agreement_minimum=0.0, critical_error_maximum=0.5,
    )
    assert receipt["passed"] is False
    assert receipt["critical_errors"]["errors"] == 1
    assert receipt["catastrophic_windows"] == 0


def test_dev_audit_expands_in_40_window_blocks_to_event_floor():
    manifest = {
        "windows": [
            {"window_id": f"w{i:03d}", "split": "development"}
            for i in range(100)
        ]
    }
    counts = {f"w{i:03d}": 20 for i in range(100)}
    selected = select_dev_audit_windows(
        manifest,
        counts,
        initial_window_ids=[f"w{i:03d}" for i in range(19)],
    )
    assert selected["initial_windows"] == 19
    assert selected["expanded_windows"] == 59
    assert selected["expansion_blocks"] == 1
    assert selected["event_denominator"] == 1180
    assert selected["decision_ready"] is True
