import json

import pytest

from research_factory.signal_desk_frontier import FrontierCalibrationError, measure_frontier


def _event():
    return {
        "speaker_id": "A",
        "attribution_type": "direct_speech",
        "issue_label": "jobs",
        "stance": "warning",
        "evidence_start": 0,
        "evidence_end": 10,
        "claim_text": "AI changes jobs",
    }


def test_frontier_requires_complete_development_and_freezes_measured_gates(tmp_path):
    manifest = {
        "manifest_sha256": "m" * 64,
        "windows": [
            {
                "window_id": "w1",
                "split": "development",
                "text_sha256": "t" * 64,
                "show_id": "show",
                "episode_id": "episode",
                "transcript_structure": "speaker_turn",
            }
        ],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    gold = tmp_path / "gold"
    prediction = tmp_path / "prediction"
    gold.mkdir(); prediction.mkdir()
    output = {"window_id": "w1", "events": [_event()]}
    (gold / "w1.json").write_text(json.dumps(output))
    with pytest.raises(FrontierCalibrationError, match="predictions=1"):
        measure_frontier(
            manifest_path=manifest_path, gold_c_root=gold, prediction_root=prediction
        )
    (prediction / "w1.json").write_text(json.dumps(output))
    receipt, gates = measure_frontier(
        manifest_path=manifest_path, gold_c_root=gold, prediction_root=prediction
    )
    assert receipt["passes"] == 1
    assert receipt["metrics"]["macro_composite"] == 1.0
    assert gates["calibration"]["measured_ceiling_point"]["event_recall"] == 1.0
    assert gates["calibration"]["window_bootstrap_bounds"]["event_recall"]["lcb"] == 1.0
    assert receipt["window_bootstrap_bounds"]["event_recall"]["resampling_unit"] == "window"
    assert gates["gates"]["event_recall"]["minimum"] == pytest.approx(0.97)
