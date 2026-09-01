from __future__ import annotations

import hashlib
import json

from research_factory.signal_desk_rebuild_asr_sanity import evaluate_episode


def _fixture(tmp_path, *, gap: float = 0.6):
    words = []
    cursor = 0.0
    tokens = []
    for index in range(100):
        token = f"token{index}"
        tokens.append(token)
        words.append(
            {"text": token, "start": cursor, "end": cursor + 0.3, "speaker": index % 2}
        )
        cursor += gap
    response = {
        "duration": 60.0,
        "language": "en",
        "text": " ".join(tokens),
        "words": words,
    }
    text = response["text"] + "\n"
    text_path = tmp_path / "fixture.txt"
    response_path = tmp_path / "fixture.json"
    text_path.write_text(text, encoding="utf-8")
    response_path.write_text(json.dumps(response), encoding="utf-8")
    return {
        "id": "fixture",
        "path": str(text_path),
        "response_path": str(response_path),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "source_id": "fixture-show",
    }


def test_asr_sanity_accepts_normal_timed_diarized_transcript(tmp_path) -> None:
    result = evaluate_episode(_fixture(tmp_path))
    assert result["passed"] is True
    assert result["words_per_minute"] == 100.0
    assert result["speaker_label_count"] == 2
    assert result["long_gaps"] == []


def test_asr_sanity_fails_closed_on_gap_over_thirty_seconds(tmp_path) -> None:
    row = _fixture(tmp_path)
    response_path = tmp_path / "fixture.json"
    response = json.loads(response_path.read_text())
    response["words"][50]["start"] += 31
    response["words"][50]["end"] += 31
    for word in response["words"][51:]:
        word["start"] += 31
        word["end"] += 31
    response["duration"] += 31
    response_path.write_text(json.dumps(response), encoding="utf-8")
    result = evaluate_episode(row)
    assert result["passed"] is False
    assert "text_gap_over_30_seconds" in result["failures"]
