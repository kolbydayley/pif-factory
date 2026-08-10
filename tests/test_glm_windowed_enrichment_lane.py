from __future__ import annotations

from research_factory.glm_windowed_enrichment_lane import (
    WINDOW_CHARACTERS,
    WINDOW_OVERLAP_CHARACTERS,
    _is_overlap_duplicate,
    plan_segment_windows,
)


def test_window_plan_is_gapless_and_overlapping() -> None:
    windows = plan_segment_windows(5008)
    assert windows[0][0] == 0
    assert windows[-1][1] == 5008
    assert all(end - start <= WINDOW_CHARACTERS for start, end in windows)
    assert all(left[1] - right[0] == WINDOW_OVERLAP_CHARACTERS for left, right in zip(windows, windows[1:]))


def test_original_offset_overlap_duplicate() -> None:
    first = {"window_id": "w0", "event_type": "forecast", "evidence_start": 100, "evidence_end": 180, "claim_text": "A forecast."}
    second = {"window_id": "w1", "event_type": "forecast", "evidence_start": 110, "evidence_end": 175, "claim_text": "The same forecast."}
    assert _is_overlap_duplicate(second, first)
