from research_factory.signal_desk_gold_measurement import (
    AUDIT_SYSTEM_PROMPT, B_SYSTEM_PROMPT, C_SYSTEM_PROMPT, _p90, _turn_metrics,
)


def test_turn_prompts_preserve_independence_and_transcript_authority():
    assert "independent Gold B" in B_SYSTEM_PROMPT
    assert "must not assume or imitate" in B_SYSTEM_PROMPT
    assert "transcript remains the only semantic authority" in C_SYSTEM_PROMPT
    assert "Do not defer to Gold A, B, or C" in AUDIT_SYSTEM_PROMPT


def test_p90_and_outlier_decomposition():
    rows = []
    for i, total in enumerate(range(10, 110, 10)):
        rows.append({"total_tokens": total, "input_tokens": total - 3,
                     "output_tokens": 2, "reasoning_output_tokens": 1,
                     "window_id_sha256": str(i), "transcript_structure": "flattened",
                     "disposition": "claims_found", "events": i})
    assert _p90([row["total_tokens"] for row in rows]) == 91.0
    metrics = _turn_metrics(rows)
    assert metrics["mean_tokens"] == 55.0
    assert metrics["outlier"]["events"] == 9
    assert round(metrics["outlier"]["input_share"], 2) == 0.97
