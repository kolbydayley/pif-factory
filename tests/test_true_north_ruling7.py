from research_factory import true_north_ruling7 as ruling7


def test_ruling7_result_is_glm_only_and_preserves_seven_gates() -> None:
    result = ruling7.compose_ruling7_glm_only(
        suite_root=(
            "/Users/kolbydayley/Library/Application Support/"
            "Podcast Intelligence Factory/true-north/ai-safety-v1"
        )
    )

    assert result["selected_lane"] == ruling7.LANE_ID
    assert result["passed_gate_count"] == 7
    accounting = result["runtime_lane_accounting_syntax_episode"]
    assert accounting["glm_call_floor"] == 56
    assert accounting["codex_lane_calls"] == 0
    assert accounting["codex_call_reduction"] == 1.0
    assert result["provider_calls"] == 0
    assert result["holdout_opened"] is False
    failed = {
        row["metric"]
        for row in result["nine_gate_table"]
        if not row["passed"]
    }
    assert failed == {
        "acceptable_atomic_count_rate",
        "claim_text_faithfulness_proxy",
    }
