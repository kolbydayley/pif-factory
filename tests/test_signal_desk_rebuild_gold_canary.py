import sqlite3

from research_factory.signal_desk_rebuild_gold_canary import (
    GOLD_BUDGET_LANE,
    SYSTEM_PROMPT,
    _select,
    record_canary_usage,
)


def test_gold_system_prompt_contains_fail_closed_attribution_and_grounding_rules() -> None:
    folded = SYSTEM_PROMPT.casefold()
    assert "exact zero-based character offsets" in folded
    assert "never infer identity from show metadata" in folded
    assert "unresolved_speaker" in folded
    assert "never self-approve accepted evidence" in folded
    assert "transcript-surface" in folded
    assert "quoted_speech requires both" in folded
    assert "if the quoted person is not textually identifiable" in folded
    assert "merely discussed is not the speaker" in folded


def test_canary_selection_round_robins_structures() -> None:
    packets = tuple(
        {"input": {"transcript_structure": structure, "window_id": f"{structure}-{i}"}}
        for structure in ("flattened", "paragraph", "speaker_turn")
        for i in range(4)
    )
    selected = _select(packets)
    assert len(selected) == 10
    assert {row["input"]["transcript_structure"] for row in selected} == {
        "flattened", "paragraph", "speaker_turn"
    }


def test_canary_usage_is_idempotent_and_never_uses_gpt55_grant(tmp_path) -> None:
    database = tmp_path / "budget.sqlite"
    receipt = {
        "passed": True,
        "calls": 10,
        "total_tokens": 345403,
        "created_at": "2026-09-01T15:00:00Z",
        "receipt_sha256": "a" * 64,
        "model": "gpt-5.6-sol",
    }
    first = record_canary_usage(receipt, database)
    second = record_canary_usage(receipt, database)
    assert first["status"] == "recorded"
    assert second["status"] == "already_recorded"
    assert first["lane"] == GOLD_BUDGET_LANE
    assert first["gpt_5_5_rebuild_grant_used"] is False
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT COUNT(*) FROM pif_subscription_budget_ledger").fetchone()[0] == 1
