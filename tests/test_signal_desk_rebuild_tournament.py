from __future__ import annotations

import sqlite3

import pytest

from research_factory.signal_desk_rebuild_tournament import (
    TournamentError,
    ensure_tournament_schema,
    evaluate_variant_promotion,
    open_sealed_holdout_once,
    record_score,
    register_variant,
)


BASE_REP = {
    "window_chars": 6000,
    "turn_aligned_overlap": False,
    "speaker_map_header": False,
}


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    ensure_tournament_schema(db)
    return db


def _register(conn, **overrides):
    values = {
        "variant_id": "base",
        "campaign_id": "campaign",
        "family_id": "prompt-family",
        "family_type": "prompt",
        "parent_variant_id": None,
        "round_number": 1,
        "hypothesis": "baseline",
        "changed_dimension": None,
        "model": "glm-5.2",
        "provider": "zai",
        "prompt": "extract supported consequential claims",
        "representation": BASE_REP,
        "scorer_version": "scorer-v1",
        "seed": "seed-1",
    }
    values.update(overrides)
    register_variant(conn, **values)


def test_prompt_child_changes_prompt_only(conn):
    _register(conn)
    _register(
        conn,
        variant_id="child",
        parent_variant_id="base",
        round_number=2,
        hypothesis="make attribution explicit",
        changed_dimension="attribution instruction",
        prompt="extract claims and distinguish speakers from mentioned people",
    )
    with pytest.raises(TournamentError, match="prompt child"):
        _register(
            conn,
            variant_id="bad",
            parent_variant_id="base",
            round_number=2,
            hypothesis="confounded",
            changed_dimension="two things",
            prompt="new prompt",
            representation={**BASE_REP, "window_chars": 8000},
        )


def test_representation_family_is_rounds_three_to_nine_and_prompt_frozen(conn):
    _register(
        conn,
        variant_id="rep-base",
        family_id="rep-family",
        family_type="representation",
        round_number=3,
    )
    _register(
        conn,
        variant_id="rep-8k",
        family_id="rep-family",
        family_type="representation",
        parent_variant_id="rep-base",
        round_number=3,
        hypothesis="long claims cross the baseline boundary",
        changed_dimension="window_chars",
        representation={**BASE_REP, "window_chars": 8000},
    )
    with pytest.raises(TournamentError, match="rounds 3-9"):
        _register(
            conn,
            variant_id="rep-late",
            family_id="rep-family",
            family_type="representation",
            parent_variant_id="rep-8k",
            round_number=10,
            hypothesis="late",
            changed_dimension="speaker_map_header",
            representation={
                **BASE_REP,
                "window_chars": 8000,
                "speaker_map_header": True,
            },
        )


def test_scorer_version_is_fenced(conn):
    _register(conn)
    record_score(
        conn,
        variant_id="base",
        split="development",
        scorer_version="scorer-v1",
        metrics={"recall": 0.8},
    )
    with pytest.raises(TournamentError, match="scorer change"):
        record_score(
            conn,
            variant_id="base",
            split="validation",
            scorer_version="scorer-v2",
            metrics={"recall": 0.9},
        )


def test_holdout_can_open_only_once(conn):
    _register(conn)
    open_sealed_holdout_once(
        conn,
        campaign_id="campaign",
        winner_variant_id="base",
        configuration={"prompt": "hash"},
    )
    with pytest.raises(TournamentError, match="already been opened"):
        open_sealed_holdout_once(
            conn,
            campaign_id="campaign",
            winner_variant_id="base",
            configuration={"prompt": "hash"},
        )


def _show_macro_score(value: float) -> dict:
    return {
        "aggregation": {"selection_metrics": "unweighted_show_macro"},
        "metrics": {"macro_composite": value},
        "per_show": {
            f"show-{index}": {
                "counts": {"gold_events": 50},
                "metrics": {"macro_composite": value},
            }
            for index in range(100)
        },
    }


def test_score_registry_persists_show_rows_and_selects_on_show_lcb(conn):
    _register(conn)
    _register(
        conn,
        variant_id="child",
        parent_variant_id="base",
        round_number=2,
        hypothesis="improve recall",
        changed_dimension="claim selection",
        prompt="extract only supported consequential claims with explicit boundaries",
    )
    record_score(
        conn,
        variant_id="base",
        split="validation",
        scorer_version="scorer-v1",
        metrics=_show_macro_score(0.70),
    )
    record_score(
        conn,
        variant_id="child",
        split="validation",
        scorer_version="scorer-v1",
        metrics=_show_macro_score(0.71),
    )
    assert conn.execute("SELECT COUNT(*) FROM signal_desk_rebuild_per_show_scores").fetchone()[0] == 200
    decision = evaluate_variant_promotion(
        conn,
        candidate_variant_id="child",
        parent_variant_id="base",
        scorer_version="scorer-v1",
    )
    assert decision["resampling_unit"] == "show"
    assert decision["candidate_show_macro_composite"]["lcb"] > decision["parent_show_macro_composite"]["lcb"]
    assert decision["powered_show_promotion"]["passed"] is True
    assert decision["passed"] is True


def test_holdout_score_rejects_per_show_rows(conn):
    _register(conn)
    with pytest.raises(TournamentError, match="aggregate-only"):
        record_score(
            conn,
            variant_id="base",
            split="sealed_holdout",
            scorer_version="scorer-v1",
            metrics=_show_macro_score(0.7),
        )
