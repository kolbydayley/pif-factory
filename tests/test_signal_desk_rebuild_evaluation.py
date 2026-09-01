from research_factory.signal_desk_rebuild_evaluation import diagnostic_pairs, evaluate_windows


def _event(*, speaker="A", issue="jobs", stance="warning", start=0, end=20):
    return {
        "speaker_id": speaker,
        "attribution_type": "direct_speech",
        "issue_label": issue,
        "stance": stance,
        "evidence_start": start,
        "evidence_end": end,
        "claim_text": "AI may change some jobs",
    }


def test_diagnostics_expose_field_errors_instead_of_hiding_them_as_missing_pairs():
    pairs = diagnostic_pairs(
        [_event(speaker="Alice", issue="AI employment")],
        [_event(speaker="Bob", issue="automation and work")],
        transcript_structure="speaker_turn",
    )
    assert len(pairs) == 1
    assert pairs[0]["field_agreement"]["speaker"] is False
    assert pairs[0]["field_agreement"]["issue"] is False


def test_aggregate_metrics_keep_strata_and_hashed_window_diagnostics():
    output = evaluate_windows(
        [
            {
                "window_id": "secret-window",
                "show_id": "show-a",
                "episode_id": "episode-a",
                "transcript_structure": "speaker_turn",
                "gold": {"events": [_event()]},
                "predicted": {"events": [_event()]},
            }
        ]
    )
    assert output["metrics"]["macro_composite"] == 1.0
    assert output["metrics"]["event_recall"] == 1.0
    assert output["strata"]["speaker_turn"]["counts"]["windows"] == 1
    assert output["per_window_scores"][0]["window_id_sha256"] != "secret-window"
