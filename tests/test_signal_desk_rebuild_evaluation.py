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


def test_free_form_issue_proposals_are_diagnostic_not_extraction_quality():
    output = evaluate_windows(
        [
            {
                "window_id": "issue-alias",
                "show_id": "show-a",
                "episode_id": "episode-a",
                "transcript_structure": "speaker_turn",
                "gold": {"events": [_event(issue="AI impact on employment")]},
                "predicted": {"events": [_event(issue="automation and work")]},
            }
        ]
    )
    assert output["metrics"]["issue_proposal_agreement"] == 0.0
    assert output["metrics"]["macro_composite"] == 1.0


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


def test_selection_metrics_are_unweighted_show_macro_not_pooled_micro():
    perfect_events = [_event(start=index * 30, end=index * 30 + 20) for index in range(100)]
    output = evaluate_windows(
        [
            {
                "window_id": "large-show-window",
                "show_id": "large-show",
                "episode_id": "episode-a",
                "transcript_structure": "speaker_turn",
                "gold": {"events": perfect_events},
                "predicted": {"events": perfect_events},
            },
            {
                "window_id": "small-show-window",
                "show_id": "small-show",
                "episode_id": "episode-b",
                "transcript_structure": "speaker_turn",
                "gold": {"events": [_event()]},
                "predicted": {"events": []},
            },
        ]
    )
    assert output["aggregation"]["selection_metrics"] == "unweighted_show_macro"
    assert output["metrics"]["event_recall"] == 0.5
    assert output["micro_metrics"]["event_recall"] > 0.99
    assert output["per_show"]["large-show"]["metrics"]["event_recall"] == 1.0
    assert output["per_show"]["small-show"]["metrics"]["event_recall"] == 0.0


def test_no_consequential_claims_is_correct_empty_for_recall_and_density():
    output = evaluate_windows(
        [
            {
                "window_id": "empty",
                "show_id": "show-a",
                "episode_id": "episode-a",
                "transcript_structure": "speaker_turn",
                "gold": {"window_disposition": "no_consequential_claims", "events": []},
                "predicted": {"window_disposition": "no_consequential_claims", "events": []},
            }
        ]
    )
    assert output["metrics"]["event_recall"] == 1.0
    assert output["metrics"]["density_ratio"] == 1.0
    assert output["per_window_scores"][0]["correct_empty"] is True
