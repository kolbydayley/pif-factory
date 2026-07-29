from research_factory import true_north


def _retained(candidates):
    return {
        row["candidate_id"]: {
            "candidate_id": row["candidate_id"],
            "disposition": "retain",
            "junk_reason": None,
        }
        for row in candidates
    }


def test_local_paraphrase_screen_uses_proximity_and_informative_stems():
    candidates = [
        {
            "candidate_id": "earlier",
            "segment_id": "segment",
            "evidence_start": 100,
            "evidence_end": 180,
            "claim_text": (
                "Sources describe alleged jailbreak capabilities."
            ),
            "evidence_text": (
                "Sources described the alleged jailbreak in circulation."
            ),
        },
        {
            "candidate_id": "restatement",
            "segment_id": "segment",
            "evidence_start": 306,
            "evidence_end": 390,
            "claim_text": (
                "The alleged jailbreak showed different capabilities."
            ),
            "evidence_text": (
                "The jailbreak allegation described those skills again."
            ),
        },
    ]
    composed = _retained(candidates)
    ensemble = (
        {key: dict(value) for key, value in composed.items()},
        {key: dict(value) for key, value in composed.items()},
    )

    screened = true_north.screen_phase_c_marginal_candidates(
        {"episode": candidates}, composed, ensemble
    )

    restatement = screened["restatement"]
    assert "local_paraphrase_repetition" in restatement[
        "screen_classes"
    ]
    local = restatement["local_proximity_neighbors"][0]
    assert local["candidate_id"] == "earlier"
    assert local["evidence_distance"] == 206
    assert set(local["shared_informative_stems"]).issuperset(
        {"alleg", "jailbreak"}
    )


def test_local_paraphrase_screen_respects_segment_and_distance():
    candidates = [
        {
            "candidate_id": "base",
            "segment_id": "segment-a",
            "evidence_start": 0,
            "evidence_end": 50,
            "claim_text": "Alleged jailbreak capabilities.",
            "evidence_text": "The alleged jailbreak was discussed.",
        },
        {
            "candidate_id": "too_far",
            "segment_id": "segment-a",
            "evidence_start": 601,
            "evidence_end": 660,
            "claim_text": "Alleged jailbreak details.",
            "evidence_text": "The alleged jailbreak appeared again.",
        },
        {
            "candidate_id": "other_segment",
            "segment_id": "segment-b",
            "evidence_start": 100,
            "evidence_end": 150,
            "claim_text": "Alleged jailbreak details.",
            "evidence_text": "The alleged jailbreak appeared elsewhere.",
        },
    ]
    composed = _retained(candidates)
    ensemble = (
        {key: dict(value) for key, value in composed.items()},
        {key: dict(value) for key, value in composed.items()},
    )

    screened = true_north.screen_phase_c_marginal_candidates(
        {"episode": candidates}, composed, ensemble
    )

    assert "local_paraphrase_repetition" not in screened[
        "too_far"
    ]["screen_classes"]
    assert "local_paraphrase_repetition" not in screened[
        "other_segment"
    ]["screen_classes"]


def test_packet_catalog_includes_low_jaccard_local_pair():
    segment_text = "a" * 200 + "local paraphrase" + "b" * 200
    candidate = {
        "candidate_id": "candidate",
        "segment_id": "segment",
        "claim_text": "A paraphrased repetition.",
        "evidence_text": "local paraphrase",
        "evidence_start": 200,
        "evidence_end": 216,
    }
    local_neighbor = {
        "candidate_id": "local",
        "segment_id": "segment",
        "claim_text": "Different wording.",
        "evidence_text": "Different local wording.",
    }
    screened = {
        "candidate": {
            "candidate_id": "candidate",
            "screen_classes": ["local_paraphrase_repetition"],
            "ensemble_rejectors": [],
            "top_neighbors": [],
            "local_proximity_neighbors": [
                {
                    "candidate_id": "local",
                    "similarity": 0.2,
                    "evidence_distance": 206,
                    "shared_informative_stems": [
                        "jailbreak",
                        "alleg",
                    ],
                }
            ],
        }
    }

    packet = true_north.build_phase_c_marginal_packet(
        suite="suite",
        candidate_ids=["candidate"],
        candidate_by_id={
            "candidate": candidate,
            "local": local_neighbor,
        },
        segment_by_id={
            "segment": {"segment_id": "segment", "text": segment_text}
        },
        screened=screened,
    )

    row = packet["input"]["candidates"][0]
    assert [neighbor["candidate_id"] for neighbor in row["neighbors"]] == [
        "local"
    ]
    assert packet["input"]["neighbor_catalog"][0][
        "candidate_id"
    ] == "local"
    context_id = row["segment_context"]["context_id"]
    assert packet["input"]["segment_context_catalog"][0][
        "context_id"
    ] == context_id
