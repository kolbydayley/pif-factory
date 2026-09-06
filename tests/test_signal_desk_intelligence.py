from research_factory.signal_desk_intelligence import (
    SCHEMA_VERSION,
    build_payloads,
    canonical_person_name,
    classify_evidence,
    prepare_funnel,
)


def evidence(**overrides):
    row = {
        "id": "ev_1",
        "person": "Dario Amodei",
        "role": "person",
        "confidence": 0.91,
        "evidence": "We expect model capabilities to keep improving while deployment constraints remain significant.",
        "source_url": "https://example.com/episode",
        "episode_id": "ep_1",
        "episode": "Episode",
        "show": "Show One",
        "date": "2026-08-01",
        "month": "2026-08",
        "group": "positive",
        "stance": "supportive",
        "context_before": "Earlier context.",
        "context_after": "Later context.",
        "speaker_attribution": {
            "status": "direct", "confidence": 0.95,
            "basis": "episode_context", "role": "guest",
        },
    }
    row.update(overrides)
    return row


def test_third_person_reference_is_never_published_as_direct_speech():
    row = classify_evidence(evidence(
        evidence="The American AI CEO most hawkish on chip exports is Dario Amodei.",
    ))
    assert row["attribution_type"] == "mentioned_person"
    assert row["publishability"] != "accepted"
    assert "third_person_reference" in row["quality_reasons"]


def test_page_chrome_is_quarantined():
    row = classify_evidence(evidence(
        evidence="Recent episodes Subscribe Listen on Spotify YouTube RSS Feed Privacy Terms",
    ))
    assert row["publishability"] == "quarantined"
    assert "webpage_chrome" in row["quality_reasons"]


def test_grounded_direct_speech_can_be_accepted():
    row = classify_evidence(evidence())
    assert row["attribution_type"] == "direct_speech_verified"
    assert row["publishability"] == "accepted"
    assert row["quality_score"] > 0.7


def test_issue_source_excerpt_without_speaker_is_withheld():
    row = classify_evidence(evidence(
        role="source_excerpt", person="Unattributed voice",
        speaker_attribution={"status": "unresolved", "confidence": 0.9},
    ))
    assert row["attribution_type"] == "unresolved_voice"
    assert row["publishability"] != "accepted"
    assert "missing_speaker_assignment" in row["quality_reasons"]


def test_unverified_speaker_fails_closed():
    row = classify_evidence(evidence(speaker_attribution={
        "status": "unresolved", "confidence": 0,
        "basis": "missing_episode_context", "role": None,
    }))
    assert row["attribution_type"] == "unresolved_voice"
    assert row["publishability"] != "accepted"
    assert "speaker_not_verified" in row["quality_reasons"]


def test_known_person_aliases_are_canonicalized():
    assert canonical_person_name("Daniel Whitenack") == "Daniel Witenack"
    assert canonical_person_name("swyx") == "Shawn Wang"


def test_duplicate_feeds_become_one_canonical_show_with_alias():
    funnel = prepare_funnel({
        "shows": [
            {"name": "Decoder", "rss_url": "https://feeds.example.com/decoder",
             "catalogued_episodes": 9, "intelligence_ready_episodes": 9},
            {"name": "Decoder with Nilay Patel",
             "rss_url": "https://feeds.example.com/decoder",
             "catalogued_episodes": 965, "intelligence_ready_episodes": 147},
        ],
        "stages": [],
    })
    assert funnel["raw_show_count"] == 2
    assert funnel["canonical_show_count"] == 1
    assert len(funnel["duplicate_source_groups"]) == 1
    assert funnel["shows"][0]["name"] == "Decoder with Nilay Patel"
    assert funnel["shows"][0]["aliases"] == ["Decoder"]


def test_build_payloads_filters_operational_topics_and_cites_brief():
    evs = [
        evidence(id=f"ev_{i}", episode_id=f"ep_{i}",
                 show=f"Show {i}", person=f"Person {i}")
        for i in range(1, 4)
    ]
    data = {
        "generated_at": "2026-08-31T10:00:00",
        "data_through": "2026-08-31",
        "latest_episode": "2026-08-31",
        "corpus": {"shows": 3, "episodes": 3},
        "months": ["2026-07", "2026-08"],
        "month_totals": [100, 100],
        "detectors": {
            "emerging": [{"topic": "agent economics", "tier": "strong",
                          "pulse_vol": 12, "episodes": 3, "shows": 3}],
            "shifting": [], "contested": [], "fading": [],
        },
        "topics": {
            "agent economics": {
                "total": 12, "pulse_vol": 12, "pulse_episodes": 3,
                "pulse_shows": 3, "series": [], "aliases": ["agent costs"],
                "evidence": evs, "related": [],
            },
            "agent labor": {
                "total": 7, "pulse_vol": 7, "pulse_episodes": 3,
                "pulse_shows": 3, "series": [], "aliases": [],
                "evidence": [evidence(
                    id=f"watch_{i}", episode_id=f"watch_ep_{i}",
                    show=f"Watch Show {i}", role="source_excerpt",
                    person="Unattributed voice",
                    speaker_attribution={
                        "status": "unresolved", "confidence": 0.9,
                    },
                ) for i in range(1, 4)],
                "related": [],
            },
            "guest identity affiliation": {
                "total": 99, "pulse_vol": 99, "series": [], "evidence": [],
            },
        },
        "people": [],
        "network": {"nodes": [], "edges": []},
        "funnel": {"shows": [], "stages": []},
    }
    payloads = build_payloads(data)
    assert payloads["index"]["schema_version"] == SCHEMA_VERSION
    assert len(payloads["issues"]["issues"]) == 2
    assert [item["name"] for item in payloads["index"]["issues"]] == [
        "agent economics"
    ]
    assert {item["name"] for item in payloads["index"]["briefing"]} == {
        "agent economics"
    }
    brief = payloads["index"]["briefing"][0]["brief"]
    assert brief["decision_grade"] is True
    assert brief["what_changed"]["citations"]
    assert payloads["index"]["aliases"]["issues"]["agent costs"].startswith("issue_")
