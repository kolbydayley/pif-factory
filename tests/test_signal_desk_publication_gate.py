import json
from pathlib import Path

import pytest

from research_factory.signal_desk_intelligence import (
    build_payloads,
    classify_evidence,
    is_public_evidence,
    validate_public_payloads,
)


ROOT = Path(__file__).resolve().parents[1]


def _evidence(**overrides):
    row = {
        "id": "ev-1",
        "person": "Dario Amodei",
        "role": "person",
        "confidence": 0.95,
        "evidence": "Model deployment will require careful evaluation and meaningful safeguards.",
        "source_url": "https://example.com/episode",
        "episode_id": "ep-1",
        "episode": "Episode",
        "show": "Show",
        "date": "2026-08-01",
        "month": "2026-08",
        "group": "positive",
        "stance": "supportive",
        "speaker_attribution": {"status": "direct", "confidence": 0.95},
    }
    row.update(overrides)
    return row


def _data(evidence):
    return {
        "generated_at": "2026-08-31T10:00:00",
        "data_through": "2026-08-31",
        "latest_episode": "2026-08-31",
        "corpus": {"shows": 3, "episodes": 3},
        "months": ["2026-07", "2026-08"],
        "month_totals": [100, 100],
        "detectors": {"emerging": [], "shifting": [], "contested": [], "fading": []},
        "topics": {
            "model safety": {
                "total": 3, "pulse_vol": 3, "pulse_episodes": 3,
                "pulse_shows": 2, "series": [], "aliases": [],
                "evidence": evidence, "related": [],
            },
        },
        "people": [],
        "network": {"nodes": [], "edges": []},
        "funnel": {"shows": [], "stages": []},
    }


def test_unresolved_source_excerpt_is_not_public_evidence():
    row = classify_evidence(_evidence(
        role="source_excerpt",
        person="Unattributed voice",
        speaker_attribution={"status": "unresolved"},
    ))
    assert not is_public_evidence(row)
    assert row["attribution_type"] == "unresolved_voice"


def test_invalid_source_is_not_public_evidence():
    row = classify_evidence(_evidence(source_url="not-a-url"))
    assert not is_public_evidence(row)
    assert "missing_original_source" in row["quality_reasons"]


def test_build_payloads_strips_unresolved_and_third_party_rows():
    evidence = [
        _evidence(id="direct-1", episode_id="ep-1"),
        _evidence(id="unresolved-1", episode_id="ep-2", role="source_excerpt",
                  person="Unattributed voice",
                  speaker_attribution={"status": "unresolved"}),
        _evidence(id="mentioned-1", episode_id="ep-3",
                  speaker_attribution={"status": "mentioned"}),
        _evidence(id="invalid-source-1", episode_id="ep-4", source_url="bad"),
    ]
    payloads = build_payloads(_data(evidence))
    issue = next(iter(payloads["issues"]["issues"].values()))
    assert [row["id"] for row in issue["accepted_evidence"]] == ["direct-1"]
    assert issue["uncertain_evidence"] == []
    assert issue["withheld_evidence_count"] == 3
    assert all(is_public_evidence(row) for row in issue["evidence"])


def test_public_payload_validator_rejects_tampered_quarantine_row():
    payloads = build_payloads(_data([_evidence(id="direct-1")]))
    issue = next(iter(payloads["issues"]["issues"].values()))
    issue["uncertain_evidence"] = [_evidence(id="quarantined")]
    with pytest.raises(ValueError, match="uncertain_evidence"):
        validate_public_payloads(payloads)


def test_render_routes_have_no_quarantine_or_mention_fallbacks():
    js = (ROOT / "scripts/signal_desk_assets/signal-desk.js").read_text()
    assert "id=\"uncertain-toggle\"" not in js
    assert "issue.uncertain_evidence" not in js
    assert "person.mentions" not in js
    assert "filter(publicEvidence)" in js
