from __future__ import annotations

from copy import deepcopy

from research_factory.signal_desk_gold_repair_v3 import project_window


def _event(*, speaker=None, attribution="unresolved_speaker", start=8, evidence="claim"):
    return {
        "event_id": "e1", "claim_text": "A consequential claim.", "speech_act": "assertion",
        "evidence_text": evidence, "evidence_start": start, "evidence_end": start + len(evidence),
        "speaker_id": speaker, "quoted_person_id": None, "mentioned_person_ids": [],
        "attribution_type": attribution, "attribution_confidence": 0.5,
        "issue_label": "Fixed issue", "issue_aliases": ["fixed alias"],
        "stance": "warning", "publishability_state": "candidate",
    }


def _output(event):
    return {"schema_version": "pif_signal_desk_clean_event_v2", "window_id": "w",
            "window_disposition": "claims_found", "events": [event]}


def test_bare_name_speaker_turn_repairs_only_attribution_fields() -> None:
    transcript = "Alice Smith\nclaim follows"
    event = _event(start=12, evidence="claim")
    baseline = _output(event)
    a = _output({**deepcopy(event), "speaker_id": "Alice Smith", "attribution_type": "direct_speech"})
    projected, receipt = project_window(
        metadata={"window_id": "w", "start_char": 0, "end_char": len(transcript),
                  "transcript_structure": "speaker_turn"},
        transcript=transcript, gold_a=a, gold_b=baseline, baseline_c=baseline,
    )
    repaired = projected["events"][0]
    assert repaired["speaker_id"] == "Alice Smith"
    assert repaired["attribution_type"] == "direct_speech"
    assert repaired["claim_text"] == event["claim_text"]
    assert repaired["issue_label"] == "Fixed issue"
    assert repaired["stance"] == "warning"
    assert receipt["speaker_assignments_added"] == 1


def test_inline_paragraph_label_repairs_cross_window_turn() -> None:
    transcript = "Alice Smith: prior words. " + ("x" * 20) + " claim follows"
    start = 25
    window = transcript[start:]
    event = _event(start=window.index("claim"), evidence="claim")
    baseline = _output(event)
    a = _output({**deepcopy(event), "speaker_id": "Alice Smith", "attribution_type": "direct_speech"})
    projected, _ = project_window(
        metadata={"window_id": "w", "start_char": start, "end_char": len(transcript),
                  "transcript_structure": "paragraph"},
        transcript=transcript, gold_a=a, gold_b=baseline, baseline_c=baseline,
    )
    assert projected["events"][0]["speaker_id"] == "Alice Smith"
    assert projected["events"][0]["evidence_text"] == "claim"


def test_unresolved_structured_event_is_quarantined_without_text() -> None:
    transcript = "unknown claim"
    event = _event(start=8, evidence="claim")
    projected, receipt = project_window(
        metadata={"window_id": "w", "start_char": 0, "end_char": len(transcript),
                  "transcript_structure": "paragraph"},
        transcript=transcript, gold_a=_output(event), gold_b=_output(event), baseline_c=_output(event),
    )
    assert projected["events"] == []
    assert projected["window_disposition"] == "no_consequential_claims"
    assert receipt["quarantined_event_count"] == 1
    assert receipt["contains_claim_or_evidence_text"] is False
    assert "claim" not in str(receipt["quarantined"]).casefold()


def test_third_party_identity_collision_is_quarantined() -> None:
    transcript = "Alice Smith\nclaim"
    event = _event(start=12, evidence="claim")
    event["mentioned_person_ids"] = ["Alice Smith"]
    a_event = {**deepcopy(event), "speaker_id": "Alice Smith", "attribution_type": "direct_speech"}
    projected, receipt = project_window(
        metadata={"window_id": "w", "start_char": 0, "end_char": len(transcript),
                  "transcript_structure": "speaker_turn"},
        transcript=transcript, gold_a=_output(a_event), gold_b=_output(event), baseline_c=_output(event),
    )
    assert projected["events"] == []
    assert receipt["quarantined_event_count"] == 1
