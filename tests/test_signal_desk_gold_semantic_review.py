import json

import pytest

from research_factory.signal_desk_gold_semantic_review import (
    build_review_batches, validate_atomicity_decision, validate_decisions,
)


def test_build_review_batches_binds_claims_to_frozen_development_text(tmp_path):
    transcript = tmp_path / "transcript.txt"
    transcript.write_text("Brands lack control over pricing.")
    text = transcript.read_text()
    manifest = {"windows": [{
        "window_id": "w1", "split": "development", "transcript_path": "transcript.txt",
        "start_char": 0, "end_char": len(text), "text_sha256": "",
        "transcript_structure": "speaker_turn",
    }]}
    # Use the exact sliced-text hash required by the frozen loader.
    import hashlib
    manifest["windows"][0]["text_sha256"] = hashlib.sha256(
        text.encode()
    ).hexdigest()
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
    event = {"claim_text": "Brands lack control", "evidence_text": "lack control",
             "evidence_start": 7, "evidence_end": 19, "speaker_id": "A",
             "attribution_type": "direct_speech", "stance": "neutral"}
    (root / "C" / "w1.json").write_text(json.dumps({"events": [event]}))
    (root / "AUDIT" / "w1.json").write_text(json.dumps({"events": [event]}))
    receipt = {"semantic_reversal_review": {"candidates": [{
        "candidate_id": "c1", "window_id": "w1", "gold_index": 0, "independent_index": 0,
    }]}}
    batches = build_review_batches(
        manifest=manifest, audit_receipt=receipt, result_root=root, project_root=tmp_path,
    )
    assert batches[0]["transcript_window"] == transcript.read_text()
    assert batches[0]["candidates"][0]["candidate_id"] == "c1"


def test_validate_decisions_requires_exact_gpt55_candidate_coverage():
    good = {"model": "gpt-5.5", "decisions": [
        {"candidate_id": "c1", "verdict": "equivalent", "rationale": "Same proposition."}
    ]}
    assert validate_decisions(good, expected_candidate_ids=["c1"]) == {"c1": "equivalent"}
    with pytest.raises(ValueError, match="exact candidate set"):
        validate_decisions(good, expected_candidate_ids=["c1", "c2"])


def test_atomicity_decision_allows_drops_only_for_gold_correction():
    correction = {"model": "gpt-5.5", "window_id": "w1",
                  "verdict": "gold_c_needs_correction", "rationale": "Duplicate claim.",
                  "gold_c_drop_indices": [3, 3, 4]}
    assert validate_atomicity_decision(correction, window_id="w1")["gold_c_drop_indices"] == [3, 4]
    correction["verdict"] = "valid_atomic_split"
    with pytest.raises(ValueError, match="drop Gold-C"):
        validate_atomicity_decision(correction, window_id="w1")
