from research_factory.signal_desk_gold_disagreement import (
    build_batches,
    validate_decisions,
)


def test_batches_preserve_exact_case_ids_and_hash_input() -> None:
    cases = [
        {"case_id": "a", "window_id": "w", "kind": "paired", "fields": ["stance"], "gold": {"claim_text": "x"}, "audit": {"claim_text": "y"}},
        {"case_id": "b", "window_id": "w", "kind": "gold_unmatched", "fields": ["event_presence"], "gold": {"claim_text": "x"}, "audit": None},
    ]
    batches = build_batches(cases=cases, window_texts={"w": "frozen transcript"})
    assert len(batches) == 1
    assert [row["case_id"] for row in batches[0]["cases"]] == ["a", "b"]
    assert len(batches[0]["input_sha256"]) == 64


def test_decisions_require_exact_coverage_and_model_attestation() -> None:
    output = {
        "model": "gpt-5.5",
        "decisions": [{
            "case_id": "a", "decision": "both_supported", "rationale": "same supported proposition",
            "correction": None,
        }],
    }
    assert validate_decisions(output, ["a"])["a"]["decision"] == "both_supported"
