from __future__ import annotations

from research_factory import true_north
from research_factory import true_north_decoupled_rescore as rescore


def _aggregate() -> dict:
    values = {
        metric: threshold
        for metric, (_comparison, threshold) in (
            true_north.APPROVED_GATE_POLICY.items()
        )
    }
    values["coupled_diagnostics"] = {
        "speaker_exactness": 0.5,
        "reported_actor_exactness": 0.4,
        "claim_text_faithfulness_proxy": 0.6,
    }
    values["hallucination_rate_proxy_coupled_diagnostic"] = 0.2
    return values


def test_gate_table_uses_matched_results_and_discloses_coupled() -> None:
    table = {
        row["metric"]: row for row in rescore._gate_table(_aggregate())
    }

    assert table["speaker_exactness"]["passed"] is True
    assert table["speaker_exactness"]["coupled_diagnostic"] == 0.5
    assert table["acceptable_atomic_count_rate"][
        "coupled_diagnostic"
    ] is None
    assert table["hallucination_rate_proxy"][
        "coupled_diagnostic"
    ] == 0.2
    assert len(table) == 9


def test_actor_span_baseline_only_changes_reported_actor() -> None:
    prediction = {
        "candidate_id": "candidate",
        "disposition": "retain",
        "atomic_claims": [
            {
                "claim_text": "The lab changed policy.",
                "raw_speaker": "Host",
                "reported_actor": "Missing Lab",
                "evidence_text": "A lab changed policy.",
            }
        ],
    }

    output, report = rescore._apply_actor_span([prediction])

    assert output[0]["atomic_claims"][0]["reported_actor"] is None
    assert output[0]["atomic_claims"][0]["claim_text"] == (
        prediction["atomic_claims"][0]["claim_text"]
    )
    assert prediction["atomic_claims"][0][
        "reported_actor"
    ] == "Missing Lab"
    assert report["nulled_absent_span"] == 1
