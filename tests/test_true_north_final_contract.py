from __future__ import annotations

import copy

from research_factory import true_north
from research_factory import true_north_final_contract as contract


def _prior() -> dict:
    body = {
        "version": contract.SUPERSEDED_CONTRACT_VERSION,
        "owner_decision_id": "prior",
    }
    body["contract_sha256"] = true_north.sha256_text(
        true_north.dumps_json(body)
    )
    return body


def _calibration() -> dict:
    return {
        "path": "calibration.json",
        "sha256": "c" * 64,
        "matched_pair_gold_vs_gold_rate": 0.07368421052631578,
        "margin": 0.02,
        "threshold": 0.093684,
        "live_coupled_gold_vs_gold_rate": 0.1456140350877193,
        "original_aspirational_target": 0.02,
    }


def test_final_gate_policy_uses_lower_is_better_margin() -> None:
    policy = contract.gate_policy_document(
        calibration=_calibration(),
        previous_policy_sha256="p" * 64,
    )

    assert policy["rules"]["hallucination_rate_proxy"] == {
        "comparison": "<=",
        "threshold": 0.093684,
    }
    assert policy["hallucination_gate"][
        "original_0_02_aspiration"
    ] == "mandatory_diagnostic"


def test_final_contract_records_delegation_and_preserves_v6() -> None:
    policy = contract.gate_policy_document(
        calibration=_calibration(),
        previous_policy_sha256="p" * 64,
    )
    result = contract.measurement_contract(
        previous_contract=_prior(),
        calibration=_calibration(),
        gate_policy=policy,
    )

    body = copy.deepcopy(result)
    digest = body.pop("contract_sha256")
    assert digest == true_north.sha256_text(
        true_north.dumps_json(body)
    )
    assert result["delegation"]["explicit"] is True
    assert result["previous_contract"]["version"] == (
        contract.SUPERSEDED_CONTRACT_VERSION
    )
    assert result["atomicity"]["gate"] == 0.90
    assert result["certification"]["scope"] == "development_fold_only"

