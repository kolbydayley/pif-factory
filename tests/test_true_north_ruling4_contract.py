from __future__ import annotations

import copy

from research_factory import true_north
from research_factory import true_north_ruling4_contract as contract


def _prior() -> dict:
    body = {
        "version": contract.SUPERSEDED_CONTRACT_VERSION,
        "contract_sha256": "p" * 64,
        "certification": {"scope": "development_fold_only"},
        "atomicity": {"gate": 0.9},
        "hallucination": {"threshold": 0.093684},
    }
    return body


def _calibration() -> dict:
    return {
        "path": "calibration.json",
        "sha256": "c" * 64,
        "calibration_sha256": "d" * 64,
        "measured_ceiling": 0.830664,
        "margin": 0.04,
        "threshold": 0.790664,
        "raw_disposition_exact_agreement": 0.807018,
        "value_state_exact_agreement": 0.963158,
        "original_aspiration": 0.9,
    }


def test_ruling4_policy_references_candidate_state_ceiling() -> None:
    policy = contract.gate_policy_document(
        calibration=_calibration(),
        previous_policy_sha256="p" * 64,
    )

    assert policy["rules"]["consensus_candidate_state_macro_f1"] == {
        "comparison": ">=",
        "threshold": 0.790664,
    }
    assert policy["candidate_state_macro_f1_gate"][
        "original_0_90_aspiration"
    ] == "mandatory_diagnostic"


def test_ruling4_contract_preserves_v7_and_delegation() -> None:
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
    assert result["previous_contract"]["version"] == (
        contract.SUPERSEDED_CONTRACT_VERSION
    )
    assert result["delegation"]["explicit"] is True
    assert result["candidate_state_macro_f1"]["gate"] == 0.790664
