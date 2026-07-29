from __future__ import annotations

import copy
import json
import sqlite3

from research_factory import true_north
from research_factory import true_north_decoupled_contract as contract


def _calibration() -> dict:
    return {
        "calibration_sha256": "c" * 64,
        "ceilings": {
            "speaker_exactness": {
                "mean": 0.994615,
                "recommended_threshold": 0.954615,
            },
            "reported_actor_exactness": {
                "mean": 0.775385,
                "recommended_threshold": 0.735385,
            },
            "claim_text_faithfulness_proxy": {
                "mean": 0.784435,
                "recommended_threshold": 0.744435,
            },
        },
    }


def _previous_contract() -> dict:
    body = {
        "version": contract.SUPERSEDED_CONTRACT_VERSION,
        "decision": "option_2",
    }
    body["contract_sha256"] = true_north.sha256_text(
        true_north.dumps_json(body)
    )
    return body


def _atomic_ceiling() -> dict:
    return {
        "rule": "pass_c_count_within_pass_a_pass_b_inclusive_range",
        "accepted": 1132,
        "total": 1140,
        "rate": 0.992982,
        "gate": 0.90,
        "gate_fair": True,
        "reconciliation_document_sha256": "r" * 64,
    }


def test_artifact_formula_controls_thresholds() -> None:
    policy = contract.gate_policy_document(
        calibration_document=_calibration(),
        previous_policy_sha256="p" * 64,
    )

    assert policy["rules"]["speaker_exactness"]["threshold"] == 0.954615
    assert policy["rules"]["reported_actor_exactness"]["threshold"] == 0.735385
    assert policy["rules"]["claim_text_faithfulness_proxy"][
        "threshold"
    ] == 0.744435


def test_measurement_contract_is_hashed_and_inherits_option2() -> None:
    calibration = _calibration()
    policy = contract.gate_policy_document(
        calibration_document=calibration,
        previous_policy_sha256="p" * 64,
    )
    result = contract.measurement_contract(
        previous_contract=_previous_contract(),
        calibration_document=calibration,
        gate_policy=policy,
        atomic_ceiling=_atomic_ceiling(),
    )

    body = copy.deepcopy(result)
    digest = body.pop("contract_sha256")
    assert digest == true_north.sha256_text(
        true_north.dumps_json(body)
    )
    assert result["disposition_contract"]["version"] == (
        contract.SUPERSEDED_CONTRACT_VERSION
    )
    assert result["atomicity"]["gate"] == 0.90
    assert "speaker=0.954615" in result["semantic_field_scoring"][
        "arithmetic_correction"
    ]


def test_manifest_update_preserves_history_and_is_idempotent(
    tmp_path, monkeypatch
) -> None:
    suite_root = tmp_path / true_north.SUITE_ID
    suite_root.mkdir()
    shadow = suite_root / "shadow.sqlite"
    conn = sqlite3.connect(shadow)
    conn.execute(
        """
        CREATE TABLE true_north_suites (
          suite_id TEXT PRIMARY KEY,
          manifest_sha256 TEXT NOT NULL
        )
        """
    )
    previous = _previous_contract()
    manifest = {
        "schema_version": "fixture",
        "suite_id": true_north.SUITE_ID,
        "shadow_database": str(shadow),
        "source_database": {"path": "source", "sha256": "a" * 64},
        "bundles": [],
        "frozen_interfaces": {
            "gate_policy_version": "pif_true_north_gate_policy_v2",
            "gate_policy_sha256": "p" * 64,
        },
        "measurement_contract": previous,
    }
    manifest["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(manifest)
    )
    conn.execute(
        "INSERT INTO true_north_suites VALUES (?, ?)",
        (true_north.SUITE_ID, manifest["manifest_sha256"]),
    )
    conn.commit()
    conn.close()
    (suite_root / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    monkeypatch.setattr(
        contract, "_calibration_document", lambda _: _calibration()
    )
    monkeypatch.setattr(
        contract, "_atomic_range_ceiling", lambda _: _atomic_ceiling()
    )
    monkeypatch.setattr(
        true_north,
        "verify_suite",
        lambda **_: {"ok": True, "errors": []},
    )
    monkeypatch.setattr(
        true_north, "_source_unchanged", lambda _: True
    )

    first = contract.apply_manifest_contract(output_root=tmp_path)
    second = contract.apply_manifest_contract(output_root=tmp_path)

    assert first["changed"] is True
    assert second["changed"] is False
    updated = json.loads(
        (suite_root / "manifest.json").read_text(encoding="utf-8")
    )
    assert updated["measurement_contract"]["version"] == (
        contract.CONTRACT_VERSION
    )
    assert (
        suite_root
        / "manifest-history"
        / f"manifest-{first['old_manifest_sha256']}.json"
    ).is_file()
