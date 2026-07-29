from __future__ import annotations

import json
import sqlite3

from research_factory import true_north
from research_factory import true_north_option2 as option2


def _documents(reason: str):
    consensus = {
        "items": [
            {
                "candidate_id": "value",
                "strictly_scoreable": True,
                "consensus_state": "consensus_value",
            },
            {
                "candidate_id": "junk",
                "strictly_scoreable": True,
                "consensus_state": "consensus_junk",
            },
        ]
    }
    gold = {
        "items": [
            {
                "candidate_id": "value",
                "disposition": "retain",
                "reason_code": "supported",
            },
            {
                "candidate_id": "junk",
                "disposition": "reject",
                "reason_code": reason,
            },
        ]
    }
    predictions = {
        "value": {
            "candidate_id": "value",
            "disposition": "retain",
        },
        "junk": {
            "candidate_id": "junk",
            "disposition": "retain",
        },
    }
    return consensus, gold, predictions


def test_relational_escape_moves_out_of_disposition_gate_only():
    consensus, gold, predictions = _documents(
        "non_useful_repetition_of_definition"
    )

    result = true_north._score_phase_c_dispositions(
        consensus, predictions, gold
    )

    assert result["passed"] is True
    assert result["intrinsic_junk_escape_count"] == 0
    assert result["relational_junk_escape_count"] == 1
    assert result["all_junk_escape_count"] == 1
    assert result["measurement_contract"]["old_gate"]["passed"] is False


def test_intrinsic_escape_still_blocks_disposition_gate():
    consensus, gold, predictions = _documents(
        "bare_policy_artifact_mention"
    )

    result = true_north._score_phase_c_dispositions(
        consensus, predictions, gold
    )

    assert result["passed"] is False
    assert result["intrinsic_junk_escape_count"] == 1
    assert result["relational_junk_escape_count"] == 0


def test_held_candidates_are_symmetric_across_junk_and_value_metrics():
    consensus, gold, predictions = _documents(
        "nonasserted_question_frame"
    )

    held_junk = true_north._score_phase_c_dispositions(
        consensus,
        predictions,
        gold,
        held_candidate_ids=["junk"],
    )
    held_value = true_north._score_phase_c_dispositions(
        consensus,
        predictions,
        gold,
        held_candidate_ids=["value"],
    )

    assert held_junk["all_junk_escape_count"] == 0
    assert held_junk["held_gold_junk_count"] == 1
    assert held_value["retained_value_recall"] == 0.0
    assert held_value["false_reject_count"] == 1
    assert held_value["held_gold_value_count"] == 1


def test_bracket_link_chrome_rule_is_narrow_and_model_free():
    predictions = {
        "chrome": {"candidate_id": "chrome", "disposition": "retain"},
        "assertion": {
            "candidate_id": "assertion",
            "disposition": "retain",
        },
    }
    candidates = {
        "chrome": {
            "candidate_id": "chrome",
            "claim_text": "The speaker references a current request.",
            "evidence_text": "The agency has a [request] out right now",
        },
        "assertion": {
            "candidate_id": "assertion",
            "claim_text": "The executive called an official about concerns.",
            "evidence_text": (
                "The executive called [Secretary] Jones and discussed "
                "the concerns."
            ),
        },
    }

    result = true_north.apply_phase_c_intrinsic_composition_rules(
        predictions, candidates
    )

    assert result["flipped_candidate_ids"] == ["chrome"]
    assert result["model_calls_made"] == 0
    assert result["predictions"]["chrome"]["disposition"] == "reject"
    assert result["predictions"]["assertion"]["disposition"] == "retain"


def test_measurement_contract_hash_is_stable_and_auditable():
    first = option2.measurement_contract()
    second = option2.measurement_contract()

    assert first == second
    body = dict(first)
    expected = body.pop("contract_sha256")
    assert expected == true_north.sha256_text(
        true_north.dumps_json(body)
    )
    assert first["old_gate"]["scope"] == "all_gold_junk"
    assert first["new_gate"]["scope"] == [
        "chrome",
        "bare_mention",
        "fragment",
    ]
    assert first["evidence_trail"]["offline_parameter_sweep"][
        "parameterizations"
    ] == 36
    assert first["held_item_accounting"]["value_effect"].startswith(
        "exclude from the retained-value recall numerator"
    )


def test_manifest_contract_is_versioned_and_idempotent(
    tmp_path, monkeypatch
):
    suite_root = tmp_path / true_north.SUITE_ID
    suite_root.mkdir(parents=True)
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
    base = {
        "schema_version": "test",
        "suite_id": true_north.SUITE_ID,
        "shadow_database": str(shadow),
        "source_database": {"path": "source", "sha256": "a" * 64},
        "bundles": [],
        "frozen_interfaces": {},
    }
    base["manifest_sha256"] = true_north.sha256_text(
        true_north.dumps_json(base)
    )
    conn.execute(
        "INSERT INTO true_north_suites VALUES (?, ?)",
        (true_north.SUITE_ID, base["manifest_sha256"]),
    )
    conn.commit()
    conn.close()
    (suite_root / "manifest.json").write_text(
        json.dumps(base), encoding="utf-8"
    )
    monkeypatch.setattr(
        option2,
        "verify_suite",
        lambda **_: {"ok": True, "errors": []},
    )
    monkeypatch.setattr(
        option2, "_source_unchanged", lambda _: True
    )

    first = option2.apply_manifest_contract(
        output_root=tmp_path
    )
    second = option2.apply_manifest_contract(
        output_root=tmp_path
    )

    assert first["changed"] is True
    assert second["changed"] is False
    manifest = json.loads(
        (suite_root / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["measurement_contract"]["version"] == (
        option2.CONTRACT_VERSION
    )
    assert (
        suite_root
        / "manifest-history"
        / f"manifest-{first['old_manifest_sha256']}.json"
    ).is_file()
    conn = sqlite3.connect(shadow)
    stored = conn.execute(
        "SELECT manifest_sha256 FROM true_north_suites"
    ).fetchone()[0]
    conn.close()
    assert stored == manifest["manifest_sha256"]
