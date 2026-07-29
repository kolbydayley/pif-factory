from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import true_north_transfer as transfer


def test_episode_constants_are_distinct() -> None:
    assert transfer.AUTHORIZED_EPISODE_ID != transfer.FORBIDDEN_EPISODE_ID
    assert transfer.MAX_CALLS == 150
    assert transfer.MAX_TOKENS == 4_800_000


def test_compose_disposition_uses_value_or_and_resolves_hold(monkeypatch) -> None:
    candidate_a = {
        "candidate_id": "a",
        "segment_id": "s",
        "claim_text": "A is useful.",
        "evidence_text": "A is useful.",
        "evidence_start": 0,
        "evidence_end": 12,
    }
    candidate_b = {
        "candidate_id": "b",
        "segment_id": "s",
        "claim_text": "B is useful.",
        "evidence_text": "B is useful.",
        "evidence_start": 13,
        "evidence_end": 25,
    }
    job = {
        "input": {
            "episode": {"episode_id": transfer.AUTHORIZED_EPISODE_ID},
            "segment": {"segment_id": "s"},
            "candidates": [candidate_a, candidate_b],
        }
    }
    first = {
        "schema_version": "pif_true_north_multipass_v1",
        "items": [
            {"candidate_id": "a", "disposition": "reject", "junk_reason": "fragment"},
            {"candidate_id": "b", "disposition": "hold", "junk_reason": None},
        ],
    }
    second = {
        "schema_version": "pif_true_north_multipass_v1",
        "items": [
            {"candidate_id": "a", "disposition": "retain", "junk_reason": None},
            {"candidate_id": "b", "disposition": "hold", "junk_reason": None},
        ],
    }
    monkeypatch.setattr(
        transfer.true_north,
        "validate_multipass_disposition",
        lambda output, packet: None,
    )
    outputs, report = transfer._compose_disposition(
        {(transfer.AUTHORIZED_EPISODE_ID, "s"): job},
        {(transfer.AUTHORIZED_EPISODE_ID, "s"): first},
        {(transfer.AUTHORIZED_EPISODE_ID, "s"): second},
    )
    rows = {
        row["candidate_id"]: row
        for row in outputs[(transfer.AUTHORIZED_EPISODE_ID, "s")]["items"]
    }
    assert rows["a"]["disposition"] == "retain"
    assert rows["b"]["disposition"] == "retain"
    assert report["value_state_disagreement_count"] == 1


def test_verify_freeze_rejects_hash_drift(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(transfer, "_root", lambda value: tmp_path)
    run_root = tmp_path / "sealed-transfer" / "runs" / transfer.RUN_ID
    run_root.mkdir(parents=True)
    prediction = run_root / "prediction.json"
    prediction.write_text("{}", encoding="utf-8")
    freeze = {
        "episode_id": transfer.AUTHORIZED_EPISODE_ID,
        "prediction_path": str(prediction),
        "prediction_file_sha256": transfer.true_north._sha256_file(prediction),
    }
    freeze["freeze_sha256"] = transfer.true_north.sha256_text(
        transfer.true_north.dumps_json(freeze)
    )
    (run_root / "blind-freeze.json").write_text(
        json.dumps(freeze), encoding="utf-8"
    )
    prediction.write_text('{"drift":true}', encoding="utf-8")
    with pytest.raises(transfer.TransferError, match="prediction file hash"):
        transfer.verify_blind_freeze(suite_root=tmp_path)
