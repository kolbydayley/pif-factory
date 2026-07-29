from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_development_matrix as matrix
from research_factory import app_server_canonical_v31_episode_batch as adapter
from research_factory import app_server_canonical_v31_epoch7_input_package as inputs
from research_factory import app_server_expanded_cap_development_matrix as legacy


def _write(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _quality(text: str, index: int) -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": len(text.split()),
        "transcript_preparation_id": f"prep_epoch7_input_{index}",
    }


def _no_signal_label(*, episode_id: str, segment_id: str, quality: dict) -> dict:
    return {
        "schema_version": adapter.CANONICAL_LABEL_PACK,
        "segment_id": segment_id,
        "episode_id": episode_id,
        "extraction_status": "no_signal",
        "segment_quality": copy.deepcopy(quality),
        "segment_source_context": {
            "kind": "show_setup",
            "confidence": 1.0,
            "rationale": "This fixture contains only neutral show setup.",
        },
        "discourse_events": [],
        "concept_candidates": [],
        "rejected_candidates": [],
        "no_signal_reason": "No grounded canonical discourse event is present.",
        "overall_confidence": 1.0,
        "needs_review": False,
        "review_reason": None,
    }


def _capacity_policy() -> dict:
    return {
        "schema_version": matrix.CAPACITY_POLICY_VERSION,
        "state": "frozen_offline_policy",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "official_app_server_initialized_before_capacity": True,
        "admission_required_before_thread_start": True,
        "admission_required_before_thread_resume": True,
        "admission_required_before_turn_start": True,
        "admission_must_bind_future_plan_and_directive": True,
        "complete_usage_cache_reasoning_wall_required": True,
        "unknown_usage_hard_stop": True,
        "minimum_remaining_reserve_percent": 20,
        "capacity_safety_margin_percent": 10,
        "quota_points_per_million_tokens": 1_250,
        "quota_calibration_id": "canonical_v31_epoch7_input_fixture_calibration_v1",
        "quota_calibration_conservative_uplift_applied": True,
        "maximum_total_tokens_per_turn": 2_000,
        "maximum_wall_seconds_per_turn": 10.0,
        "maximum_rate_limit_snapshot_age_seconds": 30,
        "operator_wall_deadline_safety_margin_seconds": 1.0,
        "token_capacity_is_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_required": True,
        "postturn_measured_stop_required": True,
        "semantic_retry_count": 0,
        "required_arms": [
            {"batch_size": size, "thread_mode": mode}
            for size, mode in matrix.EXPECTED_ARMS
        ],
        "full_canonical_v31_outputs_required": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    complete_context: bool = True,
    valid_reference: bool = True,
) -> dict:
    monkeypatch.setattr(inputs, "PROJECT_ROOT", tmp_path)
    database_path = tmp_path / "factory.sqlite"
    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE fixture_guard(value INTEGER)")
    connection.execute("INSERT INTO fixture_guard VALUES (1)")
    connection.commit()
    connection.close()

    episode_id = "ep_epoch7_input_fixture"
    source_id = "source_epoch7_input_fixture"
    example = json.loads(
        adapter.PROJECT_ROOT.joinpath(
            "label_packs/ai_discourse_v3_1/examples.json"
        ).read_text()
    )[0]
    coded_text = example["input"]["text"]
    no_signal_text = "HOST: Welcome to this neutral fixture setup."
    texts = [coded_text, no_signal_text]
    segments = []
    labels = []
    rows = []
    for index, text in enumerate(texts):
        segment_id = f"seg_epoch7_input_{index}"
        quality = _quality(text, index)
        segment = {
            "segment_id": segment_id,
            "segment_index": index,
            "segment_text": text,
            "density_stratum": "dense" if index == 0 else "no_signal",
            "boundaries": [
                {
                    "window_id": 0,
                    "chunk_index": 0,
                    "owner_start": 0,
                    "owner_end": len(text),
                    "extract_start": 0,
                    "extract_end": len(text),
                }
            ],
        }
        segments.append(segment)
        if index == 0:
            label = copy.deepcopy(example["output"])
            label["segment_id"] = segment_id
            label["episode_id"] = episode_id
            label["segment_quality"] = copy.deepcopy(quality)
        else:
            label = _no_signal_label(
                episode_id=episode_id,
                segment_id=segment_id,
                quality=quality,
            )
        labels.append(label)
        rows.append(
            {
                "segment_id": segment_id,
                "episode_id": episode_id,
                "density_stratum": segment["density_stratum"],
                "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
        )
    if not valid_reference:
        labels[0]["schema_version"] = "drifted_reference_schema"

    context = {
        "schema_version": "ai_discourse_v3_1_episode_context",
        "episode_id": episode_id,
        "context_summary": "A checksum-bound development context fixture.",
        "speaker_map": [
            {"name": "HOST", "role": "host"},
            {"name": "GUEST", "role": "guest"},
        ],
        "section_map": [
            {
                "section_id": "fixture",
                "segment_ids": [segment["segment_id"] for segment in segments],
            }
        ],
        "entity_seed": {"organizations": ["Google"]},
        "concept_seed": ["frontier_ai_end_state_language"],
        "extraction_guidance": "Evaluate every canonical field from source evidence.",
        "excluded_source_context": ["neutral setup when unsupported"],
        "overall_confidence": 1.0,
        "needs_review": False,
        "review_reason": None,
        "quality_flags": [],
    }
    if not complete_context:
        del context["excluded_source_context"]
    context_path = tmp_path / "legacy-context.json"
    context_sha = _write(context_path, context)
    legacy_manifest = {
        "schema_version": "fixture_legacy_manifest",
        "episodes": [
            {
                "episode_id": episode_id,
                "episode_title": "Epoch 7 input fixture",
                "source_id": source_id,
                "source_name": "Epoch 7 input fixture source",
                "episode_context": {
                    "artifact_path": str(context_path),
                    "artifact_sha256": context_sha,
                    "run_id": "context_fixture_run",
                    "transcript_id": "transcript_fixture",
                },
                "segments": copy.deepcopy(rows),
            }
        ],
    }
    legacy_manifest_path = tmp_path / "legacy-manifest.json"
    legacy_manifest_sha = _write(legacy_manifest_path, legacy_manifest)
    prepared_episodes = [
        {
            "episode_id": episode_id,
            "episode_title": "Epoch 7 input fixture",
            "source_id": source_id,
            "source_name": "Epoch 7 input fixture source",
            "episode_context": copy.deepcopy(context),
            "segments": copy.deepcopy(segments),
        }
    ]
    legacy_info = {
        "path": str(legacy_manifest_path),
        "sha256": legacy_manifest_sha,
        "payload": legacy_manifest,
        "rows": copy.deepcopy(rows),
        "reference_by_segment": {
            row["segment_id"]: {
                "segment_id": row["segment_id"],
                "episode_id": episode_id,
                "golden_output": copy.deepcopy(label),
            }
            for row, label in zip(rows, labels)
        },
    }

    def fake_verify(path: Path, *, expected_sha256: str) -> dict:
        assert Path(path) == legacy_manifest_path
        assert expected_sha256 == legacy_manifest_sha
        return copy.deepcopy(legacy_info)

    def fake_prepare(
        connection: sqlite3.Connection,
        *,
        manifest: dict,
        manifest_rows: list,
    ) -> list[dict]:
        assert int(connection.execute("PRAGMA query_only").fetchone()[0]) == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO fixture_guard VALUES (2)")
        assert manifest == legacy_manifest
        assert manifest_rows == rows
        return copy.deepcopy(prepared_episodes)

    monkeypatch.setattr(legacy, "verify_development_manifest", fake_verify)
    monkeypatch.setattr(legacy, "prepare_development_episodes", fake_prepare)
    capacity_path = tmp_path / "capacity-policy.json"
    capacity_sha = _write(capacity_path, _capacity_policy())
    return {
        "root": tmp_path / "package",
        "database_path": database_path,
        "legacy_manifest_path": legacy_manifest_path,
        "legacy_manifest_sha": legacy_manifest_sha,
        "capacity_path": capacity_path,
        "capacity_sha": capacity_sha,
        "context_path": context_path,
        "context": context,
        "legacy_info": legacy_info,
    }


def _freeze(fixture: dict, tmp_path: Path) -> dict:
    return inputs.freeze_input_package(
        root=fixture["root"],
        database_path=fixture["database_path"],
        legacy_manifest_path=fixture["legacy_manifest_path"],
        legacy_manifest_sha256=fixture["legacy_manifest_sha"],
        capacity_policy_path=fixture["capacity_path"],
        capacity_policy_sha256=fixture["capacity_sha"],
        project_root=tmp_path,
    )


def test_ready_package_round_trips_through_canonical_matrix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)

    receipt = _freeze(fixture, tmp_path)

    assert receipt["state"] == "passed"
    assert receipt["episode_count"] == 1
    assert receipt["case_count"] == 2
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["operator_authorization_present"] is False
    assert receipt["executable_plan_created"] is False
    assert inputs.verify_input_package(
        fixture["root"], project_root=tmp_path
    ) == receipt
    preflight = json.loads(Path(receipt["dry_preflight"]["path"]).read_text())
    assert preflight["receipt"]["arm_count"] == 6
    assert preflight["receipt"]["case_count"] == 2


def test_missing_context_and_invalid_reference_freeze_sanitized_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(
        tmp_path,
        monkeypatch,
        complete_context=False,
        valid_reference=False,
    )

    receipt = _freeze(fixture, tmp_path)

    assert receipt["state"] == "waiting"
    assert receipt["blocking_condition_count"] == 2
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["canonical_manifest"] is None
    assert not (fixture["root"] / inputs.MANIFEST_FILENAME).exists()
    gap = json.loads(Path(receipt["input_gap"]["path"]).read_text())
    classes = [row["blocker_class"] for row in gap["blocking_conditions"]]
    assert classes == [
        "canonical_episode_context_authority_incomplete",
        "legacy_reference_not_current_canonical_v31",
    ]
    serialized = json.dumps(gap)
    assert "neutral fixture setup" not in serialized
    assert "frontier_ai_end_state_language" not in serialized


def test_context_artifact_drift_fails_before_creating_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    fixture["context_path"].write_text("{}\n")

    with pytest.raises(
        inputs.CanonicalV31Epoch7InputPackageError,
        match="context checksum drifted",
    ):
        _freeze(fixture, tmp_path)

    assert not fixture["root"].exists()


def test_passed_package_tamper_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    receipt = _freeze(fixture, tmp_path)
    preflight_path = Path(receipt["dry_preflight"]["path"])
    payload = json.loads(preflight_path.read_text())
    payload["case_count"] = 999
    preflight_path.write_text(json.dumps(payload, sort_keys=True) + "\n")

    with pytest.raises(
        inputs.CanonicalV31Epoch7InputPackageError,
        match="dry preflight record drifted",
    ):
        inputs.verify_input_package(fixture["root"], project_root=tmp_path)


def test_read_only_database_and_cli_status_are_zero_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    connection = inputs._open_read_only_database(fixture["database_path"])
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO fixture_guard VALUES (3)")
    finally:
        connection.close()
    receipt = _freeze(fixture, tmp_path)
    real_status = inputs.status_input_package
    monkeypatch.setattr(
        inputs,
        "status_input_package",
        lambda root: real_status(root, project_root=tmp_path),
    )

    assert inputs.main(["status", "--root", str(fixture["root"])]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status == {
        "blocking_condition_count": 0,
        "holdout_inspected": False,
        "production_mutated": False,
        "schema_version": inputs.RECEIPT_VERSION,
        "semantic_model_call_count": 0,
        "state": "passed",
        "terminal_reason": receipt["terminal_reason"],
    }
