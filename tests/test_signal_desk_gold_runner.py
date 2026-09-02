from research_factory.signal_desk_gold_runner import (
    RESERVE_TOKENS,
    SYSTEM_PROMPTS,
    archive_retryable_sidecar_for_retry,
    repair_unique_evidence_offsets,
)


def test_runner_reserves_above_measured_p90_and_keeps_turn_prompts_distinct():
    measured_p90 = {"A": 39288, "B": 40725.1, "C": 47078.8, "AUDIT": 39774.4}
    assert all(RESERVE_TOKENS[key] > value for key, value in measured_p90.items())
    assert set(SYSTEM_PROMPTS) == {"A", "B", "C", "AUDIT"}
    assert len(set(SYSTEM_PROMPTS.values())) == 4


def test_unique_exact_excerpt_repairs_offsets_without_changing_semantics():
    output = {
        "window_id": "w1",
        "events": [
            {
                "evidence_text": "unique excerpt",
                "evidence_start": 3,
                "evidence_end": 17,
                "claim_text": "unchanged",
            }
        ],
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="prefix unique excerpt suffix"
    )
    assert count == 1
    assert repaired["events"][0]["evidence_start"] == 7
    assert repaired["events"][0]["evidence_end"] == 21
    assert repaired["events"][0]["claim_text"] == "unchanged"
    assert output["events"][0]["evidence_start"] == 3


def test_ambiguous_excerpt_is_never_rebound():
    output = {
        "events": [
            {"evidence_text": "same", "evidence_start": 1, "evidence_end": 5}
        ]
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="same and same"
    )
    assert count == 0
    assert repaired == output


def test_declared_span_may_restore_source_whitespace_only():
    output = {
        "events": [
            {"evidence_text": "caption text", "evidence_start": 0, "evidence_end": 11}
        ]
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="captiontext"
    )
    assert count == 1
    assert repaired["events"][0]["evidence_text"] == "captiontext"

    changed, changed_count = repair_unique_evidence_offsets(
        {"events": [{"evidence_text": "caption best", "evidence_start": 0, "evidence_end": 11}]},
        transcript_window="captiontext",
    )
    assert changed_count == 0
    assert changed["events"][0]["evidence_text"] == "caption best"


def test_cancelled_sidecar_is_preserved_before_same_lineage_retry(tmp_path):
    sidecar = tmp_path / "sidecars" / "window.json"
    sidecar.parent.mkdir()
    sidecar.write_text('{"state":"cancelled","turn_id":"t1"}\n', encoding="utf-8")
    output = tmp_path / "results" / "window.json"

    archived = archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=output,
        recovery_root=tmp_path / "recovery",
        attempt_id=12,
        lease_generation=3,
    )

    assert archived == tmp_path / "recovery/window.attempt-12.generation-3.json"
    assert archived.read_text(encoding="utf-8").startswith('{"state":"cancelled"')
    assert not sidecar.exists()


def test_completed_sidecar_is_never_archived_for_retry(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text('{"state":"completed"}\n', encoding="utf-8")

    assert archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=1,
        lease_generation=1,
    ) is None
    assert sidecar.exists()


def test_allowlisted_provider_failure_is_preserved_for_retry(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text(
        '{"state":"failed","error_class":"turn_failed",'
        '"turn_error":{"codex_error_info":"serverOverloaded"}}\n',
        encoding="utf-8",
    )

    archived = archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=8,
        lease_generation=2,
    )

    assert archived is not None
    assert archived.exists()
    assert not sidecar.exists()


def test_unknown_provider_failure_remains_fail_closed(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text(
        '{"state":"failed","error_class":"turn_failed",'
        '"turn_error":{"codex_error_info":"unknown"}}\n',
        encoding="utf-8",
    )

    assert archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=8,
        lease_generation=2,
    ) is None
    assert sidecar.exists()
