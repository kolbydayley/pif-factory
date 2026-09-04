import json
import hashlib
import copy

import pytest

from research_factory.signal_desk_gold_audit import (
    GoldAuditError,
    evaluate_dev_audit,
    select_dev_audit_windows,
)
from research_factory.signal_desk_rebuild_contracts import SCHEMA_VERSION


SOURCE_TEXT = "Host: AI changes jobs."
EVIDENCE_TEXT = "AI changes jobs."
EVIDENCE_START = SOURCE_TEXT.index(EVIDENCE_TEXT)
EVIDENCE_END = EVIDENCE_START + len(EVIDENCE_TEXT)


def _output(window_id, speaker="A", *, events=None):
    return {
        "schema_version": SCHEMA_VERSION,
        "window_id": window_id,
        "window_disposition": "claims_found",
        "events": [
            {
                "event_id": f"{window_id}-event-1",
                "claim_text": "AI changes jobs",
                "speech_act": "forecast",
                "evidence_text": EVIDENCE_TEXT,
                "evidence_start": EVIDENCE_START,
                "evidence_end": EVIDENCE_END,
                "speaker_id": speaker,
                "quoted_person_id": None,
                "mentioned_person_ids": [],
                "attribution_type": "direct_speech",
                "attribution_confidence": 0.9,
                "issue_label": "jobs",
                "issue_aliases": [],
                "stance": "warning",
                "publishability_state": "candidate",
            }
        ] if events is None else events,
    }


def _manifest(tmp_path, *, split="development"):
    transcript_path = tmp_path / "corpus/transcripts/w1.txt"
    transcript_path.parent.mkdir(parents=True)
    transcript_path.write_text(SOURCE_TEXT, encoding="utf-8")
    return {
        "manifest_sha256": "a" * 64,
        "windows": [
            {
                "window_id": "w1",
                "show_id": "s1",
                "episode_id": "e1",
                "split": split,
                "transcript_structure": "speaker_turn",
                "transcript_path": "corpus/transcripts/w1.txt",
                "transcript_sha256": hashlib.sha256(SOURCE_TEXT.encode()).hexdigest(),
                "start_char": 0,
                "end_char": len(SOURCE_TEXT),
                "text_sha256": hashlib.sha256(SOURCE_TEXT.encode()).hexdigest(),
            }
        ],
    }


def test_dev_audit_passes_identical_outputs(tmp_path):
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
        (root / turn / "w1.json").write_text(json.dumps(_output("w1")))
    receipt = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1,
        agreement_minimum=0.0, critical_error_maximum=1.0,
        project_root=tmp_path,
    )
    assert receipt["passed"] is True
    assert receipt["agreement"]["point"] == 1.0
    assert receipt["source_validation"]["validated_windows"] == 1
    assert receipt["item_outputs_exposed"] is False


def test_dev_audit_fails_closed_when_incomplete_or_critical(tmp_path):
    manifest = _manifest(tmp_path)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    (root / "C").mkdir(parents=True)
    (root / "C" / "w1.json").write_text(json.dumps(_output("w1")))
    with pytest.raises(GoldAuditError, match="incomplete"):
        evaluate_dev_audit(
            manifest_path=path, result_root=root, expected_windows=1,
            initial_windows=1, minimum_events=1,
            agreement_minimum=0.0, critical_error_maximum=0.5,
            project_root=tmp_path,
        )
    (root / "AUDIT").mkdir()
    (root / "AUDIT" / "w1.json").write_text(json.dumps(_output("w1", speaker="B")))
    receipt = evaluate_dev_audit(
        manifest_path=path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1,
        agreement_minimum=0.0, critical_error_maximum=0.5,
        project_root=tmp_path,
    )
    assert receipt["passed"] is False
    assert receipt["critical_errors"]["errors"] == 1
    assert receipt["catastrophic_windows"] == 0


def test_dev_audit_expands_in_40_window_blocks_to_event_floor():
    manifest = {
        "windows": [
            {"window_id": f"w{i:03d}", "split": "development"}
            for i in range(100)
        ]
    }
    counts = {f"w{i:03d}": 20 for i in range(100)}
    selected = select_dev_audit_windows(
        manifest,
        counts,
        initial_window_ids=[f"w{i:03d}" for i in range(19)],
    )
    assert selected["initial_windows"] == 19
    assert selected["expanded_windows"] == 59
    assert selected["expansion_blocks"] == 1
    assert selected["event_denominator"] == 1180
    assert selected["decision_ready"] is True


def test_dev_audit_fails_catastrophically_on_fabricated_evidence(tmp_path):
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
    (root / "C" / "w1.json").write_text(json.dumps(_output("w1")))
    fabricated = _output("w1")
    fabricated["events"][0]["evidence_text"] = "Not in the frozen source."
    (root / "AUDIT" / "w1.json").write_text(json.dumps(fabricated))
    receipt = evaluate_dev_audit(
        manifest_path=manifest_path,
        result_root=root,
        expected_windows=1,
        initial_windows=1,
        minimum_events=1,
        agreement_minimum=0.0,
        critical_error_maximum=1.0,
        project_root=tmp_path,
    )
    assert receipt["passed"] is False
    assert receipt["catastrophic_windows"] == 1
    assert receipt["catastrophic"]["reason_counts"]["independent_fabricated_or_unusable_evidence"] == 1


def test_dev_audit_flags_same_span_claim_splitting_for_readjudication(tmp_path):
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    events = []
    for index in range(4):
        event = copy.deepcopy(_output("w1")["events"][0])
        event["event_id"] = f"w1-event-{index}"
        event["claim_text"] = f"AI changes jobs consequence {index}"
        events.append(event)
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
        (root / turn / "w1.json").write_text(json.dumps(_output("w1", events=events)))
    receipt = evaluate_dev_audit(
        manifest_path=manifest_path,
        result_root=root,
        expected_windows=1,
        initial_windows=1,
        minimum_events=1,
        agreement_minimum=0.0,
        critical_error_maximum=1.0,
        project_root=tmp_path,
    )
    assert receipt["status"] == "requires_readjudication"
    assert receipt["over_splitting"]["requires_readjudication"] is True
    assert receipt["over_splitting"]["flagged_windows"][0]["gold_c_max_same_span"] == 4

    resolved = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1, agreement_minimum=0.0,
        critical_error_maximum=1.0, project_root=tmp_path,
        atomicity_adjudications={"w1": "valid_atomic_split"},
    )
    assert resolved["over_splitting"]["requires_readjudication"] is False
    assert resolved["over_splitting"]["resolved_windows"][0]["verdict"] == "valid_atomic_split"


def test_negation_asymmetry_requires_semantic_review_instead_of_becoming_catastrophic(tmp_path):
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
    gold = _output("w1")
    gold["events"][0]["claim_text"] = "Brands lack control over pricing"
    independent = _output("w1")
    independent["events"][0]["claim_text"] = "Brands do not control pricing"
    (root / "C" / "w1.json").write_text(json.dumps(gold))
    (root / "AUDIT" / "w1.json").write_text(json.dumps(independent))

    receipt = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1, agreement_minimum=0.0,
        critical_error_maximum=1.0, project_root=tmp_path,
    )
    assert receipt["catastrophic_windows"] == 0
    assert receipt["status"] == "requires_semantic_review"
    review = receipt["semantic_reversal_review"]
    assert review["candidate_count"] == 1 and review["unresolved_count"] == 1

    candidate_id = review["candidates"][0]["candidate_id"]
    resolved = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1, agreement_minimum=0.0,
        critical_error_maximum=1.0, project_root=tmp_path,
        semantic_reversal_adjudications={candidate_id: "equivalent"},
    )
    assert resolved["semantic_reversal_review"]["complete"] is True
    assert resolved["catastrophic_windows"] == 0


def test_only_adjudicated_reversal_is_catastrophic(tmp_path):
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = tmp_path / "results"
    for turn in ("C", "AUDIT"):
        (root / turn).mkdir(parents=True)
    gold = _output("w1")
    gold["events"][0]["claim_text"] = "The system will not fail"
    independent = _output("w1")
    independent["events"][0]["claim_text"] = "The system will fail"
    (root / "C" / "w1.json").write_text(json.dumps(gold))
    (root / "AUDIT" / "w1.json").write_text(json.dumps(independent))
    first = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1, agreement_minimum=0.0,
        critical_error_maximum=1.0, project_root=tmp_path,
    )
    candidate_id = first["semantic_reversal_review"]["candidates"][0]["candidate_id"]
    resolved = evaluate_dev_audit(
        manifest_path=manifest_path, result_root=root, expected_windows=1,
        initial_windows=1, minimum_events=1, agreement_minimum=0.0,
        critical_error_maximum=1.0, project_root=tmp_path,
        semantic_reversal_adjudications={candidate_id: "reversed_meaning"},
    )
    assert resolved["catastrophic_windows"] == 1
    assert resolved["catastrophic"]["reason_counts"] == {"adjudicated_reversed_meaning": 1}
