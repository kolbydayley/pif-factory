from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_structure_audit as audit


def _source() -> dict:
    segment_text = "Revenue was $10. Costs fell."
    first_end = len("Revenue was $10.")
    return {
        "episode_id": "episode-a",
        "segments": [
            {
                "segment_id": "segment-a",
                "segment_text": segment_text,
                "boundaries": [
                    {
                        "window_id": 0,
                        "extract_start": 0,
                        "extract_end": len(segment_text),
                        "owner_start": 0,
                        "owner_end": len(segment_text),
                    }
                ],
                "units": [
                    {
                        "unit_id": "u1",
                        "window_id": 0,
                        "start_char": 0,
                        "end_char": first_end,
                        "text": "Revenue was $10.",
                    },
                    {
                        "unit_id": "u2",
                        "window_id": 0,
                        "start_char": first_end + 1,
                        "end_char": len(segment_text),
                        "text": "Costs fell.",
                    },
                ],
            }
        ],
    }


def _event(*, claim: str, start: str = "u1", direction: str = "increase") -> dict:
    return {
        "evidence_start_unit_id": start,
        "evidence_end_unit_id": start,
        "actor": "Company",
        "claim": claim,
        "metric_value": "",
        "metric_unit": "",
        "metric_comparator": "",
        "metric_raw_text": "",
        "metric_direction": direction,
    }


def _output() -> dict:
    return {
        "episode_id": "episode-a",
        "segments": [
            {
                "segment_id": "segment-a",
                "status": "coded",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "unit_receipts": [
                    {"unit_id": "u1", "eligible_event_count": 1, "unresolved_count": 1}
                ],
                "events": [
                    _event(claim="First"),
                    {
                        **_event(claim="Second", start="u2", direction="not_applicable"),
                        "metric_unit": "percent",
                    },
                ],
            }
        ],
    }


def _schema() -> dict:
    return {"type": "object"}


def test_audit_enumerates_independent_contract_defects() -> None:
    result = audit.audit_candidate_structure(
        source=_source(), output=_output(), schema=_schema()
    )

    assert result["structural_gate_passed"] is False
    segment = result["segment_audits"][0]
    assert segment["missing_receipt_count"] == 1
    assert segment["unresolved_receipt_count"] == 1
    assert segment["receipt_event_total_matches"] is False
    assert segment["ownership_mismatch_unit_count"] == 1
    assert result["totals"]["metric_applicability_error_count"] == 2
    assert result["totals"]["metric_literal_error_count"] == 1
    assert result["deterministic_semantic_decision_made"] is False
    assert result["candidate_fields_changed"] is False


def _write(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_frozen_audit_rejects_direct_input_mutation(tmp_path: Path) -> None:
    source = _write(tmp_path / "source.json", _source())
    output = _write(tmp_path / "output.json", _output())
    schema = _write(tmp_path / "schema.json", _schema())
    predecessor = _write(tmp_path / "predecessor.json", {"state": "frozen"})
    root = tmp_path / "audit"

    terminal = audit.freeze_structural_audit(
        root=root,
        source_path=source,
        output_path=output,
        schema_path=schema,
        direct_lineage_paths=[predecessor],
    )
    assert terminal["semantic_model_call_count"] == 0
    assert terminal["production_mutated"] is False
    assert audit.verify_frozen_audit(root) == terminal

    output.write_text(json.dumps({"changed": True}), encoding="utf-8")
    with pytest.raises(audit.CandidateStructureAuditError):
        audit.verify_frozen_audit(root)
