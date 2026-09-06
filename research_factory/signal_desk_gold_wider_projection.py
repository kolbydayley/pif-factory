"""Apply immutable GPT-5.5 wider-speaker decisions to the V5 Gold-C projection.

This is a deterministic speaker-only projection.  Accepted decisions may restore
an event only from the frozen baseline C record, and may change only attribution
fields.  Indeterminable and terminal-failed events remain quarantined.
"""

from __future__ import annotations

import json
import sqlite3
import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_repair_v3 import MUTABLE_ATTRIBUTION_FIELDS
from .signal_desk_gold_repair_v4 import _digest_outputs, _sha_json
from .signal_desk_gold_repair_v5 import compare as compare_v5
from .signal_desk_gold_wider_speaker import EXPECTED_CALLS, MODEL, validate_decision
from .signal_desk_rebuild_contracts import validate_output
from .util import sha256_text


SCHEMA_VERSION = "pif_signal_desk_gold_wider_projection_v1"
VARIANT_ID = "gold-c-gpt55-wider-speaker-projection-v6"
PARENT_VARIANT_ID = "gold-c-full-source-provenance-v5"
FAILED_TASK_PREFIX = "gold-speaker-wide-v1:"


def build_resurrection_receipt(
    *, failed_window_id: str, private_root: Path, dispatch_path: Path,
) -> dict[str, Any]:
    """Decide whether the sole failed task earns one explicit resurrection.

    Mere occurrence of a host/guest name is deliberately insufficient.  The
    only acceptable new evidence is an exact transcript source label or an
    explicit first-person full-name identification.  The current failed packet
    contains neither, so this function records a no-call, fail-closed verdict.
    """

    packet_path = private_root / f"{failed_window_id}.packet.json"
    packet = json.loads(packet_path.read_text())
    context = str(packet["context"]["context_text"])
    explicit_labels = re.findall(
        r"(?m)(?:^|\n)\s*([A-Z][A-Za-z'’-]+(?:\s+[A-Z][A-Za-z'’-]+)+)\s*:\s*", context
    )
    self_identifications = re.findall(
        r"\b(?:[Mm]y name is|I am|I'm)\s+([A-Z][A-Za-z'’-]+\s+[A-Z][A-Za-z'’-]+)\b", context
    )
    dispatch = sqlite3.connect(dispatch_path)
    row = dispatch.execute(
        """SELECT t.id,t.status,a.id,a.attempt_number,a.status,a.semantic_failure_code,
                  a.semantic_failure_detail,a.resurrects_attempt_id
           FROM signal_desk_rebuild_tasks t
           JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id
           WHERE t.task_key=?""", (FAILED_TASK_PREFIX + failed_window_id,),
    ).fetchone()
    dispatch.close()
    if not row or row[1] != "terminal_failed" or row[4] != "terminal_failed":
        raise RuntimeError("failed speaker task is not terminally failed")
    proof_count = len(explicit_labels) + len(self_identifications)
    receipt = {
        "schema_version": "pif_signal_desk_gold_wider_speaker_resurrection_v1",
        "window_id": failed_window_id,
        "task_id": row[0], "terminal_attempt_id": row[2],
        "terminal_attempt_number": row[3], "terminal_status": row[4],
        "semantic_failure_code": row[5],
        "semantic_failure_detail_sha256": sha256_text(str(row[6] or "")),
        "resurrects_attempt_id": row[7],
        "packet_sha256": packet["packet_sha256"],
        "strengthened_contract": {
            "speaker_requires_exact_source_label_or_first_person_full_name_self_identification": True,
            "mere_mention_quote_address_or_outside_knowledge_forbidden": True,
            "identity_provenance_must_be_distinct_from_claim_evidence": True,
            "claim_or_semantic_rewrite_forbidden": True,
            "maximum_additional_provider_calls": 1,
        },
        "exact_source_label_proof_count": len(explicit_labels),
        "first_person_full_name_proof_count": len(self_identifications),
        "resurrection_authorized": proof_count > 0,
        "resurrection_performed": False,
        "additional_provider_calls_started": 0,
        "decision": "eligible_for_one_explicit_resurrection" if proof_count else "preserve_indeterminable_quarantine",
        "receipt_contains_transcript_claim_or_speaker_text": False,
    }
    receipt["receipt_sha256"] = _sha_json(receipt)
    return receipt


def _load_decisions(private_root: Path, dispatch_path: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    accepted: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    dispatch = sqlite3.connect(dispatch_path)
    for decision_path in sorted(private_root.glob("*.decision.json")):
        window_id = decision_path.name.removesuffix(".decision.json")
        packet_path = private_root / f"{window_id}.packet.json"
        packet = json.loads(packet_path.read_text())
        stored = json.loads(decision_path.read_text())
        sidecars = sorted(private_root.glob(f"{window_id}.attempt-*.generation-*.sidecar.json"))
        if len(sidecars) != 1:
            raise RuntimeError(f"expected one immutable sidecar for {window_id}")
        sidecar = json.loads(sidecars[0].read_text())
        task_key = f"gold-speaker-wide-v1:{window_id}"
        task_row = dispatch.execute(
            """SELECT t.status,a.output_json FROM signal_desk_rebuild_tasks t
               JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id
               WHERE t.task_key=?""", (task_key,),
        ).fetchone()
        completed = json.loads(task_row[1]) if task_row and task_row[1] else {}
        if (
            sidecar.get("model") != MODEL or not task_row or task_row[0] != "succeeded"
            or completed.get("decision_sha256") != sha256_text(decision_path.read_text())
        ):
            raise RuntimeError(f"decision sidecar binding mismatch: {window_id}")
        # Execution stores the already-validated normalized decision, which
        # intentionally omits the redundant model field. Re-attach only the
        # immutable sidecar's attested model to replay contract validation.
        decision = validate_decision({"model": MODEL, **stored}, packet)
        accepted[window_id] = decision
        counts = Counter(row["decision"] for row in decision["decisions"])
        rows.append({
            "window_id": window_id,
            "packet_sha256": packet["packet_sha256"],
            "decision_file_sha256": _sha_json(stored),
            "sidecar_sha256": _sha_json(sidecar),
            "supported_count": counts["supported_speaker"],
            "indeterminable_count": counts["indeterminable"],
        })
    dispatch.close()
    return accepted, rows


def build_plan(
    *, manifest_path: Path, baseline_root: Path, v5_root: Path,
    private_root: Path, dispatch_path: Path, output_root: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    selected = [str(row["window_id"]) for row in manifest["windows"] if row["split"] == "development"]
    accepted, rows = _load_decisions(private_root, dispatch_path)
    if len(rows) != EXPECTED_CALLS - 1:
        raise RuntimeError(f"expected exactly eight accepted immutable decisions, found {len(rows)}")
    failed = sorted(
        path.name.removesuffix(".packet.json")
        for path in private_root.glob("*.packet.json")
        if path.name.removesuffix(".packet.json") not in accepted
    )
    if len(failed) != 1:
        raise RuntimeError(f"expected exactly one fail-closed window, found {len(failed)}")
    plan = {
        "schema_version": SCHEMA_VERSION,
        "variant_id": VARIANT_ID,
        "parent_variant_id": PARENT_VARIANT_ID,
        "changed_dimension": "apply_digest_bound_gpt55_speaker_decisions_only",
        "selected_window_count": len(selected),
        "selected_window_ids": selected,
        "manifest_sha256": manifest["manifest_sha256"],
        "baseline_c_digest": _digest_outputs(baseline_root / "C", selected),
        "v5_c_digest": _digest_outputs(v5_root / "C", selected),
        "accepted_decision_count": len(rows),
        "accepted_decisions_digest": _sha_json(rows),
        "accepted_decisions": rows,
        "failed_window_ids_sha256": _sha_json(failed),
        "failed_window_count": len(failed),
        "provider_calls_started": False,
        "additional_provider_calls": 0,
        "one_change_invariant": {
            "semantic_fields_frozen": True,
            "supported_speakers_restore_baseline_event_only": True,
            "indeterminable_events_remain_quarantined": True,
            "terminal_failure_remains_quarantined": True,
            "sealed_items_opened": False,
        },
        "pass_criteria": {
            "structured_missing_speakers": 0,
            "event_recall_minimum": 0.99,
            "evidence_grounding": 1.0,
            "fabricated_or_unsupported_attribution": 0,
            "third_party_presented_as_own": 0,
            "macro_delta_vs_baseline_minimum": -0.02,
            "semantic_field_drift": 0,
        },
        "output_root": str(output_root),
    }
    plan["plan_sha256"] = _sha_json(plan)
    return plan


def project_window(
    *, baseline_c: Mapping[str, Any], v5_c: Mapping[str, Any],
    v5_provenance: Mapping[str, Any], decision: Mapping[str, Any] | None,
    transcript_window: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    output = json.loads(json.dumps(v5_c))
    original = {str(event["event_id"]): event for event in baseline_c["events"]}
    output_events = {str(event["event_id"]): event for event in output["events"]}
    unresolved = {str(row["event_id"]): row for row in v5_provenance["unresolved"]}
    restored: list[str] = []
    indeterminable: list[str] = []
    if decision is not None:
        for row in decision["decisions"]:
            event_id = str(row["event_id"])
            if event_id not in unresolved or unresolved[event_id]["event_sha256"] != _sha_json(original[event_id]):
                raise RuntimeError(f"decision is not bound to a V5 unresolved event: {event_id}")
            if row["decision"] == "indeterminable":
                indeterminable.append(event_id)
                continue
            event = json.loads(json.dumps(original[event_id]))
            event["speaker_id"] = row["speaker_surface"]
            if event.get("attribution_type") == "unresolved_speaker":
                event["attribution_type"] = "direct_speech"
            event["attribution_confidence"] = 1.0
            output_events[event_id] = event
            restored.append(event_id)
    order = {str(event["event_id"]): index for index, event in enumerate(baseline_c["events"])}
    output["events"] = sorted(output_events.values(), key=lambda event: order[str(event["event_id"])])
    if output["events"]:
        output["window_disposition"] = baseline_c["window_disposition"]
    validate_output(output, transcript_window=transcript_window, expected_window_id=str(output["window_id"]))
    residual = sorted(set(unresolved) - set(restored))
    receipt = {
        "window_id": str(output["window_id"]),
        "restored_count": len(restored),
        "restored_ids_sha256": _sha_json(sorted(restored)),
        "indeterminable_count": len(indeterminable),
        "residual_count": len(residual),
        # Compatibility with the shared comparison helper.
        "unresolved_event_count": len(residual),
        "residual_ids_sha256": _sha_json(residual),
        "decision_applied": decision is not None,
        "contains_claim_evidence_or_speaker_text": False,
    }
    receipt["receipt_sha256"] = _sha_json(receipt)
    return output, receipt


def execute(
    *, manifest_path: Path, project_root: Path, baseline_root: Path,
    v5_root: Path, private_root: Path, dispatch_path: Path, output_root: Path, plan: Mapping[str, Any],
) -> dict[str, Any]:
    selected = list(plan["selected_window_ids"])
    if _digest_outputs(baseline_root / "C", selected) != plan["baseline_c_digest"]:
        raise RuntimeError("frozen baseline C changed after planning")
    if _digest_outputs(v5_root / "C", selected) != plan["v5_c_digest"]:
        raise RuntimeError("V5 C changed after planning")
    decisions, rows = _load_decisions(private_root, dispatch_path)
    if _sha_json(rows) != plan["accepted_decisions_digest"]:
        raise RuntimeError("accepted decision set changed after planning")
    manifest = json.loads(manifest_path.read_text())
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    (output_root / "C").mkdir(parents=True, exist_ok=True)
    (output_root / "provenance").mkdir(parents=True, exist_ok=True)
    totals = Counter()
    residual_windows = []
    for window_id in selected:
        row = metadata[window_id]
        path = Path(str(row["transcript_path"])); path = path if path.is_absolute() else project_root / path
        text = path.read_text(encoding="utf-8")[int(row["start_char"]):int(row["end_char"])]
        baseline = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        v5 = json.loads((v5_root / "C" / f"{window_id}.json").read_text())
        provenance = json.loads((v5_root / "provenance" / f"{window_id}.json").read_text())
        output, receipt = project_window(
            baseline_c=baseline, v5_c=v5, v5_provenance=provenance,
            decision=decisions.get(window_id), transcript_window=text,
        )
        (output_root / "C" / f"{window_id}.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
        (output_root / "provenance" / f"{window_id}.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        totals.update(windows=1, restored=receipt["restored_count"],
                      indeterminable=receipt["indeterminable_count"], residual=receipt["residual_count"])
        if receipt["residual_count"]:
            residual_windows.append({
                "window_id": window_id, "show_id": str(row["show_id"]),
                "transcript_structure": str(row["transcript_structure"]),
                "residual_count": receipt["residual_count"],
                "reason": "gpt55_indeterminable" if window_id in decisions else "terminal_semantic_failure",
            })
    run_receipt = {
        "schema_version": "pif_signal_desk_gold_wider_projection_run_v1",
        "variant_id": VARIANT_ID, "complete": totals["windows"] == len(selected),
        "provider_calls_started": 0, "totals": dict(totals),
        "residual_windows": residual_windows,
        "residual_windows_digest": _sha_json(residual_windows),
        "receipt_contains_claim_evidence_or_speaker_text": False,
        "plan_sha256": plan["plan_sha256"],
    }
    run_receipt["receipt_sha256"] = _sha_json(run_receipt)
    return run_receipt


def compare(**kwargs: Any) -> dict[str, Any]:
    receipt = compare_v5(**kwargs)
    receipt["schema_version"] = "pif_signal_desk_gold_wider_projection_comparison_v1"
    receipt["variant_id"] = VARIANT_ID
    receipt["attribution_gate"]["context_scope"] = "digest_bound_gpt55_wider_speaker_only"
    receipt["receipt_sha256"] = _sha_json({k: v for k, v in receipt.items() if k != "receipt_sha256"})
    return receipt
