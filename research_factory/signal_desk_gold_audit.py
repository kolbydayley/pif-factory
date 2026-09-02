"""Aggregate-only development reliability audit for adjudicated Gold C."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_atomicity import same_span_counts
from .signal_desk_rebuild_contracts import EvidenceContractError, validate_output
from .signal_desk_rebuild_evaluation import diagnostic_pairs, evaluate_windows
from .signal_desk_rebuild_gates import evaluate_rate_gate
from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_dev_gold_audit_v2"
AGREEMENT_MINIMUM = 0.95
CRITICAL_ERROR_MAXIMUM = 0.01
MINIMUM_CONSEQUENTIAL_EVENTS = 1_000
EXPANSION_BLOCK_WINDOWS = 40


class GoldAuditError(RuntimeError):
    pass


_NEGATION_RE = re.compile(
    r"\b(?:not|never|no|cannot|can't|won't|isn't|aren't|doesn't|don't|didn't)\b",
    re.I,
)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _event_count(output: Mapping[str, Any], *, window_id: str) -> int:
    events = output.get("events")
    if not isinstance(events, list):
        raise GoldAuditError(f"Gold C output has no event array for {window_id}")
    return len(events)


def _load_frozen_window_text(
    metadata: Mapping[str, Any], *, project_root: Path | None
) -> str:
    """Load and hash-bind a manifest window without placing source text in receipts."""

    path_value = str(metadata.get("transcript_path") or "").strip()
    if not path_value:
        raise GoldAuditError("manifest window is missing transcript_path")
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        if project_root is None:
            raise GoldAuditError("project_root is required to verify transcript-backed audit evidence")
        path = project_root / path
    try:
        transcript = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise GoldAuditError("manifest transcript cannot be read") from exc
    expected_transcript_sha = str(metadata.get("transcript_sha256") or "")
    if expected_transcript_sha and _sha256_text(transcript) != expected_transcript_sha:
        raise GoldAuditError("manifest transcript hash does not match frozen bytes")
    try:
        start = int(metadata["start_char"])
        end = int(metadata["end_char"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GoldAuditError("manifest window has invalid character offsets") from exc
    if start < 0 or end <= start or end > len(transcript):
        raise GoldAuditError("manifest window offsets are outside its frozen transcript")
    window_text = transcript[start:end]
    expected_text_sha = str(metadata.get("text_sha256") or "")
    if expected_text_sha and _sha256_text(window_text) != expected_text_sha:
        raise GoldAuditError("manifest window hash does not match frozen bytes")
    return window_text


def _max_same_span(events: Sequence[Mapping[str, Any]]) -> int:
    try:
        return max(same_span_counts(list(events)).values(), default=0)
    except (KeyError, TypeError, ValueError):
        # Contract validation records the underlying malformed event as a
        # catastrophe. Do not let a secondary aggregation exception hide it.
        return 0


def select_dev_audit_windows(
    manifest: Mapping[str, Any], event_counts: Mapping[str, int], *,
    initial_window_ids: Sequence[str], minimum_events: int = MINIMUM_CONSEQUENTIAL_EVENTS,
    expansion_block: int = EXPANSION_BLOCK_WINDOWS,
) -> dict[str, Any]:
    """Expand the frozen 19-window dev slice in preordered 40-window blocks."""

    dev_ids = {
        str(row["window_id"])
        for row in manifest["windows"]
        if row["split"] == "development"
    }
    selected = [str(window_id) for window_id in initial_window_ids]
    if len(selected) != len(set(selected)) or set(selected) - dev_ids:
        raise GoldAuditError("initial development audit ids are invalid")
    missing_counts = dev_ids - set(event_counts)
    if missing_counts:
        raise GoldAuditError(f"development event counts missing for {len(missing_counts)} windows")
    if any(
        isinstance(event_counts[window_id], bool)
        or not isinstance(event_counts[window_id], int)
        or event_counts[window_id] < 0
        for window_id in dev_ids
    ):
        raise GoldAuditError("development event counts must be non-negative integers")
    remaining = sorted(
        dev_ids - set(selected),
        key=lambda window_id: hashlib.sha256(
            f"signal-desk-dev-audit-expansion-v1:{window_id}".encode()
        ).hexdigest(),
    )
    event_total = sum(int(event_counts[window_id]) for window_id in selected)
    expansion_blocks = 0
    while event_total < minimum_events and remaining:
        block, remaining = remaining[:expansion_block], remaining[expansion_block:]
        selected.extend(block)
        event_total += sum(int(event_counts[window_id]) for window_id in block)
        expansion_blocks += 1
    return {
        "window_ids": tuple(selected),
        "initial_windows": len(initial_window_ids),
        "expanded_windows": len(selected),
        "expansion_blocks": expansion_blocks,
        "event_denominator": event_total,
        "minimum_events": minimum_events,
        "decision_ready": event_total >= minimum_events,
    }


def _load_outputs(path: Path) -> dict[str, Mapping[str, Any]]:
    return {
        item.stem: json.loads(item.read_text(encoding="utf-8"))
        for item in sorted(path.glob("*.json"))
    }


def evaluate_dev_audit(
    *, manifest_path: Path, result_root: Path,
    expected_windows: int | None = None,
    blind_window_ids: Sequence[str] | None = None,
    initial_window_ids: Sequence[str] | None = None,
    initial_windows: int = 19,
    minimum_events: int = MINIMUM_CONSEQUENTIAL_EVENTS,
    agreement_minimum: float = AGREEMENT_MINIMUM,
    critical_error_maximum: float = CRITICAL_ERROR_MAXIMUM,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Audit Gold C against an independent pass using frozen source bytes.

    Selection begins with the deterministic blind slice and expands in
    deterministic forty-window blocks until it covers at least 1,000 Gold-C
    consequential events. Evidence text is loaded only to validate exact
    offsets and hashes; the receipt never contains transcript text.
    """

    if initial_window_ids is not None and blind_window_ids is not None:
        raise GoldAuditError("provide initial_window_ids or blind_window_ids, not both")
    if initial_windows < 1:
        raise GoldAuditError("initial_windows must be positive")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    development_ids = {
        str(row["window_id"])
        for row in manifest["windows"]
        if row.get("split") == "development"
    }
    if not development_ids:
        raise GoldAuditError("manifest has no development windows")
    adjudicated = _load_outputs(result_root / "C")
    audit = _load_outputs(result_root / "AUDIT")
    missing_gold = development_ids - set(adjudicated)
    if missing_gold:
        raise GoldAuditError(
            f"development Gold C is incomplete: {len(development_ids) - len(missing_gold)}/{len(development_ids)} windows"
        )
    # ``blind_window_ids`` remains a safe compatibility alias. New callers
    # should use initial_window_ids so the expansion origin is explicit.
    seed_ids = tuple(
        initial_window_ids
        if initial_window_ids is not None
        else (blind_window_ids if blind_window_ids is not None else sorted(audit))
    )
    if not seed_ids:
        raise GoldAuditError("development audit is incomplete: no deterministic initial window ids")
    if initial_window_ids is not None and len(seed_ids) != initial_windows:
        raise GoldAuditError("initial_windows does not match initial_window_ids")
    audit_plan = select_dev_audit_windows(
        manifest,
        {
            window_id: _event_count(adjudicated[window_id], window_id=window_id)
            for window_id in development_ids
        },
        initial_window_ids=seed_ids,
        minimum_events=minimum_events,
    )
    selected = tuple(audit_plan["window_ids"])
    if expected_windows is not None and len(selected) != expected_windows:
        raise GoldAuditError(
            f"development audit selection changed: {len(selected)}/{expected_windows} windows"
        )
    missing_audit = set(selected) - set(audit)
    if missing_audit:
        raise GoldAuditError(
            f"development audit is incomplete: {len(selected) - len(missing_audit)}/{len(selected)} windows"
        )
    rows = []
    critical_errors = 0
    critical_denominator = 0
    catastrophic_window_ids: set[str] = set()
    catastrophic_reasons: Counter[str] = Counter()
    oversplitting_windows: list[dict[str, Any]] = []
    source_validated_windows = 0
    for window_id in selected:
        meta = metadata[window_id]
        try:
            transcript_window = _load_frozen_window_text(meta, project_root=project_root)
        except GoldAuditError:
            catastrophic_window_ids.add(window_id)
            catastrophic_reasons["source_window_integrity"] += 1
            continue
        try:
            gold = validate_output(
                adjudicated[window_id],
                transcript_window=transcript_window,
                expected_window_id=window_id,
            )
        except EvidenceContractError as exc:
            catastrophic_window_ids.add(window_id)
            reason = "wrong_source_or_window" if "identity" in str(exc) else "fabricated_or_unusable_evidence"
            catastrophic_reasons[f"gold_{reason}"] += 1
            continue
        try:
            independent = validate_output(
                audit[window_id],
                transcript_window=transcript_window,
                expected_window_id=window_id,
            )
        except EvidenceContractError as exc:
            catastrophic_window_ids.add(window_id)
            reason = "wrong_source_or_window" if "identity" in str(exc) else "fabricated_or_unusable_evidence"
            catastrophic_reasons[f"independent_{reason}"] += 1
            continue
        source_validated_windows += 1
        if {
            str(gold.get("window_disposition")),
            str(independent.get("window_disposition")),
        } == {"claims_found", "unusable_input"}:
            catastrophic_window_ids.add(window_id)
            catastrophic_reasons["unusable_context_disagreement"] += 1
        structure = str(meta["transcript_structure"])
        pairs = diagnostic_pairs(
            gold["events"], independent["events"], transcript_structure=structure
        )
        critical_denominator += len(gold["events"])
        paired_gold = {int(pair["gold_index"]) for pair in pairs}
        # Pairing is only evidence-plus-claim identity. Attribution and stance
        # discrepancies are counted here, instead of being hidden as recall.
        window_critical = len(gold["events"]) - len(paired_gold)
        for pair in pairs:
            agreement = pair["field_agreement"]
            attribution_disagrees = (
                not agreement["speaker"]
                or not agreement["speaker_role"]
                or not agreement["quoted_person"]
                or not agreement["mentioned_people"]
                or bool(pair["unsupported_attribution"])
            )
            if attribution_disagrees or not agreement["stance"]:
                window_critical += 1
            gold_claim = str(gold["events"][int(pair["gold_index"])]["claim_text"])
            independent_claim = str(
                independent["events"][int(pair["predicted_index"])]["claim_text"]
            )
            if bool(_NEGATION_RE.search(gold_claim)) != bool(_NEGATION_RE.search(independent_claim)):
                catastrophic_window_ids.add(window_id)
                catastrophic_reasons["reversed_meaning_negation"] += 1
        critical_errors += window_critical
        gold_max_same_span = _max_same_span(gold["events"])
        independent_max_same_span = _max_same_span(independent["events"])
        if max(gold_max_same_span, independent_max_same_span) >= 4:
            # This is a mandatory re-adjudication gate, not a passive audit
            # annotation. It prevents one-span claim splitting from laundering
            # itself into the tournament's reference truth.
            oversplitting_windows.append(
                {
                    "window_id": window_id,
                    "gold_c_max_same_span": gold_max_same_span,
                    "independent_max_same_span": independent_max_same_span,
                }
            )
        rows.append(
            {
                "window_id": window_id,
                "show_id": meta["show_id"],
                "episode_id": meta["episode_id"],
                "transcript_structure": structure,
                "gold": gold,
                "predicted": independent,
            }
        )
    if rows:
        evaluation = evaluate_windows(rows)
        agreement_numerator = 2 * evaluation["counts"]["diagnostic_pairs"]
        agreement_denominator = (
            evaluation["counts"]["gold_events"] + evaluation["counts"]["predicted_events"]
        )
        aggregate_metrics = evaluation["metrics"]
        strata = evaluation["strata"]
    else:
        agreement_numerator = 0
        agreement_denominator = 0
        aggregate_metrics = {}
        strata = {}
    agreement_gate = evaluate_rate_gate(
        agreement_numerator, agreement_denominator,
        threshold=agreement_minimum, direction="minimum",
    )
    critical_gate = evaluate_rate_gate(
        critical_errors, critical_denominator,
        threshold=critical_error_maximum, direction="maximum",
    )
    # The event floor is determined before viewing audit output. Invalid
    # source-backed rows still fail the catastrophe gate and cannot improve a
    # thin sample by disappearing from the critical denominator.
    powered = audit_plan["decision_ready"] and critical_denominator >= minimum_events
    requires_readjudication = bool(oversplitting_windows)
    passed = (
        agreement_gate["passed"]
        and critical_gate["passed"]
        and not catastrophic_window_ids
        and not requires_readjudication
        and powered
    )
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "status": "passed" if passed else (
            "requires_readjudication" if requires_readjudication else "failed"
        ),
        "passed": passed,
        "split": "development",
        "audited_windows": len(selected),
        "audit_selection": {
            **audit_plan,
            "selection_owned_by": "evaluate_dev_audit",
        },
        "initial_windows": audit_plan["initial_windows"],
        "expansion_windows": max(0, len(selected) - audit_plan["initial_windows"]),
        "event_power": {
            "event_denominator": critical_denominator,
            "selected_gold_c_events": audit_plan["event_denominator"],
            "minimum": minimum_events,
            "passed": powered,
        },
        "agreement": {
            "metric": "diagnostic_event_f1",
            "numerator": agreement_numerator,
            "denominator": agreement_denominator,
            **agreement_gate,
        },
        "critical_errors": {
            "definition": (
                "adjudicated event absent from independent audit, or a paired event with "
                "stance or attribution (speaker, role, quoted/mentioned person) disagreement"
            ),
            "errors": critical_errors,
            "event_denominator": critical_denominator,
            "one_sided_wilson_ucb": critical_gate["bound"],
            **critical_gate,
        },
        "catastrophic": {
            "definition": (
                "fabricated or wrong-source evidence, frozen source/window mismatch, unusable "
                "context disagreement, or matched claims with reversed negation meaning"
            ),
            "windows": len(catastrophic_window_ids),
            "reason_counts": dict(sorted(catastrophic_reasons.items())),
            "passed": not catastrophic_window_ids,
        },
        "catastrophic_windows": len(catastrophic_window_ids),
        "source_validation": {
            "validated_windows": source_validated_windows,
            "failed_windows": len(selected) - source_validated_windows,
            "receipt_exposes_transcript_text": False,
        },
        "over_splitting": {
            "rule": "max_same_span >= 4 requires Gold C re-adjudication",
            "flagged_windows": oversplitting_windows,
            "requires_readjudication": requires_readjudication,
        },
        "aggregate_metrics": aggregate_metrics,
        "strata": strata,
        "item_outputs_exposed": False,
        "manifest_sha256": manifest["manifest_sha256"],
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return receipt
