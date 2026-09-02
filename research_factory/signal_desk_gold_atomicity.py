"""Focused Gold-C review for one-evidence-span/many-claim splitting risk."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Mapping

from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_gold_atomicity_review_v1"
ATOMICITY_REVIEW_WINDOW_IDS = frozenset(
    {
        "sdw_c793c28d8a642b4251af",
        "sdw_a7e2014f6f65b19908df",
        "sdw_ae102e2d95ddb5c2d4b2",
        "sdw_40b34ba5473fab3b4955",
        "sdw_54fab3caf0501d37c800",
        "sdw_74129106997596aeb5b4",
        "sdw_ac3274cc98d691be4954",
        "sdw_2f683002db7c19be2d7c",
    }
)


def same_span_counts(events: list[Mapping[str, Any]]) -> Counter[tuple[int, int]]:
    return Counter(
        (int(event["evidence_start"]), int(event["evidence_end"]))
        for event in events
    )


def _near_duplicate_pairs(events: list[Mapping[str, Any]]) -> int:
    by_span: dict[tuple[int, int], list[str]] = defaultdict(list)
    for event in events:
        span = (int(event["evidence_start"]), int(event["evidence_end"]))
        by_span[span].append(" ".join(str(event["claim_text"]).casefold().split()))
    duplicates = 0
    for claims in by_span.values():
        for left_index, left in enumerate(claims):
            for right in claims[left_index + 1 :]:
                if SequenceMatcher(None, left, right).ratio() >= 0.88:
                    duplicates += 1
    return duplicates


def evaluate_atomicity_review(*, result_root: Path) -> dict[str, Any]:
    """Compare Gold C with an independent audit on every stress window."""

    results = []
    for window_id in sorted(ATOMICITY_REVIEW_WINDOW_IDS):
        c_path = result_root / "C" / f"{window_id}.json"
        audit_path = result_root / "AUDIT" / f"{window_id}.json"
        if not c_path.exists() or not audit_path.exists():
            raise RuntimeError(f"atomicity review is incomplete for {window_id}")
        adjudicated = json.loads(c_path.read_text(encoding="utf-8"))["events"]
        independent = json.loads(audit_path.read_text(encoding="utf-8"))["events"]
        c_counts = same_span_counts(adjudicated)
        audit_counts = same_span_counts(independent)
        c_max = max(c_counts.values(), default=0)
        audit_max = max(audit_counts.values(), default=0)
        near_duplicates = _near_duplicate_pairs(adjudicated)
        requires_readjudication = c_max >= 4 and c_max > audit_max and near_duplicates > 0
        results.append(
            {
                "window_id": window_id,
                "gold_c_events": len(adjudicated),
                "independent_audit_events": len(independent),
                "gold_c_extra_repeated_spans": sum(count - 1 for count in c_counts.values()),
                "gold_c_max_same_span": c_max,
                "independent_max_same_span": audit_max,
                "near_duplicate_same_span_claim_pairs": near_duplicates,
                "requires_readjudication": requires_readjudication,
            }
        )
    flagged = [row["window_id"] for row in results if row["requires_readjudication"]]
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "status": "requires_readjudication" if flagged else "passed",
        "passed": not flagged,
        "reviewed_windows": len(results),
        "flagged_windows": flagged,
        "results": results,
        "item_text_exposed": False,
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return receipt
