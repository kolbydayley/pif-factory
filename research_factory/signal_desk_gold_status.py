"""Resolve one canonical Gold-authoring status from immutable split receipts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_gold_campaign_status_v1"


def resolve_gold_campaign_status(root: Path) -> dict[str, Any]:
    paths = {
        "development": root / "results/development/gold-development-receipt.json",
        "validation": root / "sealed-gold-results/validation/gold-validation-receipt.json",
        "sealed_holdout": root / "sealed-gold-results/sealed_holdout/gold-sealed_holdout-receipt.json",
    }
    splits: dict[str, Any] = {}
    for split, path in paths.items():
        if not path.exists():
            splits[split] = {"complete": False, "receipt": None}
            continue
        receipt = json.loads(path.read_text(encoding="utf-8"))
        splits[split] = {
            "complete": bool(receipt.get("complete")),
            "split_windows": receipt.get("split_windows"),
            "quarantined_windows": receipt.get("quarantined_windows"),
            "accepted_incomplete_by_ruling": receipt.get("accepted_incomplete_by_ruling"),
            "receipt_sha256": receipt.get("receipt_sha256"),
            "receipt": str(path.relative_to(root)),
        }
    complete = all(row["complete"] for row in splits.values())
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "status": "complete" if complete else "incomplete",
        "complete": complete,
        "source_of_truth": "immutable split receipts",
        "supersedes_ephemeral_split_status_files": True,
        "splits": splits,
    }
    digest_body = json.dumps(body, sort_keys=True, separators=(",", ":"))
    body["receipt_sha256"] = hashlib.sha256(digest_body.encode()).hexdigest()
    return body
