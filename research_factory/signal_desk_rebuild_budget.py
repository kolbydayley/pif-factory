"""Durable, campaign-scoped budget authority for the Signal Desk rebuild.

The ordinary subscription governor intentionally keeps its conservative
five-million-token default.  This module layers the owner-authorized rebuild
grant on top without turning that exception into a global cap increase.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .subscription_budget import budget_gate
from .util import dumps_json, now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_rebuild_budget_grant_v1"
BURN_PROBE_SCHEMA_VERSION = "pif_signal_desk_rebuild_burn_probe_v1"
EXPECTED_SCOPE = "signal-desk-clean-corpus-rebuild"
MAX_GRANT_DAYS = 45
MAX_DAILY_CAP_TOKENS = 20_000_000
MIN_BURN_PROBE_DAYS = 5
PREFERRED_BURN_PROBE_DAYS = 7
MIN_BURN_PROBE_TOKENS = 18_000_000


class RebuildBudgetError(RuntimeError):
    """Raised when rebuild authority or receipts are incomplete."""


def _instant(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RebuildBudgetError("invalid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise RebuildBudgetError("timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def grant_hash(payload: Mapping[str, Any]) -> str:
    body = {key: value for key, value in payload.items() if key != "grant_sha256"}
    return sha256_text(dumps_json(body))


@dataclass(frozen=True)
class RebuildGrant:
    path: Path
    campaign_id: str
    granted_at: datetime
    expires_at: datetime
    daily_cap_tokens: int
    payload: Mapping[str, Any]


def load_grant(
    path: Path,
    *,
    campaign_id: str,
    at: datetime | None = None,
    clean_release_exists: bool = False,
) -> RebuildGrant:
    """Load and strictly validate a rebuild-only owner grant."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RebuildBudgetError("rebuild budget grant is unreadable") from exc
    required = {
        "schema_version",
        "granted_at",
        "expires_at",
        "expiry_condition",
        "daily_cap_tokens",
        "scope",
        "authorized_by",
        "campaign_id",
        "grant_sha256",
    }
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise RebuildBudgetError("rebuild budget grant is incomplete")
    if payload["schema_version"] != SCHEMA_VERSION:
        raise RebuildBudgetError("unsupported rebuild budget grant schema")
    if payload["scope"] != EXPECTED_SCOPE:
        raise RebuildBudgetError("grant scope does not authorize this lane")
    if payload["authorized_by"].strip().lower() != "kolby":
        raise RebuildBudgetError("grant is not owner-authorized")
    if payload["campaign_id"] != campaign_id:
        raise RebuildBudgetError("grant campaign does not match")
    if payload["expiry_condition"] != "first_clean_release":
        raise RebuildBudgetError("grant has an unsupported expiry condition")
    if payload["grant_sha256"] != grant_hash(payload):
        raise RebuildBudgetError("grant hash mismatch")
    cap = payload["daily_cap_tokens"]
    if isinstance(cap, bool) or not isinstance(cap, int) or cap != MAX_DAILY_CAP_TOKENS:
        raise RebuildBudgetError("grant must authorize exactly 20M tokens/day")
    granted_at = _instant(payload["granted_at"])
    expires_at = _instant(payload["expires_at"])
    duration = (expires_at - granted_at).total_seconds()
    if duration <= 0 or duration > MAX_GRANT_DAYS * 86400:
        raise RebuildBudgetError("grant expiry must be within 45 days")
    if clean_release_exists:
        raise RebuildBudgetError("grant expired at first clean release")
    current = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if current < granted_at or current >= expires_at:
        raise RebuildBudgetError("rebuild budget grant is not active")
    return RebuildGrant(
        path=path,
        campaign_id=campaign_id,
        granted_at=granted_at,
        expires_at=expires_at,
        daily_cap_tokens=cap,
        payload=payload,
    )


def campaign_kill_path(budget_dir: Path, campaign_id: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in campaign_id)
    return budget_dir / f"KILL-{safe}.json"


def rebuild_budget_gate(
    conn: sqlite3.Connection,
    *,
    day: str,
    campaign_id: str,
    grant_path: Path,
    budget_dir: Path,
    at: datetime | None = None,
    clean_release_exists: bool = False,
    additional_budget_db_paths: Sequence[Path] = (),
    window_start_iso: str | None = None,
) -> dict[str, Any]:
    """Apply the durable grant and campaign-scoped fail-closed KILL gate."""

    grant = load_grant(
        grant_path,
        campaign_id=campaign_id,
        at=at,
        clean_release_exists=clean_release_exists,
    )
    budget_dir.mkdir(parents=True, exist_ok=True)
    kill_path = campaign_kill_path(budget_dir, campaign_id)
    if kill_path.exists():
        return {
            "allowed": False,
            "reason": "rebuild_campaign_kill_present",
            "campaign_id": campaign_id,
            "kill_path": str(kill_path),
            "grant_path": str(grant.path),
        }
    base = budget_gate(
        conn,
        day=day,
        cap_tokens=grant.daily_cap_tokens,
        budget_dir=budget_dir / "global-governor",
        additional_budget_db_paths=additional_budget_db_paths,
        window_start_iso=window_start_iso,
    )
    if base.get("kill_engaged"):
        kill_body = {
            "schema_version": "pif_signal_desk_rebuild_kill_v1",
            "campaign_id": campaign_id,
            "engaged_at": now_iso(),
            "day": day,
            "tokens_used": int(base.get("tokens_used") or 0),
            "cap_tokens": grant.daily_cap_tokens,
            "threshold_multiplier": 1.2,
            "reason": "usage_reached_120_percent_of_rebuild_daily_cap",
            "clearance_authority": "Kolby or explicitly delegated current-turn operator",
            "recovery": "reconcile all ledgers, identify bypass, then explicitly remove this campaign KILL",
        }
        kill_body["kill_sha256"] = sha256_text(dumps_json(kill_body))
        kill_path.write_text(dumps_json(kill_body) + "\n", encoding="utf-8")
        base["allowed"] = False
        base["reason"] = "rebuild_campaign_kill_engaged"
    return {
        **base,
        "campaign_id": campaign_id,
        "grant_path": str(grant.path),
        "grant_expires_at": grant.expires_at.isoformat(),
        "kill_path": str(kill_path),
    }


def qualify_burn_probe(receipts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Verify 5-7 consecutive useful-work days near the authorized cap."""

    normalized = sorted(receipts, key=lambda row: str(row.get("day") or ""))
    qualifying: list[Mapping[str, Any]] = []
    previous = None
    consecutive = True
    for receipt in normalized:
        day = datetime.fromisoformat(str(receipt["day"]))
        if previous is not None and (day.date() - previous.date()).days != 1:
            consecutive = False
        previous = day
        if (
            int(receipt.get("tokens") or 0) >= MIN_BURN_PROBE_TOKENS
            and int(receipt.get("tokens") or 0) <= MAX_DAILY_CAP_TOKENS
            and not bool(receipt.get("provider_quota_failure"))
            and bool(receipt.get("useful_work", True))
        ):
            qualifying.append(receipt)
    passed = consecutive and len(qualifying) >= MIN_BURN_PROBE_DAYS
    return {
        "schema_version": BURN_PROBE_SCHEMA_VERSION,
        "passed": passed,
        "consecutive": consecutive,
        "observed_days": len(normalized),
        "qualifying_days": len(qualifying),
        "preferred_seven_day_probe": consecutive and len(qualifying) >= PREFERRED_BURN_PROBE_DAYS,
        "minimum_daily_tokens": MIN_BURN_PROBE_TOKENS,
        "maximum_daily_tokens": MAX_DAILY_CAP_TOKENS,
    }
