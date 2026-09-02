"""Durable, campaign-scoped budget authority for the Signal Desk rebuild.

The ordinary subscription governor intentionally keeps its conservative
five-million-token default.  This module layers the owner-authorized rebuild
grant on top without turning that exception into a global cap increase.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .subscription_budget import budget_gate
from .util import dumps_json, now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_rebuild_budget_grant_v1"
BURN_PROBE_SCHEMA_VERSION = "pif_signal_desk_rebuild_burn_probe_v2"
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


def _probe_day(value: object) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise RebuildBudgetError("burn probe receipt has an invalid day") from exc


def five_day_budget_burn_probe(
    receipts: Sequence[Mapping[str, Any]],
    *,
    require_provider_window: bool = True,
) -> dict[str, Any]:
    """Evaluate a real five-to-seven-day capacity probe before Phase 4.

    A qualifying day must demonstrate useful rebuild-lane work close to the
    authorized 20M ceiling without quota/capacity failure. We search for a
    *contiguous qualifying* streak, rather than treating five good days around
    a failed day as a pass. With ``require_provider_window`` enabled, every
    selected day must also attest the same real weekly provider window.
    """

    seen_days: set[date] = set()
    normalized: list[dict[str, Any]] = []
    duplicate_day = False
    for raw in receipts:
        if not isinstance(raw, Mapping):
            raise RebuildBudgetError("burn probe receipts must be mappings")
        day = _probe_day(raw.get("day"))
        duplicate_day = duplicate_day or day in seen_days
        seen_days.add(day)
        try:
            tokens = int(raw.get("tokens") or 0)
        except (TypeError, ValueError) as exc:
            raise RebuildBudgetError("burn probe tokens must be an integer") from exc
        window_id = str(
            raw.get("provider_window_id") or raw.get("weekly_window_id") or ""
        ).strip()
        qualifying = (
            0 <= tokens <= MAX_DAILY_CAP_TOKENS
            and tokens >= MIN_BURN_PROBE_TOKENS
            and not bool(raw.get("provider_quota_failure"))
            and not bool(raw.get("provider_capacity_failure"))
            and bool(raw.get("useful_work", True))
            and (bool(window_id) or not require_provider_window)
        )
        normalized.append(
            {
                "day": day,
                "tokens": tokens,
                "provider_window_id": window_id,
                "qualifying": qualifying,
            }
        )
    normalized.sort(key=lambda row: row["day"])
    longest: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    for row in normalized:
        same_window = (
            not current
            or not require_provider_window
            or row["provider_window_id"] == current[0]["provider_window_id"]
        )
        contiguous = not current or (row["day"] - current[-1]["day"]).days == 1
        if row["qualifying"] and contiguous and same_window:
            current.append(row)
        elif row["qualifying"]:
            current = [row]
        else:
            current = []
        if len(current) > len(longest):
            longest = list(current)

    qualifying_days = sum(int(row["qualifying"]) for row in normalized)
    streak_days = len(longest)
    provider_window_verified = bool(longest) and (
        not require_provider_window or bool(longest[0]["provider_window_id"])
    )
    passed = (
        not duplicate_day
        and streak_days >= MIN_BURN_PROBE_DAYS
        and provider_window_verified
    )
    body: dict[str, Any] = {
        "schema_version": BURN_PROBE_SCHEMA_VERSION,
        "passed": passed,
        "status": "passed" if passed else "failed",
        "probe_type": "five_day_weekly_window_capacity_burn",
        "observed_days": len(normalized),
        "qualifying_days": qualifying_days,
        "longest_contiguous_qualifying_streak_days": streak_days,
        "streak_start_day": longest[0]["day"].isoformat() if longest else None,
        "streak_end_day": longest[-1]["day"].isoformat() if longest else None,
        "weekly_window_id": longest[0]["provider_window_id"] if longest else None,
        "provider_window_verified": provider_window_verified,
        "duplicate_day_detected": duplicate_day,
        "require_provider_window": require_provider_window,
        "preferred_seven_day_probe": streak_days >= PREFERRED_BURN_PROBE_DAYS,
        "minimum_days": MIN_BURN_PROBE_DAYS,
        "minimum_daily_tokens": MIN_BURN_PROBE_TOKENS,
        "maximum_daily_tokens": MAX_DAILY_CAP_TOKENS,
        "receipt_exposes_provider_responses": False,
    }
    body["receipt_sha256"] = sha256_text(dumps_json(body))
    return body


def write_five_day_budget_burn_probe(
    path: Path,
    *,
    receipts: Sequence[Mapping[str, Any]],
    require_provider_window: bool = True,
) -> dict[str, Any]:
    """Write one immutable probe receipt, including a failed probe for triage."""

    if path.exists():
        raise RebuildBudgetError("burn probe receipt is immutable")
    result = five_day_budget_burn_probe(
        receipts,
        require_provider_window=require_provider_window,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dumps_json(result) + "\n", encoding="utf-8")
    return result


def qualify_burn_probe(receipts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Backward-compatible local qualification helper without provider attestation.

    Phase-4 release code must call ``five_day_budget_burn_probe`` with its
    default strict provider-window contract. This helper remains for prior
    offline tests and never constitutes a release receipt by itself.
    """

    return five_day_budget_burn_probe(receipts, require_provider_window=False)
