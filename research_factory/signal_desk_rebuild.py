"""Fail-closed campaign controller for the Signal Desk clean-corpus rebuild."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .lane_profiles import LANE_PROFILES
from .signal_desk_rebuild_approval import initialize_approval_schema
from .signal_desk_rebuild_budget import load_grant
from .signal_desk_rebuild_contracts import contract_sha256
from .signal_desk_rebuild_dispatch import initialize_dispatch_schema
from .signal_desk_rebuild_scorer import scorer_sha256
from .signal_desk_rebuild_tournament import ensure_tournament_schema, hash_prompt
from .util import dumps_json, sha256_text


CAMPAIGN_SCHEMA_VERSION = "pif_signal_desk_rebuild_campaign_v1"
DEFAULT_CAMPAIGN_PATH = Path("config/signal_desk_rebuild_campaign.json")
DEFAULT_GRANT_PATH = Path("config/signal_desk_rebuild_budget_grant.json")
DEFAULT_PROMPT_PATH = Path("config/signal_desk_rebuild_system_prompt_v1.txt")
DEFAULT_SHOW_ALIAS_PATH = Path("config/signal_desk_show_aliases.json")
ROUND1_ARTIFACTS = {
    "split_manifest_frozen": "split-manifest.json",
    "gold_adjudicated": "gold-adjudication.json",
    "gold_audit_passed": "gold-audit.json",
    "scorer_qualified": "scorer-qualified.json",
    "frontier_ceiling_measured": "frontier-ceiling.json",
    "absolute_gates_frozen": "frozen-gates.json",
    "dispatch_invariants_qualified": "dispatch-qualified.json",
}


class RebuildCampaignError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RebuildCampaignError(f"unreadable JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise RebuildCampaignError(f"artifact must be an object: {path}")
    return value


def load_campaign(path: Path) -> dict[str, Any]:
    campaign = _read_json(path)
    if campaign.get("schema_version") != CAMPAIGN_SCHEMA_VERSION:
        raise RebuildCampaignError("unsupported campaign schema")
    if campaign.get("public_intelligence_state") != "withdrawn_until_clean_release":
        raise RebuildCampaignError("public intelligence must remain withdrawn")
    blocking = campaign.get("blocking_before_round_1")
    if blocking != list(ROUND1_ARTIFACTS):
        raise RebuildCampaignError("round-1 blocking order has drifted")
    concurrency = campaign.get("concurrency") or {}
    expected = {
        "zai_glm_5_2": int(LANE_PROFILES["glm-zai"]["concurrency"]),
        "opencode_go_glm_5_2": int(LANE_PROFILES["glm"]["concurrency"]),
        "zai_glm_5_3_flash_separate_qualification": int(
            LANE_PROFILES["glm-zai-flash"]["concurrency"]
        ),
    }
    if any(int(concurrency.get(key, -1)) != value for key, value in expected.items()):
        raise RebuildCampaignError("campaign concurrency differs from live qualified lanes")
    if int(concurrency.get("total_glm_ceiling", -1)) != sum(expected.values()):
        raise RebuildCampaignError("campaign GLM ceiling does not equal lane totals")
    return campaign


def campaign_preflight(
    *,
    campaign_path: Path,
    grant_path: Path,
    prompt_path: Path,
    show_alias_path: Path = DEFAULT_SHOW_ALIAS_PATH,
    at: datetime | None = None,
) -> dict[str, Any]:
    campaign = load_campaign(campaign_path)
    campaign_id = str(campaign["campaign_id"])
    grant = load_grant(
        grant_path,
        campaign_id=campaign_id,
        at=at or datetime.now(timezone.utc),
    )
    try:
        prompt = prompt_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RebuildCampaignError("system prompt is unreadable") from exc
    prompt_sha = hash_prompt(prompt)
    show_aliases = _read_json(show_alias_path)
    if show_aliases.get("schema_version") != "pif_signal_desk_show_aliases_v1":
        raise RebuildCampaignError("unsupported show alias registry")
    if not isinstance(show_aliases.get("aliases"), dict):
        raise RebuildCampaignError("show alias registry is malformed")
    return {
        "schema_version": "pif_signal_desk_rebuild_preflight_v1",
        "passed": True,
        "campaign_id": campaign_id,
        "campaign_sha256": sha256_text(dumps_json(campaign)),
        "prompt_sha256": prompt_sha,
        "contract_sha256": contract_sha256(),
        "scorer_sha256": scorer_sha256(),
        "show_alias_registry_sha256": sha256_text(dumps_json(show_aliases)),
        "grant_sha256": str(grant.payload["grant_sha256"]),
        "grant_expires_at": grant.expires_at.isoformat(),
        "glm_concurrency": campaign["concurrency"],
        "public_intelligence_state": campaign["public_intelligence_state"],
    }


def initialize_campaign_database(path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        initialize_dispatch_schema(conn)
        ensure_tournament_schema(conn)
        initialize_approval_schema(conn)
        conn.commit()
        tables = sorted(
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'signal_desk_rebuild_%'"
            )
        )
    finally:
        conn.close()
    return {"database": str(path), "tables": tables, "initialized": True}


def _artifact_passed(name: str, payload: Mapping[str, Any]) -> bool:
    if name == "split_manifest_frozen":
        return payload.get("frozen") is True and bool(payload.get("manifest_sha256"))
    if name == "gold_adjudicated":
        return payload.get("status") == "adjudicated" and bool(payload.get("gold_sha256"))
    if name == "gold_audit_passed":
        return payload.get("status") == "passed" and payload.get("passed") is True
    if name == "scorer_qualified":
        return payload.get("status") == "qualified" and float(payload.get("agreement_lcb", 0)) >= 0.97
    if name == "frontier_ceiling_measured":
        return payload.get("model") == "gpt-5.6-sol" and payload.get("passes") == 1
    if name == "absolute_gates_frozen":
        return payload.get("frozen") is True and bool(payload.get("manifest_sha256"))
    if name == "dispatch_invariants_qualified":
        return payload.get("passed") is True and int(payload.get("tests_passed", 0)) >= 5
    return False


def round1_readiness(artifact_root: Path) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for name, filename in ROUND1_ARTIFACTS.items():
        path = artifact_root / filename
        if not path.exists():
            checks[name] = {"passed": False, "reason": "missing", "path": str(path)}
            continue
        try:
            payload = _read_json(path)
            passed = _artifact_passed(name, payload)
            checks[name] = {
                "passed": passed,
                "reason": None if passed else "artifact_gate_failed",
                "path": str(path),
                "sha256": sha256_text(dumps_json(payload)),
            }
        except RebuildCampaignError as exc:
            checks[name] = {"passed": False, "reason": str(exc), "path": str(path)}
    passed = all(check["passed"] for check in checks.values())
    return {
        "schema_version": "pif_signal_desk_round1_readiness_v1",
        "passed": passed,
        "disposition": "round_1_authorized" if passed else "blocked_before_round_1",
        "checks": checks,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    preflight = sub.add_parser("preflight")
    preflight.add_argument("--campaign", type=Path, default=DEFAULT_CAMPAIGN_PATH)
    preflight.add_argument("--grant", type=Path, default=DEFAULT_GRANT_PATH)
    preflight.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_PATH)
    preflight.add_argument("--show-aliases", type=Path, default=DEFAULT_SHOW_ALIAS_PATH)
    init = sub.add_parser("init")
    init.add_argument("--database", type=Path, required=True)
    status = sub.add_parser("round1-status")
    status.add_argument("--artifact-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "preflight":
        result = campaign_preflight(
            campaign_path=args.campaign,
            grant_path=args.grant,
            prompt_path=args.prompt,
            show_alias_path=args.show_aliases,
        )
    elif args.command == "init":
        result = initialize_campaign_database(args.database)
    else:
        result = round1_readiness(args.artifact_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("passed", result.get("initialized", False)) else 2


if __name__ == "__main__":
    raise SystemExit(main())
