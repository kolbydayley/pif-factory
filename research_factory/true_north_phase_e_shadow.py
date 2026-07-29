"""Fail-closed preflight for the Phase-E production shadow trial."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from . import true_north


SCHEMA_VERSION = "pif_true_north_phase_e_shadow_v1"
EXPERIMENT_ID = "phase-e-three-episode-production-shadow-20260729-v1"
MAX_CALLS = 60
MAX_TOKENS = 1_500_000

# Darwin's dataless file flag is not exposed by Python's stat module.
SF_DATALESS = 0x40000000


class PhaseEShadowError(RuntimeError):
    """Raised before provider dispatch when isolation cannot be proved."""


def database_identity(path: str | Path) -> dict[str, Any]:
    """Return metadata without opening or hydrating database contents."""
    resolved = Path(path).expanduser().resolve()
    info = os.stat(resolved)
    flags = int(getattr(info, "st_flags", 0))
    return {
        "path": str(resolved),
        "size_bytes": int(info.st_size),
        "inode": int(info.st_ino),
        "mtime_ns": int(info.st_mtime_ns),
        "flags": flags,
        "dataless": bool(flags & SF_DATALESS),
    }


def strict_isolation_preflight(
    *,
    production_database: str | Path,
    shadow_root: str | Path,
) -> dict[str, Any]:
    """Prove the minimum prerequisites before any production shadow call."""
    database = database_identity(production_database)
    shadow = Path(shadow_root).expanduser().resolve()
    production = Path(database["path"])
    reasons: list[str] = []
    if database["dataless"]:
        reasons.append("production_database_is_dataless_and_unreadable")
    if shadow == production or production in shadow.parents:
        reasons.append("shadow_store_overlaps_production_database")
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
        },
        "production_database": database,
        "requested_access": "sqlite_uri_mode_ro_plus_query_only",
        "shadow_root": str(shadow),
        "production_writes_allowed": False,
        "queue_mutation_allowed": False,
        "release_or_label_publication_allowed": False,
        "provider_calls_made": 0,
        "provider_tokens": 0,
        "eligible_to_dispatch": not reasons,
        "stop_reasons": reasons,
    }
    result["preflight_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    return result


def require_dispatch_eligibility(preflight: dict[str, Any]) -> None:
    if not preflight.get("eligible_to_dispatch"):
        raise PhaseEShadowError(
            "Phase E stopped before provider dispatch: "
            + ", ".join(preflight.get("stop_reasons", []))
        )
