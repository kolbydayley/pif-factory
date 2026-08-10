"""GLM<->Codex provider-agreement loop with fail-closed auto-freeze.

Kolby's rulings (2026-08-10): the hybrid system promotes a stage to the GLM
lane only on measured agreement, and a breach auto-freezes the GLM share back
to Codex without waiting for a human. Thresholds live per stage in
``config/provider_policy.json`` (``agreement_threshold``).

Mechanics:

- ``pif_provider_agreement`` rows record each sampled comparison (one GLM
  output re-judged by Codex), append-only;
- ``agreement_gate`` evaluates one day's samples for a stage against the
  policy threshold. On breach it writes ``FREEZE_<stage>`` under
  ``work/pif-ops/agreement/``;
- ``effective_stage_policy`` is what dispatch call sites consult: the policy's
  provider, downgraded to the stage's fallback (Codex) whenever the freeze
  file exists. Freezes never auto-lift — a later good day keeps the freeze
  until a human reviews the breach and removes the file;
- zero samples is not a pass (fail closed), but it does not engage the freeze
  either: nothing was measured.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

from .paths import root
from .provider_policy import (
    ProviderPolicyError,
    StagePolicy,
    load_policy,
    stage_policy,
)
from .util import dumps_json, now_iso, stable_id

__all__ = [
    "ensure_agreement_schema",
    "record_agreement",
    "daily_agreement",
    "agreement_gate",
    "effective_stage_policy",
    "default_freeze_dir",
]


def default_freeze_dir() -> Path:
    return root() / "work" / "pif-ops" / "agreement"


def ensure_agreement_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pif_provider_agreement (
          id TEXT PRIMARY KEY,
          day TEXT NOT NULL,
          stage TEXT NOT NULL,
          sample_id TEXT NOT NULL,
          glm_output_sha256 TEXT NOT NULL,
          codex_output_sha256 TEXT NOT NULL,
          exact_match INTEGER NOT NULL CHECK(exact_match IN (0, 1)),
          agreement_score REAL NOT NULL,
          disagreement_fields_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(day, stage, sample_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_agreement_day_stage
          ON pif_provider_agreement(day, stage)
        """
    )


def record_agreement(
    conn: sqlite3.Connection,
    *,
    day: str,
    stage: str,
    sample_id: str,
    glm_output_sha256: str,
    codex_output_sha256: str,
    exact_match: bool,
    agreement_score: float,
    disagreement_fields: Sequence[str],
) -> str:
    ensure_agreement_schema(conn)
    row_id = stable_id("provider_agreement", day, stage, sample_id, prefix="ppa_")
    conn.execute(
        """
        INSERT OR IGNORE INTO pif_provider_agreement
          (id, day, stage, sample_id, glm_output_sha256, codex_output_sha256,
           exact_match, agreement_score, disagreement_fields_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            day,
            stage,
            sample_id,
            glm_output_sha256,
            codex_output_sha256,
            1 if exact_match else 0,
            max(0.0, min(1.0, float(agreement_score))),
            dumps_json(list(disagreement_fields)),
            now_iso(),
        ),
    )
    return row_id


def daily_agreement(
    conn: sqlite3.Connection,
    *,
    day: str,
    stage: str,
) -> dict[str, Any]:
    ensure_agreement_schema(conn)
    row = conn.execute(
        """
        SELECT COUNT(*) AS samples,
               COALESCE(AVG(agreement_score), 0.0) AS mean_score,
               COALESCE(AVG(exact_match), 0.0) AS exact_rate
        FROM pif_provider_agreement
        WHERE day = ? AND stage = ?
        """,
        (day, stage),
    ).fetchone()
    samples = int(row["samples"] or 0)
    return {
        "day": day,
        "stage": stage,
        "samples": samples,
        "agreement_rate": round(float(row["exact_rate"] or 0.0), 6),
        "mean_agreement_score": round(float(row["mean_score"] or 0.0), 6),
    }


def _stage_threshold(
    stage: str,
    policy: Mapping[str, Any] | None,
) -> float:
    resolved = policy if policy is not None else load_policy()
    entry = resolved["stages"].get(stage)
    if not isinstance(entry, Mapping):
        raise ProviderPolicyError(
            f"unknown stage {stage!r}: not present in provider policy"
        )
    threshold = entry.get("agreement_threshold")
    if not isinstance(threshold, (int, float)):
        raise ProviderPolicyError(
            f"stage {stage!r} has no agreement_threshold in provider policy"
        )
    return float(threshold)


def agreement_gate(
    conn: sqlite3.Connection,
    *,
    day: str,
    stage: str,
    policy: Mapping[str, Any] | None = None,
    freeze_dir: Path | None = None,
) -> dict[str, Any]:
    """Evaluate one day's agreement for a stage; breach engages the freeze.

    Fail closed: zero samples is not a pass. Freezes never auto-lift.
    """

    directory = freeze_dir if freeze_dir is not None else default_freeze_dir()
    directory.mkdir(parents=True, exist_ok=True)
    freeze_path = directory / f"FREEZE_{stage}"
    threshold = _stage_threshold(stage, policy)
    summary = daily_agreement(conn, day=day, stage=stage)
    if summary["samples"] == 0:
        passed = False
        reason = "no_samples"
    elif summary["agreement_rate"] >= threshold:
        passed = True
        reason = None
    else:
        passed = False
        reason = "agreement_below_threshold"
        if not freeze_path.exists():
            freeze_path.write_text(
                dumps_json(
                    {
                        "engaged_at": now_iso(),
                        "day": day,
                        "stage": stage,
                        "agreement_rate": summary["agreement_rate"],
                        "threshold": threshold,
                        "samples": summary["samples"],
                        "reason": reason,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
    return {
        **summary,
        "threshold": threshold,
        "passed": passed,
        "reason": reason,
        "frozen": freeze_path.exists(),
        "freeze_path": str(freeze_path),
    }


def effective_stage_policy(
    stage: str,
    *,
    policy: Mapping[str, Any] | None = None,
    freeze_dir: Path | None = None,
) -> StagePolicy:
    """The provider dispatch must actually use: policy, unless frozen.

    When ``FREEZE_<stage>`` exists the stage downgrades to its declared
    fallback (``fallback_provider``/``fallback_model``, defaulting to
    codex/gpt-5.5) — fail closed, no exceptions, until a human removes the
    file after reviewing the breach.
    """

    resolved = policy if policy is not None else load_policy()
    base = stage_policy(stage, policy=resolved)
    directory = freeze_dir if freeze_dir is not None else default_freeze_dir()
    freeze_path = directory / f"FREEZE_{stage}"
    if not freeze_path.exists() or base.provider == "codex":
        return base
    entry = resolved["stages"][stage]
    fallback_provider = str(entry.get("fallback_provider") or "codex")
    fallback_model = str(entry.get("fallback_model") or "gpt-5.5")
    provider_entry = resolved["providers"].get(fallback_provider)
    if not isinstance(provider_entry, Mapping):
        raise ProviderPolicyError(
            f"stage {stage!r} freeze fallback names unknown provider "
            f"{fallback_provider!r}"
        )
    return StagePolicy(
        stage=stage,
        provider=fallback_provider,
        model=fallback_model,
        transport=str(provider_entry.get("transport") or ""),
        billing=str(provider_entry.get("billing") or ""),
    )
