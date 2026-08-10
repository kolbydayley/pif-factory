"""GLM<->Codex provider-agreement loop with fail-closed auto-freeze (Phase 3).

Kolby's ruling: GLM takes a bulk stage only with measured agreement; a breach
auto-freezes the GLM share back to Codex without waiting for a human.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from research_factory import provider_agreement as pa


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    pa.ensure_agreement_schema(conn)
    return conn


def _record(conn, *, day: str, stage: str, agree: bool, sample: str) -> None:
    pa.record_agreement(
        conn,
        day=day,
        stage=stage,
        sample_id=sample,
        glm_output_sha256="a" * 64,
        codex_output_sha256=("a" * 64) if agree else ("b" * 64),
        exact_match=agree,
        agreement_score=1.0 if agree else 0.0,
        disagreement_fields=[] if agree else ["claim_text"],
    )


def test_daily_agreement_aggregates() -> None:
    conn = _conn()
    for index in range(9):
        _record(conn, day="2026-08-10", stage="episode_context", agree=True, sample=f"s{index}")
    _record(conn, day="2026-08-10", stage="episode_context", agree=False, sample="s9")
    summary = pa.daily_agreement(conn, day="2026-08-10", stage="episode_context")
    assert summary["samples"] == 10
    assert summary["agreement_rate"] == 0.9


def test_gate_passes_at_or_above_threshold(tmp_path: Path) -> None:
    conn = _conn()
    for index in range(20):
        _record(conn, day="2026-08-10", stage="episode_context", agree=index != 0, sample=f"s{index}")
    # 19/20 = 0.95 exactly meets the episode_context threshold.
    gate = pa.agreement_gate(
        conn, day="2026-08-10", stage="episode_context", freeze_dir=tmp_path
    )
    assert gate["passed"] is True
    assert gate["threshold"] == 0.95
    assert not (tmp_path / "FREEZE_episode_context").exists()


def test_breach_engages_the_freeze_file(tmp_path: Path) -> None:
    conn = _conn()
    for index in range(10):
        _record(conn, day="2026-08-10", stage="episode_context", agree=index >= 2, sample=f"s{index}")
    # 8/10 = 0.80 < 0.95: breach.
    gate = pa.agreement_gate(
        conn, day="2026-08-10", stage="episode_context", freeze_dir=tmp_path
    )
    assert gate["passed"] is False
    assert gate["frozen"] is True
    assert (tmp_path / "FREEZE_episode_context").exists()


def test_no_samples_is_not_a_pass(tmp_path: Path) -> None:
    conn = _conn()
    gate = pa.agreement_gate(
        conn, day="2026-08-10", stage="episode_context", freeze_dir=tmp_path
    )
    assert gate["passed"] is False
    assert gate["reason"] == "no_samples"
    # But an empty day does not engage the freeze - nothing was measured.
    assert gate["frozen"] is False


def test_effective_provider_downgrades_to_codex_when_frozen(tmp_path: Path) -> None:
    policy = {
        "schema_version": "pif_provider_policy_v1",
        "fail_closed": True,
        "providers": {
            "codex": {"transport": "codex_exec_subscription", "billing": "codex_subscription"},
            "glm_opencode": {"transport": "opencode_local", "billing": "opencode_glm"},
        },
        "stages": {
            "episode_context": {
                "provider": "glm_opencode",
                "model": "opencode-go/glm-5.2",
                "fallback_provider": "codex",
                "fallback_model": "gpt-5.5",
                "agreement_threshold": 0.95,
            }
        },
    }
    resolved = pa.effective_stage_policy(
        "episode_context", policy=policy, freeze_dir=tmp_path
    )
    assert resolved.provider == "glm_opencode"

    (tmp_path / "FREEZE_episode_context").write_text("{}", encoding="utf-8")
    frozen = pa.effective_stage_policy(
        "episode_context", policy=policy, freeze_dir=tmp_path
    )
    assert frozen.provider == "codex"
    assert frozen.model == "gpt-5.5"


def test_freeze_persists_until_removed(tmp_path: Path) -> None:
    conn = _conn()
    for index in range(10):
        _record(conn, day="2026-08-10", stage="episode_context", agree=index >= 2, sample=f"s{index}")
    pa.agreement_gate(conn, day="2026-08-10", stage="episode_context", freeze_dir=tmp_path)
    # A later perfect day does NOT auto-unfreeze: a human reviews the breach.
    for index in range(10):
        _record(conn, day="2026-08-11", stage="episode_context", agree=True, sample=f"t{index}")
    gate = pa.agreement_gate(
        conn, day="2026-08-11", stage="episode_context", freeze_dir=tmp_path
    )
    assert gate["passed"] is True
    assert gate["frozen"] is True
    assert (tmp_path / "FREEZE_episode_context").exists()
