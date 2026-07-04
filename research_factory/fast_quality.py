from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from . import db
from .paths import exports_dir
from .scale_ops import reviewer_findings
from .util import dumps_json, now_iso, stable_id, write_text_atomic


SENTINEL_LIMIT_DEFAULT = 10


SYNTHETIC_FAILURE_FIXTURES = [
    {
        "id": "metric_false_positive_qualitative_comparison",
        "description": "Qualitative comparison was forced into metric.raw_text even though no actual measurement was present.",
        "expected_class": "metric_false_positive",
    },
    {
        "id": "weak_numeric_claim",
        "description": "A list number and timestamp were treated as a quantitative product-quality signal.",
        "expected_class": "numeric_artifact",
    },
    {
        "id": "speaker_label_only_identity",
        "description": "Transcript speaker label was extracted as a standalone identity graph event.",
        "expected_class": "speaker_label_only",
    },
    {
        "id": "overlap_duplicate_event",
        "description": "Duplicate of an event from overlapping segment text; supported but redundant.",
        "expected_class": "duplicate_event",
    },
    {
        "id": "missed_product_market_signal",
        "description": "Missed product-market signal: model release, pricing, budget, customer adoption, or release access change.",
        "expected_class": "product_market_signal",
    },
    {
        "id": "missed_benchmark_comparison",
        "description": "Benchmark comparison was missed: GPT 5.6 was on par with Mythos on Exploit Bench.",
        "expected_class": "benchmark_signal",
    },
    {
        "id": "missed_forecast_horizon",
        "description": "Near-term forecast was missed: substitution pressure is months away, not years.",
        "expected_class": "forecast_time_horizon",
    },
]


def classify_failure(text: Any) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if re.search(r"\bduplicate\b|\boverlap", value):
        return "duplicate_event"
    if re.search(r"\bspeaker label\b|\btranscript speaker\b|\bstandalone identity\b", value):
        return "speaker_label_only"
    if re.search(r"\bfootnote\b|\btimestamp\b|\blist number\b|\bnumeric artifact\b|\bmarker\b", value):
        return "numeric_artifact"
    if re.search(r"\bmetric\b|\bquantitative\b|\bqualitative comparison\b|\bbetter than\b|\boutweigh\b", value):
        return "metric_false_positive"
    if re.search(r"\bbenchmark\b|\bbench\b|\beval\b|\bscore\b|\bon par\b", value):
        return "benchmark_signal"
    if re.search(r"\bforecast\b|\bmonths away\b|\byears\b|\btime horizon\b|\bnear-term\b", value):
        return "forecast_time_horizon"
    if re.search(r"\bproduct\b|\bmarket\b|\bpricing\b|\bprice\b|\bbudget\b|\bcost\b|\bcustomer\b|\brelease\b|\bmodel\b|\badoption\b", value):
        return "product_market_signal"
    if re.search(r"\bidentity\b|\bguest\b|\bhost\b|\baffiliation\b|\balias\b|\bmentions whom\b", value):
        return "identity_graph"
    if re.search(r"\bground\b|\bevidence\b|\bunsupported\b|\bweak\b|\bfalse\b", value):
        return "grounding_precision"
    return "general_quality"


def build_failure_bank(
    conn,
    *,
    pilot_id: str,
    patch_tag: str,
    output: str | Path | None = None,
    check_only: bool = False,
) -> dict[str, Any]:
    deterministic = _deterministic_audit_failures(conn, pilot_id=pilot_id)
    reviewer = reviewer_findings(conn, pilot_id=pilot_id, severities=["P0", "P1"])
    reviewer_items = [
        {
            "source": "reviewer",
            "severity": item["severity"],
            "finding_type": item["finding_type"],
            "category": item["category"],
            "failure_class": classify_failure(item.get("description")),
            "episode_id": item.get("episode_id"),
            "segment_id": item.get("segment_id"),
            "discourse_event_id": item.get("discourse_event_id"),
            "description": item.get("description"),
        }
        for item in reviewer["findings"]
    ]
    synthetic_checks = [
        {
            **fixture,
            "actual_class": classify_failure(fixture["description"]),
            "passed": classify_failure(fixture["description"]) == fixture["expected_class"],
        }
        for fixture in SYNTHETIC_FAILURE_FIXTURES
    ]
    examples = deterministic + reviewer_items
    class_counts: dict[str, int] = {}
    for item in examples:
        failure_class = item.get("failure_class") or "general_quality"
        class_counts[failure_class] = class_counts.get(failure_class, 0) + 1
    status = "passed" if all(item["passed"] for item in synthetic_checks) else "failed"
    payload = {
        "ok": status == "passed",
        "pilot_id": pilot_id,
        "patch_tag": patch_tag,
        "tier": "tier0_failure_bank",
        "status": status,
        "privacy": "sanitized_no_transcript_text",
        "synthetic_checks": synthetic_checks,
        "synthetic_passed": sum(1 for item in synthetic_checks if item["passed"]),
        "synthetic_total": len(synthetic_checks),
        "deterministic_failure_count": len(deterministic),
        "reviewer_failure_count": len(reviewer_items),
        "failure_class_counts": class_counts,
        "examples": examples[:300],
    }
    if not check_only:
        _record_quality_iteration(conn, pilot_id=pilot_id, patch_tag=patch_tag, tier="tier0", status=status, metrics=payload)
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"failure-bank-{_safe(pilot_id)}-{_safe(patch_tag)}.json"
    write_text_atomic(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    payload["path"] = str(path)
    return payload


def queue_delta_audits(
    conn,
    *,
    pilot_id: str,
    label_pack: str,
    model: str,
    patch_tag: str,
    sentinel: int = SENTINEL_LIMIT_DEFAULT,
    fresh: bool = False,
) -> dict[str, Any]:
    selected = _delta_audit_label_rows(conn, pilot_id=pilot_id, label_pack=label_pack, sentinel=sentinel)
    queued = 0
    refreshed = 0
    skipped = 0
    for row in selected:
        payload = {
            "segment_id": row["segment_id"],
            "label_pack": label_pack,
            "model": model,
            "pilot_id": pilot_id,
            "patch_tag": patch_tag,
            "delta_audit": True,
            "selection_reason": row["selection_reason"],
            "failure_class": row["failure_class"],
        }
        if fresh:
            refreshed += conn.execute(
                """
                DELETE FROM jobs
                WHERE lane = 'quality'
                  AND job_type = 'audit_label'
                  AND target_id = ?
                  AND status IN ('completed', 'failed')
                  AND json_extract(payload_json, '$.delta_audit') = 1
                  AND json_extract(payload_json, '$.patch_tag') = ?
                """,
                (row["label_id"], patch_tag),
            ).rowcount
        existing = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE lane = 'quality'
              AND job_type = 'audit_label'
              AND target_id = ?
              AND status IN ('pending', 'claimed')
            LIMIT 1
            """,
            (row["label_id"],),
        ).fetchone()
        if existing:
            skipped += 1
            continue
        if db.enqueue_job(conn, lane="quality", job_type="audit_label", target_id=row["label_id"], payload=payload, priority=30):
            queued += 1
    result = {
        "ok": True,
        "pilot_id": pilot_id,
        "label_pack": label_pack,
        "model": model,
        "patch_tag": patch_tag,
        "selected": len(selected),
        "queued": queued,
        "skipped_active": skipped,
        "refreshed_jobs": refreshed,
        "failure_class_counts": _count_by(selected, "failure_class"),
        "selection_reason_counts": _count_by(selected, "selection_reason"),
    }
    _record_quality_iteration(conn, pilot_id=pilot_id, patch_tag=patch_tag, tier="tier1_delta_audit_queued", status="queued", metrics=result)
    conn.commit()
    return result


def quality_velocity(conn, *, pilot_id: str | None = None) -> dict[str, Any]:
    filter_sql = ""
    params: list[Any] = []
    if pilot_id:
        filter_sql = "WHERE pilot_id = ?"
        params.append(pilot_id)
    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT id, pilot_id, patch_tag, tier, status, metrics_json, created_at, completed_at
            FROM quality_iterations
            {filter_sql}
            ORDER BY created_at DESC
            LIMIT 20
            """,
            params,
        ).fetchall()
    ]
    rows = [_safe_quality_iteration(row) for row in rows]
    latest = rows[0] if rows else None
    raw_latest_metrics = {}
    if latest:
        raw_latest_metrics = latest.get("metrics", {})
    return {
        "pilot_id": pilot_id,
        "latest_patch_tag": latest["patch_tag"] if latest else None,
        "latest_tier": latest["tier"] if latest else None,
        "latest_status": latest["status"] if latest else None,
        "state": _iteration_state({**latest, "metrics": raw_latest_metrics} if latest else None),
        "recent_iterations": rows[:10],
    }


def _safe_quality_iteration(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    metrics = json.loads(item.pop("metrics_json") or "{}")
    item["metrics"] = {
        key: metrics.get(key)
        for key in [
            "ok",
            "status",
            "tier",
            "selected",
            "queued",
            "processed",
            "submitted",
            "failed",
            "deterministic_failure_count",
            "reviewer_failure_count",
            "synthetic_passed",
            "synthetic_total",
            "failure_class_counts",
            "selection_reason_counts",
        ]
        if key in metrics
    }
    return item


def _deterministic_audit_failures(conn, *, pilot_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH latest_labels AS (
          SELECT l.*, row_number() OVER (PARTITION BY l.segment_id ORDER BY l.created_at DESC, l.id DESC) AS rn
          FROM labels l
          WHERE l.label_pack = 'ai_discourse_v3_1'
        ),
        pilot_segments AS (
          SELECT DISTINCT target_id AS segment_id
          FROM jobs
          WHERE job_type = 'label_segment'
            AND json_extract(payload_json, '$.pilot_id') = ?
        ),
        latest_audits AS (
          SELECT qa.*, row_number() OVER (PARTITION BY qa.label_id ORDER BY qa.created_at DESC, qa.id DESC) AS rn
          FROM quality_audits qa
          WHERE qa.label_pack = 'ai_discourse_v3_1'
        )
        SELECT ll.id AS label_id, ll.segment_id, la.status, la.score, la.disagreement_json
        FROM pilot_segments ps
        JOIN latest_labels ll ON ll.segment_id = ps.segment_id AND ll.rn = 1
        LEFT JOIN latest_audits la ON la.label_id = ll.id AND la.rn = 1
        WHERE COALESCE(la.status, '') != 'passed'
        ORDER BY ll.segment_id
        """,
        (pilot_id,),
    ).fetchall()
    items = []
    for row in rows:
        issues = _audit_issues(row["disagreement_json"])
        failure_class = classify_failure(" ".join(issue.get("message", "") for issue in issues))
        items.append(
            {
                "source": "deterministic_audit",
                "severity": "P1",
                "label_id": row["label_id"],
                "segment_id": row["segment_id"],
                "audit_status": row["status"] or "missing",
                "score": row["score"],
                "failure_class": failure_class,
                "issue_count": len(issues),
                "description": _summarize_issues(issues),
            }
        )
    return items


def _delta_audit_label_rows(conn, *, pilot_id: str, label_pack: str, sentinel: int) -> list[dict[str, str]]:
    audit_failures = _deterministic_audit_failures(conn, pilot_id=pilot_id)
    segment_to_failure = {item["segment_id"]: item["failure_class"] for item in audit_failures}
    impacted = reviewer_findings(conn, pilot_id=pilot_id, severities=["P0", "P1"])["impacted_segment_ids"]
    selected: dict[str, dict[str, str]] = {}
    rows = conn.execute(
        """
        WITH latest_labels AS (
          SELECT l.*, row_number() OVER (PARTITION BY l.segment_id ORDER BY l.created_at DESC, l.id DESC) AS rn
          FROM labels l
          WHERE l.label_pack = ?
        )
        SELECT id AS label_id, segment_id
        FROM latest_labels
        WHERE rn = 1
        """,
        (label_pack,),
    ).fetchall()
    by_segment = {row["segment_id"]: row["label_id"] for row in rows}
    for segment_id, failure_class in segment_to_failure.items():
        label_id = by_segment.get(segment_id)
        if label_id:
            selected[label_id] = {"label_id": label_id, "segment_id": segment_id, "selection_reason": "deterministic_audit_failure", "failure_class": failure_class}
    for segment_id in impacted:
        label_id = by_segment.get(segment_id)
        if label_id:
            selected[label_id] = {"label_id": label_id, "segment_id": segment_id, "selection_reason": "reviewer_p0_p1_finding", "failure_class": "reviewer_finding"}
    sentinel_rows = conn.execute(
        """
        WITH latest_audits AS (
          SELECT qa.*, row_number() OVER (PARTITION BY qa.label_id ORDER BY qa.created_at DESC, qa.id DESC) AS rn
          FROM quality_audits qa
          WHERE qa.label_pack = ?
        )
        SELECT labels.id AS label_id, labels.segment_id
        FROM labels
        JOIN latest_audits ON latest_audits.label_id = labels.id AND latest_audits.rn = 1
        WHERE labels.label_pack = ?
          AND latest_audits.status = 'passed'
        ORDER BY labels.id
        LIMIT ?
        """,
        (label_pack, label_pack, sentinel),
    ).fetchall()
    for row in sentinel_rows:
        selected.setdefault(
            row["label_id"],
            {"label_id": row["label_id"], "segment_id": row["segment_id"], "selection_reason": "sentinel_previously_passing_label", "failure_class": "sentinel"},
        )
    return list(selected.values())


def _record_quality_iteration(conn, *, pilot_id: str, patch_tag: str, tier: str, status: str, metrics: dict[str, Any]) -> str:
    ts = now_iso()
    run_id = stable_id("quality_iteration", pilot_id, patch_tag, tier, ts, prefix="qit_")
    conn.execute(
        """
        INSERT INTO quality_iterations
          (id, pilot_id, patch_tag, tier, status, metrics_json, created_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, pilot_id, patch_tag, tier, status, dumps_json(metrics), ts, ts),
    )
    conn.commit()
    return run_id


def _audit_issues(value: str | None) -> list[dict[str, Any]]:
    try:
        payload = json.loads(value or "{}")
    except json.JSONDecodeError:
        return [{"message": "invalid audit json"}]
    issues = payload.get("issues") or []
    return [item for item in issues if isinstance(item, dict)]


def _summarize_issues(issues: list[dict[str, Any]]) -> str:
    messages = [str(item.get("message") or "") for item in issues[:3]]
    return "; ".join(message for message in messages if message)[:500]


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _iteration_state(latest: dict[str, Any] | None) -> str:
    if not latest:
        return "patch"
    tier = str(latest.get("tier") or "")
    status = str(latest.get("status") or "")
    if status in {"failed", "blocked"}:
        return "patch"
    if "tier0" in tier:
        return "targeted-review"
    if "tier1" in tier:
        return "full-gate"
    if "tier2" in tier:
        return "scale-candidate"
    return "patch"


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value)[:120]
