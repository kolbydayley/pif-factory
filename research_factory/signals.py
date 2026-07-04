from __future__ import annotations

import datetime as dt
import json
from collections import Counter, defaultdict
from typing import Any

from . import db
from .discourse import normalize_concept_name, upsert_concept
from .util import dumps_json, now_iso, parse_datetime, stable_id


def discover_concepts(
    conn,
    *,
    window: str,
    min_evidence: int,
    min_source_diversity: int = 2,
    min_usefulness: float = 0.6,
) -> dict[str, Any]:
    rows = _event_rows(conn)
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = dict(row)
        candidate = str(event.get("candidate_concept") or event.get("canonical_concept_name") or "").strip()
        if not candidate:
            continue
        normalized = normalize_concept_name(candidate)
        bucket = buckets.setdefault(
            normalized,
            {
                "candidate": candidate,
                "sources": set(),
                "events": [],
                "terms": Counter(),
                "usefulness": 0.0,
            },
        )
        bucket["sources"].add(str(event.get("source_name") or "unknown"))
        bucket["events"].append(event)
        for term in _json_list(event.get("surface_terms_json")) + _json_list(event.get("model_names_json")) + _json_list(event.get("product_names_json")):
            bucket["terms"][term] += 1
        if event.get("confidence") is not None:
            bucket["usefulness"] = max(float(bucket["usefulness"]), min(0.95, float(event["confidence"]) + 0.08))
    created = 0
    needs_adjudication = 0
    aliases_updated = 0
    queued = 0
    ts = now_iso()
    for normalized, bucket in buckets.items():
        evidence_count = len(bucket["events"])
        source_diversity = len(bucket["sources"])
        usefulness = float(bucket["usefulness"] or 0.6)
        candidate = str(bucket["candidate"])
        concept_id = upsert_concept(
            conn,
            candidate,
            surface_terms=list(bucket["terms"].keys()),
            usefulness_score=usefulness,
            description=f"Discovered from ai_discourse_v3 events over {window} windows.",
            ts=ts,
        )
        candidate_id = stable_id(normalized, prefix="ccand_")
        status = "needs_adjudication" if evidence_count >= min_evidence and source_diversity >= min_source_diversity and usefulness >= min_usefulness else "candidate"
        if status == "needs_adjudication":
            needs_adjudication += 1
        evidence = [
            {
                "discourse_event_id": event["id"],
                "label_id": event["label_id"],
                "segment_id": event["segment_id"],
                "episode_id": event["episode_id"],
                "published_at": event["published_at"],
                "source_name": event["source_name"],
            }
            for event in bucket["events"][:10]
        ]
        cur = conn.execute(
            """
            INSERT INTO concept_candidates
              (id, candidate, normalized_candidate, status, surface_terms_json, evidence_count, source_diversity,
               usefulness_score, rationale, evidence_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(normalized_candidate) DO UPDATE SET
              status = excluded.status,
              surface_terms_json = excluded.surface_terms_json,
              evidence_count = excluded.evidence_count,
              source_diversity = excluded.source_diversity,
              usefulness_score = excluded.usefulness_score,
              rationale = excluded.rationale,
              evidence_json = excluded.evidence_json,
              updated_at = excluded.updated_at
            """,
            (
                candidate_id,
                candidate,
                normalized,
                status,
                dumps_json([term for term, _ in bucket["terms"].most_common(30)]),
                evidence_count,
                source_diversity,
                usefulness,
                f"{evidence_count} event(s) across {source_diversity} source(s).",
                dumps_json(evidence),
                ts,
                ts,
            ),
        )
        if cur.rowcount:
            created += 1
        before_changes = conn.total_changes
        conn.execute(
            """
            UPDATE concept_aliases
            SET evidence_count = ?,
                source_diversity = ?,
                status = CASE
                  WHEN ? = 'needs_adjudication' AND status = 'candidate' THEN 'needs_adjudication'
                  ELSE status
                END,
                updated_at = ?
            WHERE concept_id = ?
            """,
            (evidence_count, source_diversity, status, ts, concept_id),
        )
        aliases_updated += conn.total_changes - before_changes
        if status == "needs_adjudication":
            job_id = db.enqueue_job(
                conn,
                lane="quality",
                job_type="adjudicate_concept",
                target_id=candidate_id,
                payload={"candidate": candidate, "concept_id": concept_id, "window": window, "reason": "concept_promotion_threshold_met"},
                priority=40,
                max_attempts=1,
            )
            if job_id:
                queued += 1
    conn.commit()
    return {
        "ok": True,
        "window": window,
        "concepts_seen": len(buckets),
        "candidates_upserted": created,
        "needs_adjudication": needs_adjudication,
        "adjudication_jobs": queued,
        "aliases_updated": aliases_updated,
        "thresholds": {
            "min_evidence": min_evidence,
            "min_source_diversity": min_source_diversity,
            "min_usefulness": min_usefulness,
        },
    }


def detect_shifts(conn, *, slice_key: str | None, window: str, min_support: int) -> dict[str, Any]:
    rows = [row for row in _event_rows(conn) if _matches_slice(row, slice_key)]
    run_id = stable_id("detect_shifts", slice_key or "all", window, now_iso(), prefix="srun_")
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO signal_runs
          (id, run_type, window, slice_key, parameters_json, status, metrics_json, created_at)
        VALUES (?, 'detect_shifts', ?, ?, ?, 'running', '{}', ?)
        """,
        (run_id, window, slice_key, dumps_json({"min_support": min_support}), ts),
    )
    term_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    concept_event_ids: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    stance_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        bucket = _bucket(row["published_at"], window)
        concept = str(row["candidate_concept"] or row["canonical_concept_name"] or "unknown_concept")
        concept_key = normalize_concept_name(concept)
        terms = _json_list(row["surface_terms_json"]) + _json_list(row["model_names_json"]) + _json_list(row["product_names_json"])
        for term in terms:
            clean_term = str(term).strip()
            if not clean_term:
                continue
            term_counts[(concept_key, bucket)][clean_term] += 1
            concept_event_ids[(concept_key, bucket, clean_term)].append(row["id"])
        if row["stance"]:
            stance_counts[(concept_key, bucket)][str(row["stance"])] += 1
    signals = []
    signals.extend(_burst_signals(conn, run_id, slice_key, term_counts, concept_event_ids, min_support, ts))
    signals.extend(_substitution_signals(conn, run_id, slice_key, term_counts, concept_event_ids, min_support, ts))
    signals.extend(_stance_shift_signals(conn, run_id, slice_key, stance_counts, min_support, ts))
    conn.execute(
        """
        UPDATE signal_runs
        SET status = 'completed',
            metrics_json = ?
        WHERE id = ?
        """,
        (dumps_json({"events_scanned": len(rows), "signals": len(signals)}), run_id),
    )
    conn.commit()
    return {
        "ok": True,
        "signal_run_id": run_id,
        "slice": slice_key or "all",
        "window": window,
        "events_scanned": len(rows),
        "signals": len(signals),
        "signal_preview": signals[:10],
    }


def _event_rows(conn) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              discourse_events.*,
              episodes.id AS episode_id,
              episodes.published_at,
              episodes.title AS episode_title,
              sources.name AS source_name
            FROM discourse_events
            JOIN segments ON segments.id = discourse_events.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = segments.source_id
            ORDER BY episodes.published_at ASC, discourse_events.created_at ASC
            """
        ).fetchall()
    ]


def _burst_signals(
    conn,
    run_id: str,
    slice_key: str | None,
    term_counts: dict[tuple[str, str], Counter[str]],
    concept_event_ids: dict[tuple[str, str, str], list[str]],
    min_support: int,
    ts: str,
) -> list[dict[str, Any]]:
    signals = []
    by_concept: dict[str, list[str]] = defaultdict(list)
    for concept, bucket in term_counts:
        by_concept[concept].append(bucket)
    for concept, buckets in by_concept.items():
        prior: Counter[str] = Counter()
        for bucket in sorted(set(buckets)):
            current = term_counts[(concept, bucket)]
            prior_total = max(sum(prior.values()), 1)
            for term, count in current.items():
                prior_count = prior.get(term, 0)
                if count < min_support:
                    continue
                if prior_count and count < prior_count * 2:
                    continue
                if not prior_count and sum(current.values()) < max(min_support, 2):
                    continue
                score = round((count + 1) / (prior_count + 1), 3)
                signal = _insert_signal(
                    conn,
                    run_id=run_id,
                    signal_type="term_burst",
                    slice_key=slice_key,
                    concept_name=concept,
                    term_a=term,
                    term_b=None,
                    window_start=bucket,
                    window_end=bucket,
                    score=score,
                    support=count,
                    summary=f"Term `{term}` burst for concept `{concept}` in {bucket}.",
                    event_ids=concept_event_ids[(concept, bucket, term)],
                    ts=ts,
                )
                signals.append(signal)
            prior.update(current)
    return signals


def _substitution_signals(
    conn,
    run_id: str,
    slice_key: str | None,
    term_counts: dict[tuple[str, str], Counter[str]],
    concept_event_ids: dict[tuple[str, str, str], list[str]],
    min_support: int,
    ts: str,
) -> list[dict[str, Any]]:
    signals = []
    by_concept: dict[str, list[str]] = defaultdict(list)
    for concept, bucket in term_counts:
        by_concept[concept].append(bucket)
    for concept, buckets in by_concept.items():
        sorted_buckets = sorted(set(buckets))
        for previous, current_bucket in zip(sorted_buckets, sorted_buckets[1:]):
            previous_counts = term_counts[(concept, previous)]
            current_counts = term_counts[(concept, current_bucket)]
            if sum(current_counts.values()) < min_support or sum(previous_counts.values()) < min_support:
                continue
            prev_total = sum(previous_counts.values())
            curr_total = sum(current_counts.values())
            prev_top, prev_count = previous_counts.most_common(1)[0]
            curr_top, curr_count = current_counts.most_common(1)[0]
            if prev_top == curr_top:
                continue
            prev_share = prev_count / prev_total
            curr_share = curr_count / curr_total
            old_share_now = current_counts.get(prev_top, 0) / curr_total
            if prev_share < 0.35 or curr_share < 0.35 or old_share_now > prev_share - 0.2:
                continue
            score = round((prev_share - old_share_now) + curr_share, 3)
            event_ids = concept_event_ids[(concept, previous, prev_top)] + concept_event_ids[(concept, current_bucket, curr_top)]
            signal = _insert_signal(
                conn,
                run_id=run_id,
                signal_type="term_substitution",
                slice_key=slice_key,
                concept_name=concept,
                term_a=prev_top,
                term_b=curr_top,
                window_start=previous,
                window_end=current_bucket,
                score=score,
                support=prev_count + curr_count,
                summary=f"Term use shifted from `{prev_top}` toward `{curr_top}` for concept `{concept}`.",
                event_ids=event_ids,
                ts=ts,
            )
            signals.append(signal)
    return signals


def _stance_shift_signals(
    conn,
    run_id: str,
    slice_key: str | None,
    stance_counts: dict[tuple[str, str], Counter[str]],
    min_support: int,
    ts: str,
) -> list[dict[str, Any]]:
    signals = []
    by_concept: dict[str, list[str]] = defaultdict(list)
    for concept, bucket in stance_counts:
        by_concept[concept].append(bucket)
    for concept, buckets in by_concept.items():
        sorted_buckets = sorted(set(buckets))
        for previous, current_bucket in zip(sorted_buckets, sorted_buckets[1:]):
            prev_counts = stance_counts[(concept, previous)]
            curr_counts = stance_counts[(concept, current_bucket)]
            if sum(prev_counts.values()) < min_support or sum(curr_counts.values()) < min_support:
                continue
            prev_top, prev_count = prev_counts.most_common(1)[0]
            curr_top, curr_count = curr_counts.most_common(1)[0]
            if prev_top == curr_top:
                continue
            score = round((prev_count / sum(prev_counts.values())) + (curr_count / sum(curr_counts.values())), 3)
            signal = _insert_signal(
                conn,
                run_id=run_id,
                signal_type="stance_shift",
                slice_key=slice_key,
                concept_name=concept,
                term_a=prev_top,
                term_b=curr_top,
                window_start=previous,
                window_end=current_bucket,
                score=score,
                support=prev_count + curr_count,
                summary=f"Dominant stance shifted from `{prev_top}` to `{curr_top}` for concept `{concept}`.",
                event_ids=[],
                ts=ts,
            )
            signals.append(signal)
    return signals


def _insert_signal(
    conn,
    *,
    run_id: str,
    signal_type: str,
    slice_key: str | None,
    concept_name: str,
    term_a: str,
    term_b: str | None,
    window_start: str,
    window_end: str,
    score: float,
    support: int,
    summary: str,
    event_ids: list[str],
    ts: str,
) -> dict[str, Any]:
    normalized = normalize_concept_name(concept_name)
    concept_id = stable_id(normalized, prefix="con_")
    signal_id = stable_id(run_id, signal_type, slice_key or "all", concept_name, term_a, term_b or "", window_start, window_end, prefix="sig_")
    evidence = {"discourse_event_ids": event_ids[:20], "evidence_policy": "trace_ids_only_no_raw_transcript"}
    conn.execute(
        """
        INSERT OR REPLACE INTO shift_signals
          (id, signal_run_id, signal_type, slice_key, concept_id, concept_name, term_a, term_b,
           window_start, window_end, score, support, status, summary, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'llm_adjudicated', ?, ?, ?)
        """,
        (
            signal_id,
            run_id,
            signal_type,
            slice_key,
            concept_id,
            concept_name,
            term_a,
            term_b,
            window_start,
            window_end,
            score,
            support,
            summary,
            dumps_json(evidence),
            ts,
        ),
    )
    return {
        "signal_id": signal_id,
        "signal_type": signal_type,
        "concept": concept_name,
        "term_a": term_a,
        "term_b": term_b,
        "window_start": window_start,
        "window_end": window_end,
        "score": score,
        "support": support,
        "summary": summary,
    }


def _matches_slice(row: dict[str, Any], slice_key: str | None) -> bool:
    if not slice_key:
        return True
    if ":" not in slice_key:
        needle = slice_key.lower()
        return needle in json.dumps(row, ensure_ascii=True).lower()
    kind, value = slice_key.split(":", 1)
    needle = value.strip().lower()
    if kind == "org":
        return needle in " ".join(_json_list(row.get("organizations_json")) + [str(row.get("actor_affiliation") or ""), str(row.get("actor_name") or "")]).lower()
    if kind == "actor":
        return needle in str(row.get("actor_name") or "").lower()
    if kind == "source":
        return needle in str(row.get("source_name") or "").lower()
    if kind == "concept":
        return needle in str(row.get("candidate_concept") or "").lower()
    return needle in json.dumps(row, ensure_ascii=True).lower()


def _bucket(value: str | None, window: str) -> str:
    parsed = parse_datetime(value)
    if not parsed:
        return "unknown"
    if window == "day":
        return parsed.date().isoformat()
    if window == "week":
        year, week, _ = parsed.isocalendar()
        return f"{year}-W{week:02d}"
    if window == "quarter":
        quarter = ((parsed.month - 1) // 3) + 1
        return f"{parsed.year}-Q{quarter}"
    return f"{parsed.year}-{parsed.month:02d}"


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except Exception:
            return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]
