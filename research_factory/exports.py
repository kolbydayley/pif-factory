from __future__ import annotations

import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .paths import exports_dir
from .util import now_iso, stable_id, write_text_atomic


def export_trend_report(conn, *, topic: str, window: str, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT labels.*, segments.episode_id, episodes.title, episodes.published_at, sources.name AS source_name
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = episodes.source_id
        WHERE labels.status = 'ready'
        ORDER BY episodes.published_at ASC
        """
    ).fetchall()
    bucket_counts: Counter[str] = Counter()
    evidence: list[dict[str, Any]] = []
    normalized_topic = topic.lower().replace("-", "_").replace(" ", "_")
    for row in rows:
        payload = json.loads(row["output_json"])
        matches = []
        for item in payload.get("topics", []):
            item_topic = str(item.get("topic", "")).lower()
            if normalized_topic in item_topic or topic.lower() in item_topic:
                matches.append(item)
        for claim in payload.get("claims", []):
            text = json.dumps(claim).lower()
            if topic.lower() in text:
                matches.append(claim)
        if not matches:
            continue
        bucket = _bucket(row["published_at"], window)
        bucket_counts[bucket] += len(matches)
        evidence.append(
            {
                "label_id": row["id"],
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "episode_title": row["title"],
                "source_name": row["source_name"],
                "published_at": row["published_at"],
                "matches": matches[:3],
            }
        )
    observation_rows = conn.execute(
        """
        SELECT
          coded_observations.*,
          labels.id AS label_id,
          labels.label_pack,
          segments.episode_id,
          episodes.title,
          episodes.published_at,
          sources.name AS source_name
        FROM coded_observations
        JOIN labels ON labels.id = coded_observations.label_id
        JOIN segments ON segments.id = coded_observations.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = episodes.source_id
        WHERE labels.status = 'ready'
          AND (
            LOWER(coded_observations.code_id) LIKE ?
            OR LOWER(coded_observations.code_family) LIKE ?
            OR LOWER(coded_observations.evidence_text) LIKE ?
          )
        ORDER BY episodes.published_at ASC
        """,
        (f"%{normalized_topic}%", f"%{normalized_topic}%", f"%{topic.lower()}%"),
    ).fetchall()
    for row in observation_rows:
        bucket = _bucket(row["published_at"], window)
        bucket_counts[bucket] += 1
        evidence.append(
            {
                "label_id": row["label_id"],
                "observation_id": row["id"],
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "episode_title": row["title"],
                "source_name": row["source_name"],
                "published_at": row["published_at"],
                "matches": [
                    {
                        "code_family": row["code_family"],
                        "code_id": row["code_id"],
                        "stance": row["stance"],
                        "evidence": row["evidence_text"],
                    }
                ],
            }
        )
    lines = [
        f"# Trend Report: {topic}",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Window: `{window}`",
        f"- Labels scanned: `{len(rows)}`",
        f"- Evidence segments: `{len(evidence)}`",
        "",
        "## Timeline",
        "",
    ]
    if bucket_counts:
        for bucket, count in sorted(bucket_counts.items()):
            lines.append(f"- `{bucket}`: {count}")
    else:
        lines.append("- No matching labels found yet.")
    lines.extend(["", "## Evidence Index", ""])
    for item in evidence[:100]:
        lines.append(
            f"- `{item['published_at'] or 'unknown-date'}` `{item['source_name']}` `{item['episode_id']}` "
            f"{item['episode_title']} (segment `{item['segment_id']}`)"
        )
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"trend-{topic}-{window}.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    memo_id = stable_id(topic, window, str(path), prefix="memo_")
    conn.execute(
        "INSERT OR IGNORE INTO trend_memos (id, topic, window, memo_path, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (memo_id, topic, window, str(path), json.dumps(evidence[:200], ensure_ascii=True), now_iso()),
    )
    conn.commit()
    return path


def export_graph(conn, *, graph_type: str, output: str | None = None) -> Path:
    graph: dict[str, Any] = {"type": graph_type, "generated_at": now_iso(), "nodes": [], "edges": []}
    if graph_type == "guest_network":
        _people_graph(conn, graph)
    elif graph_type == "org_network":
        _org_graph(conn, graph)
    elif graph_type == "concept_network":
        _concept_graph(conn, graph)
    else:
        raise ValueError("graph type must be guest_network, org_network, or concept_network")
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"{graph_type}.json"
    write_text_atomic(path, json.dumps(graph, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return path


def export_terminology_drift(conn, *, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT
          term_mentions.*,
          coded_observations.label_id,
          coded_observations.evidence_text,
          episodes.title,
          episodes.published_at,
          sources.name AS source_name
        FROM term_mentions
        JOIN coded_observations ON coded_observations.id = term_mentions.observation_id
        JOIN segments ON segments.id = term_mentions.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = episodes.source_id
        ORDER BY episodes.published_at DESC
        LIMIT 250
        """
    ).fetchall()
    lines = [
        "# Terminology Drift Table",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Rows: `{len(rows)}`",
        "",
        "| Date | Source | Term | Role | Evidence Trace |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['published_at'] or 'unknown'} | {row['source_name']} | {row['term']} | {row['term_role']} | "
            f"`{row['label_id']}` / `{row['observation_id']}` |"
        )
    if not rows:
        lines.append("| none | none | none | none | No terminology-drift observations have landed yet. |")
    path = Path(output).expanduser().resolve() if output else exports_dir() / "terminology-drift.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def export_product_correlation_memo(conn, *, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT
          product_signals.*,
          coded_observations.label_id,
          episodes.title,
          episodes.published_at,
          sources.name AS source_name
        FROM product_signals
        JOIN coded_observations ON coded_observations.id = product_signals.observation_id
        JOIN segments ON segments.id = product_signals.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = episodes.source_id
        ORDER BY episodes.published_at DESC
        LIMIT 250
        """
    ).fetchall()
    lines = [
        "# Product-Release Narrative Movement Memo",
        "",
        f"- Generated: `{now_iso()}`",
        "- Causality rule: treat podcast movement as narrative evidence, not proof of internal product causality.",
        f"- Product signals: `{len(rows)}`",
        "",
        "## Signals",
        "",
    ]
    if rows:
        for row in rows:
            product = row["product"] or "unknown-product"
            org = row["organization"] or "unknown-org"
            lines.append(
                f"- `{row['published_at'] or 'unknown-date'}` `{row['source_name']}` `{org}` / `{product}` "
                f"`{row['signal_type']}` trace `{row['label_id']}` / `{row['observation_id']}`"
            )
    else:
        lines.append("- No product-release signal observations have landed yet.")
    path = Path(output).expanduser().resolve() if output else exports_dir() / "product-release-narrative-memo.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def export_signal_report(conn, *, window: str, limit: int, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT shift_signals.*, signal_runs.window
        FROM shift_signals
        JOIN signal_runs ON signal_runs.id = shift_signals.signal_run_id
        WHERE signal_runs.window = ?
        ORDER BY shift_signals.score DESC, shift_signals.support DESC, shift_signals.created_at DESC
        LIMIT ?
        """,
        (window, limit),
    ).fetchall()
    lines = [
        "# Discourse Signal Report",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Window: `{window}`",
        f"- Signals: `{len(rows)}`",
        "- Validation note: unreviewed findings are `LLM-adjudicated`, not human-validated.",
        "- Evidence policy: trace IDs and offsets only; no full transcript text.",
        "",
        "| Rank | Type | Slice | Concept | Terms | Window | Score | Support | Status | Trace |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for rank, row in enumerate(rows, 1):
        terms = " -> ".join([item for item in [row["term_a"], row["term_b"]] if item])
        evidence = json.loads(row["evidence_json"] or "{}")
        trace_count = len(evidence.get("discourse_event_ids") or [])
        lines.append(
            f"| {rank} | {row['signal_type']} | {row['slice_key'] or 'all'} | {row['concept_name']} | {terms} | "
            f"{row['window_start']} to {row['window_end']} | {row['score']} | {row['support']} | {row['status']} | "
            f"`{row['id']}` / {trace_count} event trace(s) |"
        )
    if not rows:
        lines.append("| none | none | none | none | none | none | 0 | 0 | none | Run `detect-shifts` first. |")
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"signal-report-{window}.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def export_actor_stance_report(conn, *, window: str, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT
          actor_positions.actor_name,
          actor_positions.actor_type,
          actor_positions.actor_affiliation,
          actor_positions.concept_name,
          actor_positions.stance,
          actor_positions.claim_type,
          actor_positions.certainty,
          actor_positions.temporal_horizon,
          actor_positions.confidence,
          actor_positions.discourse_event_id,
          episodes.published_at,
          sources.name AS source_name
        FROM actor_positions
        JOIN segments ON segments.id = actor_positions.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        ORDER BY episodes.published_at DESC, actor_positions.created_at DESC
        LIMIT 500
        """
    ).fetchall()
    grouped: Counter[tuple[str, str, str]] = Counter()
    traces: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in rows:
        bucket = _bucket(row["published_at"], window)
        actor = row["actor_affiliation"] or row["actor_name"] or "unknown"
        concept = row["concept_name"] or "unknown_concept"
        stance = row["stance"] or "unknown"
        key = (bucket, actor, concept, stance)
        grouped[key] += 1
        traces[key].append(row["discourse_event_id"])
    lines = [
        "# Actor Stance Report",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Window: `{window}`",
        "- Evidence policy: trace IDs only; no full transcript text.",
        "",
        "| Window | Actor | Concept | Stance | Count | Trace |",
        "| --- | --- | --- | --- | ---: | --- |",
    ]
    for (bucket, actor, concept, stance), count in grouped.most_common(200):
        lines.append(f"| {bucket} | {actor} | {concept} | {stance} | {count} | {', '.join(f'`{item}`' for item in traces[(bucket, actor, concept, stance)][:3])} |")
    if not grouped:
        lines.append("| none | none | none | none | 0 | No actor positions have landed yet. |")
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"actor-stance-report-{window}.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def export_term_drift_report(conn, *, window: str, output: str | None = None) -> Path:
    rows = conn.execute(
        """
        SELECT
          term_usages.term,
          term_usages.concept_id,
          term_usages.actor_name,
          term_usages.discourse_event_id,
          concepts.canonical_name AS concept_name,
          episodes.published_at,
          sources.name AS source_name
        FROM term_usages
        LEFT JOIN concepts ON concepts.id = term_usages.concept_id
        JOIN segments ON segments.id = term_usages.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        ORDER BY episodes.published_at DESC
        LIMIT 1000
        """
    ).fetchall()
    grouped: Counter[tuple[str, str, str]] = Counter()
    traces: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in rows:
        bucket = _bucket(row["published_at"], window)
        concept = row["concept_name"] or row["concept_id"] or "unknown_concept"
        term = row["term"]
        key = (bucket, concept, term)
        grouped[key] += 1
        traces[key].append(row["discourse_event_id"])
    signal_rows = conn.execute(
        """
        SELECT signal_type, concept_name, term_a, term_b, window_start, window_end, score, support, id
        FROM shift_signals
        ORDER BY score DESC, support DESC, created_at DESC
        LIMIT 100
        """
    ).fetchall()
    lines = [
        "# Term Drift Report",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Window: `{window}`",
        "- Evidence policy: trace IDs only; no full transcript text.",
        "",
        "## Term Timeline",
        "",
        "| Window | Concept | Term | Count | Trace |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for (bucket, concept, term), count in grouped.most_common(250):
        lines.append(f"| {bucket} | {concept} | {term} | {count} | {', '.join(f'`{item}`' for item in traces[(bucket, concept, term)][:3])} |")
    if not grouped:
        lines.append("| none | none | none | 0 | No term usage rows have landed yet. |")
    lines.extend(["", "## Detected Drift Signals", "", "| Type | Concept | Terms | Window | Score | Support | Trace |", "| --- | --- | --- | --- | ---: | ---: | --- |"])
    for row in signal_rows:
        terms = " -> ".join([item for item in [row["term_a"], row["term_b"]] if item])
        lines.append(f"| {row['signal_type']} | {row['concept_name']} | {terms} | {row['window_start']} to {row['window_end']} | {row['score']} | {row['support']} | `{row['id']}` |")
    if not signal_rows:
        lines.append("| none | none | none | none | 0 | 0 | Run `detect-shifts` first. |")
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"term-drift-report-{window}.md"
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def export_narrative_map(conn, *, window: str, output: str | None = None) -> Path:
    graph: dict[str, Any] = {"type": "narrative_map", "window": window, "generated_at": now_iso(), "nodes": [], "edges": [], "signals": []}
    nodes: dict[str, dict[str, Any]] = {}
    edges: Counter[tuple[str, str, str]] = Counter()
    for row in conn.execute(
        """
        SELECT
          discourse_events.id,
          discourse_events.event_type,
          discourse_events.actor_name,
          discourse_events.actor_affiliation,
          discourse_events.candidate_concept,
          discourse_events.stance,
          discourse_events.surface_terms_json,
          discourse_events.product_names_json,
          discourse_events.organizations_json,
          episodes.published_at
        FROM discourse_events
        JOIN segments ON segments.id = discourse_events.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        ORDER BY episodes.published_at DESC
        LIMIT 1000
        """
    ).fetchall():
        concept = row["candidate_concept"] or "unknown_concept"
        actor = row["actor_affiliation"] or row["actor_name"] or "unknown_actor"
        nodes[concept] = {"id": concept, "label": concept, "kind": "concept"}
        nodes[actor] = {"id": actor, "label": actor, "kind": "actor"}
        edges[(actor, concept, row["event_type"] or "discusses")] += 1
        for term in json.loads(row["surface_terms_json"] or "[]"):
            nodes[term] = {"id": term, "label": term, "kind": "term"}
            edges[(concept, term, "uses_term")] += 1
        for product in json.loads(row["product_names_json"] or "[]"):
            nodes[product] = {"id": product, "label": product, "kind": "product"}
            edges[(concept, product, "mentions_product")] += 1
        for org in json.loads(row["organizations_json"] or "[]"):
            nodes[org] = {"id": org, "label": org, "kind": "org"}
            edges[(org, concept, "associated_with")] += 1
    graph["nodes"] = list(nodes.values())
    graph["edges"] = [{"source": a, "target": b, "kind": kind, "weight": weight} for (a, b, kind), weight in edges.items()]
    graph["signals"] = [
        dict(row)
        for row in conn.execute(
            "SELECT signal_type, slice_key, concept_name, term_a, term_b, window_start, window_end, score, support, status FROM shift_signals ORDER BY score DESC LIMIT 200"
        ).fetchall()
    ]
    path = Path(output).expanduser().resolve() if output else exports_dir() / f"narrative-map-{window}.json"
    write_text_atomic(path, json.dumps(graph, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return path


def _people_graph(conn, graph: dict[str, Any]) -> None:
    nodes = {}
    edges = Counter()
    rows = conn.execute("SELECT output_json, segment_id FROM labels WHERE label_pack = 'people_network_v1'").fetchall()
    for row in rows:
        payload = json.loads(row["output_json"])
        names = []
        for key in ["hosts", "guests", "mentioned_people"]:
            for item in payload.get(key, []):
                name = item.get("name") if isinstance(item, dict) else item
                if name:
                    names.append(str(name))
                    nodes[str(name)] = {"id": str(name), "label": str(name), "kind": "person"}
        for left in names:
            for right in names:
                if left < right:
                    edges[(left, right)] += 1
    for row in conn.execute("SELECT name, segment_id FROM entity_mentions WHERE entity_type = 'person'").fetchall():
        name = str(row["name"])
        nodes[name] = {"id": name, "label": name, "kind": "person"}
    for row in conn.execute("SELECT speaker, code_id FROM speaker_positions WHERE speaker IS NOT NULL").fetchall():
        speaker = str(row["speaker"])
        code = str(row["code_id"])
        nodes[speaker] = {"id": speaker, "label": speaker, "kind": "person"}
        nodes[code] = {"id": code, "label": code, "kind": "concept"}
        edges[(speaker, code)] += 1
    graph["nodes"] = list(nodes.values())
    graph["edges"] = [{"source": a, "target": b, "weight": w} for (a, b), w in edges.items()]


def _org_graph(conn, graph: dict[str, Any]) -> None:
    nodes = {}
    edges = Counter()
    rows = conn.execute("SELECT output_json FROM labels").fetchall()
    for row in rows:
        payload = json.loads(row["output_json"])
        orgs = []
        entities = payload.get("entities") if isinstance(payload.get("entities"), dict) else payload
        for item in entities.get("organizations", []) if isinstance(entities, dict) else []:
            name = item.get("name") if isinstance(item, dict) else item
            if name:
                orgs.append(str(name))
                nodes[str(name)] = {"id": str(name), "label": str(name), "kind": "org"}
        for left in orgs:
            for right in orgs:
                if left < right:
                    edges[(left, right)] += 1
    for row in conn.execute("SELECT name, role FROM entity_mentions WHERE entity_type = 'organization'").fetchall():
        name = str(row["name"])
        role = str(row["role"] or "mention")
        nodes[name] = {"id": name, "label": name, "kind": "org"}
        nodes[role] = {"id": role, "label": role, "kind": "concept"}
        edges[(name, role)] += 1
    for row in conn.execute("SELECT organization, product, signal_type FROM product_signals WHERE organization IS NOT NULL").fetchall():
        org = str(row["organization"])
        signal = str(row["signal_type"])
        nodes[org] = {"id": org, "label": org, "kind": "org"}
        nodes[signal] = {"id": signal, "label": signal, "kind": "signal"}
        edges[(org, signal)] += 1
        if row["product"]:
            product = str(row["product"])
            nodes[product] = {"id": product, "label": product, "kind": "product"}
            edges[(org, product)] += 1
    graph["nodes"] = list(nodes.values())
    graph["edges"] = [{"source": a, "target": b, "weight": w} for (a, b), w in edges.items()]


def _concept_graph(conn, graph: dict[str, Any]) -> None:
    nodes = {}
    edges = Counter()
    rows = conn.execute("SELECT output_json FROM labels WHERE label_pack = 'ai_discourse_v1'").fetchall()
    for row in rows:
        payload = json.loads(row["output_json"])
        topics = [item.get("topic") for item in payload.get("topics", []) if item.get("topic")]
        for topic in topics:
            nodes[topic] = {"id": topic, "label": topic, "kind": "concept"}
        for left in topics:
            for right in topics:
                if left < right:
                    edges[(left, right)] += 1
    for row in conn.execute("SELECT code_family, code_id FROM coded_observations").fetchall():
        family = str(row["code_family"])
        code = str(row["code_id"])
        nodes[family] = {"id": family, "label": family, "kind": "code_family"}
        nodes[code] = {"id": code, "label": code, "kind": "concept"}
        edges[(family, code)] += 1
    for row in conn.execute("SELECT canonical_name, status FROM concepts").fetchall():
        concept = str(row["canonical_name"])
        status = str(row["status"])
        nodes[concept] = {"id": concept, "label": concept, "kind": "concept", "status": status}
    for row in conn.execute("SELECT candidate_concept, event_type FROM discourse_events").fetchall():
        concept = str(row["candidate_concept"])
        event_type = str(row["event_type"])
        nodes[concept] = {"id": concept, "label": concept, "kind": "concept"}
        nodes[event_type] = {"id": event_type, "label": event_type, "kind": "event_type"}
        edges[(event_type, concept)] += 1
    graph["nodes"] = list(nodes.values())
    graph["edges"] = [{"source": a, "target": b, "weight": w} for (a, b), w in edges.items()]


def _bucket(value: str | None, window: str) -> str:
    if not value:
        return "unknown"
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    if window == "week":
        year, week, _ = parsed.isocalendar()
        return f"{year}-W{week:02d}"
    if window == "day":
        return parsed.date().isoformat()
    return f"{parsed.year}-{parsed.month:02d}"
