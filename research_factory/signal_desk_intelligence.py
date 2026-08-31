"""Trust-aware, presentation-safe payloads for Signal Desk.

This module deliberately does not mutate the canonical PIF database.  It turns
the read-only discourse aggregate into public, cited research payloads and
fails closed when attribution or excerpt quality is not strong enough.
"""

from __future__ import annotations

import hashlib
import datetime as dt
import re
from collections import Counter, defaultdict
from copy import deepcopy
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


SCHEMA_VERSION = "signal_desk_v5"
MIN_EXCERPT_CHARS = 40

_CHROME_PATTERNS = (
    "subscribe listen on", "privacy terms", "discussion about this video",
    "recent episodes", "ready for more", "rss feed", "apple podcasts",
    "spotify youtube", "comments restacks", "cookie policy", "sign in",
    "all rights reserved", "appears in episode", "download the app",
)
_SPONSOR_PATTERNS = (
    "brought to you by", "this episode is sponsored", "our sponsor",
    "use code ", "visit our sponsor", "limited time offer",
)
_PLACEHOLDER_PATTERNS = (
    "unknown host", "unlabeled", "the cloudcast", "practical ai host",
    "latent space host", "speaker 1", "speaker 2",
)
_NON_ISSUE_PATTERNS = (
    "identity affiliation", "speaker identity", "guest identity",
    "expert identity", "transcript quality", "audio quality",
)
_PERSON_ALIASES = {
    "daniel whitenack": "Daniel Witenack",
    "daniel witenack": "Daniel Witenack",
    "swyx": "Shawn Wang",
    "swyx / shawn wang": "Shawn Wang",
    "shawn wang": "Shawn Wang",
}


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", (value or "").casefold()).strip("-")
    return text or "unknown"


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.casefold().encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{slugify(value)[:44]}_{digest}"


def normalize_feed_url(value: str | None) -> str:
    if not value:
        return ""
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return value.strip().casefold()
    host = (parts.hostname or "").casefold()
    if host.startswith("www."):
        host = host[4:]
    port = f":{parts.port}" if parts.port else ""
    path = re.sub(r"/+", "/", parts.path or "/").rstrip("/") or "/"
    query = urlencode(sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.casefold().startswith(("utm_", "ref", "source"))
    ))
    return urlunsplit(((parts.scheme or "https").casefold(), host + port,
                       path, query, ""))


def canonical_person_name(value: str | None) -> str:
    name = re.sub(r"\s+", " ", (value or "").strip())
    return _PERSON_ALIASES.get(name.casefold(), name)


def is_primary_person(value: str | None) -> bool:
    name = canonical_person_name(value)
    folded = name.casefold()
    if not name or any(p in folded for p in _PLACEHOLDER_PATTERNS):
        return False
    if " and " in folded or "," in name:
        return False
    # Single-token entities are usually unresolved speaker labels.  Keep
    # recognized public aliases but quarantine generic first names.
    if len(name.split()) == 1 and folded not in {"swyx"}:
        return False
    return True


def is_public_issue(value: str | None) -> bool:
    folded = str(value or "").casefold()
    return bool(folded) and not any(pattern in folded
                                    for pattern in _NON_ISSUE_PATTERNS)


def clean_excerpt(value: str | None) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    text = re.sub(r"^(?:speaker\s*\d+|host|guest)\s*:\s*", "", text,
                  flags=re.IGNORECASE)
    return text.strip(" \t\n\r\"“”")


def _contains_any(text: str, patterns: Iterable[str]) -> bool:
    folded = text.casefold()
    return any(pattern in folded for pattern in patterns)


def _looks_like_mention(person: str, excerpt: str) -> bool:
    """Conservative proxy until canonical speaker spans are complete.

    Actor-position rows can identify the subject of a sentence rather than its
    speaker.  If the alleged speaker is named in the excerpt in third person,
    we expose it as a mention, never as a statement by that person.
    """
    person = canonical_person_name(person)
    tokens = [t for t in re.findall(r"[a-z]+", person.casefold()) if len(t) > 2]
    folded = excerpt.casefold()
    if tokens and (person.casefold() in folded or tokens[-1] in folded):
        return True
    return False


def classify_evidence(raw: dict[str, Any], alleged_person: str | None = None
                      ) -> dict[str, Any]:
    item = deepcopy(raw)
    text = clean_excerpt(item.get("evidence"))
    person = canonical_person_name(alleged_person or item.get("person"))
    role = str(item.get("role") or "person").casefold()
    confidence = float(item.get("confidence") or 0)
    speaker_attribution = item.get("speaker_attribution") or {}
    speaker_status = str(speaker_attribution.get("status") or "unresolved")
    reasons: list[str] = []

    if len(text) < MIN_EXCERPT_CHARS:
        reasons.append("excerpt_too_short")
    if _contains_any(text, _CHROME_PATTERNS):
        reasons.append("webpage_chrome")
    if _contains_any(text, _SPONSOR_PATTERNS):
        reasons.append("sponsor_copy")
    if not str(item.get("source_url") or "").startswith(("http://", "https://")):
        reasons.append("missing_original_source")

    if role == "organization":
        attribution_type = "mentioned_organization"
        reasons.append("organization_not_speaker")
    elif not is_primary_person(person):
        attribution_type = "unresolved_voice"
        reasons.append("unresolved_or_composite_person")
    elif speaker_status == "mentioned" or _looks_like_mention(person, text):
        attribution_type = "mentioned_person"
        reasons.append("third_person_reference")
    elif speaker_status != "direct":
        attribution_type = "unresolved_voice"
        reasons.append("speaker_not_verified")
    else:
        attribution_type = "direct_speech_verified"

    attribution_confidence = min(0.94, max(0.0, confidence))
    if attribution_type != "direct_speech_verified":
        attribution_confidence = min(attribution_confidence, 0.45)
    publishability = "accepted"
    if reasons or attribution_confidence < 0.72:
        publishability = "quarantined" if any(
            r in reasons for r in ("webpage_chrome", "sponsor_copy")
        ) else "uncertain"

    quality = round(max(0.0, min(1.0,
        0.45 * max(confidence, 0.0)
        + 0.20 * (1 if len(text) >= 90 else len(text) / 90)
        + 0.20 * (1 if item.get("context_before") or item.get("context_after") else 0)
        + 0.15 * (1 if item.get("source_url") else 0)
        - 0.35 * len(reasons)
    )), 3)

    item.update({
        "person": person or "Unattributed voice",
        "evidence": text,
        "attribution_type": attribution_type,
        "attribution_confidence": round(attribution_confidence, 3),
        "publishability": publishability,
        "quality_score": quality,
        "quality_reasons": reasons,
        "deduplication_key": hashlib.sha1(
            f"{item.get('episode_id','')}|{text.casefold()}".encode("utf-8")
        ).hexdigest()[:16],
    })
    return item


def _citation_sentence(text: str, evidence: list[dict[str, Any]]) -> dict[str, Any]:
    return {"text": text, "citations": [e["id"] for e in evidence[:2]]}


def _signal_copy(kind: str, item: dict[str, Any], topic: dict[str, Any]) -> str:
    if kind == "emerging":
        return (f"Discussion rose from a near-zero baseline to "
                f"{item.get('pulse_vol', topic.get('pulse_vol', 0))} tracked mentions "
                f"across {item.get('episodes', topic.get('pulse_episodes', 0))} episodes.")
    if kind == "shifting" and item.get("now") and item.get("before"):
        labels = ("supportive", "skeptical", "neutral")
        delta = [a - b for a, b in zip(item["now"], item["before"])]
        idx = max(range(len(delta)), key=lambda i: abs(delta[i]))
        return (f"The largest recorded stance movement was {labels[idx]}: "
                f"{item['before'][idx] * 100:.0f}% to {item['now'][idx] * 100:.0f}%.")
    if kind == "contested":
        return (f"The corpus contains {item.get('positive', 0)} supportive and "
                f"{item.get('negative', 0)} skeptical recorded positions.")
    if kind == "fading":
        return (f"Discussion fell from a prior monthly peak of "
                f"{item.get('peak_month_vol', 0)} mentions to "
                f"{item.get('pulse_vol', 0)} in the latest three-month window.")
    return (f"The issue appears in {topic.get('pulse_vol', 0)} recent mentions "
            f"across {topic.get('pulse_shows', 0)} shows.")


def build_issue_brief(name: str, topic: dict[str, Any], signal: dict[str, Any]
                     ) -> dict[str, Any]:
    accepted = topic.get("accepted_evidence", [])
    shows = {e.get("show") for e in accepted if e.get("show")}
    episodes = {e.get("episode_id") for e in accepted if e.get("episode_id")}
    people = {e.get("person") for e in accepted if e.get("person")}
    kind = signal.get("kind", "discussed")
    item = signal.get("item") or {}
    detector_tier = item.get("tier")
    threshold_met = (len(accepted) >= 3 and len(episodes) >= 3
                     and len(shows) >= 2 and len(people) >= 2)
    decision_grade = threshold_met and (
        detector_tier in {"strong", "moderate"} or kind == "accelerating"
    )
    confidence = "high" if decision_grade and detector_tier == "strong" \
        else "medium" if decision_grade else "watchlist"

    if accepted:
        what_changed = _citation_sentence(_signal_copy(kind, item, topic), accepted)
        why = _citation_sentence(
            f"The signal is supported by {len(episodes)} "
            f"{'episode' if len(episodes) == 1 else 'episodes'} across "
            f"{len(shows)} {'show' if len(shows) == 1 else 'shows'}; source "
            "breadth is the useful part of the change.",
            accepted,
        )
        groups = Counter(e.get("group", "neutral") for e in accepted)
        dominant = groups.most_common(1)[0][0] if groups else "mixed"
        implication = _citation_sentence(
            f"The accepted evidence currently leans {dominant}; treat that as a "
            "map of recorded discourse, not a forecast of the outcome.", accepted)
    else:
        what_changed = {"text": _signal_copy(kind, item, topic), "citations": []}
        why = {"text": "No publishable excerpt currently substantiates this signal.",
               "citations": []}
        implication = {"text": "Keep this on the watchlist until source-grounded evidence arrives.",
                       "citations": []}

    return {
        "issue_id": topic["id"],
        "title": name,
        "classification": kind,
        "detector_tier": detector_tier,
        "decision_grade": decision_grade,
        "confidence": confidence,
        "what_changed": what_changed,
        "why_it_matters": why,
        "implications": [implication],
        "watchpoints": [
            "Does the signal persist across the next two refreshes?",
            "Does source breadth grow rather than one show driving the movement?",
            "Do supportive and skeptical claims gain comparable evidence coverage?",
        ],
        "coverage": {
            "accepted_excerpts": len(accepted),
            "uncertain_excerpts": len(topic.get("uncertain_evidence", [])),
            "episodes": len(episodes),
            "shows": len(shows),
            "voices": len(people),
            "threshold_met": threshold_met,
        },
    }


def _detector_map(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for kind in ("emerging", "shifting", "contested", "fading"):
        for item in data.get("detectors", {}).get(kind, []):
            current = out.get(item["topic"])
            candidate = {"kind": kind, "item": deepcopy(item)}
            if not current or item.get("tier") == "strong":
                out[item["topic"]] = candidate
    return out


def prepare_topics(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    signals = _detector_map(data)
    topics: dict[str, Any] = {}
    aliases: dict[str, str] = {}
    for name, raw in data.get("topics", {}).items():
        if name == "other" or not is_public_issue(name):
            continue
        item = deepcopy(raw)
        issue_id = item.get("stable_issue_id") or stable_id("issue", name)
        item["id"] = issue_id
        item["name"] = name
        item["aliases"] = sorted(set(item.get("aliases") or []))
        classified = [classify_evidence(e) for e in item.get("evidence", [])]
        deduped: dict[str, dict[str, Any]] = {}
        for evidence in classified:
            key = evidence["deduplication_key"]
            if key not in deduped or evidence["quality_score"] > deduped[key]["quality_score"]:
                deduped[key] = evidence
        evidence = list(deduped.values())
        evidence.sort(key=lambda e: (e["publishability"] == "accepted",
                                     e["quality_score"], e.get("date", "")),
                      reverse=True)
        item["accepted_evidence"] = [e for e in evidence
                                     if e["publishability"] == "accepted"]
        item["uncertain_evidence"] = [e for e in evidence
                                      if e["publishability"] != "accepted"]
        item["evidence"] = item["accepted_evidence"]
        signal = signals.get(name)
        if signal is None and len(item["accepted_evidence"]) >= 3:
            recent = [float(s.get("share_smooth") or s.get("share") or 0)
                      for s in (item.get("series") or [])[-3:]]
            prior = [float(s.get("share_smooth") or s.get("share") or 0)
                     for s in (item.get("series") or [])[-6:-3]]
            recent_mean = sum(recent) / max(len(recent), 1)
            prior_mean = sum(prior) / max(len(prior), 1)
            if recent_mean > max(prior_mean * 1.2, 0):
                signal = {"kind": "accelerating",
                          "item": {"tier": "moderate"}}
        item["brief"] = build_issue_brief(
            name, item, signal or {"kind": "discussed", "item": {}}
        )
        topics[issue_id] = item
        for alias in [name, issue_id, slugify(name), *item["aliases"]]:
            aliases[str(alias).casefold()] = issue_id
    return topics, aliases


def _merge_topic_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        topic = str(row.get("topic") or "")
        if not topic:
            continue
        for key in ("count", "positive", "negative", "neutral"):
            grouped[topic][key] += int(row.get(key) or 0)
    return [dict(topic=topic, **counts) for topic, counts in sorted(
        grouped.items(), key=lambda pair: pair[1]["count"], reverse=True)]


def prepare_people(data: dict[str, Any], topic_aliases: dict[str, str]
                  ) -> tuple[dict[str, Any], dict[str, str]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in data.get("people", []):
        grouped[canonical_person_name(raw.get("name"))].append(raw)
    people: dict[str, Any] = {}
    aliases: dict[str, str] = {}
    for name, records in grouped.items():
        if not is_primary_person(name):
            continue
        person_id = stable_id("person", name)
        evidence: list[dict[str, Any]] = []
        mentions: list[dict[str, Any]] = []
        shows: set[str] = set()
        authority = None
        trust = None
        episode_count = 0
        for raw in records:
            shows.update(raw.get("shows") or [])
            episode_count += int(raw.get("n_episodes") or 0)
            authority = max(filter(lambda x: x is not None,
                                   (authority, raw.get("authority"))), default=None)
            if not trust or (raw.get("trust") or {}).get("score", 0) > trust.get("score", 0):
                trust = deepcopy(raw.get("trust"))
            for ev in raw.get("evidence") or []:
                classified = classify_evidence({**ev, "person": name}, name)
                topic_key = str(ev.get("topic") or "").casefold()
                classified["issue_id"] = topic_aliases.get(topic_key)
                if classified["attribution_type"] == "direct_speech_verified" \
                        and classified["publishability"] == "accepted":
                    evidence.append(classified)
                else:
                    mentions.append(classified)
        evidence = list({e["deduplication_key"]: e for e in evidence}.values())
        mentions = list({e["deduplication_key"]: e for e in mentions}.values())
        evidence.sort(key=lambda e: (e["quality_score"], e.get("date", "")),
                      reverse=True)
        direct_topics: dict[str, Counter] = defaultdict(Counter)
        for ev in evidence:
            if ev.get("topic"):
                direct_topics[ev["topic"]][ev.get("group", "neutral")] += 1
        topic_rows = [
            {"topic": topic, "count": sum(counts.values()),
             "positive": counts.get("positive", 0),
             "negative": counts.get("negative", 0),
             "neutral": counts.get("neutral", 0)}
            for topic, counts in direct_topics.items()
        ]
        topic_rows.sort(key=lambda row: row["count"], reverse=True)

        moves: list[dict[str, Any]] = []
        last_by_topic: dict[str, dict[str, Any]] = {}
        for ev in sorted(evidence, key=lambda row: row.get("date", "")):
            topic = ev.get("topic")
            if not topic or ev.get("group") == "neutral":
                continue
            previous = last_by_topic.get(topic)
            if previous and previous.get("group") != ev.get("group") \
                    and previous.get("episode_id") != ev.get("episode_id"):
                try:
                    from_date = dt.date.fromisoformat(previous["date"])
                    to_date = dt.date.fromisoformat(ev["date"])
                    days_apart = abs((to_date - from_date).days)
                except (KeyError, TypeError, ValueError):
                    days_apart = 0
                if days_apart >= 14:
                    moves.append({
                        "topic": topic, "from": previous["group"],
                        "to": ev["group"], "from_date": previous["date"],
                        "to_date": ev["date"],
                        "from_evidence_id": previous["id"],
                        "to_evidence_id": ev["id"],
                    })
            last_by_topic[topic] = ev
        person = {
            "id": person_id, "name": name,
            "aliases": sorted({r.get("name") for r in records if r.get("name")}),
            "authority": authority, "network_reach": trust,
            "n_episodes": episode_count, "shows": sorted(shows),
            "top_topics": topic_rows[:8],
            "moves": moves[-8:], "against_field": [],
            "direct_evidence": evidence[:40], "mentions": mentions[:40],
            "evidence_coverage": {
                "direct": len(evidence), "mentions": len(mentions),
                "shows": len({e.get("show") for e in evidence if e.get("show")}),
            },
        }
        people[person_id] = person
        for alias in [name, person_id, slugify(name), *person["aliases"]]:
            aliases[str(alias).casefold()] = person_id
    return people, aliases


def prepare_network(data: dict[str, Any], person_aliases: dict[str, str],
                    people: dict[str, Any]) -> dict[str, Any]:
    edges: Counter[tuple[str, str]] = Counter()
    for edge in data.get("network", {}).get("edges", []):
        a = person_aliases.get(canonical_person_name(edge.get("a")).casefold())
        b = person_aliases.get(canonical_person_name(edge.get("b")).casefold())
        if not a or not b or a == b:
            continue
        edges[tuple(sorted((a, b)))] += float(edge.get("w") or 0)
    adjacency: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (a, b), weight in edges.items():
        adjacency[a].append({"person_id": b, "weight": weight,
                             "name": people[b]["name"]})
        adjacency[b].append({"person_id": a, "weight": weight,
                             "name": people[a]["name"]})
    for rows in adjacency.values():
        rows.sort(key=lambda row: row["weight"], reverse=True)
    return {"relationship": "co_appearance", "people": adjacency}


def prepare_funnel(raw: dict[str, Any]) -> dict[str, Any]:
    funnel = deepcopy(raw)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for show in funnel.get("shows", []):
        groups[normalize_feed_url(show.get("rss_url"))].append(show)
    canonical: list[dict[str, Any]] = []
    duplicate_groups: list[dict[str, Any]] = []
    for feed, shows in groups.items():
        ranked = sorted(shows, key=lambda s: (
            int(s.get("intelligence_ready") or
                s.get("intelligence_ready_episodes") or 0),
            int(s.get("catalogued") or s.get("catalogued_episodes") or 0)),
                        reverse=True)
        primary = deepcopy(ranked[0])
        primary["id"] = stable_id("show", feed or primary.get("name", ""))
        primary["aliases"] = [s.get("name") for s in ranked[1:]]
        primary["duplicate_source"] = len(ranked) > 1
        primary["catalogued_episodes"] = int(
            primary.get("catalogued") or primary.get("catalogued_episodes") or 0
        )
        primary["transcript_attempts"] = int(
            primary.get("transcript_attempted") or
            primary.get("transcript_attempts") or 0
        )
        primary["segmented_episodes"] = int(
            primary.get("segmented") or primary.get("segmented_episodes") or 0
        )
        primary["intelligence_ready_episodes"] = int(
            primary.get("intelligence_ready") or
            primary.get("intelligence_ready_episodes") or 0
        )
        catalogued = primary["catalogued_episodes"]
        ready = primary["intelligence_ready_episodes"]
        primary["coverage_percent"] = round(100 * ready / max(catalogued, 1), 1)
        primary["missing_episodes"] = max(0, catalogued - ready)
        canonical.append(primary)
        if len(ranked) > 1:
            duplicate_groups.append({
                "feed_url": feed,
                "canonical_show_id": primary["id"],
                "names": [s.get("name") for s in ranked],
                "counts": [{
                    "name": s.get("name"),
                    "catalogued": s.get("catalogued",
                                         s.get("catalogued_episodes", 0)),
                    "ready": s.get("intelligence_ready",
                                   s.get("intelligence_ready_episodes", 0)),
                } for s in ranked],
            })
    canonical.sort(key=lambda s: (s["missing_episodes"],
                                  s.get("transcript_quarantined", 0)),
                   reverse=True)
    funnel["shows"] = canonical
    funnel["raw_show_count"] = len(raw.get("shows", []))
    funnel["canonical_show_count"] = len(canonical)
    funnel["duplicate_source_groups"] = duplicate_groups
    return funnel


def build_payloads(data: dict[str, Any], diff: dict[str, Any] | None = None
                  ) -> dict[str, dict[str, Any]]:
    topics, topic_aliases = prepare_topics(data)
    people, person_aliases = prepare_people(data, topic_aliases)
    network = prepare_network(data, person_aliases, people)
    funnel = prepare_funnel(data.get("funnel", {}))

    topic_index = []
    for issue_id, topic in topics.items():
        series = topic.get("series") or []
        latest = series[-1] if series else {}
        topic_index.append({
            "id": issue_id, "name": topic["name"], "aliases": topic["aliases"],
            "total": topic.get("total", 0), "pulse_vol": topic.get("pulse_vol", 0),
            "pulse_shows": topic.get("pulse_shows", 0),
            "pulse_episodes": topic.get("pulse_episodes", 0),
            "latest_share": latest.get("share_smooth", latest.get("share", 0)),
            "series_preview": series[-12:], "brief": topic["brief"],
        })
    topic_index.sort(key=lambda t: (
        t["brief"]["decision_grade"], t["pulse_shows"], t["pulse_vol"]),
                     reverse=True)

    people_index = [{
        "id": pid, "name": p["name"], "authority": p["authority"],
        "network_reach": p["network_reach"], "n_episodes": p["n_episodes"],
        "shows": p["shows"], "top_topics": p["top_topics"][:3],
        "moves_count": len(p["moves"]),
        "contrarian_count": len(p["against_field"]),
        "direct_evidence_count": len(p["direct_evidence"]),
    } for pid, p in people.items()]
    people_index.sort(key=lambda p: (
        p["authority"] is not None, p["authority"] or 0,
        p["direct_evidence_count"], p["n_episodes"]), reverse=True)

    briefing = []
    for topic in topic_index:
        brief = topic["brief"]
        bucket = {
            "emerging": "new", "shifting": "changing_consensus",
            "fading": "fading", "contested": "changing_consensus",
        }.get(brief["classification"], "accelerating")
        if not brief["decision_grade"]:
            bucket = "watchlist"
        briefing.append({**topic, "bucket": bucket})

    common = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": data.get("generated_at"),
        "data_through": data.get("data_through"),
        "latest_episode": data.get("latest_episode"),
    }
    return {
        "index": {**common, "corpus": data.get("corpus", {}),
                  "months": data.get("months", []),
                  "month_totals": data.get("month_totals", []),
                  "briefing": briefing, "issues": topic_index,
                  "voices": people_index, "diff": diff,
                  "aliases": {"issues": topic_aliases,
                              "voices": person_aliases}},
        "issues": {**common, "issues": topics},
        "voices": {**common, "voices": people},
        "network": {**common, "network": network},
        "coverage": {**common, "funnel": funnel},
    }
