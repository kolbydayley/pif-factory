from __future__ import annotations

import re
from typing import Any

from .util import dumps_json, now_iso, stable_id


def normalize_concept_name(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return text or "unknown_concept"


def insert_discourse_events(conn, output: dict[str, Any], *, segment_id: str, label_id: str) -> None:
    events = output.get("discourse_events")
    if not isinstance(events, list):
        return
    segment = conn.execute("SELECT episode_id FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not segment:
        raise ValueError(f"Segment not found: {segment_id}")
    episode_id = segment["episode_id"]
    ts = now_iso()
    candidates = {
        str(item.get("candidate")): item
        for item in output.get("concept_candidates") or []
        if isinstance(item, dict) and item.get("candidate")
    }
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        target = event.get("target") if isinstance(event.get("target"), dict) else {}
        candidate = str(target.get("candidate_concept") or target.get("raw_target") or "unknown_concept")
        candidate_payload = candidates.get(candidate, {})
        concept_id = upsert_concept(
            conn,
            candidate,
            surface_terms=_safe_string_list(event.get("surface_terms")),
            usefulness_score=float(candidate_payload.get("usefulness_score") or 0.65),
            description=str(candidate_payload.get("rationale") or ""),
            ts=ts,
        )
        actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
        frames = _safe_string_list(event.get("frames"))
        surface_terms = _safe_string_list(event.get("surface_terms"))
        model_names = _safe_string_list(event.get("model_names"))
        product_names = _safe_string_list(event.get("product_names"))
        organizations = _safe_string_list(event.get("organizations"))
        people = _safe_string_list(event.get("people"))
        canonical_name = target.get("canonical_concept") or candidate
        event_id = stable_id(
            label_id,
            str(index),
            str(event.get("event_type")),
            candidate,
            str(event.get("evidence")),
            prefix="dev_",
        )
        frame = frames[0] if frames else None
        conn.execute(
            """
            INSERT INTO coded_observations
              (id, label_id, segment_id, observation_index, code_family, code_id, construct_type,
               speaker, speaker_role, stance, temporal_horizon, claim_strength, confidence,
               evidence_text, evidence_start, evidence_end, entities_json, status, audit_status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'discourse_event', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', NULL, ?)
            """,
            (
                event_id,
                label_id,
                segment_id,
                index,
                event.get("event_type"),
                candidate,
                actor.get("name"),
                actor.get("role"),
                event.get("stance"),
                event.get("temporal_horizon"),
                event.get("certainty"),
                event.get("confidence"),
                event.get("evidence"),
                event.get("evidence_start"),
                event.get("evidence_end"),
                dumps_json({"people": people, "organizations": organizations, "products": product_names + model_names}),
                ts,
            ),
        )
        conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, actor_name, actor_type, actor_affiliation,
               target_raw, candidate_concept, canonical_concept_id, canonical_concept_name, stance, claim_text,
               claim_type, certainty, temporal_horizon, frame, causal_mechanism, counterclaim, confidence,
               evidence_text, evidence_start, evidence_end, surface_terms_json, model_names_json, product_names_json,
               organizations_json, people_json, audit_status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                event_id,
                label_id,
                segment_id,
                index,
                event.get("event_type"),
                actor.get("name"),
                actor.get("actor_type"),
                actor.get("affiliation"),
                target.get("raw_target"),
                candidate,
                concept_id,
                canonical_name,
                event.get("stance"),
                event.get("claim_text"),
                event.get("claim_type"),
                event.get("certainty"),
                event.get("temporal_horizon"),
                frame,
                event.get("causal_mechanism"),
                event.get("counterclaim"),
                event.get("confidence"),
                event.get("evidence"),
                event.get("evidence_start"),
                event.get("evidence_end"),
                dumps_json(surface_terms),
                dumps_json(model_names),
                dumps_json(product_names),
                dumps_json(organizations),
                dumps_json(people),
                ts,
            ),
        )
        source_context = event.get("source_context") if isinstance(event.get("source_context"), dict) else {}
        speaker_context = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
        reported_actor = event.get("reported_actor") if isinstance(event.get("reported_actor"), dict) else {}
        if source_context or speaker_context or reported_actor or output.get("schema_version") == "ai_discourse_v3_1":
            conn.execute(
                """
                INSERT OR REPLACE INTO discourse_event_contexts
                  (discourse_event_id, label_id, segment_id, label_pack, source_context_kind, source_context_confidence,
                   speaker_name, speaker_role, speaker_affiliation, speaker_confidence,
                   reported_actor_name, reported_actor_type, reported_actor_affiliation, reported_actor_confidence,
                   event_subtype, signal_reason, metric_json, exclusion_flags_json, quality_flags_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    label_id,
                    segment_id,
                    str(output.get("schema_version") or ""),
                    str(source_context.get("kind") or "unknown"),
                    source_context.get("confidence"),
                    speaker_context.get("name"),
                    speaker_context.get("role"),
                    speaker_context.get("affiliation"),
                    speaker_context.get("confidence"),
                    reported_actor.get("name"),
                    reported_actor.get("actor_type"),
                    reported_actor.get("affiliation"),
                    reported_actor.get("confidence"),
                    event.get("event_subtype"),
                    event.get("signal_reason"),
                    dumps_json(event.get("metric") if isinstance(event.get("metric"), dict) else {}),
                    dumps_json(_safe_string_list(event.get("exclusion_flags"))),
                    dumps_json(_safe_string_list(event.get("quality_flags"))),
                    ts,
                ),
            )
        evidence_json = dumps_json(
            {
                "evidence": event.get("evidence"),
                "start": event.get("evidence_start"),
                "end": event.get("evidence_end"),
                "label_id": label_id,
                "discourse_event_id": event_id,
            }
        )
        for term in surface_terms + model_names + product_names:
            insert_term_usage(
                conn,
                discourse_event_id=event_id,
                segment_id=segment_id,
                term=term,
                concept_id=concept_id,
                actor_name=actor.get("name"),
                term_role="surface_term",
                confidence=event.get("confidence"),
                evidence_json=evidence_json,
                ts=ts,
            )
        for frame_value in frames:
            conn.execute(
                """
                INSERT OR IGNORE INTO frame_usages
                  (id, discourse_event_id, segment_id, frame, concept_id, actor_name, stance, confidence, evidence_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stable_id(event_id, "frame", frame_value.lower(), prefix="frm_"),
                    event_id,
                    segment_id,
                    frame_value,
                    concept_id,
                    actor.get("name"),
                    event.get("stance"),
                    event.get("confidence"),
                    evidence_json,
                    ts,
                ),
            )
        conn.execute(
            """
            INSERT OR IGNORE INTO actor_positions
              (id, discourse_event_id, segment_id, actor_name, actor_type, actor_affiliation, concept_id, concept_name,
               stance, claim_type, certainty, temporal_horizon, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(event_id, "actor", str(actor.get("name")), candidate, prefix="apos_"),
                event_id,
                segment_id,
                actor.get("name"),
                actor.get("actor_type"),
                actor.get("affiliation"),
                concept_id,
                canonical_name,
                event.get("stance"),
                event.get("claim_type"),
                event.get("certainty"),
                event.get("temporal_horizon"),
                event.get("confidence"),
                evidence_json,
                ts,
            ),
        )
        _insert_v2_compatible_dimensions(
            conn,
            event_id=event_id,
            segment_id=segment_id,
            event=event,
            concept_id=concept_id,
            surface_terms=surface_terms,
            model_names=model_names,
            product_names=product_names,
            organizations=organizations,
            people=people,
            evidence_json=evidence_json,
            ts=ts,
        )
        _insert_raw_identity_mentions(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            event=event,
            evidence_json=evidence_json,
            ts=ts,
        )
    for candidate in candidates.values():
        if isinstance(candidate, dict):
            upsert_concept_candidate(conn, candidate, ts=ts)


def upsert_concept(
    conn,
    candidate: str,
    *,
    surface_terms: list[str],
    usefulness_score: float,
    description: str,
    ts: str | None = None,
) -> str:
    ts = ts or now_iso()
    normalized = normalize_concept_name(candidate)
    concept_id = stable_id(normalized, prefix="con_")
    conn.execute(
        """
        INSERT INTO concepts
          (id, canonical_name, normalized_name, status, description, usefulness_score, first_seen_at, last_seen_at, created_at, updated_at)
        VALUES (?, ?, ?, 'candidate', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET
          usefulness_score = MAX(concepts.usefulness_score, excluded.usefulness_score),
          description = COALESCE(NULLIF(concepts.description, ''), excluded.description),
          last_seen_at = excluded.last_seen_at,
          updated_at = excluded.updated_at
        """,
        (concept_id, candidate, normalized, description, usefulness_score, ts, ts, ts, ts),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO concept_versions
          (id, concept_id, version, change_type, change_json, created_at)
        VALUES (?, ?, 1, 'created_candidate', ?, ?)
        """,
        (stable_id(concept_id, "v1", "created_candidate", prefix="cver_"), concept_id, dumps_json({"source": "ai_discourse_v3"}), ts),
    )
    for alias in [candidate, *surface_terms]:
        normalized_alias = normalize_concept_name(alias)
        if normalized_alias == "unknown_concept":
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO concept_aliases
              (id, concept_id, alias, normalized_alias, alias_type, status, version, evidence_count, source_diversity, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'surface_term', 'candidate', 1, 0, 0, ?, ?)
            """,
            (
                stable_id(concept_id, normalized_alias, "v1", prefix="cal_"),
                concept_id,
                alias,
                normalized_alias,
                ts,
                ts,
            ),
        )
    return concept_id


def upsert_concept_candidate(conn, candidate: dict[str, Any], *, ts: str | None = None) -> str:
    ts = ts or now_iso()
    name = str(candidate.get("candidate") or "")
    normalized = normalize_concept_name(name)
    candidate_id = stable_id(normalized, prefix="ccand_")
    evidence = {
        "evidence": candidate.get("evidence"),
        "start": candidate.get("evidence_start"),
        "end": candidate.get("evidence_end"),
        "confidence": candidate.get("confidence"),
    }
    conn.execute(
        """
        INSERT INTO concept_candidates
          (id, candidate, normalized_candidate, status, surface_terms_json, evidence_count, source_diversity,
           usefulness_score, rationale, evidence_json, created_at, updated_at)
        VALUES (?, ?, ?, 'candidate', ?, 1, 1, ?, ?, ?, ?, ?)
        ON CONFLICT(normalized_candidate) DO UPDATE SET
          usefulness_score = MAX(concept_candidates.usefulness_score, excluded.usefulness_score),
          rationale = COALESCE(NULLIF(concept_candidates.rationale, ''), excluded.rationale),
          evidence_json = excluded.evidence_json,
          updated_at = excluded.updated_at
        """,
        (
            candidate_id,
            name,
            normalized,
            dumps_json(_safe_string_list(candidate.get("surface_terms"))),
            candidate.get("usefulness_score") or 0,
            candidate.get("rationale") or "",
            dumps_json(evidence),
            ts,
            ts,
        ),
    )
    return candidate_id


def insert_term_usage(
    conn,
    *,
    discourse_event_id: str,
    segment_id: str,
    term: str,
    concept_id: str | None,
    actor_name: str | None,
    term_role: str,
    confidence: float | None,
    evidence_json: str,
    ts: str,
) -> None:
    normalized = normalize_concept_name(term)
    if normalized == "unknown_concept":
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO term_usages
          (id, discourse_event_id, segment_id, term, concept_id, actor_name, term_role, confidence, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_id(discourse_event_id, "term", normalized, prefix="tuse_"),
            discourse_event_id,
            segment_id,
            term,
            concept_id,
            actor_name,
            term_role,
            confidence,
            evidence_json,
            ts,
        ),
    )


def _insert_v2_compatible_dimensions(
    conn,
    *,
    event_id: str,
    segment_id: str,
    event: dict[str, Any],
    concept_id: str,
    surface_terms: list[str],
    model_names: list[str],
    product_names: list[str],
    organizations: list[str],
    people: list[str],
    evidence_json: str,
    ts: str,
) -> None:
    target = event.get("target") if isinstance(event.get("target"), dict) else {}
    candidate = str(target.get("candidate_concept") or event.get("event_type"))
    conn.execute(
        """
        INSERT OR IGNORE INTO topic_mentions
          (id, observation_id, segment_id, topic, stance, confidence, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_id(event_id, "v3topic", prefix="top_"),
            event_id,
            segment_id,
            candidate,
            event.get("stance"),
            event.get("confidence"),
            evidence_json,
            ts,
        ),
    )
    for term in surface_terms + model_names + product_names:
        conn.execute(
            """
            INSERT OR IGNORE INTO term_mentions
              (id, observation_id, segment_id, term, term_role, speaker, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(event_id, "v3term", term.lower(), prefix="term_"),
                event_id,
                segment_id,
                term,
                "surface_term",
                (event.get("actor") or {}).get("name") if isinstance(event.get("actor"), dict) else None,
                event.get("confidence"),
                evidence_json,
                ts,
            ),
        )
    for entity_type, values in [("person", people), ("organization", organizations), ("product", product_names + model_names)]:
        for name in values:
            conn.execute(
                """
                INSERT OR IGNORE INTO entity_mentions
                  (id, observation_id, segment_id, entity_type, name, role, confidence, evidence_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stable_id(event_id, "v3entity", entity_type, name.lower(), prefix="entm_"),
                    event_id,
                    segment_id,
                    entity_type,
                    name,
                    event.get("event_type"),
                    event.get("confidence"),
                    evidence_json,
                    ts,
                ),
            )
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    conn.execute(
        """
        INSERT OR IGNORE INTO speaker_positions
          (id, observation_id, segment_id, speaker, speaker_role, code_id, stance, confidence, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_id(event_id, "v3speaker", str(actor.get("name")), prefix="spk_"),
            event_id,
            segment_id,
            actor.get("name"),
            actor.get("role"),
            candidate,
            event.get("stance"),
            event.get("confidence"),
            evidence_json,
            ts,
        ),
    )
    if event.get("event_type") == "product_signal":
        product = product_names[0] if product_names else (model_names[0] if model_names else None)
        organization = organizations[0] if organizations else None
        signal_type = candidate
        conn.execute(
            """
            INSERT OR IGNORE INTO product_signals
              (id, observation_id, segment_id, product, organization, signal_type, signal_text, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(event_id, "v3product", prefix="psig_"),
                event_id,
                segment_id,
                product,
                organization,
                signal_type,
                str(event.get("claim_text") or event.get("evidence")),
                event.get("confidence"),
                evidence_json,
                ts,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO release_signal_links
              (id, observation_id, segment_id, organization, product, signal_type, release_event_id, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                stable_id(event_id, "v3release", prefix="rsig_"),
                event_id,
                segment_id,
                organization,
                product,
                signal_type,
                event.get("confidence"),
                evidence_json,
                ts,
            ),
        )


def _safe_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value:
        text = str(item).strip()
        if text:
            cleaned.append(text)
    return cleaned


def _insert_raw_identity_mentions(
    conn,
    *,
    episode_id: str,
    segment_id: str,
    event_id: str,
    event: dict[str, Any],
    evidence_json: str,
    ts: str,
) -> None:
    speaker_context = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    reported_actor = event.get("reported_actor") if isinstance(event.get("reported_actor"), dict) else {}
    speaker_name = _surface_name(speaker_context.get("name"))
    if speaker_name:
        conn.execute(
            """
            INSERT OR IGNORE INTO raw_speaker_mentions
              (id, episode_id, segment_id, discourse_event_id, surface_name, role, affiliation_surface,
               resolution_status, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved', ?, ?, ?)
            """,
            (
                stable_id(event_id, "speaker", speaker_name.lower(), str(speaker_context.get("role")), prefix="rsm_"),
                episode_id,
                segment_id,
                event_id,
                speaker_name,
                speaker_context.get("role"),
                speaker_context.get("affiliation"),
                speaker_context.get("confidence"),
                evidence_json,
                ts,
            ),
        )
    actor_name = _surface_name(actor.get("name"))
    if actor_name:
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=actor_name,
            mention_type="claim_actor",
            speaker_surface=speaker_name,
            reported_actor_surface=_surface_name(reported_actor.get("name")),
            role_context=actor.get("role") or actor.get("actor_type"),
            canonical_entity_type=_entity_type_from_actor_type(actor.get("actor_type")),
            confidence=event.get("confidence"),
            why_matters=event.get("signal_reason") or event.get("event_type"),
            evidence_json=evidence_json,
            ts=ts,
        )
    reported_name = _surface_name(reported_actor.get("name"))
    if reported_name:
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=reported_name,
            mention_type="reported_actor",
            speaker_surface=speaker_name,
            reported_actor_surface=reported_name,
            role_context=reported_actor.get("actor_type"),
            canonical_entity_type=_entity_type_from_actor_type(reported_actor.get("actor_type")),
            confidence=reported_actor.get("confidence") or event.get("confidence"),
            why_matters=event.get("signal_reason") or event.get("event_type"),
            evidence_json=evidence_json,
            ts=ts,
        )
    for name in _safe_string_list(event.get("people")):
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=name,
            mention_type="person_reference",
            speaker_surface=speaker_name,
            reported_actor_surface=reported_name,
            role_context=event.get("event_type"),
            canonical_entity_type="person",
            confidence=event.get("confidence"),
            why_matters=event.get("signal_reason") or "person referenced in discourse event",
            evidence_json=evidence_json,
            ts=ts,
        )
    for name in _safe_string_list(event.get("organizations")):
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=name,
            mention_type="org_reference",
            speaker_surface=speaker_name,
            reported_actor_surface=reported_name,
            role_context=event.get("event_type"),
            canonical_entity_type="org",
            confidence=event.get("confidence"),
            why_matters=event.get("signal_reason") or "organization referenced in discourse event",
            evidence_json=evidence_json,
            ts=ts,
        )
    for name in _safe_string_list(event.get("product_names")):
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=name,
            mention_type="product_reference",
            speaker_surface=speaker_name,
            reported_actor_surface=reported_name,
            role_context=event.get("event_type"),
            canonical_entity_type="product",
            confidence=event.get("confidence"),
            why_matters=event.get("signal_reason") or "product referenced in discourse event",
            evidence_json=evidence_json,
            ts=ts,
        )
    for name in _safe_string_list(event.get("model_names")):
        _insert_raw_actor_mention(
            conn,
            episode_id=episode_id,
            segment_id=segment_id,
            event_id=event_id,
            surface_name=name,
            mention_type="model_reference",
            speaker_surface=speaker_name,
            reported_actor_surface=reported_name,
            role_context=event.get("event_type"),
            canonical_entity_type="model",
            confidence=event.get("confidence"),
            why_matters=event.get("signal_reason") or "model referenced in discourse event",
            evidence_json=evidence_json,
            ts=ts,
        )


def _insert_raw_actor_mention(
    conn,
    *,
    episode_id: str,
    segment_id: str,
    event_id: str,
    surface_name: str,
    mention_type: str,
    speaker_surface: str | None,
    reported_actor_surface: str | None,
    role_context: str | None,
    canonical_entity_type: str | None,
    confidence: float | None,
    why_matters: str | None,
    evidence_json: str,
    ts: str,
) -> None:
    surface = _surface_name(surface_name)
    if not surface:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO raw_actor_mentions
          (id, episode_id, segment_id, discourse_event_id, surface_name, mention_type, speaker_surface,
           reported_actor_surface, role_context, canonical_entity_type, canonical_entity_id,
           resolution_status, alias_flag, confidence, why_matters, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'unresolved', ?, ?, ?, ?, ?)
        """,
        (
            stable_id(event_id, mention_type, surface.lower(), prefix="ram_"),
            episode_id,
            segment_id,
            event_id,
            surface,
            mention_type,
            speaker_surface,
            reported_actor_surface,
            role_context,
            canonical_entity_type,
            1 if _looks_like_alias(surface) else 0,
            confidence,
            why_matters,
            evidence_json,
            ts,
        ),
    )


def _surface_name(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null", "unknown", "n/a"}:
        return None
    return text[:300]


def _looks_like_alias(value: str) -> bool:
    text = value.strip()
    return bool(text.startswith("@") or len(text.split()) == 1 or text.lower() != text)


def _entity_type_from_actor_type(value: Any) -> str | None:
    actor_type = str(value or "").strip().lower()
    if actor_type in {"person", "host", "guest", "founder", "researcher", "executive"}:
        return "person"
    if actor_type in {"organization", "org", "company", "lab", "frontier_lab"}:
        return "org"
    if actor_type in {"product", "tool"}:
        return "product"
    if actor_type in {"model", "ai_model"}:
        return "model"
    return None
