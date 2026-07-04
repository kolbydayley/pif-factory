from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import label_pack_dir
from .util import read_text


class ValidationError(ValueError):
    pass


@dataclass(frozen=True)
class LabelPack:
    name: str
    version: str
    schema: dict[str, Any]
    prompt: str
    codebook: str
    examples: list[dict[str, Any]]
    evals: list[dict[str, Any]]


def load_label_pack(name: str) -> LabelPack:
    path = label_pack_dir(name)
    schema = json.loads(read_text(path / "schema.json"))
    prompt = read_text(path / "prompt.md")
    codebook_path = path / "codebook.md"
    examples_path = path / "examples.json"
    evals_path = path / "evals.json"
    examples = json.loads(read_text(examples_path)) if examples_path.exists() else []
    evals = json.loads(read_text(evals_path)) if evals_path.exists() else []
    return LabelPack(
        name=name,
        version=str(schema.get("$id", name)).rsplit("/", 1)[-1],
        schema=schema,
        prompt=prompt,
        codebook=read_text(codebook_path) if codebook_path.exists() else "",
        examples=examples,
        evals=evals,
    )


def validate_label_output(label_pack: str, value: dict[str, Any], *, segment_text: str | None = None) -> None:
    pack = load_label_pack(label_pack)
    _validate_schema(pack.schema, value, path="$")
    _validate_pack_logic(label_pack, value, segment_text=segment_text)
    if segment_text is not None:
        validate_label_grounding(label_pack, value, segment_text=segment_text)


def repair_label_output_for_submission(label_pack: str, value: dict[str, Any], *, segment_text: str) -> int:
    if label_pack == "ai_discourse_v1":
        repairs = 0
        for item in _iter_dicts_with_evidence(value):
            evidence = item.get("evidence")
            if not isinstance(evidence, str) or not evidence or evidence in segment_text:
                continue
            repaired = _repair_v1_evidence(evidence, segment_text)
            if not repaired or repaired == evidence:
                continue
            item["evidence"] = repaired
            repairs += 1
        return repairs
    if label_pack not in {"ai_discourse_v2", "ai_discourse_v3", "ai_discourse_v3_1"}:
        return 0
    repairs = 0
    for item in _iter_items_with_evidence(value):
        evidence = item.get("evidence")
        start = item.get("evidence_start")
        end = item.get("evidence_end")
        if not isinstance(evidence, str) or not evidence:
            continue
        if isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(segment_text) and segment_text[start:end] == evidence:
            continue
        positions = [match.start() for match in re.finditer(re.escape(evidence), segment_text)]
        if not positions:
            continue
        preferred = start if isinstance(start, int) else positions[0]
        repaired_start = min(positions, key=lambda position: abs(position - preferred))
        repaired_end = repaired_start + len(evidence)
        item["evidence_start"] = repaired_start
        item["evidence_end"] = repaired_end
        _append_repair_note(item, f"deterministic evidence offset repair from {start}-{end} to {repaired_start}-{repaired_end}")
        repairs += 1
    if label_pack == "ai_discourse_v3_1":
        events = value.get("discourse_events") if isinstance(value.get("discourse_events"), list) else []
        kept_events = []
        for event in events:
            if not isinstance(event, dict):
                kept_events.append(event)
                continue
            rejection_reason = _v31_rejectable_event_reason(event)
            if rejection_reason:
                source_context = event.get("source_context") if isinstance(event.get("source_context"), dict) else {}
                if isinstance(value.get("rejected_candidates"), list):
                    value["rejected_candidates"].append(
                        {
                            "text": str(event.get("evidence") or event.get("claim_text") or "")[:500],
                            "reason": rejection_reason,
                            "source_context_kind": str(source_context.get("kind") or "unknown"),
                        }
                    )
                repairs += 1
                continue
            event_type = _enum_key(event.get("event_type"))
            repaired_event_type = V31_EVENT_TYPE_REPAIRS.get(event_type)
            if repaired_event_type:
                original = event.get("event_type")
                event["event_type"] = repaired_event_type
                if not event.get("event_subtype"):
                    event["event_subtype"] = str(original or "")
                _append_repair_note(event, f"deterministic event_type repair from {original!r} to {repaired_event_type!r}")
                repairs += 1
            stance = str(event.get("stance") or "").strip().lower()
            repaired_stance = V31_STANCE_REPAIRS.get(stance)
            if repaired_stance:
                event["stance"] = repaired_stance
                _append_repair_note(event, f"deterministic stance repair from {stance!r} to {repaired_stance!r}")
                repairs += 1
            actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
            speaker = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
            actor_role = str(actor.get("role") or "").strip().lower()
            speaker_role = str(speaker.get("role") or "").strip().lower()
            repaired_speaker_role = V31_SPEAKER_ROLE_REPAIRS.get(speaker_role)
            if repaired_speaker_role:
                speaker["role"] = repaired_speaker_role
                event["speaker_context"] = speaker
                _append_repair_note(event, f"deterministic speaker_context role repair from {speaker_role!r} to {repaired_speaker_role!r}")
                repairs += 1
                speaker_role = repaired_speaker_role
            if actor_role and actor_role != "unknown" and speaker_role in {"", "unknown"}:
                speaker["role"] = actor.get("role")
                event["speaker_context"] = speaker
                _append_repair_note(event, f"deterministic speaker_context role repair from {speaker_role!r} to {actor.get('role')!r}")
                repairs += 1
            kept_events.append(event)
        if len(kept_events) != len(events):
            value["discourse_events"] = kept_events
            if not kept_events and value.get("extraction_status") == "coded":
                value["extraction_status"] = "no_signal"
                value["no_signal_reason"] = value.get("no_signal_reason") or "Only setup, sponsor, or filler-like evidence was produced and was removed before submission."
    return repairs


def _validate_schema(schema: dict[str, Any], value: Any, *, path: str) -> None:
    expected_type = schema.get("type")
    if isinstance(expected_type, list):
        errors = []
        for item_type in expected_type:
            try:
                _validate_schema({**schema, "type": item_type}, value, path=path)
                return
            except ValidationError as exc:
                errors.append(str(exc))
        raise ValidationError("; ".join(errors))
    if expected_type == "object":
        if not isinstance(value, dict):
            raise ValidationError(f"{path} must be an object")
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                raise ValidationError(f"{path}.{key} is required")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extra = sorted(set(value) - set(properties))
            if extra:
                raise ValidationError(f"{path} has unexpected keys: {', '.join(extra)}")
        for key, child_schema in properties.items():
            if key in value:
                _validate_schema(child_schema, value[key], path=f"{path}.{key}")
    elif expected_type == "array":
        if not isinstance(value, list):
            raise ValidationError(f"{path} must be an array")
        item_schema = schema.get("items", {})
        for index, item in enumerate(value):
            _validate_schema(item_schema, item, path=f"{path}[{index}]")
    elif expected_type == "string":
        if not isinstance(value, str):
            raise ValidationError(f"{path} must be a string")
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            raise ValidationError(f"{path} must be at least {schema['minLength']} characters")
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            raise ValidationError(f"{path} must be at most {schema['maxLength']} characters")
        if "enum" in schema and value not in schema["enum"]:
            raise ValidationError(f"{path} must be one of {schema['enum']}")
    elif expected_type == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValidationError(f"{path} must be a number")
        if "minimum" in schema and value < schema["minimum"]:
            raise ValidationError(f"{path} must be >= {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValidationError(f"{path} must be <= {schema['maximum']}")
    elif expected_type == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValidationError(f"{path} must be an integer")
        if "minimum" in schema and value < schema["minimum"]:
            raise ValidationError(f"{path} must be >= {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValidationError(f"{path} must be <= {schema['maximum']}")
    elif expected_type == "boolean":
        if not isinstance(value, bool):
            raise ValidationError(f"{path} must be a boolean")
    elif expected_type == "null":
        if value is not None:
            raise ValidationError(f"{path} must be null")
    if "enum" in schema and value not in schema["enum"]:
        raise ValidationError(f"{path} must be one of {schema['enum']}")


def render_prompt(label_pack: str, segment: dict[str, Any], context: dict[str, Any]) -> str:
    pack = load_label_pack(label_pack)
    static = {
        "label_pack": pack.name,
        "label_pack_version": pack.version,
        "schema": pack.schema,
        "examples": pack.examples,
        "eval_questions": pack.evals,
    }
    return "\n\n".join(
        [
            "# Static Instructions",
            pack.prompt.strip(),
            "# Codebook",
            pack.codebook.strip() or "No separate codebook has been defined for this label pack.",
            "# Static Schema And Examples",
            json.dumps(static, ensure_ascii=True, indent=2, sort_keys=True),
            "# Variable Context",
            json.dumps(context, ensure_ascii=True, indent=2, sort_keys=True),
            "# Segment Text",
            segment["text"],
            "# Output Contract",
            "Return only one JSON object that validates against the schema. Every field named evidence must be an exact contiguous substring copied from the segment text. Do not use ellipses, stitched quotes, or paraphrases as evidence. Do not include entities that appear only in URLs, boilerplate, title cards, sponsor/ad-read copy, or inferred context. Sponsor/ad-read copy is excluded from durable ai_discourse_v3_1 extraction; route it to rejected_candidates or no_signal. Do not include Markdown fences.",
        ]
    )


def validate_label_grounding(label_pack: str, value: dict[str, Any], *, segment_text: str) -> None:
    missing_evidence = [
        f"{path}={evidence!r}"
        for path, evidence in _iter_evidence_values(value)
        if evidence and evidence not in segment_text
    ]
    if missing_evidence:
        raise ValidationError("Evidence must be exact contiguous segment text: " + "; ".join(missing_evidence[:5]))
    unsupported_entities = [] if label_pack == "ai_discourse_v3_1" else _unsupported_entities(value, segment_text)
    if unsupported_entities:
        raise ValidationError("Entities must be explicitly present in segment text: " + ", ".join(unsupported_entities[:10]))


def _validate_pack_logic(label_pack: str, value: dict[str, Any], *, segment_text: str | None) -> None:
    if label_pack not in {"ai_discourse_v2", "ai_discourse_v3", "ai_discourse_v3_1"}:
        return
    extraction_status = value.get("extraction_status")
    not_present_reason = value.get("not_present_reason")
    if label_pack == "ai_discourse_v2":
        observations = value.get("observations") or []
        if extraction_status == "coded" and not observations:
            raise ValidationError("$.observations must not be empty when extraction_status is coded")
        if extraction_status in {"not_present", "insufficient_evidence", "low_signal"} and observations:
            raise ValidationError("$.observations must be empty for non-coded extraction_status")
        if extraction_status in {"not_present", "insufficient_evidence", "low_signal"} and not not_present_reason:
            raise ValidationError("$.not_present_reason is required for non-coded extraction_status")
        for index, observation in enumerate(observations):
            _validate_exact_offset(f"$.observations[{index}]", observation, segment_text)
        return

    events = value.get("discourse_events") or []
    candidates = value.get("concept_candidates") or []
    no_signal_reason = value.get("no_signal_reason")
    if extraction_status == "coded" and not events:
        raise ValidationError("$.discourse_events must not be empty when extraction_status is coded")
    non_coded_statuses = {"no_signal", "insufficient_evidence", "low_signal", "excluded_source_context"}
    if extraction_status in non_coded_statuses and events:
        raise ValidationError("$.discourse_events must be empty for non-coded extraction_status")
    if extraction_status in non_coded_statuses and not no_signal_reason:
        raise ValidationError("$.no_signal_reason is required for non-coded extraction_status")
    if extraction_status == "coded" and not candidates and label_pack != "ai_discourse_v3_1":
        raise ValidationError("$.concept_candidates should include at least one candidate when discourse_events are coded")
    if label_pack == "ai_discourse_v3_1":
        segment_source_context = value.get("segment_source_context") if isinstance(value.get("segment_source_context"), dict) else {}
        if segment_source_context.get("kind") in {"show_setup", "page_chrome"} and extraction_status == "coded":
            raise ValidationError("$.segment_source_context excluded source contexts cannot produce coded events")
    for index, event in enumerate(events):
        _validate_exact_offset(f"$.discourse_events[{index}]", event, segment_text)
        target = event.get("target") if isinstance(event, dict) else {}
        if not isinstance(target, dict) or not target.get("candidate_concept"):
            raise ValidationError(f"$.discourse_events[{index}].target.candidate_concept is required")
        if not event.get("surface_terms") and event.get("event_type") in {"term_usage", "product_signal", "capability_claim"}:
            raise ValidationError(f"$.discourse_events[{index}].surface_terms must not be empty for {event.get('event_type')}")
        if label_pack == "ai_discourse_v3_1":
            _validate_v31_event_quality(index, event)
    for index, candidate in enumerate(candidates):
        _validate_exact_offset(f"$.concept_candidates[{index}]", candidate, segment_text)


GENERIC_V31_CLAIM_PATTERNS = [
    re.compile(r"^segment (discusses|contains|mentions|signals|raises|frames)\b", re.I),
    re.compile(r"\bcontains a discourse signal\b", re.I),
    re.compile(r"\bforward-looking AI forecast around\b", re.I),
    re.compile(r"\bproduct or model narrative around\b", re.I),
]


FILLER_EVIDENCE_PATTERNS = [
    re.compile(r"\bwe(?:'re| are) going to (talk|welcome|discuss|cover)\b", re.I),
    re.compile(r"\bgoing to be story number\b", re.I),
    re.compile(r"\bsubscribe\b|\bsponsor\b|\bpromo code\b|\bshow notes\b", re.I),
    re.compile(r"\bcoming up (?:next|after|on)\b|\bin this episode\b|\btoday['’]?s episode\b", re.I),
    re.compile(r"\btranscript\b.*\b(show notes|timestamps|links)\b", re.I),
]


PAGE_NUMERIC_ARTIFACT_PATTERNS = [
    re.compile(r"^\s*(?:\[\s*)?\d{1,3}(?:\s*\])?[\).:]?\s*$"),
    re.compile(r"^\s*\d{1,2}:\d{2}(?::\d{2})?\s*$"),
    re.compile(r"^\s*(?:footnote|note|source|chapter|timestamp)\s*\d{1,3}\s*$", re.I),
    re.compile(r"^\s*\d{1,3}\s+(?:comments?|likes?|shares?|views?)\s*$", re.I),
]


QUANTITATIVE_EVENT_TYPES = {
    "capability_claim",
    "product_signal",
    "market_signal",
    "forecast",
    "causal_mechanism",
    "adoption_signal",
}


QUANTIFIED_EVIDENCE_PATTERN = re.compile(
    r"(\$\s*\d|\b\d+(?:[,.]\d+)*(?:\.\d+)?(?:\s*[-–]\s*\d+(?:\.\d+)?)?\s*(?:%|x|gb|tb|mb|kb|mph|times|fold|percent|million|billion|trillion|thousand|k|m|bn|users?|customers?|tokens?|dollars?|months?|years?|weeks?|days?|hours?|minutes?|seconds?|arr|revenue|valuation|rate|margin|growth|latency|parameters?|gpu|gpus|chips?|queries?|requests?|gates?|ors?)?\b|\b[a-z]\s+times\b|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|hundred|thousand|million|billion|trillion|half|third|quarter|twentieth|full[- ]year|quarterly|deca[- ]?billions?)\b)",
    re.I,
)

GROUNDED_METRIC_CONTEXT_PATTERN = re.compile(
    r"(\$\s*\d|\b\d+(?:[,.]\d+)*(?:\.\d+)?\s*(?:%|x|gb|tb|mb|kb|mph|times|fold|percent|million|billion|trillion|thousand|k|m|bn|users?|customers?|institutions?|tokens?|dollars?|months?|years?|weeks?|days?|hours?|minutes?|seconds?|arr|revenue|valuation|rate|margin|growth|latency|parameters?|gpu|gpus|chips?|queries?|requests?|gates?|ors?)\b|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|hundred|thousand|million|billion|trillion|half|third|quarter|twentieth|full[- ]year|quarterly|deca[- ]?billions?)\s+(?:times|fold|percent|users?|customers?|institutions?|tokens?|dollars?|months?|years?|weeks?|days?|hours?|minutes?|seconds?|gpus?|chips?|parameters?)\b)",
    re.I,
)


V31_STANCE_REPAIRS = {
    "analytical": "neutral",
    "cautionary": "warning",
    "cautious": "uncertain",
    "critical": "skeptical",
    "descriptive": "neutral",
    "exploratory": "uncertain",
    "hedged": "uncertain",
    "optimistic": "supportive",
    "positive": "supportive",
    "negative": "skeptical",
}


V31_SPEAKER_ROLE_REPAIRS = {
    "co-host": "host",
    "cohost": "host",
    "interviewer": "host",
    "moderator": "host",
    "presenter": "host",
    "narrator": "host",
    "guest_expert": "guest",
    "expert_guest": "guest",
    "panelist": "guest",
    "interviewee": "guest",
    "participant": "speaker",
    "source": "quoted_source",
    "quoted": "quoted_source",
    "reported_source": "quoted_source",
}


V31_EVENT_TYPE_REPAIRS = {
    "actor_position": "stance_position",
    "actor_org_position": "stance_position",
    "adoption_pattern": "adoption_signal",
    "adoption": "adoption_signal",
    "causal_claim": "causal_mechanism",
    "claim": "capability_claim",
    "descriptive_claim": "capability_claim",
    "entity_mention": "entity_reference",
    "entity_relation": "entity_reference",
    "entity_relationship": "entity_reference",
    "forecast_claim": "forecast",
    "frame": "frame_usage",
    "market_investment_narrative": "market_signal",
    "market_narrative": "market_signal",
    "model_reference": "entity_reference",
    "org_reference": "entity_reference",
    "person_reference": "actor_mention",
    "product_release_signal": "product_signal",
    "relationship_edge": "entity_reference",
    "risk": "risk_signal",
    "safety_risk": "risk_signal",
    "technical_mechanism": "causal_mechanism",
    "term": "term_usage",
    "terminology_drift": "term_usage",
    "uncertainty_marker": "uncertainty",
}


def _enum_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _validate_v31_event_quality(index: int, event: dict[str, Any]) -> None:
    claim_text = str(event.get("claim_text") or "")
    evidence = str(event.get("evidence") or "")
    signal_reason = str(event.get("signal_reason") or "")
    if any(pattern.search(claim_text) for pattern in GENERIC_V31_CLAIM_PATTERNS):
        raise ValidationError(f"$.discourse_events[{index}].claim_text is generic/template-like")
    if _is_filler_v31_event(event):
        raise ValidationError(f"$.discourse_events[{index}].evidence appears to be setup, sponsor, or filler text")
    source_context = event.get("source_context") if isinstance(event.get("source_context"), dict) else {}
    if source_context.get("kind") == "sponsor_ad_read":
        raise ValidationError(f"$.discourse_events[{index}].sponsor/ad-read event must be isolated from durable discourse extraction")
    if _is_numeric_artifact_v31_event(event):
        raise ValidationError(f"$.discourse_events[{index}].metric or quantitative claim is not grounded in substantive evidence")
    if _is_low_value_identity_v31_event(event):
        raise ValidationError(f"$.discourse_events[{index}] is a low-value identity mention without graph-useful context")
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    speaker = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
    if not claim_text or claim_text.lower() == evidence.lower():
        raise ValidationError(f"$.discourse_events[{index}].claim_text must be a faithful proposition, not a copied evidence span")
    if len(signal_reason.split()) < 5:
        raise ValidationError(f"$.discourse_events[{index}].signal_reason must explain why the event is useful")
    if actor.get("actor_type") == "unknown" and speaker.get("role") == "unknown" and source_context.get("kind") != "mixed_or_uncertain":
        raise ValidationError(f"$.discourse_events[{index}] must identify an actor or speaker context")


def _is_filler_v31_event(event: dict[str, Any]) -> bool:
    evidence = str(event.get("evidence") or "")
    source_context = event.get("source_context") if isinstance(event.get("source_context"), dict) else {}
    if source_context.get("kind") == "sponsor_ad_read":
        return True
    return any(pattern.search(evidence) for pattern in FILLER_EVIDENCE_PATTERNS)


def _is_allowed_sponsor_v31_event(event: dict[str, Any]) -> bool:
    return False


def _v31_rejectable_event_reason(event: dict[str, Any]) -> str | None:
    evidence = str(event.get("evidence") or "")
    source_context = event.get("source_context") if isinstance(event.get("source_context"), dict) else {}
    if source_context.get("kind") == "sponsor_ad_read":
        return "sponsor_or_ad"
    if _is_filler_v31_event(event):
        return "sponsor_or_ad" if re.search(r"\bsponsor\b|\bpromo code\b", evidence, re.I) else "show_setup"
    if _is_numeric_artifact_v31_event(event):
        return "insufficient_evidence"
    if _is_low_value_identity_v31_event(event):
        return "unsupported_actor"
    return None


LOW_VALUE_IDENTITY_NAME_PATTERN = re.compile(
    r"\b(?:unknown|responding|unnamed|anonymous|pseudonymous|commenter|user|reader|poster|speaker|host|guest|interviewer|interviewee)\b",
    re.I,
)

SPEAKER_LABEL_ONLY_PATTERN = re.compile(
    r"^\s*(?:host|guest|speaker|interviewer|interviewee|[A-Z][A-Za-z0-9 ._'’-]{0,48})\s*:?\s*$",
    re.I,
)

GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN = re.compile(
    r"\b(?:"
    r"ceo|cto|founder|co[- ]founder|researcher|scientist|economist|professor|partner|investor|"
    r"lead|director|head|president|minister|author|writer|journalist|analyst|guest|host|"
    r"interview|interviews|interviewed|mentioned|mentions|quoted|quotes|according to|reported|"
    r"said|says|argued|argues|claims|claimed|criticized|endorsed|compared|discussed|"
    r"affiliation|affiliated|from|at|works at|joined|left|runs|leads|founded|"
    r"podcast|episode|source|publication|paper|blog|newsletter"
    r")\b",
    re.I,
)


def _is_low_value_identity_v31_event(event: dict[str, Any]) -> bool:
    event_type = str(event.get("event_type") or "").strip()
    if event_type != "actor_mention":
        return False
    evidence = re.sub(r"\s+", " ", str(event.get("evidence") or "")).strip()
    claim_text = re.sub(r"\s+", " ", str(event.get("claim_text") or "")).strip()
    signal_reason = re.sub(r"\s+", " ", str(event.get("signal_reason") or "")).strip()
    event_subtype = str(event.get("event_subtype") or "")
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    reported_actor = event.get("reported_actor") if isinstance(event.get("reported_actor"), dict) else {}
    speaker = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
    actor_name = str(actor.get("name") or "").strip()
    reported_name = str(reported_actor.get("name") or "").strip()
    speaker_name = str(speaker.get("name") or "").strip()
    combined = " ".join([evidence, claim_text, signal_reason, event_subtype, actor_name, reported_name, speaker_name])

    if SPEAKER_LABEL_ONLY_PATTERN.fullmatch(evidence) and not GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN.search(combined):
        return True
    if re.search(r"\bspeaker[_ -]?label\b|\btranscript speaker\b", combined, re.I):
        return True
    if LOW_VALUE_IDENTITY_NAME_PATTERN.search(actor_name) and not GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN.search(combined):
        return True
    if LOW_VALUE_IDENTITY_NAME_PATTERN.search(reported_name) and not GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN.search(combined):
        return True
    if LOW_VALUE_IDENTITY_NAME_PATTERN.search(speaker_name) and not GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN.search(combined):
        return True
    if len(evidence.split()) <= 5 and not GRAPH_USEFUL_IDENTITY_CONTEXT_PATTERN.search(combined):
        return True
    return False


def _is_numeric_artifact_v31_event(event: dict[str, Any]) -> bool:
    evidence = str(event.get("evidence") or "").strip()
    if not evidence:
        return False
    quality_flags = {str(flag).strip().lower() for flag in event.get("quality_flags") or []}
    if quality_flags.intersection({"footnote_source", "referent_outside_current_segment", "show_notes_not_dialogue"}):
        metric = event.get("metric") if isinstance(event.get("metric"), dict) else {}
        if any(str(metric.get(key) or "").strip() for key in ["value", "unit", "comparator", "raw_text"]):
            return True
    if any(pattern.search(evidence) for pattern in PAGE_NUMERIC_ARTIFACT_PATTERNS):
        return _event_has_quantitative_intent(event)
    if not _event_has_quantitative_intent(event):
        return False
    metric = event.get("metric") if isinstance(event.get("metric"), dict) else {}
    metric_raw = str(metric.get("raw_text") or "").strip()
    metric_value = str(metric.get("value") or "").strip()
    normalized_evidence = re.sub(r"\s+", " ", evidence).lower()
    normalized_metric_raw = re.sub(r"\s+", " ", metric_raw).lower()
    normalized_metric_value = re.sub(r"\s+", " ", metric_value).lower()
    if metric_raw and _looks_quantitative_text(metric_raw) and normalized_metric_raw not in normalized_evidence and not QUANTIFIED_EVIDENCE_PATTERN.search(evidence):
        return True
    if metric_value and _looks_quantitative_text(metric_value) and normalized_metric_value not in normalized_evidence and not QUANTIFIED_EVIDENCE_PATTERN.search(evidence):
        return True
    if _metric_is_bare_or_artifact_like(metric_raw, evidence) or _metric_is_bare_or_artifact_like(metric_value, evidence):
        return True
    claim_text = str(event.get("claim_text") or "")
    if re.search(r"\b\d+(?:\.\d+)?\s*(?:x|times|%|percent|million|billion|arr|revenue|valuation)\b", claim_text, re.I) and not QUANTIFIED_EVIDENCE_PATTERN.search(evidence):
        return True
    return False


def _metric_is_bare_or_artifact_like(metric_value: str, evidence: str) -> bool:
    value = str(metric_value or "").strip()
    if not value or not re.fullmatch(r"\d{1,3}(?:\.\d+)?", value):
        return False
    if GROUNDED_METRIC_CONTEXT_PATTERN.search(evidence):
        return False
    return True


def _event_has_quantitative_intent(event: dict[str, Any]) -> bool:
    metric = event.get("metric") if isinstance(event.get("metric"), dict) else {}
    metric_has_value = any(_looks_quantitative_text(metric.get(key)) for key in ["value", "unit", "raw_text"])
    claim_text = str(event.get("claim_text") or "")
    metric_language = re.search(
        r"(\b\d+(?:\.\d+)?\b|\$\s*\d|%|\bpercent\b)",
        claim_text,
        re.I,
    )
    return bool(metric_has_value or metric_language)


def _looks_quantitative_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or text == "not_applicable":
        return False
    return bool(
        re.search(r"\d|\$|%|\b(?:percent|million|billion|trillion|thousand|hundred|half|third|quarter|twentieth|fold|times|x|gb|tb|mb|kb|mph|users?|customers?|tokens?|dollars?|months?|years?|weeks?|days?|hours?|minutes?|seconds?|arr|parameters?|gpu|gpus|chips?|queries?|requests?|gates?|ors?|deca[- ]?billions?)\b", text, re.I)
    )


def _validate_exact_offset(path: str, item: dict[str, Any], segment_text: str | None) -> None:
    start = item.get("evidence_start")
    end = item.get("evidence_end")
    evidence = item.get("evidence")
    if not isinstance(start, int) or not isinstance(end, int) or start >= end:
        raise ValidationError(f"{path} evidence offsets must be valid")
    if segment_text is not None:
        if end > len(segment_text):
            raise ValidationError(f"{path}.evidence_end is outside segment text")
        if segment_text[start:end] != evidence:
            raise ValidationError(f"{path} evidence offsets do not match evidence text")


def audit_label_grounding(label_pack: str, value: dict[str, Any], *, segment_text: str, expected_context: dict[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    checks = 0
    try:
        validate_label_output(label_pack, value)
    except ValidationError as exc:
        issues.append({"severity": "critical", "field": "schema", "message": str(exc)})
    checks += 1
    if value.get("segment_id") != expected_context.get("segment_id"):
        issues.append({"severity": "critical", "field": "segment_id", "message": "segment_id does not match audited segment"})
    checks += 1
    if value.get("episode_id") != expected_context.get("episode_id"):
        issues.append({"severity": "critical", "field": "episode_id", "message": "episode_id does not match audited episode"})
    checks += 1
    for path, evidence in _iter_evidence_values(value):
        checks += 1
        if evidence not in segment_text:
            issues.append({"severity": "critical", "field": path, "message": "evidence is not an exact contiguous substring"})
    if label_pack in {"ai_discourse_v2", "ai_discourse_v3", "ai_discourse_v3_1"}:
        checks += 1
        try:
            _validate_pack_logic(label_pack, value, segment_text=segment_text)
        except ValidationError as exc:
            issues.append({"severity": "critical", "field": "evidence_offsets", "message": str(exc)})
    if label_pack != "ai_discourse_v3_1":
        for entity in _unsupported_entities(value, segment_text):
            checks += 1
            issues.append({"severity": "warning", "field": "entities", "message": f"entity is not explicit in non-URL segment text: {entity}"})
    if value.get("overall_confidence", 0) < 0.6 and not value.get("needs_review") and _has_substantive_codes(value):
        checks += 1
        issues.append({"severity": "warning", "field": "needs_review", "message": "low-confidence label should be marked needs_review"})
    if _looks_low_signal(segment_text) and any(_iter_evidence_values(value)):
        checks += 1
        issues.append({"severity": "warning", "field": "segment", "message": "low-signal or boilerplate-like segment received substantive codes"})
    penalty = sum(0.35 if issue["severity"] == "critical" else 0.15 for issue in issues)
    score = max(0.0, min(1.0, 1.0 - penalty))
    status = "passed" if score >= 0.85 and not any(issue["severity"] == "critical" for issue in issues) else "needs_adjudication"
    return {"status": status, "score": score, "checks": checks, "issues": issues}


def _iter_evidence_values(value: Any, path: str = "$"):
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key == "evidence" and isinstance(child, str):
                yield child_path, child
            else:
                yield from _iter_evidence_values(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_evidence_values(child, f"{path}[{index}]")


def _iter_items_with_evidence(value: Any):
    if isinstance(value, dict):
        if {"evidence", "evidence_start", "evidence_end"}.issubset(value.keys()):
            yield value
        for child in value.values():
            yield from _iter_items_with_evidence(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_items_with_evidence(child)


def _iter_dicts_with_evidence(value: Any):
    if isinstance(value, dict):
        if isinstance(value.get("evidence"), str):
            yield value
        for child in value.values():
            yield from _iter_dicts_with_evidence(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_dicts_with_evidence(child)


def _repair_v1_evidence(evidence: str, segment_text: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", evidence).strip()
    if not cleaned:
        return None
    if cleaned in segment_text:
        return cleaned
    return None


def _append_repair_note(item: dict[str, Any], note: str) -> None:
    if isinstance(item.get("quality_flags"), list):
        flags = item["quality_flags"]
        if note not in flags:
            flags.append(note)
    elif "audit_notes" in item:
        current = str(item.get("audit_notes") or "").strip()
        item["audit_notes"] = f"{current} {note}".strip()


def _unsupported_entities(value: dict[str, Any], segment_text: str) -> list[str]:
    text_without_urls = re.sub(r"https?://\S+|www\.\S+", " ", segment_text, flags=re.I)
    normalized_segment = _normalize_entity_text(text_without_urls)
    unsupported = []
    entity_sources = []
    for observation in value.get("observations") or []:
        if isinstance(observation, dict) and isinstance(observation.get("entities"), dict):
            entity_sources.append(observation["entities"])
    for event in value.get("discourse_events") or []:
        if isinstance(event, dict):
            entity_sources.append(
                {
                    "people": event.get("people") or [],
                    "organizations": event.get("organizations") or [],
                    "products": (event.get("product_names") or []) + (event.get("model_names") or []),
                }
            )
            actor = event.get("actor")
            if isinstance(actor, dict) and actor.get("name") and actor.get("actor_type") != "unknown":
                entity_sources.append({"people": [actor["name"]] if actor.get("actor_type") in {"person", "host", "guest"} else [], "organizations": [actor["name"]] if actor.get("actor_type") == "organization" else []})
    entities = value.get("entities") if isinstance(value.get("entities"), dict) else value
    if isinstance(entities, dict):
        entity_sources.append(entities)
    if not isinstance(entities, dict):
        entity_sources.append(value)
    for entity_source in entity_sources:
        for key in ["people", "organizations", "products", "hosts", "guests", "mentioned_people"]:
            for name in _entity_names_for_validation(entity_source.get(key) if isinstance(entity_source, dict) else []):
                normalized_name = _normalize_entity_text(name)
                if normalized_name and normalized_name not in normalized_segment:
                    unsupported.append(name)
    return sorted(set(unsupported))


def _entity_names_for_validation(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    names = []
    for value in values:
        if isinstance(value, str):
            names.append(value)
        elif isinstance(value, dict):
            name = value.get("name") or value.get("person") or value.get("organization") or value.get("product")
            if name:
                names.append(str(name))
    return names


def _normalize_entity_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _looks_low_signal(text: str) -> bool:
    lowered = text.lower()
    boilerplate_hits = sum(
        token in lowered
        for token in [
            "subscribe",
            "newsletter",
            "sponsor",
            "promo code",
            "show notes",
            "transcript",
            "follow us",
            "github.com",
            "http://",
            "https://",
        ]
    )
    return boilerplate_hits >= 3 or len(text.split()) < 30


def _has_substantive_codes(value: dict[str, Any]) -> bool:
    for key in [
        "topics",
        "terminology_shifts",
        "claims",
        "observations",
        "market_narratives",
        "release_signals",
        "correlation_hypotheses",
        "durable_facts",
        "workflow_patterns",
        "skill_candidates",
        "discourse_events",
        "concept_candidates",
    ]:
        if value.get(key):
            return True
    entities = value.get("entities")
    if isinstance(entities, dict) and any(entities.get(key) for key in ["people", "organizations", "products"]):
        return True
    return False


TOPIC_PATTERNS = {
    "agi": re.compile(r"\bagi\b|artificial general intelligence|singularity", re.I),
    "agents": re.compile(r"\bagents?\b|agentic|tool use|computer use", re.I),
    "test_time_compute": re.compile(r"test[- ]time compute|inference scaling|reasoning compute", re.I),
    "ai_coding": re.compile(r"codex|cursor|claude code|coding agent|software engineer", re.I),
    "open_weights": re.compile(r"open[- ]weights?|open source model|llama|mistral", re.I),
    "safety_alignment": re.compile(r"safety|alignment|evals?|risk|misuse", re.I),
    "enterprise_ai": re.compile(r"enterprise ai|copilot|workflow|automation|roi", re.I),
}


def local_draft_label(label_pack: str, *, segment_text: str, context: dict[str, Any]) -> dict[str, Any]:
    """Deterministic fallback for tests and bootstrapping, not a replacement for Codex labels."""
    if label_pack == "ai_discourse_v1":
        topics = []
        for topic, pattern in TOPIC_PATTERNS.items():
            if pattern.search(segment_text):
                topics.append(
                    {
                        "topic": topic,
                        "stance": "mixed_or_descriptive",
                        "intensity": 0.5,
                        "evidence": _excerpt(segment_text, pattern),
                    }
                )
        claims = []
        if topics:
            claims.append(
                {
                    "claim_text": f"Segment discusses {', '.join(topic['topic'] for topic in topics)}.",
                    "claim_type": "descriptive",
                    "confidence": 0.45,
                    "evidence": topics[0]["evidence"],
                }
            )
        value = {
            "schema_version": "ai_discourse_v1",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "summary": segment_text[:280],
            "topics": topics,
            "terminology_shifts": [],
            "claims": claims,
            "entities": {"people": [], "organizations": [], "products": []},
            "overall_confidence": 0.45 if topics else 0.25,
            "needs_review": True,
            "review_reason": "local_draft_bootstrap",
        }
    elif label_pack == "ai_discourse_v2":
        observations = _local_v2_observations(segment_text, context)
        artifact_type = context.get("transcript_artifact_type") or "unknown"
        boilerplate_ratio = float(context.get("transcript_boilerplate_ratio") or 0)
        boilerplate_risk = "high" if boilerplate_ratio >= 0.35 or _looks_low_signal(segment_text) else "medium" if boilerplate_ratio >= 0.15 else "low"
        quality_flags = ["llm_adjudicated_not_human_validated"] if observations else []
        if artifact_type == "mixed_page":
            quality_flags.append("mixed_page")
        if boilerplate_risk == "high":
            quality_flags.append("boilerplate_risk")
        if not observations and _looks_low_signal(segment_text):
            quality_flags.append("low_signal")
        extraction_status = "coded" if observations else "low_signal" if _looks_low_signal(segment_text) else "not_present"
        value = {
            "schema_version": "ai_discourse_v2",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "extraction_status": extraction_status,
            "segment_quality": {
                "artifact_type": artifact_type if artifact_type in {"dialogue_transcript", "caption_transcript", "article_show_notes", "mixed_page", "boilerplate"} else "unknown",
                "boilerplate_risk": boilerplate_risk,
                "substantive_word_count": int(context.get("transcript_substantive_word_count") or len(segment_text.split())),
                "transcript_preparation_id": context.get("transcript_preparation_id"),
            },
            "observations": observations,
            "quality_flags": sorted(set(quality_flags)),
            "not_present_reason": None if observations else "No systematic-review target construct is sufficiently present in this segment.",
            "overall_confidence": 0.68 if observations else 0.72,
            "needs_review": artifact_type in {"mixed_page", "boilerplate"} or boilerplate_risk == "high",
            "review_reason": "local_draft_bootstrap" if artifact_type in {"mixed_page", "boilerplate"} or boilerplate_risk == "high" else None,
        }
    elif label_pack == "ai_discourse_v3":
        events = _local_v3_discourse_events(segment_text, context)
        candidates = _local_v3_concept_candidates(events)
        artifact_type = context.get("transcript_artifact_type") or "unknown"
        boilerplate_ratio = float(context.get("transcript_boilerplate_ratio") or 0)
        boilerplate_risk = "high" if boilerplate_ratio >= 0.35 or _looks_low_signal(segment_text) else "medium" if boilerplate_ratio >= 0.15 else "low"
        review_reasons = []
        if artifact_type in {"mixed_page", "boilerplate"}:
            review_reasons.append(artifact_type)
        if any(event["event_type"] in {"product_signal", "market_signal", "forecast"} for event in events):
            review_reasons.append("high_impact_signal")
        if not events and _looks_low_signal(segment_text):
            review_reasons.append("low_signal")
        extraction_status = "coded" if events else "low_signal" if _looks_low_signal(segment_text) else "no_signal"
        value = {
            "schema_version": "ai_discourse_v3",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "extraction_status": extraction_status,
            "segment_quality": {
                "artifact_type": artifact_type if artifact_type in {"dialogue_transcript", "caption_transcript", "article_show_notes", "mixed_page", "boilerplate"} else "unknown",
                "boilerplate_risk": boilerplate_risk,
                "substantive_word_count": int(context.get("transcript_substantive_word_count") or len(segment_text.split())),
                "transcript_preparation_id": context.get("transcript_preparation_id"),
            },
            "discourse_events": events,
            "concept_candidates": candidates,
            "no_signal_reason": None if events else "No dynamic discourse signal is sufficiently present in this segment.",
            "overall_confidence": 0.72 if events else 0.7,
            "needs_review": bool(review_reasons),
            "review_reason": ",".join(sorted(set(review_reasons))) if review_reasons else None,
        }
    elif label_pack == "ai_discourse_v3_1":
        raise ValueError("ai_discourse_v3_1 requires GPT-5.5 full-episode extraction; local-draft extraction is disabled")
    elif label_pack == "people_network_v1":
        value = {
            "schema_version": "people_network_v1",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "hosts": [],
            "guests": [],
            "mentioned_people": [],
            "organizations": [],
            "relationship_edges": [],
            "overall_confidence": 0.2,
            "needs_review": True,
            "review_reason": "local_draft_bootstrap",
        }
    elif label_pack == "product_release_v1":
        value = {
            "schema_version": "product_release_v1",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "products": [],
            "release_signals": [],
            "correlation_hypotheses": [],
            "overall_confidence": 0.2,
            "needs_review": True,
            "review_reason": "local_draft_bootstrap",
        }
    elif label_pack == "investment_signal_v1":
        value = {
            "schema_version": "investment_signal_v1",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "companies": [],
            "market_narratives": [],
            "risk_flags": [],
            "overall_confidence": 0.2,
            "needs_review": True,
            "review_reason": "local_draft_bootstrap_no_investment_advice",
        }
    elif label_pack == "memory_candidate_v1":
        value = {
            "schema_version": "memory_candidate_v1",
            "segment_id": context["segment_id"],
            "episode_id": context["episode_id"],
            "durable_facts": [],
            "workflow_patterns": [],
            "skill_candidates": [],
            "overall_confidence": 0.2,
            "needs_review": True,
            "review_reason": "local_draft_bootstrap",
        }
    else:
        raise ValueError(f"Unknown label pack: {label_pack}")
    validate_label_output(label_pack, value)
    return value


V2_PATTERN_SPECS = [
    ("topic", "agi", "descriptive_code", re.compile(r"\bagi\b|artificial general intelligence", re.I)),
    ("topic", "singularity", "descriptive_code", re.compile(r"\bsingularity\b", re.I)),
    ("topic", "agents", "descriptive_code", re.compile(r"\bagents?\b|agentic|tool use|computer use", re.I)),
    ("adoption_pattern", "ai_coding", "extracted_claim", re.compile(r"codex|cursor|claude code|coding agents?|software engineer", re.I)),
    ("technical_mechanism", "test_time_compute", "analytic_code", re.compile(r"test[- ]time compute|reasoning compute", re.I)),
    ("technical_mechanism", "inference_scaling", "analytic_code", re.compile(r"inference scaling|inference[- ]time search", re.I)),
    ("market_investment_narrative", "open_weights", "signal", re.compile(r"open[- ]weights?|open source model|llama|mistral", re.I)),
    ("safety_risk", "safety_alignment", "analytic_code", re.compile(r"safety|alignment|misuse|governance|deployment safeguards?", re.I)),
    ("adoption_pattern", "enterprise_ai", "extracted_claim", re.compile(r"enterprise ai|copilot|workflow|automation|roi|procurement", re.I)),
    ("technical_mechanism", "evals", "analytic_code", re.compile(r"\bevals?\b|benchmark|score|accuracy", re.I)),
    ("forecast", "forecast_capability", "extracted_claim", re.compile(r"\bwill\s+(replace|change|enable|become|lead|move|scale|improve|reduce)\b|\bgoing to\b|\bnext (big|year|decade)\b|\bfuture\b", re.I)),
]


V3_TERM_SPECS = [
    {
        "pattern": re.compile(r"\bAGI\b|artificial general intelligence", re.I),
        "candidate": "frontier_ai_end_state",
        "target": "AGI",
        "event_type": "term_usage",
        "frames": ["capability_timeline", "frontier_ai_language"],
    },
    {
        "pattern": re.compile(r"\bsingularity\b", re.I),
        "candidate": "frontier_ai_end_state",
        "target": "singularity",
        "event_type": "term_usage",
        "frames": ["terminology_shift", "frontier_ai_language"],
    },
    {
        "pattern": re.compile(r"\bagents?\b|agentic|tool use|computer use", re.I),
        "candidate": "agentic_ai_systems",
        "target": "agents",
        "event_type": "capability_claim",
        "frames": ["agentic_systems", "workflow_automation"],
    },
    {
        "pattern": re.compile(r"coding agents?|codex|cursor|claude code|software engineer", re.I),
        "candidate": "agentic_software_development",
        "target": "AI coding",
        "event_type": "adoption_signal",
        "frames": ["developer_workflow", "software_automation"],
    },
    {
        "pattern": re.compile(r"test[- ]time compute|inference scaling|inference[- ]time search|reasoning compute", re.I),
        "candidate": "inference_time_scaling",
        "target": "inference scaling",
        "event_type": "causal_mechanism",
        "frames": ["capability_mechanism", "compute_scaling"],
    },
    {
        "pattern": re.compile(r"\bevals?\b|benchmark|score|accuracy", re.I),
        "candidate": "ai_evaluation_reliability",
        "target": "evals",
        "event_type": "capability_claim",
        "frames": ["measurement", "deployment_quality"],
    },
    {
        "pattern": re.compile(r"safety|alignment|misuse|governance|deployment safeguards?|security", re.I),
        "candidate": "ai_safety_governance",
        "target": "safety and alignment",
        "event_type": "risk_signal",
        "frames": ["safety_governance", "deployment_risk"],
    },
    {
        "pattern": re.compile(r"enterprise ai|copilot|workflow|automation|roi|procurement", re.I),
        "candidate": "enterprise_ai_adoption",
        "target": "enterprise AI",
        "event_type": "adoption_signal",
        "frames": ["enterprise_adoption", "workflow_integration"],
    },
    {
        "pattern": re.compile(r"open[- ]weights?|open source model|llama|mistral", re.I),
        "candidate": "open_weight_competition",
        "target": "open weights",
        "event_type": "market_signal",
        "frames": ["model_distribution", "competitive_positioning"],
    },
    {
        "pattern": re.compile(r"gemini|chatgpt|claude|gpt[- ]?5|gpt[- ]?4|llama|copilot|codex|cursor|mistral", re.I),
        "candidate": "ai_product_model_narrative",
        "target": "AI model or product",
        "event_type": "product_signal",
        "frames": ["product_narrative", "model_competition"],
    },
    {
        "pattern": re.compile(r"\bwill\s+(replace|change|enable|become|lead|move|scale|improve|reduce)\b|\bgoing to\b|\bnext (big|year|decade)\b|\bfuture\b", re.I),
        "candidate": "ai_capability_forecast",
        "target": "AI forecast",
        "event_type": "forecast",
        "frames": ["forecast", "capability_timeline"],
    },
    {
        "pattern": re.compile(r"not just|instead|rather than|but now|used to|replacing|shift(?:ing)? from|moving from", re.I),
        "candidate": "ai_narrative_shift",
        "target": "narrative shift",
        "event_type": "frame_usage",
        "frames": ["terminology_shift", "framing_change"],
    },
]


def _local_v2_observations(segment_text: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    observations = []
    speaker = _detect_speaker(segment_text)
    entities = _detect_entities(segment_text)
    for code_family, code_id, construct_type, pattern in V2_PATTERN_SPECS:
        match = pattern.search(segment_text)
        if not match:
            continue
        evidence, start, end = _excerpt_span(segment_text, pattern)
        observations.append(
            {
                "code_family": code_family,
                "code_id": code_id,
                "construct_type": construct_type,
                "speaker": speaker,
                "speaker_role": "unknown",
                "entities": entities,
                "stance": _local_stance(segment_text),
                "temporal_horizon": _local_temporal_horizon(segment_text),
                "claim_strength": "explicit",
                "confidence": 0.66,
                "evidence": evidence,
                "evidence_start": start,
                "evidence_end": end,
                "notes": "local_draft_bootstrap",
            }
        )
    return observations[:12]


def _local_v3_discourse_events(segment_text: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    events = []
    speaker = _detect_speaker(segment_text)
    detected = _detect_entities(segment_text)
    for spec in V3_TERM_SPECS:
        match = spec["pattern"].search(segment_text)
        if not match:
            continue
        evidence, start, end = _excerpt_span(segment_text, spec["pattern"])
        surface_terms = _surface_terms_for_span(evidence, spec["pattern"])
        if not surface_terms:
            surface_terms = [match.group(0)]
        event_type = str(spec["event_type"])
        organizations = detected["organizations"]
        product_names = detected["products"]
        model_names = [name for name in product_names if name.lower() in {"gemini", "chatgpt", "claude", "llama", "mistral", "gpt-5", "gpt-4"}]
        actor = _local_v3_actor(speaker, organizations, evidence)
        target = str(spec["target"])
        candidate = str(spec["candidate"])
        frames = list(spec["frames"])
        claim_text = _local_v3_claim_text(event_type, target, surface_terms, candidate)
        events.append(
            {
                "event_type": event_type,
                "actor": actor,
                "target": {
                    "raw_target": target,
                    "candidate_concept": candidate,
                    "canonical_concept": None,
                    "concept_confidence": 0.66,
                },
                "surface_terms": sorted(set(surface_terms), key=str.lower),
                "frames": frames,
                "model_names": model_names,
                "product_names": product_names,
                "organizations": organizations,
                "people": detected["people"],
                "stance": _local_v3_stance(segment_text, event_type),
                "claim_text": claim_text,
                "claim_type": _local_v3_claim_type(event_type),
                "certainty": _local_v3_certainty(segment_text),
                "temporal_horizon": _local_v3_temporal_horizon(segment_text),
                "causal_mechanism": _local_v3_causal_mechanism(event_type, evidence),
                "counterclaim": evidence if event_type == "counterclaim" else "",
                "evidence": evidence,
                "evidence_start": start,
                "evidence_end": end,
                "confidence": 0.67,
                "audit_notes": "local_draft_bootstrap_llm_adjudicated_not_human_validated",
            }
        )
    return _dedupe_v3_events(events)[:30]


def _local_v3_concept_candidates(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_candidate: dict[str, dict[str, Any]] = {}
    for event in events:
        candidate = event["target"]["candidate_concept"]
        existing = by_candidate.get(candidate)
        surface_terms = list(event.get("surface_terms") or [])
        if existing:
            existing["surface_terms"] = sorted(set(existing["surface_terms"] + surface_terms), key=str.lower)
            existing["usefulness_score"] = max(existing["usefulness_score"], _candidate_usefulness(candidate, surface_terms))
            existing["confidence"] = max(existing["confidence"], event["confidence"])
            continue
        by_candidate[candidate] = {
            "candidate": candidate,
            "surface_terms": sorted(set(surface_terms), key=str.lower),
            "rationale": _candidate_rationale(candidate),
            "usefulness_score": _candidate_usefulness(candidate, surface_terms),
            "evidence": event["evidence"],
            "evidence_start": event["evidence_start"],
            "evidence_end": event["evidence_end"],
            "confidence": event["confidence"],
        }
    return list(by_candidate.values())


def _surface_terms_for_span(evidence: str, pattern: re.Pattern[str]) -> list[str]:
    terms = []
    for match in pattern.finditer(evidence):
        term = match.group(0).strip()
        if term:
            terms.append(term)
    return terms


def _local_v3_actor(speaker: str | None, organizations: list[str], evidence: str) -> dict[str, str | None]:
    if speaker:
        return {"name": speaker, "actor_type": "person", "affiliation": None, "role": "speaker"}
    for organization in organizations:
        if re.search(rf"\b{re.escape(organization)}\b", evidence):
            return {"name": organization, "actor_type": "organization", "affiliation": organization, "role": "mentioned_actor"}
    return {"name": "unknown", "actor_type": "unknown", "affiliation": None, "role": None}


def _local_v3_claim_text(event_type: str, target: str, surface_terms: list[str], candidate: str) -> str:
    terms = ", ".join(surface_terms[:4]) if surface_terms else target
    if event_type == "term_usage":
        return f"Segment uses terms around {target}: {terms}."
    if event_type == "product_signal":
        return f"Segment discusses product or model narrative around {terms}."
    if event_type == "forecast":
        return f"Segment contains a forward-looking AI forecast around {target}."
    if event_type == "risk_signal":
        return f"Segment raises risk or governance framing around {target}."
    if event_type == "market_signal":
        return f"Segment frames a market or competitive signal around {target}."
    if event_type == "adoption_signal":
        return f"Segment signals adoption movement around {target}."
    if event_type == "causal_mechanism":
        return f"Segment states or implies a mechanism for {candidate}."
    return f"Segment contains a discourse signal for {candidate}."


def _local_v3_claim_type(event_type: str) -> str:
    mapping = {
        "term_usage": "terminology",
        "frame_usage": "descriptive",
        "stance_position": "descriptive",
        "forecast": "prediction",
        "causal_mechanism": "causal",
        "capability_claim": "descriptive",
        "product_signal": "product_market",
        "market_signal": "product_market",
        "risk_signal": "descriptive",
        "counterclaim": "counterclaim",
        "uncertainty": "uncertainty",
        "adoption_signal": "descriptive",
    }
    return mapping.get(event_type, "descriptive")


def _local_v3_stance(text: str, event_type: str) -> str:
    if event_type == "risk_signal":
        return "warning"
    lowered = text.lower()
    if any(token in lowered for token in ["not", "can't", "cannot", "skeptical", "unlikely", "instead", "rather than"]):
        return "skeptical"
    if any(token in lowered for token in ["risk", "misuse", "danger", "unsafe", "warning"]):
        return "warning"
    if any(token in lowered for token in ["better", "useful", "growth", "improve", "opportunity", "enable"]):
        return "supportive"
    return "neutral"


def _local_v3_certainty(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["maybe", "might", "could", "unclear", "i think", "probably"]):
        return "hedged"
    if any(token in lowered for token in ["definitely", "certain", "always", "must"]):
        return "high"
    return "medium"


def _local_v3_temporal_horizon(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["next year", "next decade", "future", "will", "going to"]):
        return "near_future"
    if any(token in lowered for token in ["today", "now", "currently", "this year"]):
        return "present"
    if any(token in lowered for token in ["used to", "previously", "last year"]):
        return "past"
    return "unspecified"


def _local_v3_causal_mechanism(event_type: str, evidence: str) -> str:
    if event_type != "causal_mechanism":
        return ""
    return evidence[:500]


def _candidate_rationale(candidate: str) -> str:
    rationales = {
        "frontier_ai_end_state": "Tracks substitutions and frames around AGI, singularity, and long-run frontier AI outcomes.",
        "agentic_ai_systems": "Tracks agent language, tool-use framing, and shifts from chatbot to agentic workflows.",
        "agentic_software_development": "Tracks AI coding, developer workflow, and software labor substitution narratives.",
        "inference_time_scaling": "Tracks reasoning compute, test-time compute, and inference-time capability mechanisms.",
        "ai_evaluation_reliability": "Tracks evaluation, benchmark, measurement, and deployment-quality discourse.",
        "ai_safety_governance": "Tracks safety, alignment, misuse, governance, and deployment-risk framing.",
        "enterprise_ai_adoption": "Tracks enterprise workflow, procurement, ROI, and deployment adoption signals.",
        "open_weight_competition": "Tracks open-weight model competition and distribution narratives.",
        "ai_product_model_narrative": "Tracks product and model narrative movement across companies and releases.",
        "ai_capability_forecast": "Tracks forward-looking capability, market, and adoption forecasts.",
        "ai_narrative_shift": "Tracks explicit framing and terminology changes.",
    }
    return rationales.get(candidate, "Candidate concept proposed by local discourse extractor.")


def _candidate_usefulness(candidate: str, surface_terms: list[str]) -> float:
    base = 0.7 if candidate in {
        "frontier_ai_end_state",
        "agentic_ai_systems",
        "agentic_software_development",
        "inference_time_scaling",
        "enterprise_ai_adoption",
        "ai_product_model_narrative",
        "ai_narrative_shift",
    } else 0.62
    if len(set(surface_terms)) >= 2:
        base += 0.08
    return min(base, 0.92)


def _dedupe_v3_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for event in events:
        key = (
            event["event_type"],
            event["target"]["candidate_concept"],
            event["evidence_start"],
            event["evidence_end"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)
    return deduped


def _excerpt_span(text: str, pattern: re.Pattern[str]) -> tuple[str, int, int]:
    match = pattern.search(text)
    if not match:
        end = min(180, len(text))
        return text[:end], 0, end
    start = max(match.start() - 70, 0)
    end = min(match.end() + 110, len(text))
    while start < match.start() and start > 0 and not text[start].isspace():
        start += 1
    while end < len(text) and not text[end - 1].isspace():
        end -= 1
    evidence = text[start:end].strip()
    stripped_start = text.find(evidence, start, end + 1)
    if stripped_start >= 0:
        start = stripped_start
        end = start + len(evidence)
    return evidence, start, end


def _detect_speaker(text: str) -> str | None:
    match = re.match(r"\s*([A-Z][A-Za-z0-9 ._'&-]{1,48}|[A-Z][A-Z0-9 ._'&-]{1,48}):\s+", text)
    return match.group(1).strip() if match else None


def _detect_entities(text: str) -> dict[str, list[str]]:
    organizations = [
        name
        for name in ["OpenAI", "Anthropic", "Google", "Microsoft", "Meta", "NVIDIA", "Apple", "Amazon", "Mistral", "xAI", "DeepMind"]
        if re.search(rf"\b{re.escape(name)}\b", text)
    ]
    products = [
        name
        for name in ["Codex", "Claude Code", "Cursor", "Gemini", "Llama", "Copilot", "ChatGPT", "Claude", "GPT-5", "GPT-4", "Mistral"]
        if re.search(rf"\b{re.escape(name)}\b", text, re.I)
    ]
    return {"people": [], "organizations": organizations, "products": products}


def _local_stance(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["risk", "misuse", "danger", "unsafe", "warning"]):
        return "warning"
    if any(token in lowered for token in ["not", "can't", "cannot", "skeptical", "unlikely"]):
        return "skeptical"
    if any(token in lowered for token in ["better", "useful", "growth", "improve", "opportunity"]):
        return "supportive"
    return "neutral"


def _local_temporal_horizon(text: str) -> str:
    lowered = text.lower()
    if any(token in lowered for token in ["next year", "next decade", "future", "will", "going to"]):
        return "near_future"
    if any(token in lowered for token in ["today", "now", "currently"]):
        return "present"
    if any(token in lowered for token in ["used to", "previously", "last year"]):
        return "past"
    return "unspecified"


def _excerpt(text: str, pattern: re.Pattern[str]) -> str:
    match = pattern.search(text)
    if not match:
        return text[:180]
    start = max(match.start() - 80, 0)
    end = min(match.end() + 120, len(text))
    return text[start:end].strip()
