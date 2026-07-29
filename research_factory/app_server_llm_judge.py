from __future__ import annotations

"""Blinded, app-server-only semantic judging for extraction evaluations.

This module intentionally has no paid API or per-call CLI fallback.  Every
semantic call goes through :class:`CodexAppServerClient`, which verifies that the
active account is a managed ChatGPT account.  Deterministic code here is limited
to artifact integrity, opaque identifiers, exact evidence spans, schema/partition
checks, accounting, and scoring already-frozen calibration truth.
"""

import argparse
import asyncio
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_checkpoint import (
    build_holdout_leaf_binding,
    validate_holdout_leaf_binding,
    validate_managed_sidecar_execution_lineage,
)
from .codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    TURN_SIDECAR_SCHEMA_VERSION,
    CodexAppServerClient,
)
from .util import sha256_text, stable_id, write_text_atomic


SHARED_WITNESS_POOL_VERSION = "pif_app_server_shared_witness_pool_v2"
WITNESS_MAPPING_VERSION = "pif_app_server_witness_mapping_v2"
JUDGE_VARIANT_VERSION = "pif_app_server_semantic_judge_variant_v2"
JUDGE_RUN_VERSION = "pif_app_server_semantic_judge_run_v3"
JUDGE_CONSENSUS_VERSION = "pif_app_server_semantic_judge_consensus_v2"
JUDGE_CALIBRATION_VERSION = "pif_app_server_semantic_judge_calibration_v3"
SHARED_REFERENCE_SCORE_VERSION = "pif_app_server_shared_reference_score_v2"

PARTITION_CALIBRATION_GATES = {
    "pairwise_f1_min": 0.95,
    "exact_case_rate_min": 0.95,
    "same_side_exact_rate_min": 1.0,
    "minimum_same_side_cases": 6,
}

SUPPORT_VERDICTS = ("supported", "unsupported", "abstain")
ALIGNMENT_RELATIONS = ("equivalent", "partial", "non_equivalent", "abstain")
MISMATCH_FIELDS = (
    "actor",
    "attribution",
    "causal_mechanism",
    "certainty",
    "event_boundary",
    "event_type",
    "evidence",
    "metric",
    "negation",
    "reported_actor",
    "speaker",
    "stance",
    "target",
    "temporal_horizon",
    "unsupported_inference",
)

_CASE_ID_RE = re.compile(r"^jcase_[0-9a-f]{24}$")
_WITNESS_ID_RE = re.compile(r"^wit_[0-9a-f]{24}$")
_PROVENANCE_KEYS = {
    "provenance",
    "system_id",
    "system_name",
    "source_system",
    "arm_id",
    "variant_id",
    "run_id",
    "artifact_path",
    "output_path",
    "label_output_path",
}
_UNSUPPORTED_APP_SERVER_SCHEMA_KEYWORDS = {
    "$schema",
    "allOf",
    "dependentRequired",
    "dependentSchemas",
    "else",
    "if",
    "not",
    "patternProperties",
    "then",
    "uniqueItems",
}

# Model-facing events use one origin-neutral schema.  The source extractors and
# the historical baseline intentionally have different storage schemas; sending
# those raw objects to the judge would disclose origin through field names even
# after system IDs were removed.  These keys contain only semantic content used
# by support/equivalence decisions.  Storage-only confidence, offsets, audit,
# window, and validator fields remain in the private mapping's ``original_event``.
NEUTRAL_EVENT_KEYS = (
    "event_type",
    "event_subtype",
    "claim_type",
    "claim_text",
    "speaker_name",
    "speaker_role",
    "speaker_affiliation",
    "actor_name",
    "actor_type",
    "actor_role",
    "actor_affiliation",
    "reported_actor_name",
    "reported_actor_type",
    "reported_actor_affiliation",
    "target_name",
    "target_concept",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "counterclaim",
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
    "source_context_kind",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "evidence",
    "submitted_evidence",
    "submitted_evidence_exact",
)

_KNOWN_EVENT_INPUT_KEYS = {
    # Shared semantic fields.
    "event_type",
    "event_subtype",
    "claim_type",
    "claim_text",
    "claim",
    "atomic_claim",
    "speaker",
    "speaker_name",
    "speaker_role",
    "speaker_context",
    "actor",
    "actor_name",
    "actor_type",
    "reported_actor",
    "reported_actor_name",
    "reported_actor_type",
    "target",
    "target_concept",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "counterclaim",
    "metric",
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
    "source_context",
    "source_context_kind",
    "source_kind",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "evidence",
    "submitted_evidence",
    "submitted_evidence_exact",
    # Known storage/validator metadata deliberately omitted from the prompt.
    "audit_notes",
    "confidence",
    "evidence_start",
    "evidence_end",
    "exclusion_flags",
    "frames",
    "quality_flags",
    "signal_reason",
    "surface_terms",
    "window_id",
}

_ACTOR_OBJECT_KEYS = {"name", "actor_type", "affiliation", "role", "confidence"}
_SPEAKER_OBJECT_KEYS = {"name", "role", "affiliation", "confidence"}
_TARGET_OBJECT_KEYS = {
    "raw_target",
    "candidate_concept",
    "canonical_concept",
    "concept_confidence",
}
_METRIC_OBJECT_KEYS = {"value", "unit", "comparator", "direction", "raw_text"}
_SOURCE_CONTEXT_OBJECT_KEYS = {"kind", "confidence", "rationale"}


class JudgeArtifactError(ValueError):
    """Raised when an immutable or checkpointed judge artifact is inconsistent."""


class JudgeAttemptFailed(JudgeArtifactError):
    """A terminal app-server judge turn failed and must never be retried implicitly."""

    def __init__(self, *, variant: str, error_class: str, sidecar_path: Path) -> None:
        self.variant = variant
        self.error_class = error_class
        self.sidecar_path = Path(sidecar_path).expanduser().resolve()
        super().__init__(
            "app-server judge variant %s failed with %s; terminal sidecar requires explicit "
            "versioned recovery" % (variant, error_class)
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise JudgeArtifactError("judge artifact is missing or invalid JSON: %s" % path) from exc


def write_immutable_json(path: Path, payload: Any) -> bool:
    """Create ``path`` once, or verify its canonical content on a resumed run.

    Returns ``True`` when the file was created and ``False`` when an identical
    checkpoint already existed.  A changed checkpoint always fails closed.
    """

    target = Path(path).expanduser().resolve()
    if target.exists():
        prior = _load_json(target)
        if _canonical_json(prior) != _canonical_json(payload):
            raise JudgeArtifactError("immutable judge artifact changed: %s" % target)
        return False
    write_text_atomic(target, _pretty_json(payload))
    return True


def _write_immutable_text(path: Path, text: str) -> bool:
    target = Path(path).expanduser().resolve()
    if target.exists():
        if target.read_text(encoding="utf-8") != text:
            raise JudgeArtifactError("immutable judge artifact changed: %s" % target)
        return False
    write_text_atomic(target, text)
    return True


def _provenance_paths(value: Any, prefix: str = "event") -> List[str]:
    hits = []
    if isinstance(value, dict):
        for key, child in value.items():
            path = "%s.%s" % (prefix, key)
            if key in _PROVENANCE_KEYS:
                hits.append(path)
            hits.extend(_provenance_paths(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            hits.extend(_provenance_paths(child, "%s[%d]" % (prefix, index)))
    return hits


def _event_and_provenance(value: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if (
        isinstance(value, dict)
        and isinstance(value.get("event"), dict)
        and set(value).issubset({"event", "provenance"})
    ):
        provenance = value.get("provenance") or {}
        if not isinstance(provenance, dict):
            raise ValueError("witness provenance must be an object")
        return deepcopy(value["event"]), deepcopy(provenance)
    if not isinstance(value, dict):
        raise ValueError("each witness event must be an object")
    return deepcopy(value), {}


def _exact_event_evidence(event: Mapping[str, Any], source_excerpt: str) -> None:
    evidence = event.get("evidence")
    if not isinstance(evidence, str) or not evidence:
        raise ValueError("every witness event requires a non-empty evidence string")
    if evidence not in source_excerpt:
        raise ValueError("witness event evidence is not an exact source span")


def _neutral_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _canonical_json(value)
    if isinstance(value, str):
        return value
    raise ValueError("neutral judge scalar must be a string, number, boolean, or null")


def _neutral_string_list(value: Any, *, field: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("neutral judge %s must be a string array" % field)
    return sorted(set(value))


def _checked_object(value: Any, *, field: str, allowed: Set[str]) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return {"name": value}
    if not isinstance(value, Mapping):
        raise ValueError("neutral judge %s must be an object, string, or null" % field)
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(
            "unknown semantic fields in %s: %s" % (field, ", ".join(unknown))
        )
    return value


def neutral_event_projection(
    event: Mapping[str, Any], source_excerpt: str
) -> Dict[str, Any]:
    """Render one storage event into the common blinded judge schema.

    This is structural schema normalization, not semantic pruning.  Every known
    semantic field has one neutral destination and every output has the exact
    same keys and missing-value conventions.  Unknown fields fail closed so a
    newly introduced semantic field cannot silently disappear from evaluation.
    """

    if not isinstance(event, Mapping):
        raise ValueError("neutral judge event must be an object")
    unknown = sorted(set(event) - _KNOWN_EVENT_INPUT_KEYS)
    if unknown:
        raise ValueError("unknown semantic event fields: %s" % ", ".join(unknown))

    actor = _checked_object(event.get("actor"), field="actor", allowed=_ACTOR_OBJECT_KEYS)
    reported = _checked_object(
        event.get("reported_actor"), field="reported_actor", allowed=_ACTOR_OBJECT_KEYS
    )
    speaker = _checked_object(
        event.get("speaker_context", event.get("speaker")),
        field="speaker",
        allowed=_SPEAKER_OBJECT_KEYS,
    )
    target = _checked_object(
        event.get("target"), field="target", allowed=_TARGET_OBJECT_KEYS
    )
    metric = _checked_object(
        event.get("metric"), field="metric", allowed=_METRIC_OBJECT_KEYS
    )
    source_context = _checked_object(
        event.get("source_context"),
        field="source_context",
        allowed=_SOURCE_CONTEXT_OBJECT_KEYS,
    )

    original_evidence = event.get("evidence")
    evidence_exact = bool(
        isinstance(original_evidence, str)
        and original_evidence
        and original_evidence in source_excerpt
    )
    evidence = original_evidence if evidence_exact else source_excerpt
    prior_submitted = event.get("submitted_evidence")
    submitted_evidence = (
        _neutral_text(prior_submitted)
        if prior_submitted is not None
        else ""
        if evidence_exact
        else _neutral_text(original_evidence)
    )
    prior_exact = event.get("submitted_evidence_exact")
    submitted_exact = (
        bool(prior_exact) if isinstance(prior_exact, bool) else evidence_exact
    )

    claim_text = event.get("claim_text")
    if claim_text in (None, ""):
        claim_text = event.get("atomic_claim", event.get("claim"))
    speaker_name = event.get("speaker_name")
    if speaker_name in (None, ""):
        speaker_name = speaker.get("name")
    speaker_role = event.get("speaker_role")
    if speaker_role in (None, ""):
        speaker_role = speaker.get("role")
    actor_name = event.get("actor_name")
    if actor_name in (None, ""):
        actor_name = actor.get("name")
    actor_type = event.get("actor_type")
    if actor_type in (None, ""):
        actor_type = actor.get("actor_type")
    reported_name = event.get("reported_actor_name")
    if reported_name in (None, ""):
        reported_name = reported.get("name")
    reported_type = event.get("reported_actor_type")
    if reported_type in (None, ""):
        reported_type = reported.get("actor_type")

    target_name = target.get("raw_target") or (
        event.get("target") if isinstance(event.get("target"), str) else None
    )
    target_concept = event.get("target_concept")
    if target_concept in (None, ""):
        target_concept = target.get("canonical_concept") or target.get("candidate_concept")

    result = {
        "event_type": _neutral_text(event.get("event_type")),
        "event_subtype": _neutral_text(event.get("event_subtype")),
        "claim_type": _neutral_text(event.get("claim_type")),
        "claim_text": _neutral_text(claim_text),
        "speaker_name": _neutral_text(speaker_name),
        "speaker_role": _neutral_text(speaker_role),
        "speaker_affiliation": _neutral_text(speaker.get("affiliation")),
        "actor_name": _neutral_text(actor_name),
        "actor_type": _neutral_text(actor_type),
        "actor_role": _neutral_text(actor.get("role")),
        "actor_affiliation": _neutral_text(actor.get("affiliation")),
        "reported_actor_name": _neutral_text(reported_name),
        "reported_actor_type": _neutral_text(reported_type),
        "reported_actor_affiliation": _neutral_text(reported.get("affiliation")),
        "target_name": _neutral_text(target_name),
        "target_concept": _neutral_text(target_concept),
        "stance": _neutral_text(event.get("stance")),
        "certainty": _neutral_text(event.get("certainty")),
        "temporal_horizon": _neutral_text(event.get("temporal_horizon")),
        "causal_mechanism": _neutral_text(event.get("causal_mechanism")),
        "counterclaim": _neutral_text(event.get("counterclaim")),
        "metric_value": _neutral_text(event.get("metric_value", metric.get("value"))),
        "metric_unit": _neutral_text(event.get("metric_unit", metric.get("unit"))),
        "metric_comparator": _neutral_text(
            event.get("metric_comparator", metric.get("comparator"))
        ),
        "metric_direction": _neutral_text(
            event.get("metric_direction", metric.get("direction"))
        ),
        "metric_raw_text": _neutral_text(
            event.get("metric_raw_text", metric.get("raw_text"))
        ),
        "source_context_kind": _neutral_text(
            event.get(
                "source_context_kind",
                event.get("source_kind", source_context.get("kind")),
            )
        ),
        "model_names": _neutral_string_list(event.get("model_names"), field="model_names"),
        "product_names": _neutral_string_list(
            event.get("product_names"), field="product_names"
        ),
        "organizations": _neutral_string_list(
            event.get("organizations"), field="organizations"
        ),
        "people": _neutral_string_list(event.get("people"), field="people"),
        "evidence": str(evidence),
        "submitted_evidence": submitted_evidence,
        "submitted_evidence_exact": submitted_exact,
    }
    if tuple(result) != NEUTRAL_EVENT_KEYS:
        raise AssertionError("neutral judge projection key order drift")
    return result


def make_shared_witness_pool(
    raw_cases: Sequence[Mapping[str, Any]],
    *,
    seed: str = "app-server-semantic-judge-v1",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Blind source packets and events into one shared, immutable witness pool.

    ``raw_cases`` contain ``case_key``, ``source_excerpt``, ``event_set_a``, and
    ``event_set_b``.  An event may be wrapped as ``{"event": ..., "provenance":
    ...}``; provenance is retained only in the returned private mapping and is
    never copied into the model-facing pool.
    """

    if not raw_cases:
        raise ValueError("a shared witness pool requires at least one case")
    rows = []
    mapping_rows = []
    case_keys = set()  # type: Set[str]
    witness_ids = set()  # type: Set[str]
    for case_index, raw_case in enumerate(raw_cases):
        if not isinstance(raw_case, Mapping):
            raise ValueError("each raw witness case must be an object")
        case_key = raw_case.get("case_key")
        if not isinstance(case_key, str) or not case_key:
            raise ValueError("each raw witness case requires a stable case_key")
        if case_key in case_keys:
            raise ValueError("raw witness case_key values must be unique")
        case_keys.add(case_key)
        source_excerpt = raw_case.get("source_excerpt")
        if not isinstance(source_excerpt, str) or not source_excerpt:
            raise ValueError("each witness case requires a non-empty source_excerpt")
        case_id = stable_id(seed, "case", case_key, prefix="jcase_")
        rendered_sets = {}  # type: Dict[str, List[Dict[str, Any]]]
        mapped_witnesses = []
        for side in ("a", "b"):
            raw_events = raw_case.get("event_set_%s" % side)
            if not isinstance(raw_events, list):
                raise ValueError("event_set_%s must be an array" % side)
            rendered = []
            for event_index, raw_event in enumerate(raw_events):
                event, provenance = _event_and_provenance(raw_event)
                paths = _provenance_paths(event)
                if paths:
                    raise ValueError(
                        "event contains origin provenance that cannot enter a judge prompt: %s"
                        % ", ".join(paths)
                    )
                neutral_event = neutral_event_projection(event, source_excerpt)
                _exact_event_evidence(neutral_event, source_excerpt)
                witness_id = stable_id(
                    seed,
                    "witness",
                    case_key,
                    side,
                    str(event_index),
                    prefix="wit_",
                )
                if witness_id in witness_ids:
                    raise ValueError("opaque witness ID collision")
                witness_ids.add(witness_id)
                rendered.append({"witness_id": witness_id, "event": neutral_event})
                private_provenance = deepcopy(provenance)
                if "original_event" in private_provenance:
                    raise ValueError("witness provenance reserves original_event")
                private_provenance["original_event"] = event
                mapped_witnesses.append(
                    {
                        "witness_id": witness_id,
                        "canonical_side": side,
                        "canonical_index": event_index,
                        "provenance": private_provenance,
                    }
                )
            rendered_sets[side] = rendered
        rows.append(
            {
                "case_id": case_id,
                "source_excerpt": source_excerpt,
                "event_set_a": rendered_sets["a"],
                "event_set_b": rendered_sets["b"],
            }
        )
        mapping_rows.append(
            {
                "case_id": case_id,
                "case_key": case_key,
                "case_index": case_index,
                "case_provenance": deepcopy(raw_case.get("provenance") or {}),
                "witnesses": mapped_witnesses,
            }
        )
    pool = {
        "schema_version": SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text(seed),
        "cases": rows,
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }
    mapping = {
        "schema_version": WITNESS_MAPPING_VERSION,
        "pool_seed_sha256": sha256_text(seed),
        "cases": mapping_rows,
        "privacy": "private_analysis_only_contains_origin_provenance_never_prompted",
    }
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise ValueError("invalid generated witness pool: %s" % "; ".join(errors))
    return pool, mapping


def build_shared_witness_pool(
    raw_cases: Sequence[Mapping[str, Any]],
    *,
    output_path: Path,
    private_mapping_path: Path,
    seed: str = "app-server-semantic-judge-v1",
) -> Dict[str, Any]:
    pool, mapping = make_shared_witness_pool(raw_cases, seed=seed)
    write_immutable_json(Path(output_path), pool)
    write_immutable_json(Path(private_mapping_path), mapping)
    return pool


def validate_shared_witness_pool(pool: Any) -> List[str]:
    errors = []
    if not isinstance(pool, dict) or set(pool) != {
        "schema_version",
        "seed_sha256",
        "cases",
        "privacy",
    }:
        return ["invalid_pool_root"]
    if pool.get("schema_version") != SHARED_WITNESS_POOL_VERSION:
        errors.append("unsupported_pool_version")
    cases = pool.get("cases")
    if not isinstance(cases, list) or not cases:
        return errors + ["pool_cases_missing"]
    seen_cases = set()
    seen_witnesses = set()
    for case_index, case in enumerate(cases):
        prefix = "case_%d" % case_index
        if not isinstance(case, dict) or set(case) != {
            "case_id",
            "source_excerpt",
            "event_set_a",
            "event_set_b",
        }:
            errors.append(prefix + "_invalid_shape")
            continue
        case_id = case.get("case_id")
        if (
            not isinstance(case_id, str)
            or not _CASE_ID_RE.fullmatch(case_id)
            or case_id in seen_cases
        ):
            errors.append(prefix + "_invalid_or_duplicate_id")
        else:
            seen_cases.add(case_id)
        source = case.get("source_excerpt")
        if not isinstance(source, str) or not source:
            errors.append(prefix + "_source_missing")
            source = ""
        case_witness_count = 0
        for side in ("a", "b"):
            witnesses = case.get("event_set_%s" % side)
            if not isinstance(witnesses, list):
                errors.append(prefix + "_set_%s_not_array" % side)
                continue
            case_witness_count += len(witnesses)
            for witness_index, witness in enumerate(witnesses):
                witness_prefix = "%s_%s_%d" % (prefix, side, witness_index)
                if not isinstance(witness, dict) or set(witness) != {"witness_id", "event"}:
                    errors.append(witness_prefix + "_invalid_shape")
                    continue
                witness_id = witness.get("witness_id")
                if (
                    not isinstance(witness_id, str)
                    or not _WITNESS_ID_RE.fullmatch(witness_id)
                    or witness_id in seen_witnesses
                ):
                    errors.append(witness_prefix + "_invalid_or_duplicate_id")
                else:
                    seen_witnesses.add(witness_id)
                event = witness.get("event")
                if not isinstance(event, dict):
                    errors.append(witness_prefix + "_event_not_object")
                    continue
                if _provenance_paths(event):
                    errors.append(witness_prefix + "_contains_provenance")
                evidence = event.get("evidence")
                if not isinstance(evidence, str) or not evidence or evidence not in source:
                    errors.append(witness_prefix + "_evidence_not_exact")
    return errors


def build_judge_variants(pool: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise ValueError("invalid shared witness pool: %s" % "; ".join(errors))
    variants = {}
    for name, reverse in (("ab", False), ("ba", True)):
        cases = []
        for case in pool["cases"]:
            cases.append(
                {
                    "case_id": case["case_id"],
                    "source_excerpt": case["source_excerpt"],
                    "event_set_a": deepcopy(
                        case["event_set_b"] if reverse else case["event_set_a"]
                    ),
                    "event_set_b": deepcopy(
                        case["event_set_a"] if reverse else case["event_set_b"]
                    ),
                }
            )
        variants[name] = {
            "schema_version": JUDGE_VARIANT_VERSION,
            "variant": name,
            "orientation": "ba" if reverse else "ab",
            "cases": cases,
        }
    if [case["case_id"] for case in variants["ab"]["cases"]] != [
        case["case_id"] for case in variants["ba"]["cases"]
    ]:
        raise AssertionError("judge variants changed case IDs or order")
    return variants


def semantic_judge_output_schema(variant: Mapping[str, Any]) -> Dict[str, Any]:
    cases = variant.get("cases") or []
    case_ids = [case["case_id"] for case in cases]
    witness_ids = [
        witness["witness_id"]
        for case in cases
        for side in ("a", "b")
        for witness in case["event_set_%s" % side]
    ]
    witness_id_schema = (
        {"type": "string", "enum": witness_ids}
        if witness_ids
        else {"type": "string", "pattern": r"^wit_[0-9a-f]{24}$"}
    )
    support = {
        "type": "object",
        "additionalProperties": False,
        "required": ["witness_id", "verdict", "evidence_spans", "rationale"],
        "properties": {
            "witness_id": witness_id_schema,
            "verdict": {"type": "string", "enum": list(SUPPORT_VERDICTS)},
            "evidence_spans": {
                "type": "array",
                "maxItems": 4,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 800},
        },
    }
    alignment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "left_witness_id",
            "right_witness_id",
            "relation",
            "mismatch_fields",
            "rationale",
        ],
        "properties": {
            "left_witness_id": witness_id_schema,
            "right_witness_id": witness_id_schema,
            "relation": {"type": "string", "enum": list(ALIGNMENT_RELATIONS)},
            "mismatch_fields": {
                "type": "array",
                "items": {"type": "string", "enum": list(MISMATCH_FIELDS)},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 800},
        },
    }
    equivalence_group = {
        "type": "object",
        "additionalProperties": False,
        "required": ["witness_ids", "rationale"],
        "properties": {
            "witness_ids": {
                "type": "array",
                "minItems": 1,
                "items": witness_id_schema,
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 800},
        },
    }
    case_result = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "support_results",
            "equivalence_groups",
            "alignments",
            "unaligned_left_witness_ids",
            "unaligned_right_witness_ids",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "support_results": {"type": "array", "items": support},
            "equivalence_groups": {"type": "array", "items": equivalence_group},
            "alignments": {"type": "array", "items": alignment},
            "unaligned_left_witness_ids": {
                "type": "array",
                "items": witness_id_schema,
            },
            "unaligned_right_witness_ids": {
                "type": "array",
                "items": witness_id_schema,
            },
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": case_result,
            }
        },
    }
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise ValueError("judge output schema uses unsupported app-server keywords")
    return schema


def validate_app_server_output_schema_subset(schema: Any) -> List[str]:
    """Reject JSON Schema keywords outside the Structured Outputs subset.

    Uniqueness and partition constraints remain deterministic post-generation
    validation; they are intentionally not sent as ``uniqueItems`` because the
    app-server Structured Outputs backend rejects that keyword.
    """

    errors = []

    def visit(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = "%s.%s" % (path, key)
                if key in _UNSUPPORTED_APP_SERVER_SCHEMA_KEYWORDS:
                    errors.append(child_path)
                visit(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, "%s[%d]" % (path, index))

    visit(schema, "$")
    return errors


def judge_base_instructions() -> str:
    return (
        "You are a blinded semantic evaluator for private research extraction. "
        "Judge only the supplied anonymous source excerpts and anonymous events. "
        "Never infer which system produced an event and never prefer a side. First "
        "decide source support independently for every event; then partition all events "
        "into full-semantic-equivalence groups without regard to side; only then align the two "
        "multi-event sets one-to-one for partial/split/merge diagnostics. Supported means the full event is grounded by "
        "the source, including actor, attribution, stance, certainty, time, metric, "
        "negation, target, mechanism, and event boundary. Use abstain when the supplied "
        "source cannot resolve the decision. Equivalent permits harmless wording only; "
        "partial requires a shared core event with a material merge, split, omission, "
        "or expansion; non_equivalent means the aligned events materially conflict. "
        "Distinct analytical lenses may share the same evidence and remain distinct. "
        "Judge meaning directly. Do not use keyword, regex, token-overlap, embedding, "
        "or phrase-match heuristics."
    )


def build_judge_prompt(variant: Mapping[str, Any]) -> str:
    if variant.get("schema_version") != JUDGE_VARIANT_VERSION:
        raise ValueError("unsupported judge variant")
    instructions = (
        "For each case, complete support_results first and include every witness ID "
        "from both sets exactly once. A supported verdict requires at least one exact, "
        "verbatim source span in evidence_spans; every returned span must be an exact "
        "substring of source_excerpt. Next return equivalence_groups as a disjoint, "
        "non-empty partition of every witness ID from both sets exactly once. Put fully "
        "semantically equivalent events together even when they are on the same side; "
        "use singleton groups for events with no full equivalent. Grouping must ignore "
        "presentation side and harmless wording but preserve every material field. Then "
        "align semantically corresponding events one-to-one only for partial/split/merge "
        "diagnostics. Every left and right witness ID must occur exactly once, either "
        "in one alignment or in its side's unaligned list. Do not force unrelated "
        "events into pairs. Use an empty mismatch_fields list for equivalent or abstain. "
        "Return every opaque case_id exactly once."
    )
    packets = {"cases": variant["cases"]}
    return instructions + "\n\n# Blinded cases\n" + _canonical_json(packets) + "\n"


def _case_by_id(variant: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {str(case["case_id"]): case for case in variant.get("cases") or []}


def validate_judge_output(output: Any, variant: Mapping[str, Any]) -> List[str]:
    if not isinstance(output, dict) or set(output) != {"cases"} or not isinstance(
        output.get("cases"), list
    ):
        return ["invalid_output_root"]
    expected = _case_by_id(variant)
    errors = []
    if len(output["cases"]) != len(expected):
        errors.append("case_count_mismatch")
    seen_cases = set()
    for row_index, row in enumerate(output["cases"]):
        prefix = "case_%d" % row_index
        if not isinstance(row, dict) or set(row) != {
            "case_id",
            "support_results",
            "equivalence_groups",
            "alignments",
            "unaligned_left_witness_ids",
            "unaligned_right_witness_ids",
        }:
            errors.append(prefix + "_invalid_shape")
            continue
        case_id = row.get("case_id")
        if not isinstance(case_id, str) or case_id not in expected or case_id in seen_cases:
            errors.append(prefix + "_invalid_or_duplicate_id")
            continue
        seen_cases.add(case_id)
        case = expected[case_id]
        source = case["source_excerpt"]
        left_ids = {item["witness_id"] for item in case["event_set_a"]}
        right_ids = {item["witness_id"] for item in case["event_set_b"]}
        all_ids = left_ids | right_ids
        support_rows = row.get("support_results")
        support_seen = set()
        if not isinstance(support_rows, list):
            errors.append(prefix + "_support_not_array")
        else:
            for support_index, support in enumerate(support_rows):
                support_prefix = "%s_support_%d" % (prefix, support_index)
                if not isinstance(support, dict) or set(support) != {
                    "witness_id",
                    "verdict",
                    "evidence_spans",
                    "rationale",
                }:
                    errors.append(support_prefix + "_invalid_shape")
                    continue
                witness_id = support.get("witness_id")
                verdict = support.get("verdict")
                spans = support.get("evidence_spans")
                rationale = support.get("rationale")
                if (
                    not isinstance(witness_id, str)
                    or witness_id not in all_ids
                    or witness_id in support_seen
                ):
                    errors.append(support_prefix + "_invalid_or_duplicate_id")
                else:
                    support_seen.add(witness_id)
                if verdict not in SUPPORT_VERDICTS:
                    errors.append(support_prefix + "_invalid_verdict")
                spans_are_strings = isinstance(spans, list) and all(
                    isinstance(span, str) for span in spans
                )
                if (
                    not spans_are_strings
                    or len(spans) > 4
                    or len(spans) != len(set(spans))
                    or any(not span or len(span) > 1000 or span not in source for span in spans)
                ):
                    errors.append(support_prefix + "_evidence_not_exact")
                elif verdict == "supported" and not spans:
                    errors.append(support_prefix + "_supported_without_evidence")
                if not isinstance(rationale, str) or not rationale or len(rationale) > 800:
                    errors.append(support_prefix + "_invalid_rationale")
            if support_seen != all_ids:
                errors.append(prefix + "_support_id_partition_mismatch")
        groups = row.get("equivalence_groups")
        grouped_ids = set()
        if not isinstance(groups, list):
            errors.append(prefix + "_equivalence_groups_not_array")
        else:
            seen_groups = set()
            for group_index, group in enumerate(groups):
                group_prefix = "%s_equivalence_group_%d" % (prefix, group_index)
                if not isinstance(group, dict) or set(group) != {"witness_ids", "rationale"}:
                    errors.append(group_prefix + "_invalid_shape")
                    continue
                ids = group.get("witness_ids")
                rationale = group.get("rationale")
                ids_are_strings = isinstance(ids, list) and all(
                    isinstance(item, str) for item in ids
                )
                if (
                    not ids_are_strings
                    or not ids
                    or len(ids) != len(set(ids))
                    or any(item not in all_ids for item in ids)
                    or set(ids) & grouped_ids
                ):
                    errors.append(group_prefix + "_invalid_witness_partition")
                    continue
                canonical_group = tuple(sorted(ids))
                if canonical_group in seen_groups:
                    errors.append(group_prefix + "_duplicate_group")
                seen_groups.add(canonical_group)
                grouped_ids.update(ids)
                if not isinstance(rationale, str) or not rationale or len(rationale) > 800:
                    errors.append(group_prefix + "_invalid_rationale")
            if grouped_ids != all_ids:
                errors.append(prefix + "_equivalence_id_partition_mismatch")
        used_left = set()
        used_right = set()
        alignments = row.get("alignments")
        if not isinstance(alignments, list):
            errors.append(prefix + "_alignments_not_array")
        else:
            for alignment_index, alignment in enumerate(alignments):
                alignment_prefix = "%s_alignment_%d" % (prefix, alignment_index)
                if not isinstance(alignment, dict) or set(alignment) != {
                    "left_witness_id",
                    "right_witness_id",
                    "relation",
                    "mismatch_fields",
                    "rationale",
                }:
                    errors.append(alignment_prefix + "_invalid_shape")
                    continue
                left_id = alignment.get("left_witness_id")
                right_id = alignment.get("right_witness_id")
                relation = alignment.get("relation")
                fields = alignment.get("mismatch_fields")
                rationale = alignment.get("rationale")
                if (
                    not isinstance(left_id, str)
                    or left_id not in left_ids
                    or left_id in used_left
                ):
                    errors.append(alignment_prefix + "_invalid_or_duplicate_left")
                else:
                    used_left.add(left_id)
                if (
                    not isinstance(right_id, str)
                    or right_id not in right_ids
                    or right_id in used_right
                ):
                    errors.append(alignment_prefix + "_invalid_or_duplicate_right")
                else:
                    used_right.add(right_id)
                if relation not in ALIGNMENT_RELATIONS:
                    errors.append(alignment_prefix + "_invalid_relation")
                fields_are_strings = isinstance(fields, list) and all(
                    isinstance(field, str) for field in fields
                )
                if (
                    not fields_are_strings
                    or len(fields) != len(set(fields))
                    or any(field not in MISMATCH_FIELDS for field in fields)
                    or (relation in ("equivalent", "abstain") and fields)
                ):
                    errors.append(alignment_prefix + "_invalid_mismatch_fields")
                if not isinstance(rationale, str) or not rationale or len(rationale) > 800:
                    errors.append(alignment_prefix + "_invalid_rationale")
        for side, valid, used in (
            ("left", left_ids, used_left),
            ("right", right_ids, used_right),
        ):
            unaligned = row.get("unaligned_%s_witness_ids" % side)
            unaligned_are_strings = isinstance(unaligned, list) and all(
                isinstance(item, str) for item in unaligned
            )
            if (
                not unaligned_are_strings
                or len(unaligned) != len(set(unaligned))
                or any(item not in valid for item in unaligned)
                or set(unaligned) & used
                or set(unaligned) | used != valid
            ):
                errors.append(prefix + "_%s_id_partition_mismatch" % side)
    if seen_cases != set(expected):
        errors.append("case_id_set_mismatch")
    return errors


def _normalized_case_output(
    output: Mapping[str, Any],
    *,
    orientation: str,
) -> Dict[str, Dict[str, Any]]:
    normalized = {}
    for row in output.get("cases") or []:
        support = {item["witness_id"]: item for item in row["support_results"]}
        equivalence_partition = frozenset(
            frozenset(group["witness_ids"]) for group in row["equivalence_groups"]
        )
        pairs = {}
        for item in row["alignments"]:
            if orientation == "ab":
                key = (item["left_witness_id"], item["right_witness_id"])
            else:
                key = (item["right_witness_id"], item["left_witness_id"])
            pairs[key] = item
        if orientation == "ab":
            unaligned_a = set(row["unaligned_left_witness_ids"])
            unaligned_b = set(row["unaligned_right_witness_ids"])
        else:
            unaligned_a = set(row["unaligned_right_witness_ids"])
            unaligned_b = set(row["unaligned_left_witness_ids"])
        normalized[row["case_id"]] = {
            "support": support,
            "equivalence_partition": equivalence_partition,
            "pairs": pairs,
            "unaligned_a": unaligned_a,
            "unaligned_b": unaligned_b,
        }
    return normalized


def combine_judge_consensus(
    pool: Mapping[str, Any],
    outputs: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    variants = build_judge_variants(pool)
    if set(outputs) != {"ab", "ba"}:
        raise ValueError("AB and BA outputs are both required for consensus")
    for name in ("ab", "ba"):
        errors = validate_judge_output(outputs[name], variants[name])
        if errors:
            raise ValueError("invalid %s judge output: %s" % (name, "; ".join(errors)))
    normalized = {
        name: _normalized_case_output(outputs[name], orientation=name) for name in ("ab", "ba")
    }
    consensus_cases = []
    support_abstentions = 0
    alignment_abstentions = 0
    topology_abstentions = 0
    partition_abstentions = 0
    support_decisions = 0
    alignment_decisions = 0
    for case in pool["cases"]:
        case_id = case["case_id"]
        ab = normalized["ab"][case_id]
        ba = normalized["ba"][case_id]
        all_witnesses = [
            item["witness_id"]
            for side in ("a", "b")
            for item in case["event_set_%s" % side]
        ]
        support_results = []
        disagreement = False
        for witness_id in all_witnesses:
            support_decisions += 1
            left = ab["support"][witness_id]
            right = ba["support"][witness_id]
            if left["verdict"] == right["verdict"]:
                verdict = left["verdict"]
                spans = sorted(set(left["evidence_spans"]) | set(right["evidence_spans"]))
            else:
                verdict = "abstain"
                spans = []
                support_abstentions += 1
                disagreement = True
            support_results.append(
                {"witness_id": witness_id, "verdict": verdict, "evidence_spans": spans}
            )
            if verdict == "abstain" and left["verdict"] == right["verdict"]:
                support_abstentions += 1
        partition_agrees = ab["equivalence_partition"] == ba["equivalence_partition"]
        if partition_agrees:
            equivalence_groups = [
                sorted(group)
                for group in sorted(
                    ab["equivalence_partition"], key=lambda item: tuple(sorted(item))
                )
            ]
            partition_abstained = []
        else:
            equivalence_groups = []
            partition_abstained = sorted(all_witnesses)
            partition_abstentions += 1
            disagreement = True
        topology_agrees = (
            set(ab["pairs"]) == set(ba["pairs"])
            and ab["unaligned_a"] == ba["unaligned_a"]
            and ab["unaligned_b"] == ba["unaligned_b"]
        )
        alignment_results = []
        if topology_agrees:
            for left_id, right_id in sorted(ab["pairs"]):
                alignment_decisions += 1
                left = ab["pairs"][(left_id, right_id)]
                right = ba["pairs"][(left_id, right_id)]
                left_fields = sorted(left["mismatch_fields"])
                right_fields = sorted(right["mismatch_fields"])
                if left["relation"] == right["relation"] and left_fields == right_fields:
                    relation = left["relation"]
                    fields = left_fields
                else:
                    relation = "abstain"
                    fields = []
                    alignment_abstentions += 1
                    disagreement = True
                alignment_results.append(
                    {
                        "left_witness_id": left_id,
                        "right_witness_id": right_id,
                        "relation": relation,
                        "mismatch_fields": fields,
                    }
                )
                if relation == "abstain" and left["relation"] == right["relation"]:
                    alignment_abstentions += 1
            unaligned_a = sorted(ab["unaligned_a"])
            unaligned_b = sorted(ab["unaligned_b"])
            alignment_abstained = []
        else:
            alignment_results = []
            unaligned_a = []
            unaligned_b = []
            alignment_abstained = sorted(all_witnesses)
            topology_abstentions += 1
            disagreement = True
        consensus_cases.append(
            {
                "case_id": case_id,
                "status": "abstain"
                if not topology_agrees or not partition_agrees
                else "partial_abstain"
                if disagreement
                else "agreed",
                "support_results": support_results,
                "equivalence_groups": equivalence_groups,
                "partition_abstained_witness_ids": partition_abstained,
                "alignment_results": alignment_results,
                "unaligned_a_witness_ids": unaligned_a,
                "unaligned_b_witness_ids": unaligned_b,
                "alignment_abstained_witness_ids": alignment_abstained,
            }
        )
    support_rate = _ratio(support_abstentions, support_decisions)
    alignment_rate = _ratio(alignment_abstentions, alignment_decisions)
    topology_rate = _ratio(topology_abstentions, len(pool["cases"]))
    partition_rate = _ratio(partition_abstentions, len(pool["cases"]))
    return {
        "schema_version": JUDGE_CONSENSUS_VERSION,
        "pool_sha256": sha256_text(_canonical_json(pool)),
        "cases": consensus_cases,
        "abstentions": {
            "support": {
                "numerator": support_abstentions,
                "denominator": support_decisions,
                "rate": support_rate,
            },
            "alignment_labels": {
                "numerator": alignment_abstentions,
                "denominator": alignment_decisions,
                "rate": alignment_rate,
            },
            "alignment_topology": {
                "numerator": topology_abstentions,
                "denominator": len(pool["cases"]),
                "rate": topology_rate,
            },
            "equivalence_partition": {
                "numerator": partition_abstentions,
                "denominator": len(pool["cases"]),
                "rate": partition_rate,
            },
        },
        # Abstention is an allowed judge result.  A separately frozen downstream
        # gate decides whether these measured rates are acceptable.
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


def _unit_key(case_id: str, event: Mapping[str, Any]) -> Tuple[str, str]:
    return case_id, sha256_text(_canonical_json(event))


def _f1(precision: float, recall: float) -> float:
    if not precision + recall:
        return 0.0
    return round(2 * precision * recall / (precision + recall), 6)


def score_named_systems_against_shared_reference(
    *,
    pool: Mapping[str, Any],
    private_mapping: Mapping[str, Any],
    consensus: Mapping[str, Any],
    reference_system_ids: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Score named systems once against one shared, support-first reference.

    System membership is read only from the private mapping and never from a
    prompt.  Reference units are the agreed all-witness full-equivalence groups
    that contain a supported exact-evidence witness from a selected reference
    system.  One-to-one ``partial`` edges are diagnostic only.

    A witness mapping may carry ``provenance.system_id``.  As a convenience for
    packet builders, a case mapping may instead carry ``case_provenance`` keys
    ``system_a_id`` and ``system_b_id``; witness-level membership wins.
    """

    pool_errors = validate_shared_witness_pool(pool)
    if pool_errors:
        raise ValueError("invalid shared witness pool: %s" % "; ".join(pool_errors))
    if (
        not isinstance(private_mapping, Mapping)
        or private_mapping.get("schema_version") != WITNESS_MAPPING_VERSION
        or not isinstance(private_mapping.get("cases"), list)
    ):
        raise ValueError("invalid private witness mapping")
    if (
        not isinstance(consensus, Mapping)
        or consensus.get("schema_version") != JUDGE_CONSENSUS_VERSION
        or not isinstance(consensus.get("cases"), list)
    ):
        raise ValueError("invalid semantic judge consensus")

    pool_cases = {case["case_id"]: case for case in pool["cases"]}
    mapped_cases = {}
    witness_system = {}  # type: Dict[str, str]
    for mapped in private_mapping["cases"]:
        if not isinstance(mapped, Mapping):
            raise ValueError("invalid private witness case mapping")
        case_id = mapped.get("case_id")
        if not isinstance(case_id, str) or case_id in mapped_cases or case_id not in pool_cases:
            raise ValueError("private mapping case IDs do not partition the pool")
        mapped_cases[case_id] = mapped
        case_provenance = mapped.get("case_provenance") or {}
        if not isinstance(case_provenance, Mapping):
            raise ValueError("case provenance must be an object")
        witnesses = mapped.get("witnesses")
        if not isinstance(witnesses, list):
            raise ValueError("private mapping witnesses must be an array")
        for witness in witnesses:
            if not isinstance(witness, Mapping):
                raise ValueError("invalid private witness mapping row")
            witness_id = witness.get("witness_id")
            side = witness.get("canonical_side")
            provenance = witness.get("provenance") or {}
            if not isinstance(provenance, Mapping):
                raise ValueError("witness provenance must be an object")
            system_id = provenance.get("system_id")
            if not isinstance(system_id, str) or not system_id:
                system_id = case_provenance.get("system_%s_id" % side)
            if not isinstance(system_id, str) or not system_id:
                raise ValueError("every scored witness requires a private system_id membership")
            if not isinstance(witness_id, str) or witness_id in witness_system:
                raise ValueError("private witness IDs must be unique")
            witness_system[witness_id] = system_id
    if set(mapped_cases) != set(pool_cases):
        raise ValueError("private mapping case IDs do not partition the pool")

    witness_event = {}  # type: Dict[str, Mapping[str, Any]]
    witness_case = {}  # type: Dict[str, str]
    all_pool_witnesses = set()
    for case_id, case in pool_cases.items():
        for side in ("a", "b"):
            for witness in case["event_set_%s" % side]:
                witness_id = witness["witness_id"]
                all_pool_witnesses.add(witness_id)
                witness_event[witness_id] = witness["event"]
                witness_case[witness_id] = case_id
    if set(witness_system) != all_pool_witnesses:
        raise ValueError("private witness membership does not exactly partition the pool")

    consensus_cases = {}  # type: Dict[str, Mapping[str, Any]]
    support_verdict = {}  # type: Dict[str, str]
    alignment_edges = []  # type: List[Tuple[str, str, str]]
    witness_group = {}  # type: Dict[str, Tuple[str, str]]
    for row in consensus["cases"]:
        if not isinstance(row, Mapping):
            raise ValueError("invalid consensus case")
        case_id = row.get("case_id")
        if not isinstance(case_id, str) or case_id in consensus_cases or case_id not in pool_cases:
            raise ValueError("consensus case IDs do not exactly partition the pool")
        consensus_cases[case_id] = row
        support_rows = row.get("support_results")
        if not isinstance(support_rows, list):
            raise ValueError("consensus support results must be an array")
        for support in support_rows:
            if not isinstance(support, Mapping):
                raise ValueError("invalid consensus support result")
            witness_id = support.get("witness_id")
            verdict = support.get("verdict")
            if (
                not isinstance(witness_id, str)
                or witness_id in support_verdict
                or witness_id not in all_pool_witnesses
                or witness_case[witness_id] != case_id
                or verdict not in SUPPORT_VERDICTS
            ):
                raise ValueError("consensus support IDs do not exactly partition the pool")
            support_verdict[witness_id] = str(verdict)
        groups = row.get("equivalence_groups")
        partition_abstained = row.get("partition_abstained_witness_ids") or []
        if not isinstance(groups, list) or not isinstance(partition_abstained, list):
            raise ValueError("consensus equivalence partition is missing")
        case_witnesses = {
            witness_id for witness_id in all_pool_witnesses if witness_case[witness_id] == case_id
        }
        if partition_abstained:
            raise ValueError("cannot score an abstained semantic equivalence partition")
        grouped = set()
        for group in groups:
            if (
                not isinstance(group, list)
                or not group
                or len(group) != len(set(group))
                or set(group) & grouped
                or not set(group).issubset(case_witnesses)
            ):
                raise ValueError("consensus equivalence groups are malformed")
            grouped.update(group)
            group_id = (case_id, sha256_text("|".join(sorted(group))))
            for witness_id in group:
                witness_group[witness_id] = group_id
        if grouped != case_witnesses:
            raise ValueError("consensus equivalence groups do not partition the case")
        alignments = row.get("alignment_results")
        if not isinstance(alignments, list):
            raise ValueError("consensus alignment results must be an array")
        for alignment in alignments:
            if not isinstance(alignment, Mapping):
                raise ValueError("invalid consensus alignment")
            left_id = alignment.get("left_witness_id")
            right_id = alignment.get("right_witness_id")
            relation = alignment.get("relation")
            if (
                not isinstance(left_id, str)
                or not isinstance(right_id, str)
                or left_id not in all_pool_witnesses
                or right_id not in all_pool_witnesses
                or witness_case[left_id] != case_id
                or witness_case[right_id] != case_id
                or relation not in ALIGNMENT_RELATIONS
            ):
                raise ValueError("invalid consensus alignment membership")
            alignment_edges.append((left_id, right_id, str(relation)))
    if set(consensus_cases) != set(pool_cases) or set(support_verdict) != all_pool_witnesses:
        raise ValueError("consensus IDs do not exactly partition the pool")

    systems = sorted(set(witness_system.values()))
    if reference_system_ids is not None and any(
        not isinstance(item, str) or not item for item in reference_system_ids
    ):
        raise ValueError("reference_system_ids must be non-empty strings")
    selected_reference_systems = (
        sorted(set(reference_system_ids)) if reference_system_ids is not None else systems
    )
    if not selected_reference_systems or any(item not in systems for item in selected_reference_systems):
        raise ValueError("reference_system_ids must name systems present in the pool")

    reference_witnesses = {
        witness_id
        for witness_id in all_pool_witnesses
        if witness_system[witness_id] in selected_reference_systems
        and support_verdict[witness_id] == "supported"
        and witness_event[witness_id].get("submitted_evidence_exact") is True
    }
    reference_units = {witness_group[witness_id] for witness_id in reference_witnesses}
    if not reference_units:
        raise ValueError("shared reference has no consensus-supported units")
    overlap_neighbors = {}  # type: Dict[str, Set[str]]
    for left_id, right_id, relation in alignment_edges:
        if relation == "partial":
            overlap_neighbors.setdefault(left_id, set()).add(right_id)
            overlap_neighbors.setdefault(right_id, set()).add(left_id)

    def covered_reference_units(system_id: str, *, overlap: bool) -> Set[Tuple[str, str]]:
        supported_system_witnesses = {
            witness_id
            for witness_id in all_pool_witnesses
            if witness_system[witness_id] == system_id
            and support_verdict[witness_id] == "supported"
            and witness_event[witness_id].get("submitted_evidence_exact") is True
        }
        covered = {witness_group[item] for item in supported_system_witnesses} & reference_units
        if overlap:
            for witness_id in supported_system_witnesses:
                for neighbor in overlap_neighbors.get(witness_id, set()):
                    if neighbor in reference_witnesses:
                        covered.add(witness_group[neighbor])
        return covered

    system_scores = {}
    for system_id in systems:
        submitted_witnesses = {
            item for item in all_pool_witnesses if witness_system[item] == system_id
        }
        submitted_units = {witness_group[item] for item in submitted_witnesses}
        supported_units = {
            witness_group[item]
            for item in submitted_witnesses
            if support_verdict[item] == "supported"
            and witness_event[item].get("submitted_evidence_exact") is True
        }
        abstained_units = {
            witness_group[item]
            for item in submitted_witnesses
            if support_verdict[item] == "abstain"
        }
        precision = _ratio(len(supported_units), len(submitted_units))
        strict_covered = covered_reference_units(system_id, overlap=False)
        overlap_covered = covered_reference_units(system_id, overlap=True)
        strict_recall = _ratio(len(strict_covered), len(reference_units))
        overlap_recall = _ratio(len(overlap_covered), len(reference_units))
        system_scores[system_id] = {
            "submitted_unit_count": len(submitted_units),
            "supported_unit_count": len(supported_units),
            "support_abstained_unit_count": len(abstained_units),
            "support_precision_lower_bound": precision,
            "strict_reference_units_covered": len(strict_covered),
            "strict_recall": strict_recall,
            "strict_f1": _f1(precision, strict_recall),
            "overlap_reference_units_covered": len(overlap_covered),
            "overlap_recall": overlap_recall,
            "overlap_f1": _f1(precision, overlap_recall),
        }
    return {
        "schema_version": SHARED_REFERENCE_SCORE_VERSION,
        "pool_sha256": sha256_text(_canonical_json(pool)),
        "consensus_sha256": sha256_text(_canonical_json(consensus)),
        "reference_system_ids": selected_reference_systems,
        "reference_unit_count": len(reference_units),
        "deduplication": "llm_consensus_full_semantic_equivalence_groups_within_case",
        "strict_coverage": "own_supported_exact_member_in_consensus_equivalence_group",
        "overlap_coverage": "strict_coverage_plus_one_to_one_partial_diagnostics",
        "abstention_policy": "support_abstain_is_worst_case_not_supported",
        "systems": system_scores,
    }


def _read_sidecar(path: Path) -> Dict[str, Any]:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        raise JudgeArtifactError("judge sidecar is not an object: %s" % path)
    return payload


def _validate_completed_checkpoint(
    *,
    raw_output_path: Path,
    sidecar_path: Path,
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    reasoning_effort: str,
    instruction_contract: Mapping[str, Any] | None = None,
    execution_lineage: Mapping[str, Any] | None = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if raw_output_path.exists() != sidecar_path.exists():
        raise JudgeArtifactError("judge checkpoint has output/sidecar mismatch")
    output = _load_json(raw_output_path)
    sidecar = _read_sidecar(sidecar_path)
    required = {
        "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
        "state": "completed",
        "status": "completed",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "model": model,
        "effort": reasoning_effort,
        "prompt_sha256": sha256_text(prompt),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_instructions_sha256": sha256_text(base_instructions),
        "base_instructions_bytes": len(base_instructions.encode("utf-8")),
        "output_schema_sha256": sha256_text(_canonical_json(schema)),
        "output_schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise JudgeArtifactError("judge checkpoint sidecar mismatch: %s" % key)
    for key in ("thread_id", "turn_id", "app_server_user_agent"):
        if not isinstance(sidecar.get(key), str) or not sidecar.get(key):
            raise JudgeArtifactError("judge checkpoint lacks exact %s" % key)
    if Path(str(sidecar.get("output_path") or "")).expanduser().resolve() != raw_output_path.resolve():
        raise JudgeArtifactError("judge checkpoint output path mismatch")
    if (instruction_contract is None) != (execution_lineage is None):
        raise JudgeArtifactError("judge checkpoint lineage inputs are incomplete")
    if instruction_contract is not None and execution_lineage is not None:
        try:
            validate_managed_sidecar_execution_lineage(
                sidecar=sidecar,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
        except ValueError as exc:
            raise JudgeArtifactError(str(exc)) from exc
    output_text = raw_output_path.read_text(encoding="utf-8")
    acceptable_output_hashes = {sha256_text(output_text)}
    if output_text.endswith("\n"):
        # CodexAppServerClient adds one storage newline when the model message
        # did not already contain one; its sidecar hashes the original message.
        acceptable_output_hashes.add(sha256_text(output_text[:-1]))
    if sidecar.get("output_sha256") not in acceptable_output_hashes:
        raise JudgeArtifactError("judge checkpoint output hash mismatch")
    return output, sidecar


def _aggregate_accounting(sidecars: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    complete = len(sidecars) == 2 and all(
        sidecar.get("usage_complete") is True and isinstance(sidecar.get("usage"), dict)
        for sidecar in sidecars
    )
    if not complete:
        return {"accounting_complete": False, "usage": None, "usage_status": "unknown"}
    usage = {field: 0 for field in fields}
    for sidecar in sidecars:
        per_turn = sidecar["usage"]
        for field in fields:
            value = per_turn.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return {"accounting_complete": False, "usage": None, "usage_status": "unknown"}
            usage[field] += value
        if (
            per_turn["cached_input_tokens"] > per_turn["input_tokens"]
            or per_turn["reasoning_output_tokens"] > per_turn["output_tokens"]
            or per_turn["total_tokens"]
            != per_turn["input_tokens"] + per_turn["output_tokens"]
        ):
            return {"accounting_complete": False, "usage": None, "usage_status": "unknown"}
    return {"accounting_complete": True, "usage": usage, "usage_status": "complete"}


async def run_app_server_semantic_judge(
    *,
    pool_path: Path,
    output_dir: Path,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    instruction_contract: Mapping[str, Any] | None = None,
    execution_lineage: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Run independent AB/BA app-server judges with resumable checkpoints."""

    if (instruction_contract is None) != (execution_lineage is None):
        raise ValueError("judge instruction contract and execution lineage are inseparable")
    pool_file = Path(pool_path).expanduser().resolve()
    pool = _load_json(pool_file)
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise ValueError("invalid shared witness pool: %s" % "; ".join(errors))
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    variants = build_judge_variants(pool)
    base_instructions = judge_base_instructions()
    base_instructions_path = root / "base-instructions.private.md"
    _write_immutable_text(base_instructions_path, base_instructions)
    prepared = {}
    for name in ("ab", "ba"):
        prompt = build_judge_prompt(variants[name])
        schema = semantic_judge_output_schema(variants[name])
        paths = {
            "prompt": root / ("prompt-%s.private.md" % name),
            "schema": root / ("schema-%s.json" % name),
            "output": root / ("output-%s.private.json" % name),
            "sidecar": root / "sidecars" / ("%s.json" % name),
            "capacity": root / "sidecars" / ("%s.capacity.json" % name),
        }
        _write_immutable_text(paths["prompt"], prompt)
        write_immutable_json(paths["schema"], schema)
        prepared[name] = {"prompt": prompt, "schema": schema, "paths": paths}
    spec = {
        "schema_version": JUDGE_RUN_VERSION,
        "state": "frozen_before_model_calls",
        "pool_path": str(pool_file),
        "pool_file_sha256": _sha256_file(pool_file),
        "pool_content_sha256": sha256_text(_canonical_json(pool)),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "app_server_client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
        "capacity_gate": "managed_chatgpt_live_primary_lte_20_before_each_turn",
        "retry_count": 0,
        "base_instructions_sha256": sha256_text(base_instructions),
        "base_instructions_path": str(base_instructions_path),
        "variant_prompt_sha256": {
            name: sha256_text(prepared[name]["prompt"]) for name in ("ab", "ba")
        },
        "variant_schema_sha256": {
            name: sha256_text(_canonical_json(prepared[name]["schema"]))
            for name in ("ab", "ba")
        },
        "case_ids": [case["case_id"] for case in pool["cases"]],
        "privacy": "private_analysis_only_prompts_and_outputs_are_private",
    }
    if instruction_contract is not None and execution_lineage is not None:
        spec["instruction_contract"] = dict(instruction_contract)
        spec["execution_lineage"] = dict(execution_lineage)
        spec["turn_semantic_outputs"] = ["support", "alignment"]
        spec["deterministic_consensus_additional_model_calls"] = 0
    write_immutable_json(root / "judge-spec.json", spec)

    terminal_report_path = root / "report.json"
    if terminal_report_path.exists():
        prior_report = _load_json(terminal_report_path)
        if (
            not isinstance(prior_report, dict)
            or prior_report.get("schema_version") != JUDGE_RUN_VERSION
            or prior_report.get("state") != "completed"
            or prior_report.get("spec_sha256") != _sha256_file(root / "judge-spec.json")
            or prior_report.get("instruction_contract")
            != (dict(instruction_contract) if instruction_contract is not None else None)
            or prior_report.get("execution_lineage")
            != (dict(execution_lineage) if execution_lineage is not None else None)
        ):
            raise JudgeArtifactError("terminal judge report does not match the frozen specification")
        consensus_path = root / "consensus.private.json"
        if (
            not consensus_path.exists()
            or prior_report.get("consensus_sha256") != _sha256_file(consensus_path)
        ):
            raise JudgeArtifactError("terminal judge consensus checkpoint is missing or changed")
        if instruction_contract is not None and execution_lineage is not None:
            bindings = prior_report.get("leaf_bindings")
            if not isinstance(bindings, list) or len(bindings) != 2:
                raise JudgeArtifactError("terminal judge leaf bindings are incomplete")
            try:
                validated_bindings = [
                    validate_holdout_leaf_binding(
                        binding,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                        expected_model=model,
                        expected_effort=reasoning_effort,
                    )
                    for binding in bindings
                ]
            except ValueError as exc:
                raise JudgeArtifactError(str(exc)) from exc
            if len({item["turn_id"] for item in validated_bindings}) != 2:
                raise JudgeArtifactError("terminal judge leaf turn identity is duplicated")
        for name in ("ab", "ba"):
            paths = prepared[name]["paths"]
            output, _sidecar = _validate_completed_checkpoint(
                raw_output_path=paths["output"],
                sidecar_path=paths["sidecar"],
                prompt=prepared[name]["prompt"],
                schema=prepared[name]["schema"],
                base_instructions=base_instructions,
                model=model,
                reasoning_effort=reasoning_effort,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            output_errors = validate_judge_output(output, variants[name])
            if output_errors:
                raise JudgeArtifactError("terminal judge output is no longer valid")
        return prior_report

    outputs = {}  # type: Dict[str, Dict[str, Any]]
    sidecars = {}  # type: Dict[str, Dict[str, Any]]
    missing = []
    for name in ("ab", "ba"):
        paths = prepared[name]["paths"]
        if paths["output"].exists() or paths["sidecar"].exists():
            output, sidecar = _validate_completed_checkpoint(
                raw_output_path=paths["output"],
                sidecar_path=paths["sidecar"],
                prompt=prepared[name]["prompt"],
                schema=prepared[name]["schema"],
                base_instructions=base_instructions,
                model=model,
                reasoning_effort=reasoning_effort,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            outputs[name] = output
            sidecars[name] = sidecar
        else:
            missing.append(name)

    if missing:
        async with client_factory() as client:
            async def execute(name: str) -> Tuple[str, Any]:
                item = prepared[name]
                result = await client.run_ephemeral_structured_turn(
                    model=model,
                    effort=reasoning_effort,
                    base_instructions=base_instructions,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    cwd=Path.cwd(),
                    sidecar_path=item["paths"]["sidecar"],
                    capacity_checkpoint_path=item["paths"]["capacity"],
                    output_path=item["paths"]["output"],
                    batch_size=len(pool["cases"]),
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                )
                if not result.status_ok or not isinstance(result.output, dict):
                    raise JudgeAttemptFailed(
                        variant=name,
                        error_class=str(result.error_class or result.status or "turn_failed"),
                        sidecar_path=item["paths"]["sidecar"],
                    )
                return name, result.output

            # Execute in frozen AB then BA order. A failed or unknown-usage AB
            # blocks BA and the whole version; no concurrent sibling can start
            # after a terminal failure races with gather cancellation.
            for name in missing:
                executed_name, output = await execute(name)
                if executed_name != name:
                    raise AssertionError("judge variant execution order changed")
                outputs[name] = output

        for name in missing:
            paths = prepared[name]["paths"]
            checkpoint_output, sidecar = _validate_completed_checkpoint(
                raw_output_path=paths["output"],
                sidecar_path=paths["sidecar"],
                prompt=prepared[name]["prompt"],
                schema=prepared[name]["schema"],
                base_instructions=base_instructions,
                model=model,
                reasoning_effort=reasoning_effort,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            if _canonical_json(checkpoint_output) != _canonical_json(outputs[name]):
                raise JudgeArtifactError("returned and checkpointed judge outputs differ")
            sidecars[name] = sidecar

    validation = {}
    for name in ("ab", "ba"):
        validation[name] = validate_judge_output(outputs[name], variants[name])
    if any(validation.values()):
        write_immutable_json(
            root / "validation-errors.json",
            {"schema_version": JUDGE_RUN_VERSION, "validation_errors": validation},
        )
        raise JudgeArtifactError("semantic judge output failed exact validation")

    consensus = combine_judge_consensus(pool, outputs)
    write_immutable_json(root / "consensus.private.json", consensus)
    accounting = _aggregate_accounting([sidecars[name] for name in ("ab", "ba")])
    leaf_bindings = None
    if instruction_contract is not None and execution_lineage is not None:
        leaf_bindings = [
            build_holdout_leaf_binding(
                sidecar_path=prepared[name]["paths"]["sidecar"],
                prompt_path=prepared[name]["paths"]["prompt"],
                output_schema_path=prepared[name]["paths"]["schema"],
                base_instructions_path=base_instructions_path,
                raw_output_path=prepared[name]["paths"]["output"],
                model=model,
                effort=reasoning_effort,
                thread_mode="new_thread",
                batch_size=len(pool["cases"]),
                prompt=prepared[name]["prompt"],
                output_schema=prepared[name]["schema"],
                base_instructions=base_instructions,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            for name in ("ab", "ba")
        ]
    report = {
        "schema_version": JUDGE_RUN_VERSION,
        "state": "completed",
        "completed_at": max(str(sidecars[name].get("finished_at") or "") for name in ("ab", "ba")),
        "spec_sha256": _sha256_file(root / "judge-spec.json"),
        "pool_sha256": spec["pool_file_sha256"],
        "model": model,
        "reasoning_effort": reasoning_effort,
        "transport": spec["transport"],
        "variant_count": 2,
        "case_count": len(pool["cases"]),
        "witness_count": sum(
            len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
        ),
        "validation_errors": validation,
        "consensus_sha256": _sha256_file(root / "consensus.private.json"),
        "abstentions": consensus["abstentions"],
        "abstention_gate_applied": False,
        "selection_admissible": bool(
            consensus["selection_admissible"] and accounting["accounting_complete"]
        ),
        "sidecar_paths": [str(prepared[name]["paths"]["sidecar"]) for name in ("ab", "ba")],
        "leaf_bindings": leaf_bindings,
        "instruction_contract": (
            dict(instruction_contract) if instruction_contract is not None else None
        ),
        "execution_lineage": (
            dict(execution_lineage) if execution_lineage is not None else None
        ),
        "turn_semantic_outputs": ["support", "alignment"],
        "deterministic_consensus_additional_model_calls": 0,
        **accounting,
    }
    write_immutable_json(terminal_report_path, report)
    return report


def make_v2_calibration_pool(
    *,
    fixture_path: Optional[Path] = None,
    seed: str = "app-server-judge-calibration-v2",
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Adapt the existing >=60-case v2 fixture to the shared witness interface."""

    from .windowed_evaluation import (
        DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH,
        load_expanded_judge_calibration_fixture,
    )

    selected = Path(fixture_path or DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH).expanduser().resolve()
    fixture = load_expanded_judge_calibration_fixture(selected)
    raw_cases = []
    for case in fixture["expanded_cases"]:
        raw_cases.append(
            {
                "case_key": case["case_id"],
                "source_excerpt": case["source_excerpt"],
                "event_set_a": [
                    {"event": event, "provenance": {"canonical_side": "a", "index": index}}
                    for index, event in enumerate(case["set_a"])
                ],
                "event_set_b": [
                    {"event": event, "provenance": {"canonical_side": "b", "index": index}}
                    for index, event in enumerate(case["set_b"])
                ],
                "provenance": {"fixture_case_id": case["case_id"], "shape": case["shape"]},
            }
        )
    same_side_cases = []
    for index, (speaker, product, benefit) in enumerate(
        (
            ("Maya", "assistant", "saves two hours each week"),
            ("Noah", "compiler", "cuts build time in half"),
            ("Priya", "monitor", "detects failed invoices"),
            ("Omar", "database", "handles one million rows"),
            ("Elena", "router", "reduces request latency"),
            ("Kai", "agent", "finds duplicate records"),
        )
    ):
        evidence = "%s says the %s %s." % (speaker, product, benefit)
        case_key = "synthetic_same_side_equivalence_%02d" % index
        left = {
            "event_type": "capability_claim",
            "claim_text": "%s says the %s %s." % (speaker, product, benefit),
            "speaker_name": speaker,
            "actor_name": product,
            "target_concept": benefit.replace(" ", "_"),
            "stance": "supportive",
            "certainty": "high",
            "temporal_horizon": "present",
            "evidence": evidence,
        }
        paraphrase = {
            **left,
            "claim_text": "According to %s, the %s %s." % (speaker, product, benefit),
        }
        raw_cases.append(
            {
                "case_key": case_key,
                "source_excerpt": evidence,
                "event_set_a": [
                    {"event": left, "provenance": {"synthetic_index": 0}},
                    {"event": paraphrase, "provenance": {"synthetic_index": 1}},
                ],
                "event_set_b": [],
                "provenance": {
                    "fixture_case_id": case_key,
                    "shape": "same_side_equivalence_partition",
                    "synthetic_same_side": True,
                },
            }
        )
        same_side_cases.append({"case_key": case_key})
    pool, mapping = make_shared_witness_pool(raw_cases, seed=seed)
    pool_by_key = {
        mapped["case_key"]: (pool["cases"][mapped["case_index"]], mapped)
        for mapped in mapping["cases"]
    }
    expected_cases = {}
    for case in fixture["expanded_cases"]:
        pooled, mapped = pool_by_key[case["case_id"]]
        witness_by_position = {
            (item["canonical_side"], item["canonical_index"]): item["witness_id"]
            for item in mapped["witnesses"]
        }
        support = {}
        for side in ("a", "b"):
            for index, supported in enumerate(case["support_%s" % side]):
                support[witness_by_position[(side, index)]] = (
                    "supported" if supported else "unsupported"
                )
        pairs = {}
        for pair in case["expected_pairs"]:
            pair_key = (
                witness_by_position[("a", pair["a_id"])],
                witness_by_position[("b", pair["b_id"])],
            )
            pairs[pair_key] = {
                "relation": pair["relation"],
                "mismatch_fields": sorted(pair["mismatch_fields"]),
            }
        parent = {witness_id: witness_id for witness_id in support}

        def find(witness_id: str) -> str:
            while parent[witness_id] != witness_id:
                parent[witness_id] = parent[parent[witness_id]]
                witness_id = parent[witness_id]
            return witness_id

        for (left_id, right_id), value in pairs.items():
            if value["relation"] == "equivalent":
                left_root = find(left_id)
                right_root = find(right_id)
                parent[right_root] = left_root
        grouped = {}
        for witness_id in support:
            grouped.setdefault(find(witness_id), []).append(witness_id)
        expected_cases[pooled["case_id"]] = {
            "base_case_id": case["case_id"],
            "shape": case["shape"],
            "support": support,
            "equivalence_groups": sorted(
                (sorted(group) for group in grouped.values()), key=lambda group: tuple(group)
            ),
            "same_side_equivalence_required": False,
            "pairs": [
                {
                    "left_witness_id": key[0],
                    "right_witness_id": key[1],
                    **value,
                }
                for key, value in sorted(pairs.items())
            ],
        }
    for synthetic in same_side_cases:
        pooled, mapped = pool_by_key[synthetic["case_key"]]
        witness_ids = [item["witness_id"] for item in mapped["witnesses"]]
        expected_cases[pooled["case_id"]] = {
            "base_case_id": synthetic["case_key"],
            "shape": "same_side_equivalence_partition",
            "support": {witness_id: "supported" for witness_id in witness_ids},
            "equivalence_groups": [sorted(witness_ids)],
            "same_side_equivalence_required": True,
            "pairs": [],
        }
    expected = {
        "schema_version": JUDGE_CALIBRATION_VERSION,
        "fixture_path": str(selected),
        "fixture_sha256": _sha256_file(selected),
        "mismatch_fields": list(fixture["mismatch_fields"]),
        "synthetic_same_side_case_count": len(same_side_cases),
        "cases": expected_cases,
    }
    return pool, mapping, expected


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def score_calibration_variant(
    output: Mapping[str, Any],
    *,
    variant: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> Dict[str, Any]:
    errors = validate_judge_output(output, variant)
    if errors:
        raise ValueError("cannot score invalid calibration output: %s" % "; ".join(errors))
    orientation = str(variant["orientation"])
    predicted = _normalized_case_output(output, orientation=orientation)
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    equivalent_tp = equivalent_fp = equivalent_tn = equivalent_fn = 0
    field_tp = field_fp = field_fn = 0
    support_tp = support_fp = support_tn = support_fn = 0
    partition_tp = partition_fp = partition_fn = 0
    partition_exact_cases = 0
    same_side_partition_cases = 0
    same_side_partition_exact = 0
    abstentions = 0
    by_base_case = {}
    for case_id, truth in expected["cases"].items():
        row = predicted[case_id]
        expected_pairs = {
            (pair["left_witness_id"], pair["right_witness_id"]): pair
            for pair in truth["pairs"]
        }
        predicted_pairs = row["pairs"]
        expected_partition = frozenset(
            frozenset(group) for group in truth["equivalence_groups"]
        )
        predicted_partition = row["equivalence_partition"]

        def co_members(partition: Any) -> Set[Tuple[str, str]]:
            pairs = set()
            for group in partition:
                ordered = sorted(group)
                for left_index, left_id in enumerate(ordered):
                    for right_id in ordered[left_index + 1 :]:
                        pairs.add((left_id, right_id))
            return pairs

        expected_co_members = co_members(expected_partition)
        predicted_co_members = co_members(predicted_partition)
        partition_tp += len(expected_co_members & predicted_co_members)
        partition_fp += len(predicted_co_members - expected_co_members)
        partition_fn += len(expected_co_members - predicted_co_members)
        partition_exact = predicted_partition == expected_partition
        partition_exact_cases += int(partition_exact)
        if truth.get("same_side_equivalence_required") is True:
            same_side_partition_cases += 1
            same_side_partition_exact += int(partition_exact)
        expected_keys = set(expected_pairs)
        predicted_keys = set(predicted_pairs)
        alignment_tp += len(expected_keys & predicted_keys)
        alignment_fp += len(predicted_keys - expected_keys)
        alignment_fn += len(expected_keys - predicted_keys)
        normalized_pairs = {}
        for key, expected_pair in expected_pairs.items():
            prediction = predicted_pairs.get(key)
            predicted_relation = prediction.get("relation") if prediction else None
            predicted_fields = set(prediction.get("mismatch_fields") or []) if prediction else set()
            expected_relation = expected_pair["relation"]
            expected_fields = set(expected_pair["mismatch_fields"])
            relation_total += 1
            relation_correct += int(predicted_relation == expected_relation)
            if predicted_relation == "abstain" or predicted_relation is None:
                abstentions += 1
            if expected_relation == "equivalent":
                if predicted_relation == "equivalent":
                    equivalent_tp += 1
                else:
                    equivalent_fn += 1
            elif predicted_relation == "equivalent" or predicted_relation in {None, "abstain"}:
                equivalent_fp += 1
            else:
                equivalent_tn += 1
            field_tp += len(expected_fields & predicted_fields)
            field_fp += len(predicted_fields - expected_fields)
            field_fn += len(expected_fields - predicted_fields)
            normalized_pairs["%s|%s" % key] = {
                "relation": predicted_relation,
                "mismatch_fields": sorted(predicted_fields),
            }
        normalized_support = {}
        for witness_id, expected_verdict in truth["support"].items():
            verdict = row["support"][witness_id]["verdict"]
            expected_supported = expected_verdict == "supported"
            if verdict == "abstain":
                abstentions += 1
            if expected_supported and verdict == "supported":
                support_tp += 1
            elif expected_supported:
                support_fn += 1
            elif verdict == "unsupported":
                support_tn += 1
            else:
                support_fp += 1
            normalized_support[witness_id] = verdict
        by_base_case[truth["base_case_id"]] = {
            "pairs": normalized_pairs,
            "equivalence_groups": [
                sorted(group)
                for group in sorted(predicted_partition, key=lambda item: tuple(sorted(item)))
            ],
            "support": normalized_support,
            "unaligned_a": sorted(row["unaligned_a"]),
            "unaligned_b": sorted(row["unaligned_b"]),
        }
    alignment_precision = _ratio(alignment_tp, alignment_tp + alignment_fp)
    alignment_recall = _ratio(alignment_tp, alignment_tp + alignment_fn)
    alignment_f1 = (
        round(2 * alignment_precision * alignment_recall / (alignment_precision + alignment_recall), 6)
        if alignment_precision + alignment_recall
        else 0.0
    )
    field_precision = _ratio(field_tp, field_tp + field_fp)
    field_recall = _ratio(field_tp, field_tp + field_fn)
    field_f1 = (
        round(2 * field_precision * field_recall / (field_precision + field_recall), 6)
        if field_precision + field_recall
        else 1.0
        if field_tp == field_fp == field_fn == 0
        else 0.0
    )
    partition_precision = _ratio(partition_tp, partition_tp + partition_fp)
    partition_recall = _ratio(partition_tp, partition_tp + partition_fn)
    partition_f1 = (
        round(
            2
            * partition_precision
            * partition_recall
            / (partition_precision + partition_recall),
            6,
        )
        if partition_precision + partition_recall
        else 1.0
        if partition_tp == partition_fp == partition_fn == 0
        else 0.0
    )
    return {
        "case_count": len(expected["cases"]),
        "missing_case_ids": 0,
        "alignment_precision": alignment_precision,
        "alignment_recall": alignment_recall,
        "alignment_f1": alignment_f1,
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_precision": field_precision,
        "field_diagnostic_recall": field_recall,
        "field_diagnostic_f1": field_f1,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "equivalence_partition_pairwise_precision": partition_precision,
        "equivalence_partition_pairwise_recall": partition_recall,
        "equivalence_partition_pairwise_f1": partition_f1,
        "equivalence_partition_exact_case_rate": _ratio(
            partition_exact_cases, len(expected["cases"])
        ),
        "same_side_partition_case_count": same_side_partition_cases,
        "same_side_partition_exact_rate": _ratio(
            same_side_partition_exact, same_side_partition_cases
        ),
        "abstention_count": abstentions,
        "by_base_case": by_base_case,
    }


async def run_app_server_judge_calibration(
    *,
    output_dir: Path,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    fixture_path: Optional[Path] = None,
    evaluator_spec_path: Optional[Path] = None,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> Dict[str, Any]:
    from .windowed_evaluation import (
        DEFAULT_EVALUATOR_SPEC_PATH,
        combine_expanded_judge_calibration_scores,
        load_windowed_evaluator_spec,
    )

    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    pool, mapping, expected = make_v2_calibration_pool(fixture_path=fixture_path)
    pool_path = root / "shared-witness-pool.private.json"
    write_immutable_json(pool_path, pool)
    write_immutable_json(root / "private-mapping.json", mapping)
    write_immutable_json(root / "expected.private.json", expected)
    judge_root = root / "judge"
    judge_report = await run_app_server_semantic_judge(
        pool_path=pool_path,
        output_dir=judge_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    variants = build_judge_variants(pool)
    scores = {}
    for name in ("ab", "ba"):
        output = _load_json(judge_root / ("output-%s.private.json" % name))
        scores[name] = score_calibration_variant(
            output,
            variant=variants[name],
            expected=expected,
        )
    evaluator_file = Path(
        evaluator_spec_path or DEFAULT_EVALUATOR_SPEC_PATH
    ).expanduser().resolve()
    evaluator = load_windowed_evaluator_spec(evaluator_file)
    combined = combine_expanded_judge_calibration_scores(
        scores,
        gates=evaluator["judge_calibration"]["gates"],
    )
    partition_metrics = {
        "pairwise_f1": min(
            score["equivalence_partition_pairwise_f1"] for score in scores.values()
        ),
        "exact_case_rate": min(
            score["equivalence_partition_exact_case_rate"] for score in scores.values()
        ),
        "same_side_exact_rate": min(
            score["same_side_partition_exact_rate"] for score in scores.values()
        ),
        "same_side_case_count": min(
            score["same_side_partition_case_count"] for score in scores.values()
        ),
    }
    partition_checks = {
        "pairwise_f1": partition_metrics["pairwise_f1"]
        >= PARTITION_CALIBRATION_GATES["pairwise_f1_min"],
        "exact_case_rate": partition_metrics["exact_case_rate"]
        >= PARTITION_CALIBRATION_GATES["exact_case_rate_min"],
        "same_side_exact_rate": partition_metrics["same_side_exact_rate"]
        >= PARTITION_CALIBRATION_GATES["same_side_exact_rate_min"],
        "minimum_same_side_cases": partition_metrics["same_side_case_count"]
        >= PARTITION_CALIBRATION_GATES["minimum_same_side_cases"],
    }
    partition_calibration = {
        "passed": all(partition_checks.values()),
        "gates": PARTITION_CALIBRATION_GATES,
        "metrics": partition_metrics,
        "checks": partition_checks,
    }
    report = {
        "schema_version": JUDGE_CALIBRATION_VERSION,
        "completed_at": judge_report.get("completed_at"),
        "calibrated": bool(
            combined["passed"]
            and partition_calibration["passed"]
            and judge_report["accounting_complete"]
            and all(score["abstention_count"] == 0 for score in scores.values())
        ),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "fixture_sha256": expected["fixture_sha256"],
        "fixture_case_count": len(expected["cases"]),
        "evaluator_spec_sha256": _sha256_file(evaluator_file),
        "scores": scores,
        "combined": combined,
        "equivalence_partition_calibration": partition_calibration,
        "judge_report_sha256": _sha256_file(judge_root / "report.json"),
        "accounting_complete": judge_report["accounting_complete"],
        "usage": judge_report["usage"],
        "fail_closed_reason": None,
    }
    if not report["calibrated"]:
        if not judge_report["accounting_complete"]:
            report["fail_closed_reason"] = "incomplete_accounting"
        elif any(score["abstention_count"] for score in scores.values()):
            report["fail_closed_reason"] = "calibration_abstentions"
        else:
            report["fail_closed_reason"] = "fixture_gates_failed"
    write_immutable_json(root / "report.json", report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="App-server-only blinded semantic judge")
    subparsers = parser.add_subparsers(dest="command", required=True)
    judge = subparsers.add_parser("judge")
    judge.add_argument("--pool", required=True)
    judge.add_argument("--output-dir", required=True)
    judge.add_argument("--model", default="gpt-5.6-sol")
    judge.add_argument("--reasoning-effort", default="high")
    judge.add_argument("--timeout-seconds", type=float, default=1200.0)
    calibration = subparsers.add_parser("calibrate")
    calibration.add_argument("--output-dir", required=True)
    calibration.add_argument("--model", default="gpt-5.6-sol")
    calibration.add_argument("--reasoning-effort", default="high")
    calibration.add_argument("--timeout-seconds", type=float, default=1200.0)
    calibration.add_argument("--fixture")
    calibration.add_argument("--evaluator-spec")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "judge":
        report = asyncio.run(
            run_app_server_semantic_judge(
                pool_path=Path(args.pool),
                output_dir=Path(args.output_dir),
                model=args.model,
                reasoning_effort=args.reasoning_effort,
                timeout_seconds=args.timeout_seconds,
            )
        )
    else:
        report = asyncio.run(
            run_app_server_judge_calibration(
                output_dir=Path(args.output_dir),
                model=args.model,
                reasoning_effort=args.reasoning_effort,
                timeout_seconds=args.timeout_seconds,
                fixture_path=Path(args.fixture) if args.fixture else None,
                evaluator_spec_path=Path(args.evaluator_spec) if args.evaluator_spec else None,
            )
        )
    print(_pretty_json(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
