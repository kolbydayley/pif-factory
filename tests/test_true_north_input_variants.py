import json
import hashlib
import re
from copy import deepcopy
from pathlib import Path

from research_factory import true_north
from research_factory import true_north_input_optimization as optimization


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "config" / "true_north_input_variants_v1.json"

FACTORS = {
    "system_prompt": ("sp", "append_system_directives"),
    "task_rules": ("tr", "rewrite_task_rules"),
    "surrounding_context": ("cx", "select_context_strategy"),
    "candidate_priors": ("cp", "present_candidate_priors"),
    "output_schema": ("os", "emit_output_schema"),
}

REQUIRED_INVARIANTS = {
    "change_exactly_one_factor",
    "preserve_candidate_id",
    "preserve_label_id",
    "preserve_exact_evidence_text",
    "preserve_exact_evidence_start_end",
    "preserve_parent_candidate_lineage",
    "preserve_segment_id_index_text_sha256",
    "never_embed_gold_content",
    "never_reference_sealed_holdouts",
    "never_mutate_production",
}


def load_registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def all_variants(registry: dict) -> list[dict]:
    return [
        variant
        for factor_payload in registry["factors"].values()
        for variant in factor_payload["variants"]
    ]


def hydrated_frozen_job() -> dict:
    context = {
        "artifact_path": "private/context.json",
        "concept_seed": ["alignment", "evaluation"],
        "context_run_id": "ctx-fixture",
        "entity_seed": {"speaker-a": "Person A"},
        "extraction_guidance": "Preserve exact scope.",
        "model": "fixture-context-model",
        "section_map": [{"title": "Evaluation", "start": 0}],
        "speaker_map": [{"raw": "SPEAKER_1", "name": "Person A"}],
    }
    segment_specs = [
        ("c1", "seg-c", 1, 2, 800, 0.91),
        ("c3", "seg-b", 0, 1, 600, 0.55),
        ("c2", "seg-a", 2, 0, 400, 0.20),
    ]
    segments = []
    candidates = []
    for candidate_id, segment_id, segment_index, event_index, start, confidence in segment_specs:
        evidence = f"Exact evidence for {candidate_id}."
        text = ("x" * start) + evidence + ("y" * (2000 - start - len(evidence)))
        segments.append(
            {
                "episode_id": "ep-fixture",
                "segment_id": segment_id,
                "segment_index": segment_index,
                "text": text,
                "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
        )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "label_id": f"label-{candidate_id}",
                "segment_id": segment_id,
                "segment_index": segment_index,
                "event_index": event_index,
                "event_type": "claim",
                "actor_name": "Person A",
                "actor_type": "person",
                "actor_affiliation": "Lab",
                "target_raw": "evaluation",
                "candidate_concept": "safety evaluation",
                "stance": "support",
                "claim_text": f"Atomic claim {candidate_id}.",
                "claim_type": "assertion",
                "certainty": "high",
                "time_horizon": "present",
                "frame": "technical",
                "causal_mechanism": "measurement",
                "counterclaim": None,
                "confidence": confidence,
                "evidence_text": evidence,
                "evidence_start": start,
                "evidence_end": start + len(evidence),
                "speaker": {
                    "name": "Person A",
                    "role": "guest",
                    "affiliation": "Lab",
                    "confidence": confidence,
                },
                "reported_actor": {
                    "name": "Person B",
                    "type": "person",
                    "affiliation": "Other Lab",
                    "confidence": confidence,
                },
                "source_context_kind": "direct",
                "source_context_confidence": confidence,
                "provenance": {"fixture": True},
                "_source_episode_id": "ep-fixture",
            }
        )
    candidate_ids = [row["candidate_id"] for row in candidates]
    return {
        "schema_version": true_north.RUN_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "task": "Fixture extraction task.",
        "instructions": true_north._atomic_instructions(gold=False),
        "output_schema": true_north.atomic_output_schema(candidate_ids),
        "input": {
            "episode": {"episode_id": "composite-fixture"},
            "episode_context": {"composite": True},
            "episode_contexts": [{"episode_id": "ep-fixture", "context": context}],
            "segment": {"segment_id": "composite-fixture", "segment_index": 0},
            "segments": segments,
            "candidates": candidates,
        },
    }


def raw_presentation_hash(value: object) -> str:
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(raw).hexdigest()


def test_registry_has_exactly_twenty_variants_for_each_factor() -> None:
    registry = load_registry()

    assert registry["schema_version"] == 1
    assert set(registry["factors"]) == set(FACTORS)
    assert len(all_variants(registry)) == 100
    for factor, payload in registry["factors"].items():
        assert len(payload["variants"]) == 20, factor


def test_each_factor_has_twenty_unique_live_fixture_presentations() -> None:
    registry = load_registry()
    baseline_job = hydrated_frozen_job()

    for factor, payload in registry["factors"].items():
        hashes = []
        for variant in payload["variants"]:
            packet, system_prompt = optimization._apply_transform(
                deepcopy(baseline_job),
                factor,
                variant["transform"],
            )
            hashes.append(
                raw_presentation_hash(
                    {"packet": packet, "system_prompt": system_prompt}
                )
            )
        assert len(hashes) == len(set(hashes)) == 20, factor


def test_variant_ids_are_unique_stable_and_have_one_baseline_per_factor() -> None:
    registry = load_registry()
    variants = all_variants(registry)
    ids = [variant["id"] for variant in variants]

    assert len(ids) == len(set(ids))
    for factor, (prefix, _) in FACTORS.items():
        factor_variants = registry["factors"][factor]["variants"]
        assert sum(variant["baseline"] is True for variant in factor_variants) == 1
        assert factor_variants[0]["id"] == f"{prefix}-00-baseline"
        assert factor_variants[0]["baseline"] is True
        assert [
            int(variant["id"].split("-", 2)[1]) for variant in factor_variants
        ] == list(range(20))
        assert all(
            re.fullmatch(rf"{prefix}-[0-1][0-9]-[a-z0-9-]+", variant["id"])
            for variant in factor_variants
        )


def test_every_variant_has_required_experimental_metadata() -> None:
    registry = load_registry()
    required = {
        "id",
        "factor",
        "baseline",
        "changes_only",
        "hypothesis",
        "transform",
        "risk",
        "expected_tradeoff",
    }

    for variant in all_variants(registry):
        assert set(variant) == required, variant["id"]
        assert isinstance(variant["baseline"], bool)
        assert variant["hypothesis"].strip()
        assert variant["risk"].strip()
        assert variant["expected_tradeoff"].strip()
        assert set(variant["transform"]) == {"operation", "parameters"}
        assert isinstance(variant["transform"]["parameters"], dict)


def test_factor_isolation_and_operation_dsl_are_explicit() -> None:
    registry = load_registry()
    all_factor_names = set(FACTORS)

    for factor, payload in registry["factors"].items():
        isolation = payload["isolation"]
        assert isolation["change_only"] == factor
        assert set(isolation["freeze"]) == all_factor_names - {factor}
        prefix, factor_operation = FACTORS[factor]

        for variant in payload["variants"]:
            assert variant["factor"] == factor
            assert variant["changes_only"] == factor
            expected_operation = "identity" if variant["baseline"] else factor_operation
            assert variant["transform"]["operation"] == expected_operation
            assert variant["id"].startswith(prefix + "-")


def test_global_safety_and_lineage_invariants_are_predeclared() -> None:
    registry = load_registry()

    assert set(registry["global_invariants"]) == REQUIRED_INVARIANTS
    assert registry["canonical_output_contract"] == "canonical_atomic_output_v1"


def test_every_schema_surface_normalizes_to_canonical_atomic_output() -> None:
    registry = load_registry()
    schema_variants = registry["factors"]["output_schema"]["variants"]
    allowed_styles = {
        "reversible_property_order",
        "reversible_wrapper",
        "reversible_aliases",
        "reversible_candidate_keyed",
        "reversible_description_level",
        "reversible_wrapper_aliases",
        "reversible_candidate_keyed_aliases",
    }
    canonical_fields = {
        "schema_version",
        "items",
        "candidate_id",
        "disposition",
        "reason_code",
        "atomic_claims",
    }

    for variant in schema_variants:
        parameters = variant["transform"]["parameters"]
        assert (
            parameters["normalization_contract"]
            == registry["canonical_output_contract"]
        ), variant["id"]
        if not variant["baseline"]:
            assert parameters["schema_style"] in allowed_styles
            assert set(parameters["required_fields"]) == canonical_fields
            assert parameters["shape"] in {"canonical", "wrapped", "candidate_keyed"}
            aliases = parameters["aliases"]
            assert isinstance(aliases, dict)
            assert set(aliases) <= canonical_fields
            assert len(set(aliases.values())) == len(aliases)
            assert all(isinstance(value, str) and value for value in aliases.values())
            assert parameters["root_order"]
            assert parameters["item_order"]
            if parameters["shape"] == "wrapped":
                assert parameters["wrapper"] in {"result", "output", "extraction"}
            else:
                assert "wrapper" not in parameters
            if parameters["shape"] == "candidate_keyed":
                assert "candidate_id" not in parameters["item_order"]


def test_context_variants_use_only_live_fields_and_deterministic_crops() -> None:
    registry = load_registry()
    live_fields = {
        "artifact_path",
        "concept_seed",
        "context_run_id",
        "entity_seed",
        "extraction_guidance",
        "model",
        "section_map",
        "speaker_map",
    }
    variants = registry["factors"]["surrounding_context"]["variants"]

    for variant in variants:
        if variant["baseline"]:
            continue
        parameters = variant["transform"]["parameters"]
        if parameters["strategy"] == "episode_context_fields":
            fields = parameters["ordered_fields"]
            assert fields
            assert len(fields) == len(set(fields))
            assert set(fields) <= live_fields
        else:
            assert parameters == {
                "strategy": "evidence_union_crop",
                "margin_chars": parameters["margin_chars"],
                "offset_basis": "original_segment_text",
                "preserve_segment_identity": True,
            }
            assert parameters["margin_chars"] in {128, 256, 512}


def test_candidate_prior_variants_use_only_finite_live_shape_transforms() -> None:
    registry = load_registry()
    variants = registry["factors"]["candidate_priors"]["variants"]
    allowed_strategies = {
        "candidate_field_projection",
        "confidence_projection",
        "candidate_field_order",
        "candidate_order",
        "candidate_policy",
    }
    allowed_sort_keys = {
        "candidate_id",
        "segment_id",
        "segment_index",
        "event_index",
        "evidence_start",
    }

    for variant in variants:
        if variant["baseline"]:
            continue
        parameters = variant["transform"]["parameters"]
        strategy = parameters["strategy"]
        assert strategy in allowed_strategies
        if strategy == "candidate_field_projection":
            assert ("drop_fields" in parameters) ^ ("keep_fields" in parameters)
            fields = parameters.get("drop_fields", parameters.get("keep_fields"))
            assert fields and len(fields) == len(set(fields))
        elif strategy == "confidence_projection":
            assert parameters == {
                "strategy": "confidence_projection",
                "mode": "three_buckets",
                "cut_points": [0.34, 0.67],
            }
        elif strategy == "candidate_field_order":
            assert parameters["ordered_fields"]
            assert len(parameters["ordered_fields"]) == len(
                set(parameters["ordered_fields"])
            )
        elif strategy == "candidate_order":
            assert set(parameters["sort_keys"]) <= allowed_sort_keys
            assert isinstance(parameters["descending"], bool)
        else:
            assert parameters["policy"] in {
                "upstream_candidate_fields_are_nonbinding",
                "exact_evidence_overrides_conflicting_prior_fields",
            }


def test_variants_embed_no_episode_ids_holdout_material_or_answer_keys() -> None:
    registry = load_registry()
    variant_text = json.dumps(all_variants(registry), sort_keys=True).lower()

    assert re.search(r"\bep_[0-9a-f]{8,}\b", variant_text) is None
    assert re.search(r"\btr_[0-9a-f]{8,}\b", variant_text) is None
    assert "holdout" not in variant_text
    assert "gold answer" not in variant_text
    assert "gold_answer" not in variant_text
    assert "expected answer" not in variant_text
    assert "expected_answer" not in variant_text


def test_no_variant_can_drop_identifier_evidence_or_lineage_invariants() -> None:
    registry = load_registry()
    invariant_text = " ".join(registry["global_invariants"])

    for required_fragment in (
        "candidate_id",
        "label_id",
        "evidence_text",
        "evidence_start_end",
        "parent_candidate_lineage",
        "segment_id_index_text_sha256",
    ):
        assert required_fragment in invariant_text

    for variant in all_variants(registry):
        serialized = json.dumps(variant).lower()
        assert "drop_candidate_id" not in serialized
        assert "evidence_id" not in serialized
        assert "rewrite_evidence" not in serialized
