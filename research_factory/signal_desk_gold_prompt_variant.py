"""One-change, development-only Gold prompt repair for Signal Desk.

This module deliberately keeps the repair narrow: the frozen transcript
representation, output schema, model, decoding settings, and scorer remain
unchanged.  The only changed input is the system-prompt addendum below.
Taxonomy helpers read development Gold-C and the private development
GPT-5.5 adjudications; they never open validation or sealed-holdout items.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_disagreement import build_cases
from .signal_desk_rebuild_gold import verify_frozen_manifest


SCHEMA_VERSION = "pif_signal_desk_gold_prompt_variant_v1"
VARIANT_ID = "gold-c-context-adjudication-v1"
FAMILY_ID = "signal-desk-gold-authoring-prompt"
PARENT_VARIANT_ID = "gold-authoring-baseline-v2"
MODEL = "gpt-5.6-sol"
EFFORT = "medium"
SCORER_VERSION = "scorer-v6"
CHANGED_DIMENSION = "system_prompt"

# One prompt-only change.  This is intentionally phrased as an ordering and
# conservatism contract rather than a new field, heuristic, or representation.
CONTEXT_FIRST_ADDENDUM = """\

CONTEXT-FIRST DISAMBIGUATION (the sole repair under this prompt variant):
Before emitting each event, inspect the complete supplied window, not only the
candidate evidence excerpt. Resolve fields in this order: (1) the complete
proposition, including nearby qualifiers and contrast words; (2) who is
speaking versus which people are named, quoted, or discussed; (3) speech act
and stance for that complete proposition. A named person is not the speaker
unless the transcript identifies that person as speaking. For structured text,
carry a speaker identity only when supported by transcript labels or nearby
dialogue; otherwise set speaker_id to null and attribution_type to
unresolved_speaker. For flattened or ASR text, never upgrade an indeterminable
speaker from show metadata or from a named subject; use only transcript-surface
identity or an explicit speaker map. Keep third_party_mention for named people
being discussed, never for the actual speaker. Treat an explicit assertion as
supportive, an explicit concern or risk as warning or skeptical, and retain
nearby "but", "however", and "although" qualifiers as mixed; use neutral only
for descriptive facts and unknown only when stance genuinely cannot be
determined. If attribution or the complete proposition is clipped or ambiguous
at a window boundary, omit the event or use the conservative field value; never
fill missing context by guessing.
"""


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def baseline_system_prompts() -> dict[str, str]:
    """Load baseline prompts lazily to avoid the measurement import cycle."""

    from .signal_desk_gold_measurement import (
        A_SYSTEM_PROMPT,
        AUDIT_SYSTEM_PROMPT,
        B_SYSTEM_PROMPT,
        C_SYSTEM_PROMPT,
    )

    return {"A": A_SYSTEM_PROMPT, "B": B_SYSTEM_PROMPT,
            "C": C_SYSTEM_PROMPT, "AUDIT": AUDIT_SYSTEM_PROMPT}


def system_prompts_for_variant(variant_id: str = VARIANT_ID) -> dict[str, str]:
    """Return only the prompt-mutated Gold A/B/C/AUDIT system prompts."""

    if variant_id != VARIANT_ID:
        raise ValueError(f"unknown Signal Desk Gold prompt variant: {variant_id}")
    return {turn: prompt + CONTEXT_FIRST_ADDENDUM
            for turn, prompt in baseline_system_prompts().items()}


def variant_registry_entry() -> dict[str, Any]:
    """Return the immutable JSON registry entry for this one-change child."""

    base = baseline_system_prompts()
    variant = system_prompts_for_variant()
    return {
        "schema_version": SCHEMA_VERSION,
        "variant_id": VARIANT_ID,
        "campaign_id": "signal-desk-clean-corpus-2026-08-31",
        "family_id": FAMILY_ID,
        "family_type": "prompt",
        "parent_variant_id": PARENT_VARIANT_ID,
        "round_number": 1,
        "hypothesis": (
            "Complete-window ordering and conservative boundary handling will reduce "
            "stance qualifier loss and speaker-versus-subject attribution errors."
        ),
        "changed_dimension": CHANGED_DIMENSION,
        "model": MODEL,
        "provider": "codex_subscription",
        "reasoning_effort": EFFORT,
        "scorer_version": SCORER_VERSION,
        "seed": "dev-repair-v1",
        "one_change_invariant": {
            "changed": [CHANGED_DIMENSION],
            "representation_unchanged": True,
            "schema_unchanged": True,
            "scorer_unchanged": True,
            "sealed_items_opened": False,
        },
        "baseline_prompt_sha256": {turn: _sha(text) for turn, text in base.items()},
        "prompt_sha256": {turn: _sha(text) for turn, text in variant.items()},
        "addendum_sha256": _sha(CONTEXT_FIRST_ADDENDUM),
        "eligible_splits": ["development"],
        "expected_windows": 189,
        "expected_calls": {"A": 189, "B": 189, "C": 189, "total": 567},
        "output_root": (
            "work/signal-desk-rebuild/gold-authoring-v2/"
            "results/development-prompt-variants/gold-c-context-adjudication-v1"
        ),
        "provider_calls_started": False,
    }


def _load_decisions(private_root: Path) -> dict[str, Mapping[str, Any]]:
    decisions: dict[str, Mapping[str, Any]] = {}
    for path in sorted(private_root.glob("*.output.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("decisions", []):
            case_id = str(row.get("case_id") or "")
            if case_id:
                decisions[case_id] = row
    return decisions


def _primary_category(fields: Sequence[str]) -> str:
    values = set(fields)
    attribution = {"speaker", "speaker_role", "unsupported_attribution"}
    entities = {"mentioned_people", "quoted_person"}
    if values & attribution:
        return "attribution_plus_stance" if "stance" in values else "attribution"
    if values & entities:
        return "entity_reference_plus_stance" if "stance" in values else "entity_reference"
    if "stance" in values:
        return "stance"
    return "other"


def build_rejected_event_taxonomy(
    *, manifest_path: Path, result_root: Path, project_root: Path,
    audit_receipt_path: Path, private_decisions_root: Path,
) -> dict[str, Any]:
    """Taxonomize GPT-5.5-rejected Gold-C events from development only.

    The output is aggregate-only: no claim, evidence, rationale, or transcript
    text is copied into the returned receipt.
    """

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    audit = json.loads(audit_receipt_path.read_text(encoding="utf-8"))
    selected = [str(value) for value in audit["audit_selection"]["window_ids"]]
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    if any(metadata[window_id].get("split") != "development" for window_id in selected):
        raise ValueError("Gold-C repair taxonomy may read development items only")
    cases = build_cases(
        manifest_path=manifest_path,
        result_root=result_root,
        project_root=project_root,
        selected_window_ids=selected,
    )
    decisions = _load_decisions(private_decisions_root)
    rejected = [
        case for case in cases
        if decisions.get(str(case["case_id"]), {}).get("decision")
        in {"audit_supported", "neither_supported"}
    ]
    if len(rejected) != 135:
        raise ValueError(f"expected 135 rejected Gold-C events, found {len(rejected)}")

    categories = Counter()
    structures = Counter()
    decisions_count = Counter()
    fields = Counter()
    category_structures: dict[str, Counter[str]] = defaultdict(Counter)
    windows = Counter()
    for case in rejected:
        decision = str(decisions[str(case["case_id"])]["decision"])
        category = _primary_category(case["fields"])
        structure = str(metadata[str(case["window_id"])]["transcript_structure"])
        categories[category] += 1
        structures[structure] += 1
        decisions_count[decision] += 1
        windows[str(case["window_id"])] += 1
        category_structures[category][structure] += 1
        fields.update(str(field) for field in case["fields"])

    taxonomy = {
        "schema_version": "pif_signal_desk_gold_c_rejected_taxonomy_v1",
        "variant_id": VARIANT_ID,
        "source": {
            "audit_receipt_sha256": _sha(audit_receipt_path.read_text(encoding="utf-8")),
            "selected_windows": len(selected),
            "decision_count": len(decisions),
            "development_only": True,
        },
        "rejected_event_count": len(rejected),
        "decision_counts": dict(sorted(decisions_count.items())),
        "primary_categories": dict(sorted(categories.items())),
        "transcript_structures": dict(sorted(structures.items())),
        "disputed_fields": dict(sorted(fields.items())),
        "category_by_structure": {
            category: dict(sorted(counts.items()))
            for category, counts in sorted(category_structures.items())
        },
        "concentration": {
            "top_window_rejected_event_counts": [
                {"window_id": window_id, "rejected_events": count}
                for window_id, count in windows.most_common(10)
            ],
        },
        "interpretation": {
            "dominant_error_families": [
                "stance_and_qualifier_resolution",
                "speaker_vs_named_subject_attribution",
            ],
            "overlap_note": "Primary categories are exclusive; field counts are overlapping.",
            "prompt_change_scope": "system_prompt_only",
            "sealed_items_opened": False,
        },
    }
    taxonomy["receipt_sha256"] = _sha(json.dumps(taxonomy, sort_keys=True, separators=(",", ":")))
    return taxonomy


def build_dev_variant_plan(*, manifest_path: Path, output_root: Path) -> dict[str, Any]:
    """Build a no-call, resumable development rerun plan."""

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    development = [row for row in manifest["windows"] if row.get("split") == "development"]
    if len(development) != 189:
        raise ValueError(f"expected 189 development windows, found {len(development)}")
    plan = variant_registry_entry()
    plan.update({
        "manifest_sha256": manifest["manifest_sha256"],
        "output_root": str(output_root),
        "resume": True,
        "provider_calls_started": False,
        "command": (
            "python3 -B scripts/pif_signal_desk_gold_prompt_variant.py "
            "--execute --concurrency 2"
        ),
    })
    plan["plan_sha256"] = _sha(json.dumps(plan, sort_keys=True, separators=(",", ":")))
    return plan
