from __future__ import annotations

import json
import asyncio
from pathlib import Path

from research_factory.signal_desk_gold_prompt_variant import (
    CONTEXT_FIRST_ADDENDUM,
    VARIANT_ID,
    build_dev_variant_plan,
    build_rejected_event_taxonomy,
    system_prompts_for_variant,
    variant_registry_entry,
)


ROOT = Path("work/signal-desk-rebuild/gold-authoring-v2")
MANIFEST = Path("work/signal-desk-rebuild/benchmark/partial-manifest.json")


def test_variant_changes_only_the_system_prompt_and_stays_development_only():
    entry = variant_registry_entry()
    config = json.loads(Path("config/signal_desk_gold_prompt_variants.json").read_text())
    registered = config["variants"][0]
    assert entry["variant_id"] == VARIANT_ID
    assert registered["variant_id"] == entry["variant_id"]
    assert registered["addendum_sha256"] == entry["addendum_sha256"]
    assert registered["prompt_sha256"] == entry["prompt_sha256"]
    assert entry["changed_dimension"] == "system_prompt"
    assert entry["one_change_invariant"] == {
        "changed": ["system_prompt"],
        "representation_unchanged": True,
        "schema_unchanged": True,
        "scorer_unchanged": True,
        "sealed_items_opened": False,
    }
    assert entry["eligible_splits"] == ["development"]
    assert entry["expected_calls"] == {"A": 189, "B": 189, "C": 189, "total": 567}
    baseline = entry["baseline_prompt_sha256"]
    variant = entry["prompt_sha256"]
    assert all(baseline[turn] != variant[turn] for turn in ("A", "B", "C", "AUDIT"))
    assert all(prompt.endswith(CONTEXT_FIRST_ADDENDUM) for prompt in system_prompts_for_variant().values())


def test_dev_variant_plan_is_resumable_and_starts_no_provider_calls(tmp_path: Path):
    plan = build_dev_variant_plan(manifest_path=MANIFEST, output_root=tmp_path / VARIANT_ID)
    assert plan["resume"] is True
    assert plan["provider_calls_started"] is False
    assert plan["eligible_splits"] == ["development"]
    assert plan["expected_calls"]["total"] == 567
    assert "--execute --concurrency 2" in plan["command"]


def test_rejected_taxonomy_is_aggregate_only_and_matches_adjudicated_135():
    taxonomy = build_rejected_event_taxonomy(
        manifest_path=MANIFEST,
        result_root=ROOT / "results/development",
        project_root=Path.cwd(),
        audit_receipt_path=ROOT / "artifacts/gold-audit-dev.json",
        private_decisions_root=ROOT / "private-gpt55-disagreement-review",
    )
    assert taxonomy["rejected_event_count"] == 135
    assert taxonomy["decision_counts"] == {"audit_supported": 126, "neither_supported": 9}
    assert taxonomy["primary_categories"] == {
        "attribution": 61,
        "attribution_plus_stance": 13,
        "entity_reference": 3,
        "entity_reference_plus_stance": 3,
        "stance": 55,
    }
    assert taxonomy["transcript_structures"] == {
        "asr_diarized": 12,
        "flattened": 88,
        "paragraph": 23,
        "speaker_turn": 12,
    }
    encoded = json.dumps(taxonomy)
    assert "claim_text" not in encoded
    assert "evidence_text" not in encoded


def test_runner_receives_variant_id_without_opening_sealed_stages(tmp_path: Path, monkeypatch):
    import research_factory.signal_desk_gold_runner as runner

    captured = {}

    async def fake_run(**kwargs):
        captured.update(kwargs)
        return {"complete": True}

    monkeypatch.setattr(runner, "_run_gold_split_phases", fake_run)
    result = asyncio.run(runner.run_gold_split_phase(
        manifest_path=MANIFEST,
        project_root=Path.cwd(),
        result_root=tmp_path / "results",
        dispatch_database=tmp_path / "dispatch.sqlite",
        budget_database=tmp_path / "budget.sqlite",
        grant_path=tmp_path / "grant.json",
        session_root=tmp_path / "sessions",
        budget_dir=tmp_path / "budget",
        split="development",
        turn_type="PIPELINE",
        task_namespace="dev-prompt-variant",
        prompt_variant_id=VARIANT_ID,
    ))
    assert result == {"complete": True}
    assert captured["prompt_variant_id"] == VARIANT_ID
    assert captured["phase_order"] == ("A", "B", "C")
