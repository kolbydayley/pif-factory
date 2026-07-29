from __future__ import annotations

import os
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.radar import (
    BUDGETS,
    PIPELINE_CONTRACT,
    AuthorityStatus,
    RadarPolicyError,
    RadarTransitionError,
    ResearchRadar,
    default_radar_db_path,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ResearchRadarTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary.name) / "research-radar.sqlite3"
        self.radar = ResearchRadar(self.db_path)

    def tearDown(self) -> None:
        self.radar.close()
        self.temporary.cleanup()

    def seed_source_and_evidence(
        self,
        *,
        workspace_id: str,
        suffix: str = "one",
        trust_tier: str = "primary",
        directly_usable: bool = True,
    ) -> tuple[dict, dict, dict]:
        source = self.radar.create_source(
            source_id=f"src_{suffix}",
            workspace_id=workspace_id,
            name=f"Source {suffix}",
            canonical_url=f"https://example.com/{suffix}",
            source_type="official_post" if trust_tier == "primary" else "news",
            trust_tier=trust_tier,
            discovery_reason="fixture",
        )
        if directly_usable:
            for index in range(3):
                source = self.radar.record_source_sample(
                    source["id"],
                    workspace_id=workspace_id,
                    relevant=True,
                    sampled_at=f"2026-07-0{index + 1}T12:00:00+00:00",
                )
            source = self.radar.promote_source(
                source["id"],
                workspace_id=workspace_id,
                promoted_at="2026-07-04T12:00:00+00:00",
            )
        text = f"Source {suffix} says the release changes the public model interface."
        content = self.radar.create_content_item(
            content_item_id=f"content_{suffix}",
            source_id=source["id"],
            canonical_url=f"https://example.com/{suffix}/release",
            content_type="official_post",
            title=f"Release {suffix}",
            normalized_text=text,
            token_count=12,
            published_at="2026-07-10T00:00:00+00:00",
        )
        evidence = self.radar.create_evidence_span(
            evidence_span_id=f"evidence_{suffix}",
            content_item_id=content["id"],
            start_char=0,
            end_char=len(text),
            support_kind=(
                "primary_evidence" if trust_tier == "primary" else "reputable_reporting"
            ),
            status="provisional",
            confidence=0.95,
            extractor_version="radar-extractor-frozen-v1",
            release_id="pilot-release-v1",
            lineage={"content_sha256": content["content_sha256"], "window": 0},
        )
        return source, content, evidence

    def seed_development_and_briefing(
        self,
        *,
        workspace_id: str,
        evidence_id: str,
        directly_establishes: bool = True,
    ) -> tuple[dict, dict]:
        common = {
            "workspace_id": workspace_id,
            "evidence_span_id": evidence_id,
            "extractor_version": "radar-extractor-frozen-v1",
            "release_id": "pilot-release-v1",
            "lineage": {"document_merge": "merge_fixture", "graph_reconciliation": "graph_fixture"},
            "status": "provisional",
            "confidence": 0.92,
        }
        development = self.radar.create_development(
            development_id=f"dev_{evidence_id}",
            title="A public model interface changed",
            summary="The published interface changed in a material way.",
            event_at="2026-07-10T00:00:00+00:00",
            significance=0.8,
            directly_establishes=directly_establishes,
            **common,
        )
        briefing = self.radar.create_briefing(
            briefing_id=f"brief_{evidence_id}",
            development_id=development["id"],
            headline="The public model interface changed",
            what_changed="The provider published a changed model interface.",
            why_it_matters="Integrators may need to reassess compatibility.",
            competing_interpretations=["A major platform shift", "A bounded interface revision"],
            unresolved_questions=["Will compatibility remain stable?"],
            watch_next=["Migration documentation"],
            event_at="2026-07-10T00:00:00+00:00",
            directly_establishes=directly_establishes,
            **common,
        )
        return development, briefing

    def test_default_database_path_is_outside_documents_and_configurable(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RESEARCH_RADAR_DB", None)
            path = default_radar_db_path()
        self.assertIn("Library/Application Support/Research Radar", str(path))
        self.assertNotIn("/Documents/", str(path))
        configured = Path(self.temporary.name) / "configured.sqlite"
        with patch.dict(os.environ, {"RESEARCH_RADAR_DB": str(configured)}):
            self.assertEqual(default_radar_db_path(), configured.resolve())

    def test_schema_is_isolated_and_contract_is_bounded(self) -> None:
        status = self.radar.status()
        self.assertEqual(status["schema_version"], "research_radar_v1")
        self.assertFalse(status["legacy_database_attached"])
        self.assertFalse(status["scheduler_enabled"])
        self.assertEqual(BUDGETS.max_windows_per_item, 4)
        self.assertEqual(BUDGETS.max_input_tokens_per_item, 75_000)
        self.assertEqual(BUDGETS.max_item_seconds, 900)
        self.assertEqual(BUDGETS.max_items_per_cycle, 25)
        self.assertEqual(BUDGETS.max_cycle_seconds, 7_200)
        self.assertEqual(BUDGETS.max_schema_repairs_per_item, 1)
        self.assertEqual(BUDGETS.max_transport_retries_per_item, 1)
        self.assertFalse(PIPELINE_CONTRACT.embeddings_allowed)
        self.assertFalse(PIPELINE_CONTRACT.deterministic_semantic_matching_allowed)
        self.assertFalse(PIPELINE_CONTRACT.automatic_model_transport)
        relations = {
            row[0]
            for row in self.radar.conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            ).fetchall()
        }
        self.assertIn("accepted_briefings", relations)
        self.assertIn("dead_letters", relations)
        self.assertNotIn("jobs", relations)
        self.assertNotIn("labels", relations)

    def test_frozen_five_item_fixture_seeds_idempotently(self) -> None:
        fixture = json.loads(
            (PROJECT_ROOT / "config" / "research_radar" / "demo_five_items_v1.json").read_text(
                encoding="utf-8"
            )
        )
        first = self.radar.seed_demo(fixture)
        second = self.radar.seed_demo(fixture)
        self.assertEqual(first["inserted_items"], 5)
        self.assertEqual(second["inserted_items"], 5)
        self.assertEqual(second["status"]["counts"]["briefings"], 5)
        self.assertEqual(second["status"]["counts"]["evidence_spans"], 5)
        self.assertEqual(second["status"]["counts"]["workspaces"], 1)

    def test_exact_evidence_offsets_and_direct_acceptance_bypass_are_rejected(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_test")
        _source, content, evidence = self.seed_source_and_evidence(
            workspace_id=workspace["id"], directly_usable=False
        )
        with self.assertRaises(RadarPolicyError):
            self.radar.create_evidence_span(
                content_item_id=content["id"],
                start_char=0,
                end_char=len(content["normalized_text"]) + 1,
                support_kind="primary_evidence",
                confidence=1,
                extractor_version="v1",
                release_id="r1",
                lineage={"source": "fixture"},
            )
        with self.assertRaises(RadarPolicyError):
            self.radar.create_development(
                workspace_id=workspace["id"],
                title="Bypass",
                summary="Should not enter accepted authority directly.",
                event_at="2026-07-10T00:00:00+00:00",
                evidence_span_id=evidence["id"],
                extractor_version="v1",
                release_id="r1",
                lineage={"source": "fixture"},
                status="verified",
            )

    def test_provisional_overlay_is_excluded_until_explicit_verification(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_acceptance")
        source, _content, evidence = self.seed_source_and_evidence(workspace_id=workspace["id"])
        development, briefing = self.seed_development_and_briefing(
            workspace_id=workspace["id"], evidence_id=evidence["id"]
        )
        self.assertEqual(source["status"], "active")
        self.assertEqual(
            self.radar.conn.execute("SELECT COUNT(*) FROM accepted_briefings").fetchone()[0], 0
        )
        with self.assertRaises(RadarPolicyError):
            self.radar.transition_authority(
                "development", development["id"], "verified", reason="premature"
            )
        self.radar.transition_authority(
            "evidence_span", evidence["id"], "verified", reason="exact evidence accepted"
        )
        development = self.radar.transition_authority(
            "development", development["id"], "verified", reason="primary source verification"
        )
        briefing = self.radar.transition_authority(
            "briefing", briefing["id"], "verified", reason="brief verified"
        )
        self.assertEqual(development["status"], "verified")
        self.assertEqual(briefing["status"], "verified")
        self.assertEqual(
            self.radar.conn.execute("SELECT COUNT(*) FROM accepted_briefings").fetchone()[0], 1
        )
        briefing = self.radar.transition_authority(
            "briefing", briefing["id"], "amended", reason="clarified scope"
        )
        self.assertEqual(briefing["status"], "amended")
        briefing = self.radar.transition_authority(
            "briefing", briefing["id"], "retracted", reason="source retracted announcement"
        )
        self.assertEqual(briefing["status"], "retracted")
        self.assertEqual(
            self.radar.conn.execute("SELECT COUNT(*) FROM accepted_briefings").fetchone()[0], 0
        )
        history = self.radar.authority_history("briefing", briefing["id"])
        self.assertEqual(
            [row["to_status"] for row in history],
            ["provisional", "verified", "amended", "retracted"],
        )
        with self.assertRaises(RadarTransitionError):
            self.radar.transition_authority(
                "briefing", briefing["id"], "verified", reason="terminal means terminal"
            )

    def test_verification_requires_active_sources_and_policy_valid_evidence(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_policy")
        _source, _content, evidence = self.seed_source_and_evidence(
            workspace_id=workspace["id"], directly_usable=False
        )
        development, _briefing = self.seed_development_and_briefing(
            workspace_id=workspace["id"], evidence_id=evidence["id"]
        )
        self.radar.transition_authority(
            "evidence_span", evidence["id"], "verified", reason="span reviewed"
        )
        decision = self.radar.verification_evidence("development", development["id"])
        self.assertFalse(decision["accepted"])
        with self.assertRaises(RadarPolicyError):
            self.radar.transition_authority(
                "development", development["id"], "verified", reason="probation is insufficient"
            )

    def test_two_independent_reputable_sources_can_verify_but_weak_signal_cannot(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_secondary")
        _first, _content, first_evidence = self.seed_source_and_evidence(
            workspace_id=workspace["id"], suffix="secondary_a", trust_tier="reputable_secondary"
        )
        _second, _content, second_evidence = self.seed_source_and_evidence(
            workspace_id=workspace["id"], suffix="secondary_b", trust_tier="reputable_secondary"
        )
        _weak, _content, weak_evidence = self.seed_source_and_evidence(
            workspace_id=workspace["id"], suffix="weak", trust_tier="weak_signal"
        )
        for evidence in (first_evidence, second_evidence, weak_evidence):
            self.radar.transition_authority(
                "evidence_span", evidence["id"], "verified", reason="span reviewed"
            )
        development, briefing = self.seed_development_and_briefing(
            workspace_id=workspace["id"],
            evidence_id=first_evidence["id"],
            directly_establishes=False,
        )
        self.radar.link_evidence(
            "development", development["id"], weak_evidence["id"], role="supports"
        )
        self.assertFalse(
            self.radar.verification_evidence("development", development["id"])["accepted"]
        )
        self.radar.link_evidence(
            "development", development["id"], second_evidence["id"], role="supports"
        )
        self.radar.link_evidence(
            "briefing", briefing["id"], second_evidence["id"], role="supports"
        )
        self.assertTrue(
            self.radar.verification_evidence("development", development["id"])["accepted"]
        )
        self.radar.transition_authority(
            "development", development["id"], "verified", reason="two independent reports"
        )
        self.radar.transition_authority(
            "briefing", briefing["id"], "verified", reason="two independent reports"
        )

    def test_source_probation_and_failure_pause_policy(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_sources")
        source = self.radar.create_source(
            workspace_id=workspace["id"],
            name="Probation Source",
            canonical_url="https://example.net/probation",
            source_type="newsletter",
            trust_tier="specialist_individual",
            discovery_reason="citation",
        )
        with self.assertRaises(RadarPolicyError):
            self.radar.promote_source(source["id"], workspace_id=workspace["id"])
        for index in range(3):
            source = self.radar.record_source_sample(
                source["id"], workspace_id=workspace["id"], relevant=True
            )
        source = self.radar.promote_source(source["id"], workspace_id=workspace["id"])
        self.assertEqual(source["status"], "active")
        for _index in range(5):
            source = self.radar.record_source_sample(
                source["id"], workspace_id=workspace["id"], relevant=False, fetch_succeeded=False
            )
        self.assertEqual(source["status"], "paused")
        self.assertEqual(source["paused_reason"], "five_consecutive_fetch_failures")

    def test_query_feedback_and_fts_surfaces_are_json_safe(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_queries")
        _source, _content, evidence = self.seed_source_and_evidence(workspace_id=workspace["id"])
        development, briefing = self.seed_development_and_briefing(
            workspace_id=workspace["id"], evidence_id=evidence["id"]
        )
        self.radar.transition_authority(
            "evidence_span", evidence["id"], "verified", reason="reviewed"
        )
        self.radar.transition_authority(
            "development", development["id"], "verified", reason="primary"
        )
        self.radar.transition_authority("briefing", briefing["id"], "verified", reason="primary")
        feedback = self.radar.record_feedback(briefing["id"], "useful")
        self.assertEqual(feedback["rating"], "useful")
        detail = self.radar.get_briefing(briefing["id"])
        self.assertEqual(detail["feedback"][0]["rating"], "useful")
        self.assertEqual(detail["evidence"][0]["source_name"], "Source one")
        self.assertEqual(self.radar.list_workspaces()[0]["briefing_count"], 1)
        self.assertEqual(self.radar.list_briefings(status="accepted")[0]["id"], briefing["id"])
        self.assertEqual(
            self.radar.workspace_timeline(workspace["id"], status="accepted")[0]["id"],
            development["id"],
        )
        self.assertEqual(self.radar.search("model interface")[0]["record_id"], briefing["id"])

    def test_bounded_cycle_is_dry_without_processor_and_dead_letters_budget_breaches(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_cycle")
        for index in range(30):
            self.radar.enqueue_pipeline_item(
                workspace_id=workspace["id"],
                stage="discover",
                idempotency_key=f"cycle-{index}",
            )
        dry_receipt = self.radar.run_cycle(max_items=100)
        self.assertEqual(dry_receipt["status"], "dry_run")
        self.assertEqual(dry_receipt["item_count"], 25)
        self.assertEqual(
            self.radar.conn.execute(
                "SELECT COUNT(*) FROM pipeline_items WHERE status='queued'"
            ).fetchone()[0],
            30,
        )

        good = self.radar.run_cycle(
            lambda _item, _budgets: {
                "status": "succeeded",
                "input_tokens": 100,
                "window_count": 1,
                "schema_repairs": 0,
                "transport_retries": 0,
                "wall_seconds": 1,
            },
            max_items=1,
        )
        self.assertEqual(good["status"], "completed")
        bad = self.radar.run_cycle(
            lambda _item, _budgets: {
                "status": "succeeded",
                "input_tokens": 100,
                "window_count": 5,
                "wall_seconds": 1,
            },
            max_items=1,
        )
        self.assertEqual(bad["status"], "completed_with_dead_letters")
        operations = self.radar.operations_summary()
        self.assertEqual(operations["queue"]["succeeded"], 1)
        self.assertEqual(operations["queue"]["dead_letter"], 1)
        self.assertEqual(operations["dead_letters"]["open"], 1)

    def test_cycle_reclaims_expired_leases_and_measures_processor_time(self) -> None:
        workspace = self.radar.create_workspace(workspace_id="ws_reclaim")
        stale = self.radar.enqueue_pipeline_item(
            workspace_id=workspace["id"], stage="fetch", idempotency_key="stale-lease"
        )
        with self.radar.conn:
            self.radar.conn.execute(
                """
                UPDATE pipeline_items SET status='in_progress',claimed_at='2000-01-01T00:00:00+00:00'
                WHERE id=?
                """,
                (stale["id"],),
            )
        receipt = self.radar.run_cycle(max_items=1)
        self.assertEqual(receipt["details"]["reclaimed_stale_item_leases"], 1)
        self.assertEqual(
            self.radar.conn.execute(
                "SELECT status FROM pipeline_items WHERE id=?", (stale["id"],)
            ).fetchone()[0],
            "queued",
        )

        ticks = iter((0.0, 0.0, 1.0, 902.0, 902.0))
        slow_receipt = self.radar.run_cycle(
            lambda _item, _budgets: {
                "status": "succeeded",
                "input_tokens": 10,
                "window_count": 1,
                "wall_seconds": 0,
            },
            max_items=1,
            now_monotonic=lambda: next(ticks),
        )
        self.assertEqual(slow_receipt["status"], "completed_with_dead_letters")
        dead = self.radar.conn.execute(
            "SELECT error_message FROM dead_letters ORDER BY rowid DESC LIMIT 1"
        ).fetchone()[0]
        self.assertIn("wall_seconds", dead)


if __name__ == "__main__":
    unittest.main()
