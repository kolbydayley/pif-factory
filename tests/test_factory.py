from __future__ import annotations

import fcntl
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import urllib.request
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT = Path(__file__).resolve().parents[1]


class ClaimSubjectNormalizerTest(unittest.TestCase):
    def frame(self, **overrides: object) -> dict:
        from research_factory.claim_subjects import _frame_claim

        row = {
            "claim_id": "clm_unit",
            "text": "Sundar says Google is laying the primitives needed for agents to work end to end.",
            "confidence": 0.8,
            "frame": "agent_infrastructure",
            "concept_name": "agentic_interface",
            "organizations_json": '["Google"]',
            "product_names_json": "[]",
            "model_names_json": "[]",
        }
        row.update(overrides)
        return _frame_claim(row)

    def test_company_only_fallback_uses_actual_issue_frame(self) -> None:
        frame = self.frame()
        self.assertEqual(frame["subject_text"], "Agent Infrastructure")
        self.assertNotIn("Strategic Outlook", frame["subject_text"])
        self.assertEqual(frame["object_text"], "Google")

    def test_unmentioned_organization_does_not_create_company_catchall(self) -> None:
        frame = self.frame(
            text="Grant Sanderson frames LLMs as useful search tools for finding human-written learning resources.",
            frame="human_curation_as_residual_role",
            concept_name="llm_search_tools",
            organizations_json='["Google"]',
        )
        self.assertEqual(frame["subject_text"], "Human Curation As Residual Role")
        self.assertEqual(frame["object_text"], "llm search tools")
        self.assertNotIn("Google", frame["subject_text"])
        self.assertNotIn("Strategic Outlook", frame["subject_text"])


class FactoryCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        shutil.copytree(PROJECT / "label_packs", self.root / "label_packs")
        self.env = os.environ.copy()
        self.env["PYTHONPATH"] = str(PROJECT)
        self.env["RESEARCH_FACTORY_ROOT"] = str(self.root)
        self.env["RESEARCH_FACTORY_DB"] = str(self.root / "data" / "factory.sqlite")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_cli(self, *args: str) -> dict:
        result = self.run_cli_process(*args, check=True)
        return json.loads(result.stdout)

    def run_cli_process(self, *args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "research_factory", *args],
            cwd=PROJECT,
            env=self.env,
            capture_output=True,
            text=True,
            check=check,
        )

    def db_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.env["RESEARCH_FACTORY_DB"])
        conn.row_factory = sqlite3.Row
        return conn

    def assert_operational_snapshot(self, payload: dict) -> None:
        self.assertEqual(
            set(payload),
            {
                "contract_version",
                "generated_at",
                "privacy",
                "counts",
                "queues",
                "runs",
                "failures",
                "artifacts",
                "intervention_flags",
            },
        )
        self.assertEqual(payload["contract_version"], "railway-operational-v2")
        self.assertEqual(payload["privacy"], "sanitized_operational_snapshot_no_raw_transcripts")
        self.assertIsInstance(payload["queues"], dict)
        self.assertIsInstance(payload["runs"], dict)
        self.assertIsInstance(payload["failures"], dict)
        for forbidden in (
            "trend_metrics",
            "coding_metrics",
            "useful_signal_metrics",
            "research_queue_metrics",
            "active_jobs",
            "recent_runs",
            "claim_subject_samples",
        ):
            self.assertNotIn(forbidden, payload)

    def seed_claim_normalization_rows(self, rows: list[dict]) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-07T00:00:00+00:00"
            conn.execute(
                """
                INSERT OR IGNORE INTO sources
                  (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_norm', 'Normalization Test', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_transcripts_only', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            for index, row in enumerate(rows):
                episode_id = f"ep_norm_{index}"
                transcript_id = f"tr_norm_{index}"
                segment_id = f"seg_norm_{index}"
                label_id = f"lab_norm_{index}"
                claim_id = f"clm_norm_{index}"
                published_at = row.get("published_at", "2026-06-01T00:00:00+00:00")
                label_pack = row.get("label_pack", "ai_discourse_v3_1")
                label_pack_version = row.get("label_pack_version", "3.1")
                label_status = row.get("label_status", "completed")
                conn.execute(
                    """
                    INSERT INTO episodes
                      (id, source_id, guid, title, description, url, audio_url, published_at, duration_seconds, created_at, updated_at)
                    VALUES (?, 'src_norm', ?, ?, NULL, NULL, NULL, ?, 1200, ?, ?)
                    """,
                    (episode_id, episode_id, f"Episode {index}", published_at, ts, ts),
                )
                conn.execute(
                    """
                    INSERT INTO transcripts
                      (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                    VALUES (?, ?, 'fixture', NULL, 'text/plain', ?, ?, 'ready', ?, '{}', 10, ?, ?)
                    """,
                    (transcript_id, episode_id, str(self.root / f"transcript-{index}.txt"), f"sha{index}", ts, ts, ts),
                )
                conn.execute(
                    """
                    INSERT INTO segments
                      (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                    VALUES (?, ?, ?, 'src_norm', 0, 0, 100, ?, ?, 10, ?)
                    """,
                    (segment_id, transcript_id, episode_id, str(self.root / f"segment-{index}.txt"), f"segsha{index}", ts),
                )
                conn.execute(
                    """
                    INSERT INTO labels
                      (id, segment_id, label_pack, label_pack_version, model, status, output_json, confidence, created_at)
                    VALUES (?, ?, ?, ?, 'fixture', ?, '{}', 0.9, ?)
                    """,
                    (label_id, segment_id, label_pack, label_pack_version, label_status, ts),
                )
                claim_text = row["text"]
                stance = row.get("stance", "affirming")
                conn.execute(
                    """
                    INSERT INTO claims
                      (id, label_id, segment_id, text, stance, confidence, evidence_json, created_at)
                    VALUES (?, ?, ?, ?, ?, 0.85, '{}', ?)
                    """,
                    (claim_id, label_id, segment_id, claim_text, stance, ts),
                )
                if row.get("with_discourse_event", True):
                    discourse_event_id = f"de_norm_{index}"
                    conn.execute(
                        """
                        INSERT INTO discourse_events
                          (id, label_id, segment_id, event_index, event_type, candidate_concept, canonical_concept_name, stance,
                           actor_name, actor_affiliation, claim_text, claim_type, certainty, temporal_horizon, frame,
                           evidence_text, evidence_start, evidence_end,
                           surface_terms_json, model_names_json, product_names_json, organizations_json, people_json, created_at)
                        VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, 'prediction', ?, ?, ?, 'sanitized evidence', 0, 18,
                           '[]', '[]', '[]', '[]', '[]', ?)
                        """,
                        (
                            discourse_event_id,
                            label_id,
                            segment_id,
                            row.get("event_type", "forecast"),
                            row.get("concept", "AI agents"),
                            row.get("concept", "AI agents"),
                            stance,
                            row.get("actor_name", f"Expert {index}"),
                            row.get("actor_affiliation"),
                            claim_text,
                            row.get("certainty", "medium"),
                            row.get("temporal_horizon", "future"),
                            row.get("frame", row.get("event_type", "forecast")),
                            ts,
                        ),
                    )
                    if row.get("canonical_person_name"):
                        person_id = row.get("canonical_person_id", f"cp_norm_{index}")
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO canonical_people
                              (id, display_name, normalized_name, confidence, status, evidence_json, created_at, updated_at)
                            VALUES (?, ?, ?, 0.85, 'candidate', '{}', ?, ?)
                            """,
                            (
                                person_id,
                                row["canonical_person_name"],
                                str(row["canonical_person_name"]).lower().replace(" ", "_"),
                                ts,
                                ts,
                            ),
                        )
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO raw_speaker_mentions
                              (id, episode_id, segment_id, discourse_event_id, surface_name, role, affiliation_surface,
                               canonical_person_id, resolution_status, confidence, evidence_json, created_at)
                            VALUES (?, ?, ?, ?, ?, 'speaker', ?, ?, 'candidate_match', 0.85, '{}', ?)
                            """,
                            (
                                f"rsm_norm_{index}",
                                episode_id,
                                segment_id,
                                discourse_event_id,
                                row.get("actor_name", row["canonical_person_name"]),
                                row.get("actor_affiliation"),
                                person_id,
                                ts,
                            ),
                        )
            conn.commit()

    def valid_episode_context_output(self, episode_id: str) -> dict:
        return {
            "schema_version": "ai_discourse_v3_1_episode_context",
            "episode_id": episode_id,
            "context_summary": "A fixture AI podcast episode about AGI timelines, agents, coding agents, inference scaling, enterprise AI, and terminology shifts.",
            "speaker_map": [
                {"surface": "guest", "role": "guest", "affiliation": None, "aliases": ["The guest"], "confidence": 0.72, "rationale": "The guest makes the AI timeline claim."},
                {"surface": "host", "role": "host", "affiliation": None, "aliases": ["The host"], "confidence": 0.72, "rationale": "The host asks about terminology."},
            ],
            "section_map": [
                {"section": "agentic AI and AGI timeline framing", "start_hint": "opening", "end_hint": "middle", "summary": "The discussion links tool-using agents to AGI timeline changes."},
                {"section": "enterprise workflow adoption", "start_hint": "middle", "end_hint": "late", "summary": "The episode connects coding agents and inference scaling to enterprise workflows."},
            ],
            "entity_seed": {
                "people": [],
                "organizations": ["frontier labs"],
                "products": ["coding agents"],
                "models": [],
                "other": ["AGI", "singularity", "inference scaling"],
            },
            "concept_seed": ["AGI timelines", "tool-using agents", "enterprise AI workflows", "terminology drift"],
            "extraction_guidance": "Use this compact context to separate guest claims from host questions and to watch for terminology drift around AGI and singularity.",
            "quality_flags": [],
            "overall_confidence": 0.78,
            "needs_review": False,
            "review_reason": None,
        }

    def write_fixture_source(self) -> Path:
        transcript = self.root / "episode.vtt"
        transcript.write_text(
            """WEBVTT

00:00:00.000 --> 00:00:05.000
The guest says AGI timelines are changing because agents can use tools.

00:00:05.000 --> 00:00:10.000
They compare coding agents, inference scaling, and enterprise AI workflows.

00:00:10.000 --> 00:00:15.000
The host asks whether the word singularity is replacing AGI inside frontier labs.
""",
            encoding="utf-8",
        )
        feed = self.root / "feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Fixture Tech Podcast</title>
    <item>
      <guid>fixture-1</guid>
      <title>Agents and AGI</title>
      <link>https://example.test/fixture-1</link>
      <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
      <description>Fixture episode.</description>
      <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
    </item>
  </channel>
</rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Fixture Tech Podcast
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        return sources

    def test_canonicalize_claims_quarantines_template_claims(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {"text": "segment contains a forward-looking ai forecast around ai forecast"},
                {"text": "AI agents will make software engineers more productive"},
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months")
        self.assertEqual(result["template_claims_quarantined"], 1)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_cluster_members").fetchone()[0], 1)

    def test_canonicalize_claims_merges_paraphrases_but_not_different_propositions(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {"text": "AI agents will make software engineers more productive", "concept": "AI agents"},
                {"text": "Software engineers will be more productive because of AI agents", "concept": "AI agents"},
                {"text": "AI agents will replace software engineers", "concept": "AI agents"},
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months")
        self.assertEqual(result["claims_grouped"], 3)
        with self.db_conn() as conn:
            cluster_counts = [
                row["count"]
                for row in conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM claim_cluster_members
                    GROUP BY cluster_id
                    ORDER BY count DESC
                    """
                ).fetchall()
            ]
            self.assertEqual(cluster_counts, [2, 1])

    def test_canonicalize_claims_keeps_stance_as_observation(self) -> None:
        claim = "Open source models will close the gap with proprietary models"
        self.seed_claim_normalization_rows(
            [
                {"text": claim, "stance": "affirming", "published_at": "2026-01-10T00:00:00+00:00"},
                {"text": claim, "stance": "skeptical", "published_at": "2026-02-10T00:00:00+00:00"},
            ]
        )
        self.run_cli("canonicalize-claims", "--scope", "last_18_months")
        self.run_cli("canonicalize-claims", "--scope", "last_18_months")
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_cluster_members").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(DISTINCT cluster_id) FROM claim_cluster_members").fetchone()[0], 1)
        from research_factory.observer import _trend_metrics

        with self.db_conn() as conn:
            trend = _trend_metrics(conn)
        stance_rows = trend["claim_subject_stance"]
        self.assertEqual({row["stance"] for row in stance_rows}, {"affirming", "skeptical"})
        timeline_days = {row["day"] for row in trend["claim_subject_timeline"]}
        self.assertIn("2026-01", timeline_days)
        self.assertIn("2026-02", timeline_days)
        self.assertNotIn("stance_mix", trend)
        self.assertNotIn("claim_timeline", trend)
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))

    def test_semantic_canonicalize_merges_bounded_paraphrases_idempotently(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {"text": "Teams that pay for frontier AI coding models get a different coding experience", "concept": "ai_coding_paid_frontier_access_segmentation", "event_type": "market_signal"},
                {"text": "Users who pay for access to peak AI coding models have a different AI coding experience", "concept": "ai_coding_paid_frontier_access_segmentation", "event_type": "market_signal"},
                {"text": "Teams using browser based coding tools face adoption friction", "concept": "browser_based_coding_adoption_barrier", "event_type": "counterclaim"},
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        self.assertEqual(result["semantic_merge_count"], 1)
        self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_cluster_members").fetchone()[0], 3)
            cluster_counts = [
                row["count"]
                for row in conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM claim_cluster_members
                    GROUP BY cluster_id
                    ORDER BY count DESC
                    """
                ).fetchall()
            ]
            self.assertEqual(cluster_counts, [2, 1])

    def test_semantic_canonicalize_keeps_number_and_negation_boundaries(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {"text": "Suleyman says the singularity will arrive within five years", "concept": "singularity_timeline_forecast", "event_type": "forecast"},
                {"text": "Suleyman says the singularity will not arrive within five years", "concept": "singularity_timeline_forecast", "event_type": "forecast"},
                {"text": "Beyer says more than 700 pieces of state AI legislation are active", "concept": "state_ai_policy_experimentation", "event_type": "market_signal"},
                {"text": "Beyer says more than 70 pieces of state AI legislation are active", "concept": "state_ai_policy_experimentation", "event_type": "market_signal"},
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        self.assertEqual(result["semantic_merge_count"], 0)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_cluster_members").fetchone()[0], 4)
            self.assertEqual(conn.execute("SELECT COUNT(DISTINCT cluster_id) FROM claim_cluster_members").fetchone()[0], 4)

    def test_semantic_canonicalize_quarantines_templates_and_observer_reports_quality(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {"text": "segment discusses safety_alignment"},
                {"text": "The speaker treats automation as evidence of adoption or workflow change instead of a passing reference."},
                {"text": "The speaker frames safety as a deployment or governance concern that could constrain progress."},
                {"text": "AI agents will make software engineers more productive", "concept": "agentic_software_development", "event_type": "adoption_signal"},
                {"text": "Software engineers will be more productive because of AI agents", "concept": "agentic_software_development", "event_type": "adoption_signal"},
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        self.assertEqual(result["template_claims_quarantined"], 3)
        self.assertEqual(result["semantic_merge_count"], 1)
        from research_factory.observer import _trend_metrics

        with self.db_conn() as conn:
            trend = _trend_metrics(conn)
        coverage = trend["claim_subject_coverage"]
        self.assertEqual(coverage["assigned_claims"], 2)
        self.assertEqual(coverage["position_observations"], 2)
        self.assertNotIn("canonical_claim_coverage", trend)
        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assert_operational_snapshot(payload)
        rendered = json.dumps(payload)
        self.assertNotIn("segment discusses safety_alignment", rendered)
        self.assertNotIn("passing reference", rendered)

    def test_claim_subjects_group_domain_general_technology_claims(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {
                    "text": "Open-source databases will keep taking share from proprietary databases",
                    "concept": "database_market_share",
                    "stance": "affirming",
                    "actor_name": "Software Analyst",
                    "published_at": "2025-03-01T00:00:00+00:00",
                },
                {
                    "text": "Open source databases are gaining market share from proprietary databases",
                    "concept": "database_market_share",
                    "stance": "affirming",
                    "actor_name": "Database Founder",
                    "published_at": "2025-06-01T00:00:00+00:00",
                },
                {
                    "text": "ARM servers will gain share from x86 in cloud data centers",
                    "concept": "server_cpu_architecture",
                    "stance": "affirming",
                    "actor_name": "Chip Analyst",
                    "published_at": "2025-07-01T00:00:00+00:00",
                },
                {
                    "text": "Software vendors will face more liability for security failures",
                    "concept": "cybersecurity_liability",
                    "stance": "affirming",
                    "actor_name": "Security Counsel",
                    "published_at": "2025-08-01T00:00:00+00:00",
                },
                {
                    "text": "Robotaxis will become profitable only in dense urban markets",
                    "concept": "robotaxi_business_model",
                    "stance": "affirming",
                    "actor_name": "Mobility Investor",
                    "published_at": "2025-09-01T00:00:00+00:00",
                },
                {
                    "text": "Robotaxis will face strict safety regulation before broad deployment",
                    "concept": "robotaxi_business_model",
                    "stance": "affirming",
                    "actor_name": "Transport Regulator",
                    "published_at": "2025-10-01T00:00:00+00:00",
                },
                {
                    "text": "Fusion will be grid relevant in the 2030s",
                    "concept": "fusion_commercialization",
                    "stance": "affirming",
                    "actor_name": "Energy Founder",
                    "published_at": "2026-01-01T00:00:00+00:00",
                },
                {
                    "text": "Fusion will not be grid relevant before the 2030s",
                    "concept": "fusion_commercialization",
                    "stance": "opposing",
                    "actor_name": "Grid Analyst",
                    "published_at": "2026-02-01T00:00:00+00:00",
                },
                {
                    "text": "AI-designed drugs will shorten early-stage discovery but not clinical trials",
                    "concept": "biotech_drug_discovery",
                    "stance": "mixed",
                    "actor_name": "Biotech Operator",
                    "published_at": "2026-03-01T00:00:00+00:00",
                },
                {
                    "text": "Satellite broadband will pressure rural fiber economics",
                    "concept": "telecom_infrastructure",
                    "stance": "affirming",
                    "actor_name": "Telecom Analyst",
                    "published_at": "2026-04-01T00:00:00+00:00",
                },
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        self.assertGreaterEqual(result["claim_subjects"]["subjects_considered"], 7)
        with self.db_conn() as conn:
            subject_rows = conn.execute(
                "SELECT id, subject_text FROM claim_subjects ORDER BY subject_text"
            ).fetchall()
            subject_texts = [row["subject_text"] for row in subject_rows]
            self.assertIn("Open-source Database Market Adoption", subject_texts)
            self.assertIn("ARM Server Market Adoption", subject_texts)
            self.assertIn("Software Vendor Cybersecurity Liability Regulation", subject_texts)
            self.assertIn("Robotaxi Unit Economics", subject_texts)
            self.assertIn("Robotaxi Regulation", subject_texts)
            self.assertIn("Fusion Commercialization Timeline", subject_texts)
            self.assertIn("Satellite Broadband and Rural Fiber Unit Economics", subject_texts)

            database_subject = conn.execute(
                "SELECT id FROM claim_subjects WHERE subject_text = 'Open-source Database Market Adoption'"
            ).fetchone()["id"]
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM claim_subject_members WHERE subject_id = ?",
                    (database_subject,),
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM claim_proposition_variants WHERE subject_id = ?",
                    (database_subject,),
                ).fetchone()[0],
                1,
            )

            fusion_subject = conn.execute(
                "SELECT id FROM claim_subjects WHERE subject_text = 'Fusion Commercialization Timeline'"
            ).fetchone()["id"]
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(DISTINCT stance) FROM claim_position_observations WHERE subject_id = ?",
                    (fusion_subject,),
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM claim_proposition_variants WHERE subject_id = ?",
                    (fusion_subject,),
                ).fetchone()[0],
                2,
            )

    def test_claim_subjects_adapt_legacy_label_packs_with_provenance(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {
                    "text": "Model evaluation should test robustness to indirect prompt injection before deployment.",
                    "concept": "model_evaluation_security",
                    "label_pack": "ai_discourse_v3_1",
                    "label_pack_version": "3.1",
                    "actor_name": "Current Evaluator",
                },
                {
                    "text": "Frontier models remain vulnerable to prompt-injection patterns that humans would find obviously malicious.",
                    "label_pack": "ai_discourse_v1",
                    "label_pack_version": "1",
                    "label_status": "ready",
                    "with_discourse_event": False,
                },
                {
                    "text": "Segment signals adoption movement around AI coding.",
                    "label_pack": "ai_discourse_v3",
                    "label_pack_version": "3",
                    "with_discourse_event": False,
                },
                {
                    "text": "as well. And then there’s the even bigger picture around what this will do to our job markets and so forth.",
                    "label_pack": "ai_discourse_v2",
                    "label_pack_version": "2",
                    "with_discourse_event": False,
                },
            ]
        )
        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        metrics = result["claim_subjects"]
        self.assertEqual(metrics["claims_seen_by_pack"]["ai_discourse_v1"], 1)
        self.assertEqual(metrics["claims_seen_by_pack"]["ai_discourse_v2"], 1)
        self.assertEqual(metrics["claims_seen_by_pack"]["ai_discourse_v3"], 1)
        self.assertEqual(metrics["claims_seen_by_pack"]["ai_discourse_v3_1"], 1)
        self.assertEqual(metrics["claims_subject_framed_by_pack"]["ai_discourse_v1"], 1)
        self.assertEqual(metrics["claims_subject_framed_by_pack"]["ai_discourse_v3_1"], 1)
        self.assertEqual(metrics["claims_quarantined_by_pack"]["ai_discourse_v2"], 1)
        self.assertEqual(metrics["claims_quarantined_by_pack"]["ai_discourse_v3"], 1)
        self.assertEqual(metrics["legacy_claims_framed"], 1)

        with self.db_conn() as conn:
            pack_rows = {
                row["label_pack"]: row["count"]
                for row in conn.execute(
                    """
                    SELECT labels.label_pack, COUNT(DISTINCT claims.id) AS count
                    FROM claims
                    JOIN labels ON labels.id = claims.label_id
                    JOIN claim_subject_members ON claim_subject_members.claim_id = claims.id
                    WHERE claim_subject_members.relation = 'about_subject'
                    GROUP BY labels.label_pack
                    """
                ).fetchall()
            }
            self.assertEqual(pack_rows, {"ai_discourse_v1": 1, "ai_discourse_v3_1": 1})
            legacy_evidence = json.loads(
                conn.execute(
                    "SELECT evidence_json FROM claim_position_observations WHERE claim_id = 'clm_norm_1'"
                ).fetchone()["evidence_json"]
            )
            self.assertTrue(legacy_evidence["legacy_adapter"])
            self.assertEqual(legacy_evidence["label_pack"], "ai_discourse_v1")
            current_evidence = json.loads(
                conn.execute(
                    "SELECT evidence_json FROM claim_position_observations WHERE claim_id = 'clm_norm_0'"
                ).fetchone()["evidence_json"]
            )
            self.assertFalse(current_evidence["legacy_adapter"])

    def test_claim_subjects_attach_event_only_observations_and_expert_positions(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {
                    "text": "AGI terminology will replace singularity language in frontier labs",
                    "concept": "frontier_ai_end_state",
                    "event_type": "forecast",
                    "actor_name": "Riley Expert",
                    "canonical_person_name": "Riley Expert",
                    "canonical_person_id": "cp_riley_expert",
                    "published_at": "2026-01-15T00:00:00+00:00",
                }
            ]
        )
        with self.db_conn() as conn:
            ts = "2026-07-07T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO discourse_events
                  (id, label_id, segment_id, event_index, event_type, candidate_concept, canonical_concept_name, stance,
                   actor_name, actor_affiliation, claim_text, claim_type, certainty, temporal_horizon, frame,
                   evidence_text, evidence_start, evidence_end,
                   surface_terms_json, model_names_json, product_names_json, organizations_json, people_json, created_at)
                VALUES ('de_event_only_term', 'lab_norm_0', 'seg_norm_0', 1, 'term_usage', 'frontier_ai_end_state',
                   'frontier_ai_end_state', 'neutral', 'Riley Expert', NULL,
                   'The speaker uses AGI in a way that marks a meaningful discourse move, not a generic mention.',
                   'terminology', 'medium', 'current', 'terminology', 'sanitized evidence', 0, 18,
                   '["AGI"]', '[]', '[]', '[]', '[]', ?)
                """,
                (ts,),
            )
            conn.execute(
                """
                INSERT INTO discourse_events
                  (id, label_id, segment_id, event_index, event_type, candidate_concept, canonical_concept_name, stance,
                   actor_name, actor_affiliation, claim_text, claim_type, certainty, temporal_horizon, frame,
                   evidence_text, evidence_start, evidence_end,
                   surface_terms_json, model_names_json, product_names_json, organizations_json, people_json, created_at)
                VALUES ('de_event_only_actor', 'lab_norm_0', 'seg_norm_0', 2, 'actor_mention', 'frontier_ai_end_state',
                   'frontier_ai_end_state', 'neutral', 'Riley Expert', NULL,
                   'The speaker mentions a person without making a subject-level claim.',
                   'mention', 'low', 'current', 'identity', 'sanitized evidence', 0, 18,
                   '["Riley"]', '[]', '[]', '[]', '[]', ?)
                """,
                (ts,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO raw_speaker_mentions
                  (id, episode_id, segment_id, discourse_event_id, surface_name, role, affiliation_surface,
                   canonical_person_id, resolution_status, confidence, evidence_json, created_at)
                VALUES ('rsm_event_only_term', 'ep_norm_0', 'seg_norm_0', 'de_event_only_term',
                   'Riley Expert', 'speaker', NULL, 'cp_riley_expert', 'candidate_match', 0.86, '{}', ?)
                """,
                (ts,),
            )
            conn.commit()

        result = self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        subject_metrics = result["claim_subjects"]
        self.assertEqual(subject_metrics["event_observations_framed_by_type"], {"term_usage": 1})
        self.assertEqual(subject_metrics["event_observations_quarantined_by_type"], {"actor_mention": 1})
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_subject_event_observations").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_subject_expert_positions WHERE canonical_person_id = 'cp_riley_expert'").fetchone()[0], 2)
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM claim_proposition_variants
                    JOIN claim_subjects ON claim_subjects.id = claim_proposition_variants.subject_id
                    WHERE claim_subjects.subject_text LIKE '%Terminology%'
                    """
                ).fetchone()[0],
                0,
            )
        from research_factory.observer import _trend_metrics

        with self.db_conn() as conn:
            trend = _trend_metrics(conn)
        coverage = trend["claim_subject_coverage"]
        self.assertEqual(coverage["event_observations"], 1)
        self.assertGreaterEqual(coverage["expert_positions_with_canonical_person"], 1)
        self.assertTrue(any(row["speaker_resolution"] == "canonical_person" for row in trend["claim_subject_stance"]))
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))

    def test_delete_label_derivatives_removes_canonical_claim_subject_children(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {
                    "text": "Open-source databases will keep taking share from proprietary databases",
                    "concept": "database_market_share",
                    "event_type": "forecast",
                    "actor_name": "Software Analyst",
                    "published_at": "2025-03-01T00:00:00+00:00",
                }
            ]
        )
        self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        ts = "2026-07-07T00:00:00+00:00"
        with self.db_conn() as conn:
            row = conn.execute(
                """
                SELECT
                  claims.id AS claim_id,
                  claims.label_id AS label_id,
                  discourse_events.id AS discourse_event_id,
                  claim_subject_members.subject_id AS subject_id,
                  claim_position_observations.variant_id AS variant_id
                FROM claims
                JOIN discourse_events ON discourse_events.label_id = claims.label_id
                JOIN claim_subject_members ON claim_subject_members.claim_id = claims.id
                JOIN claim_position_observations ON claim_position_observations.claim_id = claims.id
                WHERE claims.label_id = 'lab_norm_0'
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            conn.execute(
                """
                INSERT INTO claim_clusters
                  (id, canonical_claim_text, status, confidence, evidence_json, created_at, updated_at)
                VALUES ('ccl_cleanup', 'Open-source database share growth', 'candidate', 0.9, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO claim_cluster_members
                  (id, cluster_id, claim_id, relation, confidence, method, created_at, updated_at)
                VALUES ('ccm_cleanup', 'ccl_cleanup', ?, 'supports_canonical_claim', 0.9, 'fixture', ?, ?)
                """,
                (row["claim_id"], ts, ts),
            )
            conn.execute(
                """
                INSERT INTO claim_subject_event_observations
                  (id, subject_id, discourse_event_id, event_type, event_role, speaker_name, evidence_json,
                   confidence, method, created_at, updated_at)
                VALUES ('cseo_cleanup', ?, ?, 'forecast', 'signal', 'Software Analyst', '{}', 0.8, 'fixture', ?, ?)
                """,
                (row["subject_id"], row["discourse_event_id"], ts, ts),
            )
            conn.execute(
                """
                INSERT INTO forecast_outcome_checks
                  (id, claim_id, outcome_status, confidence, evidence_json, created_at, updated_at)
                VALUES ('foc_cleanup', ?, 'pending', 0.5, '{}', ?, ?)
                """,
                (row["claim_id"], ts, ts),
            )
            conn.commit()

        from research_factory.worker import _delete_label_derivatives

        with self.db_conn() as conn:
            _delete_label_derivatives(conn, "lab_norm_0")
            conn.commit()
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_cluster_members WHERE claim_id = 'clm_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_subject_members WHERE claim_id = 'clm_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_position_observations WHERE claim_id = 'clm_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claim_subject_event_observations WHERE discourse_event_id = 'de_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM forecast_outcome_checks WHERE claim_id = 'clm_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM claims WHERE label_id = 'lab_norm_0'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM discourse_events WHERE label_id = 'lab_norm_0'").fetchone()[0], 0)

    def test_observer_reports_claim_subject_views_by_published_date(self) -> None:
        self.seed_claim_normalization_rows(
            [
                {
                    "text": "Open-source databases will keep taking share from proprietary databases",
                    "concept": "database_market_share",
                    "stance": "affirming",
                    "actor_name": "Software Analyst",
                    "published_at": "2025-03-01T00:00:00+00:00",
                },
                {
                    "text": "Open source databases are gaining market share from proprietary databases",
                    "concept": "database_market_share",
                    "stance": "affirming",
                    "actor_name": "Database Founder",
                    "published_at": "2025-06-01T00:00:00+00:00",
                },
                {
                    "text": "Robotaxis will become profitable only in dense urban markets",
                    "concept": "robotaxi_business_model",
                    "stance": "affirming",
                    "actor_name": "Mobility Investor",
                    "published_at": "2026-01-01T00:00:00+00:00",
                },
            ]
        )
        self.run_cli("canonicalize-claims", "--scope", "last_18_months", "--mode", "semantic")
        from research_factory.observer import _trend_metrics

        with self.db_conn() as conn:
            trend = _trend_metrics(conn)
        self.assertGreaterEqual(trend["claim_subject_coverage"]["claim_subjects"], 2)
        self.assertGreaterEqual(trend["claim_subject_coverage"]["position_observations"], 3)
        self.assertIn("claim_subject_timeline", trend)
        self.assertTrue(any(row["day"] == "2025-03" for row in trend["claim_subject_timeline"]))
        self.assertTrue(any(row["claim_subject"] == "Open-source Database Market Adoption" for row in trend["claim_subject_top"]))
        self.assertTrue(any(row["speaker"] == "Software Analyst" for row in trend["claim_subject_stance"]))
        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assert_operational_snapshot(payload)
        rendered = json.dumps(payload)
        self.assertNotIn(str(self.root), rendered)
        self.assertNotIn("WEBVTT", rendered)
        self.assertNotIn("prompt_path", rendered)

    def write_audio_only_source(self) -> Path:
        feed = self.root / "audio-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Audio Only Fixture</title><item>
  <guid>audio-1</guid>
  <title>Audio Only Transcript Candidate</title>
  <link>https://example.test/audio-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <enclosure url="https://audio.example.test/audio-1.mp3" type="audio/mpeg" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "audio-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Audio Only Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        return sources

    def test_ingest_label_export_snapshot(self) -> None:
        sources = self.write_fixture_source()
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["episodes"], 1)
        first_status = self.run_cli("status")
        second = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        second_status = self.run_cli("status")
        self.assertEqual(first_status["counts"]["jobs"], second_status["counts"]["jobs"])
        self.assertEqual(second["episodes"], 1)

        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertGreaterEqual(run["completed"], 2)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["episodes"], 1)
        self.assertGreaterEqual(status["counts"]["segments"], 1)
        self.assertGreaterEqual(status["counts"]["labels"], 1)

        trend = self.run_cli("export", "trend-report", "--topic", "agi", "--window", "month")
        self.assertTrue(Path(trend["path"]).exists())
        graph = self.run_cli("export", "graph", "--type", "concept_network")
        self.assertTrue(Path(graph["path"]).exists())
        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assert_operational_snapshot(payload)
        self.assertGreaterEqual(payload["counts"]["jobs"], 1)
        self.assertIn("by_status", payload["queues"])

    def test_backfill_episodes_can_fill_old_feed_history_metadata_only(self) -> None:
        transcript = self.root / "history.vtt"
        transcript.write_text(
            """WEBVTT

00:00:00.000 --> 00:00:05.000
The guest discusses infrastructure and software economics.
""",
            encoding="utf-8",
        )
        feed = self.root / "history-feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>History Fixture</title>
<item>
  <guid>history-2024</guid>
  <title>Old History Episode</title>
  <link>https://example.test/history-2024</link>
  <pubDate>Mon, 01 Jul 2024 12:00:00 GMT</pubDate>
  <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
</item>
<item>
  <guid>history-2025</guid>
  <title>Current History Episode</title>
  <link>https://example.test/history-2025</link>
  <pubDate>Mon, 03 Feb 2025 12:00:00 GMT</pubDate>
</item>
<item>
  <guid>history-2026</guid>
  <title>Newest History Episode</title>
  <link>https://example.test/history-2026</link>
  <pubDate>Mon, 05 Jan 2026 12:00:00 GMT</pubDate>
  <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
</item>
</channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "history-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: History Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        current = self.run_cli("enqueue", "--lane", "podcast", "--since", "2025-01-01", "--source-list", str(sources))
        self.assertEqual(current["episodes"], 2)
        self.assertEqual(current["skipped_before_since"], 1)
        before_status = self.run_cli("status")
        self.assertEqual(before_status["counts"]["episodes"], 2)

        dry = self.run_cli(
            "backfill-episodes",
            "--source-list",
            str(sources),
            "--source",
            "History Fixture",
            "--until",
            "2024-12-31",
            "--dry-run",
        )
        self.assertEqual(dry["episodes"], 1)
        self.assertEqual(dry["episodes_inserted"], 1)
        self.assertEqual(self.run_cli("status")["counts"]["episodes"], 2)

        backfilled = self.run_cli(
            "backfill-episodes",
            "--source-list",
            str(sources),
            "--source",
            "History Fixture",
            "--until",
            "2024-12-31",
            "--oldest-first",
        )
        self.assertEqual(backfilled["episodes"], 1)
        self.assertEqual(backfilled["episodes_inserted"], 1)
        self.assertEqual(backfilled["transcript_jobs"], 0)
        self.assertEqual(backfilled["missing_transcripts"], 0)
        after_status = self.run_cli("status")
        self.assertEqual(after_status["counts"]["episodes"], 3)
        self.assertEqual(after_status["counts"]["jobs"], before_status["counts"]["jobs"])

        second = self.run_cli("backfill-episodes", "--source-list", str(sources), "--until", "2024-12-31")
        self.assertEqual(second["episodes_existing"], 1)
        self.assertEqual(second["episodes_inserted"], 0)
        from research_factory.observer import _trend_metrics

        with self.db_conn() as conn:
            trend_metrics = _trend_metrics(conn)
        inventory = trend_metrics["episode_inventory"]
        self.assertEqual(inventory["episodes"], 3)
        self.assertEqual(inventory["pre_2025_episodes"], 1)
        self.assertEqual(inventory["feed_transcript_episodes"], 2)
        months = {row["day"]: row["episodes"] for row in trend_metrics["episode_month_timeline"]}
        self.assertEqual(months["2024-07"], 1)
        self.assertEqual(months["2025-02"], 1)
        self.assertEqual(months["2026-01"], 1)
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))

    def test_preflight_fails_closed(self) -> None:
        result = self.run_cli("preflight", "--model", "gpt-5.4")
        self.assertIn("codex_cli_available", result)

    def test_run_pilot_labels_accepts_label_pack_override(self) -> None:
        from research_factory.cli import build_parser

        args = build_parser().parse_args(
            [
                "run-pilot-labels",
                "--label-pack",
                "ai_discourse_v1",
                "--model",
                "gpt-5.5",
                "--max-jobs",
                "2",
            ]
        )
        self.assertEqual(args.label_pack, "ai_discourse_v1")
        self.assertEqual(args.max_jobs, 2)

    def test_bad_feed_is_queued_as_source_error(self) -> None:
        sources = self.root / "bad-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Broken Feed
    rss_url: {self.root / 'missing-feed.xml'}
    homepage_url: https://example.test/broken
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["source_errors"], 1)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["sources"], 1)
        self.assertEqual(status["counts"]["jobs"], 1)
        self.assertEqual(status["jobs"][0]["job_type"], "source_fetch_failed")
        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertEqual(run["processed"], 0)
        after = self.run_cli("status")
        self.assertEqual(after["jobs"][0]["status"], "pending")
        (self.root / "missing-feed.xml").write_text(
            """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Recovered Feed</title></channel></rss>""",
            encoding="utf-8",
        )
        healed = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(healed["source_errors"], 0)
        final_status = self.run_cli("status")
        self.assertEqual(final_status["jobs"][0]["status"], "completed")

    def test_attach_transcript_turns_manual_candidate_into_fetch_job(self) -> None:
        transcript = self.root / "official-transcript.vtt"
        transcript.write_text(
            """WEBVTT

00:00:00.000 --> 00:00:05.000
AGI discourse is shifting toward agents and inference scaling.

00:00:05.000 --> 00:00:10.000
The guest says enterprise AI releases will change the deployment story.
""",
            encoding="utf-8",
        )
        feed = self.root / "manual-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Manual Fixture</title><item>
  <guid>manual-1</guid>
  <title>Manual Transcript Candidate</title>
  <link>https://example.test/manual-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "manual-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Manual Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.assertTrue(self.run_cli("init")["ok"])
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["missing_transcripts"], 1)
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5", "--claim", "--worker-id", "test-discovery")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.assertEqual(candidates["candidates"][0]["job_status"], "claimed")
        self.assertEqual(candidates["candidates"][0]["lease_owner"], "test-discovery")
        empty = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5", "--claim", "--worker-id", "other-discovery")
        self.assertEqual(empty["candidates"], [])
        attached = self.run_cli(
            "attach-transcript",
            "--episode-id",
            episode_id,
            "--transcript-url",
            transcript.as_uri(),
            "--transcript-type",
            "text/vtt",
            "--source-kind",
            "official_show_transcript",
        )
        self.assertEqual(attached["episode_id"], episode_id)
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["jobs"], 2)
        run = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        self.assertGreaterEqual(run["completed"], 2)
        final_status = self.run_cli("status")
        self.assertEqual(final_status["counts"]["transcripts"], 1)
        self.assertGreaterEqual(final_status["counts"]["labels"], 1)

    def test_transcript_candidate_claim_caps_and_release(self) -> None:
        feed = self.root / "manual-cap-feed.xml"
        items = "\n".join(
            f"""<item>
  <guid>manual-cap-{index}</guid>
  <title>Manual Candidate {index}</title>
  <link>https://example.test/manual-cap-{index}</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item>"""
            for index in range(3)
        )
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Manual Cap Fixture</title>{items}</channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "manual-cap-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Manual Cap Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))

        first = self.run_cli(
            "transcript-candidates",
            "--lane",
            "podcast",
            "--limit",
            "5",
            "--claim",
            "--worker-id",
            "cap-worker",
            "--max-claimed",
            "10",
            "--per-source-limit",
            "1",
        )
        self.assertEqual(len(first["candidates"]), 1)
        second = self.run_cli(
            "transcript-candidates",
            "--lane",
            "podcast",
            "--limit",
            "5",
            "--claim",
            "--worker-id",
            "cap-worker-2",
            "--max-claimed",
            "1",
            "--per-source-limit",
            "1",
        )
        self.assertEqual(second["candidates"], [])

        released = self.run_cli("release-transcript-candidates", "--lane", "podcast", "--worker-id", "cap-worker", "--limit", "5")
        self.assertEqual(released["released"], 1)
        with self.db_conn() as conn:
            attempts = conn.execute("SELECT attempts FROM jobs WHERE lease_owner IS NULL AND status = 'pending' LIMIT 1").fetchone()
        self.assertEqual(attempts["attempts"], 0)

    def test_feed_refresh_preserves_verified_transcript_url(self) -> None:
        official = self.root / "official.vtt"
        official.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:10.000\nAGI agents and enterprise AI are discussed here.\n", encoding="utf-8")
        rss_transcript = self.root / "rss.vtt"
        rss_transcript.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:10.000\nRSS transcript text is different.\n", encoding="utf-8")
        feed = self.root / "refresh-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Refresh Fixture</title><item>
  <guid>refresh-1</guid>
  <title>Refresh Candidate</title>
  <link>https://example.test/refresh-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "refresh-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Refresh Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        candidate = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "1")["candidates"][0]
        self.run_cli(
            "attach-transcript",
            "--episode-id",
            candidate["episode_id"],
            "--transcript-url",
            official.as_uri(),
            "--transcript-type",
            "text/vtt",
            "--source-kind",
            "official_show_transcript",
        )
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Refresh Fixture</title><item>
  <guid>refresh-1</guid>
  <title>Refresh Candidate</title>
  <link>https://example.test/refresh-1</link>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <podcast:transcript url="{rss_transcript.as_uri()}" type="text/vtt" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        with self.db_conn() as conn:
            episode = conn.execute("SELECT transcript_url, feed_transcript_url, verified_transcript_url FROM episodes WHERE id = ?", (candidate["episode_id"],)).fetchone()
        self.assertEqual(episode["transcript_url"], official.as_uri())
        self.assertEqual(episode["verified_transcript_url"], official.as_uri())
        self.assertEqual(episode["feed_transcript_url"], rss_transcript.as_uri())

    def test_prompt_submit_is_fenced_by_worker_and_segment_identity(self) -> None:
        sources = self.write_fixture_source()
        self.assertTrue(self.run_cli("init")["ok"])
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--worker-id", "fetcher")

        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        output_path = Path(claim["output_path"])
        with self.db_conn() as conn:
            row = conn.execute(
                """
                SELECT segments.id AS segment_id, segments.episode_id
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE jobs.id = ?
                """,
                (claim["job_id"],),
            ).fetchone()
        valid_output = {
            "schema_version": "ai_discourse_v1",
            "segment_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "summary": "The segment discusses AGI, agents, coding agents, and enterprise AI workflows.",
            "topics": [{"topic": "agents", "stance": "bullish", "intensity": 0.7, "evidence": "agents can use tools"}],
            "terminology_shifts": [],
            "claims": [{"claim_text": "Agents are framed as useful for software and enterprise workflows.", "claim_type": "descriptive", "confidence": 0.7, "evidence": "coding agents, inference scaling, and enterprise AI workflows"}],
            "entities": {"people": [], "organizations": [], "products": []},
            "overall_confidence": 0.7,
            "needs_review": False,
            "review_reason": None,
        }
        wrong_segment = dict(valid_output, segment_id="seg_wrong")
        output_path.write_text(json.dumps(wrong_segment), encoding="utf-8")
        bad_owner = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-b")
        self.assertNotEqual(bad_owner.returncode, 0)
        bad_segment = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_segment.returncode, 0)

        output_path.write_text(json.dumps(valid_output), encoding="utf-8")
        wrong_path = output_path.with_name("wrong-output.json")
        wrong_path.write_text(json.dumps(valid_output), encoding="utf-8")
        bad_path = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(wrong_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_path.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE label_runs SET output_path = ? WHERE id = ?", (str(wrong_path), claim["label_run_id"]))
            conn.commit()
        bad_run_path = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(bad_run_path.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE label_runs SET output_path = ? WHERE id = ?", (str(output_path), claim["label_run_id"]))
            conn.execute("UPDATE jobs SET leased_until = '2000-01-01T00:00:00+00:00' WHERE id = ?", (claim["job_id"],))
            conn.commit()
        stale = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertNotEqual(stale.returncode, 0)
        with self.db_conn() as conn:
            conn.execute("UPDATE jobs SET leased_until = '2999-01-01T00:00:00+00:00' WHERE id = ?", (claim["job_id"],))
            conn.commit()
        submitted = self.run_cli("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertIn("label_id", submitted)
        self.assertEqual(self.run_cli("status")["counts"]["labels"], 1)

    def test_recover_label_runs_releases_missing_output_handoff(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        self.assertFalse(Path(claim["output_path"]).exists())
        with self.db_conn() as conn:
            conn.execute("UPDATE jobs SET attempts = max_attempts WHERE id = ?", (claim["job_id"],))
            conn.commit()

        dry = self.run_cli("recover-label-runs")
        self.assertEqual(dry["missing_outputs"], 1)
        self.assertEqual(dry["released_jobs"], 0)
        recovered = self.run_cli("recover-label-runs", "--release-missing-outputs")
        self.assertEqual(recovered["failed_runs"], 1)
        self.assertEqual(recovered["released_jobs"], 1)
        with self.db_conn() as conn:
            run = conn.execute("SELECT status FROM label_runs WHERE id = ?", (claim["label_run_id"],)).fetchone()
            job = conn.execute("SELECT status, lease_owner, attempts, max_attempts FROM jobs WHERE id = ?", (claim["job_id"],)).fetchone()
        self.assertEqual(run["status"], "failed")
        self.assertEqual(job["status"], "pending")
        self.assertIsNone(job["lease_owner"])
        self.assertLess(job["attempts"], job["max_attempts"])

    def test_cleanup_pilot_label_claims_releases_only_empty_claims(self) -> None:
        from research_factory import db

        self.run_cli("init")
        pilot_id = "pilot-cleanup-test"
        with self.db_conn() as conn:
            empty_claim = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="label_segment",
                target_id="segment-empty",
                payload={"pilot_id": pilot_id, "label_pack": "ai_discourse_v3_1"},
                max_attempts=2,
            )
            output_claim = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="label_segment",
                target_id="segment-output",
                payload={"pilot_id": pilot_id, "label_pack": "ai_discourse_v3_1", "output_path": "runs/outputs/existing.json"},
                max_attempts=2,
            )
            other_pilot = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="label_segment",
                target_id="segment-other",
                payload={"pilot_id": "other-pilot", "label_pack": "ai_discourse_v3_1"},
                max_attempts=2,
            )
            conn.execute(
                "UPDATE jobs SET status='claimed', lease_owner='worker-a', attempts=1 WHERE id IN (?, ?, ?)",
                (empty_claim, output_claim, other_pilot),
            )
            conn.commit()

        dry = self.run_cli("cleanup-pilot-label-claims", "--pilot-id", pilot_id, "--dry-run")
        self.assertEqual(dry["would_release"], 1)
        self.assertEqual(dry["released"], 0)
        self.assertEqual(dry["job_ids"], [empty_claim])

        cleaned = self.run_cli("cleanup-pilot-label-claims", "--pilot-id", pilot_id)
        self.assertEqual(cleaned["released"], 1)
        with self.db_conn() as conn:
            rows = {
                row["id"]: row
                for row in conn.execute(
                    "SELECT id, status, lease_owner, attempts FROM jobs WHERE id IN (?, ?, ?)",
                    (empty_claim, output_claim, other_pilot),
                )
            }
        self.assertEqual(rows[empty_claim]["status"], "pending")
        self.assertIsNone(rows[empty_claim]["lease_owner"])
        self.assertEqual(rows[empty_claim]["attempts"], 0)
        self.assertEqual(rows[output_claim]["status"], "claimed")
        self.assertEqual(rows[other_pilot]["status"], "claimed")

    def test_db_connect_sets_busy_timeout_for_concurrent_writes(self) -> None:
        from research_factory import db

        db_path = Path(self.env["RESEARCH_FACTORY_DB"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = db.connect(db_path)
        try:
            self.assertEqual(conn.execute("PRAGMA busy_timeout").fetchone()[0], 30000)
        finally:
            conn.close()

    def test_corpus_verify_detects_segment_hash_drift(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        self.assertTrue(self.run_cli("corpus-verify")["ok"])
        with self.db_conn() as conn:
            segment = conn.execute("SELECT text_path FROM segments LIMIT 1").fetchone()
        (self.root / segment["text_path"]).write_text("mutated text", encoding="utf-8")
        result = self.run_cli("corpus-verify")
        self.assertFalse(result["ok"])
        self.assertEqual(result["issues"][0]["issue"], "hash_mismatch")
        repaired = self.run_cli("corpus-verify", "--repair-hashes")
        self.assertEqual(repaired["repaired"], 1)
        self.assertTrue(self.run_cli("corpus-verify")["ok"])

    def test_refetch_removes_stale_unlabeled_segments(self) -> None:
        transcript = self.root / "changing-transcript.txt"
        transcript.write_text(" ".join(f"word{i}" for i in range(1700)), encoding="utf-8")
        feed = self.root / "changing-feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel><title>Changing Fixture</title><item>
    <guid>changing-1</guid>
    <title>Changing Transcript</title>
    <link>https://example.test/changing-1</link>
    <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
    <podcast:transcript url="{transcript.as_uri()}" type="text/plain" />
  </item></channel>
</rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "changing-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Changing Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        with self.db_conn() as conn:
            transcript_row = conn.execute("SELECT id, episode_id FROM transcripts LIMIT 1").fetchone()
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE transcript_id = ?", (transcript_row["id"],)).fetchone()[0], 3)

        transcript.write_text(" ".join(f"short{i}" for i in range(100)), encoding="utf-8")
        from research_factory.ingest import fetch_and_segment_transcript

        with self.db_conn() as conn:
            result = fetch_and_segment_transcript(conn, transcript_row["episode_id"], label_pack="ai_discourse_v1", lane="podcast")
            conn.commit()
            self.assertEqual(result["segments"], 1)
            self.assertEqual(result["removed_stale_segments"], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE transcript_id = ?", (result["transcript_id"],)).fetchone()[0], 1)
            self.assertEqual(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM jobs
                    JOIN segments ON segments.id = jobs.target_id
                    WHERE jobs.job_type = 'label_segment'
                      AND segments.transcript_id = ?
                    """,
                    (result["transcript_id"],),
                ).fetchone()[0],
                1,
            )

    def test_privacy_scan_flags_absolute_paths(self) -> None:
        exports = self.root / "exports"
        exports.mkdir()
        (exports / "leak.json").write_text('{"path": "/Users/kolbydayley/private/file.txt"}', encoding="utf-8")
        result = self.run_cli("privacy-scan", "--path", str(exports))
        self.assertFalse(result["ok"])

    def test_transcript_parser_body_sniffs_json_and_plain_text_captions(self) -> None:
        from research_factory.text import transcript_to_text

        json_body = json.dumps(
            [
                {"start": 0.1, "end": 1.0, "text": "Hello from Substack JSON.", "words": [{"word": "Hello"}]},
                {"start": 1.0, "end": 2.0, "text": "Agents use tools."},
            ]
        )
        parsed_json = transcript_to_text(
            json_body,
            "binary/octet-stream",
            "https://substackcdn.com/video_upload/example/transcription.json?Expires=999",
        )
        self.assertEqual(parsed_json, "Hello from Substack JSON.\nAgents use tools.")
        self.assertNotIn('"start"', parsed_json)

        caption_body = """WEBVTT

00:00:00.000 --> 00:00:02.000
AGI timelines are changing.

00:00:02.000 --> 00:00:04.000
Agents can use tools.
"""
        parsed_caption = transcript_to_text(caption_body, "text/plain", "https://example.test/captions")
        self.assertEqual(parsed_caption, "AGI timelines are changing.\nAgents can use tools.")
        self.assertNotIn("-->", parsed_caption)

    def test_transcript_parser_strips_inline_timestamped_plain_text(self) -> None:
        from research_factory.text import transcript_to_text

        lines = [
            f"Speaker {index % 2}: 00:00:{index:02d} [00:00:{index:02d}] The official transcript line {index} discusses agents and production systems."
            for index in range(25)
        ]
        parsed = transcript_to_text("\n".join(lines), "text/markdown", "https://example.test/transcript.md")
        self.assertIn("The official transcript line 1", parsed)
        self.assertNotIn("00:00:", parsed)

    def test_transcript_parser_strips_html_timestamp_residue(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = "<html><body>" + "\n".join(
            f"<p>\\[Speaker {index % 3} 00:00:{index:02d}.00\\] The transcript line {index} discusses developer tooling, reliability, and operational adoption.</p>"
            for index in range(25)
        ) + "</body></html>"
        parsed = transcript_to_text(body, "text/html", "https://changelog.com/podcast/624/transcript")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://changelog.com/podcast/624/transcript")
        self.assertFalse(quality.quarantine)
        self.assertIn("The transcript line 1", parsed)
        self.assertNotIn("00:00:", parsed)

    def test_transcript_parser_strips_dash_prefixed_timestamp_residue(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = "\n".join(
            f"00:00:{index:02d} - The transcript line {index} discusses model economics, infrastructure costs, and deployment tradeoffs."
            for index in range(25)
        )
        parsed = transcript_to_text(body, "text/html", "https://lexfridman.com/example-transcript/")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://lexfridman.com/example-transcript/")
        self.assertFalse(quality.quarantine)
        self.assertIn("The transcript line 1", parsed)
        self.assertNotIn("00:00:", parsed)

    def test_transcript_parser_strips_short_mmss_timestamp_residue(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = "<html><body>" + "\n".join(
            f"<p>{index % 50:02d}:{index % 60:02d} The transcript line {index} discusses developer tools, adoption, and reliability.</p>"
            for index in range(25)
        ) + "</body></html>"
        parsed = transcript_to_text(body, "text/html", "https://example.test/episode/transcript")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://example.test/episode/transcript")
        self.assertFalse(quality.quarantine)
        self.assertIn("The transcript line 1", parsed)
        self.assertNotIn("01:01", parsed)

    def test_transcript_parser_strips_dense_inline_short_timestamps(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = "<html><body><p>" + " ".join(
            f"{index % 50:02d}:{index % 60:02d} The transcript sentence {index} discusses platform engineering, developer workflows, and reliability."
            for index in range(25)
        ) + "</p></body></html>"
        parsed = transcript_to_text(body, "text/html", "https://syntax.fm/show/example")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://syntax.fm/show/example")
        self.assertFalse(quality.quarantine)
        self.assertIn("The transcript sentence 1", parsed)
        self.assertNotRegex(parsed, r"\b\d{1,2}:\d{2}\b")

    def test_transcript_parser_strips_escaped_bracketed_timestamps(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = "<html><body>" + "\n".join(
            f"<p>\\[\\\\00:{index % 60:02d}:10.09\\\\\\] The transcript line {index} discusses platform reliability, release operations, and developer workflows.</p>"
            for index in range(25)
        ) + "</body></html>"
        parsed = transcript_to_text(body, "text/html", "https://changelog.com/podcast/example")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://changelog.com/podcast/example")
        self.assertFalse(quality.quarantine)
        self.assertIn("The transcript line 1", parsed)
        self.assertNotRegex(parsed, r"\b\d{1,2}:\d{2}:\d{2}")
        self.assertNotIn("\\[", parsed)

    def test_transcript_parser_extracts_econtalk_official_section(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p>Russ: This official transcript line {index} explains price signals, incentives, regulation, and markets.</p>"
            for index in range(24)
        )
        body = """
        <html>
          <body>
            <aside>Reader comments and page chrome should not enter the transcript.</aside>
            <div class="audio-highlight">
              <h3>Time</h3>
              <p>0:33</p>
              {transcript_lines}
              <p>(1:00:39) Guest: Another transcript line explains regulation and markets.</p>
            </div>
            <div id="comments">READER COMMENTS noisy user text</div>
          </body>
        </html>
        """.format(transcript_lines=transcript_lines)
        parsed = transcript_to_text(body, "text/html", "https://www.econtalk.org/example/")
        self.assertIn("price signals, incentives", parsed)
        self.assertIn("regulation and markets", parsed)
        self.assertNotIn("READER COMMENTS", parsed)
        self.assertNotIn("0:33", parsed)
        self.assertNotIn("1:00:39", parsed)
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://www.econtalk.org/example/")
        self.assertFalse(quality.quarantine)
        self.assertEqual(quality.issues, [])

    def test_transcript_parser_extracts_cognitive_revolution_official_section(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p>Speaker {index % 3}: 00:00:{index:02d} This official transcript line {index} explains robotics, benchmarks, model operations, and deployment risk.</p>"
            for index in range(25)
        )
        body = f"""
        <html>
          <body>
            <main class="post content">
              <p>Page chrome and related posts should not enter the transcript.</p>
              <section class="podcast-transcript">{transcript_lines}</section>
            </main>
            <aside>Related posts --></aside>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://www.cognitiverevolution.ai/example/")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://www.cognitiverevolution.ai/example/")
        self.assertIn("This official transcript line 1", parsed)
        self.assertNotIn("Page chrome", parsed)
        self.assertNotIn("00:00:", parsed)
        self.assertFalse(quality.quarantine)

    def test_transcript_parser_extracts_cognitive_revolution_transcript_heading(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p>Speaker {index % 3}: 00:00:{index:02d} This official transcript line {index} explains chips, robotics, benchmarks, and deployment risk.</p>"
            for index in range(25)
        )
        body = f"""
        <html>
          <body>
            <article class="post content">
              <p>Episode description should be excluded.</p>
              <h2 id="transcript">Transcript</h2>
              <p><em>Generated by the official podcast site.</em></p>
              <hr>
              <h2 id="introduction">Introduction</h2>
              {transcript_lines}
              <section class="post-tags">Tags and related posts should stop extraction --></section>
            </article>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://www.cognitiverevolution.ai/example/")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://www.cognitiverevolution.ai/example/")
        self.assertIn("This official transcript line 1", parsed)
        self.assertNotIn("Episode description", parsed)
        self.assertNotIn("related posts", parsed)
        self.assertNotIn("00:00:", parsed)
        self.assertFalse(quality.quarantine)

    def test_transcript_parser_extracts_substack_transcript_section(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p>Guest: This Substack transcript line {index} covers developer platforms, reliability, and deployment tradeoffs.</p>"
            for index in range(24)
        )
        body = f"""
        <html>
          <body>
            <header>Subscribe Sign in</header>
            <h1>Episode post</h1>
            <h2>Transcript</h2>
            {transcript_lines}
            <section>Comments</section>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://www.latent.space/p/example")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://www.latent.space/p/example")
        self.assertIn("Substack transcript line 1", parsed)
        self.assertNotIn("Subscribe Sign in", parsed)
        self.assertNotIn("Comments", parsed)
        self.assertFalse(quality.quarantine)

    def test_transcript_parser_extracts_verge_jsonld_article_body_when_sized_like_transcript(self) -> None:
        from research_factory.text import transcript_to_text

        article_body = " ".join(
            f"Speaker {index % 3} discusses platform policy cloud economics and hardware supply chain risk."
            for index in range(1150)
        )
        body = f"""
        <html><head>
          <script type="application/ld+json">{json.dumps({"@type": "NewsArticle", "articleBody": article_body})}</script>
        </head><body><nav>Subscribe and navigation chrome should be ignored.</nav></body></html>
        """
        parsed = transcript_to_text(body, "text/html", "https://www.theverge.com/podcast/961603/example")
        self.assertIn("platform policy cloud economics", parsed)
        self.assertNotIn("navigation chrome", parsed)
        self.assertGreaterEqual(len(parsed.split()), 1000)

    def test_transcript_parser_rejects_short_verge_article_body(self) -> None:
        from research_factory.text import transcript_to_text

        body = f"""
        <html><head>
          <script type="application/ld+json">{json.dumps({"@type": "NewsArticle", "articleBody": "Short show notes only."})}</script>
        </head><body><nav>Subscribe and navigation chrome should be ignored.</nav></body></html>
        """
        self.assertEqual(transcript_to_text(body, "text/html", "https://www.theverge.com/podcast/960810/example"), "")

    def test_transcript_parser_extracts_gcp_transcript_section(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p><strong>HOST:</strong> This GCP transcript line {index} covers cloud databases, networking, AI platforms, and reliability.</p>"
            for index in range(30)
        )
        body = f"""
        <html>
          <body>
            <nav>Global Google Cloud navigation should be ignored.</nav>
            <section id="transcript">
              <div class="transcript-title"><h5>Transcript</h5></div>
              {transcript_lines}
            </section>
            <footer>Footer links should be ignored.</footer>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://www.gcppodcast.com/post/episode-331-example/")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://www.gcppodcast.com/post/episode-331-example/")
        self.assertIn("GCP transcript line 1", parsed)
        self.assertNotIn("Global Google Cloud navigation", parsed)
        self.assertNotIn("Footer links", parsed)
        self.assertFalse(quality.quarantine)

    def test_transcript_parser_extracts_corecursive_transcript_class(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        transcript_lines = "\n".join(
            f"<p>Guest: This CoRecursive transcript line {index} covers programming history, compilers, AI systems, and engineering tradeoffs.</p>"
            for index in range(30)
        )
        body = f"""
        <html>
          <body>
            <aside>Table of contents should be ignored.</aside>
            <article class="post-text transcript">{transcript_lines}</article>
            <footer>Footer links should be ignored.</footer>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://corecursive.com/example/")
        quality = analyze_transcript_quality(parsed, content_type="text/html", source_url="https://corecursive.com/example/")
        self.assertIn("CoRecursive transcript line 1", parsed)
        self.assertNotIn("Table of contents", parsed)
        self.assertNotIn("Footer links", parsed)
        self.assertFalse(quality.quarantine)

    def test_transcript_parser_rejects_stackoverflow_blog_shell(self) -> None:
        from research_factory.text import analyze_transcript_quality, transcript_to_text

        body = """
        <html>
          <body>
            <h1>Is the enterprise ready for AI? - Stack Overflow Blog</h1>
            <p>Loading...</p>
            <nav>Everything The Heap Productivity AI/ML Open Source Business Hub Podcast Newsletter</nav>
            <article>This short article summary is not a full podcast transcript.</article>
          </body>
        </html>
        """
        parsed = transcript_to_text(body, "text/html", "https://stackoverflow.blog/2025/5/13/example/")
        self.assertEqual(parsed, "")

        stored_shell = "Is the enterprise ready for AI? - Stack Overflow Blog Loading... Everything The Heap Productivity AI/ML Open Source Business Hub Podcast Newsletter " * 4
        quality = analyze_transcript_quality(stored_shell, content_type="text/html", source_url="https://stackoverflow.blog/2025/5/13/example/")
        self.assertTrue(quality.quarantine)
        self.assertIn("official_page_boilerplate_shell", quality.issues)

    def test_stackoverflow_page_routes_to_simplecast_transcription(self) -> None:
        from research_factory.ingest import fetch_transcript_source

        page = """
        <html><body>
          <a href="https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript">Transcript</a>
        </body></html>
        """
        with patch("research_factory.ingest.fetch_url", return_value=(page, "text/html")) as fetch_url:
            with patch("research_factory.ingest._fetch_simplecast_transcription", return_value=("<p>Official transcript text.</p>", "text/html")) as fetch_simplecast:
                body, content_type = fetch_transcript_source(
                    "https://stackoverflow.blog/2026/07/01/example-episode/",
                    source_kind="official_show_transcript",
                    transcript_type="text/html",
                )
        self.assertEqual(body, "<p>Official transcript text.</p>")
        self.assertEqual(content_type, "text/html")
        fetch_url.assert_called_once()
        fetch_simplecast.assert_called_once_with("https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript")

    def test_simplecast_transcript_url_normalizes_to_episode_url(self) -> None:
        from research_factory.ingest import _simplecast_episode_url

        self.assertEqual(
            _simplecast_episode_url("https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript?ignored=1"),
            "https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode",
        )

    def test_simplecast_transcription_api_html_returns_plain_text(self) -> None:
        from research_factory.ingest import _fetch_simplecast_transcription

        class Response:
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                text = " ".join(
                    f"<p>The official transcript sentence {index} discusses developer platforms and reliability.</p>"
                    for index in range(24)
                )
                return json.dumps({"transcription": text}).encode("utf-8")

        with patch("urllib.request.urlopen", return_value=Response()):
            body, content_type = _fetch_simplecast_transcription(
                "https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript"
            )
        self.assertEqual(content_type, "text/plain")
        self.assertIn("official transcript sentence 1", body)
        self.assertNotIn("<p>", body)

    def test_stackoverflow_backlog_resolver_requires_simplecast_link(self) -> None:
        from research_factory.ingest import _resolve_backlog_transcript_url

        page = """
        <html><body>
          <a href="https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript">Transcript</a>
        </body></html>
        """
        with patch("research_factory.ingest.fetch_url", return_value=(page, "text/html")):
            resolved = _resolve_backlog_transcript_url(
                source_name="The Stack Overflow Podcast",
                transcript_url="https://stackoverflow.blog/2026/07/01/example-episode/",
                transcript_type="text/html",
                source_kind="official_show_transcript",
            )
        self.assertTrue(resolved["ok"])
        self.assertEqual(
            resolved["transcript_url"],
            "https://the-stack-overflow-podcast.simplecast.com/episodes/example-episode/transcript",
        )

        with patch("research_factory.ingest.fetch_url", return_value=("<html><body>Show notes only.</body></html>", "text/html")):
            missing = _resolve_backlog_transcript_url(
                source_name="The Stack Overflow Podcast",
                transcript_url="https://stackoverflow.blog/2026/07/02/no-transcript/",
                transcript_type="text/html",
                source_kind="official_show_transcript",
            )
        self.assertFalse(missing["ok"])
        self.assertEqual(missing["reason"], "official_transcript_link_not_found")

    def test_feed_parser_prefers_vtt_and_finds_rss_body_transcript_links(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:podcast="https://podcastindex.org/namespace/1.0"
  xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <item>
      <guid>multi</guid>
      <title>Multiple transcript formats</title>
      <podcast:transcript url="https://example.test/transcript.srt" type="application/srt" />
      <podcast:transcript url="https://example.test/transcript.vtt" type="text/vtt" />
    </item>
    <item>
      <guid>body-link</guid>
      <title>Body transcript link</title>
      <content:encoded><![CDATA[
        <p>Show notes.</p>
        <a href="https://example.test/episode-transcript.txt">Download transcript</a>
      ]]></content:encoded>
    </item>
  </channel>
</rss>
"""
        episodes = parse_feed(feed, source_id="src")
        self.assertEqual(episodes[0]["transcript_url"], "https://example.test/transcript.vtt")
        self.assertEqual(episodes[0]["transcript_type"], "text/vtt")
        self.assertEqual(episodes[1]["transcript_url"], "https://example.test/episode-transcript.txt")
        self.assertEqual(episodes[1]["transcript_type"], "text/plain")

    def test_feed_parser_derives_ai_daily_brief_markdown_transcript(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><item>
  <guid>ai-daily-1</guid>
  <title>The Big Ways AI Just Changed</title>
  <link>https://podcasters.spotify.com/pod/show/nlw/episodes/The-Big-Ways-AI-Just-Changed-e3lkje9</link>
  <pubDate>Sat, 04 Jul 2026 01:17:29 GMT</pubDate>
</item><item>
  <guid>ai-daily-old</guid>
  <title>Older Episode Without Markdown Archive</title>
  <link>https://podcasters.spotify.com/pod/show/nlw/episodes/older</link>
  <pubDate>Wed, 20 May 2026 20:00:00 GMT</pubDate>
</item></channel></rss>
"""
        episodes = parse_feed(feed, source_id="the-ai-daily-brief")
        self.assertIsNone(episodes[0]["feed_transcript_url"])
        self.assertEqual(episodes[0]["verified_transcript_url"], "https://aidailybrief.ai/e/2026-07-03/transcript.md")
        self.assertEqual(episodes[0]["verified_transcript_type"], "text/markdown")
        self.assertEqual(episodes[0]["verified_transcript_source_kind"], "official_show_transcript")
        self.assertEqual(episodes[0]["transcript_url"], "https://aidailybrief.ai/e/2026-07-03/transcript.md")
        self.assertIsNone(episodes[1]["verified_transcript_url"])
        self.assertIsNone(episodes[1]["transcript_url"])

    def test_feed_parser_promotes_direct_substack_post_transcript(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><item>
  <guid>latent-direct-post</guid>
  <title>Agents in Production</title>
  <link>https://www.latent.space/p/agents-in-production</link>
  <pubDate>Sat, 04 Jul 2026 01:17:29 GMT</pubDate>
</item></channel></rss>
"""
        episodes = parse_feed(feed, source_id="latent-space")
        self.assertEqual(episodes[0]["verified_transcript_url"], "https://www.latent.space/p/agents-in-production")
        self.assertEqual(episodes[0]["verified_transcript_type"], "text/html")
        self.assertEqual(episodes[0]["verified_transcript_source_kind"], "official_show_transcript")
        self.assertEqual(episodes[0]["transcript_url"], "https://www.latent.space/p/agents-in-production")

    def test_feed_parser_promotes_direct_official_episode_pages(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><item>
  <guid>corecursive-direct-post</guid>
  <title>Compiler Stories</title>
  <link>https://corecursive.com/compiler-stories/</link>
  <pubDate>Sat, 04 Jul 2026 01:17:29 GMT</pubDate>
</item></channel></rss>
"""
        episodes = parse_feed(feed, source_id="corecursive")
        self.assertEqual(episodes[0]["verified_transcript_url"], "https://corecursive.com/compiler-stories/")
        self.assertEqual(episodes[0]["verified_transcript_type"], "text/html")
        self.assertEqual(episodes[0]["verified_transcript_source_kind"], "official_show_transcript")

    def test_feed_parser_uses_explicit_lex_transcript_link_without_slug_guessing(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><item>
  <guid>lex-old</guid>
  <title>#455 - Adam Frank</title>
  <link>https://lexfridman.com/adam-frank/?utm_source=rss</link>
  <description><![CDATA[
    <p><b>Transcript:</b><br />
    <a href="https://lexfridman.com/adam-frank-transcript/">https://lexfridman.com/adam-frank-transcript/</a></p>
  ]]></description>
  <pubDate>Sat, 04 Jul 2026 01:17:29 GMT</pubDate>
</item><item>
  <guid>lex-no-transcript</guid>
  <title>#100 - Older Episode</title>
  <link>https://lexfridman.com/older-episode/?utm_source=rss</link>
  <pubDate>Sat, 04 Jul 2020 01:17:29 GMT</pubDate>
</item></channel></rss>
"""
        episodes = parse_feed(feed, source_id="lex-fridman-podcast")
        self.assertEqual(episodes[0]["transcript_url"], "https://lexfridman.com/adam-frank-transcript/")
        self.assertIsNone(episodes[0]["verified_transcript_url"])
        self.assertIsNone(episodes[1]["transcript_url"])

    def test_feed_parser_promotes_verge_atom_article_transcript_page(self) -> None:
        from research_factory.ingest import parse_feed

        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title type="html"><![CDATA[Inside the big business of the creator economy]]></title>
    <link rel="alternate" type="text/html" href="https://www.theverge.com/podcast/961603/example" />
    <id>https://www.theverge.com/?p=961603</id>
    <published>2026-07-06T10:30:00-04:00</published>
    <content type="html"><![CDATA[Official article body.]]></content>
  </entry>
</feed>
"""
        episodes = parse_feed(feed, source_id="decoder-with-nilay-patel")
        self.assertEqual(episodes[0]["url"], "https://www.theverge.com/podcast/961603/example")
        self.assertEqual(episodes[0]["verified_transcript_url"], "https://www.theverge.com/podcast/961603/example")
        self.assertEqual(episodes[0]["verified_transcript_type"], "text/html")
        self.assertEqual(episodes[0]["verified_transcript_source_kind"], "official_show_transcript")

    def test_sync_verge_official_transcripts_matches_existing_audio_episode(self) -> None:
        from research_factory import db
        from research_factory.ingest import sync_verge_official_transcripts

        conn = db.connect(self.root / "verge-sync.sqlite")
        db.init_db(conn)
        ts = "2026-07-09T00:00:00+00:00"
        conn.execute(
            """
            INSERT INTO sources
              (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
            VALUES ('decoder-with-nilay-patel', 'Decoder with Nilay Patel', 'https://feeds.megaphone.fm/recodedecode', NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
            """,
            (ts, ts),
        )
        conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, description, url, audio_url, published_at, duration_seconds, created_at, updated_at)
            VALUES ('ep_verge_audio', 'decoder-with-nilay-patel', 'audio-guid', 'Inside the big business of the creator economy, with Ali Berman and Raina Penchansky', NULL, NULL, NULL, '2026-07-06T09:00:00+00:00', 3600, ?, ?)
            """,
            (ts, ts),
        )
        conn.commit()
        feed = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title type="html"><![CDATA[Inside the big business of the creator economy, with the agents making it happen]]></title>
    <link rel="alternate" type="text/html" href="https://www.theverge.com/podcast/961603/example" />
    <id>https://www.theverge.com/?p=961603</id>
    <published>2026-07-06T10:30:00-04:00</published>
  </entry>
</feed>
"""
        article_body = " ".join(
            f"Speaker {index % 3} describes creator economy platforms agencies marketing economics and policy."
            for index in range(1100)
        )
        page = f"<html><head><script type=\"application/ld+json\">{json.dumps({'@type': 'NewsArticle', 'articleBody': article_body})}</script></head></html>"

        def fake_fetch(url: str) -> tuple[str, str | None]:
            if url.endswith("/rss/decoder-podcast-with-nilay-patel/index.xml"):
                return feed, "application/atom+xml"
            if url == "https://www.theverge.com/podcast/961603/example":
                return page, "text/html"
            raise AssertionError(url)

        with patch("research_factory.ingest.fetch_url", side_effect=fake_fetch):
            result = sync_verge_official_transcripts(
                conn,
                lane="podcast",
                label_pack="ai_discourse_v3_1",
                source_filter=["decoder-with-nilay-patel"],
                limit=5,
            )
        self.assertEqual(result["attached"], 1)
        self.assertEqual(result["fetch_jobs"], 1)
        episode = conn.execute("SELECT verified_transcript_url, transcript_url FROM episodes WHERE id = 'ep_verge_audio'").fetchone()
        self.assertEqual(episode["verified_transcript_url"], "https://www.theverge.com/podcast/961603/example")
        job = conn.execute("SELECT status, payload_json FROM jobs WHERE target_id = 'ep_verge_audio' AND job_type = 'fetch_transcript'").fetchone()
        self.assertEqual(job["status"], "pending")
        self.assertEqual(json.loads(job["payload_json"])["source_kind"], "official_show_transcript")

    def test_sync_gcp_official_transcripts_matches_archive_page_to_existing_episode(self) -> None:
        from research_factory import db
        from research_factory.ingest import sync_gcp_official_transcripts

        conn = db.connect(self.root / "gcp-sync.sqlite")
        db.init_db(conn)
        ts = "2026-07-09T00:00:00+00:00"
        conn.execute(
            """
            INSERT INTO sources
              (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
            VALUES ('google-cloud-platform-podcast', 'Google Cloud Platform Podcast', 'https://rss.libsyn.com/shows/385982/destinations/3174422.xml', NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
            """,
            (ts, ts),
        )
        conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, description, url, audio_url, published_at, duration_seconds, created_at, updated_at)
            VALUES ('ep_gcp_audio', 'google-cloud-platform-podcast', 'audio-guid', '2022 Year End Wrap Up', NULL, NULL, NULL, '2022-12-21T12:00:00+00:00', 3600, ?, ?)
            """,
            (ts, ts),
        )
        conn.commit()
        archive = """
        <html><body>
          <a href="../post/episode-331-2022-year-end-wrap-up/">2022 Year End Wrap Up</a>
        </body></html>
        """
        transcript_lines = "\n".join(
            f"<p><strong>HOST:</strong> This official GCP archive transcript line {index} covers cloud, databases, AI platforms, networking, and reliability.</p>"
            for index in range(35)
        )
        page = f"""
        <html><body>
          <h1 class="sr">2022 Year End Wrap Up</h1>
          <section id="transcript">{transcript_lines}</section>
        </body></html>
        """

        def fake_fetch(url: str) -> tuple[str, str | None]:
            if url == "https://www.gcppodcast.com/post/":
                return archive, "text/html"
            if url == "https://www.gcppodcast.com/post/episode-331-2022-year-end-wrap-up/":
                return page, "text/html"
            raise AssertionError(url)

        with patch("research_factory.ingest.fetch_url", side_effect=fake_fetch):
            result = sync_gcp_official_transcripts(
                conn,
                lane="podcast",
                label_pack="ai_discourse_v3_1",
                limit=5,
                max_archive_pages=1,
            )
        self.assertEqual(result["attached"], 1)
        self.assertEqual(result["fetch_jobs"], 1)
        episode = conn.execute("SELECT verified_transcript_url, transcript_url FROM episodes WHERE id = 'ep_gcp_audio'").fetchone()
        self.assertEqual(episode["verified_transcript_url"], "https://www.gcppodcast.com/post/episode-331-2022-year-end-wrap-up/")
        job = conn.execute("SELECT status, payload_json FROM jobs WHERE target_id = 'ep_gcp_audio' AND job_type = 'fetch_transcript'").fetchone()
        self.assertEqual(job["status"], "pending")
        self.assertEqual(json.loads(job["payload_json"])["source_kind"], "official_show_transcript")

    def test_backfill_ai_daily_brief_enqueues_official_markdown_transcript(self) -> None:
        feed = self.root / "ai-daily-feed.xml"
        feed.write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><item>
  <guid>ai-daily-1</guid>
  <title>The Big Ways AI Just Changed</title>
  <link>https://podcasters.spotify.com/pod/show/nlw/episodes/The-Big-Ways-AI-Just-Changed-e3lkje9</link>
  <pubDate>Sat, 04 Jul 2026 01:17:29 GMT</pubDate>
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "ai-daily-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: The AI Daily Brief
    rss_url: {feed}
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        result = self.run_cli(
            "backfill-episodes",
            "--source-list",
            str(sources),
            "--source",
            "The AI Daily Brief",
            "--enqueue-transcript-jobs",
            "--label-pack",
            "ai_discourse_v3_1",
        )
        self.assertEqual(result["transcript_jobs"], 1)
        with self.db_conn() as conn:
            episode = conn.execute("SELECT transcript_url, verified_transcript_url, verified_transcript_source_kind FROM episodes").fetchone()
            job = conn.execute("SELECT payload_json FROM jobs WHERE job_type = 'fetch_transcript'").fetchone()
        self.assertEqual(episode["transcript_url"], "https://aidailybrief.ai/e/2026-07-03/transcript.md")
        self.assertEqual(episode["verified_transcript_url"], "https://aidailybrief.ai/e/2026-07-03/transcript.md")
        self.assertEqual(episode["verified_transcript_source_kind"], "official_show_transcript")
        self.assertEqual(json.loads(job["payload_json"])["source_kind"], "official_show_transcript")
        second = self.run_cli(
            "backfill-episodes",
            "--source-list",
            str(sources),
            "--source",
            "The AI Daily Brief",
            "--enqueue-transcript-jobs",
            "--label-pack",
            "ai_discourse_v3_1",
        )
        self.assertEqual(second["transcript_jobs"], 0)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'fetch_transcript'").fetchone()[0], 1)

    def test_enqueue_transcript_backlog_refills_known_links_without_youtube_by_default(self) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_backlog', 'Backlog Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            rows = [
                ("ep_vtt", "Known VTT", "https://example.test/one.vtt", "text/vtt"),
                ("ep_stale", "Known Stale", "https://example.test/stale.vtt", "text/vtt"),
                ("ep_shell", "Known Shell", "https://example.test/shell-transcript", "text/html"),
                ("ep_forbidden", "Known Forbidden", "https://example.test/forbidden-transcript", "text/html"),
                ("ep_youtube", "Known Video", "https://www.youtube.com/watch?v=abc123", "text/html"),
                ("ep_quarantined", "Known Quarantined", "https://example.test/quarantined.vtt", "text/vtt"),
            ]
            for episode_id, title, transcript_url, transcript_type in rows:
                conn.execute(
                    """
                    INSERT INTO episodes
                      (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                    VALUES (?, 'src_backlog', ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (episode_id, episode_id, title, transcript_url, transcript_type, transcript_url, transcript_type, ts, ts),
                )
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_quarantined', 'ep_quarantined', 'creator_provided_rss_transcript', 'https://example.test/quarantined.vtt', 'text/vtt', '/tmp/quarantined.txt', 'sha-quarantined', 'quarantined', ?, '{}', 10, ?, ?)
                """,
                (ts, ts, ts),
            )
            conn.commit()
        result = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10")
        self.assertEqual(result["enqueued"], 4)
        self.assertEqual(result["by_source_kind"], {"creator_provided_rss_transcript": 2, "official_show_transcript": 2})
        self.assertEqual(result["skipped"][0]["reason"], "youtube_caption_lane_disabled")
        with self.db_conn() as conn:
            vtt_job = conn.execute("SELECT id FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_vtt'").fetchone()
            stale_job = conn.execute("SELECT id FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_stale'").fetchone()
            shell_job = conn.execute("SELECT id FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_shell'").fetchone()
            forbidden_job = conn.execute("SELECT id FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_forbidden'").fetchone()
            conn.execute("UPDATE jobs SET status = 'failed', attempts = 2, error = 'transient' WHERE id = ?", (vtt_job["id"],))
            conn.execute(
                "UPDATE jobs SET status = 'failed', attempts = max_attempts, error = 'HTTP Error 404: Not Found' WHERE id = ?",
                (stale_job["id"],),
            )
            conn.execute(
                "UPDATE jobs SET status = 'failed', attempts = max_attempts, error = 'Transcript too short after parsing: ep_shell' WHERE id = ?",
                (shell_job["id"],),
            )
            conn.execute(
                "UPDATE jobs SET status = 'failed', attempts = max_attempts, error = 'HTTP Error 403: Forbidden' WHERE id = ?",
                (forbidden_job["id"],),
            )
            conn.commit()
        retried = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10")
        self.assertEqual(retried["enqueued"], 1)
        with self.db_conn() as conn:
            job = conn.execute("SELECT status, attempts, error FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_vtt'").fetchone()
            stale = conn.execute("SELECT status, attempts, error FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_stale'").fetchone()
            shell = conn.execute("SELECT status, attempts, error FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_shell'").fetchone()
            forbidden = conn.execute("SELECT status, attempts, error FROM jobs WHERE job_type = 'fetch_transcript' AND target_id = 'ep_forbidden'").fetchone()
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["error"])
        self.assertEqual(stale["status"], "failed")
        self.assertEqual(stale["error"], "HTTP Error 404: Not Found")
        self.assertEqual(shell["status"], "failed")
        self.assertEqual(shell["error"], "Transcript too short after parsing: ep_shell")
        self.assertEqual(forbidden["status"], "failed")
        self.assertEqual(forbidden["error"], "HTTP Error 403: Forbidden")

    def test_enqueue_transcript_backlog_skips_strategy_lanes_not_ready(self) -> None:
        config_dir = self.root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "transcript_strategies.json").write_text(
            json.dumps(
                {
                    "strategies": [
                        {
                            "id": "adapter_pending",
                            "lane": "official_archive_discovery",
                            "route_type": "official_episode_page",
                            "status": "needs_adapter",
                            "sources": ["The Stack Overflow Podcast"],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('the-stack-overflow-podcast', 'The Stack Overflow Podcast', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_stack_pending', 'the-stack-overflow-podcast', 'ep_stack_pending', 'Adapter Pending', 'https://stackoverflow.blog/example-transcript', 'text/html', 'https://stackoverflow.blog/example-transcript', 'text/html', ?, ?)
                """,
                (ts, ts),
            )
            conn.commit()
        result = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10")
        self.assertEqual(result["enqueued"], 0)
        self.assertEqual(result["skipped"][0]["reason"], "strategy_not_ready")
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'fetch_transcript'").fetchone()[0], 0)

    def test_enqueue_transcript_backlog_requeues_completed_fetch_when_transcript_missing(self) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_completed_missing', 'Completed Missing Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_completed_missing', 'src_completed_missing', 'ep_completed_missing', 'Completed Missing', 'https://example.test/missing.vtt', 'text/vtt', 'https://example.test/missing.vtt', 'text/vtt', ?, ?)
                """,
                (ts, ts),
            )
            conn.commit()
        first = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10", "--priority", "42")
        self.assertEqual(first["enqueued"], 1)
        with self.db_conn() as conn:
            conn.execute("UPDATE jobs SET status = 'completed', attempts = 1 WHERE job_type = 'fetch_transcript'")
            conn.commit()
        second = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10", "--priority", "24")
        self.assertEqual(second["enqueued"], 1)
        with self.db_conn() as conn:
            job = conn.execute("SELECT status, attempts, priority, error FROM jobs WHERE job_type = 'fetch_transcript'").fetchone()
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempts"], 0)
        self.assertEqual(job["priority"], 24)
        self.assertIsNone(job["error"])

    def test_enqueue_transcript_backlog_can_retry_quarantined_when_explicit(self) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_quarantine_retry', 'Quarantine Retry Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_quarantine_retry', 'src_quarantine_retry', 'ep_quarantine_retry', 'Quarantine Retry', 'https://example.test/retry.vtt', 'text/vtt', 'https://example.test/retry.vtt', 'text/vtt', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_quarantine_retry', 'ep_quarantine_retry', 'creator_provided_rss_transcript', 'https://example.test/retry.vtt', 'text/vtt', '/tmp/retry.txt', 'sha-retry', 'quarantined', ?, '{}', 10, ?, ?)
                """,
                (ts, ts, ts),
            )
            conn.commit()
        default = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10")
        self.assertEqual(default["enqueued"], 0)
        retry = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10", "--include-quarantined-retry")
        self.assertEqual(retry["enqueued"], 1)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'fetch_transcript' AND status = 'pending'").fetchone()[0], 1)

    def test_enqueue_transcript_backlog_can_retry_parser_failed_when_explicit(self) -> None:
        from research_factory import db

        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_parser_retry', 'Parser Retry Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_parser_retry', 'src_parser_retry', 'ep_parser_retry', 'Parser Retry', 'https://example.test/retry-page', 'text/html', 'https://example.test/retry-page', 'text/html', ?, ?)
                """,
                (ts, ts),
            )
            job_id = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="fetch_transcript",
                target_id="ep_parser_retry",
                payload={"label_pack": "ai_discourse_v3_1", "source_kind": "official_show_transcript"},
                priority=35,
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    attempts = max_attempts,
                    error = 'Transcript too short after parsing: ep_parser_retry'
                WHERE id = ?
                """,
                (job_id,),
            )
            conn.commit()
        default = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10")
        self.assertEqual(default["enqueued"], 0)
        retry = self.run_cli("enqueue-transcript-backlog", "--lane", "podcast", "--limit", "10", "--include-quarantined-retry")
        self.assertEqual(retry["enqueued"], 1)
        with self.db_conn() as conn:
            job = conn.execute("SELECT status, attempts, error FROM jobs WHERE id = ?", (job_id,)).fetchone()
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["error"])

    def test_enqueue_transcript_backlog_does_not_retry_missing_simplecast_payload(self) -> None:
        from research_factory import db

        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_simplecast_missing', 'Simplecast Missing Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_simplecast_missing', 'src_simplecast_missing', 'ep_simplecast_missing', 'Simplecast Missing', 'https://example.test/retry-page', 'text/html', 'https://example.test/retry-page', 'text/html', ?, ?)
                """,
                (ts, ts),
            )
            job_id = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="fetch_transcript",
                target_id="ep_simplecast_missing",
                payload={"label_pack": "ai_discourse_v3_1", "source_kind": "official_show_transcript"},
                priority=35,
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    attempts = max_attempts,
                    error = 'Simplecast transcript missing or too short'
                WHERE id = ?
                """,
                (job_id,),
            )
            conn.commit()
        result = self.run_cli(
            "enqueue-transcript-backlog",
            "--lane",
            "podcast",
            "--limit",
            "10",
            "--include-quarantined-retry",
        )
        self.assertEqual(result["enqueued"], 0)

    def test_transcript_strategy_report_loads_catalog_and_counts_sources(self) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('talk-python-to-me', 'Talk Python To Me', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, feed_transcript_url, feed_transcript_type, transcript_url, transcript_type, created_at, updated_at)
                VALUES ('ep_strategy', 'talk-python-to-me', 'ep_strategy', 'Strategy Fixture', 'https://example.test/transcript.vtt', 'text/vtt', 'https://example.test/transcript.vtt', 'text/vtt', ?, ?)
                """,
                (ts, ts),
            )
            conn.commit()
        report = self.run_cli("transcript-strategy-report")
        rss_strategy = next(item for item in report["strategies"] if item["id"] == "rss_direct_transcript_backlog")
        self.assertGreaterEqual(rss_strategy["counts"]["feed_transcript_links"], 1)
        self.assertEqual(report["privacy"], "sanitized_operational_report_no_raw_transcripts")

    def test_prepare_transcript_text_classifies_and_strips_mixed_page(self) -> None:
        from research_factory.prep import prepare_transcript_text

        text = """Subscribe Sign in
Latent Space: The AI Engineer Podcast
Audio playback is not supported on your browser.
Transcript
HOST: Today we are talking about OpenAI agents and enterprise AI workflows.
GUEST: The important shift is that coding agents will move from demos to production workflows.
HOST: Does that mean companies are changing budgets, procurement, and release planning around these systems?
GUEST: Yes, the adoption pattern is moving toward production deployments where evals, safety checks, and workflow integration matter.
Full show notes always on https://example.test/show
"""
        prepared = prepare_transcript_text(
            text,
            content_type="text/html",
            source_kind="official_show_transcript",
            source_name="Latent Space",
            episode_title="Agents Episode",
        )
        self.assertEqual(prepared.artifact_type, "mixed_page")
        self.assertEqual(prepared.status, "prepared")
        self.assertIn("coding agents will move", prepared.cleaned_text)
        self.assertNotIn("Subscribe Sign in", prepared.cleaned_text)
        self.assertGreater(prepared.boilerplate_ratio, 0)

    def test_v2_prepare_enqueue_local_draft_normalizes_dense_observations(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v2",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "12",
            "--pilot-id",
            "pilot-v2-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)
        self.assertTrue(prepared["artifact_counts"])

        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v2",
            "--local-draft",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            label = conn.execute("SELECT id, output_json FROM labels WHERE label_pack = 'ai_discourse_v2'").fetchone()
            self.assertIsNotNone(label)
            output = json.loads(label["output_json"])
            self.assertEqual(output["schema_version"], "ai_discourse_v2")
            self.assertGreaterEqual(len(output["observations"]), 3)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM coded_observations").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM topic_mentions").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0], 0)

        from research_factory.observer import _coding_metrics, _derived_table_counts, _trend_metrics

        with self.db_conn() as conn:
            derived = _derived_table_counts(conn)
            coding = _coding_metrics(conn)
            local_trend = _trend_metrics(conn)
        self.assertGreater(derived["coded_observations"], 0)
        self.assertGreater(coding["v2_observations_per_1000_segment_words"], 0)
        self.assertIn("label_velocity", local_trend)
        self.assertIn("source_productivity", local_trend)
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))
        trend = self.run_cli("export", "trend-report", "--topic", "agents", "--window", "month")
        self.assertIn("Trend Report", Path(trend["path"]).read_text(encoding="utf-8"))

    def test_v3_dynamic_discourse_local_draft_discovers_signals(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
            "--pilot-id",
            "pilot-v3-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)

        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v3",
            "--local-draft",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            label = conn.execute("SELECT id, output_json FROM labels WHERE label_pack = 'ai_discourse_v3'").fetchone()
            self.assertIsNotNone(label)
            output = json.loads(label["output_json"])
            self.assertEqual(output["schema_version"], "ai_discourse_v3")
            self.assertGreaterEqual(len(output["discourse_events"]), 4)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM term_usages").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM actor_positions").fetchone()[0], 0)
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM concepts").fetchone()[0], 0)

        discovered = self.run_cli("discover-concepts", "--window", "month", "--min-evidence", "1", "--min-source-diversity", "1")
        self.assertGreaterEqual(discovered["concepts_seen"], 1)
        shifts = self.run_cli("detect-shifts", "--window", "month", "--min-support", "1")
        self.assertGreaterEqual(shifts["signals"], 1)

        signal_report = self.run_cli("export", "signal-report", "--window", "month", "--limit", "10")
        self.assertIn("Discourse Signal Report", Path(signal_report["path"]).read_text(encoding="utf-8"))
        stance_report = self.run_cli("export", "actor-stance-report", "--window", "month")
        self.assertIn("Claim Subject Expert Stance Report", Path(stance_report["path"]).read_text(encoding="utf-8"))
        drift_report = self.run_cli("export", "term-drift-report", "--window", "month")
        self.assertIn("Term Drift Report", Path(drift_report["path"]).read_text(encoding="utf-8"))
        narrative = self.run_cli("export", "narrative-map", "--window", "month")
        self.assertTrue(Path(narrative["path"]).exists())

        from research_factory.observer import _coding_metrics, _derived_table_counts, _useful_signal_metrics

        with self.db_conn() as conn:
            coding = _coding_metrics(conn)
            derived = _derived_table_counts(conn)
            signals = _useful_signal_metrics(conn)
        self.assertGreater(coding["v3_discourse_events"], 0)
        self.assertGreater(derived["discourse_events"], 0)
        self.assertIsInstance(signals, dict)
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))

    def test_v31_requires_gpt55_episode_context_before_segment_prompt_and_persists_identity_mentions(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")
        prepared = self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
            "--pilot-id",
            "pilot-v31-test",
        )
        self.assertEqual(prepared["prepared"], 1)
        self.assertGreaterEqual(prepared["label_jobs"], 1)

        local_draft = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "label_segment",
            "--label-pack",
            "ai_discourse_v3_1",
            "--local-draft",
        )
        self.assertEqual(local_draft["completed"], 0)
        self.assertEqual(local_draft["failed"], 1)
        self.assertIn("GPT-5.5 full-episode extraction", local_draft["details"][0]["error"])

        waiting = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        self.assertEqual(waiting["status"], "waiting_for_episode_context")
        self.assertIn("episode_context_job_id", waiting)

        context_claim = self.run_cli("claim-context", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-context-test")
        self.assertIn("job_id", context_claim)
        context_prompt_text = Path(context_claim["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("Full Prepared Episode Transcript", context_prompt_text)
        self.assertIn("===== SEGMENT", context_prompt_text)

        with self.db_conn() as conn:
            context_job = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (context_claim["job_id"],)).fetchone()
            context_output = {
                "schema_version": "ai_discourse_v3_1_episode_context",
                "episode_id": context_job["target_id"],
                "context_summary": "A fixture AI podcast episode about AGI timelines, agents, coding agents, inference scaling, enterprise AI, and singularity terminology.",
                "speaker_map": [
                    {
                        "surface": "Alex Rivera",
                        "role": "guest",
                        "affiliation": None,
                        "aliases": ["The guest"],
                        "confidence": 0.72,
                        "rationale": "The transcript says the guest makes the AGI timeline claim.",
                    },
                    {
                        "surface": "host",
                        "role": "host",
                        "affiliation": None,
                        "aliases": ["The host"],
                        "confidence": 0.72,
                        "rationale": "The transcript says the host asks about terminology replacement.",
                    },
                ],
                "section_map": [
                    {
                        "section": "agentic AI and AGI timeline framing",
                        "segment_ids": [],
                        "summary": "The dialogue links agents and tool use to AGI timeline shifts.",
                    }
                ],
                "entity_seed": {
                    "people": [],
                    "organizations": [],
                    "products": [],
                    "models": [],
                    "terms": ["AGI", "agents", "coding agents", "inference scaling", "enterprise AI", "singularity"],
                },
                "concept_seed": [
                    {
                        "candidate_concept": "agentic_ai_timeline_mechanisms",
                        "surface_terms": ["AGI", "agents", "tools"],
                        "why_useful": "Tracks whether agentic tool use is being used to reframe AGI timelines.",
                    }
                ],
                "extraction_guidance": "Prioritize causal mechanisms, terminology drift between AGI and singularity, and actor-specific stance around agentic systems.",
                "quality_flags": [],
                "overall_confidence": 0.84,
                "needs_review": False,
                "review_reason": None,
            }
        Path(context_claim["output_path"]).write_text(json.dumps(context_output), encoding="utf-8")
        submitted_context = self.run_cli(
            "submit-context",
            "--job-id",
            str(context_claim["job_id"]),
            "--output-json",
            context_claim["output_path"],
            "--worker-id",
            "codex-context-test",
        )
        self.assertIn("episode_context_run_id", submitted_context)

        claim = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        self.assertIn("job_id", claim)
        prompt_text = Path(claim["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("episode_context_artifact", prompt_text)
        self.assertIn("speaker_map", prompt_text)
        self.assertNotIn("full_segmented_episode_text", prompt_text)
        self.assertNotIn("Full Prepared Episode Transcript", prompt_text)

        with self.db_conn() as conn:
            job = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (claim["job_id"],)).fetchone()
            segment = conn.execute("SELECT episode_id, text_path FROM segments WHERE id = ?", (job["target_id"],)).fetchone()
            segment_text = (self.root / segment["text_path"]).read_text(encoding="utf-8")
            evidence = "AGI timelines are changing because agents can use tools"
            start = segment_text.find(evidence)
            self.assertGreaterEqual(start, 0)
            end = start + len(evidence)
            output = {
                "schema_version": "ai_discourse_v3_1",
                "segment_id": job["target_id"],
                "episode_id": segment["episode_id"],
                "extraction_status": "coded",
                "segment_quality": {
                    "artifact_type": "dialogue_transcript",
                    "boilerplate_risk": "low",
                    "substantive_word_count": len(segment_text.split()),
                    "transcript_preparation_id": None,
                },
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "The segment contains substantive discussion from the transcript.",
                },
                "discourse_events": [
                    {
                        "event_type": "causal_mechanism",
                        "event_subtype": "agentic_capability_timeline_mechanism",
                        "actor": {"name": "Alex Rivera", "actor_type": "guest", "affiliation": None, "role": "speaker"},
                        "speaker_context": {"name": "Alex Rivera", "role": "guest", "affiliation": None, "confidence": 0.7},
                        "reported_actor": {"name": "agents", "actor_type": "model", "affiliation": None, "confidence": 0.7},
                        "source_context": {"kind": "substantive_dialogue", "confidence": 0.9, "rationale": "The evidence is from substantive transcript content."},
                        "target": {"raw_target": "AGI timelines and tool-using agents", "candidate_concept": "agentic_ai_timeline_mechanisms", "canonical_concept": None, "concept_confidence": 0.8},
                        "surface_terms": ["AGI", "agents", "tools"],
                        "frames": ["capability_timeline", "tool_use_mechanism"],
                        "model_names": [],
                        "product_names": [],
                        "organizations": [],
                        "people": ["Sam Altman"],
                        "stance": "neutral",
                        "claim_text": "The guest links changing AGI timelines to agents gaining tool-use capability.",
                        "claim_type": "causal",
                        "certainty": "medium",
                        "temporal_horizon": "present",
                        "causal_mechanism": "Agents can use tools, which is presented as a reason AGI timelines are changing.",
                        "counterclaim": "",
                        "metric": {"value": None, "unit": None, "comparator": None, "direction": "not_applicable", "raw_text": None},
                        "signal_reason": "This supports analysis of how agentic tool use changes AGI timeline framing over time.",
                        "exclusion_flags": [],
                        "quality_flags": [],
                        "evidence": evidence,
                        "evidence_start": start,
                        "evidence_end": end,
                        "confidence": 0.82,
                        "audit_notes": "GPT-style fixture output for v3.1 persistence.",
                    }
                ],
                "concept_candidates": [
                    {
                        "candidate": "agentic_ai_timeline_mechanisms",
                        "surface_terms": ["AGI", "agents", "tools"],
                        "rationale": "Tracks claims that agentic tool use changes AGI timeline expectations.",
                        "usefulness_score": 0.82,
                        "evidence": evidence,
                        "evidence_start": start,
                        "evidence_end": end,
                        "confidence": 0.82,
                    }
                ],
                "rejected_candidates": [],
                "no_signal_reason": None,
                "overall_confidence": 0.82,
                "needs_review": True,
                "review_reason": "High-impact AGI timeline mechanism.",
            }
        Path(claim["output_path"]).write_text(json.dumps(output), encoding="utf-8")
        submitted = self.run_cli("submit", "--job-id", str(claim["job_id"]), "--output-json", claim["output_path"], "--worker-id", "codex-test")
        self.assertIn("label_id", submitted)
        report = self.run_cli("scale-gate-report", "--pilot-id", "pilot-v31-test")
        self.assertEqual(report["labels"]["labels"], 1)
        self.assertEqual(report["labels"]["labels_without_completed_context"], 0)
        self.assertEqual(report["labels"]["non_gpt55_labels"], 0)
        self.assertEqual(report["evidence_offsets"]["offset_failures"], 0)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM labels WHERE label_pack = 'ai_discourse_v3_1'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM episode_context_runs WHERE status = 'completed'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM discourse_event_contexts").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_speaker_mentions").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM raw_actor_mentions").fetchone()[0], 2)
            context = conn.execute("SELECT source_context_kind, speaker_name, reported_actor_name FROM discourse_event_contexts").fetchone()
            self.assertEqual(context["source_context_kind"], "substantive_dialogue")
            self.assertEqual(context["speaker_name"], "Alex Rivera")
            self.assertEqual(context["reported_actor_name"], "agents")
            event = conn.execute("SELECT event_type FROM discourse_events").fetchone()
            self.assertEqual(event["event_type"], "causal_mechanism")

        queued_audit = self.run_cli("audit", "--sample", "1.0", "--label-pack", "ai_discourse_v3_1")
        self.assertEqual(queued_audit["queued"], 1)
        audit_run = self.run_cli("run", "--lane", "quality", "--limit", "5", "--job-types", "audit_label", "--no-claim-prompts", "--max-label-prompts", "0")
        self.assertEqual(audit_run["completed"], 1)
        with self.db_conn() as conn:
            audit = conn.execute("SELECT status, score, disagreement_json FROM quality_audits WHERE label_pack = 'ai_discourse_v3_1'").fetchone()
            self.assertEqual(audit["status"], "passed")
            self.assertGreaterEqual(audit["score"], 0.85)
            self.assertNotIn("entity is not explicit", audit["disagreement_json"])

        identity = self.run_cli("groom-identities", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5")
        self.assertEqual(identity["ok"], True)
        self.assertEqual(identity["review_status"], "deterministic_candidate_bootstrap_pending_gpt55_judge")
        self.assertEqual(identity["authority_score_status"]["eligible"], False)
        self.assertEqual(
            identity["authority_score_status"]["reason"],
            "authority_scores_require_accepted_identities_outcomes_and_release",
        )
        with self.db_conn() as conn:
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM canonical_people").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM identity_resolution_candidates").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM podcast_guest_edges").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM person_concept_edges").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM expert_authority_scores").fetchone()[0], 0)
            segment_id = conn.execute("SELECT id FROM segments LIMIT 1").fetchone()["id"]
            conn.execute(
                """
                INSERT INTO jobs
                  (lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts,
                   dedupe_key, created_at, updated_at, completed_at)
                VALUES ('podcast', 'label_segment', ?, ?, 'completed', 100, 1, 2, ?, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z')
                """,
                (
                    segment_id,
                    json.dumps({"pilot_id": "pilot-v31-test", "label_pack": "ai_discourse_v3_1"}),
                    f"historical-relabel:{segment_id}",
                ),
            )
            conn.commit()

        reviewer = self.run_cli("reviewer-audit", "--pilot-id", "pilot-v31-test", "--episodes", "1", "--model", "gpt-5.5")
        self.assertEqual(reviewer["created"], 1)
        reviewer_handoff = reviewer["audits"][0]
        reviewer_prompt = Path(reviewer_handoff["prompt_path"]).read_text(encoding="utf-8")
        self.assertIn("Full direct episode text for private review", reviewer_prompt)
        event_packet = reviewer_prompt.split("Extracted discourse events:\n", 1)[1].split("\n\nIdentity mentions and graph evidence:", 1)[0]
        self.assertEqual(len(json.loads(event_packet)), 1)
        reviewer_output = {
            "schema_version": "reviewer_audit_v1",
            "pilot_id": "pilot-v31-test",
            "episode_id": reviewer_handoff["episode_id"],
            "scores": {
                "overall": 96,
                "coverage": 94,
                "precision": 97,
                "grounding": 98,
                "identity_graph_usefulness": 92,
                "product_market_signal_usefulness": 90,
            },
            "missed_signals": [],
            "false_or_weak_events": [],
            "p0_issues": [],
            "summary": "Fixture reviewer found the single extracted event well grounded.",
            "recommendation": "scale",
        }
        Path(reviewer_handoff["output_path"]).write_text(json.dumps(reviewer_output), encoding="utf-8")
        submitted_review = self.run_cli(
            "submit-reviewer-audit",
            "--audit-id",
            reviewer_handoff["audit_id"],
            "--output-json",
            reviewer_handoff["output_path"],
        )
        self.assertEqual(submitted_review["summary"]["completed"], 1)

        refreshed_review = self.run_cli("reviewer-audit", "--pilot-id", "pilot-v31-test", "--episodes", "1", "--model", "gpt-5.5", "--fresh")
        self.assertEqual(refreshed_review["refreshed"], 1)
        with self.db_conn() as conn:
            event_row = conn.execute("SELECT id, segment_id FROM discourse_events LIMIT 1").fetchone()
        reviewer_remediation_output = {
            "schema_version": "reviewer_audit_v1",
            "pilot_id": "pilot-v31-test",
            "episode_id": reviewer_handoff["episode_id"],
            "scores": {
                "overall": 80,
                "coverage": 78,
                "precision": 87,
                "grounding": 94,
                "identity_graph_usefulness": 74,
                "product_market_signal_usefulness": 82,
            },
            "missed_signals": [
                {
                    "severity": "P1",
                    "description": "Missed identity graph cue for the guest and who mentioned whom.",
                    "evidence": "short fixture excerpt",
                    "segment_id": event_row["segment_id"],
                }
            ],
            "false_or_weak_events": [
                {
                    "severity": "P0",
                    "discourse_event_id": event_row["id"],
                    "reason": "A numeric footnote marker was treated as a quantitative product-quality signal.",
                }
            ],
            "p0_issues": ["A numeric footnote marker created a materially false research signal."],
            "summary": "Fixture reviewer found targeted remediation issues.",
            "recommendation": "do_not_scale",
        }
        Path(reviewer_handoff["output_path"]).write_text(json.dumps(reviewer_remediation_output), encoding="utf-8")
        remediation_review = self.run_cli(
            "submit-reviewer-audit",
            "--audit-id",
            reviewer_handoff["audit_id"],
            "--output-json",
            reviewer_handoff["output_path"],
        )
        self.assertEqual(remediation_review["summary"]["p0_issues"], 1)
        with self.db_conn() as conn:
            conn.execute(
                """
                INSERT INTO reviewer_audits
                  (id, pilot_id, episode_id, model, status, overall_score, coverage_score,
                   precision_score, grounding_score, identity_score, product_market_score,
                   missed_signals_count, false_or_weak_events_count, p0_issue_count,
                   review_json, prompt_path, output_path, created_at, updated_at, completed_at,
                   review_mode, patch_tag, focus_json)
                VALUES (?, ?, ?, 'gpt-5.5', 'completed', 1, 1, 1, 1, 1, 1,
                        0, 0, 1, ?, NULL, NULL,
                        '2026-07-01T00:00:00+00:00',
                        '2026-07-01T00:00:00+00:00',
                        '2026-07-01T00:00:00+00:00',
                        'full', NULL, '{}')
                """,
                (
                    "ra_stale_fixture",
                    "pilot-v31-test",
                    reviewer_handoff["episode_id"],
                    json.dumps(
                        {
                            "schema_version": "reviewer_audit_v1",
                            "pilot_id": "pilot-v31-test",
                            "episode_id": reviewer_handoff["episode_id"],
                            "scores": {
                                "overall": 1,
                                "coverage": 1,
                                "precision": 1,
                                "grounding": 1,
                                "identity_graph_usefulness": 1,
                                "product_market_signal_usefulness": 1,
                            },
                            "missed_signals": [],
                            "false_or_weak_events": [],
                            "p0_issues": ["stale issue that a newer audit supersedes"],
                            "summary": "stale",
                            "recommendation": "do_not_scale",
                        }
                    ),
                ),
            )
            conn.commit()
        findings = self.run_cli("reviewer-findings", "--pilot-id", "pilot-v31-test", "--severity", "P0,P1")
        self.assertEqual(findings["reviewer_audits_completed"], 1)
        self.assertNotIn("stale issue", json.dumps(findings))
        self.assertEqual(findings["privacy"], "sanitized_no_transcript_text")
        self.assertGreaterEqual(len(findings["findings"]), 3)
        self.assertIn(event_row["segment_id"], findings["impacted_segment_ids"])
        self.assertNotIn("evidence", findings["findings"][0])
        failure_bank = self.run_cli("failure-bank", "--pilot-id", "pilot-v31-test", "--patch-tag", "remediation-fixture-v1")
        self.assertEqual(failure_bank["status"], "passed")
        self.assertEqual(failure_bank["synthetic_passed"], failure_bank["synthetic_total"])
        self.assertIn("numeric_artifact", failure_bank["failure_class_counts"])
        self.assertEqual(failure_bank["privacy"], "sanitized_no_transcript_text")
        self.assertTrue(Path(failure_bank["path"]).exists())
        delta = self.run_cli("audit-delta", "--pilot-id", "pilot-v31-test", "--label-pack", "ai_discourse_v3_1", "--patch-tag", "remediation-fixture-v1", "--sentinel", "1")
        self.assertGreaterEqual(delta["selected"], 1)
        self.assertGreaterEqual(delta["queued"], 1)
        self.assertTrue(delta["failure_class_counts"])
        targeted = self.run_cli(
            "reviewer-audit",
            "--pilot-id",
            "pilot-v31-test",
            "--episodes",
            "5",
            "--model",
            "gpt-5.5",
            "--mode",
            "targeted",
            "--patch-tag",
            "remediation-fixture-v1",
        )
        self.assertEqual(targeted["mode"], "targeted")
        self.assertEqual(targeted["patch_tag"], "remediation-fixture-v1")
        self.assertEqual(targeted["created"], 1)
        self.assertEqual(targeted["summary"]["review_mode"], "targeted")
        dry_requeue = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only", "--dry-run")
        self.assertGreaterEqual(dry_requeue["selected_segments"], 1)
        requeue = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only")
        self.assertGreaterEqual(requeue["enqueued"], 1)
        requeue_again = self.run_cli("requeue-reviewed-segments", "--pilot-id", "pilot-v31-test", "--mode", "failed-review-only")
        self.assertGreaterEqual(requeue_again["skipped"], 1)

        clusters = self.run_cli("cluster-claims", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5")
        self.assertGreaterEqual(clusters["clusters_upserted"], 1)
        claim_edges = self.run_cli("judge-claim-edges", "--pilot-id", "pilot-v31-test", "--model", "gpt-5.5", "--limit", "10")
        self.assertEqual(claim_edges["status"], "candidate_edges_pending_gpt55_semantic_judge")

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assert_operational_snapshot(payload)
        self.assertGreaterEqual(payload["counts"]["reviewer_audits"], 1)
        self.assertGreaterEqual(payload["counts"]["claim_clusters"], 1)
        self.assertGreaterEqual(payload["counts"]["identity_resolution_candidates"], 1)
        self.assertGreater(payload["counts"]["discourse_event_contexts"], 0)

    def test_claim_prefers_v31_labels_with_completed_episode_context(self) -> None:
        self.run_cli("init")
        (self.root / "corpus" / "segments").mkdir(parents=True, exist_ok=True)
        (self.root / "corpus" / "transcripts").mkdir(parents=True, exist_ok=True)
        (self.root / "runs" / "episode_contexts").mkdir(parents=True, exist_ok=True)
        (self.root / "corpus" / "segments" / "seg_blocked.txt").write_text("Blocked segment about developer tools and release practices.", encoding="utf-8")
        (self.root / "corpus" / "segments" / "seg_ready.txt").write_text("Ready segment about infrastructure reliability and model deployment.", encoding="utf-8")
        (self.root / "corpus" / "segments" / "seg_failed.txt").write_text("Failed-context segment about deployment practices.", encoding="utf-8")
        (self.root / "corpus" / "segments" / "seg_pending.txt").write_text("Pending-context segment about deployment practices.", encoding="utf-8")
        (self.root / "corpus" / "transcripts" / "tr_blocked.txt").write_text("Blocked transcript text.", encoding="utf-8")
        (self.root / "corpus" / "transcripts" / "tr_ready.txt").write_text("Ready transcript text.", encoding="utf-8")
        (self.root / "corpus" / "transcripts" / "tr_failed.txt").write_text("Failed transcript text.", encoding="utf-8")
        (self.root / "corpus" / "transcripts" / "tr_pending.txt").write_text("Pending transcript text.", encoding="utf-8")
        context_path = self.root / "runs" / "episode_contexts" / "ctx_ready.json"
        context_path.write_text(
            json.dumps(
                {
                    "schema_version": "ai_discourse_v3_1_episode_context",
                    "episode_id": "ep_ready",
                    "speaker_map": [],
                    "section_map": [],
                    "entity_seed": {},
                    "concept_seed": [],
                    "extraction_guidance": "Prefer precise claims.",
                    "quality_flags": [],
                    "overall_confidence": 0.9,
                    "needs_review": False,
                    "review_reason": None,
                }
            ),
            encoding="utf-8",
        )
        with self.db_conn() as conn:
            ts = "2026-07-08T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_claim_pref', 'Claim Preference Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_or_official_public_transcripts', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            for suffix in ("blocked", "ready", "failed", "pending"):
                conn.execute(
                    """
                    INSERT INTO episodes (id, source_id, guid, title, published_at, created_at, updated_at)
                    VALUES (?, 'src_claim_pref', ?, ?, ?, ?, ?)
                    """,
                    (f"ep_{suffix}", f"ep_{suffix}", f"{suffix.title()} Episode", ts, ts, ts),
                )
                conn.execute(
                    """
                    INSERT INTO transcripts
                      (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                    VALUES (?, ?, 'official_show_transcript', 'https://example.test/transcript', 'text/plain', ?, ?, 'ready', ?, '{}', 10, ?, ?)
                    """,
                    (f"tr_{suffix}", f"ep_{suffix}", f"corpus/transcripts/tr_{suffix}.txt", f"sha-{suffix}", ts, ts, ts),
                )
                conn.execute(
                    """
                    INSERT INTO segments
                      (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                    VALUES (?, ?, ?, 'src_claim_pref', 0, 0, 80, ?, ?, 10, ?)
                    """,
                    (f"seg_{suffix}", f"tr_{suffix}", f"ep_{suffix}", f"corpus/segments/seg_{suffix}.txt", f"seg-sha-{suffix}", ts),
                )
            conn.execute(
                """
                INSERT INTO episode_context_runs
                  (id, job_id, episode_id, transcript_id, label_pack, model, status, context_artifact_path,
                   speaker_map_json, section_map_json, entity_seed_json, concept_seed_json, extraction_guidance, created_at, updated_at, completed_at)
                VALUES ('ectx_ready', NULL, 'ep_ready', 'tr_ready', 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?,
                        '[]', '[]', '{}', '[]', 'Prefer precise claims.', ?, ?, ?)
                """,
                (str(context_path), ts, ts, ts),
            )
            payload = json.dumps({"pilot_id": "pilot_claim_pref", "label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"})
            context_payload = json.dumps(
                {
                    "pilot_id": "pilot_claim_pref",
                    "label_pack": "ai_discourse_v3_1",
                    "model": "gpt-5.5",
                    "episode_context_version": "ai_discourse_v3_1_episode_context",
                }
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at, error)
                VALUES (10, 'podcast', 'episode_context', 'ep_failed', ?, 'failed', 0, 2, 2, 'context-failed', ?, ?, 'fixture context failure')
                """,
                (context_payload, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (11, 'podcast', 'episode_context', 'ep_pending', ?, 'pending', 0, 0, 2, 'context-pending', ?, ?)
                """,
                (context_payload, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (4, 'podcast', 'label_segment', 'seg_pending', ?, 'pending', 0, 0, 2, 'pending-label', ?, ?)
                """,
                (payload, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (3, 'podcast', 'label_segment', 'seg_failed', ?, 'pending', 0, 0, 2, 'failed-label', ?, ?)
                """,
                (payload, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (1, 'podcast', 'label_segment', 'seg_blocked', ?, 'pending', 1, 0, 2, 'blocked', ?, ?)
                """,
                (payload, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (2, 'podcast', 'label_segment', 'seg_ready', ?, 'pending', 100, 0, 2, 'ready', ?, ?)
                """,
                (payload, ts, ts),
            )
            legacy_payload = json.dumps({"pilot_id": "pilot_claim_pref", "label_pack": "ai_discourse_v1", "model": "gpt-5.5"})
            conn.execute(
                """
                INSERT INTO jobs (id, lane, job_type, target_id, payload_json, status, priority, attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (5, 'podcast', 'label_segment', 'seg_ready', ?, 'pending', -10, 0, 2, 'legacy-label', ?, ?)
                """,
                (legacy_payload, ts, ts),
            )
            conn.commit()

        claimed = self.run_cli(
            "claim",
            "--lane",
            "podcast",
            "--label-pack",
            "ai_discourse_v3_1",
            "--model",
            "gpt-5.5",
            "--worker-id",
            "claim-pref-worker",
            "--pilot-id",
            "pilot_claim_pref",
        )
        self.assertEqual(claimed["job_id"], "2")
        with self.db_conn() as conn:
            blocked = conn.execute("SELECT status FROM jobs WHERE id = 1").fetchone()
            ready = conn.execute("SELECT status, lease_owner FROM jobs WHERE id = 2").fetchone()
        self.assertEqual(blocked["status"], "pending")
        self.assertEqual(ready["status"], "claimed")
        self.assertEqual(ready["lease_owner"], "claim-pref-worker")
        with self.db_conn() as conn:
            legacy = conn.execute("SELECT status, lease_owner FROM jobs WHERE id = 5").fetchone()
        self.assertEqual(legacy["status"], "pending")
        self.assertIsNone(legacy["lease_owner"])
        second_claim = self.run_cli(
            "claim",
            "--lane",
            "podcast",
            "--label-pack",
            "ai_discourse_v3_1",
            "--model",
            "gpt-5.5",
            "--worker-id",
            "claim-pref-worker-2",
            "--pilot-id",
            "pilot_claim_pref",
        )
        self.assertEqual(second_claim["status"], "waiting_for_episode_context")
        self.assertEqual(second_claim["released_label_job_id"], 1)
        with self.db_conn() as conn:
            failed_dep = conn.execute("SELECT status, lease_owner FROM jobs WHERE id = 3").fetchone()
            pending_dep = conn.execute("SELECT status, lease_owner FROM jobs WHERE id = 4").fetchone()
        self.assertEqual(failed_dep["status"], "pending")
        self.assertIsNone(failed_dep["lease_owner"])
        self.assertEqual(pending_dep["status"], "pending")
        self.assertIsNone(pending_dep["lease_owner"])

    def test_transcript_exhaustion_gates_transcription_and_is_disabled_by_default(self) -> None:
        sources = self.write_audio_only_source()
        self.run_cli("init")
        enqueued = self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.assertEqual(enqueued["missing_transcripts"], 1)
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.assertEqual(candidates["candidates"][0]["acquisition_status"], "browser_discovery_pending")

        blocked = self.run_cli(
            "record-transcript-attempt",
            "--episode-id",
            episode_id,
            "--method",
            "youtube_caption",
            "--status",
            "youtube_caption_blocked",
            "--source-kind",
            "youtube_captions",
            "--error-class",
            "youtube_caption_blocked",
            "--notes",
            "Fixture YouTube caption request was blocked.",
        )
        self.assertEqual(blocked["status"], "youtube_caption_blocked")
        exhausted = self.run_cli("mark-transcript-exhausted", "--episode-id", episode_id, "--worker-id", "test-browser")
        self.assertEqual(exhausted["status"], "transcription_eligible")

        dry_run = self.run_cli(
            "enqueue-transcription",
            "--lane",
            "podcast",
            "--provider",
            "voyager",
            "--label-pack",
            "ai_discourse_v3_1",
            "--limit",
            "5",
            "--dry-run",
        )
        self.assertEqual(dry_run["selected"], 1)
        self.assertEqual(dry_run["enqueued"], 0)
        queued = self.run_cli(
            "enqueue-transcription",
            "--lane",
            "podcast",
            "--provider",
            "voyager",
            "--label-pack",
            "ai_discourse_v3_1",
            "--limit",
            "5",
        )
        self.assertEqual(queued["enqueued"], 1)

        failed = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "transcribe_audio",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(failed["failed"], 1)
        self.assertIn("Paid transcription fallback is disabled", failed["details"][0]["error"])

        snapshot = self.run_cli("snapshot")
        payload = json.loads(Path(snapshot["path"]).read_text(encoding="utf-8"))
        self.assertEqual(payload["transcript_acquisition_metrics"]["status_counts"]["transcription_eligible"], 1)
        self.assertTrue(any(item["status"] == "failed" for item in payload["transcript_acquisition_metrics"]["transcription_runs"]))
        self.assertIn("transcript_acquisition_attempts", payload["counts"])
        funnel = self.run_cli("acquisition-funnel", "--source", "Audio Only Fixture", "--limit", "5")
        self.assertEqual(funnel["sources"][0]["acquisition_status"]["transcription_eligible"], 1)

    def test_scale_gate_report_blocks_until_v31_gate_criteria_pass(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        fetched = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "fetch_transcript",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(fetched["completed"], 1)
        gate = self.run_cli(
            "scale-gate-enqueue",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--pilot-id",
            "scale-gate-test",
            "--priority",
            "10",
            "--source",
            "Fixture Tech Podcast",
            "--include-low-signal",
        )
        self.assertEqual(gate["selected_episodes"], 1)
        self.assertGreaterEqual(gate["label_jobs"], 1)
        self.assertEqual(gate["episode_context_jobs"], 1)

        report = self.run_cli("scale-gate-report", "--pilot-id", "scale-gate-test")
        self.assertEqual(report["gate_state"], "blocked")
        self.assertEqual(report["episodes"]["selected"], 1)
        self.assertIn("selected_25_episodes", report["failed_checks"])
        self.assertIn("all_selected_episodes_have_gpt55_context", report["failed_checks"])
        self.assertIn("all_selected_segments_labeled_v31_gpt55", report["failed_checks"])
        self.assertIn("semantic_reviewer_audit_passes", report["failed_checks"])
        self.assertTrue(any(item["job_type"] == "label_segment" and item["status"] == "pending" for item in report["jobs"]))
        self.assertEqual(report["privacy"], "sanitized_operational_report_no_raw_transcripts")
        self.assertTrue(Path(report["path"]).exists())

    def test_enabled_voyager_fixture_transcription_uses_standard_ingest_path(self) -> None:
        sources = self.write_audio_only_source()
        transcript = self.root / "voyager-fixture.txt"
        transcript.write_text(
            "Host: Today we discuss AGI, AI agents, coding systems, and inference scaling. "
            "Guest: Frontier labs are shifting from chatbot demos toward agents that take actions across tools. "
            "Guest: Enterprise AI adoption depends on reliability, product integration, and evaluation discipline. "
            "Host: The terminology around AGI and singularity may change as model capabilities improve.",
            encoding="utf-8",
        )
        self.env["ALLOW_PAID_TRANSCRIPTION"] = "true"
        self.env["VOYAGER_API_KEY"] = "fixture-key"
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        candidates = self.run_cli("transcript-candidates", "--lane", "podcast", "--limit", "5")
        episode_id = candidates["candidates"][0]["episode_id"]
        self.run_cli("mark-transcript-exhausted", "--episode-id", episode_id, "--worker-id", "test-browser")
        queued = self.run_cli("enqueue-transcription", "--lane", "podcast", "--provider", "voyager", "--label-pack", "ai_discourse_v3_1", "--limit", "5")
        self.assertEqual(queued["enqueued"], 1)
        with self.db_conn() as conn:
            job = conn.execute("SELECT id, payload_json FROM jobs WHERE job_type = 'transcribe_audio'").fetchone()
            payload = json.loads(job["payload_json"])
            payload["fixture_text_path"] = str(transcript)
            conn.execute(
                "UPDATE jobs SET payload_json = ? WHERE id = ?",
                (json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")), job["id"]),
            )
            conn.commit()
        run = self.run_cli(
            "run",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--job-types",
            "transcribe_audio",
            "--label-pack",
            "ai_discourse_v3_1",
            "--no-claim-prompts",
        )
        self.assertEqual(run["completed"], 1)
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM transcripts WHERE source_kind = 'voyager_transcription' AND status = 'ready'").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'label_segment'").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM transcription_runs WHERE status = 'completed'").fetchone()[0], 1)

    def test_observer_density_uses_unique_segment_words_not_distinct_word_counts(self) -> None:
        self.run_cli("init")
        ts = "2026-07-01T00:00:00+00:00"
        with self.db_conn() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_density', 'Density Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_transcripts_only', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at)
                VALUES ('ep_density', 'src_density', 'density-1', 'Density Episode', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO transcripts (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_density', 'ep_density', 'official_show_transcript', 'https://example.test/transcript', 'text/plain', 'corpus/transcripts/density.txt', 'sha', 'ready', ?, '{}', 20, ?, ?)
                """,
                (ts, ts, ts),
            )
            for idx in range(2):
                segment_id = f"seg_density_{idx}"
                label_id = f"lab_density_{idx}"
                event_id = f"de_density_{idx}"
                conn.execute(
                    """
                    INSERT INTO segments (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                    VALUES (?, 'tr_density', 'ep_density', 'src_density', ?, 0, 10, ?, 'segsha', 10, ?)
                    """,
                    (segment_id, idx, f"corpus/segments/{segment_id}.txt", ts),
                )
                conn.execute(
                    """
                    INSERT INTO labels (id, segment_id, label_pack, label_pack_version, model, status, output_json, confidence, created_at)
                    VALUES (?, ?, 'ai_discourse_v3_1', 'v3.1', 'gpt-5.5', 'ready', '{}', 0.9, ?)
                    """,
                    (label_id, segment_id, ts),
                )
                conn.execute(
                    """
                    INSERT INTO discourse_events
                      (id, label_id, segment_id, event_index, event_type, claim_text, confidence, evidence_text, evidence_start, evidence_end, created_at)
                    VALUES (?, ?, ?, 0, 'term_usage', 'Fixture claim', 0.9, 'evidence', 0, 8, ?)
                    """,
                    (event_id, label_id, segment_id, ts),
                )
            conn.commit()
        from research_factory.observer import _coding_metrics

        with self.db_conn() as conn:
            coding = _coding_metrics(conn)
        self.assertEqual(coding["v3_1_discourse_events"], 2)
        self.assertEqual(coding["v3_1_events_per_1000_segment_words"], 100.0)
        snapshot = self.run_cli("snapshot")
        self.assert_operational_snapshot(json.loads(Path(snapshot["path"]).read_text(encoding="utf-8")))

    def test_quarantine_contaminated_removes_pending_label_jobs(self) -> None:
        from research_factory import db
        from research_factory.util import dumps_json, now_iso, sha256_text

        self.run_cli("init")
        with self.db_conn() as conn:
            ts = now_iso()
            conn.execute(
                "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('src', 'Source', ?, ?)",
                (ts, ts),
            )
            conn.execute(
                "INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at) VALUES ('ep', 'src', 'guid', 'Episode', ?, ?)",
                (ts, ts),
            )
            transcript_text = '[{"start":0,"end":1,"text":"AGI agents."},{"start":1,"end":2,"text":"More agents."}]'
            transcript_path = self.root / "corpus" / "transcripts" / "tr_bad.txt"
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_text(transcript_text, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_bad', 'ep', 'creator_provided_rss_transcript', 'https://example.test/transcription.json', 'binary/octet-stream', ?, ?, 'ready', ?, '{}', ?, ?, ?)
                """,
                ("corpus/transcripts/tr_bad.txt", sha256_text(transcript_text), ts, len(transcript_text.split()), ts, ts),
            )
            segment_path = self.root / "corpus" / "segments" / "seg_bad.txt"
            segment_path.parent.mkdir(parents=True, exist_ok=True)
            segment_path.write_text(transcript_text, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES ('seg_bad', 'tr_bad', 'ep', 'src', 0, 0, ?, 'corpus/segments/seg_bad.txt', ?, ?, ?)
                """,
                (len(transcript_text), sha256_text(transcript_text), len(transcript_text.split()), ts),
            )
            db.enqueue_job(conn, lane="podcast", job_type="label_segment", target_id="seg_bad", payload={"label_pack": "ai_discourse_v1"})
            conn.commit()
        result = self.run_cli("quarantine-contaminated", "--apply")
        self.assertEqual(result["found"], 1)
        with self.db_conn() as conn:
            status = conn.execute("SELECT status, policy_json FROM transcripts WHERE id = 'tr_bad'").fetchone()
            self.assertEqual(status["status"], "quarantined")
            self.assertIn("raw_json_timing_payload", dumps_json(json.loads(status["policy_json"])))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM segments WHERE id = 'seg_bad'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM jobs WHERE target_id = 'seg_bad'").fetchone()[0], 0)

    def test_claim_context_source_read_failure_clears_claim(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")
        self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
        )
        waiting = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        context_job_id = int(waiting["episode_context_job_id"])
        with self.db_conn() as conn:
            context_job = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (context_job_id,)).fetchone()
            conn.execute("UPDATE segments SET text_path = ? WHERE episode_id = ?", ("corpus/segments/missing-context-segment.txt", context_job["target_id"]))
            conn.commit()

        failed = self.run_cli("claim-context", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-context-test")
        self.assertEqual(failed["status"], "episode_context_prompt_failed")
        self.assertIn("episode_context_segment_read_failed", failed["message"])
        self.assertNotIn(str(self.root), failed["message"])
        with self.db_conn() as conn:
            job = conn.execute("SELECT status, lease_owner, error FROM jobs WHERE id = ?", (context_job_id,)).fetchone()
            self.assertEqual(job["status"], "pending")
            self.assertIsNone(job["lease_owner"])
            self.assertIn("episode_context_segment_read_failed", job["error"])

        failed_again = self.run_cli("claim-context", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-context-test")
        self.assertEqual(failed_again["status"], "episode_context_prompt_failed")
        blocked_label = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        self.assertEqual(blocked_label["status"], "episode_context_failed")
        with self.db_conn() as conn:
            label_job = conn.execute("SELECT status, lease_owner, error FROM jobs WHERE id = ?", (blocked_label["failed_label_job_id"],)).fetchone()
            self.assertEqual(label_job["status"], "failed")
            self.assertIsNone(label_job["lease_owner"])
            self.assertEqual(label_job["error"], "required_episode_context_failed")

    def test_submit_rejects_non_exact_evidence(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        claim = self.run_cli("claim", "--lane", "podcast", "--worker-id", "worker-a")
        output_path = Path(claim["output_path"])
        with self.db_conn() as conn:
            row = conn.execute(
                """
                SELECT segments.id AS segment_id, segments.episode_id
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE jobs.id = ?
                """,
                (claim["job_id"],),
            ).fetchone()
        invalid_output = {
            "schema_version": "ai_discourse_v1",
            "segment_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "summary": "The segment discusses agents.",
            "topics": [{"topic": "agents", "stance": "bullish", "intensity": 0.7, "evidence": "agents ... tools"}],
            "terminology_shifts": [],
            "claims": [],
            "entities": {"people": [], "organizations": [], "products": []},
            "overall_confidence": 0.7,
            "needs_review": False,
            "review_reason": None,
        }
        output_path.write_text(json.dumps(invalid_output), encoding="utf-8")
        submitted = self.run_cli_process("submit", "--job-id", str(claim["job_id"]), "--output-json", str(output_path), "--worker-id", "worker-a")
        self.assertEqual(submitted.returncode, 0)
        with self.db_conn() as conn:
            label = conn.execute("SELECT output_json FROM labels").fetchone()
        repaired = json.loads(label["output_json"])
        self.assertFalse(repaired["topics"])
        self.assertTrue(repaired["needs_review"])
        self.assertIn("not exact current-segment text", repaired["review_reason"])

    def test_audit_job_writes_grounding_quality_audit(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "10", "--local-draft")
        queued = self.run_cli("audit", "--sample", "1.0", "--label-pack", "ai_discourse_v1")
        self.assertGreaterEqual(queued["queued"], 1)
        audit_run = self.run_cli("run", "--lane", "quality", "--limit", "5", "--job-types", "audit_label", "--no-claim-prompts", "--max-label-prompts", "0")
        self.assertGreaterEqual(audit_run["completed"], 1)
        with self.db_conn() as conn:
            audit = conn.execute("SELECT status, score, disagreement_json FROM quality_audits LIMIT 1").fetchone()
        self.assertIn(audit["status"], {"passed", "needs_adjudication"})
        self.assertIsNotNone(audit["score"])
        self.assertIn("checks", audit["disagreement_json"])

    def test_run_respects_label_prompt_cap(self) -> None:
        transcript = self.root / "long.vtt"
        transcript.write_text(
            "WEBVTT\n\n00:00:00.000 --> 00:00:10.000\n" + " ".join("agents" for _ in range(1700)),
            encoding="utf-8",
        )
        feed = self.root / "long-feed.xml"
        feed.write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Long Fixture</title><item>
  <guid>long-1</guid>
  <title>Long Episode</title>
  <pubDate>Mon, 29 Jun 2026 12:00:00 GMT</pubDate>
  <podcast:transcript url="{transcript.as_uri()}" type="text/vtt" />
</item></channel></rss>
""",
            encoding="utf-8",
        )
        sources = self.root / "long-sources.yaml"
        sources.write_text(
            f"""sources:
  - name: Long Fixture
    rss_url: {feed}
    homepage_url: https://example.test
    category: tests
    enabled: true
""",
            encoding="utf-8",
        )
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript")
        capped = self.run_cli("run", "--lane", "podcast", "--limit", "10", "--job-types", "label_segment", "--max-label-prompts", "1", "--worker-id", "cap-labeler")
        self.assertEqual(capped["claimed_prompts"], 1)
        with self.db_conn() as conn:
            claimed = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'label_segment' AND status = 'claimed'").fetchone()[0]
        self.assertEqual(claimed, 1)

    def test_prioritize_labels_tags_bounded_pending_jobs(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        dry = self.run_cli(
            "prioritize-labels",
            "--category",
            "tests",
            "--keyword",
            "agents",
            "--limit",
            "1",
            "--priority",
            "15",
            "--pilot-id",
            "pilot-test",
            "--dry-run",
        )
        self.assertEqual(dry["selected"], 1)
        self.assertEqual(dry["updated"], 0)
        with self.db_conn() as conn:
            pending = conn.execute("SELECT priority, payload_json FROM jobs WHERE job_type = 'label_segment'").fetchone()
        self.assertEqual(pending["priority"], 100)

        applied = self.run_cli(
            "prioritize-labels",
            "--category",
            "tests",
            "--keyword",
            "agents",
            "--limit",
            "1",
            "--priority",
            "15",
            "--pilot-id",
            "pilot-test",
        )
        self.assertEqual(applied["selected"], 1)
        self.assertEqual(applied["updated"], 1)
        with self.db_conn() as conn:
            pending = conn.execute("SELECT priority, payload_json FROM jobs WHERE job_type = 'label_segment'").fetchone()
        payload = json.loads(pending["payload_json"])
        self.assertEqual(pending["priority"], 15)
        self.assertEqual(payload["pilot_id"], "pilot-test")
        self.assertEqual(payload["priority_reason"], "bounded_source_aware_label_pilot")

    def test_observer_redacts_public_state(self) -> None:
        from research_factory.ui_server import _sanitize_snapshot

        payload = {
            "contract_version": "railway-operational-v2",
            "generated_at": "2026-07-01T00:00:00+00:00",
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {"segments": 1, "Private claim text as count key": 9},
            "queues": {
                "by_status": {"failed": 1},
                "by_lane_type_status": [
                    {"lane": "podcast", "job_type": "fetch_transcript", "status": "failed", "count": 1}
                ],
            },
            "runs": {
                "label_runs": [
                    {"model": "Private claim text in model field", "status": "failed"}
                ],
                "worker_runs": [],
                "worker_status": {},
                "service_status": {},
            },
            "failures": {
                "jobs": [{"lane": "podcast", "job_type": "fetch_transcript", "count": 1}],
            },
            "artifacts": [],
            "intervention_flags": [],
            "scale_gate": {"gate_state": "passed", "failed_checks": [], "privacy": "sanitized_operational_report_no_raw_transcripts"},
            "trend_metrics": {
                "concept_momentum": [{"concept": "agents", "delta": 3, "source_url": "https://example.test/private"}],
                "episode_month_timeline": [{"day": f"2026-{index:02d}", "episodes": index} for index in range(1, 246)],
                "source_productivity": [{"source_name": "Fixture", "source_url": "https://example.test/feed", "events_per_1000_words": 2.5}],
                "raw_transcript_text": "WEBVTT\nsecret transcript",
            },
            "job_counts": {"failed": 1},
            "active_jobs": [
                {
                    "id": 123,
                    "target_id": "seg_abcdef1234567890",
                    "job_type": "fetch_transcript",
                    "error": "failed https://www.youtube.com/watch?v=abc at /Users/kolbydayley/private",
                }
            ],
            "recent_runs": [{"id": "run_abcdef1234567890", "segment_id": "seg_abcdef1234567890", "status": "failed"}],
            "unknown": {"secret": "keep out"},
        }
        sanitized = _sanitize_snapshot(payload)
        rendered = json.dumps(sanitized)
        self.assertNotIn("target_id", rendered)
        self.assertNotIn("youtube.com", rendered)
        self.assertNotIn("/Users/", rendered)
        self.assertNotIn("run_abcdef", rendered)
        self.assertNotIn("unknown", sanitized)
        self.assertNotIn("source_url", rendered)
        self.assertNotIn("WEBVTT", rendered)
        self.assertNotIn("Private claim", rendered)
        self.assert_operational_snapshot(sanitized)
        self.assertEqual(sanitized["queues"]["by_status"], {"failed": 1})
        self.assertEqual(sanitized["failures"]["jobs"][0]["count"], 1)

    def test_public_error_text_redacts_full_local_paths(self) -> None:
        from research_factory.observer import _public_text

        redacted = _public_text("failed at /Users/kolbydayley/Desktop/private/file.txt")
        self.assertEqual(redacted, "failed at <local-path>")

    def test_label_pack_examples_are_valid_outputs(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output
            from research_factory.labels import ValidationError

            for pack_dir in (self.root / "label_packs").iterdir():
                examples = json.loads((pack_dir / "examples.json").read_text(encoding="utf-8"))
                for example in examples:
                    if "output" in example:
                        validate_label_output(pack_dir.name, example["output"])
                    else:
                        self.fail(f"{pack_dir.name} example is missing a complete output object")
            invalid = json.loads(((self.root / "label_packs" / "ai_discourse_v1" / "examples.json").read_text(encoding="utf-8")))[0]["output"]
            invalid["segment_id"] = ""
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v1", invalid)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_numeric_page_artifact_as_metric(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = example["input"]["text"]
            marker_start = segment_text.rfind("4")
            self.assertGreaterEqual(marker_start, 0)
            event = output["discourse_events"][0]
            event["event_type"] = "product_signal"
            event["event_subtype"] = "quality_multiplier_artifact"
            event["claim_text"] = "The segment claims Gemini quality improved 4x."
            event["claim_type"] = "product_market"
            event["metric"] = {"value": "4x", "unit": "quality", "comparator": "improved", "direction": "increase", "raw_text": "4x"}
            event["evidence"] = "4"
            event["evidence_start"] = marker_start
            event["evidence_end"] = marker_start + 1
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_rejects_bare_digit_metric_artifact(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "Once I have proved something in Lean, the quality of the output is basically 4 as high as if it came from a human."
            output["segment_id"] = "seg_numeric_fixture"
            output["episode_id"] = "ep_numeric_fixture"
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            event["event_type"] = "market_signal"
            event["event_subtype"] = "quality_multiplier_artifact"
            event["claim_text"] = "RJ claims Lean-proven outputs are four times as high quality as human outputs."
            event["claim_type"] = "product_market"
            event["metric"] = {"value": "4", "unit": "quality multiplier", "comparator": "if it came from a human", "direction": "increase", "raw_text": "quality of the output is basically 4 as high"}
            event["evidence"] = segment_text
            event["evidence_start"] = 0
            event["evidence_end"] = len(segment_text)
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_does_not_override_llm_sponsor_classification(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import render_prompt, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "AI agent security is one of the most important and most overlooked issues in technology right now."
            output["segment_id"] = "seg_sponsor_fixture"
            output["episode_id"] = "ep_sponsor_fixture"
            output["concept_candidates"] = []
            output["segment_source_context"] = {"kind": "sponsor_ad_read", "confidence": 0.95, "rationale": "Host-read sponsor copy."}
            event = output["discourse_events"][0]
            event["event_type"] = "risk_signal"
            event["event_subtype"] = "sponsor_security_claim"
            event["claim_text"] = "Sponsor copy claims AI agent security is overlooked."
            event["source_context"] = {"kind": "sponsor_ad_read", "confidence": 0.95, "rationale": "Host-read sponsor copy."}
            event["evidence"] = segment_text
            event["evidence_start"] = 0
            event["evidence_end"] = len(segment_text)
            output["discourse_events"] = [event]
            validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)

            prompt = render_prompt("ai_discourse_v3_1", {"text": segment_text}, {"segment_id": "seg_sponsor_fixture"})
            self.assertIn("Sponsor/ad-read copy is excluded", prompt)
            self.assertNotIn("Sponsor/ad-read claims may be coded", prompt)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_allows_qualitative_comparison_metric_fields(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "Burger says the model captured his learning style better than a human."
            evidence = "captured his learning style better than a human"
            output["segment_id"] = "seg_qualitative_metric"
            output["episode_id"] = "ep_qualitative_metric"
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            event["claim_text"] = "Burger says the model captured his learning style better than a human could."
            event["metric"] = {
                "value": None,
                "unit": None,
                "comparator": "a human",
                "direction": "increase",
                "raw_text": "better than a human",
            }
            event["evidence"] = evidence
            event["evidence_start"] = segment_text.index(evidence)
            event["evidence_end"] = event["evidence_start"] + len(evidence)
            output["discourse_events"] = [event]
            validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_does_not_apply_regex_identity_pruning(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, repair_label_output_for_submission, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "Grant:\nI think the proof system bottleneck is still important."
            output["segment_id"] = "seg_identity_fixture"
            output["episode_id"] = "ep_identity_fixture"
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            event["event_type"] = "actor_mention"
            event["event_subtype"] = "speaker_label_only"
            event["actor"] = {"name": "Grant", "actor_type": "person", "affiliation": None, "role": "speaker"}
            event["speaker_context"] = {"name": "Grant", "role": "guest", "affiliation": None, "confidence": 0.6}
            event["reported_actor"] = {"name": "none", "actor_type": "unknown", "affiliation": None, "confidence": 0.0}
            event["claim_text"] = "Grant appears as a transcript speaker label."
            event["signal_reason"] = "This only records a transcript speaker label without graph-useful context."
            event["evidence"] = "Grant:"
            event["evidence_start"] = 0
            event["evidence_end"] = len("Grant:")
            output["discourse_events"] = [event]
            validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
            repairs = repair_label_output_for_submission("ai_discourse_v3_1", output, segment_text=segment_text)
            self.assertEqual(repairs, 0)
            self.assertEqual(len(output["discourse_events"]), 1)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_repair_does_not_relabel_invalid_speaker_role(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, repair_label_output_for_submission, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "The guest says smaller models are improving because deployment constraints are forcing efficiency."
            output["segment_id"] = "seg_speaker_role_fixture"
            output["episode_id"] = "ep_speaker_role_fixture"
            output["segment_quality"]["substantive_word_count"] = len(segment_text.split())
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            evidence = "smaller models are improving"
            event["evidence"] = evidence
            event["evidence_start"] = segment_text.index(evidence)
            event["evidence_end"] = event["evidence_start"] + len(evidence)
            event["claim_text"] = "The guest says smaller models are improving because deployment constraints are forcing efficiency."
            event["speaker_context"]["role"] = "developer"
            output["discourse_events"] = [event]

            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
            repairs = repair_label_output_for_submission("ai_discourse_v3_1", output, segment_text=segment_text)
            self.assertEqual(repairs, 0)
            self.assertEqual(output["discourse_events"][0]["speaker_context"]["role"], "developer")
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_repair_does_not_relabel_invalid_stance(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import ValidationError, repair_label_output_for_submission, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "The guest says benchmark corrections show smaller models are improving under deployment pressure."
            output["segment_id"] = "seg_corrective_stance_fixture"
            output["episode_id"] = "ep_corrective_stance_fixture"
            output["segment_quality"]["substantive_word_count"] = len(segment_text.split())
            output["concept_candidates"] = []
            event = output["discourse_events"][0]
            evidence = "smaller models are improving"
            event["evidence"] = evidence
            event["evidence_start"] = segment_text.index(evidence)
            event["evidence_end"] = event["evidence_start"] + len(evidence)
            event["claim_text"] = "The guest says benchmark corrections show smaller models are improving under deployment pressure."
            event["stance"] = "corrective"
            output["discourse_events"] = [event]

            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
            repairs = repair_label_output_for_submission("ai_discourse_v3_1", output, segment_text=segment_text)
            self.assertEqual(repairs, 0)
            self.assertEqual(output["discourse_events"][0]["stance"], "corrective")
            with self.assertRaises(ValidationError):
                validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v31_repair_drops_unresolved_evidence_before_submission(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import repair_label_output_for_submission, validate_label_output

            example = json.loads((self.root / "label_packs" / "ai_discourse_v3_1" / "examples.json").read_text(encoding="utf-8"))[0]
            output = json.loads(json.dumps(example["output"]))
            segment_text = "The guest says smaller models are improving, but gives no benchmark number here."
            output["segment_id"] = "seg_bad_evidence_fixture"
            output["episode_id"] = "ep_bad_evidence_fixture"
            output["discourse_events"][0]["evidence"] = "smaller models improved by 99 percent"
            output["discourse_events"][0]["evidence_start"] = 0
            output["discourse_events"][0]["evidence_end"] = len(output["discourse_events"][0]["evidence"])
            output["concept_candidates"][0]["evidence"] = "model benchmark improved by 99 percent"
            output["concept_candidates"][0]["evidence_start"] = 0
            output["concept_candidates"][0]["evidence_end"] = len(output["concept_candidates"][0]["evidence"])
            repairs = repair_label_output_for_submission("ai_discourse_v3_1", output, segment_text=segment_text)
            self.assertGreaterEqual(repairs, 3)
            self.assertEqual(output["extraction_status"], "insufficient_evidence")
            self.assertFalse(output["discourse_events"])
            self.assertFalse(output["concept_candidates"])
            self.assertTrue(output["needs_review"])
            validate_label_output("ai_discourse_v3_1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_v1_repair_drops_unresolved_evidence_before_submission(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import repair_label_output_for_submission, validate_label_output

            segment_text = "The host says AI coding agents are getting useful for routine software work."
            output = {
                "schema_version": "ai_discourse_v1",
                "segment_id": "seg_v1_bad_evidence_fixture",
                "episode_id": "ep_v1_bad_evidence_fixture",
                "summary": "The segment discusses AI coding agents.",
                "topics": [
                    {
                        "topic": "ai_coding",
                        "stance": "mixed_or_descriptive",
                        "intensity": 0.7,
                        "evidence": "AI coding agents are getting useful",
                    },
                    {
                        "topic": "agents",
                        "stance": "bullish",
                        "intensity": 0.8,
                        "evidence": "AI agents will replace all software teams",
                    },
                ],
                "terminology_shifts": [
                    {
                        "from_term": "tools",
                        "to_term": "agents",
                        "speaker_or_org": "host",
                        "evidence": "tools are now called agents everywhere",
                        "confidence": 0.4,
                    }
                ],
                "claims": [
                    {
                        "claim_text": "AI coding agents are useful for routine software work.",
                        "claim_type": "technical_claim",
                        "confidence": 0.8,
                        "evidence": "AI coding agents are getting useful for routine software work",
                    },
                    {
                        "claim_text": "AI agents will replace all software teams.",
                        "claim_type": "prediction",
                        "confidence": 0.6,
                        "evidence": "replace all software teams",
                    },
                ],
                "entities": {"people": [], "organizations": ["IBM"], "products": []},
                "overall_confidence": 0.7,
                "needs_review": False,
                "review_reason": None,
            }
            repairs = repair_label_output_for_submission("ai_discourse_v1", output, segment_text=segment_text)
            self.assertGreaterEqual(repairs, 3)
            self.assertEqual(len(output["topics"]), 1)
            self.assertEqual(len(output["claims"]), 1)
            self.assertFalse(output["terminology_shifts"])
            self.assertFalse(output["entities"]["organizations"])
            self.assertTrue(output["needs_review"])
            validate_label_output("ai_discourse_v1", output, segment_text=segment_text)
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_identity_graph_normalizes_common_asr_aliases(self) -> None:
        from research_factory.identity_graph import normalize_identity_name

        self.assertEqual(normalize_identity_name("Ron Roy"), "ranjan_roy")
        self.assertEqual(normalize_identity_name("Ranjan Roy of Margins"), "ranjan_roy")
        self.assertEqual(normalize_identity_name("Brandon Anderson / Latent Space"), "brandon_anderson")
        self.assertEqual(normalize_identity_name("Alex Kantrowitz Big Technology"), "alex_kantrowitz")

    def test_reviewer_event_dedupe_catches_overlap_restatements(self) -> None:
        from research_factory.scale_ops import _review_events_are_near_duplicates

        left = {
            "event_type": "forecast",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times says OpenAI is leaning toward holding off its initial public offering until next year.",
            "evidence_text": "OpenAI is leaning towards holding off its initial public offering until next year.",
        }
        right = {
            "event_type": "forecast",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times reports that OpenAI is leaning toward waiting until next year for its IPO.",
            "evidence_text": "OpenAI leans toward waiting next until next year for its IPO.",
        }
        unrelated = {
            "event_type": "market_signal",
            "actor_name": "The New York Times",
            "claim_text": "The New York Times reports a different valuation signal about SpaceX.",
            "evidence_text": "SpaceX valuation talk is a different market thread.",
        }
        self.assertTrue(_review_events_are_near_duplicates(left, right))
        self.assertFalse(_review_events_are_near_duplicates(left, unrelated))

    def test_research_queue_backfills_content_and_envelopes(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")

        synced = self.run_cli("queue", "sync-envelopes")
        self.assertTrue(synced["ok"])
        status = self.run_cli("queue", "status", "--by", "lane,content_type,role,status")
        self.assertTrue(status["ok"])
        self.assertFalse(status["future_mcp"]["enabled"])
        queue_rows = status["queues"]
        self.assertTrue(any(row["content_type"] == "podcast_episode" for row in queue_rows))
        self.assertTrue(any(row["role"] in {"acquisition", "extractor"} for row in queue_rows))
        with self.db_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_sources").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM content_spans").fetchone()[0], 1)
            self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM queue_envelopes").fetchone()[0], 1)

    def test_worker_run_is_role_scoped_and_records_worker_run(self) -> None:
        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources))
        run = self.run_cli(
            "worker",
            "run",
            "--role",
            "acquisition",
            "--lane",
            "podcast",
            "--limit",
            "1",
            "--worker-id",
            "test-headless-acquisition",
            "--no-claim-prompts",
        )
        self.assertTrue(run["ok"])
        self.assertEqual(run["role"], "acquisition")
        self.assertEqual(run["processed"], 1)
        with self.db_conn() as conn:
            worker = conn.execute("SELECT worker_role, status, claimed_jobs FROM worker_runs WHERE id = ?", (run["worker_run_id"],)).fetchone()
        self.assertEqual(worker["worker_role"], "acquisition")
        self.assertEqual(worker["status"], "completed")
        self.assertEqual(worker["claimed_jobs"], 1)

    def test_worker_run_requires_burst_for_more_than_four_jobs(self) -> None:
        self.run_cli("init")
        result = self.run_cli_process("worker", "run", "--role", "acquisition", "--limit", "5")
        self.assertNotEqual(result.returncode, 0)

    def test_future_remote_queue_respects_privacy_tier(self) -> None:
        from research_factory import db

        self.run_cli("init")
        with self.db_conn() as conn:
            local_job = db.enqueue_job(
                conn,
                lane="research",
                job_type="label_segment",
                target_id="local-only-target",
                payload={"privacy_tier": "local_only", "content_type": "blog_post", "model": "gpt-5.5"},
            )
            remote_job = db.enqueue_job(
                conn,
                lane="research",
                job_type="label_segment",
                target_id="public-link-target",
                payload={"privacy_tier": "public_link_only", "content_type": "blog_post", "model": "gpt-5.5"},
            )
            conn.commit()
        self.run_cli("queue", "sync-envelopes")
        claimed = self.run_cli("remote-queue", "claim-job", "--worker-id", "future-chatgpt-task", "--capability", "extractor", "--max-items", "2")
        self.assertFalse(claimed["enabled"])
        self.assertEqual([item["job_id"] for item in claimed["claimed"]], [remote_job])
        self.assertNotIn(local_job, [item["job_id"] for item in claimed["claimed"]])

    def test_mcp_broker_rejects_missing_scope(self) -> None:
        from research_factory.mcp_broker import make_access_token, verify_access_token

        token = make_access_token(
            secret="test-secret",
            issuer="https://broker.test",
            audience="https://broker.test/mcp",
            subject="tester",
            scope="factory.status",
        )
        with self.assertRaises(PermissionError):
            verify_access_token(
                token,
                secret="test-secret",
                issuer="https://broker.test",
                audience="https://broker.test/mcp",
                required_scopes={"factory.control"},
            )

    def test_mcp_broker_snapshot_status_is_sanitized(self) -> None:
        from research_factory.mcp_broker import BrokerStore, status_from_snapshot

        store = BrokerStore(self.root / "broker.sqlite")
        store.write_snapshot(
            {
                "generated_at": "2026-07-06T12:00:00+00:00",
                "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
                "counts": {"jobs": 1},
                "active_jobs": [{"target_id": "secret-local-id"}],
                "research_queue_metrics": {
                    "active_remote_leases": 0,
                    "remote_claimable_by_privacy_tier": {"full_text_allowed": 1},
                    "remote_import_failures": 0,
                    "remote_output_submissions": [],
                },
            }
        )
        snapshot = store.latest_snapshot()
        self.assertNotIn("active_jobs", snapshot)
        status = status_from_snapshot(snapshot, store.control_request_counts())
        self.assertEqual(status["remote_claimable_by_privacy_tier"], {"full_text_allowed": 1})
        self.assertEqual(status["active_remote_leases"], 0)
        self.assertEqual(status["remote_import_failures"], 0)
        self.assertEqual(status["remote_output_submissions"], [])
        self.assertFalse(status["railway_compute_enabled"])
        store.close()

    def test_mcp_http_get_status_tool(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore, make_access_token

        store = BrokerStore(self.root / "broker-http.sqlite")
        store.write_snapshot(
            {
                "generated_at": "2026-07-06T12:00:00+00:00",
                "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
                "research_queue_metrics": {"remote_claimable_by_privacy_tier": {"full_text_allowed": 2}},
            }
        )
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            import urllib.error

            base = f"http://127.0.0.1:{server.server_port}"
            token = make_access_token(
                secret=mcp_server.AUTH_SECRET,
                issuer=base,
                audience=f"{base}/mcp",
                subject="chatgpt-test",
                scope="factory.status factory.control",
            )
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "get_status", "arguments": {}},
            }
            request = urllib.request.Request(
                f"{base}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                method="POST",
            )
            response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
            structured = response["result"]["structuredContent"]
            self.assertTrue(structured["ok"])
            self.assertEqual(structured["remote_claimable_by_privacy_tier"], {"full_text_allowed": 2})
            self.assertFalse(structured["phase2_tools_enabled"])
            self.assertEqual(structured["broker_storage"]["backend"], "sqlite")
            self.assertFalse(structured["broker_storage"]["durable"])
        finally:
            server.shutdown()
            server.server_close()
            store.close()

    def test_mcp_phase2_tools_are_hidden_when_disabled(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        previous = mcp_server.PHASE2_ENABLED
        mcp_server.PHASE2_ENABLED = False
        store = BrokerStore(self.root / "broker-phase2-hidden.sqlite")
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_port}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
            names = {tool["name"] for tool in response["result"]["tools"]}
            self.assertIn("get_status", names)
            self.assertNotIn("claim_work", names)
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous

    def test_mcp_phase2_smoke_claim_context_submit(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore, make_access_token

        previous = mcp_server.PHASE2_ENABLED
        previous_ingest = mcp_server.INGEST_TOKEN
        mcp_server.PHASE2_ENABLED = True
        mcp_server.INGEST_TOKEN = "test-ingest"
        store = BrokerStore(self.root / "broker-phase2-smoke.sqlite")
        store.seed_smoke_work_package()
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            token = make_access_token(
                secret=mcp_server.AUTH_SECRET,
                issuer=base,
                audience=f"{base}/mcp",
                subject="chatgpt-smoke",
                scope="factory.status factory.claim factory.submit",
            )

            def call_tool(name: str, arguments: dict) -> dict:
                payload = {"jsonrpc": "2.0", "id": name, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
                request = urllib.request.Request(
                    f"{base}/mcp",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                    method="POST",
                )
                response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
                return response["result"]

            list_request = urllib.request.Request(
                f"{base}/mcp",
                data=json.dumps({"jsonrpc": "2.0", "id": "tools", "method": "tools/list", "params": {}}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            listed = json.loads(urllib.request.urlopen(list_request, timeout=5).read().decode("utf-8"))
            tools = {tool["name"]: tool for tool in listed["result"]["tools"]}
            self.assertEqual(tools["reserve_evidence_task"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("no source text", tools["reserve_evidence_task"]["description"])
            self.assertTrue(tools["reserve_evidence_task"]["annotations"]["readOnlyHint"])
            self.assertEqual(tools["reserve_source_card_task"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("bounded source-card", tools["reserve_source_card_task"]["description"])
            self.assertTrue(tools["reserve_source_card_task"]["annotations"]["readOnlyHint"])
            self.assertEqual(tools["reserve_source_card_batch"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("up to three", tools["reserve_source_card_batch"]["description"])
            self.assertTrue(tools["reserve_source_card_batch"]["annotations"]["readOnlyHint"])
            self.assertEqual(tools["get_work_chunk"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertEqual(tools["get_evidence_manifest"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertEqual(tools["get_evidence_packet"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertEqual(tools["get_source_card"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("never returns full transcript chunks", tools["get_source_card"]["description"])
            self.assertEqual(tools["get_source_card_batch"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("up to three", tools["get_source_card_batch"]["description"])
            self.assertIn("does not return source text", tools["get_evidence_packet"]["description"])
            self.assertEqual(tools["submit_work_notes"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertEqual(tools["submit_work_output"]["securitySchemes"][0]["scopes"], ["factory.submit"])
            self.assertEqual(tools["submit_work_outputs"]["securitySchemes"][0]["scopes"], ["factory.submit"])
            self.assertEqual(tools["skip_source_card_task"]["securitySchemes"][0]["scopes"], ["factory.claim"])
            self.assertIn("skipped", tools["skip_source_card_task"]["description"])

            claimed = call_tool("reserve_evidence_task", {"worker_id": "mcp-test-worker"})
            self.assertEqual(len(claimed["structuredContent"]["claimed"]), 1)
            claimed_item = claimed["structuredContent"]["claimed"][0]
            work_id = claimed_item["work_id"]
            self.assertEqual(claimed_item["delivery_mode"], "remote_safe_evidence")
            self.assertFalse(claimed_item["raw_transcript_returned"])
            self.assertIn("first_packet_id", claimed_item)
            self.assertIn("required_fields", claimed_item["analysis_contract"])
            self.assertIn("episode_id", claimed_item["analysis_contract"]["required_fields"])
            context = call_tool("get_evidence_manifest", {"work_id": work_id, "worker_id": "mcp-test-worker"})
            context_payload = context["structuredContent"]
            self.assertEqual(context_payload["task_kind"], "safe_evidence_analysis")
            self.assertEqual(context_payload["delivery_mode"], "remote_safe_evidence")
            self.assertFalse(context_payload["raw_transcript_returned"])
            serialized_context = json.dumps(context_payload, sort_keys=True)
            self.assertNotIn("HOST: Today", serialized_context)
            self.assertNotIn("Read the entire prepared episode transcript", serialized_context)
            self.assertNotIn("full_text_allowed", serialized_context)
            self.assertLess(len(serialized_context), 2000)
            first_chunk_id = context_payload["first_packet_id"]
            self.assertEqual(first_chunk_id, claimed_item["first_packet_id"])
            chunk = call_tool("get_evidence_packet", {"work_id": work_id, "worker_id": "mcp-test-worker", "chunk_id": first_chunk_id})
            chunk_payload = chunk["structuredContent"]
            self.assertEqual(chunk_payload["delivery_mode"], "remote_safe_evidence")
            self.assertFalse(chunk_payload["raw_transcript_returned"])
            self.assertNotIn("chunk_text", chunk_payload)
            self.assertIn("Acme AI", chunk_payload["evidence"]["entity_candidates"])
            self.assertNotIn("HOST: Today", json.dumps(chunk_payload, sort_keys=True))
            self.assertNotIn("HOST: Today", chunk["content"][0]["text"])
            noted = call_tool(
                "submit_work_notes",
                {
                    "work_id": work_id,
                    "worker_id": "mcp-test-worker",
                    "chunk_id": first_chunk_id,
                    "notes": {"organizations": ["Acme AI"], "products": ["Studio model"], "uncertainties": ["synthetic fixture"]},
                },
            )
            self.assertTrue(noted["structuredContent"]["ok"])
            notes = call_tool("get_work_notes", {"work_id": work_id, "worker_id": "mcp-test-worker"})
            self.assertEqual(notes["structuredContent"]["covered_chunk_count"], 1)
            submitted = call_tool(
                "submit_work_output",
                {
                    "work_id": work_id,
                    "worker_id": "mcp-test-worker",
                    "output": {
                        "schema_version": "pif_remote_smoke_v1",
                        "organizations": ["Acme AI"],
                        "products": ["Studio model"],
                        "signals": [{"type": "release", "summary": "Synthetic launch signal"}],
                        "uncertainties": ["Synthetic fixture has no external corroboration"],
                    },
                },
            )
            self.assertTrue(submitted["structuredContent"]["validation"]["ok"])
            status = call_tool("get_status", {})
            self.assertTrue(status["structuredContent"]["phase2_tools_enabled"])
            self.assertEqual(status["structuredContent"]["remote_work_smoke"]["counts"]["submitted"], 1)
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous

    def test_mcp_released_work_is_not_immediately_reclaimed(self) -> None:
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-release-not-reclaimed.sqlite")
        try:
            store.seed_smoke_work_package()
            claimed = store.claim_work(worker_id="worker-one")
            work_id = claimed["claimed"][0]["work_id"]
            released = store.release_work(work_id=work_id, worker_id="worker-one", reason="incomplete coverage")
            self.assertEqual(released["status"], "released")

            reclaimed = store.claim_work(worker_id="worker-two")
            self.assertEqual(reclaimed["claimed"], [])
            summary = store.remote_work_summary()
            self.assertEqual(summary["counts"].get("released"), 1)
        finally:
            store.close()

    def test_mcp_stale_claimed_work_is_reclaimable(self) -> None:
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-stale-lease-reclaimed.sqlite")
        try:
            store.seed_smoke_work_package()
            claimed = store.claim_work(worker_id="worker-one")
            work_id = claimed["claimed"][0]["work_id"]
            store._execute(
                "UPDATE remote_work_packages SET leased_until = ? WHERE id = ?",
                ("2000-01-01T00:00:00+00:00", work_id),
            )
            store.conn.commit()

            reclaimed = store.claim_work(worker_id="worker-two")
            self.assertEqual(reclaimed["claimed"][0]["work_id"], work_id)
            self.assertEqual(reclaimed["claimed"][0]["status"], "claimed")
            events = store.audit_events(limit=5)
            self.assertTrue(any(event["event_type"] == "remote_work_lease_expired" for event in events["events"]))
        finally:
            store.close()

    def test_source_card_reservation_ignores_non_source_card_work(self) -> None:
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-source-card-only.sqlite")
        try:
            packages = [
                {
                    "id": "job_generic",
                    "title": "generic package",
                    "privacy_tier": "full_text_allowed",
                    "context": {
                        "task": "episode_context",
                        "job_id": 1,
                        "job_type": "episode_context",
                        "episode_id": "generic-episode",
                        "label_pack": "ai_discourse_v3_1",
                        "model_required": "gpt-5.5",
                        "prompt_text": "# Instructions\nNo transcript marker here.",
                    },
                    "output_schema": {"type": "episode_context_output", "schema_version": "ai_discourse_v3_1_episode_context"},
                },
                {
                    "id": "job_source_card",
                    "title": "source card package",
                    "privacy_tier": "full_text_allowed",
                    "context": {
                        "task": "episode_context",
                        "job_id": 2,
                        "job_type": "episode_context",
                        "episode_id": "source-card-episode",
                        "label_pack": "ai_discourse_v3_1",
                        "model_required": "gpt-5.5",
                        "prompt_text": (
                            "# Instructions\nExtract context.\n"
                            "# Full Prepared Episode Transcript\n"
                            "HOST: Acme AI discusses enterprise agent adoption and model deployment in 2026.\n"
                            "# Output Contract\nReturn JSON."
                        ),
                    },
                    "output_schema": {"type": "episode_context_output", "schema_version": "ai_discourse_v3_1_episode_context"},
                },
            ]
            store.upsert_work_packages(packages)
            claimed = store.reserve_source_card_task(worker_id="source-card-worker")
            self.assertEqual(claimed["claimed"][0]["work_id"], "job_source_card")
            generic = store.claim_work(worker_id="generic-worker")
            self.assertEqual(generic["claimed"][0]["work_id"], "job_generic")
        finally:
            store.close()

    def test_skip_source_card_task_exports_sanitized_skip(self) -> None:
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-source-card-skip.sqlite")
        try:
            store.upsert_work_packages(
                [
                    {
                        "id": "job_skip",
                        "title": "skip package",
                        "privacy_tier": "full_text_allowed",
                        "context": {
                            "task": "episode_context",
                            "job_id": 3,
                            "job_type": "episode_context",
                            "episode_id": "skip-episode",
                            "label_pack": "ai_discourse_v3_1",
                            "model_required": "gpt-5.5",
                            "prompt_text": (
                                "# Instructions\nExtract context.\n"
                                "# Full Prepared Episode Transcript\n"
                                "HOST: Acme AI discusses enterprise agent adoption and model deployment in 2026.\n"
                                "# Output Contract\nReturn JSON."
                            ),
                        },
                        "output_schema": {"type": "episode_context_output", "schema_version": "ai_discourse_v3_1_episode_context"},
                    }
                ]
            )
            claimed = store.reserve_source_card_task(worker_id="skip-worker")
            card_id = claimed["claimed"][0]["first_card_id"]
            skipped = store.skip_source_card_task(
                work_id="job_skip",
                worker_id="skip-worker",
                card_id=card_id,
                stage="get_source_card",
                reason_code="tool_safety_block",
            )
            self.assertEqual(skipped["status"], "skipped")
            self.assertEqual(store.claim_work(worker_id="other-worker")["claimed"], [])
            pending = store.pending_skips()
            self.assertEqual(pending["skips"][0]["work_id"], "job_skip")
            self.assertEqual(pending["skips"][0]["validation"]["reason_code"], "tool_safety_block")
            serialized = json.dumps(pending, sort_keys=True)
            self.assertNotIn("Acme AI discusses", serialized)
            self.assertNotIn("Full Prepared Episode Transcript", serialized)
            marked = store.mark_submission_imported(work_id="job_skip", status="skip_imported", validation={"ok": True})
            self.assertEqual(marked["status"], "skip_imported")
        finally:
            store.close()

    def test_mcp_phase2_chunk_and_submit_tools_reject_missing_scopes(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore, make_access_token

        previous = mcp_server.PHASE2_ENABLED
        mcp_server.PHASE2_ENABLED = True
        store = BrokerStore(self.root / "broker-phase2-scope-reject.sqlite")
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            token = make_access_token(
                secret=mcp_server.AUTH_SECRET,
                issuer=base,
                audience=f"{base}/mcp",
                subject="chatgpt-scope-test",
                scope="factory.status",
            )

            def call_tool(name: str, arguments: dict) -> dict:
                payload = {"jsonrpc": "2.0", "id": name, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
                request = urllib.request.Request(
                    f"{base}/mcp",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                    method="POST",
                )
                try:
                    return json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
                except urllib.error.HTTPError as exc:
                    return json.loads(exc.read().decode("utf-8"))

            for name, arguments in [
                ("claim_work", {}),
                ("reserve_source_card_task", {}),
                ("reserve_source_card_batch", {}),
                ("get_work_context", {"work_id": "job_1"}),
                ("get_work_chunk", {"work_id": "job_1", "chunk_id": "chunk_1"}),
                ("get_source_card", {"work_id": "job_1", "card_id": "card_1"}),
                ("get_source_card_batch", {"items": [{"work_id": "job_1", "card_id": "card_1"}]}),
                ("submit_work_notes", {"work_id": "job_1", "chunk_id": "chunk_1", "notes": {}}),
                ("get_work_notes", {"work_id": "job_1"}),
                ("skip_source_card_task", {"work_id": "job_1", "stage": "get_source_card", "reason_code": "tool_safety_block"}),
                ("submit_work_output", {"work_id": "job_1", "output": {"schema_version": "x"}}),
                ("submit_work_outputs", {"outputs": [{"work_id": "job_1", "output": {"schema_version": "x"}}]}),
            ]:
                response = call_tool(name, arguments)
                self.assertEqual(response["error"]["message"], "missing required scope")
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous

    def test_mcp_phase2_smoke_can_use_control_scope_when_compat_enabled(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore, make_access_token

        previous_phase2 = mcp_server.PHASE2_ENABLED
        previous_scope = mcp_server.PHASE2_SMOKE_ALLOW_CONTROL_SCOPE
        mcp_server.PHASE2_ENABLED = True
        mcp_server.PHASE2_SMOKE_ALLOW_CONTROL_SCOPE = True
        store = BrokerStore(self.root / "broker-phase2-control-scope.sqlite")
        store.seed_smoke_work_package()
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            token = make_access_token(
                secret=mcp_server.AUTH_SECRET,
                issuer=base,
                audience=f"{base}/mcp",
                subject="chatgpt-smoke",
                scope="factory.status factory.control",
            )
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "claim_work", "arguments": {"worker_id": "mcp-control-worker"}},
            }
            request = urllib.request.Request(
                f"{base}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                method="POST",
            )
            response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
            self.assertEqual(len(response["result"]["structuredContent"]["claimed"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous_phase2
            mcp_server.PHASE2_SMOKE_ALLOW_CONTROL_SCOPE = previous_scope

    def test_mcp_phase2_smoke_can_use_status_scope_when_compat_enabled(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore, make_access_token

        previous_phase2 = mcp_server.PHASE2_ENABLED
        previous_scope = mcp_server.PHASE2_SMOKE_ALLOW_STATUS_SCOPE
        mcp_server.PHASE2_ENABLED = True
        mcp_server.PHASE2_SMOKE_ALLOW_STATUS_SCOPE = True
        store = BrokerStore(self.root / "broker-phase2-status-scope.sqlite")
        store.seed_smoke_work_package()
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            token = make_access_token(
                secret=mcp_server.AUTH_SECRET,
                issuer=base,
                audience=f"{base}/mcp",
                subject="chatgpt-smoke",
                scope="factory.status",
            )
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "claim_work", "arguments": {"worker_id": "mcp-status-worker"}},
            }
            request = urllib.request.Request(
                f"{base}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                method="POST",
            )
            response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
            self.assertEqual(len(response["result"]["structuredContent"]["claimed"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous_phase2
            mcp_server.PHASE2_SMOKE_ALLOW_STATUS_SCOPE = previous_scope

    def test_mcp_real_episode_context_package_imports_locally(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        sources = self.write_fixture_source()
        self.run_cli("init")
        self.run_cli("enqueue", "--lane", "podcast", "--since", "2026-06-01", "--source-list", str(sources), "--label-pack", "ai_discourse_v3_1")
        self.run_cli("run", "--lane", "podcast", "--limit", "1", "--job-types", "fetch_transcript", "--no-claim-prompts", "--max-label-prompts", "0")
        self.run_cli(
            "prepare-transcripts",
            "--category",
            "tests",
            "--limit",
            "1",
            "--label-pack",
            "ai_discourse_v3_1",
            "--force",
            "--enqueue-labels",
            "--include-low-signal",
            "--priority",
            "10",
            "--pilot-id",
            "remote-real-test",
        )
        waiting = self.run_cli("claim", "--lane", "podcast", "--label-pack", "ai_discourse_v3_1", "--model", "gpt-5.5", "--worker-id", "codex-test")
        context_job_id = int(waiting["episode_context_job_id"])
        with self.db_conn() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (context_job_id,)).fetchone()
            payload = json.loads(job["payload_json"])
            payload["privacy_tier"] = "full_text_allowed"
            conn.execute("UPDATE jobs SET payload_json = ? WHERE id = ?", (json.dumps(payload), context_job_id))
            conn.commit()

        previous = mcp_server.PHASE2_ENABLED
        previous_ingest = mcp_server.INGEST_TOKEN
        mcp_server.PHASE2_ENABLED = True
        mcp_server.INGEST_TOKEN = "test-ingest"
        store = BrokerStore(self.root / "broker-real-episode-context.sqlite")
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            published = self.run_cli(
                "mcp-broker",
                "publish-work-packages",
                "--url",
                base,
                "--token",
                "test-ingest",
                "--limit",
                "1",
                "--worker-id",
                "mcp-test-bridge",
            )
            self.assertTrue(published["ok"])
            self.assertEqual(published["published"], 1)
            claimed = store.reserve_source_card_task(worker_id="chatgpt-real-test")
            self.assertEqual(claimed["claimed"][0]["work_id"], f"job_{context_job_id}")
            self.assertEqual(claimed["claimed"][0]["delivery_mode"], "bounded_source_card")
            self.assertFalse(claimed["claimed"][0]["raw_transcript_returned"])
            first_card_id = claimed["claimed"][0]["first_card_id"]
            self.assertTrue(first_card_id)
            self.assertEqual(claimed["claimed"][0]["source_card_policy"]["max_excerpt_chars"], 500)
            source_card = store.get_source_card(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test", card_id=first_card_id)
            self.assertEqual(source_card["delivery_mode"], "bounded_source_card")
            self.assertFalse(source_card["raw_transcript_returned"])
            self.assertEqual(source_card["source_text_returned"], "features_only_no_continuous_transcript")
            self.assertLessEqual(source_card["card"]["excerpt_char_count"], 500)
            self.assertNotIn("excerpt_text", source_card["card"])
            self.assertFalse(source_card["card"]["excerpt_text_returned"])
            self.assertIn("offset_start", source_card["card"])
            self.assertIn("source_sha256", source_card["card"])
            self.assertTrue(source_card["card"]["safety"]["no_continuous_transcript_text"])
            self.assertIn("controlled_concept_tags", source_card["card"])
            self.assertIn("feature_counts", source_card["card"])
            self.assertNotIn("entity_candidates", source_card["card"])
            self.assertNotIn("concept_candidates", source_card["card"])
            serialized_card = json.dumps(source_card, sort_keys=True)
            self.assertNotIn("WEBVTT", serialized_card)
            self.assertNotIn("===== SEGMENT", serialized_card)
            self.assertNotIn("Full Prepared Episode Transcript", serialized_card)
            card_note = store.submit_work_notes(
                work_id=f"job_{context_job_id}",
                worker_id="chatgpt-real-test",
                chunk_id=first_card_id,
                notes={"source_card": True, "concepts": source_card["card"]["controlled_concept_tags"][:3]},
            )
            self.assertTrue(card_note["ok"])
            context = store.get_work_context(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test")
            serialized_context = json.dumps(context, sort_keys=True)
            self.assertNotIn("Full Prepared Episode Transcript", serialized_context)
            self.assertNotIn("WEBVTT", serialized_context)
            self.assertNotIn("Read the entire prepared episode transcript", serialized_context)
            self.assertLess(len(serialized_context), 10000)
            chunks = context["context"]["chunk_manifest"]["chunks"]
            self.assertGreaterEqual(len(chunks), 1)
            fetched_chunks = []
            chunk_id = context["context"]["first_chunk_id"]
            while chunk_id:
                chunk = store.get_work_chunk(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test", chunk_id=chunk_id)
                fetched_chunks.append(chunk)
                self.assertLessEqual(chunk["char_count"], 8000)
                self.assertEqual(chunk["delivery_mode"], "remote_safe_evidence")
                self.assertFalse(chunk["raw_transcript_returned"])
                self.assertNotIn("chunk_text", chunk)
                chunk_id = chunk["next_chunk_id"]
            self.assertEqual(len(fetched_chunks), len(chunks))
            serialized_chunks = json.dumps(fetched_chunks, sort_keys=True)
            self.assertNotIn("The guest says AGI timelines", serialized_chunks)
            self.assertIn("agi", serialized_chunks.lower())
            noted = store.submit_work_notes(
                work_id=f"job_{context_job_id}",
                worker_id="chatgpt-real-test",
                chunk_id=chunks[0]["chunk_id"],
                notes={"speakers": ["guest", "host"], "concepts": ["AGI timelines"], "quality": ["fixture"]},
            )
            self.assertTrue(noted["ok"])
            notes = store.get_work_notes(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test")
            self.assertEqual(notes["covered_chunk_count"], 1)
            self.assertEqual(notes["covered_source_card_count"], 1)
            with self.assertRaises(ValueError):
                store.submit_work_notes(
                    work_id=f"job_{context_job_id}",
                    worker_id="chatgpt-real-test",
                    chunk_id=chunks[0]["chunk_id"],
                    notes={"raw": "===== SEGMENT 1 =====\n" + ("x" * 7000)},
                )
            with self.db_conn() as conn:
                episode_id = conn.execute("SELECT target_id FROM jobs WHERE id = ?", (context_job_id,)).fetchone()["target_id"]
            invalid_output = {"schema_version": "3.1", "needs_review": True}
            invalid_submitted = store.submit_work_output(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test", output=invalid_output)
            self.assertFalse(invalid_submitted["validation"]["ok"])
            self.assertIn("schema_version must be", "; ".join(invalid_submitted["validation"]["errors"]))
            output = self.valid_episode_context_output(episode_id)
            submitted = store.submit_work_output(work_id=f"job_{context_job_id}", worker_id="chatgpt-real-test", output=output)
            self.assertTrue(submitted["validation"]["ok"])
            imported = self.run_cli(
                "mcp-broker",
                "import-submissions",
                "--url",
                base,
                "--token",
                "test-ingest",
                "--limit",
                "5",
            )
            self.assertTrue(imported["ok"])
            self.assertEqual(imported["imported"], 1)
            audit = self.run_cli("mcp-broker", "audit-events", "--url", base, "--token", "test-ingest", "--limit", "20")
            self.assertTrue(audit["ok"])
            serialized_audit = json.dumps(audit, sort_keys=True)
            self.assertIn("remote_work_submitted", serialized_audit)
            self.assertNotIn("Full Prepared Episode Transcript", serialized_audit)
            with self.db_conn() as conn:
                completed = conn.execute("SELECT status FROM jobs WHERE id = ?", (context_job_id,)).fetchone()["status"]
                self.assertEqual(completed, "completed")
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            mcp_server.PHASE2_ENABLED = previous
            mcp_server.INGEST_TOKEN = previous_ingest

    def test_mcp_source_card_batch_claim_fetch_submit(self) -> None:
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-source-card-batch.sqlite")
        try:
            packages = []
            for index in range(3):
                episode_id = f"episode-batch-{index}"
                transcript = (
                    f"HOST: Today we discuss Acme AI batch signal {index} and enterprise AI workflows. "
                    f"GUEST: The important concept is agent adoption, inference scaling, and model deployment in 2026."
                )
                packages.append(
                    {
                        "id": f"job_batch_{index}",
                        "title": f"batch source-card job {index}",
                        "privacy_tier": "full_text_allowed",
                        "context": {
                            "task": "episode_context",
                            "job_id": index,
                            "job_type": "episode_context",
                            "episode_id": episode_id,
                            "label_pack": "ai_discourse_v3_1",
                            "model_required": "gpt-5.5",
                            "prompt_text": (
                                "# Instructions\nExtract compact episode context.\n"
                                "# Full Prepared Episode Transcript\n"
                                f"{transcript}\n"
                                "# Output Contract\nReturn JSON only."
                            ),
                        },
                        "output_schema": {"type": "episode_context_output", "schema_version": "ai_discourse_v3_1_episode_context"},
                    }
                )
            upserted = store.upsert_work_packages(packages)
            self.assertTrue(upserted["ok"])
            self.assertEqual(upserted["accepted"], 3)

            claimed = store.reserve_source_card_batch(worker_id="chatgpt-batch-test", max_tasks=3)
            self.assertEqual(len(claimed["claimed"]), 3)
            self.assertEqual(claimed["batch_policy"]["max_tasks"], 3)
            self.assertEqual(claimed["batch_policy"]["turn_success_target"], 6)
            self.assertEqual(claimed["batch_policy"]["reserve_attempt_cap"], 12)
            self.assertEqual(claimed["batch_policy"]["skip_tool"], "skip_source_card_task")
            items = [{"work_id": item["work_id"], "card_id": item["first_card_id"]} for item in claimed["claimed"]]
            self.assertTrue(all(item["card_id"] for item in items))

            cards = store.get_source_card_batch(worker_id="chatgpt-batch-test", items=items)
            self.assertTrue(cards["ok"])
            self.assertEqual(cards["card_count"], 3)
            self.assertFalse(cards["raw_transcript_returned"])
            self.assertEqual(cards["source_text_returned"], "features_only_no_continuous_transcript")
            self.assertIn("claim_abstract", cards["cards"][0]["card"])
            self.assertIn("topic_tags", cards["cards"][0]["card"])
            self.assertIn("candidate_relation_types", cards["cards"][0]["card"])
            serialized_cards = json.dumps(cards, sort_keys=True)
            self.assertNotIn('"excerpt_text":', serialized_cards)
            self.assertNotIn("HOST: Today", serialized_cards)
            self.assertNotIn("Full Prepared Episode Transcript", serialized_cards)

            outputs = []
            for item in claimed["claimed"]:
                output = self.valid_episode_context_output(str(item["analysis_contract"]["target_episode_id"]))
                outputs.append({"work_id": item["work_id"], "output": output})
            submitted = store.submit_work_outputs(worker_id="chatgpt-batch-test", outputs=outputs)
            self.assertTrue(submitted["ok"])
            self.assertEqual(submitted["submitted_count"], 3)
            self.assertEqual(submitted["validation_ok_count"], 3)

            with self.assertRaises(ValueError):
                store.reserve_source_card_batch(worker_id="chatgpt-batch-test", max_tasks=99)
            with self.assertRaises(ValueError):
                store.get_source_card_batch(worker_id="chatgpt-batch-test", items=items + [items[0]])
            with self.assertRaises(ValueError):
                store.submit_work_outputs(worker_id="chatgpt-batch-test", outputs=outputs + [outputs[0]])
        finally:
            store.close()

    def test_mcp_tools_list_is_discoverable_without_bearer_token(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-discovery.sqlite")
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_port}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            response = json.loads(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
            self.assertIn("tools", response["result"])
            tools = {tool["name"]: tool for tool in response["result"]["tools"]}
            self.assertIn("get_status", tools)
            self.assertEqual(
                tools["get_status"]["annotations"],
                {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
            )
            for tool in tools.values():
                self.assertIn("outputSchema", tool)
                self.assertIn("securitySchemes", tool)
                self.assertIn("_meta", tool)
                self.assertEqual(tool["_meta"]["securitySchemes"], tool["securitySchemes"])
            self.assertEqual(tools["get_status"]["securitySchemes"][0]["scopes"], ["factory.status"])
            self.assertEqual(tools["request_local_cycle"]["securitySchemes"][0]["scopes"], ["factory.control"])
            self.assertFalse(tools["request_local_cycle"]["annotations"]["destructiveHint"])
        finally:
            server.shutdown()
            server.server_close()
            store.close()

    def test_mcp_oauth_authorize_token_flow(self) -> None:
        import base64
        import hashlib
        import secrets
        import urllib.error
        import urllib.parse

        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        store = BrokerStore(self.root / "broker-oauth.sqlite")
        mcp_server.Handler.store = store
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            verifier = secrets.token_urlsafe(32)
            challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
            redirect_uri = "https://chatgpt.test/callback"
            query = urllib.parse.urlencode(
                {
                    "response_type": "code",
                    "client_id": "chatgpt-test",
                    "redirect_uri": redirect_uri,
                    "scope": "factory.status factory.control factory.claim",
                    "state": "state-1",
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                }
            )

            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None

            opener = urllib.request.build_opener(NoRedirect)
            with self.assertRaises(urllib.error.HTTPError) as raised:
                opener.open(f"{base}/oauth/authorize?{query}", timeout=5)
            self.assertEqual(raised.exception.code, 302)
            location = raised.exception.headers["Location"]
            code = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)["code"][0]
            body = urllib.parse.urlencode(
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "code_verifier": verifier,
                }
            ).encode()
            token_response = json.loads(
                urllib.request.urlopen(
                    urllib.request.Request(
                        f"{base}/oauth/token",
                        data=body,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        method="POST",
                    ),
                    timeout=5,
                )
                .read()
                .decode("utf-8")
            )
            self.assertEqual(token_response["token_type"], "Bearer")
            self.assertEqual(token_response["scope"], "factory.control factory.status")
            self.assertIn("refresh_token", token_response)
            refresh_body = urllib.parse.urlencode(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": token_response["refresh_token"],
                }
            ).encode()
            refresh_response = json.loads(
                urllib.request.urlopen(
                    urllib.request.Request(
                        f"{base}/oauth/token",
                        data=refresh_body,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        method="POST",
                    ),
                    timeout=5,
                )
                .read()
                .decode("utf-8")
            )
            self.assertEqual(refresh_response["token_type"], "Bearer")
            self.assertEqual(refresh_response["scope"], "factory.control factory.status")
            self.assertIn("refresh_token", refresh_response)
            self.assertNotEqual(refresh_response["refresh_token"], token_response["refresh_token"])
        finally:
            server.shutdown()
            server.server_close()
            store.close()

    def test_mcp_bridge_publishes_sanitized_snapshot(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        self.run_cli("init")
        store = BrokerStore(self.root / "broker-bridge.sqlite")
        mcp_server.Handler.store = store
        old_token = mcp_server.INGEST_TOKEN
        mcp_server.INGEST_TOKEN = "test-ingest"
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = self.run_cli("mcp-broker", "publish-snapshot", "--url", f"http://127.0.0.1:{server.server_port}", "--token", "test-ingest")
            self.assertTrue(result["ok"])
            snapshot = store.latest_snapshot()
            self.assertEqual(snapshot["privacy"], "sanitized_broker_state_no_raw_transcripts")
            self.assertNotIn("active_jobs", snapshot)
            self.assertIn("queues", snapshot)
            self.assertIn("runs", snapshot)
            self.assertIn("failures", snapshot)
        finally:
            server.shutdown()
            server.server_close()
            mcp_server.INGEST_TOKEN = old_token
            store.close()

    def test_mcp_bridge_publishes_fast_status_snapshot(self) -> None:
        from http.server import ThreadingHTTPServer

        from research_factory import mcp_server
        from research_factory.mcp_broker import BrokerStore

        self.run_cli("init")
        store = BrokerStore(self.root / "broker-fast-status.sqlite")
        mcp_server.Handler.store = store
        old_token = mcp_server.INGEST_TOKEN
        mcp_server.INGEST_TOKEN = "test-ingest"
        server = ThreadingHTTPServer(("127.0.0.1", 0), mcp_server.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = self.run_cli("mcp-broker", "publish-status", "--url", f"http://127.0.0.1:{server.server_port}", "--token", "test-ingest")
            self.assertTrue(result["ok"])
            snapshot = store.latest_snapshot()
            self.assertEqual(snapshot["privacy"], "sanitized_broker_state_no_raw_transcripts")
            self.assertEqual(snapshot["snapshot_health"]["mode"], "mcp_broker_status_fast")
            self.assertIn("queues", snapshot)
            self.assertIn("runs", snapshot)
            self.assertIn("failures", snapshot)
            self.assertNotIn("research_queue_metrics", snapshot)
            serialized = json.dumps(snapshot)
            self.assertNotIn("Full Prepared Episode Transcript", serialized)
        finally:
            server.shutdown()
            server.server_close()
            mcp_server.INGEST_TOKEN = old_token
            store.close()

    def test_orchestrator_skips_when_pipeline_lock_is_busy(self) -> None:
        self.run_cli("init")
        path = Path(self.env["RESEARCH_FACTORY_DB"]).with_suffix(Path(self.env["RESEARCH_FACTORY_DB"]).suffix + ".pipeline.lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                result = self.run_cli("orchestrator", "run", "--cycle", "bridge_sync")
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        self.assertFalse(result["ok"])
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "pipeline_lock_busy")

    def test_orchestrator_bridge_fast_skips_full_snapshot(self) -> None:
        self.run_cli("init")
        result = self.run_cli("orchestrator", "run", "--cycle", "bridge_fast")
        self.assertTrue(result["ok"])
        self.assertEqual(result["cycle"], "bridge_fast")
        steps = [step["step"] for step in result["steps"]]
        self.assertIn("sync_queue_envelopes", steps)
        self.assertNotIn("snapshot", steps)
        self.assertNotIn("publish_observer", steps)
        self.assertNotIn("publish_mcp_broker", steps)

    def test_production_cycle_observer_only_is_local_first(self) -> None:
        self.run_cli("init")
        result = self.run_cli("production-cycle", "--worker-mode", "none")
        self.assertTrue(result["ok"])
        self.assertFalse(result["published"])
        self.assertFalse(result["railway_compute_enabled"])
        self.assertFalse(result["mcp_remote_worker_enabled"])
        self.assertTrue(any(step["step"] == "sync_queue_envelopes" for step in result["steps"]))
        self.assertTrue(any(step["step"] == "privacy_scan" and step["ok"] for step in result["steps"]))
        snapshot_step = next(step for step in result["steps"] if step["step"] == "snapshot")
        self.assertTrue(Path(snapshot_step["path"]).exists())

    def test_railway_cost_guard_allows_only_observer_footprint(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"id": "svc", "name": "observer-ui"}}]},
            "environments": {
                "edges": [
                    {
                        "node": {
                            "serviceInstances": {
                                "edges": [
                                    {
                                        "node": {
                                            "serviceName": "observer-ui",
                                            "nextCronRunAt": None,
                                            "volumeInstances": {"edges": []},
                                            "latestDeployment": {
                                                "meta": {
                                                    "volumeMounts": [],
                                                    "serviceManifest": {
                                                        "deploy": {
                                                            "cronSchedule": None,
                                                            "numReplicas": 1,
                                                            "multiRegionConfig": {},
                                                            "startCommand": "python3 -m research_factory.ui_server",
                                                            "preDeployCommand": None,
                                                            "sleepApplication": False,
                                                        }
                                                    },
                                                }
                                            },
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
        }
        path = self.root / "railway-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path))
        self.assertTrue(result["ok"])
        self.assertEqual(result["services"], ["observer-ui"])
        self.assertTrue(result["warnings"])

    def test_railway_cost_guard_blocks_compute_like_service(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"name": "observer-ui"}}, {"node": {"name": "extractor-worker"}}]},
            "environments": {"edges": []},
        }
        path = self.root / "bad-railway-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path))
        self.assertFalse(result["ok"])
        self.assertTrue(any("Unexpected Railway services" in issue["message"] for issue in result["issues"]))

    def test_railway_cost_guard_allows_explicit_mcp_broker_service(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"name": "observer-ui"}}, {"node": {"name": "mcp-broker"}}]},
            "environments": {
                "edges": [
                    {
                        "node": {
                            "serviceInstances": {
                                "edges": [
                                    {
                                        "node": {
                                            "serviceName": "observer-ui",
                                            "latestDeployment": {
                                                "meta": {
                                                    "serviceManifest": {"deploy": {"startCommand": "python3 -m research_factory.ui_server"}}
                                                }
                                            },
                                        }
                                    },
                                    {
                                        "node": {
                                            "serviceName": "mcp-broker",
                                            "latestDeployment": {
                                                "meta": {
                                                    "serviceManifest": {"deploy": {"startCommand": "python3 -m research_factory.mcp_server"}}
                                                }
                                            },
                                        }
                                    },
                                ]
                            }
                        }
                    }
                ]
            },
        }
        path = self.root / "mcp-railway-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path), "--allowed-service", "mcp-broker")
        self.assertTrue(result["ok"])
        self.assertEqual(result["policy"]["allowed_services"], ["mcp-broker", "observer-ui"])
        self.assertIn("python3 -m research_factory.mcp_server", result["policy"]["allowed_start_commands"]["mcp-broker"])
        self.assertIn("PIF_RAILWAY_MODULE", " ".join(result["policy"]["allowed_start_commands"]["mcp-broker"]))

    def test_railway_cost_guard_allows_explicit_postgres_storage(self) -> None:
        fixture = {
            "name": "podcast-intelligence-observer",
            "buckets": {"edges": []},
            "services": {"edges": [{"node": {"name": "observer-ui"}}, {"node": {"name": "mcp-broker"}}, {"node": {"name": "Postgres"}}]},
            "environments": {
                "edges": [
                    {
                        "node": {
                            "serviceInstances": {
                                "edges": [
                                    {
                                        "node": {
                                            "serviceName": "observer-ui",
                                            "latestDeployment": {
                                                "meta": {
                                                    "serviceManifest": {"deploy": {"startCommand": "python3 -m research_factory.ui_server"}},
                                                    "volumeMounts": [],
                                                }
                                            },
                                        }
                                    },
                                    {
                                        "node": {
                                            "serviceName": "mcp-broker",
                                            "latestDeployment": {
                                                "meta": {
                                                    "serviceManifest": {"deploy": {"startCommand": "python3 -m research_factory.mcp_server"}},
                                                    "volumeMounts": [],
                                                }
                                            },
                                        }
                                    },
                                    {
                                        "node": {
                                            "serviceName": "Postgres",
                                            "volumeInstances": {"edges": [{"node": {"id": "pg-volume"}}]},
                                            "latestDeployment": {
                                                "meta": {
                                                    "serviceManifest": {"deploy": {"numReplicas": 1, "sleepApplication": False}},
                                                    "volumeMounts": [{"mountPath": "/var/lib/postgresql/data"}],
                                                }
                                            },
                                        }
                                    },
                                ]
                            }
                        }
                    }
                ]
            },
        }
        path = self.root / "mcp-railway-postgres-status.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        result = self.run_cli("railway-cost-guard", "--status-json", str(path), "--allowed-service", "mcp-broker", "--allowed-service", "Postgres")
        self.assertTrue(result["ok"])
        self.assertEqual(result["policy"]["allowed_managed_storage_services"], ["Postgres"])

    def test_tech_discourse_v1_schema_validates_example(self) -> None:
        old_root = os.environ.get("RESEARCH_FACTORY_ROOT")
        os.environ["RESEARCH_FACTORY_ROOT"] = str(self.root)
        try:
            from research_factory.labels import validate_label_output

            example = json.loads((self.root / "label_packs" / "tech_discourse_v1" / "examples.json").read_text(encoding="utf-8"))[0]
            validate_label_output("tech_discourse_v1", example["output"], segment_text=example["input"]["text"])
        finally:
            if old_root is None:
                os.environ.pop("RESEARCH_FACTORY_ROOT", None)
            else:
                os.environ["RESEARCH_FACTORY_ROOT"] = old_root

    def test_queue_claim_respects_lease_and_attempt_budget_across_connections(self) -> None:
        from research_factory import db
        from research_factory.worker import claim_next_job

        Path(self.env["RESEARCH_FACTORY_DB"]).parent.mkdir(parents=True, exist_ok=True)
        conn_a = db.connect(Path(self.env["RESEARCH_FACTORY_DB"]))
        conn_b = db.connect(Path(self.env["RESEARCH_FACTORY_DB"]))
        try:
            db.init_db(conn_a)
            job_id = db.enqueue_job(conn_a, lane="podcast", job_type="fetch_transcript", target_id="episode-1", payload={}, max_attempts=2)
            conn_a.commit()
            first = claim_next_job(conn_a, lane="podcast", worker_id="worker-a", job_types=("fetch_transcript",))
            self.assertEqual(first["id"], job_id)
            self.assertIsNone(claim_next_job(conn_b, lane="podcast", worker_id="worker-b", job_types=("fetch_transcript",)))

            conn_a.execute("UPDATE jobs SET leased_until = '2000-01-01T00:00:00+00:00' WHERE id = ?", (job_id,))
            conn_a.commit()
            second = claim_next_job(conn_b, lane="podcast", worker_id="worker-b", job_types=("fetch_transcript",))
            self.assertEqual(second["id"], job_id)
            self.assertEqual(second["lease_owner"], "worker-b")

            conn_b.execute("UPDATE jobs SET status = 'pending', lease_owner = NULL, leased_until = NULL, attempts = max_attempts WHERE id = ?", (job_id,))
            conn_b.commit()
            self.assertIsNone(claim_next_job(conn_a, lane="podcast", worker_id="worker-a", job_types=("fetch_transcript",)))
        finally:
            conn_a.close()
            conn_b.close()

    def test_init_db_reports_clear_concurrent_initialization_conflict(self) -> None:
        db_path = Path(self.env["RESEARCH_FACTORY_DB"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = db_path.with_suffix(db_path.suffix + ".init.lock")
        self.env["RESEARCH_FACTORY_INIT_LOCK_TIMEOUT_SECONDS"] = "0"
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                result = self.run_cli_process("init")
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("another research_factory process is initializing the SQLite schema", result.stderr)
        self.assertNotIn("database is locked", result.stderr)

    def test_compact_v31_label_roundtrips_exactly(self) -> None:
        from research_factory.efficient_backtest import (
            compact_label,
            evidence_sparse_compact_label,
            expand_compact_label,
            expand_flat_ledger_label,
            expand_offset_sparse_compact_label,
            expand_sparse_compact_label,
            offset_sparse_compact_label,
            flat_ledger_label,
            sparse_compact_label,
        )

        label = self.minimal_v31_label("seg_eff_1", "ep_eff_1")
        compact = compact_label(label)
        self.assertEqual(expand_compact_label(compact, episode_id="ep_eff_1"), label)
        sparse = sparse_compact_label(label)
        self.assertEqual(expand_sparse_compact_label(sparse, episode_id="ep_eff_1"), label)
        segment_text = "The guest says AI agents will replace routine email triage over the next year."
        offset_sparse = offset_sparse_compact_label(label)
        self.assertNotIn("ev", offset_sparse["evs"][0])
        self.assertNotIn("ev", offset_sparse["cc"][0])
        self.assertEqual(expand_offset_sparse_compact_label(offset_sparse, episode_id="ep_eff_1", segment_text=segment_text), label)
        evidence_sparse = evidence_sparse_compact_label(label)
        self.assertNotIn("s", evidence_sparse["evs"][0])
        self.assertNotIn("e", evidence_sparse["evs"][0])
        self.assertNotIn("s", evidence_sparse["cc"][0])
        self.assertNotIn("e", evidence_sparse["cc"][0])
        flat = expand_flat_ledger_label(flat_ledger_label(label), episode_id="ep_eff_1")
        for key in ["discourse_events", "concept_candidates"]:
            for index, item in enumerate(flat[key]):
                item["evidence_start"] = label[key][index]["evidence_start"]
                item["evidence_end"] = label[key][index]["evidence_end"]
        self.assertEqual(flat, label)

    def test_compact_distillation_input_keeps_segment_and_model_context(self) -> None:
        from research_factory.efficient_backtest import _compact_distillation_input, event_detection_target

        packet = {
            "episode": {"episode_id": "ep_1"},
            "segments": [{"segment_id": "seg_1", "text": "complete current segment"}],
            "adjacent_segment_context": {"previous": "duplicate transcript"},
            "episode_context_artifact": {
                "context_summary": "model summary",
                "entity_seed": {"organizations": ["Example"]},
                "concept_seed": ["agent systems"],
                "extraction_guidance": "model guidance",
                "section_map": [{"topic": "verbose"}],
                "speaker_map": [{
                    "raw_speaker": "SPEAKER_1",
                    "likely_identity": "Guest",
                    "role": "guest",
                    "aliases_or_variants": ["G"],
                    "confidence": 0.9,
                    "notes": "verbose note",
                    "affiliations": ["Example"],
                }],
            },
        }
        compact = _compact_distillation_input(packet)
        self.assertEqual(compact["segments"], packet["segments"])
        self.assertNotIn("adjacent_segment_context", compact)
        self.assertNotIn("section_map", compact["model_generated_context"])
        self.assertNotIn("notes", compact["model_generated_context"]["speaker_map"][0])
        self.assertEqual(compact["model_generated_context"]["context_summary"], "model summary")
        detection = event_detection_target({
            "id": "seg_1",
            "st": "coded",
            "evs": [{"t": "capability_claim", "a": {"n": "Guest"}, "sp": {"n": "Guest"}, "src": {"k": "speaker_claim"}, "claim": "A claim", "ev": "evidence", "cf": 0.8, "notes": "excluded"}],
        })
        self.assertEqual(detection["evs"][0]["actor"], "Guest")
        self.assertNotIn("notes", detection["evs"][0])

    def test_windowed_packet_has_overlapping_extracts_and_single_owner_ranges(self) -> None:
        from research_factory.efficient_backtest import (
            build_windowed_segment_packet,
            windowed_event_core_schema,
        )

        text = " ".join(f"word{index}" for index in range(23))
        windows, boundaries = build_windowed_segment_packet(text, window_count=4, context_chars=12)
        self.assertEqual(len(windows), 4)
        self.assertEqual(len(boundaries), 4)
        owned_words = [
            word
            for boundary in boundaries
            for word in text[boundary["owner_start"] : boundary["owner_end"]].split()
        ]
        self.assertEqual(owned_words, text.split())
        self.assertEqual(boundaries[0]["center_start"], 0)
        self.assertEqual(boundaries[-1]["center_end"], len(text))
        for previous, current in zip(boundaries, boundaries[1:]):
            self.assertLess(previous["center_end"], current["center_start"])
            self.assertGreater(previous["extract_end"], current["owner_start"])
            self.assertLess(current["extract_start"], previous["owner_end"])
        schema = windowed_event_core_schema(max_events=25)
        self.assertIn("events", schema["required"])
        self.assertNotIn("windows", schema["properties"])
        self.assertEqual(schema["properties"]["events"]["maxItems"], 25)
        self.assertIn("window_id", schema["properties"]["events"]["items"]["required"])
        self.assertIn("segment_source_context", schema["required"])
        self.assertIn("excluded_source_context", schema["properties"]["status"]["enum"])

    def test_windowed_core_normalization_assigns_owner_and_removes_only_exact_duplicates(self) -> None:
        from research_factory.efficient_backtest import (
            build_windowed_segment_packet,
            normalize_windowed_core_payload,
        )

        text = " ".join(f"word{index}" for index in range(20))
        _windows, boundaries = build_windowed_segment_packet(text, window_count=2, context_chars=20)
        evidence = "word14 word15"
        event = {
            "window_id": 0,
            "event_type": "capability_claim",
            "claim_type": "descriptive",
            "actor_name": "Speaker",
            "speaker_name": "Speaker",
            "reported_actor_name": "",
            "target_concept": "test target",
            "claim_text": "A distinct supported claim.",
            "signal_reason": "Research useful.",
            "evidence": evidence,
            "model_names": [],
            "product_names": [],
            "organizations": [],
            "people": [],
            "confidence": 0.9,
        }
        payload, report = normalize_windowed_core_payload(
            {"segment_id": "seg_1", "status": "coded", "events": [event, dict(event)]},
            segment_text=text,
            boundaries=boundaries,
        )
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["window_id"], 1)
        self.assertEqual(report["owner_normalizations"], 2)
        self.assertEqual(report["exact_duplicates_removed"], 1)

    def test_judge_calibration_variants_are_blinded_reversed_and_source_grounded(self) -> None:
        from research_factory.windowed_evaluation import (
            build_judge_calibration_variants,
            load_judge_calibration_fixture,
        )

        fixture = load_judge_calibration_fixture()
        variants = build_judge_calibration_variants(fixture)
        self.assertEqual(len(variants["ab"]["cases"]), 24)
        self.assertFalse(
            {item["case_id"] for item in variants["ab"]["cases"]}
            & {item["case_id"] for item in variants["ba"]["cases"]}
        )
        for variant in variants.values():
            self.assertTrue(all(item["source_excerpt"] for item in variant["cases"]))
            self.assertTrue(all(not item["case_id"].endswith("wrong_actor") for item in variant["cases"]))
        nonexact_ab = next(
            opaque_id
            for opaque_id, expected in variants["ab"]["expected"].items()
            if expected["base_case_id"] == "nonexact_evidence"
        )
        row = next(item for item in variants["ab"]["cases"] if item["case_id"] == nonexact_ab)
        self.assertNotIn(row["event_b"]["evidence"], row["source_excerpt"])

    def test_judge_calibration_scoring_requires_complete_order_stable_outputs(self) -> None:
        from research_factory.windowed_evaluation import (
            build_judge_calibration_variants,
            combine_judge_calibration_scores,
            load_judge_calibration_fixture,
            load_windowed_acceptance_spec,
            score_judge_calibration_variant,
            validate_judge_calibration_output,
        )

        fixture = load_judge_calibration_fixture()
        variants = build_judge_calibration_variants(fixture)
        scores = {}
        outputs = {}
        for name, variant in variants.items():
            rows = [
                {
                    "case_id": case_id,
                    "relation": truth["expected_relation"],
                    "mismatch_fields": truth["expected_mismatch_fields"],
                }
                for case_id, truth in variant["expected"].items()
            ]
            outputs[name] = {"cases": rows}
            self.assertEqual(
                validate_judge_calibration_output(
                    outputs[name],
                    case_ids=[item["case_id"] for item in variant["cases"]],
                    mismatch_fields=fixture["mismatch_fields"],
                ),
                [],
            )
            scores[name] = score_judge_calibration_variant(outputs[name], expected=variant["expected"])
        gates = load_windowed_acceptance_spec()["judge_calibration_gates"]
        self.assertTrue(combine_judge_calibration_scores(scores, gates=gates)["passed"])

        for row in outputs["ba"]["cases"][:2]:
            row["relation"] = "partial" if row["relation"] != "partial" else "equivalent"
        changed = score_judge_calibration_variant(outputs["ba"], expected=variants["ba"]["expected"])
        self.assertFalse(combine_judge_calibration_scores({"ab": scores["ab"], "ba": changed}, gates=gates)["passed"])

        duplicate = json.loads(json.dumps(outputs["ab"]))
        duplicate["cases"][-1] = duplicate["cases"][0]
        errors = validate_judge_calibration_output(
            duplicate,
            case_ids=[item["case_id"] for item in variants["ab"]["cases"]],
            mismatch_fields=fixture["mismatch_fields"],
        )
        self.assertIn("case_id_set_mismatch", errors)

    def test_expanded_judge_fixture_covers_sixty_set_level_cases_and_scores_order_stably(self) -> None:
        from collections import Counter

        from research_factory.windowed_evaluation import (
            build_expanded_judge_calibration_variants,
            combine_expanded_judge_calibration_scores,
            load_expanded_judge_calibration_fixture,
            load_windowed_evaluator_spec,
            score_expanded_judge_calibration_variant,
            validate_expanded_judge_calibration_output,
        )

        fixture = load_expanded_judge_calibration_fixture()
        cases = fixture["expanded_cases"]
        self.assertEqual(len(cases), 60)
        self.assertEqual(len({case["topic_id"] for case in cases}), 10)
        self.assertEqual(
            Counter(case["shape"] for case in cases),
            Counter(
                {
                    "single_event_pairs": 20,
                    "merged_and_split_boundaries": 10,
                    "multi_event_set_alignment": 10,
                    "one_sided_supported_residuals": 10,
                    "one_sided_unsupported_residuals": 10,
                }
            ),
        )
        variants = build_expanded_judge_calibration_variants(fixture)
        scores = {}
        for variant_name, variant in variants.items():
            output_rows = []
            for case_id, truth in variant["expected"].items():
                output_rows.append(
                    {
                        "case_id": case_id,
                        "pairs": [
                            {
                                "a_id": pair["a_id"],
                                "b_id": pair["b_id"],
                                "relation": pair["relation"],
                                "mismatch_fields": pair["mismatch_fields"],
                            }
                            for pair in truth["pairs"]
                        ],
                        "support_a": [
                            {"id": event_id, "supported": values["supported"]}
                            for event_id, values in truth["support_a"].items()
                        ],
                        "support_b": [
                            {"id": event_id, "supported": values["supported"]}
                            for event_id, values in truth["support_b"].items()
                        ],
                    }
                )
            output = {"cases": output_rows}
            self.assertEqual(
                validate_expanded_judge_calibration_output(
                    output,
                    expected=variant["expected"],
                    mismatch_fields=fixture["mismatch_fields"],
                ),
                [],
            )
            scores[variant_name] = score_expanded_judge_calibration_variant(
                output,
                expected=variant["expected"],
            )
            self.assertEqual(scores[variant_name]["alignment_f1"], 1.0)
            self.assertEqual(scores[variant_name]["support_specificity"], 1.0)
        gates = load_windowed_evaluator_spec()["judge_calibration"]["gates"]
        combined = combine_expanded_judge_calibration_scores(scores, gates=gates)
        self.assertTrue(combined["passed"])
        self.assertEqual(combined["metrics"]["order_bias"], 0.0)
        for variant in variants.values():
            serialized = json.dumps(variant["cases"], ensure_ascii=True)
            self.assertNotIn("topic_id", serialized)
            self.assertNotIn("model_name", serialized)

    def test_paired_sampler_balances_strata_and_reaches_source_coverage(self) -> None:
        from collections import Counter

        from research_factory.windowed_evaluation import (
            _density_stratum,
            _raise_source_coverage,
            _source_balanced_sample,
            load_windowed_acceptance_spec,
        )

        spec = load_windowed_acceptance_spec()["paired_run"]
        candidates = {}
        for stratum_index, stratum in enumerate(spec["density_strata"]):
            rows = []
            for source_index in range(24):
                for repeat in range(2):
                    rows.append(
                        {
                            "segment_id": f"seg_{stratum}_{source_index}_{repeat}",
                            "source_id": f"src_{source_index}",
                            "density_stratum": stratum,
                        }
                    )
            candidates[stratum] = rows
        selected = {
            stratum: _source_balanced_sample(
                rows,
                count=15,
                seed=f"paired-test|{stratum}",
            )
            for stratum, rows in candidates.items()
        }
        _raise_source_coverage(
            selected,
            candidates,
            minimum_sources=20,
            seed="paired-test",
        )
        flat = [row for rows in selected.values() for row in rows]
        self.assertEqual(Counter(row["density_stratum"] for row in flat), Counter({name: 15 for name in candidates}))
        source_counts = Counter(row["source_id"] for row in flat)
        self.assertGreaterEqual(len(source_counts), 20)
        self.assertGreaterEqual(max(source_counts.values()), 2)
        self.assertEqual(_density_stratum(0, spec["density_strata"]), "no_signal")
        self.assertEqual(_density_stratum(4, spec["density_strata"]), "low")
        self.assertEqual(_density_stratum(15, spec["density_strata"]), "medium")
        self.assertEqual(_density_stratum(16, spec["density_strata"]), "dense")

    def test_paired_usage_accounting_includes_failed_and_retried_attempts(self) -> None:
        from research_factory.windowed_evaluation import _sum_usage

        attempts = [
            {"usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}},
            {"usage": {"input_tokens": 20, "cached_input_tokens": 5, "output_tokens": 3, "total_tokens": 23}},
        ]
        self.assertEqual(
            _sum_usage(attempts),
            {
                "input_tokens": 30,
                "cached_input_tokens": 5,
                "output_tokens": 5,
                "reasoning_output_tokens": 0,
                "total_tokens": 35,
            },
        )

    def test_paired_exact_identity_matching_is_one_to_one(self) -> None:
        from research_factory.windowed_evaluation import _exact_full_event_pairs

        event = {
            "event_type": "capability_claim",
            "claim_type": "descriptive",
            "actor": {"name": "Analyst"},
            "speaker_context": {"name": "Analyst"},
            "reported_actor": {"name": ""},
            "target": {"candidate_concept": "request handling"},
            "claim_text": "The service handles requests.",
            "evidence": "The service handles requests.",
            "model_names": [],
            "product_names": [],
            "organizations": [],
            "people": [],
            "confidence": 0.9,
        }
        changed = json.loads(json.dumps(event))
        changed["claim_text"] = "The service might handle requests."
        pairs, golden_left, system_left = _exact_full_event_pairs(
            [event, event, changed],
            [event, changed],
        )
        self.assertEqual(pairs, [(0, 0), (2, 1)])
        self.assertEqual(golden_left, [1])
        self.assertEqual(system_left, [])

    def test_paired_bootstrap_zero_delta_has_zero_interval(self) -> None:
        from research_factory.windowed_evaluation import paired_bootstrap_full_field

        rows = [
            {
                "golden_events": count,
                "baseline_events": count,
                "baseline_equivalent": count,
                "candidate_events": count,
                "candidate_equivalent": count,
            }
            for count in (0, 1, 4, 12, 20)
        ]
        report = paired_bootstrap_full_field(rows, iterations=250, seed="paired-zero-test")
        self.assertEqual(report["candidate_minus_baseline"], 0.0)
        self.assertEqual(report["ci_lower"], 0.0)
        self.assertEqual(report["ci_upper"], 0.0)
        self.assertEqual(report["method"], "paired_source_clustered_density_stratified")

    def test_paired_human_audit_neutralizes_supported_one_sided_event_by_family(self) -> None:
        from research_factory.windowed_evaluation import score_blinded_paired_adjudication

        human_path = self.root / "human.json"
        human_path.write_text(
            json.dumps(
                {
                    "reviewer_kind": "human",
                    "reviewer_id": "reviewer-1",
                    "reviewed_at": "2026-07-11T00:00:00Z",
                    "pairs": [
                        {
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        }
                    ],
                    "support_a": [{"id": 0, "supported": True}],
                    "support_b": [
                        {"id": 0, "supported": True},
                        {"id": 1, "supported": True},
                    ],
                }
            ),
            encoding="utf-8",
        )
        mapping = {
            "schema_version": "windowed_paired_adjudication_v1",
            "comparisons": [
                {
                    "comparison_id": "baseline",
                    "segment_id": "seg_1",
                    "system_name": "baseline",
                    "system_is_a": False,
                    "terminal_failure": False,
                    "golden_count": 1,
                    "system_count": 1,
                    "golden_event_families": {"0": "capability_claim"},
                    "system_event_families": {"0": "capability_claim"},
                    "exact_pairs": [[0, 0]],
                    "unresolved_golden_ids": [],
                    "unresolved_system_ids": [],
                    "requires_human": False,
                    "output_path": None,
                    "source_id": "src_1",
                    "source_name": "Source 1",
                    "density_stratum": "low",
                },
                {
                    "comparison_id": "candidate",
                    "segment_id": "seg_1",
                    "system_name": "candidate",
                    "system_is_a": False,
                    "terminal_failure": False,
                    "golden_count": 1,
                    "system_count": 2,
                    "golden_event_families": {"0": "capability_claim"},
                    "system_event_families": {"0": "capability_claim", "1": "risk_signal"},
                    "exact_pairs": [],
                    "unresolved_golden_ids": [0],
                    "unresolved_system_ids": [0, 1],
                    "requires_human": True,
                    "output_path": str(human_path),
                    "source_id": "src_1",
                    "source_name": "Source 1",
                    "density_stratum": "low",
                },
            ],
        }
        mapping_path = self.root / "mapping.json"
        mapping_path.write_text(json.dumps(mapping), encoding="utf-8")
        report = score_blinded_paired_adjudication(mapping_path=mapping_path)
        self.assertTrue(report["complete"])
        self.assertEqual(report["rows"][0]["candidate_f1"], 1.0)
        self.assertEqual(report["rows"][0]["candidate_source_supported_unscored"], 1)
        risk = report["event_level_by_family"]["risk_signal"]["candidate"]
        self.assertEqual(risk["golden_events"], 0)
        self.assertEqual(risk["system_events"], 0)
        self.assertEqual(risk["source_supported_unscored"], 1)
        self.assertEqual(risk["segments"], 0)
        self.assertEqual(risk["f1"], 1.0)

    def test_consolidated_human_audit_fans_one_review_into_oriented_path_outputs(self) -> None:
        from research_factory.windowed_evaluation import (
            export_consolidated_human_adjudication,
            materialize_consolidated_human_outputs,
        )

        golden = [{"id": 0, "claim": "The service handles requests."}]
        system = [{"id": 1, "claim": "Requests are handled by the service."}]
        output_a = self.root / "shared-a.json"
        output_b = self.root / "shared-b.json"
        mapping_paths = []
        for index, (system_is_a, output_path) in enumerate(
            ((False, output_a), (True, output_b))
        ):
            mapping = {
                "schema_version": "windowed_paired_adjudication_v1",
                "comparisons": [
                    {
                        "comparison_id": f"comparison-{index}",
                        "segment_id": "seg_1",
                        "system_name": "candidate",
                        "system_is_a": system_is_a,
                        "requires_human": True,
                        "unresolved_golden_ids": [0],
                        "unresolved_system_ids": [1],
                        "output_path": str(output_path),
                        "private_source_excerpt": "The service handles requests.",
                        "private_golden_events": golden,
                        "private_system_events": system,
                    }
                ],
            }
            path = self.root / f"mapping-{index}.json"
            path.write_text(json.dumps(mapping), encoding="utf-8")
            mapping_paths.append(path)
        exported = export_consolidated_human_adjudication(
            mapping_paths=mapping_paths,
            output_dir=self.root / "consolidated",
            seed="consolidated-test",
        )
        self.assertEqual(exported["consolidated_segment_packets"], 1)
        self.assertEqual(exported["unique_system_sets"], 1)
        mapping = json.loads(Path(exported["mapping_path"]).read_text(encoding="utf-8"))
        segment = mapping["segments"][0]
        system_set_id = next(iter(segment["system_sets"]))
        Path(segment["human_output_path"]).write_text(
            json.dumps(
                {
                    "reviewer_kind": "human",
                    "reviewer_id": "reviewer-1",
                    "reviewed_at": "2026-07-12T00:00:00Z",
                    "segment_id": "seg_1",
                    "support": [
                        {
                            "set_id": segment["anchor_set_id"],
                            "event_id": 0,
                            "supported": True,
                        },
                        {"set_id": system_set_id, "event_id": 1, "supported": True},
                    ],
                    "comparisons": [
                        {
                            "set_id": system_set_id,
                            "pairs": [
                                {
                                    "anchor_id": 0,
                                    "event_id": 1,
                                    "relation": "equivalent",
                                    "mismatch_fields": [],
                                }
                            ],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        materialized = materialize_consolidated_human_outputs(
            mapping_path=exported["mapping_path"]
        )
        self.assertTrue(materialized["ok"])
        self.assertEqual(materialized["materialized_comparison_outputs"], 2)
        a_payload = json.loads(output_a.read_text(encoding="utf-8"))
        b_payload = json.loads(output_b.read_text(encoding="utf-8"))
        self.assertEqual(a_payload["pairs"][0]["a_id"], 0)
        self.assertEqual(a_payload["pairs"][0]["b_id"], 1)
        self.assertEqual(b_payload["pairs"][0]["a_id"], 1)
        self.assertEqual(b_payload["pairs"][0]["b_id"], 0)
        self.assertEqual(a_payload["support_a"], [{"id": 0, "supported": True}])
        self.assertEqual(b_payload["support_a"], [{"id": 1, "supported": True}])

    def test_no_signal_power_requires_about_sixty_clean_zero_false_positive_cases(self) -> None:
        from research_factory.windowed_evaluation import (
            build_no_signal_power_report,
            one_sided_binomial_upper_bound,
        )

        self.assertAlmostEqual(
            one_sided_binomial_upper_bound(successes=0, trials=15),
            0.181036,
            places=6,
        )
        self.assertAlmostEqual(
            one_sided_binomial_upper_bound(successes=0, trials=60),
            0.048703,
            places=6,
        )
        underpowered = build_no_signal_power_report(
            trials=15,
            false_positives=0,
            human_confirmed_clean=15,
            output_path=self.root / "no-signal-15.json",
        )
        self.assertFalse(underpowered["passed"])
        self.assertFalse(underpowered["checks"]["sample_size"])
        self.assertFalse(underpowered["checks"]["upper_bound"])
        unverified_counts = build_no_signal_power_report(
            trials=60,
            false_positives=0,
            human_confirmed_clean=60,
            output_path=self.root / "no-signal-60.json",
        )
        self.assertFalse(unverified_counts["passed"])
        self.assertFalse(unverified_counts["checks"]["human_provenance"])
        powered = build_no_signal_power_report(
            trials=60,
            false_positives=0,
            human_confirmed_clean=60,
            output_path=self.root / "no-signal-60-verified.json",
            human_provenance_verified=True,
            candidate_result_provenance_verified=True,
            candidate_schema_successes=60,
        )
        self.assertTrue(powered["passed"])
        self.assertTrue(all(powered["checks"].values()))

    def test_interrupted_core_recovery_preserves_usage_and_does_not_rerun(self) -> None:
        from research_factory.efficient_backtest import build_windowed_segment_packet
        from research_factory.windowed_evaluation import recover_interrupted_windowed_core_run

        self.seed_efficiency_backtest_rows()
        segment_text = "The guest says AI agents will replace routine email triage over the next year."
        _windows, boundaries = build_windowed_segment_packet(
            segment_text,
            window_count=4,
            context_chars=20,
        )
        output_path = self.root / "interrupted-output.json"
        output_path.write_text(
            json.dumps({"segment_id": "seg_eff_1", "status": "no_signal", "events": []}),
            encoding="utf-8",
        )
        log_path = self.root / "interrupted.log"
        log_path.write_text(
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 100,
                        "cached_input_tokens": 20,
                        "output_tokens": 10,
                        "reasoning_output_tokens": 2,
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        source_manifest = self.root / "source-manifest.json"
        source_manifest.write_text(
            json.dumps(
                {
                    "chunks": [
                        {
                            "chunk_id": "seg_eff_1",
                            "segment_ids": ["seg_eff_1"],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        core_manifest = self.root / "core-manifest.json"
        core_manifest.write_text(
            json.dumps(
                {
                    "structural_max_events": 25,
                    "entries": [
                        {
                            "segment_id": "seg_eff_1",
                            "boundaries": boundaries,
                            "output_path": str(output_path),
                            "log_path": str(log_path),
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        with self.db_conn() as conn:
            report = recover_interrupted_windowed_core_run(
                conn,
                source_manifest_path=source_manifest,
                core_manifest_path=core_manifest,
                output_path=self.root / "recovery-report.json",
                parent_exit_code=143,
            )
        self.assertEqual(report["completed_calls"], 1)
        self.assertEqual(report["validated_segments"], 1)
        self.assertEqual(report["usage"]["total_tokens"], 110)
        self.assertFalse(report["accounting_complete"])
        self.assertTrue((self.root / "interrupted_raw_outputs" / "seg_eff_1.private.json").exists())

    def test_checkpointed_core_aggregates_existing_blocks_without_model_calls(self) -> None:
        from research_factory.windowed_evaluation import run_checkpointed_windowed_core

        self.run_cli("init")
        chunks = [
            {
                "chunk_id": f"seg_{index}",
                "segment_ids": [f"seg_{index}"],
                "episode_id": f"ep_{index}",
                "source_name": "Fixture",
                "expected_discourse_events": 0,
            }
            for index in range(5)
        ]
        manifest_path = self.root / "checkpoint-source.json"
        manifest_path.write_text(json.dumps({"chunks": chunks}), encoding="utf-8")
        output_dir = self.root / "checkpointed-core"
        for block_index, block_chunks in enumerate((chunks[:4], chunks[4:])):
            block_dir = output_dir / "blocks" / f"block-{block_index:03d}"
            block_dir.mkdir(parents=True, exist_ok=True)
            results = [
                {
                    "segment_id": chunk["chunk_id"],
                    "status_ok": True,
                    "usage": {"total_tokens": 10},
                }
                for chunk in block_chunks
            ]
            (block_dir / "run-report.json").write_text(
                json.dumps(
                    {
                        "results": results,
                        "attempted_calls": len(results),
                        "retry_attempts": 0,
                        "usage": {"total_tokens": len(results) * 10},
                        "accounting_complete": True,
                    }
                ),
                encoding="utf-8",
            )
        with self.db_conn() as conn:
            report = run_checkpointed_windowed_core(
                conn,
                manifest_path=manifest_path,
                output_dir=output_dir,
            )
        self.assertTrue(report["run_complete"])
        self.assertEqual(report["completed_blocks"], 2)
        self.assertEqual(report["validated_segments"], 5)
        self.assertEqual(report["usage"]["total_tokens"], 50)
        self.assertEqual(len(report["results"]), 5)

    def test_checkpointed_core_stops_before_rerunning_interrupted_block(self) -> None:
        from research_factory.windowed_evaluation import run_checkpointed_windowed_core

        self.run_cli("init")
        manifest_path = self.root / "interrupted-checkpoint-source.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "chunks": [
                        {
                            "chunk_id": "seg_1",
                            "segment_ids": ["seg_1"],
                            "episode_id": "ep_1",
                            "source_name": "Fixture",
                            "expected_discourse_events": 0,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        output_dir = self.root / "interrupted-checkpointed-core"
        interrupted_dir = output_dir / "blocks" / "block-000"
        interrupted_dir.mkdir(parents=True, exist_ok=True)
        (interrupted_dir / "windowed_core_manifest.json").write_text(
            json.dumps({"entries": []}),
            encoding="utf-8",
        )
        with self.db_conn() as conn:
            report = run_checkpointed_windowed_core(
                conn,
                manifest_path=manifest_path,
                output_dir=output_dir,
            )
        self.assertFalse(report["run_complete"])
        self.assertEqual(report["completed_blocks"], 0)
        self.assertEqual(report["interrupted_block"]["block_index"], 0)
        self.assertIn("efficiency-windowed-core-recover", report["interrupted_block"]["recovery_command"])

    def test_historical_context_usage_recovery_is_exact_and_idempotent(self) -> None:
        from research_factory.util import sha256_text
        from research_factory.windowed_evaluation import (
            PAIRED_CONTEXT_COST_SCHEMA_VERSION,
            recover_historical_episode_context_usage,
        )

        self.seed_efficiency_backtest_rows()
        prompt = (
            "You are the GPT-5.5 full-episode context reader for ai_discourse_v3_1.\n"
            "Synthetic private context fixture."
        )
        output = json.dumps({"schema_version": "fixture", "episode_id": "ep_eff_1"})
        prompt_path = self.root / "runs" / "prompts" / "ectx_fixture.md"
        output_path = self.root / "runs" / "outputs" / "ectx_fixture.json"
        artifact_path = self.root / "corpus" / "episode_context" / "ectx_fixture.json"
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.write_text(prompt, encoding="utf-8")
        output_path.write_text(output, encoding="utf-8")
        artifact_path.write_text(output, encoding="utf-8")
        with self.db_conn() as conn:
            conn.execute(
                """
                INSERT INTO episode_context_runs
                  (id, episode_id, transcript_id, label_pack, model, status, prompt_path,
                   output_path, context_artifact_path, created_at, updated_at, completed_at)
                VALUES ('ectx_fixture', 'ep_eff_1', 'tr_eff_1', 'ai_discourse_v3_1',
                        'gpt-5.5', 'completed', ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(prompt_path),
                    str(output_path),
                    str(artifact_path),
                    "2026-07-10T12:00:00+00:00",
                    "2026-07-10T12:01:00+00:00",
                    "2026-07-10T12:01:00+00:00",
                ),
            )
            conn.commit()

        context_cost_path = self.root / "context-cost.json"
        context_cost_path.write_text(
            json.dumps(
                {
                    "schema_version": PAIRED_CONTEXT_COST_SCHEMA_VERSION,
                    "historical_context_runs": [
                        {
                            "run_id": "ectx_fixture",
                            "prompt_sha256": sha256_text(prompt),
                            "output_sha256": sha256_text(output),
                            "evaluation_selected_segments": 1,
                            "production_reuse_segments": 2,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        rollout_path = self.root / "rollout-2026-07-10T08-00-00-fixture.jsonl"
        rollout_events = [
            {
                "timestamp": "2026-07-10T12:00:00Z",
                "type": "session_meta",
                "payload": {
                    "session_id": "session_fixture",
                    "source": "exec",
                    "cwd": str(self.root),
                },
            },
            {
                "timestamp": "2026-07-10T12:00:00Z",
                "type": "turn_context",
                "payload": {"model": "gpt-5.5", "cwd": str(self.root)},
            },
            {
                "timestamp": "2026-07-10T12:00:01Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            },
            {
                "timestamp": "2026-07-10T12:00:59Z",
                "type": "event_msg",
                "payload": {"type": "token_count", "info": {"total_token_usage": usage}},
            },
            {
                "timestamp": "2026-07-10T12:01:00Z",
                "type": "event_msg",
                "payload": {
                    "type": "task_complete",
                    "last_agent_message": output,
                    "duration_ms": 60000,
                },
            },
        ]
        rollout_path.write_text(
            "".join(json.dumps(event) + "\n" for event in rollout_events),
            encoding="utf-8",
        )
        report_path = self.root / "context-usage-recovery.json"
        with self.db_conn() as conn:
            first = recover_historical_episode_context_usage(
                conn,
                context_cost_report_path=context_cost_path,
                rollout_roots=[rollout_path],
                output_path=report_path,
                expected_usage={"total_tokens": 120},
            )
            second = recover_historical_episode_context_usage(
                conn,
                context_cost_report_path=context_cost_path,
                rollout_roots=[],
                output_path=report_path,
                expected_usage={"total_tokens": 120},
            )
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["usage"], usage)
        self.assertEqual(first["production_amortized_usage"]["total_tokens"], 60)
        self.assertEqual(first["recovered_runs"], 1)
        self.assertTrue(second["ok"])
        self.assertEqual(second["candidate_rollouts_scanned"], 0)
        self.assertEqual(second["usage"], usage)

    def test_paired_acceptance_report_fails_closed_when_evidence_is_missing(self) -> None:
        from research_factory.windowed_evaluation import (
            PAIRED_ACCEPTANCE_BUNDLE_VERSION,
            build_fail_closed_acceptance_report,
        )

        bundle_path = self.root / "acceptance-evidence.json"
        bundle_path.write_text(
            json.dumps(
                {
                    "schema_version": PAIRED_ACCEPTANCE_BUNDLE_VERSION,
                    "phase_one_report_path": None,
                    "provenance_report_path": None,
                    "context_cost_report_path": None,
                    "no_signal_power_report_path": None,
                    "prospective_shadow_report_path": None,
                    "paths": {},
                }
            ),
            encoding="utf-8",
        )
        report = build_fail_closed_acceptance_report(evidence_bundle_path=bundle_path)
        self.assertEqual(report["decision"], "pending")
        self.assertFalse(report["acceptance_eligible"])
        self.assertFalse(report["promotion_allowed"])
        self.assertEqual(report["global_checks"]["phase_one_available"]["state"], "pending")
        self.assertEqual(
            report["global_checks"]["separate_spark_and_mini_paths"]["state"],
            "pending",
        )

    def test_windowed_holdout_id_collection_reads_only_manifest_metadata(self) -> None:
        from research_factory.efficient_backtest import _collect_manifest_holdout_ids

        segments, episodes = _collect_manifest_holdout_ids(
            {
                "chunks": [
                    {
                        "segment_ids": ["seg_1", "seg_2"],
                        "episode_id": "ep_1",
                        "nested": {"segment_id": "seg_3", "episode_ids": ["ep_2"]},
                    }
                ],
                "unrelated": "seg_not_metadata",
            }
        )
        self.assertEqual(segments, {"seg_1", "seg_2", "seg_3"})
        self.assertEqual(episodes, {"ep_1", "ep_2"})

    def test_windowed_prompt_loads_structured_llm_guidelines_without_exposing_them_in_metadata(self) -> None:
        from research_factory.efficient_backtest import (
            DEFAULT_WINDOWED_GUIDELINES_PATH,
            _load_windowed_guideline_instructions,
            _windowed_event_core_instruction,
        )

        path = self.root / "guidelines.json"
        path.write_text(
            json.dumps(
                {
                    "guidelines": [
                        {"title": "Atomicity", "failure_mode": "duplication", "instruction": "Merge restatements."},
                        "Keep independently useful propositions.",
                    ]
                }
            ),
            encoding="utf-8",
        )
        guidelines, artifact_hash = _load_windowed_guideline_instructions(path)
        self.assertEqual(guidelines, ["Merge restatements.", "Keep independently useful propositions."])
        self.assertEqual(len(artifact_hash or ""), 64)
        prompt = _windowed_event_core_instruction(
            guideline_instructions=guidelines,
            max_total_events=25,
        )
        self.assertIn("Merge restatements.", prompt)
        self.assertIn("at most 25 events", prompt)
        with self.assertRaises(ValueError):
            _windowed_event_core_instruction(max_total_events=0)
        default_guidelines, default_hash = _load_windowed_guideline_instructions(DEFAULT_WINDOWED_GUIDELINES_PATH)
        self.assertEqual(len(default_guidelines), 8)
        self.assertEqual(len(default_hash or ""), 64)

    def test_windowed_enrichment_contract_has_exact_batch_counts(self) -> None:
        from research_factory.efficient_backtest import (
            validate_windowed_enrichment_contract,
            windowed_enrichment_schema,
        )

        schema = windowed_enrichment_schema([1, 2])
        self.assertEqual(schema["properties"]["b0"]["minItems"], 1)
        self.assertEqual(schema["properties"]["b1"]["maxItems"], 2)
        self.assertEqual(
            set(schema["$defs"]["row"]["properties"]),
            {"i", "terms", "frames", "ex", "q"},
        )
        row = {
            "i": 0,
            "terms": [],
            "frames": [],
            "ex": [],
            "q": [],
        }
        second = dict(row, i=1)
        self.assertEqual(validate_windowed_enrichment_contract({"b0": [row], "b1": [row, second]}, [1, 2]), [])
        self.assertIn(
            "batch_1_count_mismatch",
            validate_windowed_enrichment_contract({"b0": [row], "b1": [row]}, [1, 2]),
        )

    def test_windowed_semantic_judge_scoring_enforces_one_to_one_pairs(self) -> None:
        from research_factory.efficient_backtest import (
            _semantic_judge_event,
            score_windowed_semantic_judge_payload,
            windowed_semantic_judge_schema,
        )

        schema = windowed_semantic_judge_schema()
        evaluations = schema["properties"]["evaluations"]
        self.assertEqual(evaluations["minItems"], 1)
        self.assertEqual(evaluations["maxItems"], 1)
        payload = {
            "evaluations": [
                {
                    "system_id": "multiwindow_sol",
                    "pairs": [
                        {"gold_id": 0, "candidate_id": 0, "relation": "equivalent"},
                        {"gold_id": 0, "candidate_id": 1, "relation": "equivalent"},
                        {"gold_id": 1, "candidate_id": 1, "relation": "partial"},
                        {"gold_id": 8, "candidate_id": 1, "relation": "equivalent"},
                    ],
                }
            ]
        }
        score = score_windowed_semantic_judge_payload(payload, golden_count=2, candidate_count=2)
        self.assertEqual(score["equivalent_pairs"], 1)
        self.assertEqual(score["partial_pairs"], 1)
        self.assertEqual(score["duplicate_pairs"], 1)
        self.assertEqual(score["invalid_pairs"], 1)
        self.assertEqual(score["f1"], 0.5)
        full_event = _semantic_judge_event(
            {
                "event_type": "forecast",
                "claim_type": "prediction",
                "stance": "warning",
                "metric": {"value": "2"},
            },
            event_id=0,
            include_full_fields=True,
        )
        self.assertEqual(full_event["stance"], "warning")
        self.assertEqual(full_event["metric"]["value"], "2")

    def test_windowed_event_core_hydrates_to_valid_full_v31_label(self) -> None:
        from research_factory.efficient_backtest import (
            build_windowed_segment_packet,
            hydrate_windowed_event_core_label,
            validate_and_prune_windowed_label,
        )
        from research_factory.labels import validate_label_output

        text = "The guest says AI agents will replace routine email triage over the next year."
        _windows, boundaries = build_windowed_segment_packet(text, window_count=1, context_chars=0)
        core = {
            "segment_id": "seg_windowed",
            "status": "coded",
            "segment_source_context": {
                "kind": "substantive_dialogue",
                "confidence": 0.9,
                "rationale": "The guest states a direct workflow forecast.",
            },
            "no_signal_reason": "",
            "events": [
                {
                    "window_id": 0,
                    "event_type": "forecast",
                    "event_subtype": "workflow_substitution_timeline",
                    "claim_type": "prediction",
                    "actor_name": "Guest",
                    "actor_type": "person",
                    "speaker_name": "Guest",
                    "speaker_role": "guest",
                    "reported_actor_name": "",
                    "reported_actor_type": "none",
                    "source_context_kind": "substantive_dialogue",
                    "target_concept": "routine email triage",
                    "claim_text": "The guest predicts AI agents will replace routine email triage within a year.",
                    "stance": "warning",
                    "certainty": "high",
                    "temporal_horizon": "near_future",
                    "causal_mechanism": "Agent automation takes over repetitive message triage.",
                    "counterclaim": "",
                    "metric_value": "",
                    "metric_unit": "year",
                    "metric_comparator": "",
                    "metric_direction": "not_applicable",
                    "metric_raw_text": "next year",
                    "signal_reason": "This is a concrete near-term labor substitution forecast with a defined workflow and horizon.",
                    "evidence": text,
                    "model_names": [],
                    "product_names": [],
                    "organizations": [],
                    "people": ["Guest"],
                    "confidence": 0.9,
                }
            ],
        }
        enrichment = [
            {
                "i": 0,
                "terms": ["AI agents"],
                "frames": ["labor substitution"],
                "ex": [],
                "q": [],
            }
        ]
        label, report = hydrate_windowed_event_core_label(
            core,
            enrichment,
            segment_text=text,
            boundaries=boundaries,
            episode_id="ep_windowed",
            speaker_map=[
                {
                    "name": "Guest",
                    "aliases": [],
                    "role": "guest",
                    "affiliations": [{"title": "Researcher", "org": "Example Lab"}],
                }
            ],
        )
        validate_label_output("ai_discourse_v3_1", label, segment_text=text)
        self.assertEqual(report["invalid_evidence_events"], 0)
        self.assertEqual(label["discourse_events"][0]["evidence_start"], 0)
        self.assertEqual(label["discourse_events"][0]["evidence_end"], len(text))
        self.assertEqual(label["discourse_events"][0]["actor"]["affiliation"], "Researcher, Example Lab")
        label["discourse_events"][0]["metric"] = {
            "value": "999",
            "unit": "percent",
            "comparator": None,
            "direction": "increase",
            "raw_text": "999 percent",
        }
        pruned, pruning = validate_and_prune_windowed_label(label, segment_text=text)
        validate_label_output("ai_discourse_v3_1", pruned, segment_text=text)
        self.assertEqual(pruning["validator_cleared_metrics"], 1)
        self.assertEqual(pruned["discourse_events"][0]["metric"]["direction"], "not_applicable")

    def test_efficiency_backtest_runs_without_raw_transcript_in_report(self) -> None:
        from research_factory.efficient_backtest import (
            COMPACT_SCHEMA_VERSION,
            EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION,
            OFFSET_SPARSE_COMPACT_SCHEMA_VERSION,
            SPARSE_COMPACT_SCHEMA_VERSION,
            compact_label,
            evidence_sparse_compact_label,
            offset_sparse_compact_label,
            sparse_compact_label,
        )

        self.seed_efficiency_backtest_rows()
        export_dir = self.root / "candidate-prompts"
        result = self.run_cli("efficiency-backtest", "--episode-limit", "1", "--export-candidate-prompts", str(export_dir))
        self.assertTrue(result["ok"])
        self.assertEqual(result["summary"]["label_count"], 1)
        self.assertEqual(result["quality_backtest"]["compact_representation"]["exact_label_roundtrip_rate"], 1.0)
        self.assertEqual(result["runtime_stats"]["label_runtime_observed_count"], 1)
        self.assertIn("estimated_runtime_seconds", result["cost_estimates"]["current_segment_context_path"])
        self.assertIn("estimated_current_vs_candidate_runtime_ratio", result["summary"])
        self.assertFalse(result["summary"]["candidate_quality_gate_passed"])
        self.assertEqual(result["summary"]["candidate_quality_gate_status"], "not_ready")
        self.assertEqual(result["quality_backtest"]["candidate_quality_gate"]["status"], "not_ready")
        self.assertEqual(result["candidate_prompt_export"]["episode_count"], 1)
        self.assertEqual(result["candidate_prompt_export"]["chunk_size"], 10)
        self.assertEqual(result["candidate_prompt_export"]["chunk_count"], 1)
        self.assertEqual(result["candidate_prompt_export"]["expected_coded_segments"], 1)
        self.assertEqual(result["candidate_prompt_export"]["expected_discourse_events"], 1)
        manifest_text = Path(result["candidate_prompt_export"]["manifest_path"]).read_text(encoding="utf-8")
        self.assertIn('"chunks"', manifest_text)
        self.assertIn('"expected_coded_segments": 1', manifest_text)
        self.assertIn('"expected_discourse_events": 1', manifest_text)
        self.assertNotIn("AI agents will replace routine email triage", manifest_text)
        report_text = Path(result["path"]).read_text(encoding="utf-8")
        self.assertNotIn("AI agents will replace routine email triage", report_text)

        smoke = self.run_cli(
            "efficiency-smoke-candidates",
            "--manifest",
            result["candidate_prompt_export"]["manifest_path"],
            "--limit",
            "1",
            "--dry-run",
            "--min-expected-events",
            "1",
            "--max-expected-events",
            "5",
            "--wait-for-clear-seconds",
            "0",
        )
        self.assertTrue(smoke["ok"])
        self.assertEqual(smoke["selected"], 1)
        self.assertEqual(smoke["results"][0]["expected_discourse_events"], 1)
        self.assertEqual(smoke["waited_seconds"], 0)
        self.assertIn("active_gpt55_process_observations", smoke)
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(smoke, ensure_ascii=True))

        status = self.run_cli(
            "efficiency-candidate-status",
            "--manifest",
            result["candidate_prompt_export"]["manifest_path"],
            "--min-expected-events",
            "1",
            "--max-expected-events",
            "5",
        )
        self.assertTrue(status["ok"])
        self.assertEqual(status["chunk_count"], 1)
        self.assertEqual(status["chunks_with_output"], 0)
        self.assertEqual(status["chunks_missing_output"], 1)
        self.assertEqual(status["expected_discourse_events"], 1)
        self.assertEqual(status["next_chunks"][0]["expected_discourse_events"], 1)
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(status, ensure_ascii=True))

        sweep = self.run_cli(
            "efficiency-chunk-sweep",
            "--episode-limit",
            "1",
            "--chunk-sizes",
            "1,2",
            "--report-dir",
            str(self.root / "chunk-sweep"),
            "--output",
            str(self.root / "chunk-sweep.json"),
        )
        self.assertTrue(sweep["ok"])
        self.assertEqual([item["chunk_size"] for item in sweep["chunk_sizes"]], [1, 2])
        self.assertTrue(sweep["selection"]["consistent_sqlite_read_snapshot"])
        self.assertIn(sweep["recommended_chunk_size"], [None, 1, 2])
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(sweep, ensure_ascii=True))

        readiness = self.run_cli(
            "efficiency-readiness",
            "--manifest",
            result["candidate_prompt_export"]["manifest_path"],
            "--sweep",
            str(self.root / "chunk-sweep.json"),
            "--next-limit",
            "1",
        )
        self.assertTrue(readiness["ok"])
        self.assertIn("candidate_outputs_missing", readiness["blockers"])
        self.assertFalse(readiness["ready_for_quality_backtest"])
        self.assertEqual(readiness["candidate_chunk_size"], 10)
        self.assertIn("advisories", readiness)
        self.assertIn("--wait-for-clear-seconds 1800", readiness["remaining_command"])
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(readiness, ensure_ascii=True))

        compared = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-output-dir",
            str(export_dir / "outputs"),
        )
        comparison = compared["quality_backtest"]["candidate_output_comparison"]
        self.assertEqual(comparison["segments_expected"], 1)
        self.assertEqual(comparison["segments_missing"], 1)
        self.assertEqual(comparison["by_source"]["Efficiency Fixture"]["segments_expected"], 1)
        self.assertEqual(comparison["by_source"]["Efficiency Fixture"]["golden_events"], 1)
        self.assertEqual(comparison["by_golden_status"]["coded"]["segments_expected"], 1)
        self.assertEqual(comparison["by_golden_status"]["coded"]["golden_events"], 1)
        self.assertFalse(compared["summary"]["candidate_quality_gate_passed"])
        self.assertEqual(compared["summary"]["candidate_quality_gate_status"], "failed")
        self.assertIn(
            "no_missing_candidate_segments",
            compared["quality_backtest"]["candidate_quality_gate"]["failed_checks"],
        )

        manifest = json.loads(Path(result["candidate_prompt_export"]["manifest_path"]).read_text(encoding="utf-8"))
        output_path = Path(manifest["chunks"][0]["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        valid_label = self.minimal_v31_label("seg_eff_1", "ep_eff_1")
        output_path.write_text(
            json.dumps(
                {
                    "episode_id": "ep_eff_1",
                    "schema_version": SPARSE_COMPACT_SCHEMA_VERSION,
                    "segment_outputs": [
                        {
                            "segment_id": "seg_eff_1",
                            "label": sparse_compact_label(valid_label),
                        }
                    ],
                    "episode_level_notes": "",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        valid_compared = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-output-dir",
            str(export_dir / "outputs"),
        )
        valid_comparison = valid_compared["quality_backtest"]["candidate_output_comparison"]
        self.assertEqual(valid_comparison["segments_present"], 1)
        self.assertEqual(valid_comparison["segments_missing"], 0)
        self.assertEqual(valid_comparison["validation_ok"], 1)
        self.assertEqual(valid_comparison["validation_failed"], 0)
        self.assertEqual(valid_comparison["valid_golden_events"], 1)
        self.assertEqual(valid_comparison["valid_candidate_events"], 1)
        self.assertEqual(valid_comparison["valid_matched_events"], 1)
        self.assertEqual(valid_comparison["valid_event_count_ratio"], 1.0)
        self.assertEqual(valid_comparison["segment_completion_ratio"], 1.0)
        self.assertEqual(valid_comparison["valid_segment_ratio"], 1.0)
        self.assertEqual(valid_comparison["status_accuracy"], 1.0)
        self.assertEqual(valid_comparison["event_precision"], 1.0)
        self.assertEqual(valid_comparison["event_recall"], 1.0)
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(valid_compared, ensure_ascii=True))

        compact_export_dir = self.root / "candidate-prompts-compact"
        compact_result = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "compact",
            "--export-candidate-prompts",
            str(compact_export_dir),
        )
        self.assertEqual(compact_result["summary"]["candidate"], "episode_chunk_compact_v1")
        self.assertEqual(compact_result["summary"]["candidate_representation"], "compact")
        self.assertEqual(compact_result["candidate_prompt_export"]["candidate_representation"], "compact")
        compact_manifest = json.loads(Path(compact_result["candidate_prompt_export"]["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(compact_manifest["schema_version"], COMPACT_SCHEMA_VERSION)
        self.assertEqual(compact_manifest["candidate_representation"], "compact")
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(compact_manifest, ensure_ascii=True))

        compact_output_path = Path(compact_manifest["chunks"][0]["output_path"])
        compact_output_path.parent.mkdir(parents=True, exist_ok=True)
        compact_output_path.write_text(
            json.dumps(
                {
                    "episode_id": "ep_eff_1",
                    "schema_version": COMPACT_SCHEMA_VERSION,
                    "segment_outputs": [
                        {
                            "segment_id": "seg_eff_1",
                            "label": compact_label(valid_label),
                        }
                    ],
                    "episode_level_notes": "",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        compact_compared = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "compact",
            "--candidate-output-dir",
            str(compact_export_dir / "outputs"),
        )
        compact_comparison = compact_compared["quality_backtest"]["candidate_output_comparison"]
        self.assertEqual(compact_comparison["validation_ok"], 1)
        self.assertEqual(compact_comparison["validation_failed"], 0)
        self.assertEqual(compact_comparison["event_precision"], 1.0)
        self.assertEqual(compact_comparison["event_recall"], 1.0)

        offset_export_dir = self.root / "candidate-prompts-offset-sparse"
        offset_result = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "offset_sparse_compact",
            "--export-candidate-prompts",
            str(offset_export_dir),
        )
        self.assertEqual(offset_result["summary"]["candidate"], "episode_chunk_offset_sparse_compact_v1")
        self.assertEqual(offset_result["summary"]["candidate_representation"], "offset_sparse_compact")
        self.assertEqual(offset_result["candidate_prompt_export"]["candidate_representation"], "offset_sparse_compact")
        offset_quality = offset_result["quality_backtest"]["offset_sparse_compact_representation"]
        self.assertEqual(offset_quality["exact_label_roundtrip_rate"], 1.0)
        offset_manifest = json.loads(Path(offset_result["candidate_prompt_export"]["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(offset_manifest["schema_version"], OFFSET_SPARSE_COMPACT_SCHEMA_VERSION)
        self.assertEqual(offset_manifest["candidate_representation"], "offset_sparse_compact")
        self.assertNotIn("AI agents will replace routine email triage", json.dumps(offset_manifest, ensure_ascii=True))

        offset_output_path = Path(offset_manifest["chunks"][0]["output_path"])
        offset_output_path.parent.mkdir(parents=True, exist_ok=True)
        offset_output_path.write_text(
            json.dumps(
                {
                    "episode_id": "ep_eff_1",
                    "schema_version": OFFSET_SPARSE_COMPACT_SCHEMA_VERSION,
                    "segment_outputs": [
                        {
                            "segment_id": "seg_eff_1",
                            "label": offset_sparse_compact_label(valid_label),
                        }
                    ],
                    "episode_level_notes": "",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        offset_compared = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "offset_sparse_compact",
            "--candidate-output-dir",
            str(offset_export_dir / "outputs"),
        )
        offset_comparison = offset_compared["quality_backtest"]["candidate_output_comparison"]
        self.assertEqual(offset_comparison["validation_ok"], 1)
        self.assertEqual(offset_comparison["validation_failed"], 0)
        self.assertEqual(offset_comparison["event_precision"], 1.0)
        self.assertEqual(offset_comparison["event_recall"], 1.0)

        evidence_export_dir = self.root / "candidate-prompts-evidence-sparse"
        evidence_result = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "evidence_sparse_compact",
            "--export-candidate-prompts",
            str(evidence_export_dir),
        )
        self.assertEqual(evidence_result["summary"]["candidate"], "episode_chunk_evidence_sparse_compact_v1")
        evidence_quality = evidence_result["quality_backtest"]["evidence_sparse_compact_representation"]
        self.assertEqual(evidence_quality["exact_label_roundtrip_rate"], 1.0)
        evidence_manifest = json.loads(Path(evidence_result["candidate_prompt_export"]["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(evidence_manifest["schema_version"], EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION)
        evidence_output_path = Path(evidence_manifest["chunks"][0]["output_path"])
        evidence_output_path.write_text(
            json.dumps(
                {
                    "episode_id": "ep_eff_1",
                    "schema_version": EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION,
                    "segment_outputs": [{"segment_id": "seg_eff_1", "label": evidence_sparse_compact_label(valid_label)}],
                    "episode_level_notes": "",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        evidence_compared = self.run_cli(
            "efficiency-backtest",
            "--episode-limit",
            "1",
            "--candidate-representation",
            "evidence_sparse_compact",
            "--candidate-output-dir",
            str(evidence_export_dir / "outputs"),
        )
        evidence_comparison = evidence_compared["quality_backtest"]["candidate_output_comparison"]
        self.assertEqual(evidence_comparison["validation_ok"], 1)
        self.assertEqual(evidence_comparison["validation_failed"], 0)
        self.assertEqual(evidence_comparison["event_precision"], 1.0)
        self.assertEqual(evidence_comparison["event_recall"], 1.0)

    def seed_efficiency_backtest_rows(self) -> None:
        self.run_cli("init")
        with self.db_conn() as conn:
            ts = "2026-07-10T00:00:00+00:00"
            conn.execute(
                """
                INSERT INTO sources
                  (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
                VALUES ('src_eff', 'Efficiency Fixture', NULL, NULL, 'tests', 'private_analysis_only', 'creator_rss_transcripts_only', 1, '{}', ?, ?)
                """,
                (ts, ts),
            )
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, description, url, audio_url, published_at, duration_seconds, created_at, updated_at)
                VALUES ('ep_eff_1', 'src_eff', 'ep_eff_1', 'Efficiency Fixture Episode', NULL, NULL, NULL, ?, 600, ?, ?)
                """,
                (ts, ts, ts),
            )
            segment_text = "The guest says AI agents will replace routine email triage over the next year."
            segment_path = self.root / "corpus" / "segments" / "seg_eff_1.txt"
            segment_path.parent.mkdir(parents=True, exist_ok=True)
            segment_path.write_text(segment_text, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
                VALUES ('tr_eff_1', 'ep_eff_1', 'fixture', NULL, 'text/plain', ?, 'rawsha', 'ready', ?, '{}', 12, ?, ?)
                """,
                (str(segment_path), ts, ts, ts),
            )
            conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES ('seg_eff_1', 'tr_eff_1', 'ep_eff_1', 'src_eff', 0, 0, ?, ?, 'segsha', 12, ?)
                """,
                (len(segment_text), str(segment_path), ts),
            )
            label = self.minimal_v31_label("seg_eff_1", "ep_eff_1")
            prompt_path = self.root / "runs" / "prompts" / "run_eff_1.md"
            output_path = self.root / "runs" / "outputs" / "run_eff_1.json"
            prompt_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            prompt_path.write_text("Current segment prompt fixture.\n" * 40, encoding="utf-8")
            output_path.write_text(json.dumps(label, ensure_ascii=True), encoding="utf-8")
            conn.execute(
                """
                INSERT INTO labels
                  (id, segment_id, label_pack, label_pack_version, model, status, output_json, confidence, needs_review, prompt_path, output_path, created_at)
                VALUES ('lbl_eff_1', 'seg_eff_1', 'ai_discourse_v3_1', 'ai_discourse_v3_1', 'gpt-5.5', 'ready', ?, 0.86, 0, ?, ?, ?)
                """,
                (json.dumps(label, ensure_ascii=True, sort_keys=True), str(prompt_path), str(output_path), ts),
            )
            conn.commit()

    def minimal_v31_label(self, segment_id: str, episode_id: str) -> dict:
        evidence = "AI agents will replace routine email triage"
        return {
            "schema_version": "ai_discourse_v3_1",
            "segment_id": segment_id,
            "episode_id": episode_id,
            "extraction_status": "coded",
            "segment_quality": {
                "artifact_type": "dialogue_transcript",
                "boilerplate_risk": "low",
                "substantive_word_count": 12,
                "transcript_preparation_id": None,
            },
            "segment_source_context": {
                "kind": "substantive_dialogue",
                "confidence": 0.9,
                "rationale": "The segment contains a direct guest forecast about agent workflow adoption.",
            },
            "discourse_events": [
                {
                    "event_type": "forecast",
                    "event_subtype": "agent_email_triage_replacement",
                    "actor": {"name": "The guest", "actor_type": "guest", "affiliation": None, "role": "speaker"},
                    "speaker_context": {"name": "The guest", "role": "guest", "affiliation": None, "confidence": 0.9},
                    "reported_actor": {"name": "AI agents", "actor_type": "product", "affiliation": None, "confidence": 0.8},
                    "source_context": {"kind": "substantive_dialogue", "confidence": 0.9, "rationale": "The guest states the forecast directly."},
                    "target": {
                        "raw_target": "AI agents replacing email triage",
                        "candidate_concept": "ai_agent_email_triage_replacement",
                        "canonical_concept": None,
                        "concept_confidence": 0.82,
                    },
                    "surface_terms": ["AI agents", "email triage"],
                    "frames": ["workflow_automation"],
                    "model_names": [],
                    "product_names": ["AI agents"],
                    "organizations": [],
                    "people": ["The guest"],
                    "stance": "supportive",
                    "claim_text": "The guest predicts AI agents will replace routine email triage over the next year.",
                    "claim_type": "prediction",
                    "certainty": "medium",
                    "temporal_horizon": "near_future",
                    "causal_mechanism": "",
                    "counterclaim": "",
                    "metric": {"value": None, "unit": None, "comparator": None, "direction": "not_applicable", "raw_text": None},
                    "signal_reason": "This tracks a concrete near-term workflow substitution forecast for agent adoption analysis.",
                    "exclusion_flags": [],
                    "quality_flags": [],
                    "evidence": evidence,
                    "evidence_start": 15,
                    "evidence_end": 15 + len(evidence),
                    "confidence": 0.86,
                    "audit_notes": "Fixture evidence is exact segment text.",
                }
            ],
            "concept_candidates": [
                {
                    "candidate": "ai_agent_email_triage_replacement",
                    "surface_terms": ["AI agents", "email triage"],
                    "rationale": "Useful for tracking agent adoption in routine communication workflows.",
                    "usefulness_score": 0.82,
                    "evidence": evidence,
                    "evidence_start": 15,
                    "evidence_end": 15 + len(evidence),
                    "confidence": 0.84,
                }
            ],
            "rejected_candidates": [],
            "no_signal_reason": None,
            "overall_confidence": 0.86,
            "needs_review": False,
            "review_reason": None,
        }


if __name__ == "__main__":
    unittest.main()
