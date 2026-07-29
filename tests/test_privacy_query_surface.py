from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import db
from research_factory.exports import (
    export_actor_stance_report,
    export_narrative_map,
    export_term_drift_report,
    export_trend_report,
)
from research_factory.mcp_broker import sanitize_broker_snapshot, status_from_snapshot
from research_factory.observer import (
    _claim_subject_views,
    _codex_scheduler_status,
    _parse_codex_cron_output,
    build_snapshot,
)
from research_factory.query_api import LocalQueryService
from research_factory.ui_server import HTML, _sanitize_snapshot, _validate_snapshot_contract


TS = "2026-01-15T12:00:00+00:00"


class PrivacyAndQuerySurfaceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = db.connect(self.root / "factory.sqlite")
        db.init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self.temp.cleanup()

    def _base_episode(self) -> None:
        self.conn.execute(
            "INSERT INTO sources (id, name, category, created_at, updated_at) VALUES ('src_1', 'Private Network Name', 'technology', ?, ?)",
            (TS, TS),
        )
        self.conn.execute(
            "INSERT INTO episodes (id, source_id, guid, title, published_at, created_at, updated_at) VALUES ('ep_1', 'src_1', 'guid-1', 'Private Episode Title', ?, ?, ?)",
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256, status, created_at, updated_at)
            VALUES ('tr_1', 'ep_1', 'official', '/private/transcript.txt', 'abc', 'ready', ?, ?)
            """,
            (TS, TS),
        )
        for index in range(2):
            self.conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char,
                   text_path, text_sha256, word_count, created_at)
                VALUES (?, 'tr_1', 'ep_1', 'src_1', ?, ?, ?, ?, ?, 10, ?)
                """,
                (f"seg_{index}", index, index * 10, index * 10 + 9, f"/private/{index}.txt", f"hash-{index}", TS),
            )
            self.conn.execute(
                """
                INSERT INTO labels
                  (id, segment_id, label_pack, label_pack_version, model, status, output_json, created_at)
                VALUES (?, ?, 'ai_discourse_v3_1', ?, 'gpt-test', 'ready', '{}', ?)
                """,
                (f"lbl_{index}", f"seg_{index}", f"v{index + 1}", TS),
            )
            self.conn.execute(
                """
                INSERT INTO claims
                  (id, label_id, segment_id, text, stance, confidence, created_at)
                VALUES (?, ?, ?, ?, 'supportive', 0.9, ?)
                """,
                (f"clm_{index}", f"lbl_{index}", f"seg_{index}", f"Private claim text {index}", TS),
            )
        self.conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, claim_text,
               evidence_text, evidence_start, evidence_end, created_at)
            VALUES ('evt_1', 'lbl_0', 'seg_0', 0, 'claim', 'Private atomic claim',
                    'Private evidence unit', 0, 10, ?)
            """,
            (TS,),
        )
        self.conn.commit()

    def _subject_observations(self) -> None:
        self._base_episode()
        self.conn.execute(
            """
            INSERT INTO claim_subjects
              (id, subject_text, subject_key, domain, status, confidence, method, created_at, updated_at)
            VALUES ('subject_1', 'Private Subject', 'private_subject', 'technology', 'active', 0.9, 'test', ?, ?)
            """,
            (TS, TS),
        )
        for index in range(2):
            self.conn.execute(
                """
                INSERT INTO claim_proposition_variants
                  (id, subject_id, variant_text, variant_key, status, confidence, method, created_at, updated_at)
                VALUES (?, 'subject_1', ?, ?, 'active', 0.9, 'test', ?, ?)
                """,
                (f"variant_{index}", f"Private variant {index}", f"variant_{index}", TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO claim_position_observations
                  (id, subject_id, variant_id, claim_id, speaker_name, source_id, episode_id,
                   stance, published_at, confidence, method, created_at, updated_at)
                VALUES (?, 'subject_1', ?, ?, 'Private Person', 'src_1', 'ep_1',
                        'supportive', ?, 0.9, 'test', ?, ?)
                """,
                (f"position_{index}", f"variant_{index}", f"clm_{index}", TS, TS, TS),
            )
        self.conn.commit()

    def _atomic_claim(self) -> None:
        self._base_episode()
        self.conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, confidence, status,
               canonical_version, created_at, updated_at)
            VALUES ('candidate_person', 'Candidate Private Person', 'candidate private person',
                    0.6, 'candidate', 1, ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, confidence, status,
               canonical_version, created_at, updated_at)
            VALUES ('accepted_person', 'Accepted Private Person', 'accepted private person',
                    0.95, 'accepted', 1, ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO corpus_releases
              (id, release_version, cutoff_at, manifest_sha256, created_at)
            VALUES ('release_1', 1, ?, ?, ?)
            """,
            (TS, "a" * 64, TS),
        )
        self.conn.execute(
            """
            INSERT INTO pipeline_runs
              (id, run_type, run_schema, run_schema_version, corpus_release_id,
               configuration_sha256, status, created_at, updated_at)
            VALUES ('pipeline_1', 'extract', 'atomic_claim', '1', 'release_1',
                    ?, 'succeeded', ?, ?)
            """,
            ("c" * 64, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO corpus_release_promotions
              (id, promotion_revision, corpus_release_id, pipeline_run_id,
               promoted_by, rationale, created_at)
            VALUES ('promotion_1', 1, 'release_1', 'pipeline_1',
                    'test', 'accepted test release', ?)
            """,
            (TS,),
        )
        # Schema v4 makes a succeeded/promoted run insufficient on its own:
        # each semantic stage must have an explicit, append-only authority
        # decision.  Keep this fixture aligned with that production contract
        # instead of relying on the pre-v4 promotion shortcut.
        for stage in (
            "release",
            "atomic_claims",
            "identities",
            "claims",
            "networks",
            "relations",
            "consensus",
            "contrarian",
            "outcomes",
        ):
            self.conn.execute(
                """
                INSERT INTO pipeline_run_authority_decisions
                  (id, authority_lineage_id, revision, stage, pipeline_run_id,
                   run_type, run_schema, run_schema_version, configuration_sha256,
                   model, model_version, prompt_version, corpus_release_id,
                   run_status, decision, reviewed_by, rationale, decided_at, created_at)
                SELECT ?, ?, 1, ?, id, run_type, run_schema, run_schema_version,
                       configuration_sha256, model, model_version, prompt_version,
                       corpus_release_id, status, 'accepted', 'fixture-reviewer',
                       'Explicit accepted-stage fixture authority.', ?, ?
                FROM pipeline_runs WHERE id = 'pipeline_1'
                """,
                (f"authority_{stage}", f"authority_lineage_{stage}", stage, TS, TS),
            )
        self.conn.execute(
            """
            INSERT INTO identity_resolution_judgments
              (id, identity_lineage_id, revision, corpus_release_id, pipeline_run_id,
               raw_mention_type, raw_mention_id, canonical_person_id, decision,
               rationale, judge_model, judge_schema_version, confidence,
               review_status, decided_at, created_at)
            VALUES ('identity_1', 'identity_lineage_1', 1, 'release_1', 'pipeline_1',
                    'speaker', 'fixture-speaker', 'accepted_person', 'accepted',
                    'accepted fixture identity', 'gpt-test', '1', 0.95,
                    'accepted', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO atomic_claims
              (id, claim_lineage_id, revision, corpus_release_id, pipeline_run_id,
               claim_text, claim_type, raw_speaker, canonical_person_id,
               stance, certainty, time_horizon,
               discourse_event_id, segment_id, source_id, episode_id, evidence_unit_type,
               evidence_unit_id, evidence_text, evidence_start, evidence_end, extractor_model,
               extractor_schema, extractor_schema_version, source_artifact_sha256,
               confidence, review_status, observed_at, created_at)
            VALUES ('atomic_1', 'lineage_1', 1, 'release_1', 'pipeline_1',
                    'Private atomic claim about a market outcome', 'forecast', 'Private Person',
                    'accepted_person', 'supportive', 'probable', 'two_years',
                    'evt_1', 'seg_0', 'src_1', 'ep_1',
                    'segment', 'seg_0', 'Private evidence unit', 0, 10, 'gpt-test',
                    'atomic_claim', '1', ?, 0.92, 'accepted', ?, ?)
            """,
            ("b" * 64, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO accepted_claim_subjects
              (id, subject_lineage_id, revision, corpus_release_id, pipeline_run_id,
               subject_text, subject_type, domain, judge_model, judge_schema_version,
               confidence, rationale, review_status, decided_at, created_at)
            VALUES ('accepted_subject_1', 'subject_lineage_1', 1, 'release_1', 'pipeline_1',
                    'Accepted Private Market Subject', 'forecast_topic', 'markets',
                    'gpt-test', '1', 0.93, 'supported by accepted claim',
                    'accepted', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO accepted_proposition_variants
              (id, variant_lineage_id, revision, corpus_release_id, pipeline_run_id,
               subject_id, proposition_text, judge_model, judge_schema_version,
               confidence, rationale, review_status, decided_at, created_at)
            VALUES ('accepted_variant_1', 'variant_lineage_1', 1, 'release_1', 'pipeline_1',
                    'accepted_subject_1', 'Accepted proposition variant', 'gpt-test', '1',
                    0.92, 'supported variant', 'accepted', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO accepted_position_observations
              (id, position_lineage_id, revision, corpus_release_id, pipeline_run_id,
               subject_id, variant_id, atomic_claim_id, canonical_person_id,
               position, certainty, observed_at, judge_model, judge_schema_version,
               confidence, rationale, review_status, decided_at, created_at)
            VALUES ('accepted_position_1', 'position_lineage_1', 1, 'release_1', 'pipeline_1',
                    'accepted_subject_1', 'accepted_variant_1', 'atomic_1', 'accepted_person',
                    'supports', 'probable', ?, 'gpt-test', '1', 0.94,
                    'supported position', 'accepted', ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.commit()

    def test_claim_subject_observation_count_is_not_multiplied_by_variants(self) -> None:
        self._subject_observations()
        views = _claim_subject_views(self.conn, max_published_day="2026-01-31")
        row = next(item for item in views["top"] if item["claim_subject"] == "Private Subject")
        self.assertEqual(row["observation_count"], 2)
        self.assertEqual(row["claim_observation_count"], 2)
        self.assertEqual(row["variant_count"], 2)

    def test_public_snapshot_is_strictly_operational(self) -> None:
        self._subject_observations()
        export_dir = self.root / "exports"
        export_dir.mkdir()
        (export_dir / "private-person-network.json").write_text("{}", encoding="utf-8")
        scheduler_unavailable = {
            "state": "unavailable",
            "jobs": [],
            "enabled_count": 0,
            "disabled_count": 0,
            "evidence": "codex_cron_binary_not_found",
        }
        with patch("research_factory.observer.exports_dir", return_value=export_dir), patch(
            "research_factory.observer._codex_scheduler_status",
            return_value=scheduler_unavailable,
        ), patch.dict(
            os.environ,
            {"PIF_MCP_PHASE2_ENABLED": "0", "PIF_RAILWAY_MODULE": ""},
            clear=False,
        ):
            snapshot = build_snapshot(self.conn)
        self.assertEqual(
            set(snapshot),
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
        serialized = json.dumps(snapshot)
        for private_value in ("Private Subject", "Private Person", "Private claim text", "private-person-network"):
            self.assertNotIn(private_value, serialized)
        self.assertEqual(snapshot["runs"]["worker_status"]["remote"]["state"], "disabled")
        self.assertEqual(snapshot["runs"]["worker_status"]["local_headless"]["state"], "not_observed")

    def test_scheduler_probe_parser_excludes_non_project_compactor_and_reports_enabled_state(self) -> None:
        listed = "\n".join(
            [
                "pif-local-extractor\tdisabled\tscheduler\tevery 10 minute(s)\t/project",
                "pif-observer-publish\tenabled\tscheduler\tevery 30 minute(s)\t/project",
                "pif-discovery-thread-compactor\tenabled\tscheduler\tevery 5 minute(s)\t/Users/test",
                "daily-codex-memory-curator\tenabled\tscheduler\tdaily\t/other",
                "Private Claim Name\tenabled\tscheduler\tdaily\t/project",
            ]
        )
        status = "\n".join(
            [
                "pif-local-extractor: disabled",
                "pif-observer-publish: scheduler-managed",
                "pif-discovery-thread-compactor: scheduler-managed",
            ]
        )
        self.assertEqual(
            _parse_codex_cron_output(listed, status),
            [
                {"name": "pif-local-extractor", "enabled": False},
                {"name": "pif-observer-publish", "enabled": True},
            ],
        )

    def test_scheduler_probe_is_unavailable_when_binary_is_absent(self) -> None:
        with patch("research_factory.observer._find_codex_cron", return_value=None), patch.dict(
            os.environ,
            {"RAILWAY_ENVIRONMENT": ""},
            clear=False,
        ):
            status = _codex_scheduler_status()
        self.assertEqual(status["state"], "unavailable")
        self.assertEqual(status["jobs"], [])
        self.assertEqual(status["evidence"], "codex_cron_binary_not_found")

    def test_ui_contract_rejects_analytical_top_level_fields(self) -> None:
        payload = {
            "contract_version": "railway-operational-v2",
            "generated_at": TS,
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {"Private claim count key": 1},
            "queues": {},
            "runs": {
                "label_runs": [
                    {"model": "Private claim text hidden in a model field", "status": "ready"}
                ]
            },
            "failures": {},
            "artifacts": [],
            "intervention_flags": [],
            "trend_metrics": {"claim_samples": ["Private claim"]},
        }
        with self.assertRaisesRegex(ValueError, "non-operational fields"):
            _validate_snapshot_contract(payload, reject_unknown=True)
        sanitized = _sanitize_snapshot(payload)
        self.assertNotIn("trend_metrics", sanitized)
        self.assertNotIn("Private claim", json.dumps(sanitized))
        self.assertNotIn("Claim Subject", HTML)
        self.assertNotIn("transcript", HTML.lower())

    def test_broker_snapshot_preserves_only_v2_operational_aggregates(self) -> None:
        payload = {
            "contract_version": "railway-operational-v2",
            "generated_at": TS,
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {"jobs": 2},
            "queues": {
                "by_status": {"claimed": 2},
                "by_role_content_status": [
                    {"worker_role": "extractor", "content_type": "podcast_episode", "status": "claimed", "count": 2}
                ],
                "remote_claimable_by_privacy_tier": {"full_text_allowed": 1},
            },
            "runs": {
                "worker_runs": [],
                "worker_status": {
                    "remote": {"state": "active", "active_claims": 2, "evidence": "database_leases"}
                },
                "label_runs": [
                    {"model": "Private claim text hidden in model", "status": "claimed"}
                ],
                "service_status": {},
            },
            "failures": {
                "output_submissions": [{"status": "validation_failed", "count": 1}],
                "remote_import_failures": 1,
            },
            "artifacts": [{"artifact_type": "json", "count": 1, "bytes": 10, "most_recent_at": TS}],
            "intervention_flags": [],
        }
        snapshot = sanitize_broker_snapshot(payload)
        self.assertEqual(snapshot["privacy"], "sanitized_broker_state_no_raw_transcripts")
        self.assertNotIn("Private claim", json.dumps(snapshot))
        self.assertEqual(snapshot["queues"]["by_status"], {"claimed": 2})
        status = status_from_snapshot(snapshot)
        self.assertEqual(status["active_remote_leases"], 2)
        self.assertEqual(status["remote_import_failures"], 1)

    def test_export_signal_sections_honor_requested_window(self) -> None:
        for window, suffix in (("month", "monthly"), ("week", "weekly")):
            self.conn.execute(
                """
                INSERT INTO signal_runs
                  (id, run_type, window, status, created_at)
                VALUES (?, 'detect_shifts', ?, 'completed', ?)
                """,
                (f"run_{suffix}", window, TS),
            )
            self.conn.execute(
                """
                INSERT INTO shift_signals
                  (id, signal_run_id, signal_type, concept_name, term_a, window_start,
                   window_end, score, support, summary, created_at)
                VALUES (?, ?, 'term_burst', ?, ?, '2026-01', '2026-01', 0.9, 3, 'summary', ?)
                """,
                (f"signal_{suffix}", f"run_{suffix}", f"{suffix}-concept", f"{suffix}-term", TS),
            )
        self.conn.commit()
        term_path = export_term_drift_report(
            self.conn,
            window="month",
            output=str(self.root / "term.md"),
            as_of=TS,
        )
        term_text = term_path.read_text(encoding="utf-8")
        self.assertIn("monthly-concept", term_text)
        self.assertNotIn("weekly-concept", term_text)
        narrative_path = export_narrative_map(
            self.conn,
            window="month",
            output=str(self.root / "map.json"),
            as_of=TS,
        )
        signals = json.loads(narrative_path.read_text(encoding="utf-8"))["signals"]
        self.assertEqual([row["concept_name"] for row in signals], ["monthly-concept"])

    def test_windowed_exports_exclude_rows_before_current_calendar_bucket(self) -> None:
        self._subject_observations()
        old_ts = "2025-12-15T12:00:00+00:00"
        self.conn.execute(
            "UPDATE labels SET output_json = ? WHERE id = 'lbl_0'",
            (json.dumps({"topics": [{"topic": "market"}]}),),
        )
        self.conn.execute(
            """
            UPDATE discourse_events
            SET actor_name = 'Current Window Actor', candidate_concept = 'Current Window Concept',
                surface_terms_json = '["current-window-term"]'
            WHERE id = 'evt_1'
            """
        )
        self.conn.execute(
            """
            INSERT INTO term_usages
              (id, discourse_event_id, segment_id, term, actor_name, term_role,
               confidence, created_at)
            VALUES ('term_current', 'evt_1', 'seg_0', 'current-window-term',
                    'Current Window Actor', 'usage', 0.9, ?)
            """,
            (TS,),
        )
        self.conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, published_at, created_at, updated_at)
            VALUES ('ep_old', 'src_1', 'guid-old', 'OLD WINDOW EPISODE', ?, ?, ?)
            """,
            (old_ts, old_ts, old_ts),
        )
        self.conn.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256, status,
               created_at, updated_at)
            VALUES ('tr_old', 'ep_old', 'official', '/private/old.txt', 'old-hash',
                    'ready', ?, ?)
            """,
            (old_ts, old_ts),
        )
        self.conn.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index, start_char,
               end_char, text_path, text_sha256, word_count, created_at)
            VALUES ('seg_old', 'tr_old', 'ep_old', 'src_1', 0, 0, 10,
                    '/private/old-segment.txt', 'old-segment-hash', 10, ?)
            """,
            (old_ts,),
        )
        self.conn.execute(
            """
            INSERT INTO labels
              (id, segment_id, label_pack, label_pack_version, model, status,
               output_json, created_at)
            VALUES ('lbl_old', 'seg_old', 'ai_discourse_v3_1', 'v1', 'gpt-test',
                    'ready', ?, ?)
            """,
            (json.dumps({"topics": [{"topic": "market"}]}), old_ts),
        )
        self.conn.execute(
            """
            INSERT INTO claims
              (id, label_id, segment_id, text, stance, confidence, created_at)
            VALUES ('clm_old', 'lbl_old', 'seg_old', 'OLD WINDOW CLAIM',
                    'supportive', 0.9, ?)
            """,
            (old_ts,),
        )
        self.conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, actor_name,
               candidate_concept, claim_text, evidence_text, evidence_start,
               evidence_end, surface_terms_json, created_at)
            VALUES ('evt_old', 'lbl_old', 'seg_old', 0, 'claim', 'OLD WINDOW ACTOR',
                    'OLD WINDOW CONCEPT', 'OLD WINDOW CLAIM', 'old evidence', 0, 10,
                    '["old-window-term"]', ?)
            """,
            (old_ts,),
        )
        self.conn.execute(
            """
            INSERT INTO term_usages
              (id, discourse_event_id, segment_id, term, actor_name, term_role,
               confidence, created_at)
            VALUES ('term_old', 'evt_old', 'seg_old', 'old-window-term',
                    'OLD WINDOW ACTOR', 'usage', 0.9, ?)
            """,
            (old_ts,),
        )
        self.conn.execute(
            """
            INSERT INTO claim_proposition_variants
              (id, subject_id, variant_text, variant_key, status, confidence,
               method, created_at, updated_at)
            VALUES ('variant_old', 'subject_1', 'OLD WINDOW VARIANT', 'old_window_variant',
                    'active', 0.9, 'test', ?, ?)
            """,
            (old_ts, old_ts),
        )
        self.conn.execute(
            """
            INSERT INTO claim_position_observations
              (id, subject_id, variant_id, claim_id, discourse_event_id, speaker_name,
               source_id, episode_id, stance, published_at, confidence, method,
               created_at, updated_at)
            VALUES ('position_old', 'subject_1', 'variant_old', 'clm_old', 'evt_old',
                    'OLD WINDOW PERSON', 'src_1', 'ep_old', 'supportive', ?, 0.9,
                    'test', ?, ?)
            """,
            (old_ts, old_ts, old_ts),
        )
        self.conn.commit()

        trend = export_trend_report(
            self.conn,
            topic="market",
            window="month",
            output=str(self.root / "window-trend.md"),
            as_of=TS,
        ).read_text(encoding="utf-8")
        stance = export_actor_stance_report(
            self.conn,
            window="month",
            output=str(self.root / "window-stance.md"),
            as_of=TS,
        ).read_text(encoding="utf-8")
        terms = export_term_drift_report(
            self.conn,
            window="month",
            output=str(self.root / "window-terms.md"),
            as_of=TS,
        ).read_text(encoding="utf-8")
        narrative = export_narrative_map(
            self.conn,
            window="month",
            output=str(self.root / "window-map.json"),
            as_of=TS,
        ).read_text(encoding="utf-8")
        for rendered in (trend, stance, terms, narrative):
            self.assertNotIn("OLD WINDOW", rendered)
            self.assertNotIn("old-window-term", rendered)
        self.assertIn("Private Episode Title", trend)
        self.assertIn("Private Person", stance)
        self.assertIn("current-window-term", terms)
        self.assertIn("Current Window Actor", narrative)

    def test_private_query_service_prefers_current_accepted_schema_and_is_read_only(self) -> None:
        self._atomic_claim()
        service = LocalQueryService(self.conn)
        capabilities = service.capabilities()
        self.assertTrue(capabilities["private_local_only"])
        self.assertTrue(capabilities["read_only"])
        self.assertEqual(
            capabilities["preferred_views"]["atomic_claims"],
            "current_accepted_atomic_claims",
        )
        search = service.search("market outcome")
        self.assertEqual(search["data"][0]["id"], "atomic_1")
        self.assertEqual(search["data"][0]["canonical_person_id"], "accepted_person")
        self.assertEqual([row["id"] for row in service.people()["data"]], ["accepted_person"])
        self.assertEqual(service.people(query="Candidate Private Person")["data"], [])
        subjects = service.subjects(query="Market Subject")
        self.assertEqual(subjects["data"][0]["id"], "accepted_subject_1")
        self.assertEqual(subjects["data"][0]["position_count"], 1)
        subject_lineage = service.lineage("accepted_subject_1", kind="subject")
        self.assertEqual(subject_lineage["data"]["variants"][0]["id"], "accepted_variant_1")
        self.assertEqual(subject_lineage["data"]["positions"][0]["id"], "accepted_position_1")
        lineage = service.lineage("atomic_1")
        self.assertEqual(lineage["data"]["claim"]["canonical_person_id"], "accepted_person")
        self.assertEqual(lineage["data"]["pipeline_run"][0]["id"], "pipeline_1")
        self.assertEqual(lineage["data"]["corpus_release"][0]["id"], "release_1")
        with self.assertRaises(sqlite3.OperationalError):
            self.conn.execute("DELETE FROM atomic_claims WHERE id = 'atomic_1'")

    def test_accuracy_graph_and_relation_scope_are_wired_to_accepted_surfaces(self) -> None:
        self._atomic_claim()
        self.conn.execute(
            """
            INSERT INTO atomic_claims
              (id, claim_lineage_id, revision, corpus_release_id, pipeline_run_id,
               claim_text, claim_type, raw_speaker, canonical_person_id,
               stance, certainty, time_horizon, discourse_event_id, segment_id,
               source_id, episode_id, evidence_unit_type, evidence_unit_id,
               evidence_text, evidence_start, evidence_end, extractor_model,
               extractor_schema, extractor_schema_version, source_artifact_sha256,
               confidence, review_status, observed_at, created_at)
            SELECT 'atomic_2', 'lineage_2', 1, corpus_release_id, pipeline_run_id,
                   'Second accepted private claim', claim_type, raw_speaker,
                   canonical_person_id, stance, certainty, time_horizon,
                   discourse_event_id, segment_id, source_id, episode_id,
                   evidence_unit_type, evidence_unit_id, evidence_text,
                   evidence_start, evidence_end, extractor_model, extractor_schema,
                   extractor_schema_version, source_artifact_sha256, confidence,
                   review_status, observed_at, created_at
            FROM atomic_claims WHERE id = 'atomic_1'
            """
        )
        self.conn.execute(
            """
            INSERT INTO claim_relation_judgments
              (id, relation_lineage_id, revision, corpus_release_id, pipeline_run_id,
               source_claim_id, target_claim_id, relation, temporal_scope,
               confidence, rationale, evidence_json, judge_model, review_status,
               decided_at, created_at)
            VALUES ('relation_1', 'relation_lineage_1', 1, 'release_1', 'pipeline_1',
                    'atomic_1', 'atomic_2', 'qualifies', 'same published episode',
                    0.91, 'accepted temporal qualification', '{}', 'gpt-test',
                    'accepted', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.commit()

        service = LocalQueryService(self.conn)
        capabilities = service.capabilities()
        self.assertTrue(capabilities["surfaces"]["accuracy"])
        self.assertTrue(capabilities["surfaces"]["graphs"])
        history = service.accuracy_history("accepted_person", as_of=TS)
        self.assertEqual(history["data"]["accepted_claim_count"], 1)
        self.assertEqual(history["data"]["categorical"]["denominator"], 0)
        rankings = service.accuracy_rankings(as_of=TS)
        self.assertEqual(rankings["data"]["eligible_person_count"], 0)
        self.assertEqual(rankings["data"]["rankings"], [])
        graphs = service.graphs(as_of=TS)
        self.assertEqual(graphs["data"]["authority"], "current_accepted_only")
        relations = service.networks(claim_id="atomic_1")["data"]["claim_relations"]
        self.assertEqual(relations[0]["temporal_scope"], "same published episode")
        self.assertEqual(relations[0]["pipeline_run_id"], "pipeline_1")

    def test_private_query_requires_explicit_legacy_opt_in(self) -> None:
        self._subject_observations()
        self.conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, confidence, status,
               canonical_version, created_at, updated_at)
            VALUES ('candidate_legacy', 'Legacy Candidate Person', 'legacy candidate person',
                    0.8, 'candidate', 1, ?, ?)
            """,
            (TS, TS),
        )
        self.conn.commit()

        strict = LocalQueryService(self.conn, enforce_query_only=False)
        self.assertEqual(strict.search("Private claim")["data"], [])
        self.assertEqual(strict.people(query="Legacy Candidate")["data"], [])
        self.assertEqual(strict.subjects(query="Private Subject")["data"], [])
        self.assertTrue(strict.capabilities()["surfaces"]["subjects"])
        self.assertFalse(strict.capabilities()["legacy_fallbacks"]["enabled"])

        legacy = LocalQueryService(
            self.conn,
            enforce_query_only=False,
            allow_legacy=True,
        )
        self.assertEqual(legacy.search("Private claim")["data"][0]["result_type"], "legacy_claim")
        self.assertEqual(legacy.subjects(query="Private Subject")["data"][0]["id"], "subject_1")
        self.assertEqual(legacy.people(query="Legacy Candidate")["data"][0]["id"], "candidate_legacy")


if __name__ == "__main__":
    unittest.main()
