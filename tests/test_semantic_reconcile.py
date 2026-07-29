from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import db
from research_factory import semantic_reconcile as reconcile


TS = "2026-07-20T12:00:00+00:00"


class SemanticReconciliationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = db.connect(self.root / "factory.sqlite")
        db.init_db(self.conn)
        self._seed_release_without_promotion()

    def tearDown(self) -> None:
        self.conn.close()
        self.temp.cleanup()

    def _seed_release_without_promotion(self) -> None:
        self.conn.execute(
            """
            INSERT INTO sources
              (id, name, homepage_url, created_at, updated_at)
            VALUES ('source_1', 'Fixture Show', 'https://example.test/show', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, description, published_at, created_at, updated_at)
            VALUES ('episode_1', 'source_1', 'guid-1', 'Fixture Episode',
                    'Fixture description', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO corpus_releases
              (id, release_version, cutoff_at, manifest_sha256, manifest_json,
               source_count, item_count, status, created_at)
            VALUES ('release_1', 1, ?, ?, '{}', 1, 1, 'accepted', ?)
            """,
            (TS, "a" * 64, TS),
        )
        self.conn.execute(
            """
            INSERT INTO corpus_release_episodes
              (corpus_release_id, episode_id, content_sha256, member_index, created_at)
            VALUES ('release_1', 'episode_1', ?, 0, ?)
            """,
            ("b" * 64, TS),
        )
        self.conn.execute(
            """
            INSERT INTO pipeline_runs
              (id, run_type, run_schema, run_schema_version, corpus_release_id,
               configuration_sha256, status, parameters_json, metrics_json,
               receipt_json, input_count, output_count, failure_count,
               started_at, completed_at, created_at, updated_at)
            VALUES ('release_run_1', 'release_build', 'corpus_release_build',
                    'corpus_release_build_v1',
                    'release_1', ?, 'succeeded', '{}', '{}', '{}', 1, 1, 0,
                    ?, ?, ?, ?)
            """,
            ("c" * 64, TS, TS, TS, TS),
        )
        self.conn.commit()

    def _promote_release(self) -> None:
        from research_factory.intelligence import accept_pipeline_run

        accept_pipeline_run(
            self.conn,
            "release_run_1",
            stage="release",
            reviewed_by="fixture-reviewer",
            rationale="Accept fixture release run.",
            decided_at=TS,
        )
        self.conn.execute(
            """
            INSERT INTO corpus_release_promotions
              (id, promotion_revision, corpus_release_id, pipeline_run_id,
               promoted_by, rationale, created_at)
            VALUES ('promotion_1', 1, 'release_1', 'release_run_1',
                    'fixture', 'fixture accepted release', ?)
            """,
            (TS,),
        )
        self.conn.commit()

    def _ensure_stage_run(
        self,
        *,
        run_id: str,
        run_type: str,
        run_schema: str,
        run_schema_version: str,
        stage: str,
    ) -> None:
        from research_factory.intelligence import accept_pipeline_run

        self.conn.execute(
            """
            INSERT OR IGNORE INTO pipeline_runs
              (id, run_type, run_schema, run_schema_version, corpus_release_id,
               configuration_sha256, status, parameters_json, metrics_json,
               receipt_json, input_count, output_count, failure_count,
               started_at, completed_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'release_1', ?, 'succeeded', '{}', '{}', '{}',
                    1, 1, 0, ?, ?, ?, ?)
            """,
            (
                run_id,
                run_type,
                run_schema,
                run_schema_version,
                (run_id[0] * 64)[:64],
                TS,
                TS,
                TS,
                TS,
            ),
        )
        if (
            self.conn.execute(
                """
                SELECT COUNT(*) FROM pipeline_run_authority_decisions
                WHERE pipeline_run_id = ? AND stage = ? AND decision = 'accepted'
                """,
                (run_id, stage),
            ).fetchone()[0]
            == 0
        ):
            accept_pipeline_run(
                self.conn,
                run_id,
                stage=stage,
                reviewed_by="fixture-reviewer",
                rationale=f"Accept fixture {stage} run.",
                decided_at=TS,
            )
        self.conn.commit()

    def _add_episode(
        self,
        episode_id: str,
        *,
        source_id: str = "source_1",
        published_at: str = TS,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, description, published_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'Fixture description', ?, ?, ?)
            """,
            (episode_id, source_id, f"guid-{episode_id}", f"Title {episode_id}", published_at, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO corpus_release_episodes
              (corpus_release_id, episode_id, content_sha256, member_index, created_at)
            VALUES ('release_1', ?, ?, 99, ?)
            """,
            (episode_id, (episode_id[0] * 64)[:64], TS),
        )
        self.conn.commit()

    def _seed_claim_occurrence(
        self,
        suffix: str,
        *,
        surface_name: str,
        role: str | None = "guest",
        affiliation: str | None = "Fixture Org",
        person_id: str | None = None,
        episode_id: str = "episode_1",
        source_id: str = "source_1",
        observed_at: str = TS,
        claim_person_id: str | None = None,
    ) -> dict[str, str]:
        transcript_id = f"transcript_{suffix}"
        segment_id = f"segment_{suffix}"
        label_id = f"label_{suffix}"
        event_id = f"event_{suffix}"
        mention_id = f"mention_{suffix}"
        claim_id = f"claim_{suffix}"
        self.conn.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
               status, word_count, created_at, updated_at)
            VALUES (?, ?, 'official', ?, ?, 'ready', 3, ?, ?)
            """,
            (transcript_id, episode_id, f"/{transcript_id}.txt", suffix * 8, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index,
               start_char, end_char, text_path, text_sha256, word_count, created_at)
            VALUES (?, ?, ?, ?, 0, 0, 24, ?, ?, 3, ?)
            """,
            (segment_id, transcript_id, episode_id, source_id, f"/{segment_id}.txt", suffix * 8, TS),
        )
        self.conn.execute(
            """
            INSERT INTO labels
              (id, segment_id, label_pack, label_pack_version, model, status,
               output_json, confidence, needs_review, created_at)
            VALUES (?, ?, 'ai_discourse_v3_1', '3.1', 'gpt-5.5', 'ready',
                    '{}', 0.95, 0, ?)
            """,
            (label_id, segment_id, TS),
        )
        self.conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, actor_name,
               stance, claim_text, claim_type, certainty, temporal_horizon,
               confidence, evidence_text, evidence_start, evidence_end, created_at)
            VALUES (?, ?, ?, 0, 'claim', ?, 'supports', ?, 'forecast', 'high',
                    'near_term', 0.95, ?, 0, 24, ?)
            """,
            (event_id, label_id, segment_id, surface_name, f"Claim text {suffix}", f"Exact evidence {suffix}", TS),
        )
        self.conn.execute(
            """
            INSERT INTO raw_speaker_mentions
              (id, episode_id, segment_id, discourse_event_id, surface_name, role,
               affiliation_surface, resolution_status, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved', 0.95, ?, ?)
            """,
            (
                mention_id,
                episode_id,
                segment_id,
                event_id,
                surface_name,
                role,
                affiliation,
                json.dumps({"exact": f"Exact evidence {suffix}"}, sort_keys=True),
                TS,
            ),
        )
        if person_id is not None:
            self._ensure_stage_run(
                run_id="identity_run_1",
                run_type="semantic_reconcile_identities",
                run_schema=reconcile.OUTPUT_SCHEMA_VERSION,
                run_schema_version="1",
                stage="identities",
            )
            self.conn.execute(
                """
                INSERT OR IGNORE INTO canonical_people
                  (id, display_name, normalized_name, confidence, status,
                   canonical_version, evidence_json, created_at, updated_at)
                VALUES (?, ?, ?, 0.95, 'accepted', 1, '{}', ?, ?)
                """,
                (person_id, f"Display {person_id}", person_id, TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO identity_resolution_judgments
                  (id, identity_lineage_id, revision, corpus_release_id, pipeline_run_id,
                   raw_mention_type, raw_mention_id, canonical_person_id, decision,
                   rationale, evidence_json, judge_model, judge_schema_version,
                   confidence, review_status, decided_at, created_at)
                VALUES (?, ?, 1, 'release_1', 'identity_run_1', 'speaker', ?, ?,
                        'accepted', 'Exact fixture identity.', '{}', 'gpt-5.5', 'v1',
                        0.95, 'accepted', ?, ?)
                """,
                (f"identity_{suffix}", f"identity_lineage_{suffix}", mention_id, person_id, TS, TS),
            )
        self._ensure_stage_run(
            run_id="atomic_run_1",
            run_type="atomic_claim_import",
            run_schema="atomic_claim_v1",
            run_schema_version="atomic_claim_import_v1",
            stage="atomic_claims",
        )
        self.conn.execute(
            """
            INSERT INTO atomic_claims
              (id, claim_lineage_id, revision, corpus_release_id, pipeline_run_id,
               claim_text, claim_type, raw_speaker, canonical_person_id, stance,
               certainty, time_horizon, discourse_event_id, segment_id, source_id,
               episode_id, evidence_unit_type, evidence_unit_id, evidence_text,
               evidence_start, evidence_end, extractor_model, extractor_schema,
               extractor_schema_version, source_artifact_sha256, confidence,
               review_status, observed_at, created_at)
            VALUES (?, ?, 1, 'release_1', 'atomic_run_1', ?, 'forecast', ?, ?,
                    'supports', 'high', 'near_term', ?, ?, ?, ?, 'segment', ?, ?,
                    0, 24, 'gpt-5.5', 'ai_discourse_v3_1', '3.1', ?, 0.95,
                    'accepted', ?, ?)
            """,
            (
                claim_id,
                f"claim_lineage_{suffix}",
                f"Claim text {suffix}",
                surface_name,
                claim_person_id,
                event_id,
                segment_id,
                source_id,
                episode_id,
                segment_id,
                f"Exact evidence {suffix}",
                suffix * 8,
                observed_at,
                TS,
            ),
        )
        self.conn.commit()
        return {
            "claim_id": claim_id,
            "mention_id": mention_id,
            "event_id": event_id,
            "segment_id": segment_id,
        }

    def _prepare_network_packet(self, output_dir: Path | None = None) -> tuple[dict, dict]:
        with patch.object(reconcile, "now_iso", return_value=TS):
            receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="networks",
                release_id="release_1",
                limit=10,
                output_dir=output_dir or self.root / "artifacts",
            )
        packet = json.loads(Path(receipt["packet_path"]).read_text(encoding="utf-8"))
        return receipt, packet

    def _output(self, packet: dict, *, source_id: str = "source_1", kind: str = "publisher") -> dict:
        return {
            "schema_version": reconcile.OUTPUT_SCHEMA_VERSION,
            "target": "networks",
            "corpus_release_id": packet["corpus_release_id"],
            "packet_sha256": packet["packet_sha256"],
            "decisions": {
                "affiliations": [
                    {
                        "source_id": source_id,
                        "affiliation_kind": kind,
                        "affiliation_key": "fixture-publisher",
                        "affiliation_name": "Fixture Publisher",
                        "canonical_org_id": None,
                        "valid_from": None,
                        "valid_to": None,
                        "confidence": 0.95,
                        "evidence": {
                            "item_ids": ["episode_1"],
                            "note": "Fixture episode metadata supports the affiliation.",
                            "source_urls": ["https://example.test/show"],
                        },
                    }
                ],
                "appearances": [],
            },
            "abstentions": [],
        }

    def _identity_output(
        self,
        packet: dict,
        *,
        decision: str = "accepted",
    ) -> dict:
        item = packet["items"][0]
        has_person = decision not in {"unknown", "rejected"}
        evidence = {
            "item_ids": [item["item_id"]],
            "note": "The grouped exact episode evidence supports this judgment.",
            "source_urls": ["https://example.test/show"],
        }
        return {
            "schema_version": reconcile.OUTPUT_SCHEMA_VERSION,
            "target": "identities",
            "corpus_release_id": packet["corpus_release_id"],
            "packet_sha256": packet["packet_sha256"],
            "decisions": {
                "people": (
                    [
                        {
                            "person_key": "person-key-1",
                            "display_name": "Grouped Person",
                            "normalized_name": "grouped person",
                            "decision": "accepted",
                            "confidence": 0.97,
                            "rationale": "The exact episode evidence identifies one person.",
                            "evidence": evidence,
                        }
                    ]
                    if has_person
                    else []
                ),
                "judgments": [
                    {
                        "item_id": item["item_id"],
                        "person_key": "person-key-1" if has_person else None,
                        "decision": decision,
                        "confidence": 0.97,
                        "rationale": "The grouped occurrences have one supported resolution.",
                        "evidence": evidence,
                    }
                ],
            },
            "abstentions": [],
        }

    def _claim_output(self, packet: dict, *, person_id: str) -> dict:
        item = packet["items"][0]
        evidence = {
            "item_ids": [item["item_id"]],
            "note": "The exact claim evidence supports the decision.",
            "source_urls": [],
        }
        return {
            "schema_version": reconcile.OUTPUT_SCHEMA_VERSION,
            "target": "claims",
            "corpus_release_id": packet["corpus_release_id"],
            "packet_sha256": packet["packet_sha256"],
            "decisions": {
                "subjects": [
                    {
                        "subject_key": "subject-1",
                        "subject_text": "Fixture subject",
                        "subject_type": "forecast",
                        "domain": "testing",
                        "scope_note": None,
                        "confidence": 0.9,
                        "rationale": "Exact fixture evidence.",
                        "evidence": evidence,
                    }
                ],
                "variants": [
                    {
                        "variant_key": "variant-1",
                        "subject_key": "subject-1",
                        "proposition_text": "Fixture proposition",
                        "predicate_text": None,
                        "object_text": None,
                        "polarity": "positive",
                        "time_horizon": "near_term",
                        "conditions": {"condition_texts": []},
                        "confidence": 0.9,
                        "rationale": "Exact fixture evidence.",
                        "evidence": evidence,
                    }
                ],
                "positions": [
                    {
                        "atomic_claim_id": item["atomic_claim_id"],
                        "canonical_person_id": person_id,
                        "subject_key": "subject-1",
                        "variant_key": "variant-1",
                        "position": "supports",
                        "certainty": "high",
                        "observed_at": item["observed_at"],
                        "confidence": 0.9,
                        "rationale": "Exact fixture evidence.",
                        "evidence": evidence,
                    }
                ],
            },
            "abstentions": [],
        }

    def _write_output(self, name: str, value: dict) -> Path:
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        return path

    def test_prepare_requires_the_current_accepted_promoted_release(self) -> None:
        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "accepted promoted corpus release",
        ):
            reconcile.prepare_reconciliation_packet(
                self.conn,
                target="networks",
                release_id="release_1",
                output_dir=self.root / "not-promoted",
            )

        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        self.assertEqual(receipt["corpus_release_id"], "release_1")
        self.assertEqual(packet["item_count"], 1)

    def test_packet_is_hash_bound_and_refuses_an_overwrite(self) -> None:
        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        packet_path = Path(receipt["packet_path"])

        replay, replay_packet = self._prepare_network_packet()
        self.assertEqual(replay["packet_path"], receipt["packet_path"])
        self.assertEqual(replay_packet, packet)
        self.assertFalse(receipt["model_execution_attempted"])
        self.assertFalse(receipt["semantic_inference_performed"])

        tampered = dict(packet)
        tampered["semantic_authority"] = "tampered"
        packet_path.write_text(json.dumps(tampered, sort_keys=True), encoding="utf-8")
        with self.assertRaises(FileExistsError):
            self._prepare_network_packet()

        output_path = self._write_output("tampered-output.json", self._output(packet))
        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "packet hash does not verify",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=packet_path,
                output_path=output_path,
            )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_type LIKE 'semantic_reconcile_%'"
            ).fetchone()[0],
            0,
        )

    def test_import_rejects_decisions_outside_the_packet_scope_before_mutation(self) -> None:
        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        outside = self._output(packet, source_id="source_outside_packet")
        output_path = self._write_output("outside.json", outside)

        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "affiliation is outside packet scope",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=output_path,
            )

        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM source_affiliations").fetchone()[0], 0)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_type LIKE 'semantic_reconcile_%'"
            ).fetchone()[0],
            0,
        )

    def test_import_is_idempotent_and_rolls_back_an_invalid_decision(self) -> None:
        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        valid_path = self._write_output("valid.json", self._output(packet))

        first = reconcile.import_reconciliation_output(
            self.conn,
            packet_path=receipt["packet_path"],
            output_path=valid_path,
        )
        replay = reconcile.import_reconciliation_output(
            self.conn,
            packet_path=receipt["packet_path"],
            output_path=valid_path,
        )
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(first["pipeline_run_id"], replay["pipeline_run_id"])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM source_affiliations").fetchone()[0], 1)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_type = 'semantic_reconcile_networks'"
            ).fetchone()[0],
            1,
        )

        invalid_path = self._write_output(
            "invalid-kind.json",
            self._output(packet, kind="not-an-affiliation-kind"),
        )
        with self.assertRaises(ValueError):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=invalid_path,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM source_affiliations").fetchone()[0], 1)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_type = 'semantic_reconcile_networks'"
            ).fetchone()[0],
            1,
        )

    def test_prepare_and_import_never_start_a_model(self) -> None:
        self._promote_release()
        with patch.object(
            reconcile,
            "CodexAppServerClient",
            side_effect=AssertionError("prepare/import must not start a model"),
        ) as app_server:
            receipt, packet = self._prepare_network_packet()
            output_path = self._write_output("no-model.json", self._output(packet))
            result = reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=output_path,
            )

        app_server.assert_not_called()
        self.assertTrue(result["ok"])
        self.assertFalse(result["canonical_mutation"])

    def test_accepting_import_requires_an_explicit_reviewer(self) -> None:
        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        output_path = self._write_output("accepted.json", self._output(packet))

        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "accepted import requires a reviewer label",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=output_path,
                accept=True,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM source_affiliations").fetchone()[0], 0)

    def test_import_recursively_rejects_missing_and_extra_nested_fields(self) -> None:
        self._promote_release()
        receipt, packet = self._prepare_network_packet()
        invalid = self._output(packet)
        invalid["decisions"]["affiliations"][0]["evidence"].pop("note")
        invalid["decisions"]["affiliations"][0]["unexpected"] = "must fail closed"
        output_path = self._write_output("invalid-nested.json", invalid)

        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "missing note|extra fields",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=output_path,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM source_affiliations").fetchone()[0], 0)

    def test_identity_group_import_expands_to_every_raw_mention(self) -> None:
        self._promote_release()
        first = self._seed_claim_occurrence("group_a", surface_name="Exact Person")
        second = self._seed_claim_occurrence("group_b", surface_name="Exact Person")
        with patch.object(reconcile, "now_iso", return_value=TS):
            receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="identities",
                release_id="release_1",
                limit=200,
                output_dir=self.root / "identity-groups",
            )
        packet = json.loads(Path(receipt["packet_path"]).read_text(encoding="utf-8"))
        self.assertEqual(packet["item_count"], 1)
        self.assertEqual(
            packet["items"][0]["raw_mention_ids"],
            [first["mention_id"], second["mention_id"]],
        )
        self.assertEqual(
            packet["items"][0]["grouping_basis"],
            "release_episode_exact_surface_role_affiliation",
        )

        output_path = self._write_output("identity-groups.json", self._identity_output(packet))
        imported = reconcile.import_reconciliation_output(
            self.conn,
            packet_path=receipt["packet_path"],
            output_path=output_path,
            accept=True,
            reviewer="fixture-reviewer",
        )
        self.assertEqual(imported["output_count"], 3)  # one person plus two judgments
        judgments = self.conn.execute(
            """
            SELECT raw_mention_id, decision, review_status
            FROM identity_resolution_judgments
            WHERE pipeline_run_id = ?
            ORDER BY raw_mention_id
            """,
            (imported["pipeline_run_id"],),
        ).fetchall()
        self.assertEqual(
            [(row["raw_mention_id"], row["decision"], row["review_status"]) for row in judgments],
            [
                (first["mention_id"], "accepted", "accepted"),
                (second["mention_id"], "accepted", "accepted"),
            ],
        )

        with patch.object(reconcile, "now_iso", return_value=TS):
            replay_receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="identities",
                release_id="release_1",
                limit=200,
                output_dir=self.root / "identity-after-import",
            )
        replay_packet = json.loads(
            Path(replay_receipt["packet_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(replay_packet["item_count"], 0)

    def test_identity_import_reuses_existing_normalized_person_lineage(self) -> None:
        self._promote_release()
        self._seed_claim_occurrence("reuse_person", surface_name="Grouped Person")
        self.conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, confidence, status,
               canonical_version, evidence_json, created_at, updated_at)
            VALUES ('existing_person', 'Grouped Person', 'grouped person', 0.5,
                    'candidate', 1, '{}', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.commit()
        with patch.object(reconcile, "now_iso", return_value=TS):
            receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="identities",
                release_id="release_1",
                output_dir=self.root / "identity-reuse",
            )
        packet = json.loads(Path(receipt["packet_path"]).read_text(encoding="utf-8"))
        output_path = self._write_output(
            "identity-reuse.json",
            self._identity_output(packet),
        )

        imported = reconcile.import_reconciliation_output(
            self.conn,
            packet_path=receipt["packet_path"],
            output_path=output_path,
            accept=True,
            reviewer="fixture-reviewer",
        )

        judgment = self.conn.execute(
            """
            SELECT canonical_person_id
            FROM identity_resolution_judgments
            WHERE pipeline_run_id = ?
            """,
            (imported["pipeline_run_id"],),
        ).fetchone()
        self.assertEqual(judgment["canonical_person_id"], "existing_person")
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM canonical_people WHERE normalized_name = 'grouped person'"
            ).fetchone()[0],
            1,
        )

    def test_accepted_unknown_identity_expands_to_every_group_member(self) -> None:
        self._promote_release()
        first = self._seed_claim_occurrence("unknown_a", surface_name="Unknown Speaker")
        second = self._seed_claim_occurrence("unknown_b", surface_name="Unknown Speaker")
        with patch.object(reconcile, "now_iso", return_value=TS):
            receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="identities",
                release_id="release_1",
                output_dir=self.root / "unknown-group",
            )
        packet = json.loads(Path(receipt["packet_path"]).read_text(encoding="utf-8"))
        output_path = self._write_output(
            "unknown-group.json",
            self._identity_output(packet, decision="unknown"),
        )
        imported = reconcile.import_reconciliation_output(
            self.conn,
            packet_path=receipt["packet_path"],
            output_path=output_path,
            accept=True,
            reviewer="fixture-reviewer",
        )
        rows = self.conn.execute(
            """
            SELECT raw_mention_id, canonical_person_id, decision
            FROM identity_resolution_judgments
            WHERE pipeline_run_id = ?
            ORDER BY raw_mention_id
            """,
            (imported["pipeline_run_id"],),
        ).fetchall()
        self.assertEqual(
            [(row["raw_mention_id"], row["canonical_person_id"], row["decision"]) for row in rows],
            [
                (first["mention_id"], None, "unknown"),
                (second["mention_id"], None, "unknown"),
            ],
        )

    def test_claim_person_comes_only_from_accepted_raw_mention_resolution(self) -> None:
        self._promote_release()
        self.conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, confidence, status,
               canonical_version, evidence_json, created_at, updated_at)
            VALUES ('person_stale', 'Stale Person', 'stale person', 0.5, 'candidate',
                    1, '{}', ?, ?)
            """,
            (TS, TS),
        )
        self.conn.commit()
        seeded = self._seed_claim_occurrence(
            "resolved",
            surface_name="Resolved Surface",
            person_id="person_resolved",
            claim_person_id="person_stale",
        )
        with patch.object(reconcile, "now_iso", return_value=TS):
            receipt = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="claims",
                release_id="release_1",
                output_dir=self.root / "claim-scope",
            )
        packet = json.loads(Path(receipt["packet_path"]).read_text(encoding="utf-8"))
        self.assertEqual(packet["item_count"], 1)
        item = packet["items"][0]
        self.assertEqual(item["atomic_claim_id"], seeded["claim_id"])
        self.assertEqual(item["canonical_person_id"], "person_resolved")
        self.assertEqual(item["canonical_person_display_name"], "Display person_resolved")
        self.assertEqual(
            item["identity_evidence"]["raw_speaker_mention_id"],
            seeded["mention_id"],
        )

        invalid_path = self._write_output(
            "invalid-claim-person.json",
            self._claim_output(packet, person_id="person_outside_packet"),
        )
        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "outside the claim identity scope",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=invalid_path,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM accepted_claim_subjects").fetchone()[0], 0)

    def test_network_appearance_requires_an_accepted_person_for_that_episode(self) -> None:
        self._promote_release()
        self._seed_claim_occurrence(
            "network_person",
            surface_name="Network Person",
            person_id="person_network",
        )
        receipt, packet = self._prepare_network_packet(self.root / "network-person-scope")
        self.assertEqual(
            [person["canonical_person_id"] for person in packet["items"][0]["accepted_people"]],
            ["person_network"],
        )
        invalid = self._output(packet)
        invalid["decisions"]["appearances"] = [
            {
                "canonical_person_id": "person_outside_packet",
                "episode_id": "episode_1",
                "source_id": "source_1",
                "source_affiliation_id": None,
                "role": "guest",
                "appeared_at": TS,
                "confidence": 0.9,
                "evidence": {
                    "item_ids": ["episode_1"],
                    "note": "Fixture appearance evidence.",
                    "source_urls": [],
                },
            }
        ]
        invalid_path = self._write_output("invalid-network-person.json", invalid)
        with self.assertRaisesRegex(
            reconcile.SemanticReconciliationError,
            "outside packet identity scope",
        ):
            reconcile.import_reconciliation_output(
                self.conn,
                packet_path=receipt["packet_path"],
                output_path=invalid_path,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM person_appearances").fetchone()[0], 0)

    def test_scope_is_hash_bound_and_filters_network_and_claim_candidates(self) -> None:
        self._add_episode("episode_old", published_at="2024-12-01T12:00:00+00:00")
        self._promote_release()
        self._seed_claim_occurrence(
            "recent_scope",
            surface_name="Recent Person",
            person_id="person_recent",
            observed_at=TS,
        )
        self._seed_claim_occurrence(
            "old_scope",
            surface_name="Old Person",
            person_id="person_old",
            episode_id="episode_old",
            observed_at="2024-12-01T12:00:00+00:00",
        )
        with patch.object(reconcile, "now_iso", return_value=TS):
            recent_network = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="networks",
                release_id="release_1",
                scope="last_18_months",
                scope_as_of=TS,
                output_dir=self.root / "recent-network",
            )
            all_network = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="networks",
                release_id="release_1",
                scope="all",
                scope_as_of=TS,
                output_dir=self.root / "all-network",
            )
            recent_claims = reconcile.prepare_reconciliation_packet(
                self.conn,
                target="claims",
                release_id="release_1",
                scope="last_18_months",
                scope_as_of=TS,
                output_dir=self.root / "recent-claims",
            )
        recent_packet = json.loads(Path(recent_network["packet_path"]).read_text(encoding="utf-8"))
        all_packet = json.loads(Path(all_network["packet_path"]).read_text(encoding="utf-8"))
        claim_packet = json.loads(Path(recent_claims["packet_path"]).read_text(encoding="utf-8"))
        self.assertEqual(recent_packet["scope"], "last_18_months")
        self.assertEqual(recent_packet["scope_as_of"], TS)
        self.assertEqual(recent_packet["scope_start"], "2025-01-20T12:00:00+00:00")
        self.assertEqual([item["episode_id"] for item in recent_packet["items"]], ["episode_1"])
        self.assertEqual({item["episode_id"] for item in all_packet["items"]}, {"episode_1", "episode_old"})
        self.assertEqual([item["atomic_claim_id"] for item in claim_packet["items"]], ["claim_recent_scope"])
        self.assertNotEqual(recent_packet["packet_sha256"], all_packet["packet_sha256"])
        self.assertEqual(
            reconcile.count_reconciliation_candidates(
                self.conn,
                target="networks",
                release_id="release_1",
                scope="last_18_months",
                scope_as_of=TS,
            ),
            1,
        )

    def test_claim_balancing_covers_people_and_shows_before_repeats(self) -> None:
        rows = [
            {
                "id": "claim_a1",
                "canonical_person_id": "person_a",
                "source_id": "show_a",
                "observed_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "id": "claim_a2",
                "canonical_person_id": "person_a",
                "source_id": "show_a",
                "observed_at": "2026-01-02T00:00:00+00:00",
            },
            {
                "id": "claim_b1",
                "canonical_person_id": "person_b",
                "source_id": "show_b",
                "observed_at": "2026-01-03T00:00:00+00:00",
            },
        ]
        selected = reconcile._balanced_claim_rows(rows, 2)
        self.assertEqual([row["id"] for row in selected], ["claim_a1", "claim_b1"])


if __name__ == "__main__":
    unittest.main()
