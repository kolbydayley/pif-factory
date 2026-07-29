"""Tests for the relational-merge certification verifier.

The pure verifier is exercised with in-memory inputs only.  The loader is
exercised against a synthetic sqlite fixture so no stored campaign artifact,
gold answer key, or sealed holdout material is required to run this suite.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory import true_north_relational_merge as trm


def _atomic(candidate_id: str, *atomic_ids: str, ledger_category: str = "retained_supported_singleton"):
    return {
        "candidate_id": candidate_id,
        "atomic_claim_ids": list(atomic_ids),
        "ledger_category": ledger_category,
    }


def _group(group_id: str, *atomic_ids: str, identifying: bool = True):
    return {
        "canonical_group_id": group_id,
        "atomic_claim_ids": list(atomic_ids),
        "identifying": identifying,
    }


class RelationalReasonCodeTest(unittest.TestCase):
    def test_relational_reason_codes_cover_repetition_family_and_question_frame(self) -> None:
        self.assertTrue(trm.is_relational_junk_reason("non_useful_repetition"))
        self.assertTrue(
            trm.is_relational_junk_reason("non_useful_repetition_of_model_definition")
        )
        self.assertTrue(trm.is_relational_junk_reason("nonasserted_question_frame"))

    def test_intrinsic_reason_codes_are_not_relational(self) -> None:
        self.assertFalse(
            trm.is_relational_junk_reason("truncated_fragment_with_unresolved_watching_object")
        )
        self.assertFalse(trm.is_relational_junk_reason("bare_policy_artifact_mention"))
        self.assertFalse(trm.is_relational_junk_reason("metadata"))
        self.assertFalse(trm.is_relational_junk_reason(""))


class VerifyRelationalMergesTest(unittest.TestCase):
    def test_merged_escape_reports_zero_contamination(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk", "ac_1"), _atomic("cand_dup", "ac_2")],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2")],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"}],
        )
        self.assertTrue(report.contamination_zero)
        self.assertEqual(report.relational_escapes, 1)
        self.assertEqual(report.merged_count, 1)
        self.assertEqual(report.unmerged, ())
        entry = report.escapes[0]
        self.assertTrue(entry.merged)
        self.assertEqual(entry.candidate_id, "cand_junk")
        self.assertEqual(entry.canonical_group_id, "subject_a::prop_a")
        self.assertEqual(entry.duplicate_of, "cand_dup")

    def test_unmerged_singleton_group_escape_fails_and_is_named(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk", "ac_1"), _atomic("cand_other", "ac_2")],
            canonical_groups=[
                _group("subject_a::prop_a", "ac_1"),
                _group("subject_b::prop_b", "ac_2"),
            ],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "nonasserted_question_frame"}],
        )
        self.assertFalse(report.contamination_zero)
        self.assertEqual(report.merged_count, 0)
        self.assertEqual(report.unmerged, ("cand_junk",))
        entry = report.escapes[0]
        self.assertFalse(entry.merged)
        self.assertIsNone(entry.duplicate_of)
        self.assertEqual(entry.reason, "singleton_canonical_group")

    def test_escape_absent_from_every_canonical_group_is_unmerged(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk", "ac_1"), _atomic("cand_dup", "ac_2")],
            canonical_groups=[_group("subject_a::prop_a", "ac_2")],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"}],
        )
        self.assertFalse(report.contamination_zero)
        entry = report.escapes[0]
        self.assertFalse(entry.merged)
        self.assertIsNone(entry.canonical_group_id)
        self.assertEqual(entry.reason, "no_canonical_group")

    def test_escape_without_atomic_claims_is_unmerged_with_explicit_reason(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk", ledger_category="held_needs_review")],
            canonical_groups=[],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "nonasserted_question_frame"}],
        )
        self.assertFalse(report.contamination_zero)
        entry = report.escapes[0]
        self.assertFalse(entry.merged)
        self.assertEqual(entry.reason, "no_atomic_claims")
        self.assertEqual(entry.ledger_category, "held_needs_review")

    def test_non_identifying_group_never_counts_as_a_merge(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk", "ac_1"), _atomic("cand_dup", "ac_2")],
            canonical_groups=[
                _group("subject_a::unmapped", "ac_1", "ac_2", identifying=False)
            ],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"}],
        )
        self.assertFalse(report.contamination_zero)
        entry = report.escapes[0]
        self.assertFalse(entry.merged)
        self.assertEqual(entry.reason, "non_identifying_canonical_group")
        self.assertEqual(report.non_identifying_groups, ("subject_a::unmapped",))

    def test_declared_duplicate_of_must_share_the_canonical_group(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[
                _atomic("cand_junk", "ac_1"),
                _atomic("cand_dup", "ac_2"),
                _atomic("cand_unrelated", "ac_3"),
            ],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_3")],
            escapes=[
                {
                    "candidate_id": "cand_junk",
                    "junk_reason": "non_useful_repetition",
                    "duplicate_of": "cand_dup",
                }
            ],
        )
        self.assertFalse(report.contamination_zero)
        entry = report.escapes[0]
        self.assertFalse(entry.merged)
        self.assertEqual(entry.reason, "declared_duplicate_not_in_canonical_group")
        self.assertEqual(entry.duplicate_candidate_ids, ("cand_unrelated",))

    def test_two_co_grouped_escapes_cannot_certify_each_other(self) -> None:
        """A duplicate pair of repetition junk must fail closed.

        Two near-identical junk claims are exactly what canonicalization would
        co-group.  If each counted as the other's duplicate, ``unmerged`` would
        be empty and the verifier would report zero contamination while both
        junk candidates sit in the corpus.
        """

        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_junk_a", "ac_1"), _atomic("cand_junk_b", "ac_2")],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2")],
            escapes=[
                {"candidate_id": "cand_junk_a", "junk_reason": "non_useful_repetition"},
                {"candidate_id": "cand_junk_b", "junk_reason": "non_useful_repetition"},
            ],
        )
        self.assertFalse(report.contamination_zero)
        self.assertEqual(report.merged_count, 0)
        self.assertEqual(report.unmerged, ("cand_junk_a", "cand_junk_b"))
        for entry in report.escapes:
            self.assertFalse(entry.merged)
            self.assertIsNone(entry.duplicate_of)
            self.assertEqual(entry.duplicate_candidate_ids, ())
            self.assertEqual(entry.reason, "only_junk_peers_in_canonical_group")
            self.assertEqual(entry.canonical_group_id, "subject_a::prop_a")
        by_id = {entry.candidate_id: entry for entry in report.escapes}
        self.assertEqual(
            by_id["cand_junk_a"].excluded_peer_candidate_ids, ("cand_junk_b",)
        )
        self.assertEqual(
            by_id["cand_junk_b"].excluded_peer_candidate_ids, ("cand_junk_a",)
        )

    def test_a_retained_peer_still_certifies_a_group_holding_another_escape(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[
                _atomic("cand_junk_a", "ac_1"),
                _atomic("cand_junk_b", "ac_2"),
                _atomic("cand_retained", "ac_3"),
            ],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2", "ac_3")],
            escapes=[
                {"candidate_id": "cand_junk_a", "junk_reason": "non_useful_repetition"},
                {"candidate_id": "cand_junk_b", "junk_reason": "non_useful_repetition"},
            ],
        )
        self.assertTrue(report.contamination_zero)
        self.assertEqual(report.merged_count, 2)
        for entry in report.escapes:
            self.assertEqual(entry.duplicate_of, "cand_retained")
            self.assertEqual(entry.duplicate_candidate_ids, ("cand_retained",))
        by_id = {entry.candidate_id: entry for entry in report.escapes}
        self.assertEqual(
            by_id["cand_junk_a"].excluded_peer_candidate_ids, ("cand_junk_b",)
        )

    def test_peer_that_never_entered_the_corpus_cannot_certify_a_merge(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[
                _atomic("cand_junk", "ac_1"),
                _atomic("cand_held", "ac_2", ledger_category="held_needs_review"),
            ],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2")],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"}],
        )
        self.assertFalse(report.contamination_zero)
        entry = report.escapes[0]
        self.assertEqual(entry.reason, "only_junk_peers_in_canonical_group")
        self.assertEqual(entry.excluded_peer_candidate_ids, ("cand_held",))

    def test_declared_duplicate_that_is_itself_an_escape_fails_closed(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[
                _atomic("cand_junk_a", "ac_1"),
                _atomic("cand_junk_b", "ac_2"),
            ],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2")],
            escapes=[
                {
                    "candidate_id": "cand_junk_a",
                    "junk_reason": "non_useful_repetition",
                    "duplicate_of": "cand_junk_b",
                },
                {"candidate_id": "cand_junk_b", "junk_reason": "non_useful_repetition"},
            ],
        )
        self.assertFalse(report.contamination_zero)
        by_id = {entry.candidate_id: entry for entry in report.escapes}
        self.assertFalse(by_id["cand_junk_a"].merged)
        self.assertEqual(
            by_id["cand_junk_a"].reason, "only_junk_peers_in_canonical_group"
        )

    def test_non_relational_reason_code_is_rejected(self) -> None:
        with self.assertRaises(trm.RelationalMergeError):
            trm.verify_relational_merges(
                atomics=[_atomic("cand_junk", "ac_1")],
                canonical_groups=[],
                escapes=[
                    {
                        "candidate_id": "cand_junk",
                        "junk_reason": "bare_policy_artifact_mention",
                    }
                ],
            )

    def test_missing_junk_reason_is_rejected(self) -> None:
        with self.assertRaises(trm.RelationalMergeError):
            trm.verify_relational_merges(
                atomics=[_atomic("cand_junk", "ac_1")],
                canonical_groups=[],
                escapes=[{"candidate_id": "cand_junk"}],
            )

    def test_duplicate_escape_candidate_ids_are_rejected(self) -> None:
        with self.assertRaises(trm.RelationalMergeError):
            trm.verify_relational_merges(
                atomics=[_atomic("cand_junk", "ac_1")],
                canonical_groups=[],
                escapes=[
                    {"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"},
                    {"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"},
                ],
            )

    def test_atomic_claim_owned_by_two_candidates_is_rejected(self) -> None:
        with self.assertRaises(trm.RelationalMergeError):
            trm.verify_relational_merges(
                atomics=[_atomic("cand_a", "ac_1"), _atomic("cand_b", "ac_1")],
                canonical_groups=[],
                escapes=[],
            )

    def test_empty_escape_list_is_vacuously_zero_contamination(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[_atomic("cand_a", "ac_1")],
            canonical_groups=[_group("subject_a::prop_a", "ac_1")],
            escapes=[],
        )
        self.assertTrue(report.contamination_zero)
        self.assertEqual(report.relational_escapes, 0)
        self.assertEqual(report.merged_count, 0)
        self.assertEqual(report.unmerged, ())
        self.assertEqual(report.escapes, ())

    def test_report_is_deterministic_and_serialisable(self) -> None:
        atomics = [
            _atomic("cand_z", "ac_9"),
            _atomic("cand_a", "ac_1"),
            _atomic("cand_dup", "ac_2"),
        ]
        groups = [_group("subject_a::prop_a", "ac_2", "ac_1")]
        escapes = [
            {"candidate_id": "cand_z", "junk_reason": "non_useful_repetition"},
            {"candidate_id": "cand_a", "junk_reason": "non_useful_repetition_of_model_definition"},
        ]
        first = trm.verify_relational_merges(atomics, groups, escapes)
        second = trm.verify_relational_merges(
            list(reversed(atomics)), groups, list(reversed(escapes))
        )
        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(
            [entry.candidate_id for entry in first.escapes], ["cand_a", "cand_z"]
        )
        self.assertEqual(first.unmerged, ("cand_z",))
        json.dumps(first.to_dict())

    def test_multiple_duplicates_report_all_peers_deterministically(self) -> None:
        report = trm.verify_relational_merges(
            atomics=[
                _atomic("cand_junk", "ac_1"),
                _atomic("cand_y", "ac_2"),
                _atomic("cand_b", "ac_3"),
            ],
            canonical_groups=[_group("subject_a::prop_a", "ac_1", "ac_2", "ac_3")],
            escapes=[{"candidate_id": "cand_junk", "junk_reason": "non_useful_repetition"}],
        )
        entry = report.escapes[0]
        self.assertTrue(entry.merged)
        self.assertEqual(entry.duplicate_of, "cand_b")
        self.assertEqual(entry.duplicate_candidate_ids, ("cand_b", "cand_y"))


_LOADER_SCHEMA = """
CREATE TABLE true_north_stage_ledger (
  run_id TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  episode_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  category TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  atomic_claim_ids_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE current_accepted_position_observations (
  id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  atomic_claim_id TEXT NOT NULL
);
CREATE TABLE true_north_variant_canonical_map (
  run_id TEXT NOT NULL,
  variant_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  canonical_subject_key TEXT NOT NULL,
  canonical_proposition_key TEXT NOT NULL
);
"""


class LoaderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.suite_root = self.root / trm.SUITE_ID
        self.run_id = "tnrun_fixture"
        self.run_root = self.suite_root / "runs" / self.run_id
        self.run_root.mkdir(parents=True, exist_ok=True)
        self._write_gold()
        self._write_db()
        self._write_canonical_map()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_gold(self) -> None:
        gold_path = self.suite_root / "gold" / "development" / "final" / "gold.private.json"
        gold_path.parent.mkdir(parents=True, exist_ok=True)
        gold_path.write_text(
            json.dumps(
                {
                    "partition": "development",
                    "suite_id": trm.SUITE_ID,
                    "items": [
                        {
                            "candidate_id": "cand_rel_merged",
                            "disposition": "reject",
                            "reason_code": "non_useful_repetition",
                        },
                        {
                            "candidate_id": "cand_rel_unmerged",
                            "disposition": "reject",
                            "reason_code": "nonasserted_question_frame",
                        },
                        {
                            "candidate_id": "cand_rel_not_escaped",
                            "disposition": "reject",
                            "reason_code": "non_useful_repetition",
                        },
                        {
                            "candidate_id": "cand_intrinsic",
                            "disposition": "reject",
                            "reason_code": "bare_policy_artifact_mention",
                        },
                        {
                            "candidate_id": "cand_dup",
                            "disposition": "retain",
                            "reason_code": "valid",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

    def _write_db(self) -> None:
        conn = sqlite3.connect(self.run_root / "shadow.sqlite")
        conn.executescript(_LOADER_SCHEMA)
        ledger = [
            ("cand_rel_merged", "retained_canonical_member", ["ac_1"]),
            ("cand_rel_unmerged", "retained_supported_singleton", ["ac_2"]),
            ("cand_rel_not_escaped", "rejected_junk", []),
            ("cand_intrinsic", "retained_supported_singleton", ["ac_3"]),
            ("cand_dup", "retained_canonical_member", ["ac_4"]),
        ]
        conn.executemany(
            "INSERT INTO true_north_stage_ledger"
            " (run_id, candidate_id, episode_id, stage, category, reason_code,"
            "  atomic_claim_ids_json) VALUES (?, ?, 'ep_1', 'atomic', ?, 'x', ?)",
            [
                (self.run_id, candidate_id, category, json.dumps(ids))
                for candidate_id, category, ids in ledger
            ],
        )
        conn.executemany(
            "INSERT INTO current_accepted_position_observations"
            " (id, variant_id, atomic_claim_id) VALUES (?, ?, ?)",
            [
                ("pos_1", "var_1", "ac_1"),
                ("pos_2", "var_2", "ac_2"),
                ("pos_3", "var_3", "ac_3"),
                ("pos_4", "var_4", "ac_4"),
            ],
        )
        conn.executemany(
            "INSERT INTO true_north_variant_canonical_map"
            " (run_id, variant_id, subject_id, canonical_subject_key,"
            "  canonical_proposition_key) VALUES (?, ?, ?, ?, ?)",
            [
                (self.run_id, "var_1", "sub_1", "subject_a", "prop_a"),
                (self.run_id, "var_4", "sub_1", "subject_a", "prop_a"),
                (self.run_id, "var_2", "sub_2", "subject_b", "unmapped"),
                (self.run_id, "var_3", "sub_3", "subject_c", "prop_c"),
            ],
        )
        conn.commit()
        conn.close()

    def _write_canonical_map(self) -> None:
        path = self.run_root / "canonical-map" / "final.private.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": "pif_true_north_canonical_map_v1",
                    "run_id": self.run_id,
                    "subjects": [
                        {
                            "canonical_subject_key": "subject_a",
                            "subject_id": "sub_1",
                        },
                        {
                            "canonical_subject_key": "subject_b",
                            "subject_id": "sub_2",
                        },
                        {
                            "canonical_subject_key": "subject_c",
                            "subject_id": "sub_3",
                        },
                    ],
                    "variants": [],
                }
            ),
            encoding="utf-8",
        )

    def test_loader_selects_only_relational_escapes(self) -> None:
        inputs = trm.load_relational_merge_inputs(
            run_id=self.run_id, output_root=self.root
        )
        self.assertEqual(
            sorted(escape["candidate_id"] for escape in inputs.escapes),
            ["cand_rel_merged", "cand_rel_unmerged"],
        )
        self.assertEqual(
            inputs.diagnostics["intrinsic_junk_escapes"], ("cand_intrinsic",)
        )

    def test_loader_resolves_subject_key_through_final_canonical_map(self) -> None:
        inputs = trm.load_relational_merge_inputs(
            run_id=self.run_id, output_root=self.root
        )
        by_id = {group["canonical_group_id"]: group for group in inputs.canonical_groups}
        self.assertTrue(by_id["sub_2"]["identifying"])
        self.assertEqual(
            by_id["sub_2"]["canonical_subject_key"], "subject_b"
        )
        self.assertTrue(by_id["sub_1"]["identifying"])

    def test_verify_run_reports_the_measured_merge_state(self) -> None:
        report, inputs = trm.verify_run(run_id=self.run_id, output_root=self.root)
        self.assertEqual(report.relational_escapes, 2)
        self.assertEqual(report.merged_count, 1)
        self.assertEqual(report.unmerged, ("cand_rel_unmerged",))
        self.assertFalse(report.contamination_zero)
        merged = {entry.candidate_id: entry for entry in report.escapes}
        self.assertEqual(merged["cand_rel_merged"].duplicate_of, "cand_dup")
        self.assertEqual(
            merged["cand_rel_unmerged"].reason, "singleton_canonical_group"
        )
        self.assertEqual(inputs.run_id, self.run_id)

    def test_loader_raises_when_canonical_map_is_missing(self) -> None:
        (
            self.run_root / "canonical-map" / "final.private.json"
        ).unlink()
        with self.assertRaisesRegex(
            trm.RelationalMergeError, "missing canonical-map artifact"
        ):
            trm.load_relational_merge_inputs(
                run_id=self.run_id, output_root=self.root
            )

    def test_loader_raises_when_canonical_map_run_id_mismatches(self) -> None:
        path = self.run_root / "canonical-map" / "final.private.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["run_id"] = "tnrun_other"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(
            trm.RelationalMergeError, "run_id does not match"
        ):
            trm.load_relational_merge_inputs(
                run_id=self.run_id, output_root=self.root
            )

    def test_declared_escape_ids_override_the_ledger_derived_escape_set(self) -> None:
        inputs = trm.load_relational_merge_inputs(
            run_id=self.run_id,
            output_root=self.root,
            escape_candidate_ids=[
                "cand_rel_not_escaped",
                "cand_intrinsic",
                "cand_never_seen",
            ],
        )
        self.assertEqual(
            [escape["candidate_id"] for escape in inputs.escapes],
            [],
        )
        self.assertEqual(inputs.diagnostics["escape_source"], "declared_candidate_ids")
        self.assertEqual(
            inputs.diagnostics["declared_escapes_absent_from_gold_rejects"],
            ("cand_never_seen",),
        )
        self.assertEqual(
            inputs.diagnostics["intrinsic_junk_escapes"], ("cand_intrinsic",)
        )
        self.assertEqual(
            inputs.diagnostics["zero_atomic_claim_candidate_ids"],
            ("cand_rel_not_escaped",),
        )

    def test_declared_zero_atomic_candidate_is_not_contamination_regardless_of_label(
        self,
    ) -> None:
        conn = sqlite3.connect(self.run_root / "shadow.sqlite")
        conn.execute(
            """
            UPDATE true_north_stage_ledger
            SET category = 'retained_supported_singleton',
                atomic_claim_ids_json = '[]'
            WHERE candidate_id = 'cand_rel_unmerged'
            """
        )
        conn.commit()
        conn.close()

        inputs = trm.load_relational_merge_inputs(
            run_id=self.run_id,
            output_root=self.root,
            escape_candidate_ids=["cand_rel_unmerged"],
        )

        self.assertEqual(inputs.escapes, ())
        self.assertEqual(
            inputs.diagnostics["zero_atomic_claim_candidate_ids"],
            ("cand_rel_unmerged",),
        )

    def test_declared_held_candidate_with_atomics_is_contamination(
        self,
    ) -> None:
        conn = sqlite3.connect(self.run_root / "shadow.sqlite")
        conn.execute(
            """
            UPDATE true_north_stage_ledger
            SET category = 'held_needs_review'
            WHERE candidate_id = 'cand_rel_unmerged'
            """
        )
        conn.commit()
        conn.close()

        report, inputs = trm.verify_run(
            run_id=self.run_id,
            output_root=self.root,
            escape_candidate_ids=["cand_rel_unmerged"],
        )

        self.assertEqual(
            inputs.diagnostics["held_candidate_ids"],
            ("cand_rel_unmerged",),
        )
        self.assertEqual(
            inputs.diagnostics["zero_atomic_claim_candidate_ids"],
            (),
        )
        self.assertEqual(
            [entry.candidate_id for entry in report.escapes],
            ["cand_rel_unmerged"],
        )
        self.assertEqual(report.relational_escapes, 1)
        self.assertEqual(report.unmerged, ("cand_rel_unmerged",))
        self.assertFalse(report.contamination_zero)

    def test_loader_refuses_the_sealed_holdout_partition(self) -> None:
        with self.assertRaises(trm.RelationalMergeError):
            trm.load_relational_merge_inputs(
                run_id=self.run_id, output_root=self.root, partition="holdout"
            )

    def test_loader_refuses_a_gold_path_inside_the_sealed_holdout(self) -> None:
        sealed = self.suite_root / "gold" / "sealed-holdout" / "final" / "gold.private.json"
        sealed.parent.mkdir(parents=True, exist_ok=True)
        sealed.write_text(json.dumps({"items": []}), encoding="utf-8")
        with self.assertRaises(trm.RelationalMergeError):
            trm.load_relational_merge_inputs(
                run_id=self.run_id, output_root=self.root, gold_path=sealed
            )

    def test_discover_canonicalized_runs_finds_only_mapped_runs(self) -> None:
        empty_run = self.suite_root / "runs" / "tnrun_empty"
        empty_run.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(empty_run / "shadow.sqlite")
        conn.executescript(_LOADER_SCHEMA)
        conn.commit()
        conn.close()
        self.assertEqual(
            trm.discover_canonicalized_runs(output_root=self.root), (self.run_id,)
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
