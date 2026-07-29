from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_eval_provenance import (
    build_instruction_provenance,
    build_reconstructed_exclusion_ledger,
    build_reference_transform_noise,
    canonical_json_sha256,
)


class AppServerEvalProvenanceTests(unittest.TestCase):
    def test_instruction_provenance_checks_content_and_report_path_set(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "AGENTS.md"
            source.write_text("private instructions\n", encoding="utf-8")
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            spec = root / "spec.json"
            spec.write_text(
                json.dumps(
                    {
                        "instruction_contract": {
                            "expected_path_set_sha256": "path-set",
                            "sources": [
                                {
                                    "path": str(source),
                                    "content_sha256": source_hash,
                                    "size_bytes": source.stat().st_size,
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            report = root / "report.json"
            report.write_text(
                json.dumps(
                    {
                        "batch_size_ceiling": 3,
                        "thread_mode": "new_thread",
                        "instruction_source_sets": [
                            {"instruction_sources_sha256": "path-set", "thread_count": 1}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            result = build_instruction_provenance(
                run_spec_path=spec,
                arm_report_paths=[report],
                output_path=root / "provenance.json",
            )
            self.assertTrue(result["complete"])
            self.assertNotIn(str(source), json.dumps(result["instruction_sources"]))

    def test_reconstructed_ledger_expands_referenced_episode_and_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            work.mkdir()
            (work / "manifest.json").write_text(
                json.dumps({"episodes": [{"episode_id": "ep1", "segments": [{"segment_id": "s1"}]}]}),
                encoding="utf-8",
            )
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            conn.execute(
                "CREATE TABLE segments (id TEXT, episode_id TEXT, transcript_id TEXT, text_sha256 TEXT, text_path TEXT)"
            )
            conn.executemany(
                "INSERT INTO segments VALUES (?, ?, ?, ?, ?)",
                [
                    ("s1", "ep1", "tr1", "h1", "unused1"),
                    ("s2", "ep1", "tr1", "h2", "unused2"),
                ],
            )
            result = build_reconstructed_exclusion_ledger(
                conn,
                work_root=work,
                output_path=root / "ledger.json",
                verify_files=False,
            )
            self.assertEqual(result["episode_ids"], ["ep1"])
            self.assertEqual(result["segment_ids"], ["s1", "s2"])
            self.assertEqual(result["text_sha256s"], ["h1", "h2"])
            self.assertTrue(result["complete"])

    def test_reference_transform_records_exact_hash_deletion_without_semantic_matching(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            event_a = {"claim_text": "a", "evidence": "a"}
            event_b = {"claim_text": "b", "evidence": "b"}
            raw = {"discourse_events": [event_a, event_b], "segment_id": "s1"}
            submitted = {"discourse_events": [event_a], "segment_id": "s1"}
            raw_path = root / "raw.json"
            raw_path.write_text(json.dumps(raw), encoding="utf-8")
            raw_file_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "episodes": [
                            {
                                "segments": [
                                    {
                                        "segment_id": "s1",
                                        "label_id": "l1",
                                        "label_run_id": "r1",
                                        "label_run_output_sha256": raw_file_hash,
                                    }
                                ]
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            conn.execute("CREATE TABLE labels (id TEXT, segment_id TEXT, output_json TEXT)")
            conn.execute("CREATE TABLE label_runs (id TEXT, output_path TEXT)")
            conn.execute("INSERT INTO labels VALUES ('l1', 's1', ?)", (json.dumps(submitted),))
            conn.execute("INSERT INTO label_runs VALUES ('r1', ?)", (str(raw_path),))
            result = build_reference_transform_noise(
                conn,
                manifest_path=manifest,
                output_path=root / "noise.json",
            )
            self.assertEqual(result["summary"]["changed_segment_count"], 1)
            self.assertEqual(result["summary"]["exact_event_hashes_removed_by_submission"], 1)
            self.assertEqual(
                result["segments"][0]["exact_event_hashes_removed_by_submission"],
                [canonical_json_sha256(event_b)],
            )


if __name__ == "__main__":
    unittest.main()
