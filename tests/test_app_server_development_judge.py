from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_development_judge import (
    BASELINE_TRANSFORM_PACKET_VERSION,
    build_baseline_transform_judge_packets,
)
from research_factory.app_server_evaluation import APP_SERVER_DEVELOPMENT_MANIFEST_V2
from research_factory.util import sha256_text


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class BaselineTransformPacketTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name).resolve()
        self.env = patch.dict(os.environ, {"RESEARCH_FACTORY_ROOT": str(self.root)})
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, transcript_id TEXT,
              text_path TEXT, text_sha256 TEXT
            );
            CREATE TABLE labels (
              id TEXT PRIMARY KEY, segment_id TEXT, output_json TEXT
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, segment_id TEXT, output_path TEXT
            );
            """
        )
        self.segment_specs = []
        self.references = []
        self.raw_paths = []
        for index in range(3):
            segment_id = f"seg_{index}"
            text = f"Speaker states synthetic claim {index}."
            text_path = self.root / "corpus" / "segments" / f"{segment_id}.txt"
            text_path.parent.mkdir(parents=True, exist_ok=True)
            text_path.write_text(text, encoding="utf-8")
            if index == 0:
                raw_events = []
                submitted_events = []
                density = "no_signal"
            elif index == 1:
                raw_events = [{"evidence": text, "claim": "synthetic claim"}]
                submitted_events = list(raw_events)
                density = "dense"
            else:
                raw_events = [{"evidence": text, "claim": "raw wording"}]
                submitted_events = [
                    {"evidence": text, "claim": "submitted wording"},
                    {"evidence": text, "claim": "second submitted event"},
                ]
                density = "dense"
            raw_payload = {"discourse_events": raw_events}
            submitted_payload = {"discourse_events": submitted_events}
            submitted_json = json.dumps(submitted_payload, ensure_ascii=True, sort_keys=True)
            raw_path = self.root / "runs" / f"raw-{index}.json"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(
                json.dumps(raw_payload, ensure_ascii=True, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.raw_paths.append(raw_path)
            label_id = f"label_{index}"
            run_id = f"run_{index}"
            self.conn.execute(
                "INSERT INTO segments VALUES (?, 'ep_1', 'tr_1', ?, ?)",
                (segment_id, str(text_path.relative_to(self.root)), sha256_text(text)),
            )
            self.conn.execute(
                "INSERT INTO labels VALUES (?, ?, ?)",
                (label_id, segment_id, submitted_json),
            )
            self.conn.execute(
                "INSERT INTO label_runs VALUES (?, ?, ?)",
                (run_id, segment_id, str(raw_path)),
            )
            spec = {
                "segment_id": segment_id,
                "segment_index": index,
                "text_sha256": sha256_text(text),
                "label_id": label_id,
                "label_run_id": run_id,
                "golden_output_sha256": sha256_text(submitted_json),
                "label_run_output_sha256": file_sha(raw_path),
                "golden_event_count": len(submitted_events),
                "density_stratum": density,
            }
            self.segment_specs.append(spec)
            self.references.append(
                {
                    "segment_id": segment_id,
                    "golden_output": submitted_payload,
                }
            )
        self.conn.commit()
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                    "episodes": [{"episode_id": "ep_1", "segments": self.segment_specs}],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self.reference_path = self.root / "reference.json"
        self.reference_path.write_text(
            json.dumps(
                {
                    "schema_version": "pif_shared_reference_seed_v1",
                    "references": self.references,
                },
                ensure_ascii=True,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.conn.close()
        self.env.stop()
        self.tempdir.cleanup()

    def build(self, name: str = "packets") -> dict:
        return build_baseline_transform_judge_packets(
            self.conn,
            manifest_path=self.manifest_path,
            reference_seed_path=self.reference_path,
            output_dir=self.root / name,
            max_cases_per_shard=1,
            max_prompt_bytes=200_000,
            max_schema_bytes=100_000,
        )

    def test_builds_bounded_blinded_shards_and_preserves_double_empty_cases(self) -> None:
        report = self.build()

        self.assertEqual(report["schema_version"], BASELINE_TRANSFORM_PACKET_VERSION)
        self.assertEqual(report["selected_segment_count"], 3)
        self.assertEqual(report["judge_case_count"], 1)
        self.assertEqual(report["double_empty_case_count"], 2)
        self.assertEqual(report["raw_event_count"], 2)
        self.assertEqual(report["submitted_event_count"], 3)
        self.assertEqual(report["exact_identity_event_count"], 1)
        self.assertEqual(report["raw_residual_event_count"], 1)
        self.assertEqual(report["submitted_residual_event_count"], 2)
        self.assertEqual(report["shard_count"], 1)
        self.assertEqual(report["model_calls_made"], 0)
        for shard in report["shards"]:
            self.assertLessEqual(shard["prompt_bytes_max_orientation"], 200_000)
            self.assertLessEqual(shard["schema_bytes_max_orientation"], 100_000)
            pool_text = Path(shard["pool_path"]).read_text(encoding="utf-8")
            self.assertNotIn("baseline_raw_artifact", pool_text)
            mapping_text = Path(shard["mapping_path"]).read_text(encoding="utf-8")
            self.assertIn("baseline_raw_artifact", mapping_text)

    def test_refuses_raw_artifact_drift(self) -> None:
        self.raw_paths[1].write_text('{"discourse_events":[]}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "raw baseline output artifact hash drift"):
            self.build()

    def test_preserves_nonexact_baseline_evidence_privately_for_support_judgment(self) -> None:
        payload = {"discourse_events": [{"evidence": "not in source", "claim": "bad"}]}
        submitted_json = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        self.conn.execute(
            "UPDATE labels SET output_json = ? WHERE id = 'label_1'", (submitted_json,)
        )
        self.conn.commit()
        self.segment_specs[1]["golden_output_sha256"] = sha256_text(submitted_json)
        self.references[1]["golden_output"] = payload
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest["episodes"][0]["segments"][1] = self.segment_specs[1]
        self.manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        reference = json.loads(self.reference_path.read_text(encoding="utf-8"))
        reference["references"][1] = self.references[1]
        self.reference_path.write_text(json.dumps(reference, sort_keys=True) + "\n", encoding="utf-8")

        report = self.build("nonexact")
        projected = []
        originals = []
        for shard in report["shards"]:
            pool = json.loads(Path(shard["pool_path"]).read_text(encoding="utf-8"))
            mapping = json.loads(Path(shard["mapping_path"]).read_text(encoding="utf-8"))
            projected.extend(
                witness["event"]
                for case in pool["cases"]
                for side in ("a", "b")
                for witness in case["event_set_%s" % side]
                if witness["event"].get("submitted_evidence") == "not in source"
            )
            originals.extend(
                witness["provenance"].get("original_event")
                for case in mapping["cases"]
                for witness in case["witnesses"]
                if (witness["provenance"].get("original_event") or {}).get("evidence")
                == "not in source"
            )
        self.assertEqual(len(projected), 1)
        self.assertFalse(projected[0]["submitted_evidence_exact"])
        self.assertNotEqual(projected[0]["evidence"], "not in source")
        self.assertEqual(len(originals), 1)

    def test_refuses_overwrite(self) -> None:
        self.build()
        with self.assertRaisesRegex(ValueError, "directory already exists"):
            self.build()


if __name__ == "__main__":
    unittest.main()
