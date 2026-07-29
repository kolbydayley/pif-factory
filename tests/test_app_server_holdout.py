from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_expanded_cap_episode_batch import (
    WINNER_SYSTEM_ID,
    build_frozen_configuration,
)
from research_factory.app_server_holdout import (
    FROZEN_WINNER_VERSION,
    HOLDOUT_COVENANT_VERSION,
    load_frozen_winner,
    prepare_frozen_holdout_reservoirs,
    prospective_epoch_watermarks,
    reconstruct_prior_exclusions,
    verify_frozen_holdout,
)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AppServerHoldoutTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.env = patch.dict(os.environ, {"RESEARCH_FACTORY_ROOT": str(self.root)})
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE sources (id TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE episodes (
              id TEXT PRIMARY KEY, source_id TEXT, published_at TEXT
            );
            CREATE TABLE transcripts (
              id TEXT PRIMARY KEY, episode_id TEXT, source_kind TEXT,
              raw_text_path TEXT, raw_text_sha256 TEXT, status TEXT,
              fetched_at TEXT, created_at TEXT, updated_at TEXT
            );
            CREATE TABLE transcript_preparations (
              id TEXT PRIMARY KEY, transcript_id TEXT, status TEXT,
              cleaned_text_path TEXT, cleaned_text_sha256 TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, transcript_id TEXT, episode_id TEXT,
              source_id TEXT, segment_index INTEGER, text_path TEXT,
              text_sha256 TEXT
            );
            CREATE TABLE labels (id TEXT PRIMARY KEY, segment_id TEXT);
            CREATE TABLE label_runs (id TEXT PRIMARY KEY, segment_id TEXT);
            CREATE TABLE episode_context_runs (id TEXT PRIMARY KEY, episode_id TEXT);
            """
        )
        self.winner_path = self.root / "frozen-winner.json"
        self._write_winner()
        self.exclude_dir = self.root / "prior"
        self.exclude_dir.mkdir()

    def tearDown(self) -> None:
        self.conn.close()
        self.env.stop()
        self.tempdir.cleanup()

    def _write_winner(self, **overrides) -> None:
        config = build_frozen_configuration(
            batch_size=5, thread_mode="same_thread"
        )
        config_sha256 = sha(
            json.dumps(
                config,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        report_sha256 = "2" * 64
        payload = {
            "schema_version": FROZEN_WINNER_VERSION,
            "selection_status": "frozen_winner",
            "winner_frozen": True,
            "winner": {
                "variant_id": "batch_5_same_thread",
                "winner_system_id": WINNER_SYSTEM_ID,
                "batch_size": 5,
                "thread_mode": "same_thread",
                "model": config["model"],
                "reasoning_effort": "high",
                "concurrency": 1,
                "retry_count": 0,
                "window_count": 4,
                "context_chars": 900,
                "max_events_per_segment": config["max_events_per_segment"],
                "frozen_configuration": config,
                "frozen_configuration_sha256": config_sha256,
                "report_sha256": report_sha256,
            },
            "gates": {
                "quality_noninferior": True,
                "production_amortized_total_token_ratio_lte_0_28": True,
            },
            "frozen_artifact_hashes": {
                "matrix": "3" * 64,
                "arm_configuration_batch_5_same_thread": config_sha256,
                "arm_report_batch_5_same_thread": report_sha256,
            },
            "holdout_preparation_authorized": True,
            "holdout_model_calls_authorized": False,
            "production_changed": False,
            "production_mutated": False,
        }
        payload.update(overrides)
        self.winner_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _file(self, relative: str, text: str) -> tuple[str, str]:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return str(path.relative_to(self.root)), sha(text)

    def seed_episode(
        self,
        episode_id: str,
        source_id: str,
        *,
        published_day: int,
        acquired_hour: int,
        segment_count: int = 3,
    ) -> list[str]:
        self.conn.execute(
            "INSERT OR IGNORE INTO sources VALUES (?, ?)",
            (source_id, f"Source {source_id}"),
        )
        self.conn.execute(
            "INSERT INTO episodes VALUES (?, ?, ?)",
            (episode_id, source_id, f"2026-07-{published_day:02d}T00:00:00+00:00"),
        )
        transcript_id = f"tr_{episode_id}"
        raw_path, raw_hash = self._file(
            f"corpus/transcripts/{transcript_id}.txt", f"raw {episode_id}"
        )
        self.conn.execute(
            "INSERT INTO transcripts VALUES (?, ?, 'creator', ?, ?, 'ready', ?, ?, ?)",
            (
                transcript_id,
                episode_id,
                raw_path,
                raw_hash,
                f"2026-07-12T{acquired_hour:02d}:00:00+00:00",
                "2026-07-01T00:00:00+00:00",
                "2026-07-12T00:00:00+00:00",
            ),
        )
        prepared_path, prepared_hash = self._file(
            f"corpus/prepared/{transcript_id}.txt", f"prepared {episode_id}"
        )
        self.conn.execute(
            "INSERT INTO transcript_preparations VALUES (?, ?, 'prepared', ?, ?)",
            (f"prep_{episode_id}", transcript_id, prepared_path, prepared_hash),
        )
        segment_ids = []
        for index in range(segment_count):
            segment_id = f"seg_{episode_id}_{index}"
            text_path, text_hash = self._file(
                f"corpus/segments/{segment_id}.txt", f"{episode_id} exact segment {index}"
            )
            self.conn.execute(
                "INSERT INTO segments VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    segment_id,
                    transcript_id,
                    episode_id,
                    source_id,
                    index,
                    text_path,
                    text_hash,
                ),
            )
            segment_ids.append(segment_id)
        self.conn.commit()
        return segment_ids

    def seed_exposures_and_candidates(self) -> dict[str, list[str]]:
        seeded = {}
        for offset, kind in enumerate(("context", "label", "run", "manifest"), start=1):
            seeded[kind] = self.seed_episode(
                f"ep_{kind}", f"src_exposed_{kind}", published_day=offset, acquired_hour=offset
            )
        self.conn.execute(
            "INSERT INTO episode_context_runs VALUES ('ctx_1', 'ep_context')"
        )
        self.conn.execute("INSERT INTO labels VALUES ('label_1', ?)", (seeded["label"][0],))
        self.conn.execute("INSERT INTO label_runs VALUES ('run_1', ?)", (seeded["run"][0],))
        prior_manifest = {
            "schema_version": "old_manifest",
            "chunks": [{"segment_ids": [seeded["manifest"][0]]}],
        }
        (self.exclude_dir / "old-manifest.json").write_text(
            json.dumps(prior_manifest) + "\n", encoding="utf-8"
        )
        for source_index in range(3):
            for episode_index in range(2):
                name = f"eligible_{source_index}_{episode_index}"
                seeded[name] = self.seed_episode(
                    f"ep_{name}",
                    f"src_{source_index}",
                    published_day=5 + source_index * 2 + episode_index,
                    acquired_hour=5 + source_index * 2 + episode_index,
                )
        self.conn.commit()
        return seeded

    def test_requires_explicit_frozen_winner_and_both_gates(self) -> None:
        self.seed_exposures_and_candidates()
        self._write_winner(winner_frozen=False)
        with self.assertRaisesRegex(ValueError, "winner_frozen"):
            prepare_frozen_holdout_reservoirs(
                self.conn,
                frozen_winner_path=self.winner_path,
                output_dir=self.root / "holdout",
                exclude_roots=[self.exclude_dir],
                paired_quality_count=2,
                terminal_position_count=2,
            )
        self._write_winner(
            gates={
                "quality_noninferior": False,
                "production_amortized_total_token_ratio_lte_0_28": True,
            }
        )
        with self.assertRaisesRegex(ValueError, "quality gate"):
            prepare_frozen_holdout_reservoirs(
                self.conn,
                frozen_winner_path=self.winner_path,
                output_dir=self.root / "holdout",
                exclude_roots=[self.exclude_dir],
                paired_quality_count=2,
                terminal_position_count=2,
            )

    def test_frozen_winner_v3_rejects_old_schema_and_configuration_drift(self) -> None:
        payload = json.loads(self.winner_path.read_text(encoding="utf-8"))
        payload["schema_version"] = "pif_app_server_frozen_winner_v1"
        self.winner_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "frozen winner schema"):
            load_frozen_winner(self.winner_path)

        self._write_winner()
        payload = json.loads(self.winner_path.read_text(encoding="utf-8"))
        payload["winner"]["frozen_configuration"]["batch_size"] = 8
        self.winner_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "frozen_configuration_sha256"):
            load_frozen_winner(self.winner_path)

        self.winner_path.write_text("[]\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "must contain a JSON object"):
            load_frozen_winner(self.winner_path)

    def test_reconstructs_all_db_exposures_and_exact_manifest_text(self) -> None:
        seeded = self.seed_exposures_and_candidates()
        exclusions = reconstruct_prior_exclusions(
            self.conn, exclude_roots=[self.exclude_dir]
        )
        self.assertEqual(
            set(exclusions["excluded_episode_ids"]),
            {"ep_context", "ep_label", "ep_run", "ep_manifest"},
        )
        for kind in ("context", "label", "run", "manifest"):
            for segment_id in seeded[kind]:
                self.assertIn(segment_id, exclusions["excluded_segment_ids"])
                row = self.conn.execute(
                    "SELECT text_sha256 FROM segments WHERE id = ?", (segment_id,)
                ).fetchone()
                self.assertIn(row["text_sha256"], exclusions["excluded_text_sha256s"])
        self.assertIn("ep_context", exclusions["exposure_sources"]["episode_context_episode_ids"])
        self.assertIn("ep_label", exclusions["exposure_sources"]["label_episode_ids"])
        self.assertIn("ep_run", exclusions["exposure_sources"]["label_run_episode_ids"])

        drift_segment = seeded["manifest"][1]
        path = self.conn.execute(
            "SELECT text_path FROM segments WHERE id = ?", (drift_segment,)
        ).fetchone()[0]
        (self.root / path).write_text("changed after DB hash", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "DB/file hash drift"):
            reconstruct_prior_exclusions(self.conn, exclude_roots=[self.exclude_dir])

    def test_reconstructs_top_level_array_manifests_and_rejects_scalars(self) -> None:
        self.seed_exposures_and_candidates()
        array_segments = self.seed_episode(
            "ep_array_manifest",
            "src_array_manifest",
            published_day=11,
            acquired_hour=11,
        )
        explicit_segment = array_segments[0]
        explicit_hash = self.conn.execute(
            "SELECT text_sha256 FROM segments WHERE id = ?", (explicit_segment,)
        ).fetchone()[0]
        (self.exclude_dir / "array-manifest.json").write_text(
            json.dumps(
                [
                    {"metadata": {"schema_version": "array_manifest"}},
                    {
                        "nested": [
                            {
                                "segment_ids": [explicit_segment],
                                "text_sha256s": [explicit_hash],
                            }
                        ]
                    },
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        exclusions = reconstruct_prior_exclusions(
            self.conn, exclude_roots=[self.exclude_dir]
        )
        self.assertIn("ep_array_manifest", exclusions["excluded_episode_ids"])
        self.assertTrue(
            set(array_segments).issubset(exclusions["excluded_segment_ids"])
        )
        self.assertIn(explicit_hash, exclusions["excluded_text_sha256s"])

        (self.exclude_dir / "scalar-manifest.json").write_text(
            "42\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "JSON object or array"):
            reconstruct_prior_exclusions(self.conn, exclude_roots=[self.exclude_dir])

    def test_deterministic_immutable_balanced_reservoirs_and_verification(self) -> None:
        self.seed_exposures_and_candidates()
        first_dir = self.root / "holdout-a"
        second_dir = self.root / "holdout-b"
        kwargs = {
            "frozen_winner_path": self.winner_path,
            "exclude_roots": [self.exclude_dir],
            "paired_quality_count": 4,
            "terminal_position_count": 3,
            "paired_minimum_sources": 3,
            "terminal_minimum_sources": 3,
            "paired_max_per_source": 2,
            "paired_max_per_episode": 1,
            "terminal_max_per_source": 1,
            "terminal_max_per_episode": 1,
            "seed": "fixed-fixture-seed",
        }
        first = prepare_frozen_holdout_reservoirs(
            self.conn, output_dir=first_dir, **kwargs
        )
        second = prepare_frozen_holdout_reservoirs(
            self.conn, output_dir=second_dir, **kwargs
        )
        self.assertEqual(first["model_calls_performed"], 0)
        self.assertTrue(first["ok"])
        for name in (
            "covenant.json",
            "exclusions.json",
            "paired-quality-reservoir.json",
            "terminal-position-no-signal-candidates.json",
        ):
            self.assertEqual((first_dir / name).read_bytes(), (second_dir / name).read_bytes())

        covenant = json.loads((first_dir / "covenant.json").read_text(encoding="utf-8"))
        self.assertEqual(covenant["schema_version"], HOLDOUT_COVENANT_VERSION)
        self.assertEqual(covenant["model_calls_performed_during_preparation"], 0)
        paired = json.loads(
            (first_dir / "paired-quality-reservoir.json").read_text(encoding="utf-8")
        )
        terminal = json.loads(
            (first_dir / "terminal-position-no-signal-candidates.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(paired["selected_count"], 4)
        self.assertTrue(all(count <= 2 for count in paired["source_counts"].values()))
        self.assertTrue(all(count <= 1 for count in paired["episode_counts"].values()))
        self.assertEqual(terminal["selected_count"], 3)
        self.assertEqual(set(terminal["source_counts"].values()), {1})
        self.assertIn("not a semantic no-signal label", terminal["candidate_semantics"])
        for segment in terminal["segments"]:
            maximum = self.conn.execute(
                "SELECT MAX(segment_index) FROM segments WHERE transcript_id = ?",
                (segment["transcript_id"],),
            ).fetchone()[0]
            self.assertEqual(segment["segment_index"], maximum)
        self.assertFalse(
            {row["segment_id"] for row in paired["segments"]}
            & {row["segment_id"] for row in terminal["segments"]}
        )
        self.assertTrue(
            verify_frozen_holdout(
                self.conn, covenant_path=first_dir / "covenant.json"
            )["ok"]
        )
        with self.assertRaisesRegex(ValueError, "refusing overwrite"):
            prepare_frozen_holdout_reservoirs(
                self.conn, output_dir=first_dir, **kwargs
            )

        selected = paired["segments"][0]
        selected_path = self.conn.execute(
            "SELECT text_path FROM segments WHERE id = ?", (selected["segment_id"],)
        ).fetchone()[0]
        (self.root / selected_path).write_text("post-freeze mutation", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "DB/file hash drift"):
            verify_frozen_holdout(self.conn, covenant_path=first_dir / "covenant.json")

    def test_undercapacity_freezes_watermarks_and_does_not_weaken_requirements(self) -> None:
        self.seed_exposures_and_candidates()
        output_dir = self.root / "blocked-holdout"
        result = prepare_frozen_holdout_reservoirs(
            self.conn,
            frozen_winner_path=self.winner_path,
            output_dir=output_dir,
            exclude_roots=[self.exclude_dir],
            paired_quality_count=240,
            terminal_position_count=240,
            paired_minimum_sources=20,
            terminal_minimum_sources=20,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(
            result["selection_status"], "blocked_future_only_reservoir_undercapacity"
        )
        covenant = json.loads((output_dir / "covenant.json").read_text(encoding="utf-8"))
        self.assertFalse(covenant["holdout_model_calls_authorized"])
        self.assertIn("do not weaken", covenant["undercapacity_policy"])
        self.assertIn("strictly after", covenant["undercapacity_policy"])

    def test_freezes_independent_acquisition_and_publication_watermarks(self) -> None:
        self.seed_episode(
            "ep_older_publish_new_acquire",
            "src_a",
            published_day=2,
            acquired_hour=20,
        )
        self.seed_episode(
            "ep_newer_publish_old_acquire",
            "src_b",
            published_day=11,
            acquired_hour=2,
        )
        watermarks = prospective_epoch_watermarks(self.conn)
        self.assertEqual(
            watermarks["acquisition"]["timestamp"], "2026-07-12T20:00:00+00:00"
        )
        self.assertEqual(
            watermarks["publication"]["timestamp"], "2026-07-11T00:00:00+00:00"
        )
        self.assertIn("strictly after", watermarks["prospective_eligibility"])


if __name__ == "__main__":
    unittest.main()
