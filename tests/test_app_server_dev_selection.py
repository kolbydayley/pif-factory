from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from research_factory.app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    DEV_SELECTION_VERSION,
    EXPECTED_ARMS,
    QUALITY_GATES,
    SelectionBlocked,
    _arm_system,
    _read_only_connection,
    assemble_dev_shared_witness_pool,
    evaluate_semantic_gates,
    merge_judge_consensuses,
    plan_judge_shards,
    production_amortized_cost,
    run_app_server_dev_selection,
    run_full_judge_shards,
    score_dev_shared_reference,
    source_cluster_paired_bootstrap,
)
from research_factory.app_server_evaluation import (
    APP_SERVER_CORE_ARM_VERSION,
    APP_SERVER_DEVELOPMENT_MANIFEST_V2,
    normalize_episode_batch_output,
)
from research_factory.app_server_holdout import FROZEN_WINNER_VERSION, load_frozen_winner
from research_factory.app_server_llm_judge import (
    JUDGE_CONSENSUS_VERSION,
    make_shared_witness_pool,
)
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.efficient_backtest import build_windowed_segment_packet
from research_factory.util import sha256_text


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def file_sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def core_event(text: str, *, evidence: str | None = None) -> dict:
    return {
        "window_id": 0,
        "event_type": "forecast",
        "event_subtype": "synthetic_timeline",
        "claim_type": "prediction",
        "actor_name": "Alice",
        "actor_type": "person",
        "speaker_name": "Alice",
        "speaker_role": "guest",
        "reported_actor_name": "",
        "reported_actor_type": "none",
        "source_context_kind": "substantive_dialogue",
        "target_concept": "synthetic improvement",
        "claim_text": "Alice predicts a synthetic improvement next year.",
        "stance": "supportive",
        "certainty": "high",
        "temporal_horizon": "near_future",
        "causal_mechanism": "",
        "counterclaim": "",
        "metric_value": "",
        "metric_unit": "",
        "metric_comparator": "",
        "metric_direction": "not_applicable",
        "metric_raw_text": "",
        "signal_reason": "A concrete synthetic forecast is present.",
        "evidence": text if evidence is None else evidence,
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Alice"],
        "confidence": 0.95,
    }


def candidate_row(segment_id: str, text: str, *, dense: bool, invalid: bool = False) -> dict:
    if not dense:
        return {
            "segment_id": segment_id,
            "status": "no_signal",
            "segment_source_context": {
                "kind": "show_setup",
                "confidence": 0.9,
                "rationale": "The synthetic case intentionally contains no event.",
            },
            "no_signal_reason": "No durable synthetic event is present.",
            "events": [],
        }
    return {
        "segment_id": segment_id,
        "status": "coded",
        "segment_source_context": {
            "kind": "substantive_dialogue",
            "confidence": 0.95,
            "rationale": "The speaker states a direct forecast.",
        },
        "no_signal_reason": "",
        "events": [core_event(text, evidence="invented evidence" if invalid else None)],
    }


def baseline_payload(segment_id: str, episode_id: str, text: str, *, dense: bool) -> dict:
    events = (
        [
            {
                "event_type": "forecast",
                "claim_text": "Alice says this segment will improve next year.",
                "evidence": text,
            }
        ]
        if dense
        else []
    )
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": segment_id,
        "episode_id": episode_id,
        "extraction_status": "coded" if events else "no_signal",
        "discourse_events": events,
        "no_signal_reason": "" if events else "No event.",
    }


class SyntheticDevelopment:
    def __init__(self, root: Path):
        self.root = root
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, source_id TEXT, segment_index INTEGER,
              text_path TEXT, text_sha256 TEXT
            );
            CREATE TABLE labels (id TEXT PRIMARY KEY, segment_id TEXT, output_json TEXT);
            CREATE TABLE label_runs (id TEXT PRIMARY KEY, segment_id TEXT, output_path TEXT);
            """
        )
        self.manifest_path = root / "manifest.json"
        self.arm_reports: list[Path] = []
        self.manifest = self._make_manifest()
        self._make_arms()

    def close(self) -> None:
        self.conn.close()

    def _make_manifest(self) -> dict:
        episodes = []
        for source_index in range(4):
            episode_id = f"ep_{source_index}"
            source_id = f"source_{source_index}"
            segments = []
            for local_index in range(8):
                global_index = source_index * 8 + local_index
                segment_id = f"seg_{global_index:02d}"
                dense = local_index != 0
                text = f"Alice says synthetic segment {global_index} will improve next year."
                text_path = self.root / f"{segment_id}.txt"
                text_path.write_text(text, encoding="utf-8")
                text_sha = sha256_text(text)
                label_id = f"label_{global_index:02d}"
                run_id = f"run_{global_index:02d}"
                payload = baseline_payload(segment_id, episode_id, text, dense=dense)
                output_json = canonical(payload)
                raw_path = self.root / f"{run_id}.json"
                raw_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
                self.conn.execute(
                    "INSERT INTO segments VALUES (?, ?, ?, ?, ?, ?)",
                    (segment_id, episode_id, source_id, local_index, str(text_path), text_sha),
                )
                self.conn.execute(
                    "INSERT INTO labels VALUES (?, ?, ?)",
                    (label_id, segment_id, output_json),
                )
                self.conn.execute(
                    "INSERT INTO label_runs VALUES (?, ?, ?)",
                    (run_id, segment_id, str(raw_path)),
                )
                segments.append(
                    {
                        "segment_id": segment_id,
                        "segment_index": local_index,
                        "text_sha256": text_sha,
                        "label_id": label_id,
                        "label_run_id": run_id,
                        "golden_output_sha256": sha256_text(output_json),
                        "label_run_output_sha256": file_sha(raw_path),
                        "golden_event_count": 1 if dense else 0,
                        "density_stratum": "dense" if dense else "no_signal",
                        "shared_reference_seed_id": f"ref_{global_index:02d}",
                    }
                )
            episodes.append(
                {
                    "episode_id": episode_id,
                    "source_id": source_id,
                    "source_name": f"Source {source_index}",
                    "segments": segments,
                }
            )
        self.conn.commit()
        manifest = {
            "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
            "segment_count": 32,
            "source_count": 4,
            "episode_count": 4,
            "event_cap": 32,
            "episodes": episodes,
        }
        self.manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        return manifest

    def _make_arms(self) -> None:
        manifest_sha = file_sha(self.manifest_path)
        for batch_size in (3, 5, 8):
            for thread_mode in ("new_thread", "same_thread"):
                variant = f"batch_{batch_size}_{thread_mode}"
                arm_dir = self.root / variant
                arm_dir.mkdir()
                batches = []
                usages = []
                batch_index = 0
                for episode in self.manifest["episodes"]:
                    episode_segments = episode["segments"]
                    for offset in range(0, len(episode_segments), batch_size):
                        selected = episode_segments[offset : offset + batch_size]
                        segment_ids = [item["segment_id"] for item in selected]
                        raw_rows = []
                        prepared = []
                        for item in selected:
                            text = Path(
                                self.conn.execute(
                                    "SELECT text_path FROM segments WHERE id = ?", (item["segment_id"],)
                                ).fetchone()["text_path"]
                            ).read_text(encoding="utf-8")
                            invalid = variant == "batch_3_new_thread" and item["segment_id"] == "seg_01"
                            raw_rows.append(
                                candidate_row(
                                    item["segment_id"],
                                    text,
                                    dense=item["density_stratum"] == "dense",
                                    invalid=invalid,
                                )
                            )
                            windows, boundaries = build_windowed_segment_packet(
                                text, window_count=4, context_chars=900
                            )
                            prepared.append(
                                {
                                    **item,
                                    "segment_text": text,
                                    "windows": windows,
                                    "boundaries": boundaries,
                                }
                            )
                        raw = {"episode_id": episode["episode_id"], "segments": raw_rows}
                        normalized, _ = normalize_episode_batch_output(
                            raw,
                            episode_id=episode["episode_id"],
                            prepared_segments=prepared,
                            max_events_per_segment=32,
                        )
                        batch_id = f"batch_{batch_index:02d}"
                        raw_path = arm_dir / "raw_outputs" / f"{batch_id}.json"
                        normalized_path = arm_dir / "normalized_outputs" / f"{batch_id}.json"
                        sidecar_path = arm_dir / "sidecars" / f"{batch_id}.json"
                        raw_path.parent.mkdir(exist_ok=True)
                        normalized_path.parent.mkdir(exist_ok=True)
                        sidecar_path.parent.mkdir(exist_ok=True)
                        raw_text = canonical(raw)
                        raw_path.write_text(raw_text + "\n", encoding="utf-8")
                        normalized_path.write_text(
                            json.dumps(normalized, sort_keys=True) + "\n", encoding="utf-8"
                        )
                        usage = {
                            "input_tokens": 100,
                            "cached_input_tokens": 0,
                            "output_tokens": 10,
                            "reasoning_output_tokens": 2,
                            "total_tokens": 110,
                        }
                        sidecar = {
                            "state": "completed",
                            "status": "completed",
                            "auth_type": "chatgpt",
                            "transport": "stdio",
                            "client_version": APP_SERVER_CLIENT_VERSION,
                            "model": "gpt-5.6-sol",
                            "effort": "low",
                            "thread_mode": thread_mode,
                            "batch_size": len(selected),
                            "usage_complete": True,
                            "usage": usage,
                            "output_sha256": sha256_text(raw_text),
                        }
                        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
                        usages.append(usage)
                        batches.append(
                            {
                                "batch_id": batch_id,
                                "episode_id": episode["episode_id"],
                                "segment_ids": segment_ids,
                                "raw_output_path": str(raw_path),
                                "normalized_output_path": str(normalized_path),
                                "sidecar_path": str(sidecar_path),
                            }
                        )
                        batch_index += 1
                mapping = {
                    "schema_version": APP_SERVER_CORE_ARM_VERSION,
                    "manifest_path": str(self.manifest_path),
                    "manifest_sha256": manifest_sha,
                    "batch_size": batch_size,
                    "thread_mode": thread_mode,
                    "batches": batches,
                }
                mapping_path = arm_dir / "private-mapping.json"
                mapping_path.write_text(json.dumps(mapping, sort_keys=True) + "\n", encoding="utf-8")
                usage = {
                    field: sum(item[field] for item in usages)
                    for field in (
                        "input_tokens",
                        "cached_input_tokens",
                        "output_tokens",
                        "reasoning_output_tokens",
                        "total_tokens",
                    )
                }
                report = {
                    "schema_version": APP_SERVER_CORE_ARM_VERSION,
                    "manifest_sha256": manifest_sha,
                    "batch_size_ceiling": batch_size,
                    "thread_mode": thread_mode,
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "low",
                    "requested_segments": 32,
                    "validated_segments": 32,
                    "accounting_complete": True,
                    "usage_status": "complete",
                    "usage_unknown_attempts": 0,
                    "retry_count": 0,
                    "concurrency": 1,
                    "requested_calls": len(batches),
                    "attempted_calls": len(batches),
                    "terminal_sidecars": len(batches),
                    "usage": usage,
                    "window_count": 4,
                    "context_chars": 900,
                    "max_events_per_segment": 32,
                    "output_schema_version": "pif_app_server_episode_batch_core_v3",
                    "transport_client_version": APP_SERVER_CLIENT_VERSION,
                    "guideline_artifact_sha256": "9" * 64,
                    "core_instructions_sha256": "8" * 64,
                }
                report_path = arm_dir / "report.json"
                report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
                self.arm_reports.append(report_path)

    def convert_batch5_same_to_interruption(self) -> Path:
        report_path = next(
            path for path in self.arm_reports if path.parent.name == "batch_5_same_thread"
        )
        self.arm_reports.remove(report_path)
        mapping_path = report_path.parent / "private-mapping.json"
        mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
        completed_hashes = []
        completed_usage = []
        for index, batch in enumerate(mapping["batches"]):
            sidecar_path = Path(batch["sidecar_path"])
            raw_path = Path(batch["raw_output_path"])
            normalized_path = Path(batch["normalized_output_path"])
            if index < 4:
                sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                completed_hashes.append(file_sha(sidecar_path))
                completed_usage.append(sidecar["usage"])
            elif index == 4:
                sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                sidecar.update(
                    {
                        "state": "cancelled",
                        "status": "cancelled",
                        "usage_complete": False,
                        "usage": None,
                        "recovery_reran_model": False,
                    }
                )
                sidecar.pop("output_sha256", None)
                sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
                raw_path.unlink()
                normalized_path.unlink()
            else:
                sidecar_path.unlink()
                raw_path.unlink()
                normalized_path.unlink()
        fields = (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "total_tokens",
        )
        partial_usage = {
            field: sum(item[field] for item in completed_usage) for field in fields
        }
        cancelled_path = Path(mapping["batches"][4]["sidecar_path"])
        artifact = {
            "schema_version": "pif_app_server_arm_intent_to_treat_interruption_v1",
            "arm": {
                "batch_size": 5,
                "thread_mode": "same_thread",
                "concurrency": 1,
                "retry_count": 0,
            },
            "classification": "terminal_interrupted_accounting_incomplete",
            "automatic_retry_prohibited": True,
            "selection_eligible": False,
            "semantic_quality_score": 0,
            "planned_calls": 8,
            "attempted_calls": 5,
            "completed_measured_calls": 4,
            "cancelled_unknown_usage_calls": 1,
            "not_started_calls": 3,
            "planned_segments": 32,
            "validated_segments": 16,
            "usage_status": "partial_unknown",
            "usage": None,
            "measured_partial_usage": partial_usage,
            "artifacts": {
                "private_mapping_sha256": file_sha(mapping_path),
                "cancelled_sidecar_sha256": file_sha(cancelled_path),
                "completed_sidecar_sha256s": sorted(completed_hashes),
            },
        }
        artifact_path = report_path.parent / "intent-to-treat-interruption-v1.json"
        artifact_path.write_text(json.dumps(artifact, sort_keys=True) + "\n", encoding="utf-8")
        report_path.unlink()
        return artifact_path


def perfect_consensus(pool: dict, mapping: dict, *, abstain_cases: int = 0) -> dict:
    provenance_by_witness = {
        item["witness_id"]: item["provenance"]
        for case in mapping["cases"]
        for item in case["witnesses"]
    }
    rows = []
    for case_index, case in enumerate(pool["cases"]):
        support = []
        for witness in case["event_set_a"] + case["event_set_b"]:
            provenance = provenance_by_witness[witness["witness_id"]]
            sentinel = provenance.get("structural_sentinel")
            verdict = "unsupported" if sentinel else "supported"
            if case_index < abstain_cases and not sentinel:
                verdict = "abstain"
            support.append(
                {
                    "witness_id": witness["witness_id"],
                    "verdict": verdict,
                    "evidence_spans": [] if verdict != "supported" else [witness["event"]["evidence"]],
                }
            )
        alignments = []
        groups = []
        grouped_ids = set()
        used_a = set()
        used_b = set()
        if case["event_set_a"] and case["event_set_b"]:
            left = case["event_set_a"][0]["witness_id"]
            right = case["event_set_b"][0]["witness_id"]
            alignments.append(
                {
                    "left_witness_id": left,
                    "right_witness_id": right,
                    "relation": "equivalent",
                    "mismatch_fields": [],
                }
            )
            used_a.add(left)
            used_b.add(right)
            groups.append(sorted([left, right]))
            grouped_ids.update([left, right])
        for witness in case["event_set_a"] + case["event_set_b"]:
            if witness["witness_id"] not in grouped_ids:
                groups.append([witness["witness_id"]])
        rows.append(
            {
                "case_id": case["case_id"],
                "status": "partial_abstain" if case_index < abstain_cases else "agreed",
                "support_results": support,
                "equivalence_groups": groups,
                "partition_abstained_witness_ids": [],
                "alignment_results": alignments,
                "unaligned_a_witness_ids": [
                    item["witness_id"] for item in case["event_set_a"] if item["witness_id"] not in used_a
                ],
                "unaligned_b_witness_ids": [
                    item["witness_id"] for item in case["event_set_b"] if item["witness_id"] not in used_b
                ],
                "alignment_abstained_witness_ids": [],
            }
        )
    return {
        "schema_version": JUDGE_CONSENSUS_VERSION,
        "pool_sha256": sha256_text(canonical(pool)),
        "cases": rows,
        "abstentions": {},
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


class AssemblyAndScoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.synthetic = SyntheticDevelopment(self.root)
        self.assembled = assemble_dev_shared_witness_pool(
            self.synthetic.conn,
            manifest_path=self.synthetic.manifest_path,
            arm_report_paths=self.synthetic.arm_reports,
            output_dir=self.root / "assembled",
        )

    def tearDown(self) -> None:
        self.synthetic.close()
        self.temp.cleanup()

    def test_assembly_preserves_all_system_memberships_and_exact_only_dedup(self) -> None:
        pool = self.assembled["pool"]
        mapping = self.assembled["private_mapping"]
        self.assertEqual(len(pool["cases"]), 32)
        self.assertEqual(self.assembled["assembly"]["arm_count"], 6)
        self.assertEqual(self.assembled["assembly"]["system_count"], 14)
        self.assertGreater(
            self.assembled["assembly"]["exact_canonical_duplicates_removed"], 300
        )
        self.assertEqual(
            self.assembled["assembly"]["submitted_nonexact_evidence_events_preserved_for_judgment"],
            1,
        )
        for arm in self.assembled["arms"].values():
            self.assertEqual(arm["config"]["guideline_artifact_sha256"], "9" * 64)
            self.assertEqual(arm["config"]["core_instructions_sha256"], "8" * 64)
        dense_case = pool["cases"][1]
        self.assertEqual(len(dense_case["event_set_a"]), 1)
        self.assertEqual(len(dense_case["event_set_b"]), 2)
        mapped = mapping["cases"][1]
        candidate_provenance = [
            item["provenance"]
            for item in mapped["witnesses"]
            if item["canonical_side"] == "b"
        ]
        membership_counts = sorted(len(item["memberships"]) for item in candidate_provenance)
        self.assertEqual(membership_counts, [1, 10])
        self.assertNotIn("Source 0", json.dumps(self.assembled["assembly"]))

    def test_shards_partition_cases_and_merge_back_in_master_order(self) -> None:
        shards = plan_judge_shards(self.assembled["pool"], max_cases=3, max_witnesses=1000)
        self.assertEqual(sum(len(item["cases"]) for item in shards), 32)
        consensuses = [
            perfect_consensus(shard, {
                **self.assembled["private_mapping"],
                "cases": [
                    case
                    for case in self.assembled["private_mapping"]["cases"]
                    if case["case_id"] in {row["case_id"] for row in shard["cases"]}
                ],
            })
            for shard in shards
        ]
        merged = merge_judge_consensuses(
            master_pool=self.assembled["pool"], shard_consensuses=consensuses
        )
        self.assertEqual(
            [item["case_id"] for item in merged["cases"]],
            [item["case_id"] for item in self.assembled["pool"]["cases"]],
        )

    def test_shard_planner_rejects_one_case_above_a_hard_cap(self) -> None:
        with self.assertRaisesRegex(SelectionBlocked, "single judge case exceeds"):
            plan_judge_shards(
                self.assembled["pool"], max_cases=32, max_witnesses=1
            )

    def test_shared_reference_scoring_and_quality_gates_are_computed(self) -> None:
        consensus = perfect_consensus(
            self.assembled["pool"], self.assembled["private_mapping"]
        )
        score = score_dev_shared_reference(
            private_mapping=self.assembled["private_mapping"],
            membership_index=self.assembled["membership_index"],
            consensus=consensus,
            manifest_rows=self.assembled["manifest_rows"],
        )
        candidate = _arm_system("batch_3_same_thread", "normalized")
        gates = evaluate_semantic_gates(score, candidate_system_id=candidate)
        self.assertEqual(score["baseline_system_id"], BASELINE_REPAIRED_SYSTEM)
        self.assertEqual(gates["bootstrap"]["iterations"], 10000)
        self.assertEqual(gates["bootstrap"]["source_clusters"], 4)
        self.assertTrue(gates["passed"])
        self.assertEqual(gates["no_signal"]["false_positive_rate"], 0.0)

    def test_same_side_paraphrases_share_one_consensus_reference_unit(self) -> None:
        consensus = perfect_consensus(
            self.assembled["pool"], self.assembled["private_mapping"]
        )
        dense_case = consensus["cases"][1]
        all_ids = sorted(
            item["witness_id"]
            for side in ("a", "b")
            for item in self.assembled["pool"]["cases"][1]["event_set_%s" % side]
        )
        dense_case["equivalence_groups"] = [all_ids]
        score = score_dev_shared_reference(
            private_mapping=self.assembled["private_mapping"],
            membership_index=self.assembled["membership_index"],
            consensus=consensus,
            manifest_rows=self.assembled["manifest_rows"],
        )
        segment_id = self.assembled["manifest_rows"][1]["segment_id"]
        baseline_row = next(
            item
            for item in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
            if item["segment_id"] == segment_id
        )
        candidate_row = next(
            item
            for item in score["systems"][_arm_system("batch_3_same_thread", "normalized")][
                "cases"
            ]
            if item["segment_id"] == segment_id
        )
        self.assertEqual(baseline_row["reference_units"], 1)
        self.assertEqual(baseline_row["covered_reference_units"], 1)
        self.assertEqual(candidate_row["covered_reference_units"], 1)

    def test_more_than_five_percent_abstained_cases_fail_worst_case_gate(self) -> None:
        consensus = perfect_consensus(
            self.assembled["pool"], self.assembled["private_mapping"], abstain_cases=2
        )
        score = score_dev_shared_reference(
            private_mapping=self.assembled["private_mapping"],
            membership_index=self.assembled["membership_index"],
            consensus=consensus,
            manifest_rows=self.assembled["manifest_rows"],
        )
        gates = evaluate_semantic_gates(
            score,
            candidate_system_id=_arm_system("batch_3_same_thread", "normalized"),
        )
        self.assertEqual(score["abstained_case_rate"], 0.0625)
        self.assertFalse(gates["checks"]["abstention_rate"])
        self.assertFalse(gates["passed"])


class CostAndBootstrapTest(unittest.TestCase):
    def test_dev_selection_cli_connection_is_sqlite_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "factory.sqlite3"
            writable = sqlite3.connect(path)
            writable.execute("CREATE TABLE fixture (id INTEGER PRIMARY KEY)")
            writable.commit()
            writable.close()
            connection = _read_only_connection(path)
            try:
                self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
                self.assertEqual(connection.total_changes, 0)
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("INSERT INTO fixture (id) VALUES (1)")
                self.assertEqual(connection.total_changes, 0)
            finally:
                connection.close()

    def test_production_cost_adds_exact_shared_context_to_both_systems(self) -> None:
        contract = {
            "baseline_segments": 60,
            "baseline_usage": {
                "input_tokens": 9000,
                "cached_input_tokens": 0,
                "output_tokens": 1000,
                "reasoning_output_tokens": 100,
                "total_tokens": 10000,
            },
            "production_amortized_context_usage": {
                "input_tokens": 900,
                "cached_input_tokens": 0,
                "output_tokens": 100,
                "reasoning_output_tokens": 10,
                "total_tokens": 1000,
            },
            "formula": "frozen-test-formula",
        }
        arm = {
            "input_tokens": 1000,
            "cached_input_tokens": 0,
            "output_tokens": 100,
            "reasoning_output_tokens": 10,
            "total_tokens": 1100,
        }
        result = production_amortized_cost(arm_usage=arm, arm_segments=30, contract=contract)
        self.assertEqual(result["candidate_tokens_scaled_to_baseline_segment_scope"], 2200)
        self.assertEqual(result["candidate_end_to_end_tokens"], 3200)
        self.assertEqual(result["baseline_end_to_end_tokens"], 11000)
        self.assertEqual(result["production_amortized_total_token_ratio_numerator"], 16)
        self.assertEqual(result["production_amortized_total_token_ratio_denominator"], 55)
        self.assertEqual(result["production_amortized_total_token_ratio"], 0.290909)
        self.assertFalse(result["passed_lte_0_28"])

    def test_bootstrap_is_fixed_seed_and_source_clustered(self) -> None:
        rows = [
            {
                "source_id": f"source_{source}",
                "baseline_f1": 0.9,
                "candidate_f1": 0.91,
            }
            for source in range(4)
            for _index in range(8)
        ]
        first = source_cluster_paired_bootstrap(rows)
        second = source_cluster_paired_bootstrap(rows)
        self.assertEqual(first, second)
        self.assertEqual(first["iterations"], QUALITY_GATES["bootstrap_iterations"])
        self.assertEqual(first["candidate_minus_baseline"], 0.01)


class InterruptedMatrixTest(unittest.TestCase):
    def test_five_clean_arms_preserve_but_never_score_interrupted_arm(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            synthetic = SyntheticDevelopment(root)
            try:
                interruption = synthetic.convert_batch5_same_to_interruption()
                assembled = assemble_dev_shared_witness_pool(
                    synthetic.conn,
                    manifest_path=synthetic.manifest_path,
                    arm_report_paths=synthetic.arm_reports,
                    interrupted_arm_provenance_path=interruption,
                    output_dir=root / "assembled-interrupted",
                )
                self.assertEqual(assembled["assembly"]["arm_count"], 5)
                self.assertEqual(
                    assembled["assembly"]["matrix_mode"],
                    "five_clean_plus_terminal_interrupted",
                )
                self.assertFalse(assembled["interrupted_arm"]["selection_eligible"])
                self.assertFalse(assembled["interrupted_arm"]["semantic_pool_included"])
                self.assertNotIn(
                    _arm_system("batch_5_same_thread", "normalized"),
                    assembled["membership_index"]["systems"],
                )
                self.assertEqual(set(assembled["arms"]), set(EXPECTED_ARMS) - {"batch_5_same_thread"})
            finally:
                synthetic.close()


class DevPersistentTransportTest(unittest.IsolatedAsyncioTestCase):
    async def test_all_dev_shards_borrow_one_persistent_app_server_client(self) -> None:
        raw_cases = [
            {
                "case_key": "case_%02d" % index,
                "source_excerpt": "Evidence %02d." % index,
                "event_set_a": [
                    {
                        "event_type": "forecast",
                        "claim_text": "Claim %02d" % index,
                        "evidence": "Evidence %02d" % index,
                    }
                ],
                "event_set_b": [],
            }
            for index in range(3)
        ]
        pool, _mapping = make_shared_witness_pool(raw_cases, seed="dev-persistence-test")

        class PersistentFactory:
            def __init__(self):
                self.client = object()
                self.entered = 0
                self.exited = 0

            def __call__(self):
                outer = self

                class Context:
                    async def __aenter__(self):
                        outer.entered += 1
                        return outer.client

                    async def __aexit__(self, *_args):
                        outer.exited += 1
                        return False

                return Context()

        factory = PersistentFactory()
        borrowed_clients = []

        async def judge_runner(**kwargs):
            async with kwargs["client_factory"]() as client:
                borrowed_clients.append(client)
            shard = json.loads(Path(kwargs["pool_path"]).read_text(encoding="utf-8"))
            cases = []
            for case in shard["cases"]:
                cases.append(
                    {
                        "case_id": case["case_id"],
                        "status": "agreed",
                        "support_results": [
                            {"witness_id": witness["witness_id"], "verdict": "supported"}
                            for side in ("a", "b")
                            for witness in case["event_set_%s" % side]
                        ],
                        "alignment_results": [],
                        "alignment_abstained_witness_ids": [],
                    }
                )
            output = Path(kwargs["output_dir"])
            output.mkdir(parents=True, exist_ok=True)
            (output / "consensus.private.json").write_text(
                json.dumps({"schema_version": JUDGE_CONSENSUS_VERSION, "cases": cases})
                + "\n",
                encoding="utf-8",
            )
            (output / "report.json").write_text("{}\n", encoding="utf-8")
            return {
                "accounting_complete": True,
                "usage": {
                    "input_tokens": 10,
                    "cached_input_tokens": 0,
                    "output_tokens": 2,
                    "reasoning_output_tokens": 1,
                    "total_tokens": 12,
                },
            }

        with tempfile.TemporaryDirectory() as temp:
            report = await run_full_judge_shards(
                pool=pool,
                output_dir=Path(temp),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=10,
                client_factory=factory,
                judge_runner=judge_runner,
            )
        self.assertEqual(report["shard_count"], 2)
        self.assertEqual(factory.entered, 1)
        self.assertEqual(factory.exited, 1)
        self.assertEqual(borrowed_clients, [factory.client, factory.client])


class CalibrationOrderTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = sqlite3.connect(":memory:")

    async def asyncTearDown(self) -> None:
        self.conn.close()
        self.temp.cleanup()

    async def test_failed_calibration_blocks_without_running_full_judge(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text("{}\n", encoding="utf-8")
        arm_report = self.root / "arm.json"
        arm_report.write_text("{}\n", encoding="utf-8")

        def fake_assembly(_conn, *, output_dir, **_kwargs):
            output_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "assembly-report.json",
                "shared-witness-pool.private.json",
                "private-mapping.json",
                "membership-index.private.json",
            ):
                (output_dir / name).write_text("{}\n", encoding="utf-8")
            return {
                "assembly": {
                    "manifest_sha256": "a" * 64,
                    "matrix_mode": "six_clean",
                    "interrupted_arm_provenance_sha256": None,
                    "pool_sha256": file_sha(output_dir / "shared-witness-pool.private.json"),
                    "private_mapping_sha256": file_sha(output_dir / "private-mapping.json"),
                    "membership_index_sha256": file_sha(output_dir / "membership-index.private.json"),
                },
                "pool": {"cases": []},
                "private_mapping": {},
                "membership_index": {},
                "manifest_rows": [],
                "arms": {
                    name: {"report_sha256": str(index + 1).zfill(64)}
                    for index, name in enumerate(EXPECTED_ARMS)
                },
                "interrupted_arm": None,
            }

        cost = {
            "context_usage_recovery_sha256": "b" * 64,
            "context_cost_report_sha256": "c" * 64,
            "baseline_phase_one_sha256": "d" * 64,
        }

        async def calibration(**kwargs):
            kwargs["output_dir"].mkdir(parents=True, exist_ok=True)
            (kwargs["output_dir"] / "report.json").write_text(
                json.dumps({"calibrated": False}) + "\n", encoding="utf-8"
            )
            return {"calibrated": False}

        full_judge = AsyncMock(side_effect=AssertionError("full judge must not run"))
        with (
            patch(
                "research_factory.app_server_dev_selection.assemble_dev_shared_witness_pool",
                side_effect=fake_assembly,
            ),
            patch(
                "research_factory.app_server_dev_selection.load_exact_cost_contract",
                return_value=cost,
            ),
            patch(
                "research_factory.app_server_dev_selection.run_full_judge_shards",
                full_judge,
            ),
        ):
            result = await run_app_server_dev_selection(
                self.conn,
                manifest_path=manifest_path,
                arm_report_paths=[arm_report] * 6,
                context_usage_recovery_path=self.root / "context.json",
                output_dir=self.root / "selection",
                calibration_runner=calibration,
            )
        self.assertEqual(result["schema_version"], DEV_SELECTION_VERSION)
        self.assertEqual(result["selection_status"], "blocked")
        self.assertEqual(result["blocked_stage"], "calibration")
        self.assertIs(result["holdout_preparation_authorized"], False)
        self.assertIs(result["holdout_model_calls_authorized"], False)
        full_judge.assert_not_awaited()

    async def test_passing_selection_emits_holdout_compatible_lowest_cost_winner(self) -> None:
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text("{}\n", encoding="utf-8")

        def fake_assembly(_conn, *, output_dir, **_kwargs):
            output_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "assembly-report.json",
                "shared-witness-pool.private.json",
                "private-mapping.json",
                "membership-index.private.json",
            ):
                (output_dir / name).write_text("{}\n", encoding="utf-8")
            arms = {}
            for index, name in enumerate(EXPECTED_ARMS):
                batch = int(name.split("_")[1])
                mode = "same_thread" if name.endswith("same_thread") else "new_thread"
                arms[name] = {
                    "variant_id": name,
                    "batch_size": batch,
                    "thread_mode": mode,
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "low",
                    "report_sha256": ("%x" % (index + 1)).zfill(64),
                    "usage": {
                        "input_tokens": 90 + index * 10,
                        "cached_input_tokens": 0,
                        "output_tokens": 10,
                        "reasoning_output_tokens": 1,
                        "total_tokens": 100 + index * 10,
                    },
                    "config": {
                        "variant_id": name,
                        "batch_size": batch,
                        "thread_mode": mode,
                        "model": "gpt-5.6-sol",
                        "reasoning_effort": "low",
                        "concurrency": 1,
                        "retry_count": 0,
                        "window_count": 4,
                        "context_chars": 900,
                        "max_events_per_segment": 32,
                        "output_schema_version": "pif_app_server_episode_batch_v1",
                        "transport_client_version": APP_SERVER_CLIENT_VERSION,
                        "guideline_artifact_sha256": "e" * 64,
                        "core_instructions_sha256": "f" * 64,
                    },
                }
            return {
                "assembly": {
                    "manifest_sha256": "a" * 64,
                    "matrix_mode": "six_clean",
                    "interrupted_arm_provenance_sha256": None,
                    "pool_sha256": file_sha(output_dir / "shared-witness-pool.private.json"),
                    "private_mapping_sha256": file_sha(output_dir / "private-mapping.json"),
                    "membership_index_sha256": file_sha(output_dir / "membership-index.private.json"),
                },
                "pool": {"cases": []},
                "private_mapping": {},
                "membership_index": {},
                "manifest_rows": [],
                "arms": arms,
                "interrupted_arm": None,
            }

        cost = {
            "context_usage_recovery_sha256": "b" * 64,
            "context_cost_report_sha256": "c" * 64,
            "baseline_phase_one_sha256": "d" * 64,
            "baseline_segments": 32,
            "baseline_usage": {
                "input_tokens": 9000,
                "cached_input_tokens": 0,
                "output_tokens": 1000,
                "reasoning_output_tokens": 100,
                "total_tokens": 10000,
            },
            "production_amortized_context_usage": {
                "input_tokens": 90,
                "cached_input_tokens": 0,
                "output_tokens": 10,
                "reasoning_output_tokens": 1,
                "total_tokens": 100,
            },
            "formula": "frozen-test-formula",
        }

        async def calibration(**kwargs):
            kwargs["output_dir"].mkdir(parents=True, exist_ok=True)
            (kwargs["output_dir"] / "report.json").write_text(
                json.dumps({"calibrated": True}) + "\n", encoding="utf-8"
            )
            return {"calibrated": True}

        async def full_judge(**kwargs):
            output = kwargs["output_dir"]
            output.mkdir(parents=True, exist_ok=True)
            (output / "report.json").write_text("{}\n", encoding="utf-8")
            (output / "consensus.private.json").write_text(
                json.dumps({"schema_version": JUDGE_CONSENSUS_VERSION, "cases": []}) + "\n",
                encoding="utf-8",
            )
            return {"accounting_complete": True}

        semantic = {
            "passed": True,
            "checks": {"abstention_rate": True, "no_signal_worst_case": True},
        }
        with (
            patch(
                "research_factory.app_server_dev_selection.assemble_dev_shared_witness_pool",
                side_effect=fake_assembly,
            ),
            patch(
                "research_factory.app_server_dev_selection.load_exact_cost_contract",
                return_value=cost,
            ),
            patch(
                "research_factory.app_server_dev_selection.run_full_judge_shards",
                side_effect=full_judge,
            ),
            patch(
                "research_factory.app_server_dev_selection.score_dev_shared_reference",
                return_value={"schema_version": "synthetic_score", "systems": {}},
            ),
            patch(
                "research_factory.app_server_dev_selection.evaluate_semantic_gates",
                return_value=semantic,
            ),
        ):
            result = await run_app_server_dev_selection(
                self.conn,
                manifest_path=manifest_path,
                arm_report_paths=[self.root / "unused"] * 6,
                context_usage_recovery_path=self.root / "context.json",
                output_dir=self.root / "winner-selection",
                calibration_runner=calibration,
            )
        self.assertEqual(result["schema_version"], FROZEN_WINNER_VERSION)
        self.assertEqual(result["winner"]["variant_id"], EXPECTED_ARMS[0])
        self.assertEqual(
            set(result["winner"]["config"]),
            {
                "variant_id",
                "batch_size",
                "thread_mode",
                "model",
                "reasoning_effort",
                "concurrency",
                "retry_count",
                "window_count",
                "context_chars",
                "max_events_per_segment",
                "output_schema_version",
                "transport_client_version",
                "guideline_artifact_sha256",
                "core_instructions_sha256",
            },
        )
        self.assertEqual(
            result["winner"]["config_sha256"],
            sha256_text(canonical(result["winner"]["config"])),
        )
        loaded = load_frozen_winner(self.root / "winner-selection" / "selection-result.json")
        self.assertEqual(loaded["payload"]["winner"]["variant_id"], EXPECTED_ARMS[0])


if __name__ == "__main__":
    unittest.main()
