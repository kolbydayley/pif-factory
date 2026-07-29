from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from unittest.mock import AsyncMock, patch

from research_factory import app_server_evaluation as evaluation
from research_factory import app_server_expanded_cap_episode_batch as expanded_cap
from research_factory.app_server_checkpoint import verify_instruction_contract
from research_factory.app_server_holdout import HOLDOUT_COVENANT_VERSION
from research_factory.app_server_dev_selection import (
    DEV_MEMBERSHIP_VERSION,
    SelectionBlocked,
)
from research_factory.app_server_holdout_judge import (
    BASELINE_RAW_SYSTEM,
    BASELINE_REPAIRED_SYSTEM,
    CANDIDATE_RAW_SYSTEM,
    CANDIDATE_WINNER_SYSTEM,
    HOLDOUT_GATE_VERSION,
    HOLDOUT_GATES,
    HOLDOUT_SCORE_VERSION,
    JUDGE_MODEL,
    JUDGE_REASONING_EFFORT,
    REFERENCE_SYSTEM,
    _one_sided_binomial_upper,
    assemble_holdout_shared_witness_pool,
    evaluate_holdout_quality_gates,
    load_holdout_execution_inputs,
    production_amortized_holdout_cost,
    run_app_server_holdout_judge,
    run_holdout_judge_shards,
    _load_candidate_outputs,
)
from research_factory.app_server_holdout_client import (
    HOLDOUT_SIDECAR_LINEAGE_FIELD,
    verified_holdout_execution_lineage,
)
from research_factory.app_server_llm_judge import (
    JUDGE_CONSENSUS_VERSION,
    SHARED_WITNESS_POOL_VERSION,
    make_shared_witness_pool,
)
from research_factory.util import sha256_text
from tests.test_app_server_expanded_cap_episode_batch import _FixtureFactory
from tests.test_app_server_llm_judge import FakeAppServerClient as FakeJudgeClient


def canonical_bytes(value):
    return (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def event(claim: str, evidence: str):
    return {
        "event_type": "capability_claim",
        "claim_text": claim,
        "evidence": evidence,
    }


class HoldoutGateMathTest(unittest.TestCase):
    def test_exact_no_signal_upper_bound_requires_zero_false_positives_at_n_60(self):
        zero = _one_sided_binomial_upper(0, 60, confidence=0.95)
        one = _one_sided_binomial_upper(1, 60, confidence=0.95)
        self.assertLessEqual(zero, HOLDOUT_GATES["max_no_signal_false_positive_rate"])
        self.assertGreater(one, HOLDOUT_GATES["max_no_signal_false_positive_rate"])
        self.assertAlmostEqual(zero, 1 - (0.05 ** (1 / 60)), places=10)

    def test_cost_uses_exact_production_allocated_phase_a_symmetrically(self):
        candidate = {
            "input_tokens": 190,
            "cached_input_tokens": 10,
            "output_tokens": 10,
            "reasoning_output_tokens": 2,
            "total_tokens": 200,
        }
        baseline = {
            "input_tokens": 990,
            "cached_input_tokens": 0,
            "output_tokens": 10,
            "reasoning_output_tokens": 2,
            "total_tokens": 1000,
        }
        context = {
            "usage": {
                "input_tokens": 990,
                "cached_input_tokens": 0,
                "output_tokens": 10,
                "reasoning_output_tokens": 1,
                "total_tokens": 1000,
            },
            "production_allocated_total_tokens": Fraction(201, 2),
            "episodes": [],
        }
        result = production_amortized_holdout_cost(
            candidate_usage=candidate,
            baseline_usage=baseline,
            context=context,
        )
        self.assertEqual(result["measured_context_total_tokens"], 1000)
        self.assertEqual(result["production_allocated_context_tokens_numerator"], 201)
        self.assertEqual(result["production_allocated_context_tokens_denominator"], 2)
        self.assertEqual(result["candidate_end_to_end_tokens_numerator"], 601)
        self.assertEqual(result["candidate_end_to_end_tokens_denominator"], 2)
        self.assertEqual(result["baseline_end_to_end_tokens_numerator"], 2201)
        self.assertEqual(result["baseline_end_to_end_tokens_denominator"], 2)
        self.assertEqual(result["production_amortized_total_token_ratio_numerator"], 601)
        self.assertEqual(result["production_amortized_total_token_ratio_denominator"], 2201)
        self.assertTrue(result["passed_lte_0_28"])

    def _quality_fixture(self):
        selection = []
        baseline_cases = []
        candidate_cases = []
        candidate_memberships = {}
        for index in range(120):
            segment_id = "seg_%03d" % index
            paired = index < 60
            selection.append(
                {
                    "segment_id": segment_id,
                    "source_id": "source_%d" % (index % 4),
                    "evaluation_set": "paired_quality" if paired else "clean_no_signal_power",
                    "reference_stratum": ("no_signal", "low", "medium", "dense")[index % 4]
                    if paired
                    else "no_signal",
                }
            )
            base = {
                "segment_id": segment_id,
                "f1": 1.0,
                "case_abstained_worst_case": False,
            }
            baseline_cases.append(base)
            candidate_cases.append(dict(base))
            candidate_memberships[segment_id] = {
                "submitted_event_count": 1 if paired else 0,
                "exact_evidence_event_count": 1 if paired else 0,
            }
        score = {
            "abstained_cases": [],
            "abstained_case_rate": 0.0,
            "systems": {
                BASELINE_REPAIRED_SYSTEM: {"cases": baseline_cases},
                CANDIDATE_WINNER_SYSTEM: {"cases": candidate_cases},
            },
        }
        membership = {
            "system_cases": {CANDIDATE_WINNER_SYSTEM: candidate_memberships}
        }
        return score, membership, selection

    def test_quality_gate_uses_paired_60_and_separate_clean_no_signal_60(self):
        score, membership, selection = self._quality_fixture()
        result = evaluate_holdout_quality_gates(
            score=score,
            membership_index=membership,
            selection_rows=selection,
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["paired_bootstrap"]["segments"], 60)
        self.assertEqual(result["clean_no_signal"]["cases"], 60)
        self.assertEqual(result["clean_no_signal"]["false_positive_or_abstained_worst_case"], 0)
        self.assertLessEqual(result["clean_no_signal"]["clopper_pearson_upper"], 0.05)

        membership["system_cases"][CANDIDATE_WINNER_SYSTEM]["seg_060"][
            "submitted_event_count"
        ] = 1
        failed = evaluate_holdout_quality_gates(
            score=score,
            membership_index=membership,
            selection_rows=selection,
        )
        self.assertFalse(failed["passed"])
        self.assertFalse(failed["checks"]["clean_no_signal_exact_upper_bound"])

    def test_dense_regression_cannot_be_averaged_away_by_other_strata(self):
        score, membership, selection = self._quality_fixture()
        baseline_rows = score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
        candidate_rows = score["systems"][CANDIDATE_WINNER_SYSTEM]["cases"]
        for index in range(60):
            # Make every source contain every stratum so source-level gates see a
            # zero net delta while dense quality alone materially regresses.
            selection[index]["source_id"] = "source_%d" % ((index // 4) % 4)
            baseline_rows[index]["f1"] = 0.8
            stratum = selection[index]["reference_stratum"]
            candidate_rows[index]["f1"] = {
                "no_signal": 0.8,
                "low": 1.0,
                "medium": 1.0,
                "dense": 0.4,
            }[stratum]
        result = evaluate_holdout_quality_gates(
            score=score,
            membership_index=membership,
            selection_rows=selection,
        )
        self.assertAlmostEqual(result["paired_bootstrap"]["candidate_minus_baseline"], 0.0)
        self.assertTrue(result["checks"]["macro_source_noninferiority"])
        self.assertTrue(result["checks"]["worst_source_noninferiority"])
        self.assertFalse(result["checks"]["dense_stratum_noninferiority"])
        self.assertFalse(result["passed"])
        self.assertEqual(result["by_stratum"]["dense"]["segments"], 15)
        self.assertEqual(set(result["source_by_stratum"]), {"source_0", "source_1", "source_2", "source_3"})


class HoldoutAssemblyTest(unittest.TestCase):
    def test_unauthorized_judge_loader_stops_before_verifier_reservoir_or_sql_read(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            covenant = root / "unauthorized-covenant.json"
            covenant.write_bytes(
                canonical_bytes(
                    {
                        "schema_version": HOLDOUT_COVENANT_VERSION,
                        "holdout_model_calls_authorized": False,
                        "artifacts": {
                            "paired_quality_reservoir": {
                                "filename": "must-not-open.json"
                            }
                        },
                    }
                )
            )
            conn = sqlite3.connect(":memory:")
            statements = []
            conn.set_trace_callback(statements.append)
            try:
                with (
                    patch(
                        "research_factory.app_server_holdout_judge.verify_instruction_contract"
                    ) as instruction_reader,
                    patch(
                        "research_factory.app_server_holdout_judge.verify_frozen_holdout"
                    ) as verifier,
                    patch(
                        "research_factory.app_server_holdout_judge._load_reservoir_rows"
                    ) as reservoir_reader,
                    patch(
                        "research_factory.app_server_holdout_judge._verified_sources"
                    ) as source_reader,
                ):
                    with self.assertRaisesRegex(
                        ValueError, "does not authorize protected access"
                    ):
                        load_holdout_execution_inputs(
                            conn,
                            covenant_path=covenant,
                            selection_path=root / "must-not-open-selection.json",
                            execution_dir=root / "must-not-open-execution",
                        )
                verifier.assert_not_called()
                instruction_reader.assert_not_called()
                reservoir_reader.assert_not_called()
                source_reader.assert_not_called()
                self.assertEqual(statements, [])
            finally:
                conn.close()

    def test_one_pool_keeps_reference_raw_repaired_and_winner_memberships_separate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            text = "The host says this model can solve the task."
            evidence = "this model can solve the task"
            selection = {
                "segment_id": "seg_1",
                "episode_id": "ep_1",
                "source_id": "src_1",
                "evaluation_set": "paired_quality",
                "reference_stratum": "low",
                "text_sha256": "a" * 64,
                "reference_case_sha256": "b" * 64,
            }
            reference_event = event("The model can solve the task.", evidence)
            baseline_raw = event("Raw baseline claim.", evidence)
            baseline_repaired = event("Repaired baseline claim.", evidence)
            candidate_raw = event("Raw winner claim.", evidence)
            candidate_normalized = event("Normalized winner claim.", evidence)
            inputs = {
                "covenant_sha256": "c" * 64,
                "selection_sha256": "d" * 64,
                "selection_rows": [selection],
                "sources": {"seg_1": {"text": text}},
                "references": {"seg_1": {"supported_events": [reference_event]}},
                "baseline": {
                    "by_segment": {
                        "seg_1": {
                            "raw": {"discourse_events": [baseline_raw]},
                            "repaired": {"discourse_events": [baseline_repaired]},
                        }
                    }
                },
                "candidate": {
                    "raw_by_segment": {"seg_1": {"events": [candidate_raw]}},
                    "normalized_by_segment": {
                        "seg_1": {"events": [candidate_normalized]}
                    },
                    "manifest_path": str(root / "manifest.json"),
                    "manifest_sha256": "e" * 64,
                },
            }
            assembled = assemble_holdout_shared_witness_pool(
                inputs=inputs, output_dir=root / "pool"
            )
            self.assertEqual(assembled["membership_index"]["schema_version"], DEV_MEMBERSHIP_VERSION)
            self.assertEqual(
                set(assembled["membership_index"]["systems"]),
                {
                    REFERENCE_SYSTEM,
                    BASELINE_RAW_SYSTEM,
                    BASELINE_REPAIRED_SYSTEM,
                    CANDIDATE_RAW_SYSTEM,
                    CANDIDATE_WINNER_SYSTEM,
                },
            )
            self.assertEqual(len(assembled["pool"]["cases"]), 1)
            self.assertEqual(
                assembled["membership_index"]["system_cases"][BASELINE_RAW_SYSTEM]["seg_1"][
                    "submitted_event_count"
                ],
                1,
            )
            self.assertEqual(
                assembled["membership_index"]["system_cases"][BASELINE_REPAIRED_SYSTEM]["seg_1"][
                    "submitted_event_count"
                ],
                1,
            )

    def test_judge_revalidates_final_adapter_attempt_sidecar_and_projection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            text = "Speaker 0:\nSystem 0 improves latency.\n"
            text_path = root / "segment.txt"
            text_path.write_text(text, encoding="utf-8")
            text_sha256 = hashlib.sha256(text.encode()).hexdigest()
            context = {
                "schema_version": "ai_discourse_v3_1_episode_context",
                "episode_id": "ep_1",
                "context_summary": "Synthetic fixture context only.",
                "speaker_map": [],
                "section_map": [],
                "entity_seed": {},
                "concept_seed": ["fixture latency"],
                "extraction_guidance": "Use only the synthetic source units.",
                "excluded_source_context": ["synthetic sponsor reads"],
            }
            context_path = root / "context.json"
            context_path.write_bytes(canonical_bytes(context))
            context_sha256 = hashlib.sha256(context_path.read_bytes()).hexdigest()
            covenant_sha256 = "a" * 64
            manifest = {
                "schema_version": evaluation.APP_SERVER_DEVELOPMENT_MANIFEST_V2,
                "evaluation_role": "untouched_private_holdout_never_production",
                "covenant_sha256": covenant_sha256,
                "event_cap": expanded_cap.MAX_EVENTS_PER_SEGMENT,
                "episode_count": 1,
                "segment_count": 1,
                "episodes": [
                    {
                        "episode_id": "ep_1",
                        "source_id": "src_1",
                        "source_name": "Fixture Source",
                        "episode_title": "Fixture Episode",
                        "episode_context": {
                            "artifact_path": str(context_path),
                            "artifact_sha256": context_sha256,
                        },
                        "segments": [
                            {
                                "segment_id": "seg_1",
                                "text_sha256": text_sha256,
                                "density_stratum": "no_signal",
                                "evaluation_set": "paired_quality",
                            }
                        ],
                    }
                ],
            }
            manifest_path = root / "execution" / "phase-a-contexts" / "holdout-manifest.json"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_bytes(canonical_bytes(manifest))
            manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            conn.execute("CREATE TABLE segments (id TEXT PRIMARY KEY, text_path TEXT)")
            conn.execute("INSERT INTO segments VALUES (?, ?)", ("seg_1", str(text_path)))
            try:
                episodes = evaluation._load_prepared_episodes(  # noqa: SLF001
                    conn, manifest=manifest, window_count=4, context_chars=900
                )
                requests = expanded_cap.prepare_episode_batches(
                    episodes[0], batch_size=3, thread_mode="new_thread"
                )
                now = datetime.now(timezone.utc)
                capacity = {
                    "schema_version": expanded_cap.CAPACITY_ADMISSION_VERSION,
                    "admission_id": "judge-final-adapter-fixture",
                    "issued_at": (now - timedelta(seconds=1)).isoformat(),
                    "expires_at": (now + timedelta(minutes=5)).isoformat(),
                    "state": "admitted",
                    "issued_by": "evaluation_coordinator",
                    "verification_mode": "live_verified",
                    "fixture": False,
                    "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
                    "batch_size": 3,
                    "thread_mode": "new_thread",
                    "model": expanded_cap.MODEL,
                    "effort": expanded_cap.EFFORT,
                    "concurrency": 1,
                    "episode_ids_sha256": sha256_text(
                        expanded_cap._canonical_json(["ep_1"])  # noqa: SLF001
                    ),
                    "episode_count": 1,
                    "segment_count": 1,
                    "batch_count": len(requests),
                    "live_capacity_available": True,
                    "managed_chatgpt_auth_only": True,
                    "verification_evidence_sha256": "b" * 64,
                }
                capacity_sha256 = expanded_cap.capacity_admission_sha256(capacity)
                config = expanded_cap.build_frozen_configuration(
                    batch_size=3, thread_mode="new_thread"
                )
                arm_root = root / "execution" / "phase-b-candidate" / "candidate-arm"
                report = asyncio.run(
                    expanded_cap.run_episode_batch_arm(
                        episodes,
                        output_dir=arm_root,
                        batch_size=3,
                        thread_mode="new_thread",
                        capacity_admission=capacity,
                        capacity_admission_sha256=capacity_sha256,
                        frozen_configuration=config,
                        fixture_mode=False,
                        client_factory=_FixtureFactory(),
                    )
                )
                report_path = arm_root / "report.json"
                mapping_path = arm_root / "private-mapping.json"
                config_sha256 = sha256_text(
                    expanded_cap._canonical_json(config)  # noqa: SLF001
                )
                winner = {
                    "variant_id": "batch_3_new_thread",
                    "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
                    "batch_size": 3,
                    "thread_mode": "new_thread",
                    "model": expanded_cap.MODEL,
                    "reasoning_effort": expanded_cap.EFFORT,
                    "concurrency": 1,
                    "retry_count": 0,
                    "window_count": 4,
                    "context_chars": 900,
                    "max_events_per_segment": expanded_cap.MAX_EVENTS_PER_SEGMENT,
                    "frozen_configuration": config,
                    "frozen_configuration_sha256": config_sha256,
                    "report_sha256": "c" * 64,
                }
                candidate_phase = {
                    "evaluation_role": "untouched_private_holdout_never_production",
                    "holdout_authorized": True,
                    "fixture_mode": False,
                    "automatic_retry_prohibited": True,
                    "production_changed": False,
                    "frozen_configuration_sha256": config_sha256,
                    "capacity_admission_sha256": capacity_sha256,
                    "requested_segments": 1,
                    "usage": report["usage"],
                    "candidate_arm_report_path": str(report_path),
                    "candidate_arm_report_sha256": hashlib.sha256(
                        report_path.read_bytes()
                    ).hexdigest(),
                    "candidate_mapping_path": str(mapping_path),
                    "candidate_mapping_sha256": hashlib.sha256(
                        mapping_path.read_bytes()
                    ).hexdigest(),
                }
                kwargs = {
                    "conn": conn,
                    "execution_root": root / "execution",
                    "candidate_phase": candidate_phase,
                    "context_phase": {
                        "manifest_path": str(manifest_path),
                        "manifest_sha256": manifest_sha256,
                    },
                    "covenant": {
                        "covenant_sha256": covenant_sha256,
                        "winner": winner,
                    },
                    "selection_rows": [
                        {
                            "segment_id": "seg_1",
                            "episode_id": "ep_1",
                            "text_sha256": text_sha256,
                        }
                    ],
                    "sources": {"seg_1": {"text": text}},
                }
                loaded = _load_candidate_outputs(**kwargs)
                self.assertEqual(set(loaded["normalized_by_segment"]), {"seg_1"})

                normalized_path = Path(
                    json.loads(mapping_path.read_text())["batches"][0][
                        "normalized_output_path"
                    ]
                )
                normalized_path.write_bytes(
                    canonical_bytes({"episode_id": "ep_1", "segments": []})
                )
                with self.assertRaisesRegex(SelectionBlocked, "output drift"):
                    _load_candidate_outputs(**kwargs)
            finally:
                conn.close()


class HoldoutOrchestrationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        (self.root / "covenant.json").write_bytes(
            canonical_bytes(
                {
                    "schema_version": HOLDOUT_COVENANT_VERSION,
                    "holdout_model_calls_authorized": True,
                }
            )
        )

    async def asyncTearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def _inputs(self):
        selection_rows = [
            {
                "segment_id": "seg_000",
                "episode_id": "ep_1",
                "source_id": "src_1",
                "reference_stratum": "no_signal",
                "evaluation_set": "paired_quality",
            }
        ]
        usage_candidate = {
            "input_tokens": 190,
            "cached_input_tokens": 0,
            "output_tokens": 10,
            "reasoning_output_tokens": 1,
            "total_tokens": 200,
        }
        usage_baseline = {
            "input_tokens": 990,
            "cached_input_tokens": 0,
            "output_tokens": 10,
            "reasoning_output_tokens": 1,
            "total_tokens": 1000,
        }
        usage_context = {
            "input_tokens": 90,
            "cached_input_tokens": 0,
            "output_tokens": 10,
            "reasoning_output_tokens": 1,
            "total_tokens": 100,
        }
        return {
            "covenant_sha256": "a" * 64,
            "selection_sha256": "b" * 64,
            "execution_plan_sha256": "c" * 64,
            "phase_reports": {
                key: {"sha256": character * 64}
                for key, character in (("context", "d"), ("candidate", "e"), ("baseline", "f"))
            },
            "reference_artifacts": {"index_sha256": "1" * 64},
            "calibration_report_sha256": "7" * 64,
            "candidate": {
                "report_sha256": "2" * 64,
                "mapping_sha256": "3" * 64,
                "usage": usage_candidate,
            },
            "baseline": {"usage": usage_baseline},
            "context": {
                "usage": usage_context,
                "production_allocated_total_tokens": Fraction(100, 1),
                "episodes": [],
            },
            "selection_rows": selection_rows,
        }

    def _assembled(self, root: Path):
        pool = {
            "schema_version": SHARED_WITNESS_POOL_VERSION,
            "seed_sha256": "0" * 64,
            "cases": [],
            "privacy": "private",
        }
        mapping = {"schema_version": "fixture", "cases": []}
        membership = {"schema_version": DEV_MEMBERSHIP_VERSION}
        pool_root = root / "witness-pool"
        pool_root.mkdir(parents=True, exist_ok=True)
        pool_path = pool_root / "shared-witness-pool.private.json"
        mapping_path = pool_root / "private-mapping.json"
        membership_path = pool_root / "membership-index.private.json"
        pool_path.write_bytes(canonical_bytes(pool))
        mapping_path.write_bytes(canonical_bytes(mapping))
        membership_path.write_bytes(canonical_bytes(membership))
        file_sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        assembly_report = {
            "pool_sha256": file_sha(pool_path),
            "private_mapping_sha256": file_sha(mapping_path),
            "membership_index_sha256": file_sha(membership_path),
        }
        (pool_root / "assembly-report.json").write_bytes(
            canonical_bytes(assembly_report)
        )
        return {
            "pool": pool,
            "private_mapping": mapping,
            "membership_index": membership,
            "report": assembly_report,
        }

    async def test_passed_terminal_gate_is_immutable_and_rerun_does_not_call_judge(self):
        inputs = self._inputs()
        assembled = self._assembled(self.root / "out")

        async def full_judge(**kwargs):
            output = Path(kwargs["output_dir"])
            output.mkdir(parents=True, exist_ok=True)
            consensus = {"schema_version": JUDGE_CONSENSUS_VERSION, "cases": []}
            (output / "consensus.private.json").write_bytes(canonical_bytes(consensus))
            report = {
                "schema_version": "fixture-full-holdout-judge",
                "state": "completed",
                "model": JUDGE_MODEL,
                "reasoning_effort": JUDGE_REASONING_EFFORT,
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "shard_count": 0,
                "shards": [],
                "accounting_complete": True,
                "usage": {
                    "input_tokens": 10,
                    "cached_input_tokens": 0,
                    "output_tokens": 2,
                    "reasoning_output_tokens": 1,
                    "total_tokens": 12,
                },
                "abstentions": {"cases": {"numerator": 0, "denominator": 1, "rate": 0}},
                "instruction_contract": kwargs["instruction_contract"],
                "execution_lineage": kwargs["execution_lineage"],
                "turn_semantic_outputs": ["support", "alignment"],
                "deterministic_consensus_additional_model_calls": 0,
            }
            (output / "report.json").write_bytes(canonical_bytes(report))
            return report

        score = {"schema_version": HOLDOUT_SCORE_VERSION}
        quality = {"passed": True, "checks": {"all": True}}
        output_dir = self.root / "out"
        with (
            patch(
                "research_factory.app_server_holdout_judge.load_holdout_execution_inputs",
                return_value=inputs,
            ),
            patch(
                "research_factory.app_server_holdout_judge.assemble_holdout_shared_witness_pool",
                return_value=assembled,
            ),
            patch(
                "research_factory.app_server_holdout_judge.run_holdout_judge_shards",
                side_effect=full_judge,
            ) as runner,
            patch(
                "research_factory.app_server_holdout_judge.score_dev_shared_reference",
                return_value=score,
            ),
            patch(
                "research_factory.app_server_holdout_judge.evaluate_holdout_quality_gates",
                return_value=quality,
            ),
        ):
            result = await run_app_server_holdout_judge(
                self.conn,
                covenant_path=self.root / "covenant.json",
                selection_path=self.root / "selection.json",
                execution_dir=self.root / "execution",
                output_dir=output_dir,
            )
            self.assertEqual(result["schema_version"], HOLDOUT_GATE_VERSION)
            self.assertTrue(result["gate_passed"])
            self.assertTrue(result["quality_noninferior"])
            self.assertTrue(result["production_amortized_total_token_ratio_lte_0_28"])
            self.assertFalse(result["production_changed"])
            self.assertEqual(runner.await_count, 1)
            resolved_factory = runner.await_args.kwargs["client_factory"]
            managed_client = resolved_factory()
            inner_client = managed_client.inner_factory()
            self.assertEqual(
                inner_client._epoch4_config_overlay["project_doc_max_bytes"], 0
            )
            self.assertEqual(
                inner_client._holdout_execution_lineage["instruction_contract"][
                    "instruction_sources_count"
                ],
                result["instruction_contract"]["instruction_sources_count"],
            )

            resumed = await run_app_server_holdout_judge(
                self.conn,
                covenant_path=self.root / "covenant.json",
                selection_path=self.root / "selection.json",
                execution_dir=self.root / "execution",
                output_dir=output_dir,
            )
            self.assertEqual(resumed, result)
            self.assertEqual(runner.await_count, 1)

            (output_dir / "score-report.private.json").write_bytes(
                canonical_bytes({"tampered": True})
            )
            with self.assertRaisesRegex(SelectionBlocked, "artifact hash drift"):
                await run_app_server_holdout_judge(
                    self.conn,
                    covenant_path=self.root / "covenant.json",
                    selection_path=self.root / "selection.json",
                    execution_dir=self.root / "execution",
                    output_dir=output_dir,
                )
            self.assertEqual(runner.await_count, 1)

    async def test_terminal_gate_rejects_missing_or_tampered_leaf_lineage_without_client(self):
        inputs = self._inputs()
        output_dir = self.root / "lineaged-terminal"
        assembled = self._assembled(output_dir)
        raw = [
            {
                "case_key": "case-lineage",
                "source_excerpt": "The host says the system saves time.",
                "event_set_a": [
                    event(
                        "The system saves time.",
                        "The host says the system saves time.",
                    )
                ],
                "event_set_b": [],
            }
        ]
        pool, private_mapping = make_shared_witness_pool(
            raw, seed="holdout-terminal-lineage-test"
        )
        membership = {"schema_version": DEV_MEMBERSHIP_VERSION}
        pool_root = output_dir / "witness-pool"
        pool_path = pool_root / "shared-witness-pool.private.json"
        mapping_path = pool_root / "private-mapping.json"
        membership_path = pool_root / "membership-index.private.json"
        pool_path.write_bytes(canonical_bytes(pool))
        mapping_path.write_bytes(canonical_bytes(private_mapping))
        membership_path.write_bytes(canonical_bytes(membership))
        file_sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        assembly_report = {
            "pool_sha256": file_sha(pool_path),
            "private_mapping_sha256": file_sha(mapping_path),
            "membership_index_sha256": file_sha(membership_path),
        }
        (pool_root / "assembly-report.json").write_bytes(
            canonical_bytes(assembly_report)
        )
        assembled.update(
            {
                "pool": pool,
                "private_mapping": private_mapping,
                "membership_index": membership,
                "report": assembly_report,
            }
        )

        instruction_contract = verify_instruction_contract()
        execution_lineage = verified_holdout_execution_lineage(instruction_contract)
        fake = FakeJudgeClient(
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        score = {"schema_version": HOLDOUT_SCORE_VERSION}
        quality = {"passed": True, "checks": {"all": True}}
        with (
            patch(
                "research_factory.app_server_holdout_judge.load_holdout_execution_inputs",
                return_value=inputs,
            ),
            patch(
                "research_factory.app_server_holdout_judge.assemble_holdout_shared_witness_pool",
                return_value=assembled,
            ),
            patch(
                "research_factory.app_server_holdout_judge.score_dev_shared_reference",
                return_value=score,
            ),
            patch(
                "research_factory.app_server_holdout_judge.evaluate_holdout_quality_gates",
                return_value=quality,
            ),
        ):
            result = await run_app_server_holdout_judge(
                self.conn,
                covenant_path=self.root / "covenant.json",
                selection_path=self.root / "selection.json",
                execution_dir=self.root / "execution",
                output_dir=output_dir,
                client_factory=lambda: fake,
            )
            self.assertTrue(result["gate_passed"])
            self.assertEqual(len(fake.calls), 2)
            aggregate = json.loads(
                (output_dir / "full-judge" / "report.json").read_text(
                    encoding="utf-8"
                )
            )
            for shard in aggregate["shards"]:
                shard_report = json.loads(
                    Path(shard["report_path"]).read_text(encoding="utf-8")
                )
                self.assertEqual(
                    shard["leaf_bindings_sha256"],
                    sha256_text(
                        json.dumps(
                            shard_report["leaf_bindings"],
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    ),
                )

            sidecar_path = (
                output_dir
                / "full-judge"
                / "shard-000"
                / "judge"
                / "sidecars"
                / "ab.json"
            )
            original_text = sidecar_path.read_text(encoding="utf-8")
            original = json.loads(original_text)
            tampered_sidecars = []
            missing_lineage = json.loads(json.dumps(original))
            missing_lineage.pop(HOLDOUT_SIDECAR_LINEAGE_FIELD)
            tampered_sidecars.append(missing_lineage)
            instruction_sources = json.loads(json.dumps(original))
            instruction_sources["instruction_sources_count"] += 1
            tampered_sidecars.append(instruction_sources)
            overlay = json.loads(json.dumps(original))
            overlay[HOLDOUT_SIDECAR_LINEAGE_FIELD]["strict_config_overlay"][
                "config_sha256"
            ] = "0" * 64
            tampered_sidecars.append(overlay)
            runtime = json.loads(json.dumps(original))
            runtime[HOLDOUT_SIDECAR_LINEAGE_FIELD]["runtime"][
                "protocol_schema_sha256"
            ] = "0" * 64
            tampered_sidecars.append(runtime)
            non_pro = json.loads(json.dumps(original))
            non_pro["plan_type"] = "plus"
            tampered_sidecars.append(non_pro)

            for payload in tampered_sidecars:
                sidecar_path.write_text(
                    json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
                )
                with self.assertRaises(SelectionBlocked):
                    await run_app_server_holdout_judge(
                        self.conn,
                        covenant_path=self.root / "covenant.json",
                        selection_path=self.root / "selection.json",
                        execution_dir=self.root / "execution",
                        output_dir=output_dir,
                        client_factory=lambda: self.fail(
                            "tampered terminal judge constructed a client"
                        ),
                    )
                self.assertEqual(len(fake.calls), 2)

            ba_path = sidecar_path.with_name("ba.json")
            ba_text = ba_path.read_text(encoding="utf-8")
            sidecar_path.write_text(ba_text, encoding="utf-8")
            ba_path.write_text(original_text, encoding="utf-8")
            with self.assertRaises(SelectionBlocked):
                await run_app_server_holdout_judge(
                    self.conn,
                    covenant_path=self.root / "covenant.json",
                    selection_path=self.root / "selection.json",
                    execution_dir=self.root / "execution",
                    output_dir=output_dir,
                    client_factory=lambda: self.fail(
                        "cross-swapped terminal judge constructed a client"
                    ),
                )
            self.assertEqual(len(fake.calls), 2)
            sidecar_path.write_text(original_text, encoding="utf-8")
            ba_path.write_text(ba_text, encoding="utf-8")

            sidecar_path.unlink()
            with self.assertRaises(SelectionBlocked):
                await run_app_server_holdout_judge(
                    self.conn,
                    covenant_path=self.root / "covenant.json",
                    selection_path=self.root / "selection.json",
                    execution_dir=self.root / "execution",
                    output_dir=output_dir,
                    client_factory=lambda: self.fail(
                        "missing terminal judge sidecar constructed a client"
                    ),
                )
            self.assertEqual(len(fake.calls), 2)
            sidecar_path.write_text(original_text, encoding="utf-8")

    async def test_unauthorized_final_entrypoint_stops_before_output_or_loader(self):
        covenant_path = self.root / "covenant.json"
        covenant_path.write_bytes(
            canonical_bytes(
                {
                    "schema_version": HOLDOUT_COVENANT_VERSION,
                    "holdout_model_calls_authorized": False,
                }
            )
        )
        output_dir = self.root / "must-not-create-output"
        with patch(
            "research_factory.app_server_holdout_judge.load_holdout_execution_inputs"
        ) as loader:
            with self.assertRaisesRegex(ValueError, "does not authorize protected access"):
                await run_app_server_holdout_judge(
                    self.conn,
                    covenant_path=covenant_path,
                    selection_path=self.root / "must-not-open-selection.json",
                    execution_dir=self.root / "must-not-open-execution",
                    output_dir=output_dir,
                    client_factory=lambda: self.fail(
                        "unauthorized final judge constructed a client"
                    ),
                )
        loader.assert_not_called()
        self.assertFalse(output_dir.exists())

    async def test_failure_writes_terminal_block_and_prohibits_retry(self):
        inputs = self._inputs()
        assembled = self._assembled(self.root / "blocked")
        failed_runner = AsyncMock(side_effect=RuntimeError("synthetic judge failure"))
        output_dir = self.root / "blocked"
        with (
            patch(
                "research_factory.app_server_holdout_judge.load_holdout_execution_inputs",
                return_value=inputs,
            ),
            patch(
                "research_factory.app_server_holdout_judge.assemble_holdout_shared_witness_pool",
                return_value=assembled,
            ),
            patch(
                "research_factory.app_server_holdout_judge.run_holdout_judge_shards",
                failed_runner,
            ),
        ):
            result = await run_app_server_holdout_judge(
                self.conn,
                covenant_path=self.root / "covenant.json",
                selection_path=self.root / "selection.json",
                execution_dir=self.root / "execution",
                output_dir=output_dir,
            )
            self.assertFalse(result["gate_passed"])
            self.assertTrue(result["automatic_retry_prohibited"])
            self.assertEqual(result["retry_count"], 0)
            self.assertEqual(failed_runner.await_count, 1)

            resumed = await run_app_server_holdout_judge(
                self.conn,
                covenant_path=self.root / "covenant.json",
                selection_path=self.root / "selection.json",
                execution_dir=self.root / "execution",
                output_dir=output_dir,
            )
            self.assertEqual(resumed, result)
            self.assertEqual(failed_runner.await_count, 1)


class HoldoutPersistentTransportTest(unittest.IsolatedAsyncioTestCase):
    async def test_all_holdout_shards_borrow_one_persistent_app_server_client(self):
        raw_cases = [
            {
                "case_key": "case_%02d" % index,
                "source_excerpt": "Evidence %02d." % index,
                "event_set_a": [event("Claim %02d" % index, "Evidence %02d" % index)],
                "event_set_b": [],
            }
            for index in range(9)
        ]
        pool, _mapping = make_shared_witness_pool(raw_cases, seed="holdout-persistence-test")

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
            shard_pool = json.loads(Path(kwargs["pool_path"]).read_text(encoding="utf-8"))
            cases = []
            for case in shard_pool["cases"]:
                support = [
                    {"witness_id": witness["witness_id"], "verdict": "supported"}
                    for side in ("a", "b")
                    for witness in case["event_set_%s" % side]
                ]
                cases.append(
                    {
                        "case_id": case["case_id"],
                        "status": "agreed",
                        "support_results": support,
                        "alignment_results": [],
                        "alignment_abstained_witness_ids": [],
                    }
                )
            output = Path(kwargs["output_dir"])
            output.mkdir(parents=True, exist_ok=True)
            (output / "consensus.private.json").write_bytes(
                canonical_bytes({"schema_version": JUDGE_CONSENSUS_VERSION, "cases": cases})
            )
            (output / "report.json").write_bytes(canonical_bytes({"ok": True}))
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
            report = await run_holdout_judge_shards(
                pool=pool,
                output_dir=Path(temp),
                model=JUDGE_MODEL,
                reasoning_effort=JUDGE_REASONING_EFFORT,
                timeout_seconds=10,
                client_factory=factory,
                judge_runner=judge_runner,
            )
        self.assertEqual(report["shard_count"], 2)
        self.assertEqual(factory.entered, 1)
        self.assertEqual(factory.exited, 1)
        self.assertEqual(borrowed_clients, [factory.client, factory.client])

if __name__ == "__main__":
    unittest.main()
