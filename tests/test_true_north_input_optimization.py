from __future__ import annotations

import json
import math
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import true_north_input_optimization as opt


class InputOptimizationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.suite = self.root / "suite"
        self.campaigns = self.root / "campaigns"
        self.suite.mkdir()
        episodes = {}
        for phase, ids in opt.DEFAULT_FOLDS.items():
            for episode_id in ids:
                episodes[episode_id] = phase
        bundles = []
        for episode_id in episodes:
            bundle = {
                "bundle_sha256": "",
                "episode": {"episode_id": episode_id},
                "episode_context": {"summary": "AI safety"},
                "segments": [{"segment_id": f"seg-{episode_id}", "segment_index": 0}],
                "candidates": [{
                    "candidate_id": f"cand-{episode_id}",
                    "segment_id": f"seg-{episode_id}",
                    "claim_text": "A claim",
                    "evidence_text": "A claim",
                    "evidence_start": 0,
                    "evidence_end": 7,
                }],
            }
            path = self.suite / f"{episode_id}.json"
            bundle["bundle_sha256"] = opt.true_north.sha256_text(
                opt.true_north.dumps_json({
                    key: value for key, value in bundle.items()
                    if key != "bundle_sha256"
                })
            )
            path.write_text(json.dumps(bundle), encoding="utf-8")
            bundles.append({"episode_id": episode_id, "partition": "development", "bundle_path": str(path), "bundle_sha256": bundle["bundle_sha256"], "segment_count": 1})
        hold = self.suite / "hold.json"
        hold.write_text("{}", encoding="utf-8")
        bundles.append({"episode_id": "sealed-1", "partition": "holdout", "bundle_path": str(hold), "bundle_sha256": opt._sha({})})
        manifest = {"production_mutation_allowed": False, "bundles": bundles}
        manifest["manifest_sha256"] = opt.true_north.sha256_text(
            opt.true_north.dumps_json(manifest)
        )
        (self.suite / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        gold_root = self.suite / "gold" / "development" / "final"
        gold_root.mkdir(parents=True)
        consensus_items = []
        for phase, episode_ids in opt.DEFAULT_FOLDS.items():
            for episode_id in episode_ids:
                state = (
                    "consensus_junk" if phase == "train" and not consensus_items
                    else "contested" if phase == "train"
                    else "consensus_hold" if phase == "replication"
                    else "consensus_value"
                )
                consensus_items.append({
                    "candidate_id": f"cand-{episode_id}",
                    "episode_id": episode_id,
                    "segment_id": f"seg-{episode_id}",
                    "consensus_state": state,
                    "preferred_atomic_count": 1,
                    "preferred_disposition": (
                        "reject" if state == "consensus_junk"
                        else "hold" if state == "consensus_hold"
                        else "retain"
                    ),
                    "strictly_scoreable": state != "contested",
                    "acceptable_value_states": ["value"],
                    "acceptable_dispositions": ["retain", "reject", "hold"],
                    "acceptable_atomic_counts": [1],
                    "minimum_atomic_count": 1,
                    "maximum_atomic_count": 1,
                    "stable_atomic_count": True,
                    "stable_exact_disposition": True,
                    "decompositions": [],
                })
        preferred = {
            "items": [
                {"candidate_id": row["candidate_id"]}
                for row in consensus_items
            ],
        }
        preferred["gold_sha256"] = opt.true_north.sha256_text(
            opt.true_north.dumps_json(preferred)
        )
        (gold_root / "gold.private.json").write_text(json.dumps(preferred))
        consensus = {
            "items": consensus_items,
            "source_gold_sha256": preferred["gold_sha256"],
        }
        consensus["consensus_sha256"] = opt.true_north.sha256_text(
            opt.true_north.dumps_json(consensus)
        )
        (gold_root / "consensus.private.json").write_text(json.dumps(consensus))
        self.baseline = {factor: {"version": "base"} for factor in opt.FACTORS}
        self.variant = {
            "id": "v1",
            "factor": "system_prompt",
            "baseline": False,
            "changes_only": "system_prompt",
            "hypothesis": "Directives improve precision.",
            "transform": {
                "operation": "append_system_directives",
                "parameters": {"directives": ["Be exact."]},
            },
            "risk": "lower recall",
            "expected_tradeoff": "precision for recall",
        }
        self.fixed = {
            "model": "zai-coding-plan/glm-5.2",
            "temperature": 0.1,
            "steps": 12,
            "timeout_seconds": 60,
            "workers": 1,
            "evaluator_hashes": {"semantic": "a" * 64},
            "reserved_output_tokens": 2_000,
            "max_total_tokens_per_attempt": 10_000,
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _plan(self, **overrides):
        kwargs = {
            "suite_root": self.suite,
            "campaign_root": self.campaigns,
            "campaign_id": "c1",
            "factor": "system_prompt",
            "baseline": self.baseline,
            "variants": [self.variant],
            "fixed_config": self.fixed,
            "budget": opt.Budget(100, 1_000_000, 10_000),
            "replicates": 2,
            "dry_run": False,
        }
        kwargs.update(overrides)
        return opt.plan_campaign(**kwargs)

    def _write_passing_score(self, campaign: Path, phase: str) -> None:
        score = {
            "phase": phase,
            "selection": {
                "eligible_variant_ids": ["v1"],
                "pareto_variant_ids": ["v1"],
            },
        }
        score["score_sha256"] = opt._sha(score)
        opt._immutable_write(campaign / "scores" / f"{phase}.private.json", score)

    def _first_packet_output(self):
        plan = self._plan(replicates=1)
        row = next(item for item in plan["packets"] if item["phase"] == "train")
        packet = json.loads(Path(row["packet_path"]).read_text())
        output = {
            "schema_version": "pif_true_north_atomic_output_v2",
            "items": [{
                "candidate_id": candidate["candidate_id"],
                "disposition": "reject",
                "reason_code": "fixture",
                "atomic_claims": [],
            } for candidate in packet["input"]["candidates"]],
        }
        locator = "/".join((
            row["factor"], row["variant_id"], row["replicate_id"],
            row["fold_id"], row["episode_id"], row["packet_id"],
        ))
        return row, output, locator

    def test_plan_has_unique_replicate_paths_and_no_gold(self):
        plan = self._plan()
        paths = [row["packet_path"] for row in plan["packets"]]
        namespaces = [row["checkpoint_namespace"] for row in plan["packets"]]
        self.assertEqual(len(paths), len(set(paths)))
        self.assertEqual(len(namespaces), len(set(namespaces)))
        self.assertTrue(all("sealed-1" not in path for path in paths))
        for path in paths:
            self.assertNotIn("gold", Path(path).read_text())

    def test_shared_cohort_budget_counts_inflight_factor_reservations(self):
        self._plan(campaign_id="c1")
        self._plan(campaign_id="c2")
        cohort = {
            "schema_version": "pif_true_north_input_optimization_cohort_v1",
            "campaign_ids": ["c1", "c2"],
            "budget": {
                "max_calls": 2,
                "max_tokens": 15_000,
                "max_wall_seconds": 120,
            },
        }
        cohort["cohort_sha256"] = opt._sha(cohort)
        opt._immutable_write(self.campaigns / "cohort.json", cohort)
        first = opt._cohort_budget_reserve(
            campaign_root=self.campaigns / "c1",
            campaign_id="c1",
            locator="factor/v1/r01/train/episode/packet-1",
            calls=1,
            tokens=10_000,
            wall_seconds=60,
        )
        self.assertIsNotNone(first)
        with self.assertRaisesRegex(opt.CampaignError, "shared cohort budget"):
            opt._cohort_budget_reserve(
                campaign_root=self.campaigns / "c2",
                campaign_id="c2",
                locator="factor/v1/r01/train/episode/packet-2",
                calls=1,
                tokens=10_000,
                wall_seconds=60,
            )
        opt._cohort_budget_release(
            campaign_root=self.campaigns / "c1", reservation_id=first
        )
        second = opt._cohort_budget_reserve(
            campaign_root=self.campaigns / "c2",
            campaign_id="c2",
            locator="factor/v1/r01/train/episode/packet-2",
            calls=1,
            tokens=10_000,
            wall_seconds=60,
        )
        self.assertIsNotNone(second)

    def test_reconcile_removes_only_proven_completed_reservations(self):
        self._plan(campaign_id="c1")
        cohort = {
            "schema_version": "pif_true_north_input_optimization_cohort_v1",
            "campaign_ids": ["c1"],
            "budget": {
                "max_calls": 2,
                "max_tokens": 20_000,
                "max_wall_seconds": 120,
            },
        }
        cohort["cohort_sha256"] = opt._sha(cohort)
        opt._immutable_write(self.campaigns / "cohort.json", cohort)
        locator = "factor/v1/r01/train/episode/packet-1"
        reservation = opt._cohort_budget_reserve(
            campaign_root=self.campaigns / "c1",
            campaign_id="c1",
            locator=locator,
            calls=1,
            tokens=10_000,
            wall_seconds=60,
        )
        state_path = self.campaigns / "c1" / "state.json"
        state = json.loads(state_path.read_text())
        state["completed_locators"] = [locator]
        opt._atomic_state_write(state_path, state)
        result = opt.reconcile_completed_cohort_reservations(
            campaign_parent=self.campaigns
        )
        self.assertEqual(result["removed_count"], 1)
        self.assertEqual(result["remaining_count"], 0)
        self.assertIsNotNone(reservation)

    def test_packet_lifetime_attempt_ceiling_survives_process_restarts(self):
        plan = self._plan(replicates=1)
        row = next(item for item in plan["packets"] if item["phase"] == "train")
        output_dir = Path(row["output_dir"])
        for number in range(1, 4):
            opt._immutable_write(
                output_dir / f"failure-call-{number}.private.json",
                {"conservative_attempts_accounted": 1},
            )
        with self.assertRaisesRegex(
            opt.CampaignError, "lifetime attempt ceiling"
        ):
            opt.execute_campaign(
                campaign_dir=self.campaigns / "c1",
                runner=lambda **kwargs: self.fail("runner must not be called"),
                max_packets=1,
            )

    def test_decodable_schema_failure_is_scored_not_retried(self):
        plan = self._plan(replicates=1)
        rows = [item for item in plan["packets"] if item["phase"] == "train"]
        stream = "\n".join(
            [
                json.dumps(
                    {"type": "text", "part": {"text": '{"wrong":"shape"}'}}
                ),
                json.dumps(
                    {
                        "type": "step_finish",
                        "part": {
                            "reason": "stop",
                            "cost": 0,
                            "tokens": {
                                "input": 1,
                                "output": 1,
                                "reasoning": 1,
                                "total": 3,
                                "cache": {"read": 0, "write": 0},
                            },
                        },
                    }
                ),
            ]
        )
        for row in rows:
            output_dir = Path(row["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "attempt-1.private.jsonl").write_text(
                stream, encoding="utf-8"
            )
            (output_dir / "attempt-1.stderr.private.txt").write_text(
                "", encoding="utf-8"
            )
            opt._immutable_write(
                output_dir / "failure-call-1.private.json",
                {
                    "conservative_attempts_accounted": 1,
                    "elapsed_seconds": 1.0,
                },
            )
        result = opt.execute_campaign(
            campaign_dir=self.campaigns / "c1",
            runner=lambda **kwargs: self.fail("invalid answer must not be retried"),
            max_packets=len(rows),
        )
        self.assertEqual(result["phase_packets_complete"], len(rows))
        output_dir = Path(rows[0]["output_dir"])
        terminal = json.loads(
            (output_dir / "campaign-result.private.json").read_text()
        )
        self.assertIsNone(terminal["output"])
        self.assertEqual(
            terminal["terminal_validation_failure"]["kind"],
            "decodable_schema_or_semantic_noncompliance",
        )
        score = opt.score_campaign(campaign_dir=self.campaigns / "c1")
        private = json.loads(
            (
                self.campaigns
                / "c1"
                / "scores"
                / "train.private.json"
            ).read_text()
        )
        variant = private["variants"]["v1"]["aggregate"]
        self.assertEqual(variant["schema_parse_success_rate"], 0.0)
        self.assertNotIn(
            "v1", score["selection_summary"]["eligible_variant_ids"]
        )

    def test_exactly_one_factor_must_change(self):
        bad = dict(self.variant)
        bad["changes_only"] = "candidate_priors"
        with self.assertRaisesRegex(opt.CampaignError, "exactly"):
            self._plan(variants=[bad])

    def test_sealed_holdout_is_rejected(self):
        folds = dict(opt.DEFAULT_FOLDS)
        folds["train"] = ("sealed-1",)
        with self.assertRaisesRegex(opt.CampaignError, "sealed holdout"):
            self._plan(folds=folds)

    def test_unknown_transform_fails_closed(self):
        with self.assertRaisesRegex(opt.CampaignError, "unsupported"):
            self._plan(variants=[{
                **self.variant,
                "id": "bad",
                "transform": {"operation": "magic", "parameters": {}},
            }])

    def test_serialized_identical_variant_arms_are_rejected(self):
        duplicate = {**self.variant, "id": "v2"}
        with self.assertRaisesRegex(opt.CampaignError, "serialized-identical"):
            self._plan(variants=[self.variant, duplicate])

    def test_fixed_runner_rejects_unimplemented_temperature_steps_and_workers(self):
        for change in (
            {"temperature": 0.2},
            {"steps": 13},
            {"workers": 2},
        ):
            fixed = {**self.fixed, **change}
            with self.assertRaises(opt.CampaignError):
                self._plan(fixed_config=fixed)

    def test_immutable_registry_detects_drift(self):
        self._plan()
        changed = dict(self.fixed)
        changed["evaluator_hashes"] = {"semantic": "b" * 64}
        with self.assertRaisesRegex(opt.CampaignError, "immutable artifact differs"):
            self._plan(fixed_config=changed)

    def test_budget_rejects_oversized_plan(self):
        with self.assertRaisesRegex(opt.CampaignError, "worst-case"):
            self._plan(budget=opt.Budget(1, 1000, 60))

    def test_resume_skips_completed_packet_and_never_calls_model_in_test(self):
        plan = self._plan(replicates=1)
        calls = []

        def fake_runner(**kwargs):
            calls.append(str(kwargs["output_dir"]))
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            items = []
            for candidate in packet["input"]["candidates"]:
                items.append({"candidate_id": candidate["candidate_id"], "disposition": "reject", "reason_code": "fixture", "atomic_claims": []})
            return {"schema_version": "pif_true_north_atomic_output_v2", "items": items}, [{"provider_model": self.fixed["model"], "attempt": 1, "usage": {"input_tokens": 10, "output_tokens": 5}, "elapsed_seconds": 0.1}], self.fixed["model"]

        first = opt.execute_campaign(campaign_dir=self.campaigns / "c1", runner=fake_runner, max_packets=1)
        second = opt.resume_campaign(campaign_dir=self.campaigns / "c1", runner=fake_runner, max_packets=1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(first["phase_packets_complete"], 1)
        self.assertEqual(second["phase_packets_complete"], 2)
        self.assertNotEqual(calls[0], calls[1])

    def test_default_runner_receives_campaign_retry_ceiling(self):
        self._plan(replicates=1)
        seen = []

        def fake_default_runner(**kwargs):
            seen.append(kwargs["_semantic_retry_remaining"])
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            output = {
                "schema_version": "pif_true_north_atomic_output_v2",
                "items": [{
                    "candidate_id": candidate["candidate_id"],
                    "disposition": "reject",
                    "reason_code": "fixture",
                    "atomic_claims": [],
                } for candidate in packet["input"]["candidates"]],
            }
            return output, [{
                "provider_model": self.fixed["model"],
                "attempt": 1,
                "usage": {"total_tokens": 1},
            }], self.fixed["model"]

        with patch.object(
            opt.true_north, "_run_opencode_packet", fake_default_runner
        ):
            opt.execute_campaign(
                campaign_dir=self.campaigns / "c1",
                max_packets=1,
            )
        self.assertEqual(
            seen,
            [self.fixed.get("max_attempts_per_packet", 3) - 1],
        )

    def test_checkpoint_reuse_receipt_is_rejected(self):
        self._plan(replicates=1)

        def bad_runner(**kwargs):
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            output = {"schema_version": "pif_true_north_atomic_output_v2", "items": [{"candidate_id": packet["input"]["candidates"][0]["candidate_id"], "disposition": "reject", "reason_code": "fixture", "atomic_claims": []}]}
            return output, [{"provider_model": self.fixed["model"], "attempt": 1, "usage": {"checkpoint_reuse": True}}], self.fixed["model"]

        with self.assertRaisesRegex(opt.CampaignError, "checkpoint reuse"):
            opt.execute_campaign(campaign_dir=self.campaigns / "c1", runner=bad_runner, max_packets=1)

    def test_output_schema_wrapper_normalizes(self):
        packet = {"output_schema": {"x-pif-normalization": {
            "shape": "wrapped",
            "wrapper": "result",
            "aliases": {},
        }}}
        value = {"result": {"schema_version": "x", "items": []}}
        self.assertEqual(opt.normalize_output(value, packet), value["result"])
        with self.assertRaises(opt.CampaignError):
            opt.normalize_output({"wrong": {}}, packet)

    def test_candidate_keyed_alias_schema_normalizes_without_semantic_inference(self):
        canonical = {
            "schema_version": "pif_true_north_atomic_output_v2",
            "items": [{
                "candidate_id": "c1",
                "disposition": "reject",
                "reason_code": "junk",
                "atomic_claims": [],
            }],
        }
        packet = {"output_schema": {"x-pif-normalization": {
            "shape": "candidate_keyed",
            "aliases": {
                "schema_version": "version",
                "items": "records",
                "disposition": "decision",
                "reason_code": "reason",
                "atomic_claims": "claims",
            },
        }}}
        surface = {
            "version": canonical["schema_version"],
            "records": {"c1": {
                "decision": "reject",
                "reason": "junk",
                "claims": [],
            }},
        }
        self.assertEqual(opt.normalize_output(surface, packet), canonical)

    def test_composite_crop_preserves_original_evidence_and_every_embedded_segment(self):
        segment = {
            "segment_id": "s1",
            "segment_index": 2,
            "text_sha256": "a" * 64,
            "text": "x" * 1000,
        }
        packet = {"input": {
            "episode_context": {"composite": True},
            "segment": {"segment_id": "composite"},
            "segments": [{"episode_id": "e1", **segment}],
            "candidates": [
                {"_source_episode_id": "e1", "segment_id": "s1", "evidence_start": 400, "evidence_end": 410},
                {"_source_episode_id": "e1", "segment_id": "s1", "evidence_start": 500, "evidence_end": 510},
            ],
        }}
        opt._apply_context_crop(packet, 128)
        cropped = packet["input"]["segments"][0]
        for candidate, original_start in zip(
            packet["input"]["candidates"], (400, 500)
        ):
            self.assertEqual(cropped["segment_id"], "s1")
            self.assertEqual(cropped["text_sha256"], "a" * 64)
            self.assertEqual(cropped["crop_original_start"], 272)
            self.assertEqual(cropped["crop_original_end"], 638)
            self.assertEqual(candidate["evidence_start"], original_start)

    def test_candidate_projection_preserves_evidence_lineage_and_embedded_sources(self):
        candidate = {
            "candidate_id": "c1",
            "label_id": "l1",
            "segment_id": "s1",
            "segment_index": 0,
            "event_index": 1,
            "evidence_text": "Exact evidence",
            "evidence_start": 10,
            "evidence_end": 24,
            "provenance": {"transcript_id": "t1"},
            "claim_text": "Claim",
            "confidence": 0.8,
            "speaker": {"name": "A", "confidence": 0.9},
            "_source_segment": {"segment_id": "s1"},
            "_source_episode_context": {"speaker_map": []},
        }
        job = {
            "instructions": [],
            "output_schema": {},
            "input": {
                "candidates": [candidate],
                "episode_context": {},
                "segment": {},
            },
        }
        transformed, _ = opt._apply_transform(job, "candidate_priors", {
            "operation": "present_candidate_priors",
            "parameters": {
                "strategy": "candidate_field_projection",
                "keep_fields": ["claim_text", "speaker.confidence"],
            },
        })
        row = transformed["input"]["candidates"][0]
        self.assertEqual(row["evidence_text"], "Exact evidence")
        self.assertEqual(row["provenance"], {"transcript_id": "t1"})
        self.assertEqual(row["_source_segment"], {"segment_id": "s1"})
        self.assertEqual(row["speaker"], {"confidence": 0.9})
        self.assertNotIn("confidence", row)

    def test_candidate_confidence_and_order_transforms_are_deterministic(self):
        rows = []
        for candidate_id, confidence, evidence_start in (
            ("b", 0.2, 20),
            ("a", 0.5, 10),
            ("c", 0.9, 30),
        ):
            rows.append({
                "candidate_id": candidate_id,
                "confidence": confidence,
                "evidence_start": evidence_start,
            })
        base = {
            "instructions": [],
            "output_schema": {},
            "input": {"candidates": rows, "episode_context": {}, "segment": {}},
        }
        bucketed, _ = opt._apply_transform(base, "candidate_priors", {
            "operation": "present_candidate_priors",
            "parameters": {
                "strategy": "confidence_projection",
                "mode": "three_buckets",
                "cut_points": [0.34, 0.67],
            },
        })
        self.assertEqual(
            [row["confidence"] for row in bucketed["input"]["candidates"]],
            ["low", "medium", "high"],
        )
        ordered, _ = opt._apply_transform(base, "candidate_priors", {
            "operation": "present_candidate_priors",
            "parameters": {
                "strategy": "candidate_order",
                "sort_keys": ["evidence_start", "candidate_id"],
                "descending": False,
            },
        })
        self.assertEqual(
            [row["candidate_id"] for row in ordered["input"]["candidates"]],
            ["a", "b", "c"],
        )

    def test_combined_stack_applies_multiple_factors_in_declared_order(self):
        job = {
            "instructions": ["base"],
            "output_schema": opt.true_north.atomic_output_schema([]),
            "input": {
                "candidates": [],
                "episode_context": {
                    "speaker_map": [{"name": "A"}],
                    "section_map": [],
                    "extraction_guidance": ["ground claims"],
                    "unused": "drop me",
                },
                "segment": {},
            },
        }
        packet, system_prompt = opt._apply_transform(
            job,
            opt.COMBINED_FACTOR,
            {
                "operation": "compose_transforms",
                "parameters": {
                    "steps": [
                        {
                            "factor": "system_prompt",
                            "transform": {
                                "operation": "append_system_directives",
                                "parameters": {
                                    "directives": [
                                        "Prefer omission to an unsupported assertion."
                                    ]
                                },
                            },
                        },
                        {
                            "factor": "surrounding_context",
                            "transform": {
                                "operation": "select_context_strategy",
                                "parameters": {
                                    "strategy": "episode_context_fields",
                                    "ordered_fields": [
                                        "speaker_map",
                                        "section_map",
                                        "extraction_guidance",
                                    ],
                                },
                            },
                        },
                    ]
                },
            },
        )
        self.assertIn("Prefer omission", system_prompt)
        self.assertEqual(
            list(packet["input"]["episode_context"]),
            ["speaker_map", "section_map", "extraction_guidance"],
        )

    def test_combined_stack_rejects_duplicate_factors(self):
        step = {
            "factor": "system_prompt",
            "transform": {
                "operation": "append_system_directives",
                "parameters": {"directives": ["Be exact."]},
            },
        }
        with self.assertRaisesRegex(opt.CampaignError, "only once"):
            opt._validate_transform(
                opt.COMBINED_FACTOR,
                {
                    "operation": "compose_transforms",
                    "parameters": {"steps": [step, step]},
                },
            )

    def test_combined_campaign_records_multi_factor_components(self):
        variants = [
            {
                "id": "stack-00-baseline",
                "factor": opt.COMBINED_FACTOR,
                "baseline": True,
                "changes_only": opt.COMBINED_FACTOR,
                "transform": {"operation": "identity", "parameters": {}},
            },
            {
                "id": "stack-01-core",
                "factor": opt.COMBINED_FACTOR,
                "baseline": False,
                "changes_only": opt.COMBINED_FACTOR,
                "transform": {
                    "operation": "compose_transforms",
                    "parameters": {
                        "steps": [
                            {
                                "factor": "system_prompt",
                                "transform": {
                                    "operation": "append_system_directives",
                                    "parameters": {
                                        "directives": ["Be exact."]
                                    },
                                },
                            },
                            {
                                "factor": "surrounding_context",
                                "transform": {
                                    "operation": "select_context_strategy",
                                    "parameters": {
                                        "strategy": "episode_context_fields",
                                        "ordered_fields": ["speaker_map"],
                                    },
                                },
                            },
                        ]
                    },
                },
            },
        ]
        self._plan(
            campaign_id="combined",
            factor=opt.COMBINED_FACTOR,
            variants=variants,
            replicates=1,
        )
        registry = json.loads(
            (self.campaigns / "combined" / "registry.json").read_text()
        )
        stack = next(
            row for row in registry["variants"]
            if row["variant_id"] == "stack-01-core"
        )
        changed = {
            factor for factor in opt.FACTORS
            if stack["components"][factor] != self.baseline[factor]
        }
        self.assertEqual(changed, {"system_prompt", "surrounding_context"})

    def test_locked_packets_are_not_materialized_at_plan_time(self):
        plan = self._plan(replicates=1)
        self.assertFalse(any(row["phase"] == "locked_validation" for row in plan["packets"]))
        self.assertFalse((self.campaigns / "c1" / "phase-plans" / "locked_validation.json").exists())

    def test_state_machine_requires_score(self):
        self._plan(replicates=1)
        with self.assertRaisesRegex(opt.CampaignError, "scored"):
            opt.advance_campaign(campaign_dir=self.campaigns / "c1", selected_variant_id="v1")

    def test_advance_rejects_arbitrary_non_pareto_variant(self):
        self._plan(replicates=1)
        campaign = self.campaigns / "c1"
        score = {
            "phase": "train",
            "selection": {
                "eligible_variant_ids": [],
                "pareto_variant_ids": [],
            },
        }
        score["score_sha256"] = opt._sha(score)
        opt._immutable_write(campaign / "scores" / "train.private.json", score)
        with self.assertRaisesRegex(opt.CampaignError, "Pareto"):
            opt.advance_campaign(campaign_dir=campaign, selected_variant_id="v1")

    def test_semantic_scoring_is_grouped_and_only_train_aggregate_is_public(self):
        self._plan(replicates=1)

        def fake_runner(**kwargs):
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            output = {
                "schema_version": "pif_true_north_atomic_output_v2",
                "items": [{
                    "candidate_id": packet["input"]["candidates"][0]["candidate_id"],
                    "disposition": "reject",
                    "reason_code": "fixture",
                    "atomic_claims": [],
                }],
            }
            return output, [{"provider_model": self.fixed["model"], "attempt": 1, "usage": {"total_tokens": 17}}], self.fixed["model"]

        opt.execute_campaign(campaign_dir=self.campaigns / "c1", runner=fake_runner)
        gold_path = (
            self.suite / "gold" / "development" / "final"
            / "consensus.private.json"
        )
        calls = []

        def scorer(predictions, consensus, preferred):
            calls.append((predictions, consensus, preferred))
            return {"aggregate": {"quality": 0.75}, "candidates": [{"private": True}]}

        score = opt.score_campaign(
            campaign_dir=self.campaigns / "c1",
            scorer=scorer,
            gold_path=gold_path,
        )
        self.assertEqual(len(calls), 1)  # one composite replicate/fold scope
        self.assertEqual(score["variants"]["v1"]["aggregate_metrics"]["quality"], 0.75)
        self.assertEqual(score["selection_summary"]["eligible_variant_ids"], [])
        public_text = (self.campaigns / "c1" / "feedback" / "train.aggregate.json").read_text()
        self.assertNotIn('"private"', public_text)
        self.assertEqual(opt.status_campaign(campaign_dir=self.campaigns / "c1")["usage"]["tokens"], 34)

    def test_import_requires_surface_hash_model_and_positive_receipt(self):
        row, output, locator = self._first_packet_output()
        with self.assertRaisesRegex(opt.CampaignError, "provider model"):
            opt.import_result(
                campaign_dir=self.campaigns / "c1",
                locator=locator,
                output=output,
                receipts=[{
                    "provider_model": "wrong/model",
                    "attempt": 1,
                    "usage": {},
                }],
            )
        result = opt.import_result(
            campaign_dir=self.campaigns / "c1",
            locator=locator,
            output=output,
            receipts=[{
                "provider_model": self.fixed["model"],
                "attempt": 1,
                "usage": {"total_tokens": 10},
                "elapsed_seconds": 0.1,
            }],
        )
        self.assertEqual(result["packet_sha256"], row["packet_sha256"])

    def test_import_rejects_invalid_receipt_numbers_without_persistence(self):
        row, output, locator = self._first_packet_output()
        invalid_receipts = (
            {"attempt": 0, "usage": {"total_tokens": 1}},
            {"attempt": 1, "elapsed_seconds": -0.1, "usage": {"total_tokens": 1}},
            {"attempt": 1, "elapsed_seconds": math.nan, "usage": {"total_tokens": 1}},
            {"attempt": 1, "usage": {"total_tokens": -1}},
            {"attempt": 1, "usage": {"total_tokens": math.inf}},
            {"attempt": 1, "usage": {"total_tokens": 1, "estimated_cost": -0.1}},
        )
        state_path = self.campaigns / "c1" / "state.json"
        original_state = state_path.read_bytes()
        result_path = Path(row["output_dir"]) / "campaign-result.private.json"
        for receipt in invalid_receipts:
            with self.subTest(receipt=receipt):
                with self.assertRaises(opt.CampaignError):
                    opt.import_result(
                        campaign_dir=self.campaigns / "c1",
                        locator=locator,
                        output=output,
                        receipts=[{
                            "provider_model": self.fixed["model"],
                            **receipt,
                        }],
                    )
                self.assertFalse(result_path.exists())
                self.assertEqual(state_path.read_bytes(), original_state)

    def test_import_preflights_actual_usage_before_persistence(self):
        row, output, locator = self._first_packet_output(
        )
        campaign = self.campaigns / "c1"
        state_path = campaign / "state.json"
        state = json.loads(state_path.read_text())
        state["usage"]["tokens"] = 999_999
        opt._atomic_state_write(state_path, state)
        original_state = state_path.read_bytes()
        with self.assertRaisesRegex(opt.CampaignError, "remaining campaign budget"):
            opt.import_result(
                campaign_dir=campaign,
                locator=locator,
                output=output,
                receipts=[{
                    "provider_model": self.fixed["model"],
                    "attempt": 1,
                    "usage": {"total_tokens": 2},
                }],
            )
        self.assertFalse(
            (Path(row["output_dir"]) / "campaign-result.private.json").exists()
        )
        self.assertEqual(state_path.read_bytes(), original_state)

    def test_failed_runner_attempts_are_durably_accounted(self):
        self._plan(replicates=1)

        def failed(**kwargs):
            raise RuntimeError("provider failed after attempts")

        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            opt.execute_campaign(
                campaign_dir=self.campaigns / "c1",
                runner=failed,
                max_packets=1,
            )
        state = json.loads((self.campaigns / "c1" / "state.json").read_text())
        self.assertEqual(
            state["usage"]["calls"],
            self.fixed.get("max_attempts_per_packet", 3),
        )
        self.assertEqual(
            state["usage"]["tokens"],
            self.fixed["max_total_tokens_per_attempt"]
            * self.fixed.get("max_attempts_per_packet", 3),
        )
        self.assertTrue(list((self.campaigns / "c1" / "outputs").glob(
            "**/failure-*.private.json"
        )))

    def test_presentation_hash_distinguishes_property_order(self):
        left = {"a": 1, "b": 2}
        right = {"b": 2, "a": 1}
        self.assertEqual(opt._sha(left), opt._sha(right))
        self.assertNotEqual(opt._presentation_sha(left), opt._presentation_sha(right))

    def test_advance_unlocks_lockbox_only_after_replication_score(self):
        self._plan(replicates=1)
        campaign = self.campaigns / "c1"
        self._write_passing_score(campaign, "train")
        first = opt.advance_campaign(campaign_dir=campaign, selected_variant_id="v1")
        self.assertEqual(first["phase"], "replication")
        self.assertFalse((campaign / "phase-plans" / "locked_validation.json").exists())
        self._write_passing_score(campaign, "replication")
        second = opt.advance_campaign(campaign_dir=campaign, selected_variant_id="v1")
        self.assertEqual(second["phase"], "locked_validation")
        self.assertTrue((campaign / "phase-plans" / "locked_validation.json").is_file())

    def test_state_hash_drift_and_active_lease_fail_closed(self):
        self._plan(replicates=1)
        campaign = self.campaigns / "c1"
        state_path = campaign / "state.json"
        state = json.loads(state_path.read_text())
        state["phase"] = "replication"
        state_path.write_text(json.dumps(state))
        with self.assertRaisesRegex(opt.CampaignError, "state hash"):
            opt.status_campaign(campaign_dir=campaign)

        # Restore by building a fresh campaign and hold its execution lease.
        self._plan(campaign_id="c2", replicates=1)
        second = self.campaigns / "c2"
        with opt._exclusive_lease(second / ".campaign-state.lock"):
            with self.assertRaisesRegex(opt.CampaignError, "active lease"):
                opt.execute_campaign(campaign_dir=second, runner=lambda **_: None)

    def test_lease_heartbeat_prevents_short_ttl_reclamation(self):
        lease = self.root / "short-lived.lock"
        entered = threading.Event()
        release = threading.Event()

        def hold_lease():
            with opt._exclusive_lease(lease, ttl_seconds=0.15):
                entered.set()
                release.wait(2)

        thread = threading.Thread(target=hold_lease)
        thread.start()
        self.assertTrue(entered.wait(1))
        time.sleep(0.35)
        with self.assertRaisesRegex(opt.CampaignError, "active lease"):
            with opt._exclusive_lease(lease, ttl_seconds=0.15):
                pass
        release.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(lease.exists())

    def test_gold_tamper_is_detected_after_planning(self):
        self._plan(replicates=1)
        consensus_path = (
            self.suite / "gold" / "development" / "final"
            / "consensus.private.json"
        )
        consensus = json.loads(consensus_path.read_text())
        consensus["items"][0]["preferred_atomic_count"] = 99
        consensus_path.write_text(json.dumps(consensus))
        with self.assertRaisesRegex(opt.CampaignError, "gold"):
            opt.status_campaign(campaign_dir=self.campaigns / "c1")

    def test_score_rejects_tampered_result_hash(self):
        self._plan(replicates=1)

        def fake_runner(**kwargs):
            packet = json.loads(Path(kwargs["packet_path"]).read_text())
            output = {
                "schema_version": "pif_true_north_atomic_output_v2",
                "items": [{
                    "candidate_id": candidate["candidate_id"],
                    "disposition": "reject",
                    "reason_code": "fixture",
                    "atomic_claims": [],
                } for candidate in packet["input"]["candidates"]],
            }
            return output, [{
                "provider_model": self.fixed["model"],
                "attempt": 1,
                "usage": {"total_tokens": 1},
            }], self.fixed["model"]

        opt.execute_campaign(campaign_dir=self.campaigns / "c1", runner=fake_runner)
        result_path = next(
            (self.campaigns / "c1" / "outputs").glob(
                "**/campaign-result.private.json"
            )
        )
        result = json.loads(result_path.read_text())
        result["output_sha256"] = "0" * 64
        result_path.write_text(json.dumps(result))
        with self.assertRaisesRegex(opt.CampaignError, "identity or hash"):
            opt.score_campaign(
                campaign_dir=self.campaigns / "c1",
                scorer=lambda *_: {"aggregate": {}},
            )

    def test_output_schema_metadata_is_not_a_top_level_protocol_field(self):
        job = {
            "instructions": [],
            "output_schema": opt.true_north.atomic_output_schema([]),
            "input": {"candidates": [], "episode_context": {}, "segment": {}},
        }
        packet, _ = opt._apply_transform(job, "output_schema", {
            "operation": "emit_output_schema",
            "parameters": {
                "shape": "wrapped",
                "wrapper": "result",
                "aliases": {},
                "normalization_contract": "canonical_atomic_output_v1",
                "schema_style": "reversible_wrapper",
                "required_fields": [
                    "schema_version", "items", "candidate_id", "disposition",
                    "reason_code", "atomic_claims",
                ],
                "root_order": ["schema_version", "items"],
                "item_order": [
                    "candidate_id", "disposition", "reason_code",
                    "atomic_claims",
                ],
                "descriptions": False,
            },
        })
        self.assertNotIn("_canonical_output_normalization", packet)
        self.assertIn("x-pif-normalization", packet["output_schema"])

    def test_execution_rejects_packet_that_cannot_fit_reserved_output(self):
        fixed = {
            **self.fixed,
            "reserved_output_tokens": 9_900,
            "max_total_tokens_per_attempt": 10_000,
        }
        self._plan(replicates=1, fixed_config=fixed)
        calls = []
        with self.assertRaisesRegex(opt.CampaignError, "reserved output"):
            opt.execute_campaign(
                campaign_dir=self.campaigns / "c1",
                runner=lambda **kwargs: calls.append(kwargs),
                max_packets=1,
            )
        self.assertEqual(calls, [])

    def test_replanning_does_not_reset_completed_state(self):
        self._plan(replicates=1)
        campaign = self.campaigns / "c1"
        state = json.loads((campaign / "state.json").read_text())
        state["completed_locators"] = ["already-paid"]
        opt._atomic_state_write(campaign / "state.json", state)
        self._plan(replicates=1)
        replayed = json.loads((campaign / "state.json").read_text())
        self.assertEqual(replayed["completed_locators"], ["already-paid"])

    def test_live_composites_are_stratified_small_and_truth_free(self):
        live = (
            Path.home() / "Library" / "Application Support"
            / "Podcast Intelligence Factory" / "true-north" / "ai-safety-v1"
        )
        if not (live / "manifest.json").is_file():
            self.skipTest("local true-north suite is unavailable")
        manifest = json.loads((live / "manifest.json").read_text())
        development, _ = opt._validate_manifest(manifest)
        train_ids, train_counts = opt._screening_selection(
            live, opt.DEFAULT_FOLDS["train"], phase="train"
        )
        replication_ids, replication_counts = opt._screening_selection(
            live, opt.DEFAULT_FOLDS["replication"], phase="replication"
        )
        self.assertEqual(train_counts, {
            "selected": 60, "junk": 9, "hold": 0, "contested": 8, "value": 43,
        })
        self.assertEqual(replication_counts, {
            "selected": 60, "junk": 9, "hold": 4, "contested": 8, "value": 39,
        })
        for phase, episode_ids, selected_ids in (
            ("train", opt.DEFAULT_FOLDS["train"], train_ids),
            ("replication", opt.DEFAULT_FOLDS["replication"], replication_ids),
        ):
            jobs = opt._composite_jobs(
                development,
                episode_ids,
                selected_candidate_ids=selected_ids,
            )
            self.assertEqual(len(jobs), 2, phase)
            self.assertTrue(all(len(opt._serialized(job)) <= 150_000 for job in jobs))
            self.assertEqual(
                sum(len(job["input"]["candidates"]) for job in jobs), 60
            )
            for job in jobs:
                keys = []
                stack = [job]
                while stack:
                    value = stack.pop()
                    if isinstance(value, dict):
                        keys.extend(str(key).lower() for key in value)
                        stack.extend(value.values())
                    elif isinstance(value, list):
                        stack.extend(value)
                self.assertFalse(any("gold" in key or "consensus" in key for key in keys))

    def test_module_never_uses_sqlite_or_production_path(self):
        source = Path(opt.__file__).read_text()
        self.assertNotIn("sqlite3", source)
        self.assertNotIn("db_path(", source)
        self.assertIn('"production_database_open_allowed": False', source)


if __name__ == "__main__":
    unittest.main()
