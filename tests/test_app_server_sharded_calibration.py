from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_llm_judge import (
    JudgeAttemptFailed,
    build_judge_variants,
    make_v2_calibration_pool,
)
from research_factory.app_server_sharded_calibration import (
    EXPECTED_FIXTURE_CASES,
    MAX_OUTPUT_SCHEMA_BYTES,
    MAX_PROMPT_BYTES,
    SHARDED_CALIBRATION_VERSION,
    ShardedCalibrationError,
    balanced_shard_sizes,
    build_calibration_shard_plan,
    run_app_server_sharded_judge_calibration,
)


class DummyPersistentClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class CalibrationShardPlanningTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pool, self.mapping, self.expected = make_v2_calibration_pool()

    def test_66_cases_are_covered_once_in_bounded_deterministic_shards(self) -> None:
        plan, shards = build_calibration_shard_plan(
            pool=self.pool,
            mapping=self.mapping,
            expected=self.expected,
        )

        self.assertEqual(plan["case_count"], EXPECTED_FIXTURE_CASES)
        self.assertEqual(plan["shard_sizes"], [6] * 11)
        self.assertEqual(balanced_shard_sizes(66), plan["shard_sizes"])
        observed = [case_id for item in plan["shards"] for case_id in item["case_ids"]]
        expected_ids = [case["case_id"] for case in self.pool["cases"]]
        self.assertEqual(observed, expected_ids)
        self.assertEqual(len(set(observed)), 66)
        self.assertEqual(sum(item["same_side_case_count"] for item in plan["shards"]), 6)
        self.assertTrue(plan["ab_ba_membership_and_order_identical"])
        for item, shard in zip(plan["shards"], shards):
            self.assertGreaterEqual(item["case_count"], 6)
            self.assertEqual(item["case_count"], 6)
            variants = build_judge_variants(shard["pool"])
            ab_ids = [case["case_id"] for case in variants["ab"]["cases"]]
            ba_ids = [case["case_id"] for case in variants["ba"]["cases"]]
            self.assertEqual(ab_ids, item["case_ids"])
            self.assertEqual(ba_ids, item["case_ids"])
            for envelope in item["envelopes"].values():
                self.assertLessEqual(envelope["prompt_bytes"], MAX_PROMPT_BYTES)
                self.assertLessEqual(
                    envelope["output_schema_bytes"], MAX_OUTPUT_SCHEMA_BYTES
                )

    def test_request_size_caps_fail_before_transport(self) -> None:
        with self.assertRaisesRegex(ShardedCalibrationError, "byte cap"):
            build_calibration_shard_plan(
                pool=self.pool,
                mapping=self.mapping,
                expected=self.expected,
                prompt_byte_cap=1,
            )
        with self.assertRaisesRegex(ShardedCalibrationError, "byte cap"):
            build_calibration_shard_plan(
                pool=self.pool,
                mapping=self.mapping,
                expected=self.expected,
                output_schema_byte_cap=1,
            )


class CalibrationShardFailureTest(unittest.IsolatedAsyncioTestCase):
    async def test_all_shards_aggregate_only_after_complete_ab_and_ba(self) -> None:
        calls = []

        async def perfect_runner(**kwargs):
            calls.append(kwargs)
            pool_path = Path(kwargs["pool_path"])
            judge_root = Path(kwargs["output_dir"])
            pool = json.loads(pool_path.read_text(encoding="utf-8"))
            expected = json.loads(
                (pool_path.parent / "expected.private.json").read_text(encoding="utf-8")
            )
            variants = build_judge_variants(pool)
            for name in ("ab", "ba"):
                variant = variants[name]
                rows = []
                for case in variant["cases"]:
                    truth = expected["cases"][case["case_id"]]
                    witness_by_id = {
                        witness["witness_id"]: witness
                        for side in ("a", "b")
                        for witness in case["event_set_%s" % side]
                    }
                    support = []
                    for witness_id, verdict in truth["support"].items():
                        support.append(
                            {
                                "witness_id": witness_id,
                                "verdict": verdict,
                                "evidence_spans": (
                                    [witness_by_id[witness_id]["event"]["evidence"]]
                                    if verdict == "supported"
                                    else []
                                ),
                                "rationale": "Frozen fixture truth is reproduced exactly.",
                            }
                        )
                    alignments = []
                    used_left = set()
                    used_right = set()
                    for pair in truth["pairs"]:
                        left_id = pair["left_witness_id"]
                        right_id = pair["right_witness_id"]
                        if name == "ba":
                            left_id, right_id = right_id, left_id
                        alignments.append(
                            {
                                "left_witness_id": left_id,
                                "right_witness_id": right_id,
                                "relation": pair["relation"],
                                "mismatch_fields": pair["mismatch_fields"],
                                "rationale": "Frozen fixture alignment is reproduced exactly.",
                            }
                        )
                        used_left.add(left_id)
                        used_right.add(right_id)
                    left_ids = [item["witness_id"] for item in case["event_set_a"]]
                    right_ids = [item["witness_id"] for item in case["event_set_b"]]
                    rows.append(
                        {
                            "case_id": case["case_id"],
                            "support_results": support,
                            "equivalence_groups": [
                                {
                                    "witness_ids": group,
                                    "rationale": "Frozen fixture partition is reproduced exactly.",
                                }
                                for group in truth["equivalence_groups"]
                            ],
                            "alignments": alignments,
                            "unaligned_left_witness_ids": [
                                item for item in left_ids if item not in used_left
                            ],
                            "unaligned_right_witness_ids": [
                                item for item in right_ids if item not in used_right
                            ],
                        }
                    )
                output = {"cases": rows}
                output_path = judge_root / ("output-%s.private.json" % name)
                sidecar_path = judge_root / "sidecars" / ("%s.json" % name)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(json.dumps(output, sort_keys=True) + "\n", encoding="utf-8")
                sidecar_path.write_text(
                    json.dumps(
                        {
                            "state": "completed",
                            "status": "completed",
                            "usage_complete": True,
                            "usage_status": "complete",
                            "usage": {
                                "input_tokens": 10,
                                "cached_input_tokens": 0,
                                "output_tokens": 2,
                                "reasoning_output_tokens": 1,
                                "total_tokens": 12,
                            },
                            "recovery_reran_model": False,
                        },
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (judge_root / "sidecars" / ("%s.capacity.json" % name)).write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_app_server_capacity_checkpoint_v1",
                            "checked_at": "2026-07-12T00:00:00+00:00",
                            "limit_id": "codex",
                            "primary_used_percent": 0,
                            "primary_resets_at": None,
                            "rate_limit_reached_type": None,
                            "maximum_primary_used_percent": 20,
                            "cleared_for_semantic_turn": True,
                            "managed_chatgpt_auth_verified": True,
                            "plan_type": "pro",
                            "thread_started": False,
                            "turn_started": False,
                            "sidecar_started": False,
                            "retry_checkpoint_reuse_allowed": False,
                            "privacy": "capacity_status_only_no_prompt_output_email_credentials_or_thread_ids",
                        },
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
            usage = {
                "input_tokens": 20,
                "cached_input_tokens": 0,
                "output_tokens": 4,
                "reasoning_output_tokens": 2,
                "total_tokens": 24,
            }
            report = {"accounting_complete": True, "usage": usage}
            (judge_root / "report.json").write_text(
                json.dumps(report, sort_keys=True) + "\n", encoding="utf-8"
            )
            return report

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "calibration"
            report = await run_app_server_sharded_judge_calibration(
                output_dir=root,
                client_factory=DummyPersistentClient,
                judge_runner=perfect_runner,
            )
            self.assertTrue(report["calibrated"])
            self.assertTrue(report["aggregate_authorized"])
            self.assertTrue(report["accounting_complete"])
            self.assertEqual(report["required_shard_count"], 11)
            self.assertEqual(report["completed_shard_count"], 11)
            self.assertEqual(report["required_turn_count"], 22)
            self.assertEqual(report["completed_turn_count"], 22)
            self.assertEqual(report["usage"]["total_tokens"], 264)
            self.assertEqual(len(calls), 11)
            self.assertTrue((root / "merged-output-ab.private.json").is_file())
            self.assertTrue((root / "merged-output-ba.private.json").is_file())

    async def test_failed_ab_is_immutable_and_never_replayed(self) -> None:
        calls = []

        async def failed_runner(**kwargs):
            calls.append(kwargs)
            raise JudgeAttemptFailed(
                variant="ab",
                error_class="turn_failed",
                sidecar_path=Path(kwargs["output_dir"]) / "sidecars/ab.json",
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "calibration"
            report = await run_app_server_sharded_judge_calibration(
                output_dir=root,
                client_factory=DummyPersistentClient,
                judge_runner=failed_runner,
            )
            self.assertEqual(report["schema_version"], SHARDED_CALIBRATION_VERSION)
            self.assertFalse(report["calibrated"])
            self.assertEqual(
                report["fail_closed_reason"],
                "infrastructure_or_judge_attempt_failed",
            )
            self.assertEqual(report["failed_shard_id"], "calibration-shard-000")
            self.assertEqual(report["failed_variant"], "ab")
            self.assertEqual(report["usage_status"], "unknown")
            self.assertIsNone(report["usage"])
            self.assertFalse(report["aggregate_authorized"])
            self.assertEqual(len(calls), 1)
            self.assertFalse(
                (root / "shards/calibration-shard-001/judge/judge-spec.json").exists()
            )

            async def forbidden_runner(**_kwargs):
                self.fail("a terminal failed shard must never be replayed")

            def forbidden_client():
                self.fail("a terminal calibration version must not reopen transport")

            resumed = await run_app_server_sharded_judge_calibration(
                output_dir=root,
                client_factory=forbidden_client,
                judge_runner=forbidden_runner,
            )
            self.assertEqual(resumed, report)
            self.assertEqual(len(calls), 1)

    async def test_unknown_usage_shard_blocks_before_output_aggregation(self) -> None:
        calls = []

        async def unknown_usage_runner(**kwargs):
            calls.append(kwargs)
            return {"accounting_complete": False, "usage": None}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "calibration"
            report = await run_app_server_sharded_judge_calibration(
                output_dir=root,
                client_factory=DummyPersistentClient,
                judge_runner=unknown_usage_runner,
            )
            self.assertFalse(report["calibrated"])
            self.assertEqual(report["error_class"], "incomplete_or_unknown_usage")
            self.assertEqual(report["completed_shard_count"], 0)
            self.assertFalse(report["aggregate_authorized"])
            self.assertEqual(len(calls), 1)
            self.assertFalse((root / "merged-output-ab.private.json").exists())
            self.assertFalse((root / "merged-output-ba.private.json").exists())

    async def test_server_overloaded_is_external_infrastructure_with_unknown_usage(self) -> None:
        calls = []

        async def overloaded_runner(**kwargs):
            calls.append(kwargs)
            sidecar_path = Path(kwargs["output_dir"]) / "sidecars/ba.json"
            sidecar_path.parent.mkdir(parents=True, exist_ok=True)
            sidecar_path.write_text(
                json.dumps(
                    {
                        "state": "failed",
                        "status": "failed",
                        "usage_complete": False,
                        "usage_status": "unknown",
                        "usage": None,
                        "turn_error": {"codex_error_info": "serverOverloaded"},
                    },
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            raise JudgeAttemptFailed(
                variant="ba",
                error_class="turn_failed",
                sidecar_path=sidecar_path,
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "calibration"
            report = await run_app_server_sharded_judge_calibration(
                output_dir=root,
                client_factory=DummyPersistentClient,
                judge_runner=overloaded_runner,
            )
            self.assertEqual(len(calls), 1)
            self.assertFalse(report["calibrated"])
            self.assertEqual(
                report["terminal_classification"],
                "infrastructure_or_judge_attempt_failed",
            )
            self.assertEqual(report["failure_origin"], "external_provider")
            self.assertEqual(report["provider_error_code"], "serverOverloaded")
            self.assertEqual(report["usage_status"], "unknown")
            self.assertIsNone(report["usage"])
            failure = json.loads(
                (root / "shards/calibration-shard-000/failure.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(failure["failure_origin"], "external_provider")
            self.assertEqual(failure["provider_error_code"], "serverOverloaded")
            self.assertFalse(failure["retry_allowed"])


if __name__ == "__main__":
    unittest.main()
