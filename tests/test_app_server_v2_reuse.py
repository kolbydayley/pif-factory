from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_dev_selection import (
    INTERRUPTED_SELECTABLE_ARMS,
    load_preassembled_dev_shared_witness_pool,
)
from research_factory.app_server_v2_reuse import (
    PIPELINE_V4_REUSE_CONTRACT_VERSION,
    PIPELINE_V3_REUSE_CONTRACT_VERSION,
    REUSE_CONTRACT_VERSION,
    ReuseContractError,
    build_v2_reuse_contract,
    build_v3_reuse_contract,
    build_v4_reuse_contract,
    verify_v2_reuse_contract,
    verify_v3_reuse_contract,
    verify_v4_reuse_contract,
)


class PipelineV2ReuseContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        cls.v1_root = (
            cls.repo / "work/app-server-development-v2/unattended-pipeline-v1"
        )
        if not (cls.v1_root / "pipeline-terminal.json").is_file():
            raise unittest.SkipTest("immutable local pipeline-v1 evidence is unavailable")

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.output = Path(self.tempdir.name) / "unattended-pipeline-v2" / "reuse.json"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_contract_freezes_failed_ab_absent_ba_and_exact_five_arm_reuse(self) -> None:
        contract = build_v2_reuse_contract(
            repo_root=self.repo,
            output_path=self.output,
        )

        self.assertEqual(contract["schema_version"], REUSE_CONTRACT_VERSION)
        self.assertFalse(contract["policy"]["pipeline_v1_replay_allowed"])
        self.assertFalse(contract["policy"]["extraction_model_calls_allowed"])
        incident = contract["v1_failed_calibration_incident"]
        self.assertEqual(incident["classification"], "infrastructure_or_judge_attempt_failed")
        self.assertFalse(incident["quality_measured"])
        self.assertFalse(incident["cost_measured"])
        self.assertEqual(incident["prompt_bytes"], 190840)
        self.assertEqual(incident["output_schema_bytes"], 38080)
        self.assertEqual(incident["wall_elapsed_seconds"], 3.574)
        self.assertEqual(incident["usage_status"], "unknown")
        self.assertEqual(incident["ba_status"], "not_started")
        self.assertEqual(len(contract["clean_arms"]), 5)
        self.assertTrue(all(not Path(item["path"]).exists() for item in contract["must_remain_absent"]))
        self.assertEqual(verify_v2_reuse_contract(self.output), contract)

    def test_version_isolation_rejects_contract_inside_pipeline_v1(self) -> None:
        forbidden = self.v1_root / "forbidden-v2-reuse-contract-test.json"
        self.assertFalse(forbidden.exists())
        with self.assertRaisesRegex(ReuseContractError, "inside pipeline-v1"):
            build_v2_reuse_contract(repo_root=self.repo, output_path=forbidden)
        self.assertFalse(forbidden.exists())

    def test_contract_tampering_fails_closed(self) -> None:
        build_v2_reuse_contract(repo_root=self.repo, output_path=self.output)
        payload = json.loads(self.output.read_text(encoding="utf-8"))
        payload["policy"]["failed_ab_retry_allowed"] = True
        self.output.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ReuseContractError, "replay policy"):
            verify_v2_reuse_contract(self.output)

    def test_preassembled_pool_is_adopted_without_database_label_reads(self) -> None:
        contract = build_v2_reuse_contract(repo_root=self.repo, output_path=self.output)
        inputs = contract["selection_inputs"]
        arm_paths = [Path(item["report"]["path"]) for item in contract["clean_arms"]]
        # The loader receives no connection and therefore cannot rebuild from
        # mutable labels. It verifies only the frozen manifest and artifact set.
        assembled = load_preassembled_dev_shared_witness_pool(
            manifest_path=Path(inputs["development_manifest"]["path"]),
            witness_root=Path(inputs["preassembled_assembly_report"]["path"]).parent,
            arm_report_paths=arm_paths,
            interrupted_arm_provenance_path=Path(
                contract["interrupted_batch_5_same_thread"]["path"]
            ),
        )
        self.assertTrue(assembled["reused_preassembled_witness_pool"])
        self.assertEqual(set(assembled["arms"]), set(INTERRUPTED_SELECTABLE_ARMS))
        self.assertEqual(assembled["assembly"]["matrix_mode"], "five_clean_plus_terminal_interrupted")
        self.assertFalse(assembled["interrupted_arm"]["selection_eligible"])

    def test_v3_contract_preserves_failed_v2_shard_and_forbids_its_retry(self) -> None:
        output = Path(self.tempdir.name) / "unattended-pipeline-v3/reuse-contract-v2.json"
        contract = build_v3_reuse_contract(
            repo_root=self.repo,
            output_path=output,
        )
        self.assertEqual(
            contract["schema_version"], PIPELINE_V3_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["pipeline_v1_replay_allowed"])
        self.assertFalse(contract["policy"]["pipeline_v2_replay_allowed"])
        self.assertFalse(contract["policy"]["failed_v2_shard_retry_allowed"])
        incident = contract["v2_failed_sharded_calibration_incident"]
        self.assertEqual(incident["failed_shard_id"], "calibration-shard-000")
        self.assertEqual(incident["failed_variant"], "ab")
        self.assertEqual(incident["usage_status"], "unknown")
        self.assertEqual(incident["ba_status"], "not_started")
        self.assertEqual(incident["later_shards_started"], 0)
        self.assertTrue(
            all(not Path(item["path"]).exists() for item in contract["v2_must_remain_absent"])
        )
        self.assertEqual(verify_v3_reuse_contract(output), contract)

    def test_v3_contract_cannot_be_created_inside_failed_pipeline_v2(self) -> None:
        v2 = self.repo / "work/app-server-development-v2/unattended-pipeline-v2"
        forbidden = v2 / "forbidden-v3-reuse-contract-test.json"
        self.assertFalse(forbidden.exists())
        with self.assertRaisesRegex(ReuseContractError, "inside pipeline-v2"):
            build_v3_reuse_contract(repo_root=self.repo, output_path=forbidden)
        self.assertFalse(forbidden.exists())

    def test_v4_contract_freezes_v3_overload_and_requires_full_fresh_calibration(self) -> None:
        output = Path(self.tempdir.name) / "unattended-pipeline-v4/reuse-contract-v3.json"
        contract = build_v4_reuse_contract(
            repo_root=self.repo,
            output_path=output,
        )
        self.assertEqual(
            contract["schema_version"], PIPELINE_V4_REUSE_CONTRACT_VERSION
        )
        policy = contract["policy"]
        self.assertFalse(policy["pipeline_v1_replay_allowed"])
        self.assertFalse(policy["pipeline_v2_replay_allowed"])
        self.assertFalse(policy["pipeline_v3_replay_allowed"])
        self.assertFalse(policy["pipeline_v3_partial_calibration_scoring_allowed"])
        self.assertTrue(policy["full_fresh_v4_calibration_required"])
        incident = contract["v3_failed_sharded_calibration_incident"]
        self.assertEqual(incident["provider_error_code"], "serverOverloaded")
        self.assertEqual(incident["failure_origin"], "external_provider")
        self.assertEqual(incident["completed_measured_turns"], 5)
        self.assertEqual(incident["failed_unknown_usage_turns"], 1)
        self.assertEqual(incident["usage_status"], "unknown")
        self.assertIsNone(incident["whole_version_usage"])
        self.assertFalse(incident["aggregate_authorized"])
        self.assertFalse(incident["retry_allowed"])
        self.assertEqual(len(contract["v3_completed_attempts"]), 5)
        self.assertEqual(len(contract["v3_request_envelopes"]), 9)
        self.assertTrue(
            all(
                not Path(item["path"]).exists()
                for item in contract["v3_must_remain_absent"]
            )
        )
        v4 = contract["v4_calibration_contract"]
        self.assertEqual(v4["case_count_per_shard"], 6)
        self.assertEqual(v4["required_shard_count"], 11)
        self.assertEqual(v4["required_ab_ba_turn_count"], 22)
        self.assertEqual(verify_v4_reuse_contract(output), contract)

    def test_v4_contract_tampering_and_predecessor_overlap_fail_closed(self) -> None:
        output = Path(self.tempdir.name) / "unattended-pipeline-v4/reuse-contract-v3.json"
        build_v4_reuse_contract(repo_root=self.repo, output_path=output)
        payload = json.loads(output.read_text(encoding="utf-8"))
        payload["policy"]["failed_v3_shard_retry_allowed"] = True
        output.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ReuseContractError, "replay policy"):
            verify_v4_reuse_contract(output)

        v3 = self.repo / "work/app-server-development-v2/unattended-pipeline-v3"
        forbidden = v3 / "forbidden-v4-reuse-contract-test.json"
        self.assertFalse(forbidden.exists())
        with self.assertRaisesRegex(ReuseContractError, "immutable predecessor"):
            build_v4_reuse_contract(repo_root=self.repo, output_path=forbidden)
        self.assertFalse(forbidden.exists())


if __name__ == "__main__":
    unittest.main()
