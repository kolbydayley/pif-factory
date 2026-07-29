from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5_diagnostic_audit import (
    DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION,
    build_diagnostic_v1_truth_audit_receipt,
)
from research_factory.app_server_judge_v5_diagnostic_v2_audit import (
    DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION,
    build_diagnostic_v2_attempt_audit_receipt,
)
from research_factory.app_server_judge_v5_diagnostic_v3_audit import (
    DIAGNOSTIC_V3_QUALITY_AUDIT_RECEIPT_VERSION,
    build_diagnostic_v3_quality_audit_receipt,
)
from research_factory.app_server_judge_v5_diagnostic_v4_audit import (
    AUDIT_RECEIPT_VERSION as DIAGNOSTIC_V4_FIXTURE_AUDIT_RECEIPT_VERSION,
    build_diagnostic_v4_fixture_audit_receipt,
)
from research_factory.app_server_judge_v5_diagnostic_v5_audit import (
    AUDIT_RECEIPT_VERSION as DIAGNOSTIC_V5_QUALITY_AUDIT_RECEIPT_VERSION,
    build_diagnostic_v5_quality_audit_receipt,
)
from research_factory.app_server_v5_diagnostic_reuse import (
    DIAGNOSTIC_V2_REUSE_CONTRACT_VERSION,
    verify_diagnostic_v2_reuse_contract,
)
from research_factory.app_server_v5_diagnostic_v3_reuse import (
    DIAGNOSTIC_V3_REUSE_CONTRACT_VERSION,
    verify_diagnostic_v3_reuse_contract,
)
from research_factory.app_server_v5_diagnostic_v4_reuse import (
    DIAGNOSTIC_V4_REUSE_CONTRACT_VERSION,
    verify_diagnostic_v4_reuse_contract,
)
from research_factory.app_server_v5_diagnostic_v5_reuse import (
    DIAGNOSTIC_V5_REUSE_CONTRACT_VERSION,
    verify_diagnostic_v5_reuse_contract,
)
from research_factory.app_server_v5_diagnostic_v6_reuse import (
    DIAGNOSTIC_V6_REUSE_CONTRACT_VERSION,
    verify_diagnostic_v6_reuse_contract,
)


class DiagnosticV1TruthAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.reuse_path = (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v5.json"
        )
        if not cls.reuse_path.is_file():
            raise unittest.SkipTest("diagnostic-v2 reuse contract is unavailable")

    def test_audit_recomputes_v1_errors_and_validates_fresh_v2_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_diagnostic_v1_truth_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
        self.assertEqual(
            receipt["schema_version"], DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION
        )
        self.assertEqual(
            receipt["recomputed_v1_errors"]["support_false_negative_count"], 8
        )
        self.assertEqual(
            receipt["recomputed_v1_errors"]["structured_field_error_count"], 2
        )
        self.assertEqual(
            receipt["recomputed_v1_errors"]["canary_changed_case_count"], 4
        )
        fixture = receipt["diagnostic_v2_fixture_validation"]
        self.assertEqual(fixture["case_count"], 18)
        self.assertEqual(fixture["fresh_case_key_count"], 18)
        self.assertEqual(fixture["v1_case_key_overlap_count"], 0)
        self.assertEqual(fixture["canary_case_count"], 6)
        self.assertTrue(fixture["all_checklist_fields_covered"])
        self.assertEqual(receipt["semantic_model_calls_performed"], 0)
        self.assertFalse(receipt["full_calibration_authorized"])

    def test_v2_truth_separates_field_only_and_claim_level_errors(self):
        fixture = json.loads(
            (
                self.repo
                / "research_factory/evaluation/judge_v5_diagnostic_v2.json"
            ).read_text(encoding="utf-8")
        )
        cases = fixture["cases"]
        field_only = [
            case
            for case in cases
            if case["expected"]["proposition_b"] == "supported"
            and case["expected"]["structured_b"] == "incorrect"
        ]
        unsupported = [
            case
            for case in cases
            if case["expected"]["proposition_b"] == "unsupported"
        ]
        self.assertGreaterEqual(len(field_only), 6)
        self.assertGreaterEqual(len(unsupported), 8)
        for case in field_only:
            self.assertNotIn(
                "unsupported_inference", case["expected"]["mismatch_fields"]
            )
            self.assertNotIn(
                "unsupported_inference", case["expected"]["field_issues_b"]
            )
        for case in unsupported:
            self.assertIn(
                "unsupported_inference", case["expected"]["mismatch_fields"]
            )
            self.assertIn(
                "unsupported_inference", case["expected"]["field_issues_b"]
            )

    def test_reuse_contract_freezes_v1_and_blocks_full_calibration(self):
        contract = verify_diagnostic_v2_reuse_contract(self.reuse_path)
        self.assertEqual(
            contract["schema_version"], DIAGNOSTIC_V2_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["diagnostic_v1_replay_allowed"])
        self.assertFalse(
            contract["policy"]["diagnostic_v1_output_reuse_as_pass_allowed"]
        )
        self.assertFalse(
            contract["policy"]["full_calibration_allowed_before_v2_pass"]
        )
        self.assertEqual(len(contract["diagnostic_v1_attempts"]), 4)
        self.assertEqual(
            contract["diagnostic_v1_incident"]["usage"]["total_tokens"],
            138211,
        )


class DiagnosticV2AttemptAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.reuse_path = (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v6.json"
        )
        if not cls.reuse_path.is_file():
            raise unittest.SkipTest("diagnostic-v3 reuse contract is unavailable")

    def test_v2_attempt_audit_recomputes_evidence_failure_and_usage(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_diagnostic_v2_attempt_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
        self.assertEqual(
            receipt["schema_version"],
            DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION,
        )
        self.assertEqual(
            receipt["recomputed_failure_forensics"],
            {
                "invalid_checklist_row_count": 29,
                "invalid_source_span_count": 34,
                "affected_canary_case_count": 6,
            },
        )
        self.assertEqual(receipt["measured_sidecar_count"], 3)
        self.assertEqual(receipt["measured_sidecar_usage"]["total_tokens"], 107187)
        self.assertFalse(receipt["quality_scored"])
        self.assertFalse(receipt["diagnostic_v2_replay_allowed"])
        self.assertFalse(receipt["diagnostic_v2_output_reuse_allowed"])
        self.assertEqual(receipt["semantic_model_calls_performed"], 0)

    def test_v3_reuse_requires_full_fresh_run_and_blocks_calibration(self):
        contract = verify_diagnostic_v3_reuse_contract(self.reuse_path)
        self.assertEqual(
            contract["schema_version"], DIAGNOSTIC_V3_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["diagnostic_v2_replay_allowed"])
        self.assertFalse(contract["policy"]["diagnostic_v2_output_reuse_allowed"])
        self.assertTrue(contract["policy"]["all_diagnostic_v3_turns_must_run_fresh"])
        self.assertTrue(contract["policy"]["alignment_evidence_envelope_changed"])
        self.assertFalse(contract["policy"]["fixture_content_changed"])
        self.assertFalse(
            contract["policy"]["full_calibration_allowed_before_v3_pass"]
        )
        self.assertEqual(
            contract["diagnostic_v2_incident"]["sidecar_usage"]["total_tokens"],
            107187,
        )


class DiagnosticV3QualityAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.reuse_path = (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v7.json"
        )
        if not cls.reuse_path.is_file():
            raise unittest.SkipTest("diagnostic-v4 reuse contract is unavailable")

    def test_v3_audit_isolates_order_bias_to_unsupported_consistency(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_diagnostic_v3_quality_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
        self.assertEqual(
            receipt["schema_version"],
            DIAGNOSTIC_V3_QUALITY_AUDIT_RECEIPT_VERSION,
        )
        self.assertEqual(receipt["only_failed_gate"], "order_bias")
        self.assertEqual(
            receipt["changed_case_keys"],
            ["diag2_actor", "diag2_pronunciation_coach_field"],
        )
        self.assertTrue(receipt["both_proposition_verdicts_supported"])
        self.assertEqual(receipt["base_unsupported_decision"], "same")
        self.assertEqual(receipt["canary_unsupported_decision"], "different")
        self.assertEqual(receipt["adjudication_unsupported_decision"], "different")
        self.assertFalse(receipt["full_calibration_authorized"])
        self.assertEqual(receipt["semantic_model_calls_performed"], 0)

    def test_v4_reuse_changes_only_consistency_rule_and_runs_fresh(self):
        contract = verify_diagnostic_v4_reuse_contract(self.reuse_path)
        self.assertEqual(
            contract["schema_version"], DIAGNOSTIC_V4_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["diagnostic_v3_replay_allowed"])
        self.assertFalse(contract["policy"]["diagnostic_v3_output_reuse_allowed"])
        self.assertTrue(contract["policy"]["all_diagnostic_v4_turns_must_run_fresh"])
        self.assertTrue(
            contract["policy"]["unsupported_inference_consistency_rule_added"]
        )
        self.assertTrue(
            contract["policy"]["validator_rejects_inconsistent_unsupported_row"]
        )
        self.assertFalse(contract["policy"]["fixture_content_changed"])
        self.assertFalse(contract["policy"]["fixture_truth_changed"])
        self.assertFalse(contract["policy"]["gate_thresholds_changed"])
        self.assertFalse(
            contract["policy"]["full_calibration_allowed_before_v4_pass"]
        )


class DiagnosticV4FixtureAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.terminal = (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-diagnostic-v4/terminal.json"
        )
        if not cls.terminal.is_file():
            raise unittest.SkipTest("diagnostic-v4 evidence is unavailable")

    def test_v4_audit_repairs_truth_without_changing_events_or_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_diagnostic_v4_fixture_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
        self.assertEqual(
            receipt["schema_version"],
            DIAGNOSTIC_V4_FIXTURE_AUDIT_RECEIPT_VERSION,
        )
        self.assertEqual(receipt["only_failed_gate"], "order_bias")
        self.assertEqual(receipt["failed_case_key"], "diag2_unsupported_inference")
        self.assertEqual(receipt["semantic_truth_correction"], "event_type")
        self.assertEqual(
            receipt["truth_projection_paths"],
            ["expected.field_issues_b", "expected.mismatch_fields"],
        )
        self.assertFalse(receipt["source_and_event_inputs_changed"])
        self.assertFalse(receipt["prompt_protocol_changed"])
        self.assertFalse(receipt["gate_thresholds_changed"])
        self.assertFalse(receipt["diagnostic_v4_replay_allowed"])
        self.assertFalse(receipt["diagnostic_v4_output_reuse_allowed"])
        self.assertFalse(receipt["full_calibration_authorized"])
        self.assertEqual(receipt["semantic_model_calls_performed"], 0)

    def test_v5_reuse_freezes_v4_and_requires_fresh_turns(self):
        contract_path = (
            self.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v8.json"
        )
        contract = verify_diagnostic_v5_reuse_contract(contract_path)
        self.assertEqual(
            contract["schema_version"], DIAGNOSTIC_V5_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["diagnostic_v4_replay_allowed"])
        self.assertFalse(contract["policy"]["diagnostic_v4_output_reuse_allowed"])
        self.assertTrue(contract["policy"]["all_diagnostic_v5_turns_must_run_fresh"])
        self.assertTrue(contract["policy"]["fixture_truth_changed"])
        self.assertFalse(
            contract["policy"]["fixture_source_or_event_inputs_changed"]
        )
        self.assertFalse(contract["policy"]["prompt_protocol_changed"])
        self.assertFalse(contract["policy"]["gate_thresholds_changed"])
        self.assertFalse(
            contract["policy"]["full_calibration_allowed_before_v5_pass"]
        )
        self.assertEqual(
            contract["diagnostic_v4_incident"]["usage"]["total_tokens"],
            147819,
        )


class DiagnosticV5QualityAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.terminal = (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-diagnostic-v5/terminal.json"
        )
        if not cls.terminal.is_file():
            raise unittest.SkipTest("diagnostic-v5 evidence is unavailable")

    def test_v5_audit_isolates_boundary_event_type_order_error(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_diagnostic_v5_quality_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
        self.assertEqual(
            receipt["schema_version"],
            DIAGNOSTIC_V5_QUALITY_AUDIT_RECEIPT_VERSION,
        )
        self.assertEqual(receipt["only_failed_gate"], "order_bias")
        self.assertEqual(receipt["changed_case_key"], "diag2_unsupported_inference")
        self.assertTrue(receipt["base_boundary_without_evidence"])
        self.assertTrue(receipt["canary_and_adjudication_event_type"])
        self.assertFalse(receipt["diagnostic_v5_replay_allowed"])
        self.assertFalse(receipt["diagnostic_v5_output_reuse_allowed"])
        self.assertFalse(receipt["full_calibration_authorized"])
        self.assertEqual(receipt["semantic_model_calls_performed"], 0)

    def test_v6_reuse_freezes_v5_and_requires_fresh_turns(self):
        contract_path = (
            self.repo
            / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v9.json"
        )
        contract = verify_diagnostic_v6_reuse_contract(contract_path)
        self.assertEqual(
            contract["schema_version"], DIAGNOSTIC_V6_REUSE_CONTRACT_VERSION
        )
        self.assertFalse(contract["policy"]["diagnostic_v5_replay_allowed"])
        self.assertFalse(contract["policy"]["diagnostic_v5_output_reuse_allowed"])
        self.assertTrue(contract["policy"]["all_diagnostic_v6_turns_must_run_fresh"])
        self.assertTrue(
            contract["policy"]["boundary_evidence_consistency_rule_added"]
        )
        self.assertTrue(
            contract["policy"]["unsupported_assertion_not_boundary_rule_added"]
        )
        self.assertTrue(contract["policy"]["event_type_direct_category_rule_added"])
        self.assertFalse(contract["policy"]["fixture_truth_changed"])
        self.assertFalse(contract["policy"]["gate_thresholds_changed"])
        self.assertEqual(
            contract["diagnostic_v5_incident"]["usage"]["total_tokens"],
            149074,
        )


if __name__ == "__main__":
    unittest.main()
