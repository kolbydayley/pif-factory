from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    DIAGNOSTIC_GATES,
    JudgeV5ProtocolError,
    build_disagreement_adjudication_input,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    expected_relation_from_checklist,
    find_observable_alignment_disagreements,
    freeze_support_receipts,
    load_v5_diagnostic_fixture,
    make_v5_diagnostic_pool,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    pointwise_support_output_schema,
    project_mismatch_fields,
    reconcile_neutral_alignment,
    score_v5_diagnostic,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from research_factory.app_server_judge_v5_fixture import (
    build_fixture_truth_audit_receipt,
    compact_empty_event_fields,
    load_fixture_truth_audit,
    measure_v4_prompt_compaction,
)
from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)
from research_factory.app_server_v5_reuse import (
    PIPELINE_V5_REUSE_CONTRACT_VERSION,
    V5ReuseContractError,
    build_v5_reuse_contract,
    verify_v5_reuse_contract,
)


def perfect_pointwise(pointwise_input, expected):
    units = {
        (item["case_id"], item["witness_id"]): item
        for item in pointwise_input["units"]
    }
    rows = []
    for case_id, truth in expected["cases"].items():
        for witness_id, verdict in truth["proposition"].items():
            unit = units[(case_id, witness_id)]
            rows.append(
                {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "proposition_verdict": verdict,
                    "proposition_evidence_spans": (
                        [unit["structured_event"]["evidence"]]
                        if verdict == "supported"
                        else []
                    ),
                    "proposition_rationale": "Audited diagnostic truth.",
                    "structured_field_verdict": truth["structured_fields"][
                        witness_id
                    ],
                    "field_issue_fields": truth["field_issues"][witness_id],
                    "field_evidence_spans": [
                        unit["structured_event"]["evidence"]
                    ],
                    "field_rationale": "Audited diagnostic truth.",
                }
            )
    return {"units": rows}


def perfect_alignment(alignment_input, expected):
    rows = []
    for case in alignment_input["cases"]:
        truth = expected["cases"][case["case_id"]]
        different = set(truth["mismatch_fields"])
        rows.append(
            {
                "case_id": case["case_id"],
                "equivalence_groups": [
                    {
                        "witness_ids": group,
                        "rationale": "Audited equivalence truth.",
                    }
                    for group in truth["equivalence_groups"]
                ],
                "alignment_pairs": [
                    {
                        "witness_id_1": truth["pair_witness_ids"][0],
                        "witness_id_2": truth["pair_witness_ids"][1],
                        "relation": truth["relation"],
                        "checklist": [
                            {
                                "field": field,
                                "decision": (
                                    "different" if field in different else "same"
                                ),
                                "source_evidence_spans": [],
                                "witness_evidence_ids": truth[
                                    "pair_witness_ids"
                                ],
                                "rationale": "Audited minimal-pair truth.",
                            }
                            for field in alignment_input["checklist_field_order"]
                        ],
                        "rationale": "Audited pair truth.",
                    }
                ],
                "unpaired_witness_ids": [],
            }
        )
    return {"cases": rows}


class FixtureTruthAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]

    def test_fixture_audit_reclassifies_contradictions_and_defines_all_fields(self):
        audit = load_fixture_truth_audit()
        self.assertFalse(
            audit["legacy_truth_classification"][
                "admissible_as_pipeline_v5_proposition_support_truth"
            ]
        )
        self.assertFalse(
            audit["legacy_truth_classification"][
                "admissible_as_pipeline_v5_structured_field_truth"
            ]
        )
        self.assertEqual(len(audit["material_field_error_reclassifications"]), 10)
        self.assertEqual(
            sum(
                item["legacy_mutated_event_support"] == "supported"
                for item in audit["material_field_error_reclassifications"]
            ),
            7,
        )
        self.assertEqual(
            len(audit["language_tutor_structured_field_reclassifications"]), 6
        )
        self.assertEqual(
            tuple(item["field"] for item in audit["mismatch_checklist"]),
            CHECKLIST_FIELDS,
        )

    def test_v4_forensics_and_empty_field_measurement_reproduce(self):
        calibration = (
            self.repo
            / "work/app-server-development-v2/unattended-pipeline-v4/development-selection-sharded-v3/calibration"
        )
        if not (calibration / "report.json").is_file():
            self.skipTest("immutable pipeline-v4 calibration unavailable")
        measured = measure_v4_prompt_compaction(calibration)
        self.assertEqual(measured["prompt_count"], 22)
        self.assertEqual(measured["before_prompt_bytes"], 401660)
        self.assertEqual(measured["after_prompt_bytes"], 241992)
        self.assertEqual(measured["reduction_fraction"], 0.3975)
        self.assertFalse(measured["semantic_pruning_performed"])

        with tempfile.TemporaryDirectory() as directory:
            receipt = build_fixture_truth_audit_receipt(
                repo_root=self.repo,
                output_path=Path(directory) / "receipt.json",
            )
            self.assertEqual(receipt["semantic_model_calls_performed"], 0)
            self.assertEqual(
                receipt["forensics"]["ab"]["support_false_negative_count"], 11
            )
            self.assertEqual(
                receipt["forensics"]["ba"]["support_false_negative_count"], 22
            )

    def test_compaction_removes_only_structural_empty_values(self):
        value = {
            "empty_text": "",
            "empty_list": [],
            "empty_object": {},
            "null": None,
            "false": False,
            "zero": 0,
            "not_applicable": "not_applicable",
            "semantic": "kept",
        }
        self.assertEqual(
            compact_empty_event_fields(value),
            {
                "false": False,
                "zero": 0,
                "not_applicable": "not_applicable",
                "semantic": "kept",
            },
        )


class JudgeV5ProtocolTest(unittest.TestCase):
    def setUp(self):
        self.fixture = load_v5_diagnostic_fixture()
        self.pool, self.mapping, self.expected = make_v5_diagnostic_pool()
        self.pointwise_input = build_pointwise_support_input(self.pool)
        self.pointwise_output = perfect_pointwise(
            self.pointwise_input, self.expected
        )
        self.receipts = freeze_support_receipts(
            self.pointwise_output, self.pointwise_input
        )
        self.base_input = build_neutral_alignment_input(self.pool, self.receipts)
        self.canary_input = build_neutral_alignment_input(
            self.pool,
            self.receipts,
            case_ids=self.expected["canary_case_ids"],
            permutation="balanced_canary",
        )
        self.base_output = perfect_alignment(self.base_input, self.expected)
        self.canary_output = perfect_alignment(self.canary_input, self.expected)

    def test_diagnostic_has_18_cases_and_minimal_pairs_for_every_enum(self):
        self.assertEqual(len(self.fixture["cases"]), 18)
        by_focus = {case["focus_field"]: case for case in self.fixture["cases"]}
        for field in CHECKLIST_FIELDS:
            self.assertIn(field, by_focus)
            self.assertIn(field, by_focus[field]["expected"]["mismatch_fields"])
        self.assertEqual(len(self.expected["canary_case_ids"]), 6)

    def test_pointwise_input_has_no_side_labels_and_schema_is_supported(self):
        self.assertEqual(len(self.pointwise_input["units"]), 36)
        for unit in self.pointwise_input["units"]:
            self.assertEqual(set(unit["proposition"]), {"claim_text"})
            self.assertEqual(
                unit["proposition"]["claim_text"],
                unit["structured_event"]["claim_text"],
            )
        rendered = json.dumps(self.pointwise_input, sort_keys=True)
        self.assertNotIn("event_set_a", rendered)
        self.assertNotIn("event_set_b", rendered)
        self.assertNotIn('"system_id":', rendered)
        self.assertIn('"proposition":', rendered)
        self.assertIn('"structured_event":', rendered)
        schema = pointwise_support_output_schema(self.pointwise_input)
        self.assertEqual(validate_app_server_output_schema_subset(schema), [])
        self.assertLess(len(build_pointwise_support_prompt(self.pointwise_input)), 32768)
        self.assertEqual(
            validate_pointwise_support_output(
                self.pointwise_output, self.pointwise_input
            ),
            [],
        )

    def test_exact_evidence_does_not_override_unsupported_truth(self):
        case = next(
            item
            for item in self.fixture["cases"]
            if item["case_key"] == "diag2_exact_evidence_not_support"
        )
        self.assertEqual(case["event_b"]["evidence"], case["source_excerpt"])
        self.assertEqual(case["expected"]["proposition_b"], "unsupported")
        changed = json.loads(json.dumps(self.pointwise_output))
        case_id = next(
            cid
            for cid, truth in self.expected["cases"].items()
            if truth["case_key"] == "diag2_exact_evidence_not_support"
        )
        unsupported_id = next(
            wid
            for wid, verdict in self.expected["cases"][case_id]["proposition"].items()
            if verdict == "unsupported"
        )
        row = next(item for item in changed["units"] if item["witness_id"] == unsupported_id)
        row["proposition_verdict"] = "supported"
        row["proposition_evidence_spans"] = [case["source_excerpt"]]
        disagreements = find_observable_alignment_disagreements(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=self.canary_output,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        reconciled = reconcile_neutral_alignment(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=self.canary_output,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        score = score_v5_diagnostic(
            pointwise_output=changed,
            pointwise_input=self.pointwise_input,
            reconciled_alignment=reconciled,
            expected=self.expected,
            observable_disagreements=disagreements,
        )
        self.assertFalse(score["checks"]["support_specificity"])

    def test_versioned_fixture_patch_repairs_only_event_type_truth(self):
        corrected = next(
            item
            for item in self.fixture["cases"]
            if item["case_key"] == "diag2_unsupported_inference"
        )
        base_fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "research_factory/evaluation/judge_v5_diagnostic_v2.json"
            ).read_text(encoding="utf-8")
        )
        original = next(
            item
            for item in base_fixture["cases"]
            if item["case_key"] == "diag2_unsupported_inference"
        )
        self.assertEqual(corrected["source_excerpt"], original["source_excerpt"])
        self.assertEqual(corrected["event_a"], original["event_a"])
        self.assertEqual(corrected["event_b"], original["event_b"])
        self.assertEqual(
            corrected["expected"]["field_issues_b"],
            ["event_type", "target", "unsupported_inference"],
        )
        self.assertEqual(
            corrected["expected"]["mismatch_fields"],
            ["event_type", "target", "unsupported_inference"],
        )

    def test_checklist_projection_and_relation_precedence(self):
        for case in self.base_output["cases"]:
            pair = case["alignment_pairs"][0]
            truth = self.expected["cases"][case["case_id"]]
            self.assertEqual(
                project_mismatch_fields(pair["checklist"]),
                [field for field in CHECKLIST_FIELDS if field in truth["mismatch_fields"]],
            )
            self.assertEqual(
                expected_relation_from_checklist(pair["checklist"]),
                truth["relation"],
            )
        self.assertEqual(
            validate_neutral_alignment_output(self.base_output, self.base_input), []
        )
        partial = next(
            case
            for case in self.base_output["cases"]
            if self.expected["cases"][case["case_id"]]["relation"] == "partial"
        )
        self.assertEqual(
            set(project_mismatch_fields(partial["alignment_pairs"][0]["checklist"])),
            {"event_boundary", "evidence"},
        )

    def test_event_boundary_requires_evidence_difference(self):
        changed = json.loads(json.dumps(self.base_output))
        partial = next(
            case
            for case in changed["cases"]
            if self.expected["cases"][case["case_id"]]["relation"] == "partial"
        )
        evidence = next(
            row
            for row in partial["alignment_pairs"][0]["checklist"]
            if row["field"] == "evidence"
        )
        evidence["decision"] = "same"
        partial["alignment_pairs"][0]["relation"] = "non_equivalent"
        errors = validate_neutral_alignment_output(changed, self.base_input)
        self.assertTrue(any("boundary_without_evidence" in error for error in errors))

    def test_prompt_separates_boundary_from_unsupported_event_type_change(self):
        prompt = build_neutral_alignment_prompt(self.base_input)
        instructions = neutral_alignment_base_instructions()
        self.assertIn("witness-only unsupported clause", instructions)
        self.assertIn("measurement claim and a capability claim", instructions)
        self.assertIn("Never mark event_boundary different", prompt)

    def test_alignment_evidence_separates_source_spans_and_witness_ids(self):
        self.assertEqual(
            validate_neutral_alignment_output(self.base_output, self.base_input), []
        )
        changed = json.loads(json.dumps(self.base_output))
        pair = changed["cases"][0]["alignment_pairs"][0]
        pair["checklist"][0]["source_evidence_spans"] = ["not in source"]
        self.assertTrue(
            validate_neutral_alignment_output(changed, self.base_input)
        )

    def test_unsupported_row_must_match_frozen_proposition_receipts(self):
        changed = json.loads(json.dumps(self.base_output))
        field_only = next(
            case
            for case in changed["cases"]
            if self.expected["cases"][case["case_id"]]["case_key"]
            == "diag2_actor"
        )
        unsupported = next(
            row
            for row in field_only["alignment_pairs"][0]["checklist"]
            if row["field"] == "unsupported_inference"
        )
        unsupported["decision"] = "different"
        errors = validate_neutral_alignment_output(changed, self.base_input)
        self.assertTrue(
            any("unsupported_inference_support_inconsistent" in error for error in errors)
        )
        changed = json.loads(json.dumps(self.base_output))
        pair = changed["cases"][0]["alignment_pairs"][0]
        pair["checklist"][0]["witness_evidence_ids"] = [pair["witness_id_1"]] * 2
        self.assertTrue(
            validate_neutral_alignment_output(changed, self.base_input)
        )

    def test_origin_neutral_canary_is_order_invariant(self):
        self.assertEqual(len(self.canary_input["cases"]), 6)
        self.assertEqual(
            self.base_input["checklist_field_order"],
            self.canary_input["checklist_field_order"],
        )
        self.assertEqual(
            self.base_input["checklist_decision_order"],
            self.canary_input["checklist_decision_order"],
        )
        self.assertEqual(
            self.canary_input["permuted_axes"],
            ["anonymous_case_order", "anonymous_witness_order"],
        )
        selected_base_ids = [
            case["case_id"]
            for case in self.base_input["cases"]
            if case["case_id"] in self.expected["canary_case_ids"]
        ]
        self.assertEqual(
            [case["case_id"] for case in self.canary_input["cases"]],
            list(reversed(selected_base_ids)),
        )
        self.assertEqual(
            validate_neutral_alignment_output(
                self.canary_output, self.canary_input
            ),
            [],
        )
        base = normalize_neutral_alignment_output(self.base_output, self.base_input)
        canary = normalize_neutral_alignment_output(
            self.canary_output, self.canary_input
        )
        base_by_id = {item["case_id"]: item for item in base["cases"]}
        self.assertEqual(
            sorted(
                [base_by_id[item["case_id"]] for item in canary["cases"]],
                key=lambda item: item["case_id"],
            ),
            canary["cases"],
        )
        disagreements = find_observable_alignment_disagreements(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=self.canary_output,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        self.assertEqual(disagreements["disagreement_case_count"], 0)
        self.assertFalse(disagreements["adjudication_required"])

    def test_disagreement_gets_one_adjudication_or_abstains_without_voting(self):
        changed = json.loads(json.dumps(self.canary_output))
        changed_case = next(
            case
            for case in changed["cases"]
            if case["alignment_pairs"][0]["relation"] == "equivalent"
        )
        pair = changed_case["alignment_pairs"][0]
        actor = next(item for item in pair["checklist"] if item["field"] == "actor")
        actor["decision"] = "different"
        pair["relation"] = "non_equivalent"
        disagreements = find_observable_alignment_disagreements(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=changed,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        self.assertGreaterEqual(disagreements["disagreement_case_count"], 1)
        adjudication = build_disagreement_adjudication_input(
            base_input=self.base_input,
            base_output=self.base_output,
            canary_input=self.canary_input,
            canary_output=changed,
            support_receipts=self.receipts,
        )
        self.assertEqual(adjudication["call_cap"], 1)
        reconciled = reconcile_neutral_alignment(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=changed,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        self.assertTrue(reconciled["unresolved_cases_abstained"])
        self.assertEqual(reconciled["adjudication_call_count"], 0)
        self.assertFalse(reconciled["majority_voting_used"])

    def test_perfect_diagnostic_passes_every_frozen_gate(self):
        disagreements = find_observable_alignment_disagreements(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=self.canary_output,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        reconciled = reconcile_neutral_alignment(
            base_output=self.base_output,
            base_input=self.base_input,
            canary_output=self.canary_output,
            canary_input=self.canary_input,
            support_receipts=self.receipts,
        )
        score = score_v5_diagnostic(
            pointwise_output=self.pointwise_output,
            pointwise_input=self.pointwise_input,
            reconciled_alignment=reconciled,
            expected=self.expected,
            observable_disagreements=disagreements,
        )
        self.assertTrue(score["passed"])
        self.assertTrue(all(score["checks"].values()))
        self.assertEqual(score["gates"], DIAGNOSTIC_GATES)

    def test_alignment_schema_and_prompts_stay_bounded(self):
        schema = neutral_alignment_output_schema(self.base_input)
        self.assertEqual(validate_app_server_output_schema_subset(schema), [])
        self.assertLess(
            len(json.dumps(schema, sort_keys=True, separators=(",", ":"))), 12288
        )
        self.assertLess(len(build_neutral_alignment_prompt(self.base_input)), 32768)
        self.assertLess(len(build_neutral_alignment_prompt(self.canary_input)), 32768)


class PipelineV5ReuseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]

    def test_v5_contract_binds_v4_and_forbids_full_calibration(self):
        if not (
            self.repo
            / "work/app-server-development-v2/unattended-control-v5/terminal-receipt-v5.json"
        ).is_file():
            self.skipTest("immutable pipeline-v4 receipt unavailable")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "unattended-pipeline-v5"
            audit = root / "fixture-truth-audit-receipt-v1.json"
            build_fixture_truth_audit_receipt(
                repo_root=self.repo, output_path=audit
            )
            contract_path = root / "reuse-contract-v4.json"
            contract = build_v5_reuse_contract(
                repo_root=self.repo,
                output_path=contract_path,
                fixture_audit_receipt_path=audit,
            )
            self.assertEqual(
                contract["schema_version"], PIPELINE_V5_REUSE_CONTRACT_VERSION
            )
            self.assertFalse(contract["policy"]["pipeline_v4_replay_allowed"])
            self.assertFalse(
                contract["policy"][
                    "full_v5_calibration_allowed_before_diagnostic_pass"
                ]
            )
            self.assertEqual(len(contract["pipeline_v4_attempts"]), 22)
            self.assertEqual(verify_v5_reuse_contract(contract_path), contract)

            payload = json.loads(contract_path.read_text(encoding="utf-8"))
            payload["policy"]["pipeline_v4_replay_allowed"] = True
            contract_path.write_text(
                json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(V5ReuseContractError, "replay policy"):
                verify_v5_reuse_contract(contract_path)


if __name__ == "__main__":
    unittest.main()
