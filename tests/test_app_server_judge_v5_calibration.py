from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from research_factory.app_server_judge_v5_calibration import (
    CALIBRATION_GATES,
    calibration_case_shards,
    make_v5_calibration_pool,
    pointwise_input_subset,
    score_v5_calibration,
)
from research_factory.app_server_judge_v5 import build_pointwise_support_input
from research_factory.app_server_judge_v5_calibration_runner import (
    freeze_v5_full_calibration,
    run_v5_full_calibration,
    validate_scoreable_calibration_alignment_output,
)
from research_factory.app_server_judge_v5_calibration_v1_audit import (
    build_calibration_v1_audit_receipt,
)
from research_factory.app_server_judge_v5 import validate_neutral_alignment_output
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeCalibrationClient:
    def __init__(self, expected):
        self.expected = expected
        self.calls = []
        self.enter_count = 0
        self.exit_count = 0

    async def __aenter__(self):
        self.enter_count += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exit_count += 1

    @staticmethod
    def _capacity(path):
        payload = {
            "schema_version": CAPACITY_CHECKPOINT_VERSION,
            "primary_used_percent": 3,
            "primary_resets_at": None,
            "rate_limit_reached_type": None,
            "maximum_primary_used_percent": 20,
            "cleared_for_semantic_turn": True,
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
        }
        write_text_atomic(Path(path), _canonical_json(payload) + "\n")

    def _pointwise(self, input_value):
        rows = []
        for unit in input_value["units"]:
            truth = self.expected["cases"][unit["case_id"]]
            witness_id = unit["witness_id"]
            verdict = truth["proposition"][witness_id]
            evidence = unit["structured_event"]["evidence"]
            rows.append(
                {
                    "case_id": unit["case_id"],
                    "witness_id": witness_id,
                    "proposition_verdict": verdict,
                    "proposition_evidence_spans": [evidence] if verdict == "supported" else [],
                    "proposition_rationale": "Audited calibration truth.",
                    "structured_field_verdict": truth["structured_fields"][witness_id],
                    "field_issue_fields": truth["field_issues"][witness_id],
                    "field_evidence_spans": [evidence],
                    "field_rationale": "Audited calibration truth.",
                }
            )
        return {"units": rows}

    def _alignment(self, input_value):
        rows = []
        for case in input_value["cases"]:
            truth = self.expected["cases"][case["case_id"]]
            pairs = []
            for pair in truth["pairs"]:
                different = set(pair["mismatch_fields"])
                pairs.append(
                    {
                        "witness_id_1": pair["witness_ids"][0],
                        "witness_id_2": pair["witness_ids"][1],
                        "relation": pair["relation"],
                        "checklist": [
                            {
                                "field": field,
                                "decision": "different" if field in different else "same",
                                "source_evidence_spans": [],
                                "witness_evidence_ids": pair["witness_ids"],
                                "rationale": "Audited calibration truth.",
                            }
                            for field in input_value["checklist_field_order"]
                        ],
                        "rationale": "Audited calibration pair.",
                    }
                )
            rows.append(
                {
                    "case_id": case["case_id"],
                    "equivalence_groups": [
                        {"witness_ids": group, "rationale": "Audited partition."}
                        for group in truth["equivalence_groups"]
                    ],
                    "alignment_pairs": pairs,
                    "unpaired_witness_ids": truth["unpaired_witness_ids"],
                }
            )
        return {"cases": rows}

    async def run_ephemeral_structured_turn(self, **kwargs):
        turn_name = Path(kwargs["sidecar_path"]).parent.name.replace("-", "_")
        self.calls.append(turn_name)
        self._capacity(kwargs["capacity_checkpoint_path"])
        input_value = json.loads(
            Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text()
        )
        output = (
            self._pointwise(input_value)
            if turn_name.startswith("pointwise_support_shard")
            else self._alignment(input_value)
        )
        output_text = _canonical_json(output)
        write_text_atomic(Path(kwargs["output_path"]), output_text + "\n")
        sidecar = {
            "state": "completed",
            "status": "completed",
            "client_version": APP_SERVER_CLIENT_VERSION,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": kwargs["thread_mode"],
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "prompt_sha256": sha256_text(kwargs["prompt"]),
            "base_instructions_sha256": sha256_text(kwargs["base_instructions"]),
            "output_schema_sha256": sha256_text(_canonical_json(kwargs["output_schema"])),
            "output_sha256": sha256_text(output_text),
            "usage_complete": True,
            "usage_status": "measured",
            "usage": {
                "input_tokens": 1000,
                "cached_input_tokens": 100,
                "output_tokens": 200,
                "reasoning_output_tokens": 50,
                "total_tokens": 1200,
            },
            "wall_elapsed_seconds": 1.25,
            "error_class": None,
        }
        write_text_atomic(Path(kwargs["sidecar_path"]), _canonical_json(sidecar) + "\n")
        return SimpleNamespace(
            status_ok=True,
            output=output,
            error_class=None,
            status="completed",
        )


class JudgeV5CalibrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pool, cls.mapping, cls.expected = make_v5_calibration_pool()

    def test_truth_separates_support_and_structured_fields(self):
        self.assertEqual(len(self.expected["cases"]), 66)
        self.assertEqual(len(self.expected["canary_case_ids"]), 12)
        cases = {
            truth["base_case_id"]: truth for truth in self.expected["cases"].values()
        }
        radiology = cases["radiology__material_field_error"]
        changed = next(
            witness_id
            for witness_id, fields in radiology["field_issues"].items()
            if fields == ["actor"]
        )
        self.assertEqual(radiology["proposition"][changed], "supported")
        self.assertEqual(radiology["structured_fields"][changed], "incorrect")
        language = cases["language_tutor__identity_paraphrase"]
        self.assertEqual(
            sorted(language["structured_fields"].values()),
            ["incorrect", "incorrect"],
        )
        self.assertTrue(
            all(
                fields == ["actor", "speaker"]
                for fields in language["field_issues"].values()
            )
        )

    def test_shards_cover_every_case_once(self):
        shards = calibration_case_shards(self.pool)
        self.assertEqual(len(shards), 11)
        self.assertTrue(all(len(shard) == 6 for shard in shards))
        flattened = [case_id for shard in shards for case_id in shard]
        self.assertEqual(len(flattened), len(set(flattened)))
        self.assertEqual(set(flattened), set(self.expected["cases"]))
        pointwise = build_pointwise_support_input(self.pool)
        subset = pointwise_input_subset(pointwise, shards[0])
        self.assertEqual({row["case_id"] for row in subset["units"]}, set(shards[0]))

    def test_perfect_full_calibration_passes_all_gates(self):
        pointwise_units = []
        alignment_cases = []
        for case_id, truth in self.expected["cases"].items():
            for witness_id, verdict in truth["proposition"].items():
                pointwise_units.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "proposition_verdict": verdict,
                        "structured_field_verdict": truth["structured_fields"][witness_id],
                        "field_issue_fields": truth["field_issues"][witness_id],
                    }
                )
            alignment_cases.append(
                {
                    "case_id": case_id,
                    "status": "accepted_base",
                    "equivalence_groups": truth["equivalence_groups"],
                    "alignment_pairs": [
                        {
                            "witness_ids": pair["witness_ids"],
                            "relation": pair["relation"],
                            "mismatch_fields": pair["mismatch_fields"],
                        }
                        for pair in truth["pairs"]
                    ],
                    "unpaired_witness_ids": truth["unpaired_witness_ids"],
                }
            )
        score = score_v5_calibration(
            pointwise_output={"units": pointwise_units},
            reconciled_alignment={"cases": alignment_cases},
            expected=self.expected,
            observable_disagreements={"disagreement_case_count": 0},
        )
        self.assertTrue(score["passed"])
        self.assertTrue(all(score["checks"].values()))
        self.assertEqual(score["metrics"]["case_count"], 66)
        self.assertEqual(score["metrics"]["witness_count"], 182)
        self.assertEqual(score["metrics"]["order_bias"], 0.0)
        self.assertEqual(CALIBRATION_GATES["canary_case_count"], 12)

    def test_freeze_creates_bounded_shards_without_model_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            frozen = freeze_v5_full_calibration(
                output_dir=Path(directory),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
            )
        self.assertEqual(frozen["spec"]["semantic_model_calls_performed_during_freeze"], 0)
        self.assertEqual(frozen["spec"]["pointwise_shard_count"], 11)
        self.assertEqual(frozen["spec"]["base_alignment_shard_count"], 11)
        self.assertEqual(frozen["spec"]["cases_per_shard"], 6)
        self.assertEqual(frozen["spec"]["minimum_turn_count"], 23)

    def test_complete_v1_output_is_scoreable_not_infrastructure_failure(self):
        root = (
            Path(__file__).resolve().parents[1]
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-calibration-v5_4-v1/turns/neutral-alignment-base-shard-05"
        )
        output = json.loads((root / "output.private.json").read_text())
        input_value = json.loads((root / "input.private.json").read_text())
        self.assertEqual(
            validate_neutral_alignment_output(output, input_value),
            ["case_1_pair_0_unsupported_without_specific_root"],
        )
        self.assertEqual(
            validate_scoreable_calibration_alignment_output(output, input_value),
            [],
        )

    def test_v1_audit_forbids_replay_and_output_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_calibration_v1_audit_receipt(
                repo_root=Path(__file__).resolve().parents[1],
                output_path=Path(directory) / "receipt.json",
            )
        self.assertFalse(receipt["calibration_v1_replay_allowed"])
        self.assertFalse(receipt["calibration_v1_output_reuse_allowed"])
        self.assertTrue(receipt["calibration_v2_all_turns_must_run_fresh"])
        self.assertFalse(receipt["frozen_judge_prompt_changed"])
        self.assertFalse(receipt["calibration_gates_changed"])


class JudgeV5CalibrationRunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_full_fake_calibration_runs_twenty_three_turns_and_passes(self):
        _pool, _mapping, expected = make_v5_calibration_pool()
        client = FakeCalibrationClient(expected)
        with tempfile.TemporaryDirectory() as directory:
            terminal = await run_v5_full_calibration(
                output_dir=Path(directory), client_factory=lambda: client
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["calibration_passed"])
        self.assertTrue(terminal["selection_authorized"])
        self.assertEqual(terminal["turn_count"], 23)
        self.assertEqual(terminal["usage"]["total_tokens"], 27600)
        self.assertEqual(client.enter_count, 1)
        self.assertEqual(client.exit_count, 1)
        self.assertEqual(len(client.calls), 23)
        self.assertEqual(len(set(client.calls)), 23)
        self.assertFalse(any("adjudication" in call for call in client.calls))

    async def test_fixture_truth_audit_completion_cannot_authorize_selection(self):
        _pool, _mapping, expected = make_v5_calibration_pool()
        client = FakeCalibrationClient(expected)
        with tempfile.TemporaryDirectory() as directory:
            terminal = await run_v5_full_calibration(
                output_dir=Path(directory),
                execution_purpose="fixture_truth_audit",
                model="gpt-5.5",
                client_factory=lambda: client,
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertEqual(
            terminal["terminal_reason"],
            "fixture_truth_proposal_completed_reference_adjudication_required",
        )
        self.assertTrue(terminal["fixture_truth_proposal_completed"])
        self.assertTrue(terminal["provisional_reference_comparison_passed"])
        self.assertFalse(terminal["reference_freeze_authorized"])
        self.assertFalse(terminal["calibration_passed"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertEqual(terminal["turn_count"], 23)


if __name__ == "__main__":
    unittest.main()
