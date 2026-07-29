from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
)
from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v25 import (
    build_v25_presemantic_recovery,
)
from research_factory.app_server_judge_v5_calibration_v25_diagnostic import (
    MAX_PROMPT_BYTES,
    V25_REPAIR_DIAGNOSTIC_SPEC_VERSION,
    freeze_v25_repair_diagnostic,
    run_v25_repair_diagnostic,
)
from research_factory.app_server_judge_v5_diagnostic import _sha256_file
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeV25RepairClient:
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
        return None

    def _capacity(self, path):
        policy_path = Path(path).parents[2] / "capacity-policy.json"
        payload = {
            "schema_version": RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": "2026-07-13T00:00:00Z",
            "policy_path": str(policy_path),
            "policy_sha256": _sha256_file(policy_path),
            "phase_id": "judge_v5_4_v25_repair_diagnostic",
            "turn_name": Path(path).parent.name.replace("-", "_"),
            "turn_ordinal": len(self.calls),
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "primary_used_percent": 3,
            "primary_remaining_percent": 97,
            "primary_resets_at": None,
            "rate_limit_reached_type": None,
            "minimum_remaining_reserve_percent": 20,
            "usable_percent_above_reserve": 77,
            "remaining_turn_count": 6,
            "projected_remaining_tokens": 420000,
            "quota_points_per_million_tokens": 17,
            "projected_remaining_quota_points": 8,
            "projected_terminal_remaining_percent": 89,
            "cleared_for_semantic_turn": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": "test",
        }
        write_text_atomic(
            Path(path), json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n"
        )

    def _perfect_output(self, input_value):
        cases = []
        for case in input_value["cases"]:
            truth = self.expected["cases"][case["case_id"]]
            support_units = []
            for witness in case["witnesses"]:
                witness_id = witness["witness_id"]
                support_units.append(
                    {
                        "witness_id": witness_id,
                        "proposition_verdict": truth["proposition"][witness_id],
                        "structured_field_verdict": truth["structured_fields"][witness_id],
                        "field_issue_fields": truth["field_issues"][witness_id],
                        "source_evidence_spans": [],
                        "rationale": "Audited v25 diagnostic truth.",
                    }
                )
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
                                "rationale": "Audited checklist truth.",
                            }
                            for field in CHECKLIST_FIELDS
                        ],
                        "rationale": "Audited pair truth.",
                    }
                )
            cases.append(
                {
                    "case_id": case["case_id"],
                    "support_units": support_units,
                    "equivalence_groups": [
                        {"witness_ids": group, "rationale": "Audited group truth."}
                        for group in truth["equivalence_groups"]
                    ],
                    "alignment_pairs": pairs,
                    "unpaired_witness_ids": truth["unpaired_witness_ids"],
                }
            )
        return {"cases": cases}

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(kwargs)
        self._capacity(kwargs["capacity_checkpoint_path"])
        input_value = json.loads(
            Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text()
        )
        output = self._perfect_output(input_value)
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
        write_text_atomic(
            Path(kwargs["sidecar_path"]),
            json.dumps(sidecar, ensure_ascii=True, sort_keys=True) + "\n",
        )
        return SimpleNamespace(
            status_ok=True,
            output=output,
            usage=SimpleNamespace(total_tokens=1200),
            error_class=None,
            status="completed",
        )


class ImperfectV25RepairClient(FakeV25RepairClient):
    def _perfect_output(self, input_value):
        output = super()._perfect_output(input_value)
        for case in output["cases"]:
            for unit in case["support_units"]:
                unit["proposition_verdict"] = "supported"
                unit["structured_field_verdict"] = "incorrect"
                unit["field_issue_fields"] = CHECKLIST_FIELDS[:3]
            for pair in case["alignment_pairs"]:
                pair["relation"] = "equivalent"
                for item in pair["checklist"]:
                    item["decision"] = "same"
        return output


class JudgeV5CalibrationV25DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_freeze_declares_six_turn_repair_diagnostic_without_model_call(self):
        with tempfile.TemporaryDirectory() as temp:
            presemantic = Path(temp) / "presemantic"
            build_v25_presemantic_recovery(output_root=presemantic)
            frozen = freeze_v25_repair_diagnostic(
                output_dir=Path(temp) / "repair",
                presemantic_root=presemantic,
            )
            spec = frozen["spec"]
            self.assertEqual(spec["schema_version"], V25_REPAIR_DIAGNOSTIC_SPEC_VERSION)
            self.assertEqual(spec["semantic_model_calls_performed_during_freeze"], 0)
            self.assertEqual(spec["turn_plan"], [
                "repair_base_shard_00",
                "repair_base_shard_01",
                "repair_base_shard_02",
                "repair_canary_shard_00",
                "repair_canary_shard_01",
                "repair_canary_shard_02",
            ])
            self.assertEqual(
                spec["capacity_policy"]["size_bytes"],
                Path(spec["capacity_policy"]["path"]).stat().st_size,
            )
            for turn in spec["turns"].values():
                self.assertLessEqual(turn["prompt"]["size_bytes"], MAX_PROMPT_BYTES)

    async def test_freeze_is_idempotent_after_capacity_policy_exists(self):
        with tempfile.TemporaryDirectory() as temp:
            presemantic = Path(temp) / "presemantic"
            output = Path(temp) / "repair"
            build_v25_presemantic_recovery(output_root=presemantic)
            first = freeze_v25_repair_diagnostic(
                output_dir=output,
                presemantic_root=presemantic,
            )
            second = freeze_v25_repair_diagnostic(
                output_dir=output,
                presemantic_root=presemantic,
            )
        self.assertEqual(first["spec"], second["spec"])

    async def test_perfect_fake_repair_diagnostic_passes_and_authorizes_full_calibration(self):
        with tempfile.TemporaryDirectory() as temp:
            presemantic = Path(temp) / "presemantic"
            build_v25_presemantic_recovery(output_root=presemantic)
            frozen = freeze_v25_repair_diagnostic(
                output_dir=Path(temp) / "repair",
                presemantic_root=presemantic,
            )
            client = FakeV25RepairClient(frozen["truth"])

            def factory(_policy_path):
                return client

            terminal = await run_v25_repair_diagnostic(
                output_dir=Path(temp) / "repair",
                presemantic_root=presemantic,
                client_factory=factory,
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertEqual(
            terminal["terminal_reason"],
            "v25_repair_diagnostic_passed_full_calibration_authorized",
        )
        self.assertTrue(terminal["diagnostic_passed"])
        self.assertTrue(terminal["full_calibration_authorized"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertFalse(terminal["holdout_authorized"])
        self.assertFalse(terminal["production_mutated"])
        self.assertEqual(terminal["turn_count"], 6)
        self.assertEqual(terminal["usage"]["total_tokens"], 7200)
        self.assertEqual(len(client.calls), 6)
        self.assertEqual(client.enter_count, 1)
        self.assertEqual(client.exit_count, 1)

    async def test_failed_repair_diagnostic_emits_inactive_non_acceptance(self):
        with tempfile.TemporaryDirectory() as temp:
            presemantic = Path(temp) / "presemantic"
            repair = Path(temp) / "repair"
            build_v25_presemantic_recovery(output_root=presemantic)
            frozen = freeze_v25_repair_diagnostic(
                output_dir=repair,
                presemantic_root=presemantic,
            )
            client = ImperfectV25RepairClient(frozen["truth"])

            def factory(_policy_path):
                return client

            terminal = await run_v25_repair_diagnostic(
                output_dir=repair,
                presemantic_root=presemantic,
                client_factory=factory,
            )
            non_acceptance = json.loads((repair / "non-acceptance.json").read_text())
            next_terminal = json.loads(
                (
                    Path(temp)
                    / "judge-calibration-v5_4-v26-presemantic-design"
                    / "terminal.json"
                ).read_text()
            )
        self.assertEqual(terminal["state"], "inactive")
        self.assertEqual(terminal["terminal_reason"], "inactive_incomplete_recovery_required")
        self.assertEqual(
            terminal["development_terminal_reason"],
            "v25_repair_diagnostic_quality_gate_not_passed",
        )
        self.assertFalse(terminal["diagnostic_passed"])
        self.assertFalse(terminal["full_calibration_authorized"])
        self.assertTrue(terminal["semantic_attempt_started"])
        self.assertEqual(terminal["total_tokens"], 7200)
        self.assertEqual(non_acceptance["state"], "inactive")
        self.assertEqual(
            non_acceptance["terminal_reason"],
            "inactive_incomplete_recovery_required",
        )
        self.assertFalse(non_acceptance["acceptance_receipt_allowed"])
        self.assertEqual(next_terminal["state"], "inactive")
        self.assertFalse(next_terminal["semantic_attempt_authorized"])


if __name__ == "__main__":
    unittest.main()
