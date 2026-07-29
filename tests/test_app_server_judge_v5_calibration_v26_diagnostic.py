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
from research_factory.app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_PROMPT_BYTES,
    V26_DIAGNOSTIC_SPEC_VERSION,
    freeze_v26_staged_diagnostic,
    run_v26_staged_diagnostic,
)
from research_factory.app_server_judge_v5_diagnostic import _sha256_file
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeV26StagedClient:
    def __init__(self, truth):
        self.truth = truth
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
            "checked_at": "2026-07-14T00:00:00Z",
            "policy_path": str(policy_path),
            "policy_sha256": _sha256_file(policy_path),
            "phase_id": "judge_v5_4_v26_staged_diagnostic",
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
            "remaining_turn_count": 10 - len(self.calls),
            "projected_remaining_tokens": (10 - len(self.calls)) * 70000,
            "quota_points_per_million_tokens": 17,
            "projected_remaining_quota_points": 12,
            "projected_terminal_remaining_percent": 85,
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

    def _pointwise_output(self, input_value):
        units = []
        for unit in input_value["units"]:
            case_truth = self.truth["cases"][unit["case_id"]]
            witness_id = unit["witness_id"]
            proposition = case_truth["proposition"][witness_id]
            structured = case_truth["structured_fields"][witness_id]
            units.append(
                {
                    "case_id": unit["case_id"],
                    "witness_id": witness_id,
                    "proposition_verdict": proposition,
                    "proposition_evidence_spans": (
                        [unit["source_excerpt"][: min(40, len(unit["source_excerpt"]))]]
                        if proposition == "supported"
                        else []
                    ),
                    "proposition_rationale": "Frozen diagnostic truth.",
                    "structured_field_verdict": structured,
                    "field_issue_fields": case_truth["field_issues"][witness_id],
                    "field_evidence_spans": [],
                    "field_rationale": "Frozen diagnostic truth.",
                }
            )
        return {"units": units}

    def _alignment_output(self, input_value):
        cases = []
        for case in input_value["cases"]:
            truth = self.truth["cases"][case["case_id"]]
            witness_ids = {witness["witness_id"] for witness in case["witnesses"]}
            pairs = []
            for pair in truth["pairs"]:
                if not set(pair["witness_ids"]) <= witness_ids:
                    continue
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
                                "rationale": "Frozen diagnostic truth.",
                            }
                            for field in CHECKLIST_FIELDS
                        ],
                        "rationale": "Frozen diagnostic truth.",
                    }
                )
            cases.append(
                {
                    "case_id": case["case_id"],
                    "equivalence_groups": [
                        {"witness_ids": group, "rationale": "Frozen diagnostic truth."}
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
        output = (
            self._pointwise_output(input_value)
            if "units" in input_value
            else self._alignment_output(input_value)
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


class JudgeV5CalibrationV26DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_freeze_declares_staged_strategy_without_model_call(self):
        with tempfile.TemporaryDirectory() as temp:
            frozen = freeze_v26_staged_diagnostic(output_dir=Path(temp) / "v26")
            spec = frozen["spec"]
        self.assertEqual(spec["schema_version"], V26_DIAGNOSTIC_SPEC_VERSION)
        self.assertEqual(spec["strategy"], "staged_pointwise_support_then_alignment")
        self.assertEqual(spec["semantic_model_calls_performed_during_freeze"], 0)
        self.assertEqual(spec["case_count"], 18)
        self.assertEqual(len(spec["turn_plan"]), 10)
        for shard in spec["frozen_inputs"]["pointwise_shards"]:
            self.assertLessEqual(shard["prompt"]["size_bytes"], MAX_PROMPT_BYTES)

    async def test_perfect_fake_staged_diagnostic_passes_and_authorizes_full_calibration(self):
        with tempfile.TemporaryDirectory() as temp:
            frozen = freeze_v26_staged_diagnostic(output_dir=Path(temp) / "v26")
            client = FakeV26StagedClient(frozen["truth"])

            def factory(_policy_path):
                return client

            terminal = await run_v26_staged_diagnostic(
                output_dir=Path(temp) / "v26",
                client_factory=factory,
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertEqual(
            terminal["terminal_reason"],
            "v26_staged_diagnostic_passed_full_calibration_authorized",
        )
        self.assertTrue(terminal["diagnostic_passed"])
        self.assertTrue(terminal["full_calibration_authorized"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertFalse(terminal["holdout_authorized"])
        self.assertFalse(terminal["production_mutated"])
        self.assertEqual(terminal["turn_count"], 9)
        self.assertEqual(terminal["usage"]["total_tokens"], 10800)
        self.assertEqual(len(client.calls), 9)
        self.assertEqual(client.enter_count, 1)
        self.assertEqual(client.exit_count, 1)


if __name__ == "__main__":
    unittest.main()
