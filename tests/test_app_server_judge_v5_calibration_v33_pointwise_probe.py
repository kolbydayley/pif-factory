from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
    load_reserve_capacity_policy,
)
from research_factory.app_server_judge_v5_calibration_v33_pointwise_probe import (
    DEFAULT_V31_ROOT,
    V33_POINTWISE_SPEC_VERSION,
    freeze_v33_pointwise_probe,
    run_v33_pointwise_probe,
)
from research_factory.app_server_judge_v5_diagnostic import _sha256_file
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeV33PointwiseClient:
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
        remaining = max(1, 3 - len(self.calls))
        payload = {
            "schema_version": RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": "2026-07-14T00:00:00Z",
            "policy_path": str(policy_path),
            "policy_sha256": _sha256_file(policy_path),
            "phase_id": "judge_v5_4_v33_pointwise_probe",
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
            "remaining_turn_count": remaining,
            "projected_remaining_tokens": remaining * 70000,
            "quota_points_per_million_tokens": 17,
            "projected_remaining_quota_points": 4,
            "projected_terminal_remaining_percent": 93,
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

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(kwargs)
        self._capacity(kwargs["capacity_checkpoint_path"])
        input_value = json.loads(
            Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text()
        )
        output = self._pointwise_output(input_value)
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


class JudgeV5CalibrationV33PointwiseProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_freeze_binds_v31_failure_and_declares_pointwise_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            output_dir = Path(temp) / "v33"
            frozen = freeze_v33_pointwise_probe(output_dir=output_dir)
            frozen_again = freeze_v33_pointwise_probe(output_dir=output_dir)
            policy = load_reserve_capacity_policy(frozen["capacity_policy"])
            spec = frozen["spec"]
        self.assertEqual(frozen_again["spec"], spec)
        self.assertEqual(spec["schema_version"], V33_POINTWISE_SPEC_VERSION)
        self.assertEqual(spec["predecessor_v31_replayed"], False)
        self.assertEqual(len(spec["turn_plan"]), 3)
        self.assertEqual(policy["phase_id"], "judge_v5_4_v33_pointwise_probe")
        self.assertEqual(
            spec["v31_predecessor"]["failure"]["sha256"],
            _sha256_file(DEFAULT_V31_ROOT / "failure.json"),
        )

    async def test_perfect_fake_v33_pointwise_probe_passes_but_does_not_authorize_calibration(self):
        with tempfile.TemporaryDirectory() as temp:
            frozen = freeze_v33_pointwise_probe(output_dir=Path(temp) / "v33")
            client = FakeV33PointwiseClient(frozen["truth"])

            def factory(_policy_path):
                return client

            terminal = await run_v33_pointwise_probe(
                output_dir=Path(temp) / "v33",
                client_factory=factory,
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["pointwise_probe_passed"])
        self.assertTrue(terminal["full_diagnostic_authorized"])
        self.assertFalse(terminal["full_calibration_authorized"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertFalse(terminal["holdout_authorized"])
        self.assertFalse(terminal["production_mutated"])
        self.assertEqual(terminal["turn_count"], 3)
        self.assertEqual(terminal["usage"]["total_tokens"], 3600)
        self.assertEqual(len(client.calls), 3)


if __name__ == "__main__":
    unittest.main()
