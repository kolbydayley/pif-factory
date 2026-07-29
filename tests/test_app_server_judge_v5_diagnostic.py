from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_diagnostic import (
    DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH,
    DEFAULT_REUSE_CONTRACT_PATH,
    DIAGNOSTIC_RUN_VERSION,
    JudgeV5DiagnosticError,
    MAX_OUTPUT_SCHEMA_BYTES,
    MAX_PROMPT_BYTES,
    freeze_v5_diagnostic_protocol,
    run_v5_judge_diagnostic,
)
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _perfect_pointwise(pointwise_input, expected):
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
                    "structured_field_verdict": truth["structured_fields"][witness_id],
                    "field_issue_fields": truth["field_issues"][witness_id],
                    "field_evidence_spans": [
                        unit["structured_event"]["evidence"]
                    ],
                    "field_rationale": "Audited diagnostic truth.",
                }
            )
    return {"units": rows}


def _perfect_alignment(alignment_input, expected):
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


class FakePersistentCapacityClient:
    def __init__(self, *, expected, fail_call=None):
        self.expected = expected
        self.fail_call = fail_call
        self.enter_count = 0
        self.exit_count = 0
        self.calls = []
        self.events = []

    async def __aenter__(self):
        self.enter_count += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exit_count += 1
        return None

    def _capacity(self, path):
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
        write_text_atomic(
            Path(path), json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n"
        )

    async def run_ephemeral_structured_turn(self, **kwargs):
        call_number = len(self.calls) + 1
        turn_name = Path(kwargs["sidecar_path"]).parent.name.replace("-", "_")
        self.calls.append({"turn_name": turn_name, **kwargs})
        self._capacity(kwargs["capacity_checkpoint_path"])
        self.events.append((turn_name, "capacity_cleared"))
        if call_number == self.fail_call:
            sidecar = self._sidecar(kwargs, state="failed", usage=None, output_text=None)
            write_text_atomic(
                Path(kwargs["sidecar_path"]),
                json.dumps(sidecar, ensure_ascii=True, sort_keys=True) + "\n",
            )
            self.events.append((turn_name, "turn_failed"))
            return SimpleNamespace(
                status_ok=False,
                output=None,
                error_class="serverOverloaded",
                status="failed",
            )

        input_value = json.loads(Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text())
        if turn_name == "pointwise_support":
            output = _perfect_pointwise(input_value, self.expected)
        elif turn_name in {"neutral_alignment_base", "neutral_alignment_canary"}:
            output = _perfect_alignment(input_value, self.expected)
        else:
            alignment_input = input_value["alignment_input"]
            output = _perfect_alignment(alignment_input, self.expected)
        output_text = _canonical_json(output)
        write_text_atomic(Path(kwargs["output_path"]), output_text + "\n")
        sidecar = self._sidecar(
            kwargs,
            state="completed",
            usage={
                "input_tokens": 1000,
                "cached_input_tokens": 100,
                "output_tokens": 200,
                "reasoning_output_tokens": 50,
                "total_tokens": 1200,
            },
            output_text=output_text,
        )
        write_text_atomic(
            Path(kwargs["sidecar_path"]),
            json.dumps(sidecar, ensure_ascii=True, sort_keys=True) + "\n",
        )
        self.events.append((turn_name, "turn_completed"))
        return SimpleNamespace(
            status_ok=True,
            output=output,
            error_class=None,
            status="completed",
        )

    @staticmethod
    def _sidecar(kwargs, *, state, usage, output_text):
        return {
            "state": state,
            "status": "completed" if state == "completed" else "failed",
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
            "output_sha256": sha256_text(output_text) if output_text is not None else None,
            "usage_complete": usage is not None,
            "usage_status": "measured" if usage is not None else "unknown",
            "usage": usage,
            "wall_elapsed_seconds": 1.25,
            "error_class": None if state == "completed" else "serverOverloaded",
        }


class JudgeV5DiagnosticRunnerTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if not DEFAULT_REUSE_CONTRACT_PATH.is_file():
            raise unittest.SkipTest("pipeline-v5 predecessor contract is unavailable")
        if not DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH.is_file():
            raise unittest.SkipTest("pipeline-v5 fixture audit receipt is unavailable")

    async def test_freeze_is_side_free_bounded_and_performs_no_model_call(self):
        with tempfile.TemporaryDirectory() as directory:
            frozen = freeze_v5_diagnostic_protocol(
                output_dir=Path(directory),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
            )
            spec = frozen["spec"]
            self.assertEqual(spec["schema_version"], DIAGNOSTIC_RUN_VERSION)
            self.assertEqual(spec["semantic_model_calls_performed_during_freeze"], 0)
            self.assertEqual(spec["support_permutations"], 1)
            self.assertFalse(spec["full_side_labelled_ab_ba_support_allowed"])
            self.assertEqual(spec["permutation_canary_case_count"], 6)
            self.assertEqual(spec["adjudication_call_cap"], 1)
            self.assertEqual(len(frozen["pointwise_input"]["units"]), 36)
            rendered = json.dumps(frozen["pointwise_input"], sort_keys=True)
            self.assertNotIn("event_set_a", rendered)
            self.assertNotIn("event_set_b", rendered)
            self.assertLessEqual(
                len(frozen["pointwise_prompt"].encode("utf-8")), MAX_PROMPT_BYTES
            )
            self.assertLessEqual(
                len(_canonical_json(frozen["pointwise_schema"]).encode("utf-8")),
                MAX_OUTPUT_SCHEMA_BYTES,
            )

    async def test_three_turn_success_uses_one_context_and_capacity_before_each_turn(self):
        with tempfile.TemporaryDirectory() as directory:
            frozen = freeze_v5_diagnostic_protocol(
                output_dir=Path(directory),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
            )
            client = FakePersistentCapacityClient(expected=frozen["expected"])
            terminal = await run_v5_judge_diagnostic(
                output_dir=Path(directory), client_factory=lambda: client
            )
            self.assertEqual(terminal["state"], "completed")
            self.assertTrue(terminal["diagnostic_passed"])
            self.assertTrue(terminal["full_calibration_authorized"])
            self.assertEqual(client.enter_count, 1)
            self.assertEqual(client.exit_count, 1)
            self.assertEqual(
                [item["turn_name"] for item in client.calls],
                [
                    "pointwise_support",
                    "neutral_alignment_base",
                    "neutral_alignment_canary",
                ],
            )
            self.assertEqual(
                client.events,
                [
                    ("pointwise_support", "capacity_cleared"),
                    ("pointwise_support", "turn_completed"),
                    ("neutral_alignment_base", "capacity_cleared"),
                    ("neutral_alignment_base", "turn_completed"),
                    ("neutral_alignment_canary", "capacity_cleared"),
                    ("neutral_alignment_canary", "turn_completed"),
                ],
            )
            self.assertEqual(terminal["turn_count"], 3)
            self.assertEqual(terminal["usage"]["total_tokens"], 3600)
            self.assertEqual(terminal["semantic_retry_count"], 0)
            self.assertFalse(
                (Path(directory) / "turns/disagreement-adjudication").exists()
            )

    async def test_unknown_usage_failure_blocks_version_without_starting_later_turn(self):
        with tempfile.TemporaryDirectory() as directory:
            frozen = freeze_v5_diagnostic_protocol(
                output_dir=Path(directory),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
            )
            client = FakePersistentCapacityClient(
                expected=frozen["expected"], fail_call=2
            )
            terminal = await run_v5_judge_diagnostic(
                output_dir=Path(directory), client_factory=lambda: client
            )
            self.assertEqual(terminal["state"], "failed")
            self.assertEqual(
                terminal["terminal_reason"],
                "infrastructure_or_judge_attempt_failed",
            )
            self.assertEqual(
                [item["turn_name"] for item in client.calls],
                ["pointwise_support", "neutral_alignment_base"],
            )
            failure = json.loads((Path(directory) / "failure.json").read_text())
            self.assertEqual(failure["usage_status"], "unknown")
            self.assertIsNone(failure["usage"])
            self.assertEqual(failure["known_usage_turn_count"], 1)
            self.assertEqual(failure["unknown_usage_turn_count"], 1)
            self.assertFalse(failure["retry_allowed_in_this_version"])
            second_client = FakePersistentCapacityClient(expected=frozen["expected"])
            repeated = await run_v5_judge_diagnostic(
                output_dir=Path(directory), client_factory=lambda: second_client
            )
            self.assertEqual(repeated, terminal)
            self.assertEqual(second_client.enter_count, 0)

    async def test_terminal_artifact_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            frozen = freeze_v5_diagnostic_protocol(
                output_dir=Path(directory),
                model="gpt-5.6-sol",
                reasoning_effort="high",
                timeout_seconds=1200.0,
            )
            client = FakePersistentCapacityClient(expected=frozen["expected"])
            await run_v5_judge_diagnostic(
                output_dir=Path(directory), client_factory=lambda: client
            )
            prompt_path = Path(directory) / "turns/pointwise-support/prompt.private.md"
            prompt_path.write_text(prompt_path.read_text() + "drift", encoding="utf-8")
            with self.assertRaisesRegex(JudgeV5DiagnosticError, "drifted"):
                await run_v5_judge_diagnostic(
                    output_dir=Path(directory),
                    client_factory=lambda: FakePersistentCapacityClient(
                        expected=frozen["expected"]
                    ),
                )

    async def test_separate_roots_are_separate_declared_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            roots = [Path(directory) / "version-a", Path(directory) / "version-b"]
            clients = []
            for root in roots:
                frozen = freeze_v5_diagnostic_protocol(
                    output_dir=root,
                    model="gpt-5.6-sol",
                    reasoning_effort="high",
                    timeout_seconds=1200.0,
                )
                client = FakePersistentCapacityClient(expected=frozen["expected"])
                clients.append(client)
                terminal = await run_v5_judge_diagnostic(
                    output_dir=root, client_factory=lambda client=client: client
                )
                self.assertEqual(terminal["state"], "completed")
            self.assertEqual([len(client.calls) for client in clients], [3, 3])
            self.assertNotEqual(
                str(roots[0] / "terminal.json"), str(roots[1] / "terminal.json")
            )


class JudgeV5ChecklistCoverageTest(unittest.TestCase):
    def test_runner_depends_on_the_exact_fifteen_row_checklist(self):
        self.assertEqual(len(CHECKLIST_FIELDS), 15)
        self.assertEqual(
            CHECKLIST_FIELDS,
            (
                "actor",
                "attribution",
                "causal_mechanism",
                "certainty",
                "event_boundary",
                "event_type",
                "evidence",
                "metric",
                "negation",
                "reported_actor",
                "speaker",
                "stance",
                "target",
                "temporal_horizon",
                "unsupported_inference",
            ),
        )


if __name__ == "__main__":
    unittest.main()
