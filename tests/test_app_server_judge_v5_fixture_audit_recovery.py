from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from research_factory.app_server_judge_v5 import build_neutral_alignment_input
from research_factory.app_server_judge_v5_fixture_audit_recovery import (
    build_v2_adoption_receipt,
    find_scoreable_alignment_disagreements,
    normalize_scoreable_alignment_output,
    run_fixture_audit_recovery,
)
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeRecoveryClient:
    def __init__(self, v2_root: Path):
        self.v2_root = v2_root
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(Path(kwargs["sidecar_path"]).parent.name)
        input_value = json.loads(
            Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text()
        )
        case_id = input_value["alignment_input"]["cases"][0]["case_id"]
        canary = json.loads(
            (
                self.v2_root
                / "turns/neutral-alignment-canary/output.private.json"
            ).read_text()
        )
        output = {
            "cases": [
                next(row for row in canary["cases"] if row["case_id"] == case_id)
            ]
        }
        capacity = {
            "schema_version": CAPACITY_CHECKPOINT_VERSION,
            "primary_used_percent": 2,
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
            Path(kwargs["capacity_checkpoint_path"]),
            _canonical_json(capacity) + "\n",
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
            "output_schema_sha256": sha256_text(
                _canonical_json(kwargs["output_schema"])
            ),
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
            Path(kwargs["sidecar_path"]), _canonical_json(sidecar) + "\n"
        )
        return SimpleNamespace(
            status_ok=True, output=output, error_class=None, status="completed"
        )


class FixtureAuditRecoveryTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.v2_root = (
            Path(__file__).resolve().parents[1]
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "fixture-truth-audit-gpt55-v2"
        )

    def test_complete_v2_outputs_are_adopted_without_semantic_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = build_v2_adoption_receipt(
                v2_root=self.v2_root,
                output_path=Path(directory) / "adoption.json",
            )
        self.assertEqual(receipt["source_attempt_count"], 23)
        self.assertEqual(receipt["source_usage"]["total_tokens"], 827876)
        self.assertTrue(receipt["all_completed_outputs_adopted"])
        self.assertFalse(receipt["source_output_selection_performed"])
        self.assertFalse(receipt["semantic_fields_modified"])
        self.assertEqual(receipt["scoreable_root_omission_count"], 3)
        self.assertEqual(receipt["observable_disagreement_case_count"], 1)

    def test_scoreable_normalization_finds_only_the_permutation_disagreement(self):
        load = lambda name: json.loads((self.v2_root / name).read_text())
        pool = load("shared-witness-pool.private.json")
        receipts = load("support-receipts.private.json")
        truth = load("provisional-calibration-truth.private.json")
        base = load("base-alignment-full.private.json")
        canary = json.loads(
            (
                self.v2_root
                / "turns/neutral-alignment-canary/output.private.json"
            ).read_text()
        )
        base_input = build_neutral_alignment_input(pool, receipts)
        canary_input = build_neutral_alignment_input(
            pool,
            receipts,
            case_ids=truth["canary_case_ids"],
            permutation="balanced_canary",
        )
        normalized = normalize_scoreable_alignment_output(base, base_input)
        self.assertTrue(normalized["scoreable_root_omissions_preserved_as_model_errors"])
        disagreements = find_scoreable_alignment_disagreements(
            base_output=base,
            base_input=base_input,
            canary_output=canary,
            canary_input=canary_input,
            support_receipts=receipts,
        )
        self.assertEqual(disagreements["disagreement_case_count"], 1)
        self.assertEqual(
            disagreements["disagreements"][0]["reasons"],
            ["permutation_output_changed"],
        )

    async def test_recovery_runs_only_one_new_turn_and_never_authorizes_selection(self):
        client = FakeRecoveryClient(self.v2_root)
        with tempfile.TemporaryDirectory() as directory:
            terminal = await run_fixture_audit_recovery(
                v2_root=self.v2_root,
                output_dir=Path(directory),
                client_factory=lambda: client,
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["fixture_truth_proposal_completed"])
        self.assertFalse(terminal["calibration_passed"])
        self.assertFalse(terminal["reference_freeze_authorized"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(terminal["source_v2_usage"]["total_tokens"], 827876)
        self.assertEqual(terminal["new_recovery_usage"]["total_tokens"], 1200)
        self.assertEqual(terminal["cumulative_proposal_usage"]["total_tokens"], 829076)


if __name__ == "__main__":
    unittest.main()
