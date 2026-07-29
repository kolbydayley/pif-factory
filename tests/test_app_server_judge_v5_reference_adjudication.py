from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from research_factory.app_server_judge_v5_reference_adjudication import (
    build_reference_disagreement_manifest,
    run_reference_adjudication,
)
from research_factory.codex_app_server import APP_SERVER_CLIENT_VERSION
from research_factory.util import sha256_text, write_text_atomic


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class FakeReferenceClient:
    def __init__(self, v2_root: Path):
        self.v2_root = v2_root
        self.proposal = json.loads(
            (v2_root / "pointwise-output-full.private.json").read_text()
        )
        self.proposal_by_id = {
            row["witness_id"]: row for row in self.proposal["units"]
        }
        self.truth = json.loads(
            (v2_root / "provisional-calibration-truth.private.json").read_text()
        )
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def _pointwise(self, value):
        return {
            "units": [
                self.proposal_by_id[unit["witness_id"]] for unit in value["units"]
            ]
        }

    def _alignment(self, value):
        cases = []
        for case in value["cases"]:
            truth = self.truth["cases"][case["case_id"]]
            support = {
                witness["witness_id"]: witness["support_receipt"]
                for witness in case["witnesses"]
            }
            pairs = []
            for pair in truth["pairs"]:
                different = set(pair["mismatch_fields"])
                first_support = support[pair["witness_ids"][0]][
                    "proposition_verdict"
                ]
                second_support = support[pair["witness_ids"][1]][
                    "proposition_verdict"
                ]
                if first_support == second_support:
                    different.discard("unsupported_inference")
                else:
                    different.add("unsupported_inference")
                    if not different - {
                        "unsupported_inference",
                        "event_boundary",
                        "evidence",
                    }:
                        different.add("target")
                relation = (
                    "equivalent"
                    if not different
                    else "partial"
                    if different == {"event_boundary", "evidence"}
                    else "non_equivalent"
                )
                pairs.append(
                    {
                        "witness_id_1": pair["witness_ids"][0],
                        "witness_id_2": pair["witness_ids"][1],
                        "relation": relation,
                        "checklist": [
                            {
                                "field": field,
                                "decision": "different" if field in different else "same",
                                "source_evidence_spans": [],
                                "witness_evidence_ids": pair["witness_ids"],
                                "rationale": "Independent fixture adjudication.",
                            }
                            for field in value["checklist_field_order"]
                        ],
                        "rationale": "Independent fixture adjudication.",
                    }
                )
            cases.append(
                {
                    "case_id": case["case_id"],
                    "equivalence_groups": [
                        {
                            "witness_ids": group,
                            "rationale": "Independent fixture adjudication.",
                        }
                        for group in truth["equivalence_groups"]
                    ],
                    "alignment_pairs": pairs,
                    "unpaired_witness_ids": truth["unpaired_witness_ids"],
                }
            )
        return {"cases": cases}

    async def run_ephemeral_structured_turn(self, **kwargs):
        turn_name = Path(kwargs["sidecar_path"]).parent.name.replace("-", "_")
        self.calls.append(turn_name)
        value = json.loads(
            Path(kwargs["sidecar_path"]).with_name("input.private.json").read_text()
        )
        output = (
            self._pointwise(value)
            if turn_name.startswith("reference_pointwise_shard")
            else self._alignment(value)
        )
        capacity = {
            "schema_version": CAPACITY_CHECKPOINT_VERSION,
            "primary_used_percent": 1,
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
            "wall_elapsed_seconds": 1.0,
            "error_class": None,
        }
        write_text_atomic(
            Path(kwargs["sidecar_path"]), _canonical_json(sidecar) + "\n"
        )
        return SimpleNamespace(
            status_ok=True, output=output, error_class=None, status="completed"
        )


class ReferenceAdjudicationTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[1]
        cls.v2 = (
            repo
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "fixture-truth-audit-gpt55-v2"
        )
        cls.v3 = (
            repo
            / "work/app-server-development-v2/unattended-pipeline-v5/"
            "fixture-truth-audit-gpt55-v3"
        )

    def test_manifest_freezes_exact_disputes_in_six_case_shards(self):
        manifest = build_reference_disagreement_manifest(
            v2_root=self.v2, v3_root=self.v3
        )
        self.assertEqual(manifest["pointwise_disputed_witness_count"], 102)
        self.assertEqual(manifest["pointwise_disputed_case_count"], 49)
        self.assertEqual(manifest["alignment_disputed_case_count"], 18)
        self.assertEqual(len(manifest["pointwise_shards"]), 9)
        self.assertEqual(len(manifest["alignment_shards"]), 3)
        self.assertTrue(
            all(
                len(shard) <= 6
                for shard in manifest["pointwise_shards"]
                + manifest["alignment_shards"]
            )
        )
        self.assertFalse(manifest["candidate_labels_exposed_to_adjudicator"])
        self.assertFalse(manifest["model_identities_exposed_to_adjudicator"])
        self.assertFalse(manifest["majority_voting_allowed"])

    async def test_full_fake_adjudication_freezes_reference_without_selection(self):
        client = FakeReferenceClient(self.v2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = await run_reference_adjudication(
                v2_root=self.v2,
                v3_root=self.v3,
                output_dir=root,
                client_factory=lambda: client,
            )
            spec = json.loads((root / "reference-adjudication-spec.json").read_text())
            prompt_texts = [path.read_text() for path in root.glob("turns/*/prompt.private.md")]
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["reference_frozen"])
        self.assertTrue(terminal["fresh_calibration_authorized"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertEqual(len(client.calls), 12)
        self.assertEqual(terminal["turn_count"], 12)
        self.assertEqual(terminal["usage"]["total_tokens"], 14400)
        self.assertFalse(spec["candidate_labels_exposed_to_adjudicator"])
        self.assertFalse(spec["model_identities_exposed_to_adjudicator"])
        self.assertTrue(all("provisional" not in prompt for prompt in prompt_texts))
        self.assertTrue(all("gpt-5.5" not in prompt for prompt in prompt_texts))


if __name__ == "__main__":
    unittest.main()
