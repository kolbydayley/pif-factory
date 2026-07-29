from __future__ import annotations

import json
import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from research_factory.app_server_checkpoint import verify_instruction_contract
from research_factory.app_server_holdout_client import (
    HOLDOUT_SIDECAR_LINEAGE_FIELD,
    verified_holdout_execution_lineage,
)
from research_factory.app_server_llm_judge import (
    JudgeAttemptFailed,
    JudgeArtifactError,
    JUDGE_VARIANT_VERSION,
    NEUTRAL_EVENT_KEYS,
    build_judge_prompt,
    build_judge_variants,
    combine_judge_consensus,
    make_v2_calibration_pool,
    make_shared_witness_pool,
    run_app_server_semantic_judge,
    score_calibration_variant,
    score_named_systems_against_shared_reference,
    semantic_judge_output_schema,
    validate_judge_output,
    validate_app_server_output_schema_subset,
    validate_shared_witness_pool,
    write_immutable_json,
)
from research_factory.codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
    TURN_SIDECAR_SCHEMA_VERSION,
)
from research_factory.util import sha256_text


def raw_cases() -> list[dict]:
    source = (
        "Alice says the assistant saves two hours weekly. "
        "Bob warns that it can miss rare invoices."
    )
    return [
        {
            "case_key": "private-case-key",
            "source_excerpt": source,
            "event_set_a": [
                {
                    "event": {
                        "actor": "Alice",
                        "claim": "The assistant saves two hours weekly.",
                        "evidence": "Alice says the assistant saves two hours weekly.",
                    },
                    "provenance": {
                        "system_id": "system-a",
                        "system_name": "secret-arm-alpha",
                    },
                }
            ],
            "event_set_b": [
                {
                    "event": {
                        "actor": "Alice",
                        "claim": "Two hours are saved each week by the assistant.",
                        "evidence": "Alice says the assistant saves two hours weekly.",
                    },
                    "provenance": {
                        "system_id": "system-b",
                        "system_name": "secret-arm-beta",
                    },
                },
                {
                    "event": {
                        "actor": "Bob",
                        "claim": "The assistant can miss rare invoices.",
                        "evidence": "Bob warns that it can miss rare invoices.",
                    },
                    "provenance": {
                        "system_id": "system-b",
                        "system_name": "secret-arm-beta",
                    },
                },
            ],
            "provenance": {"segment_id": "secret-segment"},
        }
    ]


def valid_output(variant: dict, *, pair_right_index: int = 0, relation: str = "equivalent") -> dict:
    rows = []
    for case in variant["cases"]:
        left = case["event_set_a"]
        right = case["event_set_b"]
        support = []
        for witness in left + right:
            support.append(
                {
                    "witness_id": witness["witness_id"],
                    "verdict": "supported",
                    "evidence_spans": [witness["event"]["evidence"]],
                    "rationale": "The full event is directly supported by the exact span.",
                }
            )
        alignments = []
        used_left = set()
        used_right = set()
        if left and right:
            selected_right = min(pair_right_index, len(right) - 1)
            alignments.append(
                {
                    "left_witness_id": left[0]["witness_id"],
                    "right_witness_id": right[selected_right]["witness_id"],
                    "relation": relation,
                    "mismatch_fields": [] if relation in {"equivalent", "abstain"} else ["target"],
                    "rationale": "The events express the same source-grounded event.",
                }
            )
            used_left.add(left[0]["witness_id"])
            used_right.add(right[selected_right]["witness_id"])
        grouped = []
        grouped_ids = set()
        if alignments and relation == "equivalent":
            pair = alignments[0]
            ids = sorted([pair["left_witness_id"], pair["right_witness_id"]])
            grouped.append(
                {"witness_ids": ids, "rationale": "These events have identical full meaning."}
            )
            grouped_ids.update(ids)
        for witness in left + right:
            if witness["witness_id"] not in grouped_ids:
                grouped.append(
                    {
                        "witness_ids": [witness["witness_id"]],
                        "rationale": "No other event is fully equivalent.",
                    }
                )
        rows.append(
            {
                "case_id": case["case_id"],
                "support_results": support,
                "equivalence_groups": grouped,
                "alignments": alignments,
                "unaligned_left_witness_ids": [
                    item["witness_id"] for item in left if item["witness_id"] not in used_left
                ],
                "unaligned_right_witness_ids": [
                    item["witness_id"] for item in right if item["witness_id"] not in used_right
                ],
            }
        )
    return {"cases": rows}


class FakeAppServerClient:
    def __init__(
        self,
        *,
        instruction_contract=None,
        execution_lineage=None,
    ) -> None:
        if (instruction_contract is None) != (execution_lineage is None):
            raise ValueError("fake judge lineage inputs are inseparable")
        self.calls = []
        self.instruction_contract = copy.deepcopy(instruction_contract)
        self.execution_lineage = copy.deepcopy(execution_lineage)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        marker = "# Blinded cases\n"
        packets = json.loads(kwargs["prompt"].split(marker, 1)[1])
        variant = {
            "schema_version": JUDGE_VARIANT_VERSION,
            "variant": "opaque",
            "orientation": "opaque",
            "cases": packets["cases"],
        }
        output = valid_output(variant)
        output_text = json.dumps(output, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        output_path = Path(kwargs["output_path"])
        sidecar_path = Path(kwargs["sidecar_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        sidecar = {
            "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "client_version": APP_SERVER_CLIENT_VERSION,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "app_server_user_agent": "fixture-codex-app-server",
            "thread_id": "thread-%04d" % (len(self.calls) + 1),
            "turn_id": "turn-%04d" % (len(self.calls) + 1),
            "thread_mode": "new_thread",
            "batch_size": kwargs["batch_size"],
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "prompt_sha256": sha256_text(kwargs["prompt"]),
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_sha256": sha256_text(kwargs["base_instructions"]),
            "base_instructions_bytes": len(
                kwargs["base_instructions"].encode("utf-8")
            ),
            "output_schema_sha256": sha256_text(
                json.dumps(
                    kwargs["output_schema"],
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            ),
            "output_schema_bytes": len(
                json.dumps(
                    kwargs["output_schema"],
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
            "output_sha256": sha256_text(output_text),
            "output_path": str(output_path.resolve()),
            "finished_at": "2026-07-12T01:00:00+00:00",
            "usage_complete": True,
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": 20,
                "output_tokens": 10,
                "reasoning_output_tokens": 3,
                "total_tokens": 110,
            },
        }
        if self.instruction_contract is not None:
            sidecar.update(
                {
                    "cli_version": PINNED_CODEX_CLI_VERSION,
                    "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
                    "instruction_sources_sha256": self.instruction_contract[
                        "expected_path_set_sha256"
                    ],
                    "instruction_sources_count": self.instruction_contract[
                        "instruction_sources_count"
                    ],
                    HOLDOUT_SIDECAR_LINEAGE_FIELD: copy.deepcopy(
                        self.execution_lineage
                    ),
                }
            )
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
        self.calls.append(kwargs)
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
        )


class SharedWitnessPoolTest(unittest.TestCase):
    def test_judge_schema_uses_only_app_server_structured_output_subset(self) -> None:
        pool, _ = make_shared_witness_pool(raw_cases())
        schema = semantic_judge_output_schema(build_judge_variants(pool)["ab"])
        rendered = json.dumps(schema, sort_keys=True)
        self.assertEqual(validate_app_server_output_schema_subset(schema), [])
        self.assertNotIn('"uniqueItems"', rendered)
        self.assertNotIn('"$schema"', rendered)
        self.assertNotEqual(
            validate_app_server_output_schema_subset(
                {"type": "array", "uniqueItems": True, "items": {"type": "string"}}
            ),
            [],
        )

    def test_pool_blinds_origin_and_projects_every_event_to_one_neutral_schema(self) -> None:
        pool, mapping = make_shared_witness_pool(raw_cases(), seed="fixed-seed")
        self.assertEqual(validate_shared_witness_pool(pool), [])
        self.assertRegex(pool["cases"][0]["case_id"], r"^jcase_[0-9a-f]{24}$")
        witness_id = pool["cases"][0]["event_set_a"][0]["witness_id"]
        self.assertRegex(witness_id, r"^wit_[0-9a-f]{24}$")
        self.assertNotIn("secret-arm-alpha", json.dumps(pool))
        self.assertIn("secret-arm-alpha", json.dumps(mapping))
        prompt = build_judge_prompt(build_judge_variants(pool)["ab"])
        self.assertNotIn("secret-arm-alpha", prompt)
        self.assertNotIn("secret-segment", prompt)
        for side in ("a", "b"):
            for witness in pool["cases"][0]["event_set_%s" % side]:
                self.assertEqual(tuple(witness["event"]), NEUTRAL_EVENT_KEYS)
                self.assertNotIn("system_id", witness["event"])
        self.assertIn("original_event", mapping["cases"][0]["witnesses"][0]["provenance"])

        changed = raw_cases()
        changed[0]["event_set_a"][0]["event"]["evidence"] = "not a source span"
        transported, transported_mapping = make_shared_witness_pool(changed)
        event = transported["cases"][0]["event_set_a"][0]["event"]
        self.assertEqual(event["evidence"], changed[0]["source_excerpt"])
        self.assertEqual(event["submitted_evidence"], "not a source span")
        self.assertFalse(event["submitted_evidence_exact"])
        self.assertEqual(
            transported_mapping["cases"][0]["witnesses"][0]["provenance"][
                "original_event"
            ]["evidence"],
            "not a source span",
        )

        unknown = raw_cases()
        unknown[0]["event_set_a"][0]["event"]["new_semantic_dimension"] = "leak"
        with self.assertRaisesRegex(ValueError, "unknown semantic event fields"):
            make_shared_witness_pool(unknown)

    def test_event_origin_metadata_inside_semantic_payload_is_rejected(self) -> None:
        changed = raw_cases()
        changed[0]["event_set_a"][0]["event"]["arm_id"] = "leaky-arm"
        with self.assertRaisesRegex(ValueError, "cannot enter a judge prompt"):
            make_shared_witness_pool(changed)

    def test_variants_keep_identical_case_order_and_reverse_the_event_sets(self) -> None:
        pool, _ = make_shared_witness_pool(raw_cases())
        variants = build_judge_variants(pool)
        self.assertEqual(
            [row["case_id"] for row in variants["ab"]["cases"]],
            [row["case_id"] for row in variants["ba"]["cases"]],
        )
        ab = variants["ab"]["cases"][0]
        ba = variants["ba"]["cases"][0]
        self.assertEqual(ab["event_set_a"], ba["event_set_b"])
        self.assertEqual(ab["event_set_b"], ba["event_set_a"])


class JudgeValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pool, _ = make_shared_witness_pool(raw_cases())
        self.variants = build_judge_variants(self.pool)

    def test_schema_and_validator_enforce_exact_id_partitions(self) -> None:
        schema = semantic_judge_output_schema(self.variants["ab"])
        self.assertEqual(schema["properties"]["cases"]["minItems"], 1)
        output = valid_output(self.variants["ab"])
        self.assertEqual(validate_judge_output(output, self.variants["ab"]), [])

        duplicate = json.loads(json.dumps(output))
        duplicate["cases"][0]["support_results"].append(
            duplicate["cases"][0]["support_results"][0]
        )
        self.assertTrue(
            any("duplicate" in error for error in validate_judge_output(duplicate, self.variants["ab"]))
        )

        missing = json.loads(json.dumps(output))
        missing["cases"][0]["unaligned_right_witness_ids"] = []
        self.assertIn(
            "case_0_right_id_partition_mismatch",
            validate_judge_output(missing, self.variants["ab"]),
        )

    def test_validator_rejects_non_exact_returned_evidence(self) -> None:
        output = valid_output(self.variants["ab"])
        output["cases"][0]["support_results"][0]["evidence_spans"] = ["invented quote"]
        self.assertTrue(
            any("evidence_not_exact" in error for error in validate_judge_output(output, self.variants["ab"]))
        )

    def test_consensus_turns_label_disagreements_into_abstentions(self) -> None:
        ab = valid_output(self.variants["ab"], relation="equivalent")
        ba = valid_output(self.variants["ba"], relation="partial")
        ba["cases"][0]["support_results"][0]["verdict"] = "unsupported"
        ba["cases"][0]["support_results"][0]["evidence_spans"] = []
        consensus = combine_judge_consensus(self.pool, {"ab": ab, "ba": ba})
        case = consensus["cases"][0]
        self.assertEqual(case["alignment_results"][0]["relation"], "abstain")
        self.assertIn("abstain", {row["verdict"] for row in case["support_results"]})
        self.assertTrue(consensus["selection_admissible"])
        self.assertEqual(consensus["abstentions"]["support"]["numerator"], 1)
        self.assertEqual(consensus["abstentions"]["support"]["denominator"], 3)
        self.assertEqual(consensus["abstentions"]["alignment_labels"]["rate"], 1.0)

    def test_consensus_abstains_the_whole_alignment_on_topology_disagreement(self) -> None:
        ab = valid_output(self.variants["ab"], pair_right_index=0)
        # In BA the original B events are on the left. Selecting left index 1
        # requires constructing a valid alternative topology explicitly.
        ba = valid_output(self.variants["ba"], pair_right_index=0)
        ba_case = ba["cases"][0]
        original_alignment = ba_case["alignments"][0]
        left_ids = [item["witness_id"] for item in self.variants["ba"]["cases"][0]["event_set_a"]]
        old_left = original_alignment["left_witness_id"]
        new_left = left_ids[1]
        original_alignment["left_witness_id"] = new_left
        ba_case["unaligned_left_witness_ids"] = [old_left]
        self.assertEqual(validate_judge_output(ba, self.variants["ba"]), [])
        consensus = combine_judge_consensus(self.pool, {"ab": ab, "ba": ba})
        case = consensus["cases"][0]
        self.assertEqual(case["status"], "abstain")
        self.assertEqual(case["alignment_results"], [])
        self.assertEqual(len(case["alignment_abstained_witness_ids"]), 3)
        self.assertEqual(consensus["abstentions"]["alignment_topology"]["numerator"], 1)

    def test_named_systems_are_scored_once_against_one_shared_reference(self) -> None:
        ab = valid_output(self.variants["ab"])
        ba = valid_output(self.variants["ba"])
        consensus = combine_judge_consensus(self.pool, {"ab": ab, "ba": ba})
        _, mapping = make_shared_witness_pool(raw_cases())
        score = score_named_systems_against_shared_reference(
            pool=self.pool,
            private_mapping=mapping,
            consensus=consensus,
        )
        self.assertEqual(score["reference_system_ids"], ["system-a", "system-b"])
        self.assertEqual(score["reference_unit_count"], 2)
        self.assertEqual(
            score["deduplication"],
            "llm_consensus_full_semantic_equivalence_groups_within_case",
        )
        self.assertEqual(score["systems"]["system-a"]["strict_reference_units_covered"], 1)
        self.assertEqual(score["systems"]["system-b"]["strict_reference_units_covered"], 2)
        self.assertEqual(score["systems"]["system-b"]["strict_recall"], 1.0)

    def test_same_side_equivalence_partition_is_orientation_invariant_or_abstains(self) -> None:
        source = "Alice says the assistant saves two hours weekly."
        cases = [
            {
                "case_key": "same-side",
                "source_excerpt": source,
                "event_set_a": [
                    {
                        "event_type": "capability_claim",
                        "claim_text": "Alice says the assistant saves two hours weekly.",
                        "evidence": source,
                    },
                    {
                        "event_type": "capability_claim",
                        "claim_text": "The assistant saves two hours per week, according to Alice.",
                        "evidence": source,
                    },
                ],
                "event_set_b": [],
            }
        ]
        pool, _mapping = make_shared_witness_pool(cases, seed="same-side-partition")
        variants = build_judge_variants(pool)
        ab = valid_output(variants["ab"])
        ba = valid_output(variants["ba"])
        ids = sorted(
            item["witness_id"] for item in pool["cases"][0]["event_set_a"]
        )
        group = [{"witness_ids": ids, "rationale": "Harmless wording only."}]
        ab["cases"][0]["equivalence_groups"] = group
        ba["cases"][0]["equivalence_groups"] = group
        consensus = combine_judge_consensus(pool, {"ab": ab, "ba": ba})
        self.assertEqual(consensus["cases"][0]["equivalence_groups"], [ids])
        self.assertEqual(
            consensus["abstentions"]["equivalence_partition"]["numerator"], 0
        )

        ba["cases"][0]["equivalence_groups"] = [
            {"witness_ids": [witness_id], "rationale": "Singleton."}
            for witness_id in ids
        ]
        abstained = combine_judge_consensus(pool, {"ab": ab, "ba": ba})
        self.assertEqual(abstained["cases"][0]["status"], "abstain")
        self.assertEqual(abstained["cases"][0]["equivalence_groups"], [])
        self.assertEqual(
            abstained["cases"][0]["partition_abstained_witness_ids"], ids
        )

    def test_two_empty_system_sets_need_no_structural_sentinel(self) -> None:
        pool, _mapping = make_shared_witness_pool(
            [
                {
                    "case_key": "empty",
                    "source_excerpt": "There is no in-scope research claim here.",
                    "event_set_a": [],
                    "event_set_b": [],
                }
            ],
            seed="empty-case",
        )
        self.assertEqual(validate_shared_witness_pool(pool), [])
        variants = build_judge_variants(pool)
        outputs = {name: valid_output(variants[name]) for name in ("ab", "ba")}
        consensus = combine_judge_consensus(pool, outputs)
        self.assertEqual(consensus["cases"][0]["status"], "agreed")
        self.assertEqual(consensus["cases"][0]["support_results"], [])
        self.assertEqual(consensus["cases"][0]["equivalence_groups"], [])


class CalibrationPartitionTest(unittest.TestCase):
    def test_calibration_truth_and_scoring_cover_same_side_equivalence(self) -> None:
        pool, _mapping, expected = make_v2_calibration_pool()
        self.assertEqual(expected["synthetic_same_side_case_count"], 6)
        variants = build_judge_variants(pool)
        for orientation in ("ab", "ba"):
            rows = []
            for case in variants[orientation]["cases"]:
                truth = expected["cases"][case["case_id"]]
                witness_event = {
                    witness["witness_id"]: witness["event"]
                    for side in ("a", "b")
                    for witness in case["event_set_%s" % side]
                }
                support = []
                for witness_id, verdict in truth["support"].items():
                    support.append(
                        {
                            "witness_id": witness_id,
                            "verdict": verdict,
                            "evidence_spans": (
                                [witness_event[witness_id]["evidence"]]
                                if verdict == "supported"
                                else []
                            ),
                            "rationale": "Frozen calibration truth.",
                        }
                    )
                alignments = []
                used_left = set()
                used_right = set()
                for pair in truth["pairs"]:
                    left_id = pair["left_witness_id"]
                    right_id = pair["right_witness_id"]
                    if orientation == "ba":
                        left_id, right_id = right_id, left_id
                    alignments.append(
                        {
                            "left_witness_id": left_id,
                            "right_witness_id": right_id,
                            "relation": pair["relation"],
                            "mismatch_fields": pair["mismatch_fields"],
                            "rationale": "Frozen calibration truth.",
                        }
                    )
                    used_left.add(left_id)
                    used_right.add(right_id)
                left_ids = [item["witness_id"] for item in case["event_set_a"]]
                right_ids = [item["witness_id"] for item in case["event_set_b"]]
                rows.append(
                    {
                        "case_id": case["case_id"],
                        "support_results": support,
                        "equivalence_groups": [
                            {
                                "witness_ids": group,
                                "rationale": "Frozen full-equivalence truth.",
                            }
                            for group in truth["equivalence_groups"]
                        ],
                        "alignments": alignments,
                        "unaligned_left_witness_ids": [
                            item for item in left_ids if item not in used_left
                        ],
                        "unaligned_right_witness_ids": [
                            item for item in right_ids if item not in used_right
                        ],
                    }
                )
            output = {"cases": rows}
            score = score_calibration_variant(
                output, variant=variants[orientation], expected=expected
            )
            self.assertEqual(score["equivalence_partition_pairwise_f1"], 1.0)
            self.assertEqual(score["equivalence_partition_exact_case_rate"], 1.0)
            self.assertEqual(score["same_side_partition_exact_rate"], 1.0)


class ImmutableAndRunnerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    async def asyncTearDown(self) -> None:
        self.temp.cleanup()

    async def test_immutable_json_allows_identical_resume_and_rejects_drift(self) -> None:
        path = self.root / "immutable.json"
        self.assertTrue(write_immutable_json(path, {"a": 1}))
        self.assertFalse(write_immutable_json(path, {"a": 1}))
        with self.assertRaises(JudgeArtifactError):
            write_immutable_json(path, {"a": 2})

    async def test_runner_uses_two_managed_app_server_turns_and_resumes_without_rerun(self) -> None:
        pool, _ = make_shared_witness_pool(raw_cases())
        pool_path = self.root / "pool.private.json"
        pool_path.write_text(json.dumps(pool, sort_keys=True) + "\n", encoding="utf-8")
        fake = FakeAppServerClient()
        report = await run_app_server_semantic_judge(
            pool_path=pool_path,
            output_dir=self.root / "judge",
            client_factory=lambda: fake,
        )
        self.assertEqual(len(fake.calls), 2)
        self.assertTrue(all(call["thread_mode"] == "new_thread" for call in fake.calls))
        self.assertTrue(all(call["batch_size"] == 1 for call in fake.calls))
        self.assertTrue(report["accounting_complete"])
        self.assertEqual(report["usage"]["total_tokens"], 220)
        self.assertEqual(report["transport"], "official_codex_app_server_stdio_managed_chatgpt_auth")

        resumed = await run_app_server_semantic_judge(
            pool_path=pool_path,
            output_dir=self.root / "judge",
            client_factory=lambda: fake,
        )
        self.assertEqual(resumed, report)
        self.assertEqual(len(fake.calls), 2)

    async def test_holdout_ab_ba_each_return_support_and_alignment_with_no_consensus_call(self) -> None:
        pool, _ = make_shared_witness_pool(raw_cases())
        pool_path = self.root / "lineaged-pool.private.json"
        pool_path.write_text(json.dumps(pool, sort_keys=True) + "\n", encoding="utf-8")
        instruction_contract = verify_instruction_contract()
        execution_lineage = verified_holdout_execution_lineage(instruction_contract)
        fake = FakeAppServerClient(
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        judge_root = self.root / "lineaged-judge"

        report = await run_app_server_semantic_judge(
            pool_path=pool_path,
            output_dir=judge_root,
            client_factory=lambda: fake,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )

        self.assertEqual(len(fake.calls), 2)
        self.assertEqual(report["turn_semantic_outputs"], ["support", "alignment"])
        self.assertEqual(report["deterministic_consensus_additional_model_calls"], 0)
        spec = json.loads((judge_root / "judge-spec.json").read_text(encoding="utf-8"))
        self.assertEqual(spec["instruction_contract"], instruction_contract)
        self.assertEqual(spec["execution_lineage"], execution_lineage)
        self.assertEqual(spec["turn_semantic_outputs"], ["support", "alignment"])
        self.assertEqual(spec["deterministic_consensus_additional_model_calls"], 0)
        for orientation in ("ab", "ba"):
            output = json.loads(
                (judge_root / ("output-%s.private.json" % orientation)).read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(output["cases"])
            self.assertIn("support_results", output["cases"][0])
            self.assertIn("alignments", output["cases"][0])

    async def test_failed_ab_turn_prevents_ba_from_starting(self) -> None:
        class FailedABClient:
            def __init__(self) -> None:
                self.calls = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            async def run_ephemeral_structured_turn(self, **kwargs):
                self.calls.append(kwargs)
                sidecar_path = Path(kwargs["sidecar_path"])
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text(
                    json.dumps(
                        {
                            "state": "failed",
                            "status": "failed",
                            "usage_complete": False,
                            "usage_status": "unknown",
                            "usage": None,
                        },
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return SimpleNamespace(
                    status_ok=False,
                    status="failed",
                    output=None,
                    error_class="turn_failed",
                )

        pool, _ = make_shared_witness_pool(raw_cases())
        pool_path = self.root / "failed-pool.private.json"
        pool_path.write_text(json.dumps(pool, sort_keys=True) + "\n", encoding="utf-8")
        fake = FailedABClient()
        judge_root = self.root / "failed-judge"

        with self.assertRaises(JudgeAttemptFailed):
            await run_app_server_semantic_judge(
                pool_path=pool_path,
                output_dir=judge_root,
                client_factory=lambda: fake,
            )

        self.assertEqual(len(fake.calls), 1)
        self.assertTrue((judge_root / "sidecars/ab.json").is_file())
        self.assertFalse((judge_root / "sidecars/ba.json").exists())


if __name__ == "__main__":
    unittest.main()
