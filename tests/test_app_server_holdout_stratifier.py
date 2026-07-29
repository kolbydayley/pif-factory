from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research_factory.app_server_checkpoint import verify_instruction_contract
from research_factory.codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
    TURN_SIDECAR_SCHEMA_VERSION,
)
from research_factory.app_server_holdout import HOLDOUT_COVENANT_VERSION
from research_factory.app_server_capacity import CapacityGatedCodexAppServerClient
from research_factory.app_server_holdout_client import (
    HOLDOUT_SIDECAR_LINEAGE_FIELD,
    verified_holdout_execution_lineage,
)
from research_factory.app_server_holdout_execution import verify_stratified_selection
from research_factory.app_server_holdout_stratifier import (
    ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION,
    ATOMIC_REFERENCE_OUTPUT_VERSION,
    HOLDOUT_STRATIFIED_SELECTION_VERSION,
    STRATIFIER_REPORT_VERSION,
    SUPPORT_OUTPUT_VERSION,
    _load_reservoirs,
    _plan_extraction_batches,
    _plan_support_packets,
    _validate_terminal_stratifier_lineage,
    run_holdout_reference_stratifier,
)
from research_factory.util import sha256_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def canonical_bytes(value) -> bytes:
    return (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class FakeReferenceClient:
    instances = []

    def __init__(self, source_by_segment, event_counts, *, fail_segment=None):
        self.source_by_segment = source_by_segment
        self.event_counts = event_counts
        self.fail_segment = fail_segment
        self.calls = []
        self.entered = 0
        self.exited = 0
        self.__class__.instances.append(self)

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited += 1

    def _write_sidecar(self, path: Path, kwargs, *, failed: bool = False):
        path.parent.mkdir(parents=True, exist_ok=True)
        output_path = Path(kwargs["output_path"]).resolve()
        output_text = output_path.read_text(encoding="utf-8")
        contract = verify_instruction_contract()
        execution_lineage = verified_holdout_execution_lineage(contract)
        path.write_text(
            json.dumps(
                {
                    "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
                    "state": "failed" if failed else "completed",
                    "status": "failed" if failed else "completed",
                    "client_version": APP_SERVER_CLIENT_VERSION,
                    "cli_version": PINNED_CODEX_CLI_VERSION,
                    "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
                    "transport": "stdio",
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "app_server_user_agent": "fixture-codex-app-server",
                    "thread_id": "thread-%04d" % len(self.calls),
                    "turn_id": "turn-%04d" % len(self.calls),
                    "model": kwargs["model"],
                    "effort": kwargs["effort"],
                    "thread_mode": kwargs["thread_mode"],
                    "batch_size": kwargs["batch_size"],
                    "prompt_sha256": sha256_text(kwargs["prompt"]),
                    "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
                    "output_schema_sha256": sha256_text(
                        json.dumps(
                            kwargs["output_schema"],
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    ),
                    "base_instructions_sha256": sha256_text(
                        kwargs["base_instructions"]
                    ),
                    "base_instructions_bytes": len(
                        kwargs["base_instructions"].encode("utf-8")
                    ),
                    "output_schema_bytes": len(
                        json.dumps(
                            kwargs["output_schema"],
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ),
                    "instruction_sources_sha256": contract[
                        "expected_path_set_sha256"
                    ],
                    "instruction_sources_count": contract[
                        "instruction_sources_count"
                    ],
                    HOLDOUT_SIDECAR_LINEAGE_FIELD: execution_lineage,
                    "output_path": str(output_path),
                    "output_sha256": sha256_text(output_text),
                    "usage_complete": True,
                    "usage": {
                        "input_tokens": 10,
                        "cached_input_tokens": 2,
                        "output_tokens": 3,
                        "reasoning_output_tokens": 1,
                        "total_tokens": 13,
                    },
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(kwargs)
        schema_version = kwargs["output_schema"]["properties"]["schema_version"]["const"]
        if schema_version == ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION:
            output = self._extraction_batch_output(kwargs["prompt"])
            failed = any(
                row["segment_id"] == self.fail_segment for row in output["segments"]
            )
        else:
            output = self._support_output(kwargs["prompt"])
            failed = False
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, sort_keys=True) + "\n", encoding="utf-8")
        self._write_sidecar(Path(kwargs["sidecar_path"]), kwargs, failed=failed)
        if failed:
            raise RuntimeError("synthetic extraction failure")
        return SimpleNamespace(
            status_ok=True,
            output=output,
            error_class=None,
            status="completed",
        )

    def _extraction_batch_output(self, prompt):
        packet = json.loads(prompt.split("# Frozen source packets\n", 1)[1])
        return {
            "schema_version": ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION,
            "segments": [
                self._extraction_output(row["segment_id"], row["episode_id"])
                for row in packet["segments"]
            ],
        }

    def _extraction_output(self, segment_id, episode_id):
        source = self.source_by_segment[segment_id]
        count = self.event_counts[segment_id]
        events = []
        for index in range(count):
            events.append(
                {
                    "event_type": "capability_claim",
                    "atomic_claim": f"Atomic fixture event {index}",
                    "speaker": "Fixture speaker",
                    "actor": "Fixture actor",
                    "target": "Fixture target",
                    "stance": "neutral",
                    "certainty": "high",
                    "temporal_horizon": "present",
                    "evidence": source,
                    "evidence_start": 0,
                    "evidence_end": len(source),
                }
            )
        return {
            "schema_version": ATOMIC_REFERENCE_OUTPUT_VERSION,
            "segment_id": segment_id,
            "episode_id": episode_id,
            "extraction_status": "coded" if events else "no_signal",
            "coverage_truncated": False,
            "events": events,
            "no_signal_reason": None if events else "No in-scope atomic event is present.",
        }

    def _support_output(self, prompt):
        packet = json.loads(prompt.split("# Blinded reference packet\n", 1)[1])
        cases = []
        for case in packet["cases"]:
            witnesses = [*case["event_set_left"], *case["event_set_right"]]
            support_results = [
                {
                    "witness_id": witness["witness_id"],
                    "verdict": "supported",
                    "evidence_spans": [witness["event"]["evidence"]],
                    "rationale": "The complete atomic event is exactly source grounded.",
                }
                for witness in witnesses
            ]
            cases.append(
                {
                    "case_id": case["case_id"],
                    "support_results": support_results,
                    "coverage": {
                        "verdict": (
                            "complete_event_coverage" if witnesses else "clean_no_signal"
                        ),
                        "missing_event_evidence_spans": [],
                        "rationale": "The submitted atomic set completely covers the source.",
                    },
                }
            )
        return {"schema_version": SUPPORT_OUTPUT_VERSION, "cases": cases}


class CancelBeforeTurnReferenceClient(FakeReferenceClient):
    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(kwargs)
        raise asyncio.CancelledError()


class HoldoutStratifierTest(unittest.TestCase):
    def setUp(self):
        FakeReferenceClient.instances = []
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "label_packs").symlink_to(PROJECT_ROOT / "label_packs", target_is_directory=True)
        self.env = patch.dict(os.environ, {"RESEARCH_FACTORY_ROOT": str(self.root)})
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE segments (
              id TEXT PRIMARY KEY,
              episode_id TEXT,
              transcript_id TEXT,
              text_path TEXT,
              text_sha256 TEXT
            )
            """
        )
        self.segment_ids = ["paired_zero", "paired_low", "paired_medium", "paired_dense", "terminal_zero"]
        self.sources = {}
        self.frozen = {}
        for index, segment_id in enumerate(self.segment_ids):
            text = f"Exact frozen evidence for {segment_id}."
            self.sources[segment_id] = text
            path = self.root / "corpus" / f"{segment_id}.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
            text_hash = sha(text.encode())
            episode_id = f"episode_{index}"
            transcript_id = f"transcript_{index}"
            self.conn.execute(
                "INSERT INTO segments VALUES (?, ?, ?, ?, ?)",
                (segment_id, episode_id, transcript_id, str(path.relative_to(self.root)), text_hash),
            )
            self.frozen[segment_id] = {
                "segment_id": segment_id,
                "episode_id": episode_id,
                "transcript_id": transcript_id,
                "text_sha256": text_hash,
            }
        self.conn.commit()
        self.covenant_path = self._write_covenant()
        self.event_counts = {
            "paired_zero": 0,
            "paired_low": 4,
            "paired_medium": 5,
            "paired_dense": 16,
            "terminal_zero": 0,
        }

    def tearDown(self):
        self.conn.close()
        self.env.stop()
        self.tempdir.cleanup()

    def _write_covenant(self):
        root = self.root / "holdout"
        root.mkdir()
        paired = {
            "schema_version": "fixture_paired",
            "segments": [self.frozen[name] for name in self.segment_ids[:4]],
        }
        terminal = {
            "schema_version": "fixture_terminal",
            "segments": [self.frozen["terminal_zero"]],
        }
        exclusions = {"schema_version": "fixture_exclusions"}
        files = {
            "paired.json": paired,
            "terminal.json": terminal,
            "exclusions.json": exclusions,
        }
        for name, payload in files.items():
            (root / name).write_bytes(canonical_bytes(payload))
        covenant = {
            "schema_version": HOLDOUT_COVENANT_VERSION,
            "holdout_model_calls_authorized": True,
            "artifacts": {
                "paired_quality_reservoir": {
                    "filename": "paired.json",
                    "sha256": sha((root / "paired.json").read_bytes()),
                },
                "terminal_position_no_signal_candidates": {
                    "filename": "terminal.json",
                    "sha256": sha((root / "terminal.json").read_bytes()),
                },
                "exclusions": {
                    "filename": "exclusions.json",
                    "sha256": sha((root / "exclusions.json").read_bytes()),
                },
            },
        }
        path = root / "covenant.json"
        path.write_bytes(canonical_bytes(covenant))
        return path

    def run_async(self, coroutine):
        return asyncio.run(coroutine)

    def test_unauthorized_stratifier_stops_before_verifier_reservoir_or_sql_read(self):
        unauthorized = self.root / "unauthorized-stratifier-covenant.json"
        unauthorized.write_bytes(
            canonical_bytes(
                {
                    "schema_version": HOLDOUT_COVENANT_VERSION,
                    "holdout_model_calls_authorized": False,
                    "artifacts": {
                        "paired_quality_reservoir": {"filename": "must-not-open.json"}
                    },
                }
            )
        )
        statements = []
        self.conn.set_trace_callback(statements.append)
        try:
            with (
                patch(
                    "research_factory.app_server_holdout_stratifier.verify_frozen_holdout"
                ) as verifier,
                patch(
                    "research_factory.app_server_holdout_stratifier._read_json"
                ) as reservoir_reader,
                patch(
                    "research_factory.app_server_holdout_stratifier._verified_segment_text"
                ) as text_reader,
            ):
                with self.assertRaisesRegex(ValueError, "does not authorize protected access"):
                    _load_reservoirs(self.conn, covenant_path=unauthorized)
            verifier.assert_not_called()
            reservoir_reader.assert_not_called()
            text_reader.assert_not_called()
            self.assertEqual(statements, [])
        finally:
            self.conn.set_trace_callback(None)

    def test_batch_plans_bound_extraction_and_support_calls(self):
        rows = [
            {
                "segment_id": f"segment_{index}",
                "episode_id": f"episode_{index // 2}",
                "text_sha256": "a" * 64,
                "segment_text": "x" * 100,
                "events": [{} for _ in range(16 if index % 2 else 8)],
            }
            for index in range(17)
        ]
        extraction = _plan_extraction_batches(rows, batch_size=8)
        self.assertEqual([len(batch) for batch in extraction], [8, 8, 1])
        self.assertEqual(
            [row["segment_id"] for batch in extraction for row in batch],
            [row["segment_id"] for row in rows],
        )
        support = _plan_support_packets(
            rows,
            packet_size=8,
            max_witnesses=48,
            max_packet_bytes=10000,
        )
        self.assertTrue(all(len(batch) <= 8 for batch in support))
        self.assertTrue(all(sum(len(row["events"]) for row in batch) <= 48 for batch in support))
        self.assertEqual(
            [row["segment_id"] for batch in support for row in batch],
            [row["segment_id"] for row in rows],
        )

    def test_builds_llm_only_ab_ba_stratified_selection_with_one_client(self):
        execution_dir = self.root / "execution"
        output_dir = self.root / "stratifier"
        factory = lambda: FakeReferenceClient(self.sources, self.event_counts)
        before_changes = self.conn.total_changes
        report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=execution_dir,
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=factory,
            )
        )
        self.assertEqual(report["schema_version"], STRATIFIER_REPORT_VERSION)
        self.assertTrue(report["ok"])
        self.assertTrue(report["accounting_complete"])
        self.assertEqual(report["usage"]["total_tokens"], 3 * 13)
        self.assertEqual(self.conn.total_changes, before_changes)
        self.assertEqual(len(FakeReferenceClient.instances), 1)
        client = FakeReferenceClient.instances[0]
        self.assertEqual(client.entered, 1)
        self.assertEqual(client.exited, 1)
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(report["requested_extraction_calls"], 1)
        self.assertEqual(report["extraction_batch_sizes"], [5])
        support_plan = json.loads(
            (output_dir / "support-packet-plan.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(support_plan["packets"]), 1)
        self.assertLessEqual(
            support_plan["packets"][0]["witness_count"],
            support_plan["witness_ceiling"],
        )
        self.assertLessEqual(
            support_plan["packets"][0]["estimated_packet_bytes"],
            support_plan["packet_bytes_ceiling"],
        )
        frozen_plan = json.loads((output_dir / "plan.json").read_text())
        self.assertEqual(frozen_plan["label_pack_contract"]["label_pack"], "ai_discourse_v3_1")
        self.assertEqual(
            set(frozen_plan["label_pack_contract"]["artifacts"]),
            {"prompt.md", "schema.json", "codebook.md"},
        )
        self.assertEqual(client.calls[0]["model"], "gpt-5.5")
        self.assertTrue(all(call["model"] == "gpt-5.6-sol" for call in client.calls[1:]))
        self.assertTrue(all(call["effort"] == "high" for call in client.calls))

        selection = json.loads(Path(report["selection_path"]).read_text(encoding="utf-8"))
        self.assertEqual(selection["schema_version"], HOLDOUT_STRATIFIED_SELECTION_VERSION)
        self.assertFalse(selection["candidate_outputs_observed"])
        self.assertFalse(selection["baseline_outputs_observed"])
        self.assertFalse(selection["adaptive_top_up"])
        self.assertEqual(
            selection["stratum_counts"],
            {
                "clean_no_signal_power:no_signal": 1,
                "paired_quality:dense": 1,
                "paired_quality:low": 1,
                "paired_quality:medium": 1,
                "paired_quality:no_signal": 1,
            },
        )
        self.assertEqual(len(selection["segments"]), 5)
        by_id = {row["segment_id"]: row for row in selection["segments"]}
        self.assertEqual(by_id["paired_low"]["reference_event_count"], 4)
        self.assertEqual(by_id["paired_low"]["stratum"], "low")
        self.assertEqual(by_id["paired_medium"]["reference_event_count"], 5)
        self.assertEqual(by_id["paired_medium"]["stratum"], "medium")
        self.assertTrue(by_id["terminal_zero"]["reference_clean_no_signal"])

        reservoir = [
            {**self.frozen[name], "reservoir": "paired_quality_reservoir"}
            for name in self.segment_ids[:4]
        ] + [
            {
                **self.frozen["terminal_zero"],
                "reservoir": "terminal_position_no_signal_candidates",
            }
        ]
        verified = verify_stratified_selection(
            report["selection_path"],
            covenant_sha256=sha(self.covenant_path.read_bytes()),
            reservoir_segments=reservoir,
            require_acceptance_shape=False,
        )
        self.assertEqual(len(verified["segments"]), 5)

        support_prompts = sorted(output_dir.glob("support/packet-*/**/prompt.private.md"))
        self.assertEqual(len(support_prompts), 2)
        packets = [
            json.loads(path.read_text().split("# Blinded reference packet\n", 1)[1])
            for path in support_prompts
        ]
        self.assertEqual(
            [case["case_id"] for case in packets[0]["cases"]],
            [case["case_id"] for case in packets[1]["cases"]],
        )
        dense_cases = [
            [case for case in packet["cases"] if case["source_text"] == self.sources["paired_dense"]][0]
            for packet in packets
        ]
        self.assertEqual(
            dense_cases[0]["event_set_left"], dense_cases[1]["event_set_right"]
        )
        self.assertEqual(
            dense_cases[0]["event_set_right"], dense_cases[1]["event_set_left"]
        )

        no_client = lambda: self.fail("terminal stratifier report tried to rerun model calls")
        replay = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=execution_dir,
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=no_client,
            )
        )
        self.assertEqual(report, replay)

    def test_default_stratifier_factory_is_resolved_before_all_three_calls(self):
        output_dir = self.root / "default-stratifier"
        fake_factory = lambda: FakeReferenceClient(self.sources, self.event_counts)

        def substitute_default(client_factory, instruction_contract):
            self.assertIs(client_factory, CapacityGatedCodexAppServerClient)
            self.assertEqual(instruction_contract["project_doc_max_bytes"], 0)
            return fake_factory

        with patch(
            "research_factory.app_server_holdout_stratifier.resolve_holdout_client_factory",
            side_effect=substitute_default,
        ) as resolver:
            report = self.run_async(
                run_holdout_reference_stratifier(
                    self.conn,
                    covenant_path=self.covenant_path,
                    output_dir=output_dir,
                    execution_dir=self.root / "default-stratifier-execution",
                    packet_size=10,
                    paired_per_stratum=1,
                    clean_no_signal_count=1,
                )
            )

        resolver.assert_called_once()
        self.assertTrue(report["ok"])
        self.assertEqual(len(FakeReferenceClient.instances), 1)
        self.assertEqual(len(FakeReferenceClient.instances[0].calls), 3)
        self.assertEqual(
            json.loads((output_dir / "plan.json").read_text(encoding="utf-8"))[
                "execution_lineage"
            ],
            report["execution_lineage"],
        )

    def test_terminal_stratifier_rejects_missing_or_tampered_lineage_without_client(self):
        output_dir = self.root / "stratifier-lineage-tamper"
        report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "stratifier-lineage-tamper-execution",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: FakeReferenceClient(
                    self.sources, self.event_counts
                ),
            )
        )
        self.assertTrue(report["ok"])
        clients_after_run = len(FakeReferenceClient.instances)
        sidecar_path = next((output_dir / "extractions").glob("*/sidecar.json"))
        original = json.loads(sidecar_path.read_text(encoding="utf-8"))

        tampered_sidecars = []
        missing = json.loads(json.dumps(original))
        missing.pop(HOLDOUT_SIDECAR_LINEAGE_FIELD)
        tampered_sidecars.append(missing)
        instruction_sources = json.loads(json.dumps(original))
        instruction_sources["instruction_sources_sha256"] = "0" * 64
        tampered_sidecars.append(instruction_sources)
        overlay = json.loads(json.dumps(original))
        overlay[HOLDOUT_SIDECAR_LINEAGE_FIELD]["strict_config_overlay"][
            "project_doc_max_bytes"
        ] = 1
        tampered_sidecars.append(overlay)
        runtime = json.loads(json.dumps(original))
        runtime[HOLDOUT_SIDECAR_LINEAGE_FIELD]["runtime"][
            "pinned_codex_cli_version"
        ] = "tampered"
        tampered_sidecars.append(runtime)
        non_pro = json.loads(json.dumps(original))
        non_pro["plan_type"] = "plus"
        tampered_sidecars.append(non_pro)

        for payload in tampered_sidecars:
            sidecar_path.write_text(
                json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                self.run_async(
                    run_holdout_reference_stratifier(
                        self.conn,
                        covenant_path=self.covenant_path,
                        output_dir=output_dir,
                        execution_dir=self.root / "stratifier-lineage-tamper-execution",
                        packet_size=10,
                        paired_per_stratum=1,
                        clean_no_signal_count=1,
                        client_factory=lambda: self.fail(
                            "tampered terminal stratifier constructed a client"
                        ),
                    )
                )
            self.assertEqual(len(FakeReferenceClient.instances), clients_after_run)

        sidecar_path.write_text(
            json.dumps(original, sort_keys=True) + "\n", encoding="utf-8"
        )

        support_sidecars = sorted((output_dir / "support").glob("*/**/sidecar.json"))
        self.assertEqual(len(support_sidecars), 2)
        first_bytes = support_sidecars[0].read_bytes()
        second_bytes = support_sidecars[1].read_bytes()
        support_sidecars[0].write_bytes(second_bytes)
        support_sidecars[1].write_bytes(first_bytes)
        with self.assertRaises(ValueError):
            self.run_async(
                run_holdout_reference_stratifier(
                    self.conn,
                    covenant_path=self.covenant_path,
                    output_dir=output_dir,
                    execution_dir=self.root / "stratifier-lineage-tamper-execution",
                    packet_size=10,
                    paired_per_stratum=1,
                    clean_no_signal_count=1,
                    client_factory=lambda: self.fail(
                        "cross-swapped terminal stratifier constructed a client"
                    ),
                )
            )
        self.assertEqual(len(FakeReferenceClient.instances), clients_after_run)

    def test_terminal_stratifier_rejects_missing_or_foreign_leaf_bindings(self):
        output_dir = self.root / "stratifier-binding-root"
        report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "stratifier-binding-execution",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: FakeReferenceClient(
                    self.sources, self.event_counts
                ),
            )
        )
        plan = json.loads((output_dir / "plan.json").read_text(encoding="utf-8"))

        missing = copy.deepcopy(report)
        missing["ok"] = False
        missing["leaf_bindings"].pop()
        with self.assertRaisesRegex(ValueError, "binding evidence is incomplete"):
            _validate_terminal_stratifier_lineage(
                output_dir,
                report=missing,
                frozen_plan=plan,
                instruction_contract=report["instruction_contract"],
                execution_lineage=report["execution_lineage"],
            )

        foreign_dir = self.root / "stratifier-binding-foreign"
        foreign_report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=foreign_dir,
                execution_dir=self.root / "stratifier-binding-foreign-execution",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: FakeReferenceClient(
                    self.sources, self.event_counts
                ),
            )
        )
        transplanted = copy.deepcopy(report)
        transplanted["leaf_bindings"][0] = copy.deepcopy(
            foreign_report["leaf_bindings"][0]
        )
        with self.assertRaisesRegex(ValueError, "outside current root"):
            _validate_terminal_stratifier_lineage(
                output_dir,
                report=transplanted,
                frozen_plan=plan,
                instruction_contract=report["instruction_contract"],
                execution_lineage=report["execution_lineage"],
            )

    def test_underpowered_stratum_fails_closed_without_adaptive_top_up(self):
        event_counts = {**self.event_counts, "paired_dense": 15}
        output_dir = self.root / "underpowered"
        report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "execution-underpowered",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: FakeReferenceClient(self.sources, event_counts),
            )
        )
        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "underpowered_reference_strata")
        self.assertEqual(report["underpowered"]["paired_quality:dense"]["available"], 0)
        self.assertFalse((output_dir / "stratified-selection.json").exists())
        plan = json.loads((output_dir / "plan.json").read_text(encoding="utf-8"))
        self.assertFalse(plan["adaptive_top_up"])

    def test_failed_extraction_is_terminal_no_retry_and_existing_outputs_block_start(self):
        blocked_execution = self.root / "blocked-execution"
        (blocked_execution / "phase-b-candidate").mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, "outputs already exist"):
            self.run_async(
                run_holdout_reference_stratifier(
                    self.conn,
                    covenant_path=self.covenant_path,
                    output_dir=self.root / "must-not-start",
                    execution_dir=blocked_execution,
                    packet_size=10,
                    paired_per_stratum=1,
                    clean_no_signal_count=1,
                    client_factory=lambda: self.fail("client should not start"),
                )
            )

        output_dir = self.root / "failed"
        factory = lambda: FakeReferenceClient(
            self.sources, self.event_counts, fail_segment="paired_medium"
        )
        first = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "execution-failed",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=factory,
            )
        )
        self.assertFalse(first["ok"])
        self.assertEqual(first["validated_extractions"], 0)
        self.assertEqual(first["requested_extraction_calls"], 1)
        self.assertEqual(first["requested_support_calls"], 0)
        calls = len(FakeReferenceClient.instances[-1].calls)
        replay = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "execution-failed",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: self.fail("failed extraction was retried"),
            )
        )
        self.assertEqual(first, replay)
        self.assertEqual(len(FakeReferenceClient.instances[-1].calls), calls)

    def test_provably_pre_turn_attempt_resumes_same_ordinal(self):
        output_dir = self.root / "pre-turn-resume"
        with self.assertRaises(asyncio.CancelledError):
            self.run_async(
                run_holdout_reference_stratifier(
                    self.conn,
                    covenant_path=self.covenant_path,
                    output_dir=output_dir,
                    execution_dir=self.root / "execution-pre-turn-resume",
                    packet_size=10,
                    paired_per_stratum=1,
                    clean_no_signal_count=1,
                    client_factory=lambda: CancelBeforeTurnReferenceClient(
                        self.sources, self.event_counts
                    ),
                )
            )
        attempt = output_dir / "extractions" / "batch-0000" / "attempt.json"
        self.assertTrue(attempt.is_file())
        self.assertFalse((attempt.parent / "sidecar.json").exists())
        self.assertFalse((attempt.parent / "raw-output.private.json").exists())

        report = self.run_async(
            run_holdout_reference_stratifier(
                self.conn,
                covenant_path=self.covenant_path,
                output_dir=output_dir,
                execution_dir=self.root / "execution-pre-turn-resume",
                packet_size=10,
                paired_per_stratum=1,
                clean_no_signal_count=1,
                client_factory=lambda: FakeReferenceClient(self.sources, self.event_counts),
            )
        )
        self.assertTrue(report["ok"])
        self.assertEqual(json.loads(attempt.read_text())["retry_ordinal"], 0)


if __name__ == "__main__":
    unittest.main()
