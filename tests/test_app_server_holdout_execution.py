from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from research_factory.app_server_checkpoint import (
    build_holdout_leaf_binding,
    verify_instruction_contract,
)
from research_factory import app_server_expanded_cap_episode_batch as expanded_cap
from research_factory import codex_app_server as codex_transport
from research_factory.app_server_evaluation import APP_SERVER_DEVELOPMENT_MANIFEST_V2
from research_factory.app_server_holdout import (
    FROZEN_WINNER_VERSION,
    HOLDOUT_COVENANT_VERSION,
)
from research_factory.app_server_holdout_execution import (
    HOLDOUT_BASELINE_PHASE_VERSION,
    HOLDOUT_CANDIDATE_PHASE_VERSION,
    HOLDOUT_CONTEXT_PHASE_VERSION,
    HOLDOUT_STRATIFIED_SELECTION_VERSION,
    _load_execution_inputs,
    _resolve_holdout_client_factory,
    _validate_terminal_phase_lineage,
    run_holdout_baseline_phase,
    run_holdout_candidate_phase,
    run_holdout_context_phase,
    run_frozen_holdout_execution,
    verify_stratified_selection,
)
from research_factory.app_server_holdout_client import (
    HOLDOUT_SIDECAR_LINEAGE_FIELD,
    verified_holdout_execution_lineage,
)
from research_factory.app_server_capacity import CapacityGatedCodexAppServerClient
from research_factory.codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
    TURN_SIDECAR_SCHEMA_VERSION,
)
from research_factory.util import sha256_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def json_bytes(value) -> bytes:
    return (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


class FakeAppServerClient:
    instances = []

    def __init__(
        self,
        *,
        invalid_context: bool = False,
        fail_segment: str | None = None,
        cancel_before_turn: bool = False,
    ):
        self.invalid_context = invalid_context
        self.fail_segment = fail_segment
        self.cancel_before_turn = cancel_before_turn
        self.calls = []
        self.entered = 0
        self.exited = 0
        self.__class__.instances.append(self)

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited += 1

    async def run_ephemeral_structured_turn(self, **kwargs):
        self.calls.append(kwargs)
        if self.cancel_before_turn:
            raise asyncio.CancelledError()
        schema = kwargs["output_schema"]
        schema_const = schema["properties"]["schema_version"].get("const")
        if schema_const == "ai_discourse_v3_1_episode_context":
            episode_id = schema["properties"]["episode_id"]["const"]
            output = {
                "schema_version": "ai_discourse_v3_1_episode_context",
                "episode_id": episode_id,
                "context_summary": "A compact fixture episode context.",
                "speaker_map": [],
                "section_map": [],
                "entity_seed": {},
                "concept_seed": [],
                "extraction_guidance": "Use the complete episode context to resolve speakers carefully.",
                "episode_context": {
                    "context_summary": "A compact fixture episode context.",
                    "speaker_map": [],
                    "section_map": [],
                    "entity_seed": {},
                    "concept_seed": [],
                },
                "excluded_source_context": ["show setup"],
                "quality_flags": [],
                "overall_confidence": 0.9,
                "needs_review": False,
                "review_reason": None,
            }
            if self.invalid_context:
                output.pop("extraction_guidance")
        else:
            segment_id = schema["properties"]["segment_id"]["const"]
            episode_id = schema["properties"]["episode_id"]["const"]
            output = {
                "schema_version": "ai_discourse_v3_1",
                "segment_id": segment_id,
                "episode_id": episode_id,
                "extraction_status": "no_signal",
                "segment_quality": {
                    "artifact_type": "dialogue_transcript",
                    "boilerplate_risk": "low",
                    "substantive_word_count": 4,
                    "transcript_preparation_id": "prep_ep_1",
                },
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "Fixture has no coded discourse event.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "No grounded event is present in this fixture segment.",
                "overall_confidence": 0.9,
                "needs_review": False,
                "review_reason": None,
            }
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, sort_keys=True) + "\n", encoding="utf-8")
        sidecar_path = Path(kwargs["sidecar_path"])
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        segment_id = schema.get("properties", {}).get("segment_id", {}).get("const")
        failed = self.fail_segment is not None and segment_id == self.fail_segment
        contract = verify_instruction_contract()
        execution_lineage = verified_holdout_execution_lineage(contract)
        output_text = output_path.read_text(encoding="utf-8")
        sidecar_path.write_text(
            json.dumps(
                {
                    "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
                    "state": "failed" if failed else "completed",
                    "status": "client_error" if failed else "completed",
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
                    "output_path": str(output_path.resolve()),
                    "output_sha256": sha256_text(output_text),
                    "usage_complete": True,
                    "usage": usage,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        if failed:
            raise RuntimeError("synthetic baseline failure")
        return SimpleNamespace(
            status_ok=True,
            output=output,
            error_class=None,
            status="completed",
        )


class FakeCoreRunner:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    async def __call__(self, episodes, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("synthetic candidate failure")
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        configuration = kwargs["frozen_configuration"]
        capacity = kwargs["capacity_admission"]

        def write(name, value):
            path = output_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(json_bytes(value))
            return {
                "path": str(path.resolve()),
                "sha256": digest(path.read_bytes()),
                "size_bytes": path.stat().st_size,
            }

        config_record = write("frozen-configuration.json", configuration)
        capacity_record = write("capacity-admission.json", capacity)
        binding = {
            "schema_version": expanded_cap.CAPACITY_BINDING_VERSION,
            "state": "verified_before_app_server_start",
            "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
            "expected_canonical_sha256": kwargs["capacity_admission_sha256"],
            "observed_canonical_sha256": kwargs["capacity_admission_sha256"],
            "capacity_admission": capacity_record,
            "fixture": kwargs["fixture_mode"],
        }
        binding_record = write("capacity-binding.json", binding)
        run_configuration = {
            "schema_version": expanded_cap.RUN_CONFIGURATION_VERSION,
            "state": "capacity_bound_configuration",
            "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
            "batch_size": kwargs["batch_size"],
            "thread_mode": kwargs["thread_mode"],
            "model": expanded_cap.MODEL,
            "effort": expanded_cap.EFFORT,
            "concurrency": kwargs["concurrency"],
            "frozen_configuration": config_record,
            "capacity_binding": binding_record,
            "capacity_admission_canonical_sha256": kwargs[
                "capacity_admission_sha256"
            ],
            "fixture_mode": kwargs["fixture_mode"],
        }
        run_record = write("run-configuration.json", run_configuration)
        requests = [
            request
            for episode in episodes
            for request in expanded_cap.prepare_episode_batches(
                episode,
                batch_size=kwargs["batch_size"],
                thread_mode=kwargs["thread_mode"],
            )
        ]
        contract = verify_instruction_contract()
        execution_lineage = verified_holdout_execution_lineage(contract)
        per_turn_usage = {
            "input_tokens": 200,
            "cached_input_tokens": 20,
            "output_tokens": 40,
            "reasoning_output_tokens": 10,
            "total_tokens": 240,
        }
        batch_records = []
        for index, request in enumerate(requests):
            paths = expanded_cap._batch_paths(output_dir, request["batch_id"])
            paths["root"].mkdir(parents=True, exist_ok=True)
            paths["input"].write_bytes(json_bytes(request["private_input"]))
            paths["prompt"].write_text(request["prompt"], encoding="utf-8")
            paths["base"].write_text(request["base_instructions"], encoding="utf-8")
            paths["schema"].write_bytes(json_bytes(request["schema"]))
            paths["attempt"].write_bytes(json_bytes({"fixture": True}))
            output_text = json.dumps(
                {"fixture_batch_id": request["batch_id"]},
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            paths["output"].write_text(output_text + "\n", encoding="utf-8")
            paths["normalized"].write_bytes(json_bytes({"fixture": True}))
            paths["sidecar"].write_bytes(
                json_bytes(
                    {
                        "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
                        "state": "completed",
                        "status": "completed",
                        "client_version": APP_SERVER_CLIENT_VERSION,
                        "cli_version": PINNED_CODEX_CLI_VERSION,
                        "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
                        "app_server_user_agent": "fixture-codex-app-server",
                        "transport": "stdio",
                        "auth_type": "chatgpt",
                        "plan_type": "pro",
                        "thread_id": "candidate-thread-%04d" % index,
                        "turn_id": "candidate-turn-%04d" % index,
                        "model": expanded_cap.MODEL,
                        "effort": expanded_cap.EFFORT,
                        "thread_mode": request["thread_mode"],
                        "batch_size": request["effective_batch_size"],
                        "prompt_sha256": sha256_text(request["prompt"]),
                        "prompt_bytes": len(request["prompt"].encode("utf-8")),
                        "base_instructions_sha256": sha256_text(
                            request["base_instructions"]
                        ),
                        "base_instructions_bytes": len(
                            request["base_instructions"].encode("utf-8")
                        ),
                        "output_schema_sha256": sha256_text(
                            json.dumps(
                                request["schema"],
                                ensure_ascii=True,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        ),
                        "output_schema_bytes": len(
                            json.dumps(
                                request["schema"],
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
                        "output_path": str(paths["output"].resolve()),
                        "output_sha256": sha256_text(output_text),
                        "usage_complete": True,
                        "usage": per_turn_usage,
                    }
                )
            )
            batch_records.append(
                {
                    "batch_id": request["batch_id"],
                    "episode_id": request["episode_id"],
                    "segment_ids": request["segment_ids"],
                    "input_path": str(paths["input"]),
                    "attempt_receipt_path": str(paths["attempt"]),
                    "sidecar_path": str(paths["sidecar"]),
                    "raw_output_path": str(paths["output"]),
                    "normalized_output_path": str(paths["normalized"]),
                }
            )
        mapping = {
            "schema_version": expanded_cap.PRIVATE_MAPPING_VERSION,
            "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
            "batches": batch_records,
            "privacy": "private",
        }
        write("private-mapping.json", mapping)
        usage = {
            key: value * len(requests) for key, value in per_turn_usage.items()
        }
        report = {
            "schema_version": expanded_cap.RUN_REPORT_VERSION,
            "state": "passed",
            "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
            "frozen_configuration": config_record,
            "run_configuration": run_record,
            "capacity_binding": binding_record,
            "capacity_binding_valid": True,
            "fixture_mode": kwargs["fixture_mode"],
            "batch_size": kwargs["batch_size"],
            "thread_mode": kwargs["thread_mode"],
            "model": expanded_cap.MODEL,
            "effort": expanded_cap.EFFORT,
            "max_events_per_segment": expanded_cap.MAX_EVENTS_PER_SEGMENT,
            "semantic_postprocessing": False,
            "requested_calls": len(requests),
            "attempted_calls": len(requests),
            "validated_calls": len(requests),
            "attempt_contract_failures": 0,
            "terminal_sidecars": len(requests),
            "sidecar_contract_failures": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "usage_measured_attempts": len(requests),
            "usage_unknown_attempts": 0,
            "ambiguous_outcome_attempts": 0,
            "retry_count": 0,
            "ambiguous_retry_count": 0,
            "all_emitted_events_preserved": True,
            "production_mutated": False,
        }
        (output_dir / "report.json").write_bytes(json_bytes(report))
        return report


class HoldoutExecutionTest(unittest.TestCase):
    def setUp(self) -> None:
        FakeAppServerClient.instances = []
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "label_packs").symlink_to(PROJECT_ROOT / "label_packs", target_is_directory=True)
        self.env = patch.dict(os.environ, {"RESEARCH_FACTORY_ROOT": str(self.root)})
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE sources (id TEXT PRIMARY KEY, name TEXT);
            CREATE TABLE episodes (
              id TEXT PRIMARY KEY, source_id TEXT, title TEXT, published_at TEXT
            );
            CREATE TABLE transcripts (
              id TEXT PRIMARY KEY, episode_id TEXT, raw_text_path TEXT,
              raw_text_sha256 TEXT, status TEXT
            );
            CREATE TABLE transcript_preparations (
              id TEXT PRIMARY KEY, transcript_id TEXT, status TEXT,
              cleaned_text_path TEXT, cleaned_text_sha256 TEXT,
              artifact_type TEXT, substantive_word_count INTEGER,
              boilerplate_ratio REAL, speaker_turn_count INTEGER, quality_score REAL
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, transcript_id TEXT, episode_id TEXT, source_id TEXT,
              segment_index INTEGER, start_char INTEGER, end_char INTEGER,
              text_path TEXT, text_sha256 TEXT, word_count INTEGER
            );
            """
        )
        self.segment_texts = [
            "The host introduces the episode.",
            "An unselected middle segment stays outside execution.",
            "The guest closes with a brief thanks.",
        ]
        prepared_text = "".join(self.segment_texts)
        self._write("corpus/raw.txt", prepared_text)
        self._write("corpus/prepared.txt", prepared_text)
        self.conn.execute("INSERT INTO sources VALUES ('src_1', 'Fixture Show')")
        self.conn.execute(
            "INSERT INTO episodes VALUES ('ep_1', 'src_1', 'Fixture Episode', '2026-07-10T00:00:00+00:00')"
        )
        self.conn.execute(
            "INSERT INTO transcripts VALUES ('tr_ep_1', 'ep_1', 'corpus/raw.txt', ?, 'ready')",
            (digest(prepared_text.encode()),),
        )
        self.conn.execute(
            """
            INSERT INTO transcript_preparations VALUES
              ('prep_ep_1', 'tr_ep_1', 'prepared', 'corpus/prepared.txt', ?,
               'dialogue_transcript', 12, 0.0, 2, 1.0)
            """,
            (digest(prepared_text.encode()),),
        )
        offset = 0
        for index, text in enumerate(self.segment_texts):
            segment_id = f"seg_{index}"
            path = f"corpus/{segment_id}.txt"
            self._write(path, text)
            self.conn.execute(
                "INSERT INTO segments VALUES (?, 'tr_ep_1', 'ep_1', 'src_1', ?, ?, ?, ?, ?, ?)",
                (
                    segment_id,
                    index,
                    offset,
                    offset + len(text),
                    path,
                    digest(text.encode()),
                    len(text.split()),
                ),
            )
            offset += len(text)
        self.conn.commit()
        self.winner_path = self.root / "winner.json"
        winner_config = expanded_cap.build_frozen_configuration(
            batch_size=5, thread_mode="same_thread"
        )
        winner_config_sha = digest(
            json.dumps(
                winner_config,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
        self.winner = {
            "schema_version": FROZEN_WINNER_VERSION,
            "selection_status": "frozen_winner",
            "winner_frozen": True,
            "winner": {
                "variant_id": "batch_5_same_thread",
                "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
                "batch_size": 5,
                "thread_mode": "same_thread",
                "model": expanded_cap.MODEL,
                "reasoning_effort": expanded_cap.EFFORT,
                "concurrency": 1,
                "retry_count": 0,
                "window_count": 4,
                "context_chars": 900,
                "max_events_per_segment": expanded_cap.MAX_EVENTS_PER_SEGMENT,
                "frozen_configuration": winner_config,
                "frozen_configuration_sha256": winner_config_sha,
                "report_sha256": "2" * 64,
            },
            "gates": {
                "quality_noninferior": True,
                "production_amortized_total_token_ratio_lte_0_28": True,
            },
            "frozen_artifact_hashes": {
                "matrix": "3" * 64,
                "arm_configuration_batch_5_same_thread": winner_config_sha,
                "arm_report_batch_5_same_thread": "2" * 64,
            },
            "holdout_preparation_authorized": True,
            "holdout_model_calls_authorized": False,
            "production_changed": False,
            "production_mutated": False,
        }
        self.winner_path.write_bytes(json_bytes(self.winner))
        self.covenant_path = self._write_covenant()
        self.selection_path = self._write_selection()

    def tearDown(self) -> None:
        self.conn.close()
        self.env.stop()
        self.tempdir.cleanup()

    def test_default_client_factory_injects_verified_zero_byte_overlay(self) -> None:
        contract = verify_instruction_contract()
        resolved = _resolve_holdout_client_factory(
            CapacityGatedCodexAppServerClient, contract
        )
        async def construct_clients():
            outer = resolved()
            return outer, outer.inner_factory()

        outer, inner = self.run_async(construct_clients())
        self.assertIsInstance(outer, CapacityGatedCodexAppServerClient)
        self.assertIsInstance(
            inner, expanded_cap.ExpandedCapBatchCodexAppServerClient
        )
        self.assertEqual(
            inner._epoch4_config_overlay, contract["strict_config_overlay"]
        )
        self.assertEqual(inner._epoch4_config_overlay["project_doc_max_bytes"], 0)
        self.assertEqual(inner.command[-3:], ["app-server", "--stdio", "--strict-config"])
        self.assertEqual(
            inner._holdout_execution_lineage["instruction_contract"][
                "instruction_sources_sha256"
            ],
            contract["expected_path_set_sha256"],
        )
        self.assertEqual(
            inner._holdout_execution_lineage["instruction_contract"][
                "instruction_sources_count"
            ],
            contract["instruction_sources_count"],
        )
        self.assertEqual(
            inner._holdout_execution_lineage["managed_chatgpt_auth"],
            {
                "account_type": "chatgpt",
                "plan_type": "pro",
                "required_before_thread_or_turn": True,
            },
        )

        captured = {}

        async def capture_request(_client, method, params):
            captured["method"] = method
            captured["params"] = params
            return {
                "thread": {"id": "thread-overlay-fixture"},
                "instructionSources": contract["instruction_source_paths"],
            }

        async def intercept_thread_start():
            inner.account_summary = {"type": "chatgpt", "plan_type": "pro"}
            with patch.object(
                codex_transport.CodexAppServerClient,
                "_request",
                capture_request,
            ):
                await inner._request(
                    "thread/start",
                    {"model": "gpt-5.5", "cwd": str(PROJECT_ROOT)},
                )

        self.run_async(intercept_thread_start())
        self.assertEqual(captured["method"], "thread/start")
        self.assertEqual(captured["params"]["config"], contract["strict_config_overlay"])
        self.assertEqual(captured["params"]["config"]["project_doc_max_bytes"], 0)
        self.assertEqual(captured["params"]["personality"], "none")
        self.assertEqual(captured["params"]["environments"], [])
        self.assertEqual(captured["params"]["dynamicTools"], [])

        fixture = lambda: FakeAppServerClient()
        self.assertIs(_resolve_holdout_client_factory(fixture, contract), fixture)

    def test_default_client_rejects_non_pro_before_thread_or_turn_request(self) -> None:
        contract = verify_instruction_contract()
        resolved = _resolve_holdout_client_factory(
            CapacityGatedCodexAppServerClient, contract
        )

        async def exercise() -> None:
            outer = resolved()
            inner = outer.inner_factory()
            for method in ("thread/start", "turn/start"):
                inner.account_summary = {"type": "chatgpt", "plan_type": "plus"}
                downstream = AsyncMock()
                with patch.object(
                    codex_transport.CodexAppServerClient,
                    "_request",
                    downstream,
                ):
                    with self.assertRaises(codex_transport.AppServerAuthError):
                        await inner._request(method, {})
                downstream.assert_not_awaited()

        self.run_async(exercise())

    def test_unauthorized_execution_stops_before_verifier_reservoir_or_sql_read(self) -> None:
        unauthorized = self.root / "unauthorized-execution-covenant.json"
        unauthorized.write_bytes(
            json_bytes(
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
                    "research_factory.app_server_holdout_execution.verify_frozen_holdout"
                ) as verifier,
                patch(
                    "research_factory.app_server_holdout_execution._read_json"
                ) as reservoir_reader,
                patch(
                    "research_factory.app_server_holdout_execution.load_frozen_winner"
                ) as winner_reader,
            ):
                with self.assertRaisesRegex(ValueError, "does not authorize protected access"):
                    _load_execution_inputs(
                        self.conn,
                        covenant_path=unauthorized,
                        frozen_winner_path=self.root / "must-not-open-winner.json",
                        stratified_selection_path=self.root / "must-not-open-selection.json",
                        acceptance_mode=True,
                    )
            verifier.assert_not_called()
            reservoir_reader.assert_not_called()
            winner_reader.assert_not_called()
            self.assertEqual(statements, [])
        finally:
            self.conn.set_trace_callback(None)

    def _write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def _frozen_segment(self, index: int) -> dict:
        return {
            "episode_id": "ep_1",
            "source_id": "src_1",
            "source_name": "Fixture Show",
            "published_at": "2026-07-10T00:00:00+00:00",
            "transcript_id": "tr_ep_1",
            "transcript_source_kind": "creator",
            "transcript_acquired_at": "2026-07-11T00:00:00+00:00",
            "raw_text_sha256": digest("".join(self.segment_texts).encode()),
            "preparation_id": "prep_ep_1",
            "prepared_text_sha256": digest("".join(self.segment_texts).encode()),
            "segment_id": f"seg_{index}",
            "segment_index": index,
            "text_sha256": digest(self.segment_texts[index].encode()),
            "selection_hash": str(index) * 64,
            "selection_rank": 0,
        }

    def _write_covenant(self) -> Path:
        holdout = self.root / "holdout"
        holdout.mkdir()
        exclusions = {"schema_version": "fixture_exclusions", "excluded_episode_ids": []}
        paired = {
            "schema_version": "fixture_paired",
            "segments": [self._frozen_segment(0), self._frozen_segment(1)],
        }
        terminal = {"schema_version": "fixture_terminal", "segments": [self._frozen_segment(2)]}
        files = {
            "exclusions.json": exclusions,
            "paired.json": paired,
            "terminal.json": terminal,
        }
        for name, value in files.items():
            (holdout / name).write_bytes(json_bytes(value))
        covenant = {
            "schema_version": HOLDOUT_COVENANT_VERSION,
            "freeze_status": "immutable_prepare_only_ready",
            "holdout_model_calls_authorized": True,
            "seed": "fixture",
            "winner": self.winner["winner"],
            "winner_gates": self.winner["gates"],
            "winner_artifact_sha256": digest(self.winner_path.read_bytes()),
            "artifacts": {
                "exclusions": {
                    "filename": "exclusions.json",
                    "sha256": digest((holdout / "exclusions.json").read_bytes()),
                },
                "paired_quality_reservoir": {
                    "filename": "paired.json",
                    "sha256": digest((holdout / "paired.json").read_bytes()),
                },
                "terminal_position_no_signal_candidates": {
                    "filename": "terminal.json",
                    "sha256": digest((holdout / "terminal.json").read_bytes()),
                },
            },
        }
        path = holdout / "covenant.json"
        path.write_bytes(json_bytes(covenant))
        return path

    def _write_selection(self) -> Path:
        covenant_sha = digest(self.covenant_path.read_bytes())
        rows = []
        for selection_order, (segment_index, segment_id, lane) in enumerate(
            (
                (0, "seg_0", "paired_quality"),
                (2, "seg_2", "clean_no_signal_power"),
            )
        ):
            frozen = self._frozen_segment(segment_index)
            rows.append(
                {
                    "selection_order": selection_order,
                    "segment_id": segment_id,
                    "episode_id": "ep_1",
                    "transcript_id": "tr_ep_1",
                    "text_sha256": frozen["text_sha256"],
                    "evaluation_set": lane,
                    "stratum": "no_signal",
                    "reference_event_count": 0,
                    "reference_case_sha256": str(4 + selection_order) * 64,
                    "reference_clean_no_signal": (
                        True if lane == "clean_no_signal_power" else False
                    ),
                }
            )
        payload = {
            "schema_version": HOLDOUT_STRATIFIED_SELECTION_VERSION,
            "selection_status": "frozen_stratified_selection",
            "selection_frozen": True,
            "covenant_sha256": covenant_sha,
            "selection_basis": "llm_reference_only_pre_candidate_pre_baseline",
            "candidate_outputs_observed": False,
            "baseline_outputs_observed": False,
            "adaptive_top_up": False,
            "reference_artifact_sha256": "6" * 64,
            "stratum_counts": {
                "clean_no_signal_power:no_signal": 1,
                "paired_quality:no_signal": 1,
            },
            "segments": rows,
        }
        path = self.root / "selection.json"
        path.write_bytes(json_bytes(payload))
        return path

    def _capacity_admission(self, **overrides):
        now = datetime.now(timezone.utc)
        episode_ids = ["ep_1"]
        payload = {
            "schema_version": expanded_cap.CAPACITY_ADMISSION_VERSION,
            "admission_id": "fixture-holdout-capacity",
            "issued_at": (now - timedelta(seconds=5)).isoformat(),
            "expires_at": (now + timedelta(minutes=5)).isoformat(),
            "state": "admitted",
            "issued_by": "evaluation_coordinator",
            "verification_mode": "fixture_verified",
            "fixture": True,
            "winner_system_id": expanded_cap.WINNER_SYSTEM_ID,
            "batch_size": 5,
            "thread_mode": "same_thread",
            "model": expanded_cap.MODEL,
            "effort": expanded_cap.EFFORT,
            "concurrency": 1,
            "episode_ids_sha256": sha256_text(
                json.dumps(episode_ids, sort_keys=True, separators=(",", ":"))
            ),
            "episode_count": 1,
            "segment_count": 2,
            "batch_count": 1,
            "live_capacity_available": True,
            "managed_chatgpt_auth_only": True,
            "verification_evidence_sha256": "9" * 64,
        }
        payload.update(overrides)
        return payload, expanded_cap.capacity_admission_sha256(payload)

    def run_async(self, coroutine):
        return asyncio.run(coroutine)

    def _run_successful_candidate(self, execution_dir: Path) -> dict:
        factory = lambda: FakeAppServerClient()
        context = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=factory,
            )
        )
        self.assertTrue(context["ok"])
        capacity, capacity_sha256 = self._capacity_admission()
        candidate = self.run_async(
            run_holdout_candidate_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                capacity_admission=capacity,
                capacity_admission_sha256=capacity_sha256,
                fixture_mode=True,
                core_runner=FakeCoreRunner(),
                client_factory=factory,
            )
        )
        self.assertTrue(candidate["ok"])
        return candidate

    def _candidate_report_with_two_local_leaves(
        self, execution_dir: Path, report: dict
    ) -> dict:
        phase_root = execution_dir / "phase-b-candidate"
        candidate_dir = phase_root / "candidate-arm"
        expanded = copy.deepcopy(report)
        first_outcome = expanded["outcomes"][0]
        first_batch_id = str(first_outcome["batch_id"])
        second_batch_id = first_batch_id + "-clone"
        first_paths = expanded_cap._batch_paths(candidate_dir, first_batch_id)
        second_paths = expanded_cap._batch_paths(candidate_dir, second_batch_id)
        shutil.copytree(first_paths["root"], second_paths["root"])

        sidecar = json.loads(second_paths["sidecar"].read_text(encoding="utf-8"))
        sidecar["thread_id"] = str(sidecar["thread_id"]) + "-clone"
        sidecar["turn_id"] = str(sidecar["turn_id"]) + "-clone"
        sidecar["output_path"] = str(second_paths["output"].resolve())
        second_paths["sidecar"].write_bytes(json_bytes(sidecar))
        first_binding = expanded["leaf_bindings"][0]
        second_binding = build_holdout_leaf_binding(
            sidecar_path=second_paths["sidecar"],
            prompt_path=second_paths["prompt"],
            output_schema_path=second_paths["schema"],
            base_instructions_path=second_paths["base"],
            raw_output_path=second_paths["output"],
            model=first_binding["model"],
            effort=first_binding["effort"],
            thread_mode=first_binding["thread_mode"],
            batch_size=first_binding["batch_size"],
            prompt=second_paths["prompt"].read_text(encoding="utf-8"),
            output_schema=json.loads(second_paths["schema"].read_text(encoding="utf-8")),
            base_instructions=second_paths["base"].read_text(encoding="utf-8"),
            instruction_contract=expanded["instruction_contract"],
            execution_lineage=expanded["execution_lineage"],
        )
        second_outcome = copy.deepcopy(first_outcome)
        second_outcome.update(
            {
                "batch_id": second_batch_id,
                "sidecar_path": str(second_paths["sidecar"].resolve()),
                "leaf_identity_sha256": second_binding["leaf_identity_sha256"],
            }
        )
        expanded["requested_calls"] = 2
        expanded["outcomes"].append(second_outcome)
        expanded["leaf_bindings"].append(second_binding)

        mapping_path = candidate_dir / "private-mapping.json"
        mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
        second_mapping = copy.deepcopy(mapping["batches"][0])
        second_mapping.update(
            {
                "batch_id": second_batch_id,
                "input_path": str(second_paths["input"]),
                "attempt_receipt_path": str(second_paths["attempt"]),
                "sidecar_path": str(second_paths["sidecar"]),
                "raw_output_path": str(second_paths["output"]),
                "normalized_output_path": str(second_paths["normalized"]),
            }
        )
        mapping["batches"].append(second_mapping)
        mapping_path.write_bytes(json_bytes(mapping))
        expanded["candidate_mapping_path"] = str(mapping_path.resolve())
        expanded["candidate_mapping_sha256"] = digest(mapping_path.read_bytes())
        return expanded

    def test_runs_three_frozen_phases_and_checkpointed_reruns_do_not_call_models(self) -> None:
        execution_dir = self.root / "execution"
        initial_database_changes = self.conn.total_changes
        context_factory = lambda: FakeAppServerClient()
        context = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=context_factory,
            )
        )
        self.assertEqual(context["schema_version"], HOLDOUT_CONTEXT_PHASE_VERSION)
        self.assertTrue(context["ok"])
        self.assertEqual(context["requested_episodes"], 1)
        self.assertEqual(context["usage"]["total_tokens"], 120)
        manifest = json.loads(Path(context["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], APP_SERVER_DEVELOPMENT_MANIFEST_V2)
        self.assertEqual(manifest["segment_count"], 2)
        self.assertEqual(
            {row["text_sha256"] for row in manifest["episodes"][0]["segments"]},
            {digest(self.segment_texts[index].encode()) for index in (0, 2)},
        )
        self.assertEqual(len(FakeAppServerClient.instances), 1)
        self.assertEqual(FakeAppServerClient.instances[0].entered, 1)
        self.assertEqual(len(FakeAppServerClient.instances[0].calls), 1)
        self.assertEqual(FakeAppServerClient.instances[0].calls[0]["model"], "gpt-5.5")
        self.assertEqual(FakeAppServerClient.instances[0].calls[0]["effort"], "high")
        execution_plan = json.loads((execution_dir / "execution-plan.json").read_text())
        self.assertEqual(
            execution_plan["winner_config"],
            self.winner["winner"]["frozen_configuration"],
        )
        self.assertEqual(
            set(execution_plan["label_pack_contract"]["artifacts"]),
            {"prompt.md", "schema.json", "codebook.md"},
        )

        runner = FakeCoreRunner()
        capacity, capacity_sha256 = self._capacity_admission()
        candidate = self.run_async(
            run_holdout_candidate_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                capacity_admission=capacity,
                capacity_admission_sha256=capacity_sha256,
                fixture_mode=True,
                core_runner=runner,
                client_factory=context_factory,
            )
        )
        self.assertEqual(candidate["schema_version"], HOLDOUT_CANDIDATE_PHASE_VERSION)
        self.assertTrue(candidate["ok"])
        self.assertEqual(len(candidate["leaf_bindings"]), candidate["requested_calls"])
        self.assertEqual(len(candidate["outcomes"]), candidate["requested_calls"])
        self.assertEqual(
            {row["status"] for row in candidate["outcomes"]}, {"validated"}
        )
        self.assertEqual(len(runner.calls), 1)
        call = runner.calls[0]
        self.assertEqual(call["batch_size"], 5)
        self.assertEqual(call["thread_mode"], "same_thread")
        self.assertEqual(call["concurrency"], 1)
        self.assertTrue(call["fixture_mode"])
        self.assertEqual(
            call["frozen_configuration"],
            self.winner["winner"]["frozen_configuration"],
        )
        self.assertEqual(call["capacity_admission_sha256"], capacity_sha256)

        baseline = self.run_async(
            run_holdout_baseline_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=context_factory,
            )
        )
        self.assertEqual(baseline["schema_version"], HOLDOUT_BASELINE_PHASE_VERSION)
        self.assertTrue(baseline["ok"])
        self.assertEqual(baseline["requested_segments"], 2)
        self.assertEqual(baseline["usage"]["total_tokens"], 240)
        baseline_client = FakeAppServerClient.instances[-1]
        self.assertEqual(len(baseline_client.calls), 2)
        self.assertTrue(all(call["model"] == "gpt-5.5" for call in baseline_client.calls))
        self.assertTrue(all(call["effort"] == "high" for call in baseline_client.calls))
        for outcome in baseline["outcomes"]:
            self.assertNotEqual(outcome["raw_output_path"], outcome["repaired_output_path"])
            self.assertTrue(Path(outcome["raw_output_path"]).is_file())
            self.assertTrue(Path(outcome["repaired_output_path"]).is_file())
        self.assertEqual(self.conn.total_changes, initial_database_changes)

        instance_count = len(FakeAppServerClient.instances)
        no_client = lambda: self.fail("checkpointed phase tried to create another client")
        self.assertTrue(
            self.run_async(
                run_holdout_candidate_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    fixture_mode=True,
                    core_runner=no_client,
                    client_factory=no_client,
                )
            )["ok"]
        )
        self.assertTrue(
            self.run_async(
                run_holdout_context_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    client_factory=no_client,
                )
            )["ok"]
        )
        self.assertTrue(
            self.run_async(
                run_holdout_baseline_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    client_factory=no_client,
                )
            )["ok"]
        )
        self.assertEqual(len(FakeAppServerClient.instances), instance_count)

        first_sidecar = Path(baseline["outcomes"][0]["sidecar_path"])
        second_sidecar = Path(baseline["outcomes"][1]["sidecar_path"])
        first_bytes = first_sidecar.read_bytes()
        second_bytes = second_sidecar.read_bytes()
        first_sidecar.write_bytes(second_bytes)
        second_sidecar.write_bytes(first_bytes)
        with self.assertRaises(ValueError):
            self.run_async(
                run_holdout_baseline_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    client_factory=no_client,
                )
            )
        self.assertEqual(len(FakeAppServerClient.instances), instance_count)

    def test_terminal_candidate_binds_each_batch_to_its_exact_sidecar(self) -> None:
        execution_dir = self.root / "candidate-sidecar-swap"
        candidate = self._run_successful_candidate(execution_dir)
        candidate = self._candidate_report_with_two_local_leaves(
            execution_dir, candidate
        )
        phase_root = execution_dir / "phase-b-candidate"
        _validate_terminal_phase_lineage(
            candidate,
            phase_root=phase_root,
            instruction_contract=candidate["instruction_contract"],
            execution_lineage=candidate["execution_lineage"],
        )

        swapped = copy.deepcopy(candidate)
        first_sidecar = swapped["outcomes"][0]["sidecar_path"]
        swapped["outcomes"][0]["sidecar_path"] = swapped["outcomes"][1][
            "sidecar_path"
        ]
        swapped["outcomes"][1]["sidecar_path"] = first_sidecar
        with self.assertRaisesRegex(ValueError, "request identity"):
            _validate_terminal_phase_lineage(
                swapped,
                phase_root=phase_root,
                instruction_contract=swapped["instruction_contract"],
                execution_lineage=swapped["execution_lineage"],
            )

    def test_terminal_candidate_rejects_foreign_leaf_and_schema_transplants(self) -> None:
        first_dir = self.root / "candidate-transplant-a"
        second_dir = self.root / "candidate-transplant-b"
        first = self._run_successful_candidate(first_dir)
        second = self._run_successful_candidate(second_dir)

        transplanted = copy.deepcopy(first)
        transplanted["leaf_bindings"][0] = copy.deepcopy(second["leaf_bindings"][0])
        transplanted["outcomes"][0]["sidecar_path"] = second["outcomes"][0][
            "sidecar_path"
        ]
        transplanted["outcomes"][0]["leaf_identity_sha256"] = second["outcomes"][
            0
        ]["leaf_identity_sha256"]
        with self.assertRaisesRegex(ValueError, "outside current root"):
            _validate_terminal_phase_lineage(
                transplanted,
                phase_root=first_dir / "phase-b-candidate",
                instruction_contract=first["instruction_contract"],
                execution_lineage=first["execution_lineage"],
            )

        wrong_schema = copy.deepcopy(first)
        wrong_schema["schema_version"] = HOLDOUT_CONTEXT_PHASE_VERSION
        with self.assertRaisesRegex(ValueError, "schema or root identity"):
            _validate_terminal_phase_lineage(
                wrong_schema,
                phase_root=first_dir / "phase-b-candidate",
                instruction_contract=first["instruction_contract"],
                execution_lineage=first["execution_lineage"],
            )

    def test_invalid_context_is_terminal_and_never_retried(self) -> None:
        execution_dir = self.root / "invalid-context"
        factory = lambda: FakeAppServerClient(invalid_context=True)
        first = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=factory,
            )
        )
        self.assertFalse(first["ok"])
        self.assertEqual(first["failed_episodes"], 1)
        self.assertFalse((execution_dir / "phase-a-contexts" / "holdout-manifest.json").exists())
        call_count = sum(len(client.calls) for client in FakeAppServerClient.instances)
        second = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=lambda: self.fail("failed context was retried"),
            )
        )
        self.assertEqual(first, second)
        self.assertEqual(sum(len(client.calls) for client in FakeAppServerClient.instances), call_count)

    def test_acceptance_selection_requires_exact_120_case_subset_and_no_adaptive_top_up(self) -> None:
        reservoir = []
        selected = []
        counts = {}
        strata = [("no_signal", 0), ("low", 1), ("medium", 5), ("dense", 16)]
        order = 0
        for stratum, event_count in strata:
            counts[f"paired_quality:{stratum}"] = 15
            for _ in range(15):
                segment_id = f"quality_{order:03d}"
                row = {
                    "segment_id": segment_id,
                    "episode_id": f"ep_{order:03d}",
                    "transcript_id": f"tr_{order:03d}",
                    "text_sha256": hashlib.sha256(segment_id.encode()).hexdigest(),
                    "reservoir": "paired_quality_reservoir",
                }
                reservoir.append(row)
                selected.append(
                    {
                        **row,
                        "selection_order": order,
                        "evaluation_set": "paired_quality",
                        "stratum": stratum,
                        "reference_event_count": event_count,
                        "reference_case_sha256": hashlib.sha256(
                            f"reference-{segment_id}".encode()
                        ).hexdigest(),
                    }
                )
                order += 1
        counts["clean_no_signal_power:no_signal"] = 60
        for _ in range(60):
            segment_id = f"clean_{order:03d}"
            row = {
                "segment_id": segment_id,
                "episode_id": f"ep_{order:03d}",
                "transcript_id": f"tr_{order:03d}",
                "text_sha256": hashlib.sha256(segment_id.encode()).hexdigest(),
                "reservoir": "terminal_position_no_signal_candidates",
            }
            reservoir.append(row)
            selected.append(
                {
                    **row,
                    "selection_order": order,
                    "evaluation_set": "clean_no_signal_power",
                    "stratum": "no_signal",
                    "reference_event_count": 0,
                    "reference_case_sha256": hashlib.sha256(
                        f"reference-{segment_id}".encode()
                    ).hexdigest(),
                    "reference_clean_no_signal": True,
                }
            )
            order += 1
        payload = {
            "schema_version": HOLDOUT_STRATIFIED_SELECTION_VERSION,
            "selection_status": "frozen_stratified_selection",
            "selection_frozen": True,
            "covenant_sha256": "a" * 64,
            "selection_basis": "llm_reference_only_pre_candidate_pre_baseline",
            "candidate_outputs_observed": False,
            "baseline_outputs_observed": False,
            "adaptive_top_up": False,
            "reference_artifact_sha256": "b" * 64,
            "stratum_counts": counts,
            "segments": selected,
        }
        path = self.root / "acceptance-selection.json"
        path.write_bytes(json_bytes(payload))
        verified = verify_stratified_selection(
            path,
            covenant_sha256="a" * 64,
            reservoir_segments=reservoir,
            require_acceptance_shape=True,
        )
        self.assertEqual(len(verified["segments"]), 120)
        self.assertEqual(verified["counts"], counts)

        adaptive = {**payload, "adaptive_top_up": True}
        adaptive_path = self.root / "adaptive-selection.json"
        adaptive_path.write_bytes(json_bytes(adaptive))
        with self.assertRaisesRegex(ValueError, "top-up"):
            verify_stratified_selection(
                adaptive_path,
                covenant_sha256="a" * 64,
                reservoir_segments=reservoir,
            )
        drifted = json.loads(json.dumps(payload))
        drifted["segments"][0]["text_sha256"] = "f" * 64
        drifted_path = self.root / "drifted-selection.json"
        drifted_path.write_bytes(json_bytes(drifted))
        with self.assertRaisesRegex(ValueError, "text_sha256 drift"):
            verify_stratified_selection(
                drifted_path,
                covenant_sha256="a" * 64,
                reservoir_segments=reservoir,
            )

        with self.assertRaisesRegex(ValueError, "requires a frozen stratified selection"):
            self.run_async(
                run_holdout_context_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=None,
                    execution_dir=self.root / "must-block-without-selection",
                )
            )

    def test_candidate_failure_and_baseline_failure_are_preserved_intent_to_treat(self) -> None:
        execution_dir = self.root / "itt"
        factory = lambda: FakeAppServerClient()
        contexts = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=factory,
            )
        )
        self.assertTrue(contexts["ok"])
        failing_runner = FakeCoreRunner(fail=True)
        capacity, capacity_sha256 = self._capacity_admission()
        candidate = self.run_async(
            run_holdout_candidate_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                capacity_admission=capacity,
                capacity_admission_sha256=capacity_sha256,
                fixture_mode=True,
                core_runner=failing_runner,
                client_factory=factory,
            )
        )
        self.assertFalse(candidate["ok"])
        self.assertTrue(candidate["execution_complete"])
        self.assertEqual(candidate["attempt_status"], "failed_no_retry")
        replay = self.run_async(
            run_holdout_candidate_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                fixture_mode=True,
                core_runner=lambda *_args, **_kwargs: self.fail("candidate was retried"),
            )
        )
        self.assertEqual(candidate, replay)
        self.assertEqual(len(failing_runner.calls), 1)

        baseline_factory = lambda: FakeAppServerClient(fail_segment="seg_0")
        baseline = self.run_async(
            run_holdout_baseline_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=baseline_factory,
            )
        )
        self.assertFalse(baseline["ok"])
        self.assertTrue(baseline["execution_complete"])
        self.assertEqual(baseline["failed_segments_intent_to_treat"], 1)

    def test_pre_turn_context_attempt_is_terminal_and_never_replayed(self) -> None:
        execution_dir = self.root / "pre-turn-context"
        with self.assertRaises(asyncio.CancelledError):
            self.run_async(
                run_holdout_context_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    client_factory=lambda: FakeAppServerClient(cancel_before_turn=True),
                )
            )
        episode_dir = execution_dir / "phase-a-contexts" / "episodes" / "ep_1"
        self.assertTrue((episode_dir / "attempt.json").is_file())
        self.assertFalse((episode_dir / "sidecar.json").exists())
        clients_before_resume = len(FakeAppServerClient.instances)

        report = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=lambda: FakeAppServerClient(),
            )
        )
        self.assertFalse(report["ok"])
        self.assertEqual(report["failed_episodes"], 1)
        self.assertEqual(
            report["outcomes"][0]["status"],
            "ambiguous_interrupted_no_retry",
        )
        self.assertEqual(json.loads((episode_dir / "attempt.json").read_text())["retry_ordinal"], 0)
        self.assertEqual(len(FakeAppServerClient.instances), clients_before_resume)

    def test_invalid_capacity_is_rejected_before_attempt_or_client(self) -> None:
        execution_dir = self.root / "invalid-capacity"
        contexts = self.run_async(
            run_holdout_context_phase(
                self.conn,
                covenant_path=self.covenant_path,
                frozen_winner_path=self.winner_path,
                stratified_selection_path=self.selection_path,
                execution_dir=execution_dir,
                acceptance_mode=False,
                client_factory=lambda: FakeAppServerClient(),
            )
        )
        self.assertTrue(contexts["ok"])
        stale, stale_sha256 = self._capacity_admission(
            issued_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
            expires_at=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        )
        runner = FakeCoreRunner()
        clients_before = len(FakeAppServerClient.instances)
        with self.assertRaisesRegex(Exception, "capacity admission"):
            self.run_async(
                run_holdout_candidate_phase(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=execution_dir,
                    acceptance_mode=False,
                    capacity_admission=stale,
                    capacity_admission_sha256=stale_sha256,
                    fixture_mode=True,
                    core_runner=runner,
                    client_factory=lambda: self.fail("client was created"),
                )
            )
        self.assertFalse((execution_dir / "phase-b-candidate" / "attempt.json").exists())
        self.assertEqual(runner.calls, [])
        self.assertEqual(len(FakeAppServerClient.instances), clients_before)

    def test_all_phase_orchestrator_routes_capacity_only_to_candidate(self) -> None:
        context = AsyncMock(return_value={"ok": True})
        candidate = AsyncMock(return_value={"ok": True})
        baseline = AsyncMock(return_value={"ok": True})
        admission = {"sentinel": "capacity"}
        context_patch = patch(
            "research_factory.app_server_holdout_execution.run_holdout_context_phase",
            context,
        )
        candidate_patch = patch(
            "research_factory.app_server_holdout_execution.run_holdout_candidate_phase",
            candidate,
        )
        baseline_patch = patch(
            "research_factory.app_server_holdout_execution.run_holdout_baseline_phase",
            baseline,
        )
        with context_patch, candidate_patch, baseline_patch:
            result = self.run_async(
                run_frozen_holdout_execution(
                    self.conn,
                    covenant_path=self.covenant_path,
                    frozen_winner_path=self.winner_path,
                    stratified_selection_path=self.selection_path,
                    execution_dir=self.root / "all-routing",
                    acceptance_mode=False,
                    capacity_admission=admission,
                    capacity_admission_sha256="a" * 64,
                    fixture_mode=True,
                )
            )
        self.assertTrue(result["ok"])
        context_kwargs = context.await_args.kwargs
        self.assertNotIn("capacity_admission", context_kwargs)
        self.assertNotIn("capacity_admission_sha256", context_kwargs)
        self.assertNotIn("fixture_mode", context_kwargs)
        candidate_kwargs = candidate.await_args.kwargs
        self.assertIs(candidate_kwargs["capacity_admission"], admission)
        self.assertEqual(candidate_kwargs["capacity_admission_sha256"], "a" * 64)
        self.assertTrue(candidate_kwargs["fixture_mode"])
        baseline_kwargs = baseline.await_args.kwargs
        self.assertNotIn("capacity_admission", baseline_kwargs)
        self.assertNotIn("fixture_mode", baseline_kwargs)


if __name__ == "__main__":
    unittest.main()
