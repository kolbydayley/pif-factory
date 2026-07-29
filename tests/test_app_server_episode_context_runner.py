from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research_factory import app_server_episode_context_runner as context_runner
from research_factory import db as factory_db
from research_factory.app_server_episode_context_runner import (
    CAPACITY_ADMISSION_CONTRACT,
    COMPLETION_RECEIPT_VERSION,
    CONTRACT_VERSION,
    FROZEN_CONFIGURATION_VERSION,
    HOLDOUT_AUTHORIZATION_VERSION,
    PINNED_CLI_VERSION,
    SEMANTIC_AUTHORITY_FIELDS,
    THREAD_MODE,
    TRANSPORT,
    EpisodeContextItem,
    EpisodeContextRunnerError,
    SQLiteEpisodeContextQueue,
    _freeze_required_manifest,
    build_backlog_snapshot,
    build_fixture_capacity_admission,
    load_episode_context_contract,
    run_episode_context_queue,
    verify_episode_context_artifact_for_episode,
    verify_episode_context_completion_receipt,
)
from research_factory.app_server_source_integrity import (
    SOURCE_INTEGRITY_VERSION,
    SourceIntegrityError,
    VerifiedSourceLoader,
)
from research_factory.codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    TURN_SIDECAR_SCHEMA_VERSION,
    AppServerThread,
)
from research_factory.pipeline_babysitter import verify_evaluation_receipt
from research_factory.worker import EPISODE_CONTEXT_SCHEMA_VERSION


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def pretty(value) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def record(path: Path) -> dict:
    return {
        "path": str(path.resolve()),
        "sha256": sha_bytes(path.read_bytes()),
        "size_bytes": path.stat().st_size,
    }


def create_managed_context_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS episode_context_runs (
          id TEXT PRIMARY KEY, job_id INTEGER, episode_id TEXT NOT NULL,
          transcript_id TEXT, label_pack TEXT NOT NULL, model TEXT NOT NULL,
          status TEXT NOT NULL, prompt_path TEXT, output_path TEXT,
          context_artifact_path TEXT, speaker_map_json TEXT NOT NULL DEFAULT '[]',
          section_map_json TEXT NOT NULL DEFAULT '[]',
          entity_seed_json TEXT NOT NULL DEFAULT '{}',
          concept_seed_json TEXT NOT NULL DEFAULT '[]',
          extraction_guidance TEXT, error TEXT, created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL, completed_at TEXT,
          UNIQUE(episode_id, label_pack, model)
        );
        """
    )
    connection.execute(context_runner._ATTEMPT_TARGET_TABLE_SQL)
    connection.execute(context_runner._ATTEMPT_ACTIVE_INDEX_SQL)


class StubVerifiedSourceLoader:
    def episode_packet(self, episode_id: str) -> dict:
        text = f"Verified private source for {episode_id}."
        digest = sha_bytes(text.encode())
        return {
            "episode": {
                "episode_id": episode_id,
                "transcript_id": f"transcript-{episode_id}",
            },
            "full_segmented_episode_text": text,
            "source_integrity": {
                "schema_version": SOURCE_INTEGRITY_VERSION,
                "episode_id": episode_id,
                "transcript_source_bases": [
                    {
                        "transcript_id": f"transcript-{episode_id}",
                        "source_basis": "cleaned_preparation",
                        "source_sha256": digest,
                    }
                ],
                "segments": [],
                "source_integrity_sha256": digest,
            },
        }


class FakeQueue:
    fixture_only = True

    def __init__(self, items, *, preclaimed=False, required_episode_ids=None):
        self.items = list(items)
        self.required_episode_ids = list(required_episode_ids or [
            item.episode_id for item in self.items
        ])
        self.status = {
            item.job_id: ("claimed" if preclaimed else "pending") for item in self.items
        }
        self.preview_calls = 0
        self.stale_calls = 0
        self.claim_calls = 0
        self.bindings = []
        self.submissions = []
        self.required_manifest_record = None
        self.loaded = None

    def discover_required_episode_ids(self):
        return sorted(set(self.required_episode_ids))

    def bind_required_episode_manifest(self, manifest, record):
        self.required_episode_ids = list(manifest["episode_ids"])
        self.required_manifest_record = dict(record)

    def bind_execution_contract(self, loaded):
        self.loaded = loaded

    def preview(self, *, limit):
        self.preview_calls += 1
        return [item for item in self.items if self.status[item.job_id] == "pending"][:limit]

    def stale_claims(self, *, limit):
        self.stale_calls += 1
        return [item for item in self.items if self.status[item.job_id] == "claimed"][:limit]

    def recover_stale_unbound_claims(self, *, limit):
        return 0

    def claim(self, *, limit):
        self.claim_calls += 1
        selected = [
            item for item in self.items if self.status[item.job_id] == "pending"
        ][:limit]
        for item in selected:
            self.status[item.job_id] = "claimed"
        return selected

    def bind_launch(self, item, launch_path):
        self.bindings.append((item.job_id, str(launch_path.resolve())))

    def bind_terminal(self, item, terminal_path):
        return None

    def submit(self, item, output_path):
        output = json.loads(Path(output_path).read_text(encoding="utf-8"))
        self.submissions.append((item.job_id, output))
        self.status[item.job_id] = "completed"
        return {
            "job_id": str(item.job_id),
            "episode_context_run_id": item.context_run_id,
            "semantic_output_sha256": sha_bytes(Path(output_path).read_bytes()),
            "production_mutated": False,
        }

    def accounting(self):
        counts = {key: 0 for key in ("pending", "claimed", "failed", "completed", "other")}
        for status in self.status.values():
            counts[status] += 1
        completed = [
            item.episode_id
            for item in self.items
            if self.status[item.job_id] == "completed"
        ]
        return build_backlog_snapshot(
            job_status_counts=counts,
            required_episode_ids=self.required_episode_ids,
            completed_episode_ids=completed,
            required_episode_manifest=self.required_manifest_record,
        )


class ForbiddenQueue:
    fixture_only = False

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def forbidden(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"live queue method called: {name}")

        return forbidden


class FakeClient:
    fixture_only = True
    instances = []

    def __init__(
        self,
        *,
        fail_after_write=False,
        duplicate_turn_id=False,
        thread_total_extra=False,
        nested_mismatch=False,
    ):
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.fail_after_write = fail_after_write
        self.duplicate_turn_id = duplicate_turn_id
        self.thread_total_extra = thread_total_extra
        self.nested_mismatch = nested_mismatch
        self.entered = 0
        self.exited = 0
        self.threads = []
        self.turns = []
        self.__class__.instances.append(self)

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited += 1

    async def start_thread(self, *, model, base_instructions, cwd, ephemeral):
        thread = AppServerThread(
            thread_id=f"fixture-thread-{len(self.threads) + 1}",
            model=model,
            cwd=str(Path(cwd).resolve()),
            ephemeral=ephemeral,
            instruction_sources_sha256="a" * 64,
            instruction_sources_count=1,
            base_instructions_sha256=sha_bytes(base_instructions.encode()),
            base_instructions_bytes=len(base_instructions.encode()),
        )
        self.threads.append(thread)
        return thread

    async def run_structured_turn(self, **kwargs):
        packet = json.loads(kwargs["prompt"])
        source = packet["source_input"]
        episode_id = packet["episode_id"]
        persisted = {
            "context_summary": source["full_episode_text"],
            "speaker_map": [{"name": f"Fixture speaker for {episode_id}"}],
            "section_map": [{"title": "Complete fixture episode"}],
            "entity_seed": {"episodes": [episode_id]},
            "concept_seed": ["semantic context"],
        }
        output = {
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_id": episode_id,
            **persisted,
            "episode_context": dict(persisted),
            "extraction_guidance": source["requested_guidance"],
            "excluded_source_context": list(source["llm_exclusions"]),
            "quality_flags": [],
            "overall_confidence": 0.9,
            "needs_review": False,
            "review_reason": None,
        }
        if self.nested_mismatch:
            output["episode_context"]["context_summary"] = "mismatched nested context"
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(pretty(output), encoding="utf-8")
        ordinal = len(self.turns) + 1
        usage = {
            "input_tokens": 100 * ordinal,
            "cached_input_tokens": 10 * ordinal,
            "output_tokens": 25 * ordinal,
            "reasoning_output_tokens": 5 * ordinal,
            "total_tokens": 125 * ordinal,
        }
        turn_id = "fixture-turn-constant" if self.duplicate_turn_id else f"fixture-turn-{ordinal}"
        sidecar = {
            "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
            "client_version": APP_SERVER_CLIENT_VERSION,
            "cli_version": PINNED_CLI_VERSION,
            "protocol_schema_sha256": "b" * 64,
            "state": "completed",
            "status": "completed",
            "transport": "stdio",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "model": kwargs["thread"].model,
            "effort": kwargs["effort"],
            "thread_mode": kwargs["thread_mode"],
            "batch_size": kwargs["batch_size"],
            "thread_id": kwargs["thread"].thread_id,
            "turn_id": turn_id,
            "prompt_sha256": sha_bytes(kwargs["prompt"].encode()),
            "prompt_bytes": len(kwargs["prompt"].encode()),
            "base_instructions_sha256": kwargs["thread"].base_instructions_sha256,
            "base_instructions_bytes": kwargs["thread"].base_instructions_bytes,
            "output_schema_sha256": sha_bytes(canonical(kwargs["output_schema"]).encode()),
            "output_schema_bytes": len(canonical(kwargs["output_schema"]).encode()),
            "output_path": str(output_path.resolve()),
            "output_sha256": sha_bytes(output_path.read_bytes()),
            "usage_complete": True,
            "usage_status": "measured",
            "usage": usage,
            "thread_total_usage": (
                {
                    **usage,
                    "input_tokens": usage["input_tokens"] + 1,
                    "total_tokens": usage["total_tokens"] + 1,
                }
                if self.thread_total_extra
                else usage
            ),
            "wall_elapsed_seconds": 1.25 * ordinal,
        }
        sidecar_path = Path(kwargs["sidecar_path"])
        sidecar_path.write_text(pretty(sidecar), encoding="utf-8")
        self.turns.append((kwargs["thread"].thread_id, turn_id, packet, output))
        if self.fail_after_write:
            self.fail_after_write = False
            raise OSError("fixture transport disconnected after durable output")
        return SimpleNamespace(status_ok=True, output=output)


class FixtureFactory:
    fixture_only = True

    def __init__(self, **client_options):
        self.client_options = client_options
        self.calls = []

    def __call__(self, binary):
        self.calls.append(Path(binary).resolve())
        return FakeClient(**self.client_options)


class ForbiddenFactory:
    fixture_only = True

    def __init__(self):
        self.calls = []

    def __call__(self, binary):
        self.calls.append(Path(binary))
        raise AssertionError("fixture client must not be created")


class FixtureCapacity:
    fixture_only = True

    def __init__(self, *, mutate=None):
        self.mutate = mutate
        self.calls = []

    def __call__(self, *, request):
        self.calls.append(dict(request))
        admission = build_fixture_capacity_admission(request)
        if self.mutate is not None:
            self.mutate(admission)
        return admission


class EpisodeContextRunnerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = self.root / "codex"
        self.binary.write_text("fixture official pinned codex", encoding="utf-8")
        self.prompt = self.root / "context-prompt.md"
        self.prompt.write_text(
            "Frozen full-episode context instructions. The LLM owns all semantic decisions.",
            encoding="utf-8",
        )
        self.schema = self.root / "context-schema.json"
        self.schema.write_text(
            pretty(
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "schema_version",
                        "episode_id",
                        "context_summary",
                        "speaker_map",
                        "section_map",
                        "entity_seed",
                        "concept_seed",
                        "episode_context",
                        "extraction_guidance",
                        "excluded_source_context",
                        "quality_flags",
                        "overall_confidence",
                        "needs_review",
                        "review_reason",
                    ],
                    "properties": {
                        "schema_version": {"const": EPISODE_CONTEXT_SCHEMA_VERSION},
                        "episode_id": {"type": "string"},
                        "context_summary": {"type": "string"},
                        "speaker_map": {"type": "array"},
                        "section_map": {"type": "array"},
                        "entity_seed": {"type": "object"},
                        "concept_seed": {"type": "array"},
                        "episode_context": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "context_summary",
                                "speaker_map",
                                "section_map",
                                "entity_seed",
                                "concept_seed",
                            ],
                            "properties": {
                                "context_summary": {"type": "string"},
                                "speaker_map": {"type": "array"},
                                "section_map": {"type": "array"},
                                "entity_seed": {"type": "object"},
                                "concept_seed": {"type": "array"},
                            },
                        },
                        "extraction_guidance": {"type": "string"},
                        "excluded_source_context": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "quality_flags": {"type": "array"},
                        "overall_confidence": {"type": "number"},
                        "needs_review": {"type": "boolean"},
                        "review_reason": {"type": ["string", "null"]},
                    },
                }
            ),
            encoding="utf-8",
        )
        self.configuration = self.root / "frozen-context-configuration.json"
        self.configuration.write_text(
            pretty(
                {
                    "schema_version": FROZEN_CONFIGURATION_VERSION,
                    "configuration_id": "context-winner-fixture",
                    "lane": "podcast",
                    "label_pack": "ai_discourse_v3_1",
                    "model": "gpt-5.6-sol",
                    "queue_payload_model": "gpt-5.5",
                    "reasoning_effort": "high",
                    "batch_size": 2,
                    "timeout_seconds": 600,
                    "transport": TRANSPORT,
                    "auth_mode": "chatgpt",
                    "plan_type": "pro",
                    "persistent_transport": True,
                    "structured_output": True,
                    "cli_version": PINNED_CLI_VERSION,
                    "thread_mode": THREAD_MODE,
                    "capacity_admission": CAPACITY_ADMISSION_CONTRACT,
                    "semantic_retry_count": 0,
                    "ambiguous_retry_allowed": False,
                    "deterministic_semantic_pruning": False,
                    "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
                    "prompt": record(self.prompt),
                    "prompt_sha256": sha_bytes(self.prompt.read_bytes()),
                    "output_schema": record(self.schema),
                    "output_schema_sha256": sha_bytes(
                        canonical(json.loads(self.schema.read_text())).encode()
                    ),
                }
            ),
            encoding="utf-8",
        )
        self.evaluation_root = self.root / "evaluation"
        self.evaluation_receipt = self._write_valid_evaluation_receipt()
        self.verified_evaluation = verify_evaluation_receipt(
            self.evaluation_receipt, self.evaluation_root
        )
        self.holdout_receipt = self.root / "context-holdout-authorization.json"
        self.holdout_receipt.write_text(
            pretty(
                {
                    "schema_version": HOLDOUT_AUTHORIZATION_VERSION,
                    "state": "accepted",
                    "phase": "episode_context",
                    "evaluation_id": self.verified_evaluation["evaluation_id"],
                    "evaluation_receipt": record(self.evaluation_receipt),
                    "frozen_configuration_sha256": sha_bytes(
                        self.configuration.read_bytes()
                    ),
                    "production_authorized": True,
                    "untouched_holdout_passed": True,
                    "semantic_noninferior_or_better": True,
                    "managed_app_server_auth_only": True,
                    "api_key_billing_allowed": False,
                    "raw_session_token_replay_allowed": False,
                    "semantic_retry_count": 0,
                }
            ),
            encoding="utf-8",
        )
        self.contract = self.root / "context-contract.json"
        self.write_contract()
        self.items = [
            EpisodeContextItem(
                job_id=index,
                episode_id=f"episode-{index}",
                context_run_id=f"context-run-{index}",
                claim_id=f"claim-{index}",
                attempt_id=f"attempt-{index}",
                source_input={
                    "full_episode_text": (
                        "Sponsor promo and page chrome are present, but the runner must not "
                        f"lexically prune this complete episode {index}."
                    ),
                    "requested_guidance": (
                        f"Model-authored semantic extraction guidance for complete episode {index}."
                    ),
                    "llm_exclusions": [f"LLM-authored exclusion {index}"],
                },
            )
            for index in (1, 2)
        ]

    def tearDown(self):
        FakeClient.instances.clear()
        self.temp.cleanup()

    def _write_valid_evaluation_receipt(self) -> Path:
        self.evaluation_root.mkdir()
        common = {
            "evaluation_id": "evaluation-context-fixture",
            "runtime_lock_sha256": "1" * 64,
            "frozen_configuration_sha256": sha_bytes(self.configuration.read_bytes()),
            "reference_sha256": "3" * 64,
            "holdout_manifest_sha256": "4" * 64,
        }
        artifacts = {
            "judge_gate": {
                "gate_passed": True,
                "semantic_noninferior_or_better": True,
                "ab_ba_order_balanced": True,
                "shared_augmented_reference": True,
                "abstention_enabled": True,
            },
            "development_freeze": {
                "development_frozen": True,
                "selection_frozen": True,
                "development_winner_frozen": True,
                "winner_system_id": "context-winner-fixture",
            },
            "untouched_holdout": {
                "untouched_holdout": True,
                "holdout_passed": True,
                "semantic_noninferior_or_better": True,
                "winner_system_id": "context-winner-fixture",
                "baseline_system_id": "context-baseline-fixture",
                "holdout_item_count": 60,
                "paired_bootstrap_unit": "source_cluster",
                "density_stratified": True,
                "intent_to_treat_failures_included": True,
                "paired_bootstrap_ci_lower": -0.02,
                "paired_bootstrap_confidence": 0.95,
                "exact_evidence_rate": 1.0,
            },
            "usage_telemetry": {
                "input_tokens": 10,
                "cached_input_tokens": 2,
                "output_tokens": 5,
                "reasoning_output_tokens": 1,
                "total_tokens": 15,
                "production_amortized_total_tokens": 21,
                "production_baseline_total_tokens": 100,
                "expected_turn_count": 1,
                "measured_turn_count": 1,
                "unknown_usage_turn_count": 0,
                "winner_system_id": "context-winner-fixture",
                "baseline_system_id": "context-baseline-fixture",
                "paired_item_count": 60,
                "baseline_item_count": 60,
                "candidate_item_count": 60,
                "wall_time_seconds": 1.5,
                "usage_complete": True,
                "cache_telemetry_complete": True,
                "failed_and_retried_calls_included": True,
                "same_exact_items": True,
                "same_concurrency_and_workers": True,
                "same_retry_and_fallback_policy": True,
                "same_cache_policy": True,
                "same_quota_window": True,
                "same_measurement_boundary": True,
                "episode_context_generation_accounted": True,
            },
            "production_integrity": {
                "managed_app_server_auth_only": True,
                "production_unchanged": True,
                "raw_session_token_replay": False,
                "api_key_billing": False,
            },
        }
        bindings = {}
        for role, payload in artifacts.items():
            artifact = self.evaluation_root / f"{role}.json"
            artifact.write_text(pretty({"artifact_role": role, **common, **payload}))
            bindings[role] = {
                "path": artifact.name,
                "sha256": sha_bytes(artifact.read_bytes()),
            }
        receipt = self.evaluation_root / "receipt.json"
        receipt.write_text(
            pretty(
                {
                    "schema_version": "pif_pipeline_evaluation_receipt_v2",
                    **common,
                    "goal_complete": True,
                    "judge_gate_passed": True,
                    "development_frozen": True,
                    "untouched_holdout_passed": True,
                    "semantic_noninferior_or_better": True,
                    "managed_app_server_auth_only": True,
                    "usage_and_cache_telemetry_complete": True,
                    "production_unchanged_during_evaluation": True,
                    "production_amortized_total_token_ratio": 0.21,
                    "artifact_hashes": bindings,
                }
            )
        )
        return receipt

    def write_contract(self):
        self.contract.write_text(
            pretty(
                {
                    "schema_version": CONTRACT_VERSION,
                    "state": "frozen_fixture_only",
                    "phase": "episode_context",
                    "transport": TRANSPORT,
                    "auth": {
                        "type": "chatgpt",
                        "plan_type": "pro",
                        "api_key_billing_allowed": False,
                        "raw_session_token_access_allowed": False,
                    },
                    "lineage": {
                        "evaluation_root": str(self.evaluation_root.resolve()),
                        "evaluation_receipt": record(self.evaluation_receipt),
                        "holdout_receipt": record(self.holdout_receipt),
                        "evaluation_id": self.verified_evaluation["evaluation_id"],
                        "verified_artifact_hashes": self.verified_evaluation[
                            "artifact_hashes"
                        ],
                        "frozen_configuration_sha256": sha_bytes(
                            self.configuration.read_bytes()
                        ),
                    },
                    "artifacts": {
                        "codex_binary": record(self.binary),
                        "runner_source": record(
                            Path(__file__).parents[1]
                            / "research_factory"
                            / "app_server_episode_context_runner.py"
                        ),
                        "frozen_configuration": record(self.configuration),
                        "prompt": record(self.prompt),
                        "output_schema": record(self.schema),
                    },
                    "execution": {
                        "fixture_only": True,
                        "live_model_calls_allowed": False,
                        "live_database_mutation_allowed": False,
                        "persistent_app_server_processes": 1,
                        "thread_mode": THREAD_MODE,
                        "required_episode_manifest": "immutable_before_claim",
                        "capacity_admission": CAPACITY_ADMISSION_CONTRACT,
                        "semantic_retry_count": 0,
                        "ambiguous_retry_allowed": False,
                        "stale_claim_replay_allowed": False,
                        "deterministic_semantic_pruning": False,
                    },
                }
            ),
            encoding="utf-8",
        )

    def build_live_plan_database(self, name: str):
        database_path = self.root / f"{name}.sqlite"
        connection = sqlite3.connect(str(database_path))
        connection.row_factory = sqlite3.Row
        factory_db.init_db(connection)
        timestamp = "2026-07-18T00:00:00+00:00"
        connection.execute(
            "INSERT INTO sources (id, name, created_at, updated_at) "
            "VALUES ('source-live', 'Fixture', ?, ?)",
            (timestamp, timestamp),
        )
        episode_kinds = (
            ("episode-rerun", "completed"),
            ("episode-reset", "failed"),
            ("episode-new", "missing"),
        )
        word_counts = {}
        context_jobs = {}
        legacy_records = {}
        for index, (episode_id, context_state) in enumerate(episode_kinds, start=1):
            transcript_id = f"transcript-{index}"
            raw_path = self.root / f"{name}-{episode_id}.txt"
            text = " ".join([f"word{index}"] * (index * 10))
            raw_path.write_text(text, encoding="utf-8")
            digest = sha_bytes(raw_path.read_bytes())
            word_counts[episode_id] = index * 10
            connection.execute(
                "INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at) "
                "VALUES (?, 'source-live', ?, ?, ?, ?)",
                (episode_id, f"guid-{index}", episode_id, timestamp, timestamp),
            )
            connection.execute(
                "INSERT INTO transcripts (id, episode_id, source_kind, raw_text_path, "
                "raw_text_sha256, status, word_count, created_at, updated_at) "
                "VALUES (?, ?, 'fixture', ?, ?, 'ready', ?, ?, ?)",
                (
                    transcript_id,
                    episode_id,
                    str(raw_path),
                    digest,
                    index * 10,
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO segments (id, transcript_id, episode_id, source_id, "
                "segment_index, start_char, end_char, text_path, text_sha256, "
                "word_count, created_at) VALUES (?, ?, ?, 'source-live', 0, 0, ?, ?, ?, ?, ?)",
                (
                    f"segment-{index}",
                    transcript_id,
                    episode_id,
                    len(text),
                    str(raw_path),
                    digest,
                    index * 10,
                    timestamp,
                ),
            )
            factory_db.enqueue_job(
                connection,
                lane="podcast",
                job_type="label_segment",
                target_id=f"segment-{index}",
                payload={"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"},
                priority=100,
                max_attempts=2,
            )
            if context_state != "missing":
                context_job_id = factory_db.enqueue_job(
                    connection,
                    lane="podcast",
                    job_type="episode_context",
                    target_id=episode_id,
                    payload={"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"},
                    priority=99,
                    max_attempts=2,
                )
                context_jobs[episode_id] = int(context_job_id)
                connection.execute(
                    "UPDATE jobs SET status = ?, error = ? WHERE id = ?",
                    (
                        context_state,
                        "fixture failed" if context_state == "failed" else None,
                        context_job_id,
                    ),
                )
            if context_state == "completed":
                prompt_path = self.root / f"{name}-legacy-prompt.json"
                output_path = self.root / f"{name}-legacy-output.json"
                artifact_path = self.root / f"{name}-legacy-artifact.json"
                prompt_path.write_text('{"legacy":true}\n', encoding="utf-8")
                output_path.write_text('{"legacy":true}\n', encoding="utf-8")
                artifact_path.write_text('{"legacy":true}\n', encoding="utf-8")
                legacy_records = {
                    "prompt": record(prompt_path),
                    "output": record(output_path),
                    "artifact": record(artifact_path),
                }
                connection.execute(
                    "INSERT INTO episode_context_runs "
                    "(id, job_id, episode_id, transcript_id, label_pack, model, status, "
                    "prompt_path, output_path, context_artifact_path, created_at, updated_at, completed_at) "
                    "VALUES ('legacy-context-run', ?, ?, ?, 'ai_discourse_v3_1', "
                    "'gpt-5.5', 'completed', ?, ?, ?, ?, ?, ?)",
                    (
                        context_jobs[episode_id],
                        episode_id,
                        transcript_id,
                        str(prompt_path),
                        str(output_path),
                        str(artifact_path),
                        timestamp,
                        timestamp,
                        timestamp,
                    ),
                )
        connection.commit()
        return database_path, connection, word_counts, legacy_records

    def verifier(self, run_root: Path):
        return verify_episode_context_completion_receipt(
            run_root / "completion-receipt.json",
            expected_contract_sha256=sha_bytes(self.contract.read_bytes()),
            expected_evaluation_receipt=self.evaluation_receipt,
            expected_holdout_receipt=self.holdout_receipt,
        )

    def bind_sqlite_queue(self, queue, name):
        loaded = load_episode_context_contract(self.contract)
        queue.bind_execution_contract(loaded)
        manifest, manifest_record = _freeze_required_manifest(
            root=self.root / name,
            loaded=loaded,
            episode_ids=queue.discover_required_episode_ids(),
        )
        queue.bind_required_episode_manifest(manifest, manifest_record)
        return loaded

    def managed_context_queue(
        self, *, name: str, episode_id: str, file_backed: bool = False
    ):
        database = self.root / f"{name}.sqlite" if file_backed else Path(":memory:")
        connection = sqlite3.connect(str(database))
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              target_id TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL,
              priority INTEGER NOT NULL, attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, completed_at TEXT, error TEXT
            );
            """
        )
        create_managed_context_tables(connection)
        segment_id = f"segment-{name}"
        payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        connection.execute("INSERT INTO segments VALUES (?, ?)", (segment_id, episode_id))
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', ?, ?, 'pending', "
            "100, 0, 3, NULL, NULL, ?, '', '', NULL, NULL)",
            (segment_id, payload, f"label-{name}"),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'episode_context', ?, ?, 'pending', "
            "10, 0, 3, NULL, NULL, ?, '', '', NULL, NULL)",
            (episode_id, payload, f"context-{name}"),
        )
        connection.commit()
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id=f"worker-{name}",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / f"attempts-{name}",
            source_loader=StubVerifiedSourceLoader(),
        )
        loaded = self.bind_sqlite_queue(queue, f"manifest-{name}")
        return connection, queue, loaded

    def reopen_managed_context_queue(self, *, name: str, loaded, manifest_record):
        connection = sqlite3.connect(str(self.root / f"{name}.sqlite"))
        connection.row_factory = sqlite3.Row
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id=f"worker-{name}-resumed",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / f"attempts-{name}",
            source_loader=StubVerifiedSourceLoader(),
        )
        queue.bind_execution_contract(loaded)
        manifest = json.loads(
            Path(manifest_record["path"]).read_text(encoding="utf-8")
        )
        queue.bind_required_episode_manifest(manifest, manifest_record)
        return connection, queue

    def managed_context_claim(self, *, name: str, episode_id: str):
        connection, queue, loaded = self.managed_context_queue(
            name=name, episode_id=episode_id
        )
        claimed = queue.claim(limit=1)
        self.assertEqual(len(claimed), 1)
        return connection, queue, loaded, claimed[0]

    def live_capacity_fixture(self, *, name: str, loaded, queue):
        runtime_path = self.root / f"{name}-runtime.json"
        runtime_path.write_text("{}\n", encoding="utf-8")
        provision_path = self.root / f"{name}-provision.json"
        provision_path.write_text("{}\n", encoding="utf-8")
        policy_path = self.root / f"{name}-policy.json"
        policy_path.write_text("{}\n", encoding="utf-8")
        policy = {
            "minimum_remaining_reserve_percent": 20,
            "quota_points_per_million_tokens": 10_000,
            "maximum_total_tokens_per_episode": 100,
            "population_episode_count": 1,
            "population_source_segment_count": 1,
            "population_source_word_count": 10,
            "max_episode_source_word_count": 10,
            "population_total_token_bound": 100,
        }
        runtime = {
            "path": runtime_path,
            "capacity_policy": policy,
            "authorization": {
                "provision_plan": record(provision_path),
                "capacity_policy": record(policy_path),
            },
        }

        class RateClient:
            account_summary = {
                "type": "chatgpt",
                "plan_type": "pro",
                "requires_openai_auth": False,
            }

            def __init__(self):
                self.requests = 0

            async def _request(self, method, params):
                self.requests += 1
                if method != "account/rateLimits/read" or params != {}:
                    raise AssertionError("unexpected capacity request")
                bucket = {
                    "limitId": "codex",
                    "planType": "pro",
                    "primary": {
                        "usedPercent": 10,
                        "resetsAt": 1_800_000_000,
                        "windowDurationMins": 300,
                    },
                    "rateLimitReachedType": None,
                }
                return {"rateLimits": bucket}

        client = RateClient()
        provider = context_runner.ManagedEpisodeContextReserveCapacityProvider(
            client=client,
            policy=policy,
            policy_record=runtime["authorization"]["capacity_policy"],
        )
        return runtime, provider, client

    def live_capacity_request(
        self,
        *,
        loaded,
        runtime,
        queue,
        item,
        run_id,
        ordinal,
        stage,
    ):
        return context_runner._live_capacity_request_payload(
            loaded=loaded,
            run_id=run_id,
            ordinal=ordinal,
            admission_stage=stage,
            item=item,
            backlog=queue.accounting(),
            required_episode_manifest=queue._required_manifest_record,
            provision_plan_record=runtime["authorization"]["provision_plan"],
            capacity_policy_record=runtime["authorization"]["capacity_policy"],
            runtime_authorization_record=record(runtime["path"]),
            policy=runtime["capacity_policy"],
            cumulative_measured_total_tokens=0,
        )

    async def publish_capacity(
        self, *, root, loaded, runtime, request, provider
    ):
        return await context_runner._persist_live_capacity_admission(
            root=root,
            loaded=loaded,
            request=request,
            policy=runtime["capacity_policy"],
            provision_plan_record=runtime["authorization"]["provision_plan"],
            capacity_policy_record=runtime["authorization"]["capacity_policy"],
            runtime_authorization_record=record(runtime["path"]),
            provider=provider,
        )

    def context_output(self, episode_id: str) -> dict:
        persisted = {
            "context_summary": "Managed canonical episode context.",
            "speaker_map": [{"name": "Host"}],
            "section_map": [{"title": "Discussion"}],
            "entity_seed": {"orgs": ["OpenAI"]},
            "concept_seed": ["evaluation"],
        }
        return {
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_id": episode_id,
            **persisted,
            "episode_context": dict(persisted),
            "extraction_guidance": "Preserve complete context and semantic uncertainty.",
            "excluded_source_context": ["Sponsor read"],
            "quality_flags": [],
            "overall_confidence": 0.9,
            "needs_review": False,
            "review_reason": None,
        }

    def write_managed_sqlite_turn(
        self,
        *,
        queue: SQLiteEpisodeContextQueue,
        item: EpisodeContextItem,
        loaded,
        name: str,
        output: dict,
        finalize_terminal: bool = True,
        perform_submit: bool = True,
    ):
        run_root = self.root / name
        paths = context_runner._turn_paths(run_root, item)
        input_payload = context_runner._item_input(item)
        prompt = canonical(input_payload)
        context_runner._write_immutable(paths["input"], prompt.encode())
        run_id = f"fixture-{name}"
        request = context_runner._capacity_request_payload(
            loaded=loaded,
            run_id=run_id,
            backlog=queue.accounting(),
            required_episode_manifest=queue._required_manifest_record,
            items=[item],
        )
        _admission, capacity_binding = context_runner._persist_capacity_admission(
            root=run_root,
            loaded=loaded,
            request=request,
            provider=FixtureCapacity(),
        )
        thread = AppServerThread(
            thread_id=f"thread-{name}",
            model=loaded["configuration"]["model"],
            cwd=str(self.root),
            ephemeral=True,
            instruction_sources_sha256="a" * 64,
            instruction_sources_count=1,
            base_instructions_sha256=sha_bytes(
                loaded["prompt_path"].read_bytes()
            ),
            base_instructions_bytes=loaded["prompt_path"].stat().st_size,
        )
        launch = context_runner._launch_for_item(
            loaded=loaded,
            run_id=run_id,
            lifecycle_id=f"lifecycle-{name}",
            item=item,
            thread=thread,
            input_record=record(paths["input"]),
            required_episode_manifest=queue._required_manifest_record,
            capacity_binding=capacity_binding,
        )
        paths["launch"].parent.mkdir(parents=True, exist_ok=True)
        paths["launch"].write_text(pretty(launch), encoding="utf-8")
        queue.bind_launch(item, paths["launch"])
        paths["output"].write_text(pretty(output), encoding="utf-8")
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 25,
            "reasoning_output_tokens": 5,
            "total_tokens": 125,
        }
        sidecar = {
            "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
            "client_version": APP_SERVER_CLIENT_VERSION,
            "cli_version": PINNED_CLI_VERSION,
            "protocol_schema_sha256": "b" * 64,
            "state": "completed",
            "status": "completed",
            "transport": "stdio",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "model": loaded["configuration"]["model"],
            "effort": loaded["configuration"]["reasoning_effort"],
            "thread_mode": THREAD_MODE,
            "batch_size": 1,
            "thread_id": thread.thread_id,
            "turn_id": f"turn-{name}",
            "prompt_sha256": sha_bytes(prompt.encode()),
            "prompt_bytes": len(prompt.encode()),
            "base_instructions_sha256": thread.base_instructions_sha256,
            "base_instructions_bytes": thread.base_instructions_bytes,
            "output_schema_sha256": sha_bytes(
                canonical(loaded["schema"]).encode()
            ),
            "output_schema_bytes": len(canonical(loaded["schema"]).encode()),
            "output_path": str(paths["output"].resolve()),
            "output_sha256": sha_bytes(paths["output"].read_bytes()),
            "usage_complete": True,
            "usage_status": "measured",
            "usage": usage,
            "thread_total_usage": usage,
            "wall_elapsed_seconds": 1.25,
        }
        paths["sidecar"].write_text(pretty(sidecar), encoding="utf-8")
        if not perform_submit:
            return paths, None
        submission = queue.submit(item, paths["output"])
        if finalize_terminal:
            terminal = context_runner._terminal_for_turn(
                loaded=loaded,
                launch_path=paths["launch"],
                input_path=paths["input"],
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                sidecar=sidecar,
                submission=submission,
                recovered=False,
            )
            paths["terminal"].write_text(pretty(terminal), encoding="utf-8")
            queue.bind_terminal(item, paths["terminal"])
        return paths, submission

    def test_contract_requires_exact_evaluation_holdout_and_configuration_lineage(self):
        with patch(
            "research_factory.app_server_episode_context_runner.verify_evaluation_receipt",
            wraps=verify_evaluation_receipt,
        ) as strict_verifier:
            loaded = load_episode_context_contract(self.contract)
        strict_verifier.assert_called_once_with(
            self.evaluation_receipt.resolve(), self.evaluation_root.resolve()
        )
        self.assertEqual(loaded["configuration"]["model"], "gpt-5.6-sol")
        self.assertEqual(loaded["configuration"]["queue_payload_model"], "gpt-5.5")
        self.assertEqual(loaded["configuration"]["lane"], "podcast")
        self.assertEqual(loaded["configuration"]["label_pack"], "ai_discourse_v3_1")
        self.assertEqual(loaded["configuration"]["batch_size"], 2)
        self.assertEqual(
            loaded["configuration"]["semantic_authority_fields"],
            list(SEMANTIC_AUTHORITY_FIELDS),
        )
        self.prompt.write_text("tampered prompt", encoding="utf-8")
        with self.assertRaisesRegex(EpisodeContextRunnerError, "prompt artifact drifted"):
            load_episode_context_contract(self.contract)

    def test_unaccepted_holdout_fails_closed_before_queue_or_client(self):
        payload = json.loads(self.holdout_receipt.read_text())
        payload["production_authorized"] = False
        self.holdout_receipt.write_text(pretty(payload))
        self.write_contract()
        with self.assertRaisesRegex(EpisodeContextRunnerError, "did not authorize production"):
            load_episode_context_contract(self.contract)

    def test_sqlite_adapter_round_trips_canonical_semantic_authority_unmocked(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              target_id TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL,
              priority INTEGER NOT NULL, attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, completed_at TEXT, error TEXT
            );
            """
        )
        create_managed_context_tables(connection)
        connection.execute("INSERT INTO segments VALUES ('sqlite-segment', 'sqlite-episode')")
        queue_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', 'sqlite-segment', ?, "
            "'pending', 100, 0, 3, NULL, NULL, 'sqlite-label', '', '', NULL, NULL)",
            (queue_payload,),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'episode_context', 'sqlite-episode', ?, "
            "'pending', 10, 0, 3, NULL, NULL, 'sqlite-context', '', '', NULL, NULL)",
            (queue_payload,),
        )
        connection.commit()
        persisted = {
            "context_summary": "Canonical full episode context.",
            "speaker_map": [{"name": "Host"}],
            "section_map": [{"title": "Discussion"}],
            "entity_seed": {"orgs": ["OpenAI"]},
            "concept_seed": ["model evaluation"],
        }
        output = {
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_id": "sqlite-episode",
            **persisted,
            "episode_context": dict(persisted),
            "extraction_guidance": (
                "Use the complete semantic context and preserve speaker uncertainty carefully."
            ),
            "excluded_source_context": ["Sponsor read", "Page chrome"],
            "quality_flags": [],
            "overall_confidence": 0.9,
            "needs_review": False,
            "review_reason": None,
        }
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="fixture-sqlite-worker",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / "sqlite-attempts",
            source_loader=StubVerifiedSourceLoader(),
        )
        loaded = self.bind_sqlite_queue(queue, "sqlite-round-trip-manifest")
        claimed = queue.claim(limit=1)
        self.assertEqual(len(claimed), 1)
        item = claimed[0]
        _paths, result = self.write_managed_sqlite_turn(
            queue=queue,
            item=item,
            loaded=loaded,
            name="sqlite-turn",
            output=output,
        )
        artifact = json.loads(Path(result["context_artifact_path"]).read_text())
        self.assertEqual(artifact["episode_context"], persisted)
        self.assertEqual(artifact["excluded_source_context"], output["excluded_source_context"])
        self.assertEqual(artifact["extraction_guidance"], output["extraction_guidance"])
        row = connection.execute(
            "SELECT * FROM episode_context_runs WHERE id = ?", (item.context_run_id,)
        ).fetchone()
        self.assertEqual(row["status"], "completed")
        self.assertEqual(json.loads(row["speaker_map_json"]), persisted["speaker_map"])
        self.assertTrue(result["production_mutated"])
        connection.close()

    def test_sqlite_lineage_write_failure_rolls_back_completion_and_resubmits_without_replay(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              target_id TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL,
              priority INTEGER NOT NULL, attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, completed_at TEXT, error TEXT
            );
            """
        )
        create_managed_context_tables(connection)
        connection.execute(
            "INSERT INTO segments VALUES ('sqlite-atomic-segment', 'sqlite-atomic-episode')"
        )
        queue_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', "
            "'sqlite-atomic-segment', ?, 'pending', 100, 0, 3, NULL, NULL, "
            "'sqlite-atomic-label', '', '', NULL, NULL)",
            (queue_payload,),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'episode_context', "
            "'sqlite-atomic-episode', ?, 'pending', 10, 0, 3, NULL, NULL, "
            "'sqlite-atomic-context', '', '', NULL, NULL)",
            (queue_payload,),
        )
        connection.commit()
        persisted = {
            "context_summary": "Canonical context retained for recovery.",
            "speaker_map": [{"name": "Host"}],
            "section_map": [{"title": "Discussion"}],
            "entity_seed": {"orgs": ["OpenAI"]},
            "concept_seed": ["evaluation"],
        }
        output = {
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_id": "sqlite-atomic-episode",
            **persisted,
            "episode_context": dict(persisted),
            "extraction_guidance": (
                "Use complete context and preserve semantic uncertainty during extraction."
            ),
            "excluded_source_context": ["Sponsor read"],
            "quality_flags": [],
            "overall_confidence": 0.9,
            "needs_review": False,
            "review_reason": None,
        }
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="atomic-worker",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / "sqlite-atomic-attempts",
            source_loader=StubVerifiedSourceLoader(),
        )
        loaded = self.bind_sqlite_queue(queue, "sqlite-atomic-manifest")
        item = queue.claim(limit=1)[0]
        paths, _submission = self.write_managed_sqlite_turn(
            queue=queue,
            item=item,
            loaded=loaded,
            name="sqlite-atomic-turn",
            output=output,
            finalize_terminal=False,
            perform_submit=False,
        )
        original_output_sha256 = sha_bytes(paths["output"].read_bytes())
        with patch(
            "research_factory.app_server_episode_context_runner._write_immutable",
            side_effect=OSError("injected lineage publication failure"),
        ):
            with self.assertRaisesRegex(OSError, "lineage publication failure"):
                queue.submit(item, paths["output"])

        job = connection.execute("SELECT * FROM jobs WHERE id = 2").fetchone()
        context_run = connection.execute(
            "SELECT * FROM episode_context_runs WHERE id = ?", (item.context_run_id,)
        ).fetchone()
        self.assertEqual(job["status"], "claimed")
        self.assertEqual(job["lease_owner"], "atomic-worker")
        self.assertEqual(context_run["status"], "claimed")
        self.assertIsNone(context_run["context_artifact_path"])
        self.assertFalse(connection.in_transaction)

        # The completed structured output is adopted by the same attempt;
        # no semantic turn or new attempt is created.
        result = queue.submit(item, paths["output"])

        self.assertEqual(sha_bytes(paths["output"].read_bytes()), original_output_sha256)
        self.assertEqual(
            connection.execute("SELECT status FROM jobs WHERE id = 2").fetchone()["status"],
            "completed",
        )
        self.assertEqual(
            connection.execute(
                "SELECT status FROM episode_context_runs WHERE id = ?",
                (item.context_run_id,),
            ).fetchone()["status"],
            "completed",
        )
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM episode_context_run_attempts WHERE canonical_run_id = ?",
                (item.context_run_id,),
            ).fetchone()[0],
            1,
        )
        lineage_path = Path(result["artifact_lineage"]["path"])
        self.assertTrue(lineage_path.is_file())
        self.assertTrue(result["production_mutated"])
        connection.close()

    def test_claimed_source_snapshot_rejects_self_consistent_substituted_turn_input(self):
        connection, queue, loaded, item = self.managed_context_claim(
            name="source-binding", episode_id="source-binding-episode"
        )
        run_root = self.root / "source-binding-turn"
        paths = context_runner._turn_paths(run_root, item)
        substituted_source = dict(item.source_input)
        substituted_source["full_segmented_episode_text"] = (
            "A different but internally self-consistent source packet."
        )
        substituted_item = EpisodeContextItem(
            job_id=item.job_id,
            episode_id=item.episode_id,
            context_run_id=item.context_run_id,
            claim_id=item.claim_id,
            attempt_id=item.attempt_id,
            queue_attempt_number=item.queue_attempt_number,
            source_input=substituted_source,
            submission_output_path=item.submission_output_path,
        )
        input_payload = context_runner._item_input(substituted_item)
        paths["input"].parent.mkdir(parents=True, exist_ok=True)
        paths["input"].write_text(pretty(input_payload), encoding="utf-8")
        run_id = "fixture-source-binding"
        request = context_runner._capacity_request_payload(
            loaded=loaded,
            run_id=run_id,
            backlog=queue.accounting(),
            required_episode_manifest=queue._required_manifest_record,
            items=[item],
        )
        _admission, capacity_binding = context_runner._persist_capacity_admission(
            root=run_root,
            loaded=loaded,
            request=request,
            provider=FixtureCapacity(),
        )
        thread = AppServerThread(
            thread_id="thread-source-binding",
            model=loaded["configuration"]["model"],
            cwd=str(self.root),
            ephemeral=True,
            instruction_sources_sha256="c" * 64,
            instruction_sources_count=1,
            base_instructions_sha256=sha_bytes(loaded["prompt_path"].read_bytes()),
            base_instructions_bytes=loaded["prompt_path"].stat().st_size,
        )
        launch = context_runner._launch_for_item(
            loaded=loaded,
            run_id=run_id,
            lifecycle_id="lifecycle-source-binding",
            item=substituted_item,
            thread=thread,
            input_record=record(paths["input"]),
            required_episode_manifest=queue._required_manifest_record,
            capacity_binding=capacity_binding,
        )
        paths["launch"].write_text(pretty(launch), encoding="utf-8")

        with self.assertRaisesRegex(
            EpisodeContextRunnerError, "differs from the claimed source snapshot"
        ):
            queue.bind_launch(item, paths["launch"])

        attempt = connection.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?", (item.attempt_id,)
        ).fetchone()
        self.assertIsNone(attempt["launch_path"])
        self.assertEqual(
            set(json.loads(attempt["artifact_records_json"])), {"prompt"}
        )
        self.assertFalse(queue._attempt_paths(item.attempt_id)["launch"].exists())
        connection.close()

    def test_existing_terminal_is_bound_to_completed_attempt_without_replay(self):
        connection, queue, loaded, item = self.managed_context_claim(
            name="terminal-bind", episode_id="terminal-bind-episode"
        )
        run_root = self.root / "terminal-bind-turn"
        paths, submission = self.write_managed_sqlite_turn(
            queue=queue,
            item=item,
            loaded=loaded,
            name="terminal-bind-turn",
            output=self.context_output(item.episode_id),
            finalize_terminal=False,
        )
        sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
        terminal = context_runner._terminal_for_turn(
            loaded=loaded,
            launch_path=paths["launch"],
            input_path=paths["input"],
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            sidecar=sidecar,
            submission=submission,
            recovered=False,
        )
        paths["terminal"].write_text(pretty(terminal), encoding="utf-8")
        before = json.loads(
            connection.execute(
                "SELECT artifact_records_json FROM episode_context_run_attempts WHERE id = ?",
                (item.attempt_id,),
            ).fetchone()[0]
        )
        self.assertNotIn("terminal", before)

        recovered = context_runner._recover_db_completed_unterminalized_turns(
            loaded=loaded,
            root=run_root,
            queue=queue,
        )
        self.assertEqual(recovered, 1)
        after = json.loads(
            connection.execute(
                "SELECT artifact_records_json FROM episode_context_run_attempts WHERE id = ?",
                (item.attempt_id,),
            ).fetchone()[0]
        )
        self.assertEqual(after["terminal"], record(paths["terminal"]))
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM episode_context_run_attempts WHERE canonical_run_id = ?",
                (item.context_run_id,),
            ).fetchone()[0],
            1,
        )
        connection.close()

    def test_required_episode_discovery_preserves_rewritten_queue_payload_model_scope(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              target_id TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL
            );
            """
        )
        rows = [
            (
                "rewritten-in-scope",
                {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.6-sol", "queue_payload_model": "gpt-5.5"},
            ),
            (
                "rewritten-out-of-scope",
                {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.6-sol", "queue_payload_model": "other-model"},
            ),
            ("legacy-null-model", {"label_pack": "ai_discourse_v3_1"}),
        ]
        for index, (episode_id, payload) in enumerate(rows, start=1):
            segment_id = f"segment-{index}"
            connection.execute("INSERT INTO segments VALUES (?, ?)", (segment_id, episode_id))
            connection.execute(
                "INSERT INTO jobs VALUES (?, 'podcast', 'label_segment', ?, ?, 'claimed')",
                (index, segment_id, canonical(payload)),
            )
        connection.commit()
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="scope-worker",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            source_loader=StubVerifiedSourceLoader(),
        )
        self.assertEqual(
            queue.discover_required_episode_ids(),
            ["legacy-null-model", "rewritten-in-scope"],
        )
        connection.close()

    def test_sqlite_atomic_claim_cannot_mutate_higher_priority_out_of_scope_rows(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              status TEXT NOT NULL, target_id TEXT NOT NULL, payload_json TEXT NOT NULL,
              attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
              lease_owner TEXT, leased_until TEXT, updated_at TEXT NOT NULL,
              priority INTEGER NOT NULL
            );
            """
        )
        create_managed_context_tables(connection)
        timestamp = "2026-07-18T00:00:00+00:00"
        connection.execute(
            "INSERT INTO segments VALUES ('required-segment', 'required-episode')"
        )
        connection.execute(
            "INSERT INTO segments VALUES ('required-null-segment', 'required-null-episode')"
        )
        label_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', 'pending', "
            "'required-segment', ?, 0, 2, NULL, NULL, ?, 100)",
            (label_payload, timestamp),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'label_segment', 'pending', "
            "'required-null-segment', ?, 0, 2, NULL, NULL, ?, 101)",
            (label_payload, timestamp),
        )
        candidate_payloads = {
            10: {
                "label_pack": "ai_discourse_v1",
                "model": "gpt-5.5",
                "episode_context_run_id": "wrong-pack-run",
            },
            11: {
                "label_pack": "ai_discourse_v3_1",
                "model": "gpt-5.6-sol",
                "episode_context_run_id": "wrong-model-run",
            },
            12: {
                "label_pack": "ai_discourse_v3_1",
                "model": "gpt-5.5",
                "episode_context_run_id": "context-only-run",
            },
            13: {
                "label_pack": "ai_discourse_v3_1",
                "model": "gpt-5.5",
                "episode_context_run_id": "exact-run",
            },
            14: {
                "label_pack": "ai_discourse_v3_1",
                "episode_context_run_id": "null-model-run",
            },
        }
        targets = {
            10: "required-episode",
            11: "required-episode",
            12: "context-only-episode",
            13: "required-episode",
            14: "required-null-episode",
        }
        for priority, job_id in enumerate((10, 11, 12, 13, 14)):
            connection.execute(
                "INSERT INTO jobs VALUES (?, 'podcast', 'episode_context', 'pending', "
                "?, ?, 0, 2, NULL, NULL, ?, ?)",
                (
                    job_id,
                    targets[job_id],
                    canonical(candidate_payloads[job_id]),
                    timestamp,
                    priority,
                ),
            )
        connection.commit()
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="atomic-scope-worker",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / "atomic-scope-attempts",
            source_loader=StubVerifiedSourceLoader(),
        )
        self.bind_sqlite_queue(queue, "atomic-claim-required-manifest")

        self.assertEqual(
            [item.job_id for item in queue.preview(limit=2)], [13, 14]
        )
        claimed = queue.claim(limit=2)
        self.assertEqual([item.job_id for item in claimed], [13, 14])
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM episode_context_run_attempts WHERE status = 'claimed'"
            ).fetchone()[0],
            2,
        )
        rows = {
            row["id"]: row
            for row in connection.execute(
                "SELECT id, status, attempts, lease_owner FROM jobs WHERE id >= 10"
            ).fetchall()
        }
        for job_id in (10, 11, 12):
            self.assertEqual(rows[job_id]["status"], "pending")
            self.assertEqual(rows[job_id]["attempts"], 0)
            self.assertIsNone(rows[job_id]["lease_owner"])
        for job_id in (13, 14):
            self.assertEqual(rows[job_id]["status"], "claimed")
            self.assertEqual(rows[job_id]["attempts"], 1)
            self.assertEqual(rows[job_id]["lease_owner"], "atomic-scope-worker")
        connection.close()

    async def test_label_required_episode_without_context_job_blocks_completion(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY,
              lane TEXT NOT NULL,
              job_type TEXT NOT NULL,
              status TEXT NOT NULL,
              target_id TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            CREATE TABLE episode_context_runs (
              episode_id TEXT NOT NULL,
              label_pack TEXT NOT NULL,
              model TEXT NOT NULL,
              status TEXT NOT NULL,
              context_artifact_path TEXT
            );
            """
        )
        for segment_id, episode_id in (
            ("seg-v31-complete", "episode-v31-complete"),
            ("seg-v31-missing", "episode-v31-missing"),
            ("seg-v31-adopt", "episode-v31-adopt"),
            ("seg-v1-out-of-scope", "episode-v1-out-of-scope"),
        ):
            connection.execute("INSERT INTO segments VALUES (?, ?)", (segment_id, episode_id))
        v31_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        v31_default_model_payload = canonical({"label_pack": "ai_discourse_v3_1"})
        v31_other_model_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.6-sol"}
        )
        v1_payload = canonical({"label_pack": "ai_discourse_v1", "model": "gpt-5.5"})
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', 'pending', ?, ?)",
            ("seg-v31-complete", v31_default_model_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'label_segment', 'failed', ?, ?)",
            ("seg-v31-missing", v31_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (3, 'podcast', 'label_segment', 'pending', ?, ?)",
            ("seg-v1-out-of-scope", v1_payload),
        )
        connection.execute(
            "INSERT INTO segments VALUES ('seg-v31-other-model', 'episode-v31-other-model')"
        )
        connection.execute(
            "INSERT INTO jobs VALUES (5, 'podcast', 'label_segment', 'pending', ?, ?)",
            ("seg-v31-other-model", v31_other_model_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (4, 'podcast', 'episode_context', 'completed', ?, ?)",
            ("episode-v31-complete", v31_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (6, 'podcast', 'episode_context', 'failed', ?, ?)",
            ("episode-v31-missing", v31_default_model_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (7, 'podcast', 'label_segment', 'pending', ?, ?)",
            ("seg-v31-adopt", v31_default_model_payload),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (8, 'podcast', 'episode_context', 'completed', ?, ?)",
            ("episode-v31-adopt", v31_payload),
        )
        context_artifact = self.root / "existing-v31-context.json"
        context_artifact.write_text("{}", encoding="utf-8")
        connection.execute(
            "INSERT INTO episode_context_runs VALUES (?, ?, ?, 'completed', ?)",
            (
                "episode-v31-complete",
                "ai_discourse_v3_1",
                "gpt-5.5",
                str(context_artifact),
            ),
        )
        adopt_artifact = self.root / "adoptable-v31-context.json"
        adopt_persisted = {
            "context_summary": "Existing canonical context eligible for explicit adoption.",
            "speaker_map": [],
            "section_map": [],
            "entity_seed": {},
            "concept_seed": [],
        }
        adopt_artifact.write_text(
            pretty(
                {
                    "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
                    "episode_id": "episode-v31-adopt",
                    **adopt_persisted,
                    "episode_context": dict(adopt_persisted),
                    "extraction_guidance": (
                        "Preserve semantic uncertainty and exclude non-substantive source context carefully."
                    ),
                    "excluded_source_context": ["Sponsor read"],
                    "quality_flags": [],
                    "overall_confidence": 0.8,
                    "needs_review": False,
                    "review_reason": None,
                }
            ),
            encoding="utf-8",
        )
        connection.execute(
            "INSERT INTO episode_context_runs VALUES (?, ?, ?, 'completed', ?)",
            (
                "episode-v31-adopt",
                "ai_discourse_v3_1",
                "gpt-5.5",
                str(adopt_artifact),
            ),
        )
        connection.commit()
        sqlite_queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="fixture-scope-audit",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
        )
        self.bind_sqlite_queue(sqlite_queue, "failed-required-manifest")
        accounting = sqlite_queue.accounting()
        self.assertEqual(accounting["required_episode_count"], 3)
        self.assertEqual(accounting["completed_required_episode_count"], 0)
        self.assertEqual(accounting["missing_required_episode_count"], 3)
        self.assertEqual(accounting["remaining_episode_context_jobs"], 4)
        self.assertEqual(accounting["job_status_counts"]["failed"], 1)
        self.assertEqual(accounting["job_status_counts"]["completed"], 2)
        plan = sqlite_queue.provision_missing_context_jobs()
        self.assertEqual(
            plan["missing_episode_ids"],
            [
                "episode-v31-adopt",
                "episode-v31-complete",
                "episode-v31-missing",
            ],
        )
        self.assertEqual(plan["adoption_required_episode_ids"], [])
        self.assertEqual(
            plan["rerun_required_episode_ids"],
            ["episode-v31-adopt", "episode-v31-complete"],
        )
        self.assertEqual(plan["missing_context_job_episode_count"], 3)
        self.assertEqual(
            plan["failed_episode_ids_to_reset"], ["episode-v31-missing"]
        )
        self.assertEqual(plan["new_episode_ids_to_enqueue"], [])
        self.assertEqual(
            plan["completed_reconciliation_episode_ids"],
            ["episode-v31-adopt", "episode-v31-complete"],
        )
        self.assertFalse(plan["mutation_performed"])
        connection.close()

        run_root = self.root / "missing-label-required-context"
        fixture_queue = FakeQueue(
            self.items[:1], required_episode_ids=["episode-1", "episode-without-context-job"]
        )
        result = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=fixture_queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=FixtureFactory(),
            fixture_capacity_provider=FixtureCapacity(),
        )
        self.assertEqual(result["state"], "passed")
        self.assertEqual(
            result["terminal_reason"],
            "bounded_fixture_batch_completed_more_context_work_remains",
        )
        self.assertEqual(result["backlog"]["missing_required_episode_count"], 1)
        self.assertEqual(result["backlog"]["remaining_episode_context_jobs"], 1)
        self.assertFalse((run_root / "completion-receipt.json").exists())

    def test_required_scope_matches_live_null_default_model_population_shape(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY,
              lane TEXT NOT NULL,
              job_type TEXT NOT NULL,
              status TEXT NOT NULL,
              target_id TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            CREATE TABLE episode_context_runs (
              episode_id TEXT NOT NULL,
              label_pack TEXT NOT NULL,
              model TEXT NOT NULL,
              status TEXT NOT NULL,
              context_artifact_path TEXT
            );
            """
        )
        episode_count = 5_023
        job_count = 69_448
        null_model_count = 69_436
        default_payload = canonical({"label_pack": "ai_discourse_v3_1"})
        explicit_payload = canonical(
            {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}
        )
        segments = []
        jobs = []
        for index in range(job_count):
            segment_id = f"live-shape-segment-{index}"
            episode_id = f"live-shape-episode-{index % episode_count}"
            segments.append((segment_id, episode_id))
            jobs.append(
                (
                    index + 1,
                    "podcast",
                    "label_segment",
                    "pending",
                    segment_id,
                    default_payload if index < null_model_count else explicit_payload,
                )
            )
        connection.executemany("INSERT INTO segments VALUES (?, ?)", segments)
        connection.executemany("INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?)", jobs)
        connection.execute(
            "INSERT INTO segments VALUES ('different-model-segment', 'different-model-episode')"
        )
        connection.execute(
            "INSERT INTO jobs VALUES (?, 'podcast', 'label_segment', 'pending', ?, ?)",
            (
                job_count + 1,
                "different-model-segment",
                canonical({"label_pack": "ai_discourse_v3_1", "model": "gpt-5.6-sol"}),
            ),
        )
        failed_segments = []
        failed_jobs = []
        for index in range(24):
            failed_episode_index = index % 11
            episode_id = (
                f"live-shape-episode-{failed_episode_index}"
                if failed_episode_index < 2
                else f"failed-only-episode-{failed_episode_index}"
            )
            segment_id = f"failed-live-shape-segment-{index}"
            failed_segments.append((segment_id, episode_id))
            failed_jobs.append(
                (
                    job_count + 2 + index,
                    "podcast",
                    "label_segment",
                    "failed",
                    segment_id,
                    default_payload if index % 2 == 0 else explicit_payload,
                )
            )
        connection.executemany("INSERT INTO segments VALUES (?, ?)", failed_segments)
        connection.executemany("INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?)", failed_jobs)
        connection.execute(
            "INSERT INTO jobs VALUES (?, 'podcast', 'episode_context', 'failed', ?, ?)",
            (
                job_count + 26,
                "context-only-failed-episode",
                explicit_payload,
            ),
        )
        connection.execute(
            "INSERT INTO jobs VALUES (?, 'podcast', 'episode_context', 'pending', ?, ?)",
            (
                job_count + 27,
                "context-only-pending-episode",
                default_payload,
            ),
        )
        connection.commit()
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="live-shape-fixture",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
        )
        discovered = queue.discover_required_episode_ids()
        self.assertNotIn("context-only-failed-episode", discovered)
        self.assertNotIn("context-only-pending-episode", discovered)
        self.bind_sqlite_queue(queue, "live-shape-required-manifest")
        accounting = queue.accounting()
        required_episode_count = 5_032
        self.assertEqual(accounting["required_episode_count"], required_episode_count)
        self.assertEqual(accounting["completed_required_episode_count"], 0)
        self.assertEqual(
            accounting["missing_required_episode_count"], required_episode_count
        )
        self.assertEqual(
            accounting["remaining_episode_context_jobs"], required_episode_count
        )
        plan = queue.provision_missing_context_jobs()
        self.assertEqual(plan["required_episode_count"], required_episode_count)
        self.assertEqual(
            plan["missing_context_job_episode_count"], required_episode_count
        )
        connection.close()

    def test_failed_required_context_job_is_reset_once_and_completes_without_duplicate(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL);
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT NOT NULL, job_type TEXT NOT NULL,
              target_id TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL,
              priority INTEGER NOT NULL, attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL, completed_at TEXT, error TEXT
            );
            """
        )
        create_managed_context_tables(connection)
        episode_id = "failed-reset-episode"
        connection.execute("INSERT INTO segments VALUES ('failed-reset-segment', ?)", (episode_id,))
        connection.execute(
            "INSERT INTO jobs VALUES (1, 'podcast', 'label_segment', 'failed-reset-segment', ?, "
            "'pending', 100, 0, 3, NULL, NULL, 'label-dedupe', '', '', NULL, NULL)",
            (canonical({"label_pack": "ai_discourse_v3_1"}),),
        )
        stale_payload = {
            "label_pack": "ai_discourse_v3_1",
            "model": "gpt-5.5",
            "episode_context_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_context_run_id": "stale-run",
            "prompt_path": "/tmp/stale-prompt",
            "output_path": "/tmp/stale-output",
            "app_server_claim_id": "stale-claim",
            "app_server_attempt_id": "stale-attempt",
            "app_server_launch_path": "/tmp/stale-launch",
        }
        connection.execute(
            "INSERT INTO jobs VALUES (2, 'podcast', 'episode_context', ?, ?, 'failed', "
            "99, 2, 2, 'stale-worker', '2000-01-01', 'context-dedupe', '', '', '', 'failed')",
            (episode_id, canonical(stale_payload)),
        )
        connection.commit()
        queue = SQLiteEpisodeContextQueue(
            connection,
            lane="podcast",
            worker_id="failed-reset-worker",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            attempt_root=self.root / "failed-reset-attempts",
            source_loader=StubVerifiedSourceLoader(),
        )
        loaded = self.bind_sqlite_queue(queue, "failed-reset-required-manifest")

        plan = queue.provision_missing_context_jobs()
        self.assertEqual(plan["failed_episode_ids_to_reset"], [episode_id])
        self.assertFalse(plan["mutation_performed"])
        first = queue.provision_missing_context_jobs(execute=True)
        self.assertEqual(first["reset_failed_job_count"], 1)
        self.assertTrue(first["mutation_performed"])
        row = connection.execute(
            "SELECT * FROM jobs WHERE job_type = 'episode_context'"
        ).fetchone()
        self.assertEqual(row["id"], 2)
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["attempts"], 0)
        self.assertIsNone(row["lease_owner"])
        self.assertIsNone(row["leased_until"])
        self.assertIsNone(row["completed_at"])
        self.assertIsNone(row["error"])
        scrubbed = json.loads(row["payload_json"])
        for field in (
            "episode_context_run_id",
            "prompt_path",
            "output_path",
            "app_server_claim_id",
            "app_server_attempt_id",
            "app_server_launch_path",
        ):
            self.assertNotIn(field, scrubbed)
        second = queue.provision_missing_context_jobs(execute=True)
        self.assertEqual(second["reset_failed_job_count"], 0)
        self.assertEqual(second["enqueued_job_count"], 0)
        self.assertFalse(second["mutation_performed"])
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE job_type = 'episode_context' AND target_id = ?",
                (episode_id,),
            ).fetchone()[0],
            1,
        )

        persisted = {
            "context_summary": "Recovered canonical context.",
            "speaker_map": [{"name": "Host"}],
            "section_map": [{"title": "Discussion"}],
            "entity_seed": {"orgs": ["OpenAI"]},
            "concept_seed": ["evaluation"],
        }
        output = {
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "episode_id": episode_id,
            **persisted,
            "episode_context": dict(persisted),
            "extraction_guidance": "Use complete context and preserve uncertainty.",
            "excluded_source_context": ["Sponsor read"],
            "quality_flags": [],
            "overall_confidence": 0.9,
            "needs_review": False,
            "review_reason": None,
        }
        claimed = queue.claim(limit=1)
        self.assertEqual(len(claimed), 1)
        item = claimed[0]
        self.assertEqual(item.job_id, 2)
        self.assertEqual(item.episode_id, episode_id)
        self.write_managed_sqlite_turn(
            queue=queue,
            item=item,
            loaded=loaded,
            name="failed-reset-completion",
            output=output,
        )
        accounting = queue.accounting()
        self.assertEqual(accounting["remaining_episode_context_jobs"], 0)
        self.assertEqual(accounting["job_status_counts"]["pending"], 0)
        self.assertEqual(accounting["job_status_counts"]["failed"], 0)
        self.assertEqual(accounting["job_status_counts"]["completed"], 1)
        connection.close()

    def test_attempt_schema_cutover_and_exact_provision_are_authorized_idempotent(self):
        database_path, connection, word_counts, legacy_records = (
            self.build_live_plan_database("live-cutover")
        )
        connection.close()
        planning_root = self.root / "live-cutover-planning"
        migration_path = planning_root / "attempt-schema-migration-plan.json"
        provision_path = planning_root / "live-context-provision-plan.json"
        cutover_path = planning_root / "attempt-schema-cutover-before.json"
        planning_root.mkdir()
        constants = {
            "LIVE_REQUIRED_EPISODE_COUNT": 3,
            "LIVE_REQUIRED_SEGMENT_COUNT": 3,
            "LIVE_REQUIRED_SOURCE_WORD_COUNT": sum(word_counts.values()),
            "LIVE_MAX_EPISODE_SOURCE_WORD_COUNT": max(word_counts.values()),
            "LIVE_LEGACY_COMPLETED_RERUN_COUNT": 1,
            "LIVE_FAILED_CONTEXT_RESET_COUNT": 1,
            "LIVE_NEW_CONTEXT_JOB_COUNT": 1,
        }
        with patch.multiple(context_runner, **constants):
            migration = context_runner.build_attempt_schema_migration_plan(
                database_path
            )
            migration_path.write_text(pretty(migration), encoding="utf-8")
            self.assertEqual(
                migration["state"],
                "ready_for_separately_authorized_empty_table_cutover",
            )
            provision = context_runner.build_live_context_provision_plan(
                database_path
            )
            provision_path.write_text(pretty(provision), encoding="utf-8")
            self.assertEqual(provision["state"], "ready_read_only")
            cutover = context_runner.build_attempt_schema_cutover_receipt(
                database_path, migration_plan_path=migration_path
            )
            cutover_path.write_text(pretty(cutover), encoding="utf-8")
            self.assertEqual(cutover["state"], "existing_incomplete_schema")
            authorization_path = planning_root / "provision-authorization.json"
            authorization_result = (
                context_runner.build_live_context_provision_authorization_artifact(
                    contract_path=self.contract,
                    migration_plan_path=migration_path,
                    cutover_receipt_path=cutover_path,
                    provision_plan_path=provision_path,
                    output_path=authorization_path,
                    operator_authorization_id="operator_fixture_provision_001",
                    authorized_by="kolby",
                )
            )
            self.assertFalse(authorization_result["production_mutated"])
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    context_runner.main(
                        [
                            "authorize-provision",
                            "--contract",
                            str(self.contract),
                            "--migration-plan",
                            str(migration_path),
                            "--cutover-receipt",
                            str(cutover_path),
                            "--provision-plan",
                            str(provision_path),
                            "--output",
                            str(authorization_path),
                            "--operator-authorization-id",
                            "operator_fixture_provision_001",
                            "--authorized-by",
                            "kolby",
                        ]
                    ),
                    0,
                )
            self.assertEqual(
                context_runner.load_live_context_provision_authorization(
                    authorization_path,
                    contract_path=self.contract,
                    migration_plan_path=migration_path,
                    cutover_receipt_path=cutover_path,
                    provision_plan_path=provision_path,
                )["authorization"]["operator_authorization_id"],
                "operator_fixture_provision_001",
            )
            execution_root = self.root / "live-cutover-execution"
            receipt = context_runner.execute_live_context_cutover_and_provision(
                contract_path=self.contract,
                database_path=database_path,
                migration_plan_path=migration_path,
                cutover_receipt_path=cutover_path,
                provision_plan_path=provision_path,
                authorization_path=authorization_path,
                output_root=execution_root,
                execute=True,
            )
            self.assertEqual(receipt["state"], "passed")
            self.assertEqual(
                receipt["operation_counts"],
                {
                    "legacy_snapshot_attempt_count": 1,
                    "reset_completed_legacy_job_count": 1,
                    "reset_failed_job_count": 1,
                    "enqueued_job_count": 1,
                },
            )
            receipt_bytes = (
                execution_root / "live-provision-execution-receipt.json"
            ).read_bytes()
            repeated = context_runner.execute_live_context_cutover_and_provision(
                contract_path=self.contract,
                database_path=database_path,
                migration_plan_path=migration_path,
                cutover_receipt_path=cutover_path,
                provision_plan_path=provision_path,
                authorization_path=authorization_path,
                output_root=execution_root,
                execute=True,
            )
            self.assertEqual(repeated, receipt)
            self.assertEqual(
                (
                    execution_root / "live-provision-execution-receipt.json"
                ).read_bytes(),
                receipt_bytes,
            )
            capacity_policy_path = planning_root / "capacity-policy.json"
            capacity_policy_path.write_text(
                pretty(
                    {
                        "schema_version": context_runner.LIVE_CAPACITY_POLICY_VERSION,
                        "minimum_remaining_reserve_percent": 20,
                        "quota_points_per_million_tokens": 17,
                        "maximum_total_tokens_per_episode": 1000,
                        "population_episode_count": 3,
                        "population_source_segment_count": 3,
                        "population_source_word_count": sum(word_counts.values()),
                        "max_episode_source_word_count": max(word_counts.values()),
                        "population_total_token_bound": 3000,
                        "admission_scope": (
                            "one_fresh_no_thread_probe_immediately_before_each_episode_claim_thread_and_turn"
                        ),
                        "multi_window_execution_required": True,
                        "managed_chatgpt_auth_only": True,
                        "official_persistent_codex_app_server_only": True,
                        "rate_limit_reached_type_must_be_null": True,
                        "unknown_usage_hard_stop": True,
                        "retry_count_per_episode": 0,
                    }
                ),
                encoding="utf-8",
            )
            overlay_path = planning_root / "context-overlay.json"
            overlay_path.write_text(
                pretty(context_runner.live_context_control_overlay()),
                encoding="utf-8",
            )
            instruction_path = planning_root / "instruction-sources.json"
            instruction_path.write_text(
                pretty(context_runner.live_instruction_source_contract()),
                encoding="utf-8",
            )
            loaded = load_episode_context_contract(self.contract)
            runtime_path = planning_root / "runtime-authorization.json"
            runtime_result = context_runner.build_live_episode_context_runtime_authorization_artifact(
                contract_path=self.contract,
                cutover_receipt_path=context_runner._verify_record(
                    receipt["post_cutover_receipt"], label="fixture post cutover"
                ),
                provision_plan_path=provision_path,
                provision_execution_receipt_path=(
                    execution_root / "live-provision-execution-receipt.json"
                ),
                capacity_policy_path=capacity_policy_path,
                context_control_overlay_path=overlay_path,
                instruction_source_contract_path=instruction_path,
                output_path=runtime_path,
                operator_authorization_id="operator_fixture_runtime_001",
                authorized_by="kolby",
            )
            self.assertFalse(runtime_result["production_mutated"])
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    context_runner.main(
                        [
                            "authorize-runtime",
                            "--contract",
                            str(self.contract),
                            "--cutover-receipt",
                            str(
                                context_runner._verify_record(
                                    receipt["post_cutover_receipt"],
                                    label="fixture post cutover CLI",
                                )
                            ),
                            "--provision-plan",
                            str(provision_path),
                            "--provision-execution",
                            str(
                                execution_root
                                / "live-provision-execution-receipt.json"
                            ),
                            "--capacity-policy",
                            str(capacity_policy_path),
                            "--context-control-overlay",
                            str(overlay_path),
                            "--instruction-source-contract",
                            str(instruction_path),
                            "--output",
                            str(runtime_path),
                            "--operator-authorization-id",
                            "operator_fixture_runtime_001",
                            "--authorized-by",
                            "kolby",
                        ]
                    ),
                    0,
                )
            runtime = context_runner.load_live_episode_context_runtime_authorization(
                runtime_path, loaded=loaded
            )
            self.assertEqual(runtime["cutover"]["database"], provision["database"])
            self.assertEqual(
                runtime["provision_execution"]["database"], provision["database"]
            )
            runtime_artifacts = runtime["authorization"]["runtime_artifacts"]
            self.assertEqual(
                runtime_artifacts["managed_client_implementation"][
                    "qualified_name"
                ],
                (
                    "research_factory.app_server_episode_context_runner."
                    "ManagedEpisodeContextCodexAppServerClient"
                ),
            )
            self.assertEqual(
                runtime_artifacts["capacity_provider_implementation"][
                    "qualified_name"
                ],
                (
                    "research_factory.app_server_episode_context_runner."
                    "ManagedEpisodeContextReserveCapacityProvider"
                ),
            )
            tampered_runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            tampered_runtime["runtime_artifacts"][
                "capacity_provider_implementation"
            ]["implementation_sha256"] = "0" * 64
            tampered_runtime_path = planning_root / "tampered-runtime.json"
            tampered_runtime_path.write_text(
                pretty(tampered_runtime), encoding="utf-8"
            )
            with self.assertRaises(context_runner.EpisodeContextRunnerError):
                context_runner.load_live_episode_context_runtime_authorization(
                    tampered_runtime_path, loaded=loaded
                )
            original_prompt = self.prompt.read_bytes()
            self.prompt.write_bytes(original_prompt + b"\ndrift")
            with self.assertRaises(context_runner.EpisodeContextRunnerError):
                context_runner._verify_live_runtime_files_now(
                    loaded=loaded, runtime=runtime
                )
            self.prompt.write_bytes(original_prompt)
            context_runner._verify_live_runtime_files_now(
                loaded=loaded, runtime=runtime
            )
        audit = sqlite3.connect(
            f"file:{database_path}?mode=ro", uri=True
        )
        audit.row_factory = sqlite3.Row
        try:
            self.assertEqual(
                audit.execute(
                    "SELECT COUNT(*) FROM jobs WHERE job_type='episode_context' "
                    "AND status='pending'"
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                audit.execute(
                    "SELECT COUNT(*) FROM episode_context_run_attempts "
                    "WHERE attempt_kind='legacy_canonical_snapshot'"
                ).fetchone()[0],
                1,
            )
        finally:
            audit.close()
        for current in legacy_records.values():
            self.assertEqual(record(Path(current["path"])), current)

    def test_control_artifact_builders_require_explicit_kolby_authority(self):
        target = self.root / "must-not-exist-authorization.json"
        for authorization_id, authorized_by in (
            ("short", "kolby"),
            ("operator_valid_identifier_001", "not-kolby"),
        ):
            with self.assertRaisesRegex(
                context_runner.EpisodeContextRunnerError,
                "explicit Kolby operator authorization",
            ):
                context_runner.build_live_context_provision_authorization_artifact(
                    contract_path=self.contract,
                    migration_plan_path=self.root / "absent-migration.json",
                    cutover_receipt_path=self.root / "absent-cutover.json",
                    provision_plan_path=self.root / "absent-provision.json",
                    output_path=target,
                    operator_authorization_id=authorization_id,
                    authorized_by=authorized_by,
                )
        self.assertFalse(target.exists())
        with self.assertRaises(SystemExit):
            context_runner._build_cli_parser().parse_args(
                [
                    "authorize-provision",
                    "--contract",
                    str(self.contract),
                    "--migration-plan",
                    "missing",
                    "--cutover-receipt",
                    "missing",
                    "--provision-plan",
                    "missing",
                    "--output",
                    str(target),
                    "--authorized-by",
                    "kolby",
                ]
            )

    async def test_second_live_writer_fails_before_capacity_probe_or_claim(self):
        database_path = self.root / "writer-lock.sqlite"
        database_path.write_bytes(b"sqlite-fixture")
        identity = context_runner._database_identity(database_path)
        queue = ForbiddenQueue()
        runtime = {
            "cutover": {"database": identity},
            "provision_plan": {"database": identity},
            "provision_execution": {"database": identity},
        }
        with (
            context_runner.LiveContextWriterLock(identity, phase="run"),
            patch.object(
                context_runner,
                "load_episode_context_contract",
                return_value={"path": self.contract},
            ),
            patch.object(
                context_runner,
                "load_live_episode_context_runtime_authorization",
                return_value=runtime,
            ),
            patch.object(
                context_runner,
                "_queue_main_database_identity",
                return_value=identity,
            ),
            patch.object(
                context_runner,
                "_verify_live_runtime_files_now",
                return_value={},
            ),
            patch.dict(os.environ, {}, clear=True),
        ):
            with self.assertRaises(context_runner.EpisodeContextRunnerWaiting):
                await context_runner.run_live_episode_context_queue(
                    contract_path=self.contract,
                    runtime_authorization_path=self.root / "unused-runtime.json",
                    output_root=self.root / "locked-live-root",
                    queue=queue,
                    limit=1,
                    execute=True,
                )
        self.assertEqual(queue.calls, [])

    def test_live_writer_lock_rejects_symlink_without_truncating_target(self):
        database_path = self.root / "writer-lock-symlink.sqlite"
        database_path.write_bytes(b"sqlite-fixture")
        identity = context_runner._database_identity(database_path)
        target = self.root / "writer-lock-target.txt"
        target.write_text("must remain unchanged", encoding="utf-8")
        lock_path = context_runner._live_writer_lock_path(identity)
        lock_path.symlink_to(target)
        with self.assertRaisesRegex(
            context_runner.EpisodeContextRunnerError,
            "not a real file",
        ):
            with context_runner.LiveContextWriterLock(identity, phase="run"):
                self.fail("symlink lock must never be acquired")
        self.assertEqual(target.read_text(encoding="utf-8"), "must remain unchanged")

    async def test_injected_self_attested_dependencies_are_rejected_in_live_mode(self):
        database_path = self.root / "injected-live.sqlite"
        database_path.write_bytes(b"sqlite-fixture")
        identity = context_runner._database_identity(database_path)
        runtime_path = self.root / "injected-runtime.json"
        runtime_path.write_text("{}\n", encoding="utf-8")
        queue = ForbiddenQueue()
        client_calls = []
        provider_calls = []

        def injected_client():
            client_calls.append("client")
            raise AssertionError("injected client must not enter live mode")

        def injected_provider(*args, **kwargs):
            provider_calls.append("provider")
            raise AssertionError("injected provider must not enter live mode")

        loaded = {"path": self.contract, "sha256": sha_bytes(self.contract.read_bytes())}
        runtime = {
            "path": runtime_path,
            "cutover": {"database": identity},
            "provision_plan": {"database": identity},
            "provision_execution": {"database": identity},
        }
        output_root = self.root / "injected-live-root"
        with (
            patch.object(
                context_runner,
                "load_episode_context_contract",
                return_value=loaded,
            ),
            patch.object(
                context_runner,
                "load_live_episode_context_runtime_authorization",
                return_value=runtime,
            ),
            patch.object(
                context_runner,
                "_verify_live_runtime_files_now",
                return_value={},
            ),
            patch.dict(os.environ, {}, clear=True),
        ):
            with self.assertRaisesRegex(
                context_runner.EpisodeContextRunnerError,
                "rejects injected client or capacity provider",
            ):
                await context_runner.run_live_episode_context_queue(
                    contract_path=self.contract,
                    runtime_authorization_path=runtime_path,
                    output_root=output_root,
                    queue=queue,
                    execute=True,
                    client_factory=injected_client,
                    capacity_provider_factory=injected_provider,
                )
        self.assertEqual(queue.calls, [])
        self.assertEqual(client_calls, [])
        self.assertEqual(provider_calls, [])
        self.assertFalse((output_root / "live-completion-receipt.json").exists())

    async def test_offline_injected_path_is_test_only_and_nonpromotable(self):
        database_path = self.root / "offline-injected.sqlite"
        database_path.write_bytes(b"sqlite-fixture")
        identity = context_runner._database_identity(database_path)
        frozen_database_path = self.root / "frozen-live.sqlite"
        frozen_database_path.write_bytes(b"frozen-live")
        frozen_identity = context_runner._database_identity(frozen_database_path)
        runtime_path = self.root / "offline-runtime.json"
        runtime_path.write_text("{}\n", encoding="utf-8")
        loaded = {"path": self.contract, "sha256": sha_bytes(self.contract.read_bytes())}
        runtime = {
            "path": runtime_path,
            "cutover": {"database": frozen_identity},
            "provision_plan": {"database": frozen_identity},
            "provision_execution": {"database": frozen_identity},
        }
        output_root = self.root / "offline-injected-root"
        with (
            patch.object(
                context_runner,
                "load_episode_context_contract",
                return_value=loaded,
            ),
            patch.object(
                context_runner,
                "load_live_episode_context_runtime_authorization",
                return_value=runtime,
            ),
            patch.object(
                context_runner,
                "_verify_live_runtime_files_now",
                return_value={},
            ),
            patch.object(
                context_runner,
                "_queue_main_database_identity",
                return_value=identity,
            ),
            patch.dict(os.environ, {}, clear=True),
        ):
            result = await context_runner.run_live_episode_context_queue(
                contract_path=self.contract,
                runtime_authorization_path=runtime_path,
                output_root=output_root,
                queue=FakeQueue([], required_episode_ids=[]),
                execute=False,
                offline_test_mode=True,
                client_factory=lambda: None,
                capacity_provider_factory=lambda *args, **kwargs: None,
            )
        self.assertTrue(result["test_only"])
        self.assertFalse(result["promotable"])
        self.assertFalse(result["live_completion_evidence_allowed"])
        self.assertEqual(result["execution_mode"], "offline_test_only")
        self.assertFalse((output_root / "live-completion-receipt.json").exists())

    async def test_capacity_probe_crash_publishes_no_request_only_artifact(self):
        connection, queue, loaded = self.managed_context_queue(
            name="preclaim-probe-crash",
            episode_id="episode-probe-crash",
            file_backed=True,
        )
        runtime, _provider, _client = self.live_capacity_fixture(
            name="preclaim-probe-crash", loaded=loaded, queue=queue
        )
        root = self.root / "preclaim-probe-crash-root"
        preview = queue.preview(limit=1)[0]
        request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=preview,
            run_id="ectxlive_probe_crash",
            ordinal=1,
            stage="preclaim",
        )

        class CrashingProvider:
            async def __call__(self, *, request):
                raise RuntimeError("injected rate-limit RPC crash")

        with self.assertRaisesRegex(RuntimeError, "injected rate-limit RPC crash"):
            await self.publish_capacity(
                root=root,
                loaded=loaded,
                runtime=runtime,
                request=request,
                provider=CrashingProvider(),
            )
        self.assertEqual(list((root / "capacity").glob("**/*")), [])
        reconciliation = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        self.assertEqual(sum(reconciliation.values()), 0)
        self.assertEqual(queue.accounting()["job_status_counts"]["pending"], 1)
        connection.close()

    async def test_request_only_staging_is_discarded_and_root_remains_resumable(self):
        connection, queue, loaded = self.managed_context_queue(
            name="request-only-staging",
            episode_id="episode-request-only-staging",
            file_backed=True,
        )
        runtime, provider, _client = self.live_capacity_fixture(
            name="request-only-staging", loaded=loaded, queue=queue
        )
        root = self.root / "request-only-staging-root"
        preview = queue.preview(limit=1)[0]
        request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=preview,
            run_id="ectxlive_request_only_preclaim",
            ordinal=1,
            stage="preclaim",
        )
        staging = root / ".capacity-staging" / "preclaim-crash.fixture"
        staging.mkdir(parents=True)
        (staging / "request.json").write_text(pretty(request), encoding="utf-8")
        first = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        self.assertEqual(first["discarded_unpublished_staging_count"], 1)
        self.assertEqual(list((root / ".capacity-staging").iterdir()), [])

        preclaim_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=preview,
            run_id="ectxlive_request_only_preturn",
            ordinal=1,
            stage="preclaim",
        )
        _admission, preclaim = await self.publish_capacity(
            root=root,
            loaded=loaded,
            runtime=runtime,
            request=preclaim_request,
            provider=provider,
        )
        item = queue.claim(limit=1)[0]
        queue.bind_prelaunch_capacity(item, {"preclaim": preclaim})
        preturn_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=item,
            run_id="ectxlive_request_only_preturn",
            ordinal=1,
            stage="preturn",
        )
        staging = root / ".capacity-staging" / "preturn-crash.fixture"
        staging.mkdir()
        (staging / "request.json").write_text(
            pretty(preturn_request), encoding="utf-8"
        )
        second = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        third = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        attempt = connection.execute(
            "SELECT status, preturn_capacity_request_path FROM "
            "episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        self.assertEqual(second["discarded_unpublished_staging_count"], 1)
        self.assertEqual(second["released_zero_dispatch_attempt_count"], 1)
        self.assertEqual(sum(third.values()), 0)
        self.assertEqual(attempt["status"], "released_prelaunch")
        self.assertIsNone(attempt["preturn_capacity_request_path"])
        self.assertFalse((root / "turns").exists())
        connection.close()

    async def test_orphan_preclaim_bundles_are_abandoned_once_without_dispatch(self):
        connection, queue, loaded = self.managed_context_queue(
            name="orphan-preclaim",
            episode_id="episode-orphan-preclaim",
            file_backed=True,
        )
        runtime, provider, client = self.live_capacity_fixture(
            name="orphan-preclaim", loaded=loaded, queue=queue
        )
        root = self.root / "orphan-preclaim-root"
        preview = queue.preview(limit=1)[0]
        for index in (1, 2):
            request = self.live_capacity_request(
                loaded=loaded,
                runtime=runtime,
                queue=queue,
                item=preview,
                run_id=f"ectxlive_orphan_preclaim_{index}",
                ordinal=1,
                stage="preclaim",
            )
            await self.publish_capacity(
                root=root,
                loaded=loaded,
                runtime=runtime,
                request=request,
                provider=provider,
            )
        first = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        manifest_record = dict(queue._required_manifest_record)
        connection.close()
        connection, queue = self.reopen_managed_context_queue(
            name="orphan-preclaim",
            loaded=loaded,
            manifest_record=manifest_record,
        )
        second = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        evidence = context_runner._collect_live_capacity_artifacts(
            root=root, loaded=loaded, runtime=runtime
        )
        self.assertEqual(client.requests, 2)
        self.assertEqual(first["abandoned_capacity_bundle_count"], 2)
        self.assertEqual(sum(second.values()), 0)
        self.assertEqual(evidence["counts"]["admitted"], 2)
        self.assertEqual(evidence["counts"]["abandoned_no_dispatch"], 2)
        self.assertTrue(
            all(item["abandonment"] for item in evidence["by_paths"].values())
        )
        self.assertFalse((root / "turns").exists())
        self.assertEqual(queue.accounting()["job_status_counts"]["pending"], 1)
        connection.close()

    async def test_orphan_preturn_bundle_is_adopted_and_released_without_replay(self):
        connection, queue, loaded = self.managed_context_queue(
            name="orphan-preturn",
            episode_id="episode-orphan-preturn",
            file_backed=True,
        )
        runtime, provider, client = self.live_capacity_fixture(
            name="orphan-preturn", loaded=loaded, queue=queue
        )
        root = self.root / "orphan-preturn-root"
        preview = queue.preview(limit=1)[0]
        preclaim_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=preview,
            run_id="ectxlive_orphan_preturn",
            ordinal=1,
            stage="preclaim",
        )
        _admission, preclaim = await self.publish_capacity(
            root=root,
            loaded=loaded,
            runtime=runtime,
            request=preclaim_request,
            provider=provider,
        )
        item = queue.claim(limit=1)[0]
        queue.bind_prelaunch_capacity(item, {"preclaim": preclaim})
        preturn_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=item,
            run_id="ectxlive_orphan_preturn",
            ordinal=1,
            stage="preturn",
        )
        await self.publish_capacity(
            root=root,
            loaded=loaded,
            runtime=runtime,
            request=preturn_request,
            provider=provider,
        )

        first = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        manifest_record = dict(queue._required_manifest_record)
        connection.close()
        connection, queue = self.reopen_managed_context_queue(
            name="orphan-preturn",
            loaded=loaded,
            manifest_record=manifest_record,
        )
        second = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        attempt = connection.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        evidence = context_runner._collect_live_capacity_artifacts(
            root=root, loaded=loaded, runtime=runtime
        )
        self.assertEqual(client.requests, 2)
        self.assertEqual(first["adopted_capacity_bundle_count"], 1)
        self.assertEqual(first["released_zero_dispatch_attempt_count"], 1)
        self.assertEqual(sum(second.values()), 0)
        self.assertEqual(attempt["status"], "released_prelaunch")
        self.assertTrue(attempt["preturn_capacity_request_path"])
        self.assertEqual(evidence["counts"]["admitted"], 2)
        self.assertEqual(evidence["counts"]["abandoned_no_dispatch"], 0)
        self.assertFalse((root / "turns").exists())
        self.assertEqual(queue.accounting()["job_status_counts"]["pending"], 1)
        connection.close()

    async def test_preturn_probe_crash_releases_owned_preclaim_without_pair(self):
        connection, queue, loaded = self.managed_context_queue(
            name="preturn-probe-crash",
            episode_id="episode-preturn-probe-crash",
            file_backed=True,
        )
        runtime, provider, client = self.live_capacity_fixture(
            name="preturn-probe-crash", loaded=loaded, queue=queue
        )
        root = self.root / "preturn-probe-crash-root"
        preview = queue.preview(limit=1)[0]
        preclaim_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=preview,
            run_id="ectxlive_preturn_probe_crash",
            ordinal=1,
            stage="preclaim",
        )
        _admission, preclaim = await self.publish_capacity(
            root=root,
            loaded=loaded,
            runtime=runtime,
            request=preclaim_request,
            provider=provider,
        )
        item = queue.claim(limit=1)[0]
        queue.bind_prelaunch_capacity(item, {"preclaim": preclaim})
        preturn_request = self.live_capacity_request(
            loaded=loaded,
            runtime=runtime,
            queue=queue,
            item=item,
            run_id="ectxlive_preturn_probe_crash",
            ordinal=1,
            stage="preturn",
        )

        class CrashingProvider:
            async def __call__(self, *, request):
                raise RuntimeError("injected preturn RPC crash")

        with self.assertRaisesRegex(RuntimeError, "injected preturn RPC crash"):
            await self.publish_capacity(
                root=root,
                loaded=loaded,
                runtime=runtime,
                request=preturn_request,
                provider=CrashingProvider(),
            )
        first = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        manifest_record = dict(queue._required_manifest_record)
        connection.close()
        connection, queue = self.reopen_managed_context_queue(
            name="preturn-probe-crash",
            loaded=loaded,
            manifest_record=manifest_record,
        )
        second = context_runner._reconcile_live_capacity_ownership(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        evidence = context_runner._collect_live_capacity_artifacts(
            root=root, loaded=loaded, runtime=runtime
        )
        attempt = connection.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        self.assertEqual(client.requests, 1)
        self.assertEqual(first["released_zero_dispatch_attempt_count"], 1)
        self.assertEqual(sum(second.values()), 0)
        self.assertEqual(len(evidence["records"]), 1)
        self.assertEqual(attempt["status"], "released_prelaunch")
        self.assertIsNone(attempt["preturn_capacity_request_path"])
        self.assertFalse((root / "turns").exists())
        connection.close()

    async def test_capacity_provider_uses_minimum_of_all_applicable_windows(self):
        policy = {
            "minimum_remaining_reserve_percent": 20,
            "quota_points_per_million_tokens": 10_000,
            "maximum_total_tokens_per_episode": 100,
            "population_total_token_bound": 1_000,
        }
        policy_record = {"path": "fixture", "sha256": "1" * 64, "size_bytes": 1}

        class MultiWindowClient:
            account_summary = {
                "type": "chatgpt",
                "plan_type": "pro",
                "requires_openai_auth": False,
            }

            async def _request(self, method, params):
                codex = {
                    "limitId": "codex",
                    "planType": "pro",
                    "primary": {
                        "usedPercent": 10,
                        "resetsAt": 1_800_000_000,
                        "windowDurationMins": 300,
                    },
                    "secondary": {
                        "usedPercent": 65,
                        "resetsAt": 1_800_000_100,
                        "windowDurationMins": 10_080,
                    },
                    "individualLimit": {
                        "limit": "100",
                        "used": "70",
                        "remainingPercent": 30,
                        "resetsAt": 1_800_000_200,
                    },
                    "credits": {
                        "balance": "4.00",
                        "hasCredits": True,
                        "unlimited": False,
                    },
                    "rateLimitReachedType": None,
                }
                auxiliary = {
                    "limitId": "workspace-secondary",
                    "planType": "pro",
                    "primary": {
                        "usedPercent": 60,
                        "resetsAt": 1_800_000_300,
                        "windowDurationMins": 1_440,
                    },
                    "rateLimitReachedType": None,
                }
                return {
                    "rateLimits": codex,
                    "rateLimitsByLimitId": {
                        "codex": codex,
                        "workspace-secondary": auxiliary,
                    },
                    "rateLimitResetCredits": {
                        "availableCount": 1,
                        "credits": [
                            {
                                "grantedAt": 1_700_000_000,
                                "id": "credit-1",
                                "resetType": "codexRateLimits",
                                "status": "available",
                            }
                        ],
                    },
                }

        provider = context_runner.ManagedEpisodeContextReserveCapacityProvider(
            client=MultiWindowClient(), policy=policy, policy_record=policy_record
        )
        request = {
            "request_sha256": "2" * 64,
            "admission_stage": "preclaim",
            "claim_started": False,
            "cumulative_measured_total_tokens": 0,
        }
        admission = await provider(request=request)
        self.assertEqual(admission["state"], "admitted")
        self.assertEqual(admission["minimum_applicable_remaining_percent"], 30)
        self.assertEqual(admission["policy_projected_episode_quota_points"], 1)
        self.assertEqual(admission["policy_projected_terminal_remaining_percent"], 29)
        self.assertFalse(admission["reset_credits_counted_as_capacity"])
        self.assertEqual(
            [item["limit_id"] for item in admission["observed_limit_snapshots"]],
            ["codex", "workspace-secondary"],
        )

    async def test_capacity_provider_protocol_failure_is_sticky(self):
        class MalformedClient:
            account_summary = {
                "type": "chatgpt",
                "plan_type": "pro",
                "requires_openai_auth": False,
            }

            def __init__(self):
                self.requests = 0

            async def _request(self, method, params):
                self.requests += 1
                return {"rateLimits": {"unexpected": True}}

        client = MalformedClient()
        provider = context_runner.ManagedEpisodeContextReserveCapacityProvider(
            client=client,
            policy={
                "minimum_remaining_reserve_percent": 20,
                "quota_points_per_million_tokens": 10_000,
                "maximum_total_tokens_per_episode": 100,
                "population_total_token_bound": 1_000,
            },
            policy_record={"path": "fixture", "sha256": "1" * 64, "size_bytes": 1},
        )
        request = {
            "request_sha256": "2" * 64,
            "admission_stage": "preclaim",
            "claim_started": False,
            "cumulative_measured_total_tokens": 0,
        }
        with self.assertRaises(context_runner.EpisodeContextRunnerWaiting):
            await provider(request=request)
        with self.assertRaisesRegex(
            context_runner.EpisodeContextRunnerWaiting,
            "previously stopped",
        ):
            await provider(request=request)
        self.assertEqual(client.requests, 1)

    def test_claimed_source_snapshot_drift_fails_before_any_turn_artifact(self):
        connection, queue, _loaded = self.managed_context_queue(
            name="claimed-source-drift", episode_id="episode-claimed-source-drift"
        )

        class MutableLoader:
            def __init__(self):
                self.revision = 1

            def episode_packet(self, episode_id):
                return {
                    "episode": {"episode_id": episode_id},
                    "full_segmented_episode_text": f"revision-{self.revision}",
                    "source_integrity": {
                        "schema_version": SOURCE_INTEGRITY_VERSION,
                        "revision": self.revision,
                    },
                }

            def fresh_episode_packet(self, episode_id):
                return self.episode_packet(episode_id)

        loader = MutableLoader()
        queue._source_loader = loader
        item = queue.claim(limit=1)[0]
        loader.revision = 2
        with self.assertRaisesRegex(
            context_runner.EpisodeContextRunnerWaiting,
            "source packet drifted",
        ):
            queue.reverify_source(item)
        paths = context_runner._turn_paths(
            self.root / "claimed-source-drift-root", item
        )
        self.assertTrue(all(not path.exists() for path in paths.values()))
        attempt = connection.execute(
            "SELECT status, launch_path, sidecar_path, terminal_path FROM "
            "episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["status"], "claimed")
        self.assertIsNone(attempt["launch_path"])
        self.assertIsNone(attempt["sidecar_path"])
        self.assertIsNone(attempt["terminal_path"])
        connection.close()

    def test_capacity_denial_writes_only_nonterminal_checkpoint(self):
        request_path = self.root / "denied.request.json"
        admission_path = self.root / "denied.admission.json"
        runtime_path = self.root / "capacity-runtime.json"
        request_path.write_text("{}\n", encoding="utf-8")
        admission_path.write_text("{}\n", encoding="utf-8")
        runtime_path.write_text("{}\n", encoding="utf-8")
        loaded = {"path": self.contract}
        runtime = {
            "path": runtime_path,
            "capacity_policy": {},
            "authorization": {
                "provision_plan": {},
                "capacity_policy": {},
            },
        }
        denial = context_runner.LiveCapacityAdmissionDenied(
            "denied",
            binding={
                "request": record(request_path),
                "admission": record(admission_path),
            },
        )
        output_root = self.root / "capacity-wait"
        with (
            patch.object(
                context_runner,
                "_verify_live_runtime_files_now",
                return_value={},
            ),
            patch.object(
                context_runner,
                "_validate_live_capacity_request",
                return_value={"request_sha256": "1" * 64},
            ),
            patch.object(
                context_runner,
                "_validate_live_capacity_admission",
                return_value={"state": "denied"},
            ),
        ):
            result = context_runner._live_capacity_wait_checkpoint(
                output_root,
                run_id="capacity-wait-run",
                loaded=loaded,
                runtime=runtime,
                backlog={"remaining_episode_context_jobs": 1},
                denial=denial,
                admitted_capacity_count=0,
                live_model_call_count=0,
                live_database_mutation_count=0,
                recovered_count=0,
                usage={},
                wall_elapsed_seconds=0,
                offline_test_mode=False,
            )
        self.assertTrue(result["nonterminal_capacity_wait"])
        self.assertEqual(result["capacity_denial_count"], 1)
        checkpoints = list(
            (output_root / "capacity-wait-checkpoints").glob("*.json")
        )
        self.assertEqual(len(checkpoints), 1)
        checkpoint = json.loads(checkpoints[0].read_text(encoding="utf-8"))
        self.assertEqual(checkpoints[0].name, "capacity-wait-run.json")
        self.assertEqual(checkpoint, result)
        self.assertEqual(checkpoint["run_id"], "capacity-wait-run")
        self.assertEqual(
            checkpoint["capacity_denial"],
            {
                "request": record(request_path),
                "admission": record(admission_path),
            },
        )
        self.assertEqual(
            sha_bytes(checkpoints[0].read_bytes()),
            sha_bytes(pretty(result).encode()),
        )
        self.assertFalse((output_root / "capacity-wait-checkpoint.json").exists())
        self.assertFalse((output_root / "run-receipts").exists())

    def test_cli_maps_nonterminal_capacity_wait_to_exit_75(self):
        database_path = self.root / "cli-capacity.sqlite"
        database_path.write_bytes(b"fixture")
        runtime_path = self.root / "cli-runtime.json"
        runtime_path.write_text("{}\n", encoding="utf-8")

        class Connection:
            def close(self):
                return None

        async def denied_run(**kwargs):
            return {
                "state": "waiting",
                "nonterminal_capacity_wait": True,
            }

        with (
            patch.object(
                context_runner,
                "_open_authorized_live_connection",
                return_value=Connection(),
            ),
            patch.object(
                context_runner,
                "SQLiteEpisodeContextQueue",
                return_value=SimpleNamespace(),
            ),
            patch.object(
                context_runner,
                "run_live_episode_context_queue",
                new=denied_run,
            ),
            patch.dict(os.environ, {}, clear=True),
        ):
            exit_code = context_runner.main(
                [
                    "run",
                    "--database",
                    str(database_path),
                    "--contract",
                    str(self.contract),
                    "--runtime-authorization",
                    str(runtime_path),
                    "--output-root",
                    str(self.root / "cli-output"),
                    "--limit",
                    "1",
                    "--execute",
                ]
            )
        self.assertEqual(exit_code, 75)

    def test_completed_attempt_binds_explicit_turn_capacity_usage_and_terminal_columns(self):
        connection, queue, loaded, item = self.managed_context_claim(
            name="explicit-ledger", episode_id="explicit-ledger-episode"
        )
        self.write_managed_sqlite_turn(
            queue=queue,
            item=item,
            loaded=loaded,
            name="explicit-ledger-turn",
            output=self.context_output(item.episode_id),
        )
        attempt = connection.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["status"], "completed")
        self.assertTrue(Path(attempt["turn_input_path"]).is_file())
        self.assertEqual(len(attempt["source_input_sha256"]), 64)
        self.assertTrue(Path(attempt["preclaim_capacity_request_path"]).is_file())
        self.assertTrue(Path(attempt["preclaim_capacity_admission_path"]).is_file())
        self.assertIsNone(attempt["preturn_capacity_request_path"])
        self.assertIsNone(attempt["preturn_capacity_admission_path"])
        self.assertGreater(json.loads(attempt["usage_json"])["total_tokens"], 0)
        self.assertTrue(Path(attempt["terminal_path"]).is_file())
        connection.close()

    async def test_live_execution_is_hard_disabled_without_queue_or_model_calls(self):
        queue = ForbiddenQueue()
        factory = ForbiddenFactory()
        result = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "live-disabled",
            queue=queue,
            dry_run=False,
            fixture_mode=False,
            fixture_client_factory=factory,
        )
        self.assertEqual(result["state"], "waiting")
        self.assertEqual(result["terminal_reason"], "live_episode_context_execution_not_implemented")
        self.assertEqual(result["live_model_call_count"], 0)
        self.assertEqual(result["live_database_mutation_count"], 0)
        self.assertEqual(queue.calls, [])
        self.assertEqual(factory.calls, [])

    async def test_fixture_run_preserves_llm_authority_threads_telemetry_and_zero_backlog(self):
        run_root = self.root / "fixture-run"
        queue = FakeQueue(self.items)
        factory = FixtureFactory()
        with patch.dict(os.environ, {}, clear=True):
            receipt = await run_episode_context_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                dry_run=False,
                fixture_mode=True,
                fixture_client_factory=factory,
                fixture_capacity_provider=FixtureCapacity(),
            )
        self.assertEqual(receipt["schema_version"], COMPLETION_RECEIPT_VERSION)
        self.assertEqual(receipt["state"], "passed")
        self.assertEqual(receipt["backlog"]["remaining_episode_context_jobs"], 0)
        self.assertEqual(receipt["fixture_turn_count"], 2)
        self.assertEqual(receipt["live_model_call_count"], 0)
        self.assertFalse(receipt["production_mutated"])
        self.assertEqual(receipt["usage"]["input_tokens"], 300)
        self.assertEqual(receipt["usage"]["cached_input_tokens"], 30)
        self.assertEqual(receipt["usage"]["output_tokens"], 75)
        self.assertEqual(receipt["usage"]["reasoning_output_tokens"], 15)
        self.assertEqual(receipt["usage"]["total_tokens"], 375)
        self.assertEqual(receipt["usage"]["wall_elapsed_seconds"], 3.75)
        self.assertEqual(len(factory.calls), 1)
        client = FakeClient.instances[-1]
        self.assertEqual(len(client.threads), 2)
        self.assertEqual(len({thread.thread_id for thread in client.threads}), 2)
        self.assertEqual(len(client.turns), 2)
        self.assertEqual(len({turn[1] for turn in client.turns}), 2)
        self.assertEqual(len(queue.submissions), 2)
        for item, (_, submitted) in zip(self.items, queue.submissions):
            self.assertEqual(
                submitted["episode_context"]["context_summary"],
                item.source_input["full_episode_text"],
            )
            self.assertEqual(
                submitted["extraction_guidance"],
                item.source_input["requested_guidance"],
            )
            self.assertEqual(
                submitted["excluded_source_context"],
                item.source_input["llm_exclusions"],
            )
        verified = self.verifier(run_root)
        self.assertEqual(verified["remaining_episode_context_jobs"], 0)
        self.assertEqual(verified["pending_episode_context_jobs"], 0)
        self.assertEqual(verified["claimed_episode_context_jobs"], 0)
        self.assertEqual(verified["failed_episode_context_jobs"], 0)
        self.assertEqual(verified["required_episode_count"], 2)
        self.assertEqual(verified["unique_thread_count"], 2)
        self.assertEqual(verified["unique_turn_count"], 2)
        self.assertFalse(verified["production_ready"])
        self.assertEqual(set(verified["context_artifact_index"]), {"episode-1", "episode-2"})
        artifact = verify_episode_context_artifact_for_episode(
            run_root / "completion-receipt.json",
            "episode-2",
            expected_contract_sha256=sha_bytes(self.contract.read_bytes()),
            expected_evaluation_receipt=self.evaluation_receipt,
            expected_holdout_receipt=self.holdout_receipt,
        )
        self.assertEqual(
            artifact["episode_context"]["context_summary"],
            self.items[1].source_input["full_episode_text"],
        )
        self.assertEqual(
            artifact["extraction_guidance"],
            self.items[1].source_input["requested_guidance"],
        )
        self.assertEqual(
            artifact["excluded_source_context"],
            self.items[1].source_input["llm_exclusions"],
        )

    async def test_stale_completed_sidecar_recovers_without_client_or_replay(self):
        run_root = self.root / "stale-recovery"
        queue = FakeQueue(self.items[:1])
        first_factory = FixtureFactory(fail_after_write=True)
        with patch.dict(os.environ, {}, clear=True):
            first = await run_episode_context_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                dry_run=False,
                fixture_mode=True,
                fixture_client_factory=first_factory,
                fixture_capacity_provider=FixtureCapacity(),
            )
        self.assertEqual(first["state"], "waiting")
        self.assertEqual(first["semantic_retry_count"], 0)
        self.assertEqual(queue.status[1], "claimed")
        forbidden = ForbiddenFactory()
        with patch.dict(os.environ, {}, clear=True):
            completion = await run_episode_context_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                dry_run=False,
                fixture_mode=True,
                fixture_client_factory=forbidden,
            )
        self.assertEqual(completion["state"], "passed")
        self.assertEqual(forbidden.calls, [])
        self.assertEqual(len(first_factory.calls), 1)
        self.assertEqual(len(queue.submissions), 1)
        self.assertTrue(completion["turns"][0]["recovered_without_replay"])
        self.assertEqual(completion["semantic_retry_count"], 0)
        self.assertEqual(self.verifier(run_root)["remaining_episode_context_jobs"], 0)

    async def test_ambiguous_stale_claim_is_preserved_without_replay(self):
        queue = FakeQueue(self.items[:1], preclaimed=True)
        forbidden = ForbiddenFactory()
        result = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "ambiguous-stale",
            queue=queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=forbidden,
        )
        self.assertEqual(result["state"], "waiting")
        self.assertEqual(
            result["terminal_reason"],
            "ambiguous_stale_claim_preserved_without_replay",
        )
        self.assertEqual(result["ambiguous_stale_claim_count"], 1)
        self.assertEqual(result["semantic_retry_count"], 0)
        self.assertEqual(queue.status[1], "claimed")
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(forbidden.calls, [])

    async def test_capacity_admission_is_exact_and_precedes_claim(self):
        missing_queue = FakeQueue(self.items[:1])
        missing_factory = ForbiddenFactory()
        missing = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "missing-capacity",
            queue=missing_queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=missing_factory,
        )
        self.assertEqual(
            missing["terminal_reason"], "capacity_admission_required_before_claim"
        )
        self.assertEqual(missing_queue.claim_calls, 0)
        self.assertEqual(missing_queue.status[1], "pending")
        self.assertEqual(missing_factory.calls, [])

        mismatch_queue = FakeQueue(self.items[:1])
        mismatch_factory = ForbiddenFactory()
        mismatch_provider = FixtureCapacity(
            mutate=lambda admission: admission.update({"admitted_turn_count": 0})
        )
        mismatch = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "mismatched-capacity",
            queue=mismatch_queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=mismatch_factory,
            fixture_capacity_provider=mismatch_provider,
        )
        self.assertEqual(
            mismatch["terminal_reason"], "capacity_admission_failed_before_claim"
        )
        self.assertEqual(mismatch_queue.claim_calls, 0)
        self.assertEqual(mismatch_queue.status[1], "pending")
        self.assertEqual(mismatch_factory.calls, [])
        self.assertEqual(len(mismatch_provider.calls), 1)

    async def test_one_turn_usage_requires_exact_cumulative_equality(self):
        queue = FakeQueue(self.items[:1])
        factory = FixtureFactory(thread_total_extra=True)
        result = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "cumulative-usage-drift",
            queue=queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=factory,
            fixture_capacity_provider=FixtureCapacity(),
        )
        self.assertEqual(result["state"], "waiting")
        self.assertEqual(
            result["terminal_reason"], "fixture_turn_failure_preserved_without_replay"
        )
        self.assertEqual(queue.submissions, [])

    async def test_nested_semantic_map_mismatch_is_not_persisted(self):
        queue = FakeQueue(self.items[:1])
        result = await run_episode_context_queue(
            contract_path=self.contract,
            output_root=self.root / "nested-semantic-mismatch",
            queue=queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=FixtureFactory(nested_mismatch=True),
            fixture_capacity_provider=FixtureCapacity(),
        )
        self.assertEqual(result["state"], "waiting")
        self.assertEqual(queue.submissions, [])

    async def test_input_launch_tamper_and_orphan_attempt_block_completion(self):
        tamper_root = self.root / "input-launch-tamper"
        await run_episode_context_queue(
            contract_path=self.contract,
            output_root=tamper_root,
            queue=FakeQueue(self.items[:1]),
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=FixtureFactory(),
            fixture_capacity_provider=FixtureCapacity(),
        )
        attempt_root = next((tamper_root / "turns").iterdir())
        input_path = attempt_root / "input.private.json"
        launch_path = attempt_root / "launch.json"
        terminal_path = attempt_root / "terminal.json"
        input_payload = json.loads(input_path.read_text())
        input_payload["episode_id"] = "tampered-episode"
        input_path.write_text(canonical(input_payload), encoding="utf-8")
        launch = json.loads(launch_path.read_text())
        launch["input"] = record(input_path)
        launch_path.write_text(pretty(launch), encoding="utf-8")
        terminal = json.loads(terminal_path.read_text())
        terminal["input"] = record(input_path)
        terminal["launch"] = record(launch_path)
        terminal_path.write_text(pretty(terminal), encoding="utf-8")
        completion_path = tamper_root / "completion-receipt.json"
        completion = json.loads(completion_path.read_text())
        completion["turns"][0]["input"] = record(input_path)
        completion["turns"][0]["terminal"] = record(terminal_path)
        completion_path.write_text(pretty(completion), encoding="utf-8")
        with self.assertRaisesRegex(
            EpisodeContextRunnerError, "input and launch reconciliation drifted"
        ):
            self.verifier(tamper_root)

        orphan_root = self.root / "orphan-attempt"
        await run_episode_context_queue(
            contract_path=self.contract,
            output_root=orphan_root,
            queue=FakeQueue(self.items[:1]),
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=FixtureFactory(),
            fixture_capacity_provider=FixtureCapacity(),
        )
        partial = orphan_root / "turns" / "orphan-partial-attempt"
        partial.mkdir()
        (partial / "input.private.json").write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            EpisodeContextRunnerError, "orphan or partial attempt"
        ):
            self.verifier(orphan_root)

    async def test_completion_verifier_rejects_nonzero_backlog_and_artifact_tamper(self):
        run_root = self.root / "verifier-tamper"
        queue = FakeQueue(self.items[:1])
        await run_episode_context_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            dry_run=False,
            fixture_mode=True,
            fixture_client_factory=FixtureFactory(),
            fixture_capacity_provider=FixtureCapacity(),
        )
        completion = json.loads((run_root / "completion-receipt.json").read_text())
        nonzero = dict(completion)
        backlog = dict(nonzero["backlog"])
        counts = dict(backlog["job_status_counts"])
        counts["claimed"] = 1
        backlog["job_status_counts"] = counts
        backlog["remaining_episode_context_jobs"] = 1
        unhashed = dict(backlog)
        unhashed.pop("snapshot_sha256")
        backlog["snapshot_sha256"] = sha_bytes(canonical(unhashed).encode())
        nonzero["backlog"] = backlog
        nonzero_path = run_root / "nonzero-completion.json"
        nonzero_path.write_text(pretty(nonzero))
        with self.assertRaisesRegex(EpisodeContextRunnerError, "backlog is not zero"):
            verify_episode_context_completion_receipt(
                nonzero_path,
                expected_contract_sha256=sha_bytes(self.contract.read_bytes()),
                expected_evaluation_receipt=self.evaluation_receipt,
                expected_holdout_receipt=self.holdout_receipt,
            )
        missing = dict(completion)
        missing_index = dict(missing["context_artifact_index"])
        missing_index.pop("episode-1")
        missing["context_artifact_index"] = missing_index
        missing["context_artifact_index_sha256"] = sha_bytes(
            canonical(missing_index).encode()
        )
        missing_path = run_root / "missing-artifact-completion.json"
        missing_path.write_text(pretty(missing))
        with self.assertRaisesRegex(EpisodeContextRunnerError, "artifact index drifted"):
            verify_episode_context_completion_receipt(
                missing_path,
                expected_contract_sha256=sha_bytes(self.contract.read_bytes()),
                expected_evaluation_receipt=self.evaluation_receipt,
                expected_holdout_receipt=self.holdout_receipt,
            )
        artifact_path = Path(completion["context_artifact_index"]["episode-1"]["path"])
        artifact_payload = json.loads(artifact_path.read_text())
        artifact_payload["extraction_guidance"] = "drifted semantic guidance"
        artifact_path.write_text(pretty(artifact_payload))
        with self.assertRaisesRegex(EpisodeContextRunnerError, "artifact drifted"):
            verify_episode_context_artifact_for_episode(
                run_root / "completion-receipt.json",
                "episode-1",
                expected_contract_sha256=sha_bytes(self.contract.read_bytes()),
                expected_evaluation_receipt=self.evaluation_receipt,
                expected_holdout_receipt=self.holdout_receipt,
            )
        artifact_path.write_text(pretty({
            **artifact_payload,
            "extraction_guidance": self.items[0].source_input["requested_guidance"],
        }))
        sidecar_path = next((run_root / "turns").glob("*/sidecar.json"))
        sidecar = json.loads(sidecar_path.read_text())
        sidecar["usage"]["cached_input_tokens"] += 1
        sidecar_path.write_text(pretty(sidecar))
        with self.assertRaisesRegex(EpisodeContextRunnerError, "sidecar artifact drifted"):
            self.verifier(run_root)


if __name__ == "__main__":
    unittest.main()
