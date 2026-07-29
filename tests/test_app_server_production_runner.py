from __future__ import annotations

import copy
import contextlib
import hashlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from research_factory import db as factory_db
from research_factory import app_server_canonical_v31_episode_batch as episode_batch
from research_factory import app_server_canonical_v31_production_contract as production_contract
from research_factory import app_server_episode_context_runner as context_runner
from research_factory import app_server_expanded_cap_episode_batch as legacy_production_contract
from research_factory import app_server_production_runner as production_runner
from research_factory.app_server_source_integrity import VerifiedSourceLoader
from research_factory import efficient_backtest
from research_factory import worker as worker_module
from research_factory.app_server_production_runner import (
    ALLOWED_BATCH_SIZES,
    ALLOWED_THREAD_MODES,
    CLAIM_RECEIPT_VERSION,
    CONTRACT_VERSION,
    FROZEN_LABEL_PACK,
    FAILED_LABEL_RECONCILIATION_POLICY,
    FROZEN_QUEUE_LANE,
    FROZEN_QUEUE_PAYLOAD_MODEL,
    RUN_RECEIPT_VERSION,
    RUNTIME_CONFIG_VERSION,
    TRANSPORT,
    USAGE_TELEMETRY_VERSION,
    ProductionRunnerError,
    ProductionRunnerWaiting,
    ProductionWriterLock,
    QueueItem,
    SQLiteWorkerQueue,
    _default_client_factory,
    _verified_context_control_overlay,
    load_contract,
    run_production_queue,
)
from research_factory.codex_app_server import (
    AppServerThread,
    AppServerThreadArchiveState,
)
from research_factory.pipeline_babysitter import verify_evaluation_receipt
from research_factory.util import sha256_text


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


def no_signal_output(prompt: str) -> dict:
    packet = json.loads(prompt.split("\n", 1)[1])
    return {
        "episode_id": packet["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "Fixture-only structural classification.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "Fixture-only no-signal output.",
                "overall_confidence": 1.0,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in segment["source_units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
            for segment in packet["segments"]
        ],
    }


def coded_and_no_signal_output(request: dict) -> dict:
    """Build a raw canonical-adapter turn with one coded and one no-signal row."""

    example = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "label_packs/ai_discourse_v3_1/examples.json"
        ).read_text(encoding="utf-8")
    )[0]["output"]
    segments = []
    for index, source in enumerate(request["private_input"]["segments"]):
        units = source["units"]
        if index == 0:
            row = copy.deepcopy(example)
            for field in ("schema_version", "episode_id", "segment_quality"):
                row.pop(field)
            row["segment_id"] = source["segment_id"]
            unit_id = units[0]["unit_id"]
            for event in row["discourse_events"]:
                for field in ("evidence", "evidence_start", "evidence_end"):
                    event.pop(field)
                event["evidence_start_unit_id"] = unit_id
                event["evidence_end_unit_id"] = unit_id
            for candidate in row["concept_candidates"]:
                for field in ("evidence", "evidence_start", "evidence_end"):
                    candidate.pop(field)
                candidate["evidence_start_unit_id"] = unit_id
                candidate["evidence_end_unit_id"] = unit_id
            row["unit_receipts"] = [
                {
                    "unit_id": unit["unit_id"],
                    "reviewed": True,
                    "grounded_event_count": (
                        len(row["discourse_events"]) if unit["unit_id"] == unit_id else 0
                    ),
                    "grounded_concept_candidate_count": (
                        len(row["concept_candidates"])
                        if unit["unit_id"] == unit_id
                        else 0
                    ),
                    "unresolved_count": 0,
                }
                for unit in units
            ]
            row["coverage_audit"] = {
                "all_source_units_reviewed": True,
                "unresolved_count": 0,
            }
        else:
            row = {
                "segment_id": source["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "This source unit contains only show setup.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "No grounded discourse event is present.",
                "overall_confidence": 1.0,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in units
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
        segments.append(row)
    return {"episode_id": request["episode_id"], "segments": segments}


def fixture_segment_quality(text: str, *, substantive_word_count=None) -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": (
            len(text.split())
            if substantive_word_count is None
            else substantive_word_count
        ),
        "transcript_preparation_id": "prep-fixture",
    }


def fixture_source_binding(
    *, segment_id: str, episode_id: str, segment_index: int, text: str, quality: dict
) -> dict:
    text_sha256 = sha_bytes(text.encode())
    transcript_id = f"tr-{episode_id}"
    return {
        "schema_version": "pif_app_server_source_integrity_v1",
        "episode_id": episode_id,
        "source_id": "src-fixture",
        "transcript": {
            "transcript_id": transcript_id,
            "raw_text_path": f"corpus/raw/{transcript_id}.txt",
            "raw_text_sha256": text_sha256,
            "verified_file": {"expected_sha256": text_sha256},
        },
        "transcript_preparation": {
            "transcript_preparation_id": "prep-fixture",
            "transcript_preparation_status": "prepared",
            "cleaned_text_path": f"corpus/prepared/{transcript_id}.txt",
            "cleaned_text_sha256": text_sha256,
            "artifact_type": "dialogue_transcript",
            "substantive_word_count": quality["substantive_word_count"],
            "boilerplate_ratio": 0.0,
            "segment_quality": quality,
        },
        "transcript_source_basis": {
            "transcript_id": transcript_id,
            "transcript_preparation_id": "prep-fixture",
            "transcript_preparation_status": "prepared",
            "source_basis": "cleaned_preparation",
            "source_path": f"corpus/prepared/{transcript_id}.txt",
            "source_sha256": text_sha256,
            "verified_file": {"expected_sha256": text_sha256},
        },
        "segment": {
            "segment_id": segment_id,
            "transcript_id": transcript_id,
            "segment_index": segment_index,
            "start_char": 0,
            "end_char": len(text),
            "text_path": f"corpus/segments/{segment_id}.txt",
            "text_sha256": text_sha256,
            "verified_file": {"expected_sha256": text_sha256},
        },
    }


class FakeQueue:
    def __init__(self, items, *, lane=FROZEN_QUEUE_LANE):
        self.lane = lane
        self.pending = list(items)
        self.items_by_job = {item.job_id: item for item in items}
        self.production_mutated = False
        self.preview_calls = 0
        self.claim_calls = 0
        self.bindings = []
        self.submissions = []
        self.failures = []
        self.events = []
        self.same_thread_episode_roots = {}

    def preview_episode_batch(self, *, limit, episode_id=None):
        self.preview_calls += 1
        self.events.append("preview")
        eligible = [
            item
            for item in self.pending
            if episode_id is None or item.episode_id == episode_id
        ]
        if not eligible:
            return []
        selected_episode_id = episode_id or eligible[0].episode_id
        return [
            item for item in eligible if item.episode_id == selected_episode_id
        ][:limit]

    def pending_episode_segment_count(self, *, episode_id):
        return sum(item.episode_id == episode_id for item in self.pending)

    def unresolved_episode_segment_count(self, *, episode_id):
        return self.pending_episode_segment_count(episode_id=episode_id)

    def unresolved_scoped_job_count(self):
        return len(self.pending)

    def reconcile_failed_scoped_jobs(self, *, execute):
        return {
            "schema_version": "pif_app_server_failed_label_reconciliation_v1",
            "state": "no_change",
            "failed_job_count": 0,
            "execute": execute,
            "production_mutated": False,
        }

    def assert_episode_start_allowed(self, episode_id, *, thread_mode, output_root):
        if thread_mode != "same_thread":
            return
        root = str(Path(output_root).resolve())
        prior = self.same_thread_episode_roots.setdefault(episode_id, root)
        if prior != root:
            raise ProductionRunnerWaiting(
                "same-thread episode has completed production lineage in another root"
            )

    def stale_episode_batch(self, *, limit):
        return []

    def recover_stale_prelaunch_batch(self, *, limit):
        return {
            "state": "none",
            "job_ids": [],
            "semantic_model_call_count": 0,
            "production_mutated": False,
        }

    def release_verified_prelaunch_batch(
        self,
        items,
        *,
        prepared_request_path,
        claim_receipt_path,
        reason,
        thread_started_for_batch,
    ):
        requested = {item.job_id for item in items}
        self.pending = [self.items_by_job[item.job_id] for item in items] + self.pending
        self.production_mutated = True
        self.events.append("release_prelaunch")
        return {
            "state": "released_verified_prelaunch_source_drift",
            "job_ids": sorted(requested),
            "semantic_model_call_count": 0,
            "thread_started_for_batch": thread_started_for_batch,
            "turn_started": False,
            "production_mutated": True,
        }

    def claim_episode_batch(
        self, items, *, prepared_request_path, claim_receipt_path
    ):
        self.claim_calls += 1
        self.events.append("claim")
        requested = [item.job_id for item in items]
        if requested != [item.job_id for item in self.pending[: len(items)]]:
            raise ProductionRunnerWaiting("fixture exact claim drifted")
        self.pending = self.pending[len(items) :]
        self.production_mutated = True
        claimed = []
        for item in items:
            claimed.append(
                QueueItem(
                    **{
                        **item.__dict__,
                        "label_run_id": f"run-{item.job_id}",
                        "output_path": str(
                            prepared_request_path.parent
                            / f"segment-{item.job_id}.json"
                        ),
                    }
                )
            )
        claim_receipt_path.write_text(
            pretty(
                {
                    "schema_version": CLAIM_RECEIPT_VERSION,
                    "state": "claimed",
                    "episode_id": items[0].episode_id,
                    "job_ids": requested,
                    "segment_ids": [item.segment_id for item in items],
                    "worker_id": "fixture-worker",
                    "lane": self.lane,
                    "label_pack": FROZEN_LABEL_PACK,
                    "payload_model_policy": "null_or_exact_frozen_queue_model",
                    "queue_payload_model": FROZEN_QUEUE_PAYLOAD_MODEL,
                    "prepared_request": record(prepared_request_path),
                    "claimed_at": "2026-07-18T00:00:00+00:00",
                    "leased_until": "2026-07-18T00:45:00+00:00",
                    "semantic_retry_count": 0,
                }
            ),
            encoding="utf-8",
        )
        return claimed

    def bind_prompt(self, items, prompt_path):
        self.bindings.append(([item.job_id for item in items], str(prompt_path)))

    def submit(self, item, output_path):
        self.submissions.append((item.job_id, str(output_path)))
        return {"job_id": str(item.job_id), "label_id": f"lbl-{item.job_id}"}

    def verify_completed_submissions(self, turns):
        expected = []
        for turn in turns:
            for segment in turn.get("segment_records", []):
                output_path = Path(segment["output"]["path"]).resolve()
                submission = segment["submission"]
                job_id = int(segment["job_id"])
                if (
                    str(submission.get("job_id") or "") != str(job_id)
                    or submission.get("label_id") != f"lbl-{job_id}"
                ):
                    raise ProductionRunnerError(
                        "fixture completed submission lineage drifted"
                    )
                expected.append((job_id, str(output_path)))
        if (
            len(set(expected)) != len(expected)
            or any(item not in self.submissions for item in expected)
        ):
            raise ProductionRunnerError(
                "fixture completed submission persistence drifted"
            )
        return {
            "state": "verified_completed_submissions",
            "job_ids": [job_id for job_id, _ in expected],
            "submitted_segment_count": len(expected),
            "production_mutated": bool(expected),
        }

    def verify_source_packet(self, source_packet):
        rows = source_packet.get("segments", [])
        segment_ids = []
        for row in rows:
            item = self.items_by_job.get(row.get("job_id"))
            if (
                item is None
                or row.get("segment_id") != item.segment_id
                or row.get("segment_index") != item.segment_index
                or row.get("segment_text_sha256") != item.segment_text_sha256
                or row.get("segment_quality") != item.segment_quality
                or row.get("expected_source_binding")
                != item.expected_source_binding
            ):
                raise ProductionRunnerError("fixture source packet drifted")
            segment_ids.append(item.segment_id)
        return {
            "state": "verified_current_source_packet",
            "segment_ids": segment_ids,
            "segment_count": len(segment_ids),
            "production_mutated": False,
        }

    def fail(self, item, reason):
        self.failures.append((item.job_id, reason))


class MutatingClaimFailureQueue(FakeQueue):
    def claim_episode_batch(self, items, **kwargs):
        self.claim_calls += 1
        self.production_mutated = True
        raise ProductionRunnerWaiting("fixture failure after atomic claim")


class ReleasedRecoveryQueue(FakeQueue):
    def recover_stale_prelaunch_batch(self, *, limit):
        self.production_mutated = True
        return {
            "state": "released_verified_prelaunch_claim",
            "job_ids": [self.pending[0].job_id],
            "semantic_model_call_count": 0,
            "claim_receipt_sha256": "c" * 64,
            "production_mutated": True,
        }


class UnresolvedScopedQueue(FakeQueue):
    def __init__(self, items, *, unresolved_after_pending=1):
        super().__init__(items)
        self.unresolved_after_pending = unresolved_after_pending

    def unresolved_episode_segment_count(self, *, episode_id):
        return (
            super().unresolved_episode_segment_count(episode_id=episode_id)
            + self.unresolved_after_pending
        )

    def unresolved_scoped_job_count(self):
        return super().unresolved_scoped_job_count() + self.unresolved_after_pending


class PersistedPathTamperOnDrainQueue(FakeQueue):
    def __init__(self, items, *, output_root):
        super().__init__(items)
        self.output_root = Path(output_root)
        self.tampered = False

    def unresolved_episode_segment_count(self, *, episode_id):
        count = super().unresolved_episode_segment_count(episode_id=episode_id)
        if count == 0 and not self.tampered:
            launch_path = next((self.output_root / "turns").rglob("launch.json"))
            terminal_path = launch_path.parent / "terminal.json"
            launch = json.loads(launch_path.read_text(encoding="utf-8"))
            launch["thread_persisted_path_sha256"] = "f" * 64
            launch_path.write_text(pretty(launch), encoding="utf-8")
            terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
            terminal["launch"] = record(launch_path)
            terminal_path.write_text(pretty(terminal), encoding="utf-8")
            self.tampered = True
        return count


class FakeCapacityProvider:
    def __init__(self, queue=None):
        self.queue = queue
        self.calls = []

    def __call__(self, *, request, remaining_batch_count, timeout_seconds):
        if self.queue is not None:
            if self.queue.events and self.queue.events[-1] != "preview":
                raise AssertionError("capacity must follow the current batch preview")
            self.queue.events.append("capacity")
        requirements = production_runner._capacity_requirements(  # noqa: SLF001
            request,
            remaining_batch_count=remaining_batch_count,
            timeout_seconds=timeout_seconds,
        )
        policy = production_contract.capacity_admission_contract()
        payload = {
            "schema_version": production_runner.CAPACITY_RECEIPT_VERSION,
            "state": "admitted_offline_fixture_non_promotable",
            "production_scoped": False,
            "fixture_only": True,
            "offline_fixture_non_promotable": True,
            "live_capacity_authority": False,
            "official_method": None,
            "protocol_schema": None,
            "raw_snapshot_sha256": None,
            "selected_limit_id": None,
            "applicable_controls": [],
            "reset_credits_counted_as_capacity": False,
            "capacity_policy": policy,
            "capacity_policy_sha256": sha_bytes(canonical(policy).encode()),
            "requirements": requirements,
            "available_total_tokens": requirements["required_total_tokens"] + 1,
            "available_wall_seconds": requirements["required_wall_seconds"] + 1,
            "operator_deadline": None,
            "semantic_authority": False,
            "semantic_pruning_or_relabeling_allowed": False,
            "capacity_probe_before_thread_start": True,
            "semantic_thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
        }
        self.calls.append((request["batch_id"], remaining_batch_count))
        return payload


class FakeClient:
    instances = []
    durable_threads = {}
    durable_counter = 0
    turn_counter = 0

    def __init__(
        self,
        *,
        plan_type="pro",
        duplicate_turn_ids=False,
        instruction_source_drift=False,
    ):
        self.account_summary = {"type": "chatgpt", "plan_type": plan_type}
        self.duplicate_turn_ids = duplicate_turn_ids
        self.instruction_source_drift = instruction_source_drift
        self.entered = 0
        self.exited = 0
        self.started_threads = []
        self.resumed_threads = []
        self.archived_threads = []
        self.archive_state_queries = []
        self.turns = []
        self.turn_batch_sizes = []
        self.thread_totals = {}
        self.__class__.instances.append(self)

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.exited += 1

    async def start_thread(self, *, model, base_instructions, cwd, ephemeral):
        if ephemeral:
            thread_id = f"thread-{len(self.instances)}-{len(self.started_threads) + 1}"
        else:
            self.__class__.durable_counter += 1
            thread_id = f"durable-thread-{self.__class__.durable_counter}"
        sources = episode_batch.expected_instruction_source_contract()
        persisted_path_sha256 = (
            sha_bytes(f"/fixture/threads/{thread_id}.jsonl".encode())
            if not ephemeral
            else None
        )
        thread = AppServerThread(
            thread_id=thread_id,
            model=model,
            cwd=str(Path(cwd).resolve()),
            ephemeral=ephemeral,
            instruction_sources_sha256=(
                "0" * 64
                if self.instruction_source_drift
                else sources["effective_instruction_sources_sha256"]
            ),
            instruction_sources_count=(
                sources["effective_instruction_sources_count"] + 1
                if self.instruction_source_drift
                else sources["effective_instruction_sources_count"]
            ),
            base_instructions_sha256=sha_bytes(base_instructions.encode()),
            base_instructions_bytes=len(base_instructions.encode()),
            persisted_path_sha256=persisted_path_sha256,
        )
        self.started_threads.append(thread)
        if not ephemeral:
            self.__class__.durable_threads[thread_id] = {
                "model": model,
                "cwd": str(Path(cwd).resolve()),
                "base_instructions": base_instructions,
                "instruction_sources_sha256": thread.instruction_sources_sha256,
                "instruction_sources_count": thread.instruction_sources_count,
                "persisted_path_sha256": persisted_path_sha256,
                "completed_turn_ids": [],
                "thread_total_usage": {
                    field: 0 for field in episode_batch.USAGE_FIELDS
                },
                "reasoning_effort": None,
                "archived": False,
            }
        return thread

    async def resume_thread(
        self,
        *,
        thread_id,
        model,
        effort,
        base_instructions,
        cwd,
        expected_instruction_sources_sha256,
        expected_instruction_sources_count,
        expected_completed_turn_ids,
    ):
        stored = self.__class__.durable_threads.get(thread_id)
        if (
            not stored
            or stored["archived"]
            or stored["model"] != model
            or stored["cwd"] != str(Path(cwd).resolve())
            or stored["base_instructions"] != base_instructions
            or stored["instruction_sources_sha256"]
            != expected_instruction_sources_sha256
            or stored["instruction_sources_count"]
            != expected_instruction_sources_count
            or tuple(stored["completed_turn_ids"])
            != expected_completed_turn_ids
        ):
            raise ProductionRunnerWaiting("fixture durable thread resume drifted")
        stored["reasoning_effort"] = effort
        self.thread_totals[thread_id] = copy.deepcopy(
            stored["thread_total_usage"]
        )
        thread = AppServerThread(
            thread_id=thread_id,
            model=model,
            cwd=str(Path(cwd).resolve()),
            ephemeral=False,
            instruction_sources_sha256=stored[
                "instruction_sources_sha256"
            ],
            instruction_sources_count=stored["instruction_sources_count"],
            base_instructions_sha256=sha_bytes(base_instructions.encode()),
            base_instructions_bytes=len(base_instructions.encode()),
            reasoning_effort=effort,
            persisted_path_sha256=stored["persisted_path_sha256"],
            prior_completed_turn_ids=expected_completed_turn_ids,
        )
        self.resumed_threads.append(thread)
        return thread

    async def archive_thread(self, thread_id):
        stored = self.__class__.durable_threads.get(thread_id)
        if not stored or stored["archived"]:
            raise ProductionRunnerWaiting("fixture archive thread is missing")
        stored["archived"] = True
        self.archived_threads.append(thread_id)

    async def thread_archive_state(self, thread_id):
        self.archive_state_queries.append(thread_id)
        stored = self.__class__.durable_threads.get(thread_id)
        if stored is None:
            return AppServerThreadArchiveState(
                thread_id=thread_id,
                state="absent",
                active_match_count=0,
                archived_match_count=0,
            )
        if stored["archived"]:
            return AppServerThreadArchiveState(
                thread_id=thread_id,
                state="archived",
                active_match_count=0,
                archived_match_count=1,
            )
        return AppServerThreadArchiveState(
            thread_id=thread_id,
            state="active",
            active_match_count=1,
            archived_match_count=0,
        )

    async def run_structured_turn(self, **kwargs):
        output = no_signal_output(kwargs["prompt"])
        output_text = canonical(output)
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        durable = self.__class__.durable_threads.get(kwargs["thread"].thread_id)
        turn_ordinal = (
            len(durable["completed_turn_ids"]) + 1
            if durable is not None
            else sum(
                thread_id == kwargs["thread"].thread_id
                for thread_id, _ in self.turns
            )
            + 1
        )
        self.__class__.turn_counter += 1
        call_ordinal = self.__class__.turn_counter
        turn_id = (
            "turn-constant"
            if self.duplicate_turn_ids
            else f"turn-{call_ordinal}"
        )
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 20 if turn_ordinal > 1 else 0,
            "output_tokens": 25,
            "reasoning_output_tokens": 5,
            "total_tokens": 125,
        }
        total = self.thread_totals.setdefault(
            kwargs["thread"].thread_id,
            {field: 0 for field in episode_batch.USAGE_FIELDS},
        )
        for field in episode_batch.USAGE_FIELDS:
            total[field] += usage[field]
        schema_json = canonical(kwargs["output_schema"])
        started_at = datetime.now(timezone.utc)
        sidecar = {
            "schema_version": episode_batch.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "started_at": started_at.isoformat(),
            "finished_at": (started_at + timedelta(seconds=1)).isoformat(),
            "client_version": episode_batch.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "cli_version": episode_batch.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "app_server_user_agent": "production-fixture-client",
            "protocol_schema_sha256": episode_batch._sha256_file(  # noqa: SLF001
                episode_batch.codex_app_server.PROTOCOL_SCHEMA_PATH
            ),
            "transport": "stdio",
            "max_message_bytes": 32 * 1024 * 1024,
            "synthetic_debug_errors": False,
            "status": "completed",
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": copy.deepcopy(total),
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_id": kwargs["thread"].thread_id,
            "turn_id": turn_id,
            "model": kwargs["thread"].model,
            "effort": kwargs["effort"],
            "thread_mode": kwargs["thread_mode"],
            "batch_size": kwargs["batch_size"],
            "error_class": None,
            "wall_elapsed_seconds": 1.25,
            "prompt_sha256": sha_bytes(kwargs["prompt"].encode()),
            "prompt_bytes": len(kwargs["prompt"].encode()),
            "base_instructions_sha256": kwargs["thread"].base_instructions_sha256,
            "base_instructions_bytes": kwargs["thread"].base_instructions_bytes,
            "instruction_sources_sha256": kwargs[
                "thread"
            ].instruction_sources_sha256,
            "instruction_sources_count": kwargs[
                "thread"
            ].instruction_sources_count,
            "output_schema_sha256": sha_bytes(schema_json.encode()),
            "output_schema_bytes": len(schema_json.encode()),
            "output_sha256": sha_bytes(output_text.encode()),
            "output_path": str(output_path.resolve()),
            "stderr_sha256": "0" * 64,
            "stderr_bytes": 0,
            "recovery_reran_model": False,
        }
        sidecar_path = Path(kwargs["sidecar_path"])
        sidecar_path.write_text(pretty(sidecar), encoding="utf-8")
        self.turns.append((kwargs["thread"].thread_id, turn_id))
        self.turn_batch_sizes.append(kwargs["batch_size"])
        if durable is not None:
            durable["completed_turn_ids"].append(turn_id)
            durable["reasoning_effort"] = kwargs["effort"]
            durable["thread_total_usage"] = copy.deepcopy(total)
        return SimpleNamespace(status_ok=True, output=output)


class ArchiveAfterRpcCrashClient(FakeClient):
    async def archive_thread(self, thread_id):
        await super().archive_thread(thread_id)
        raise ProductionRunnerWaiting("fixture crash after archive RPC")


class ProductionRunnerTest(unittest.IsolatedAsyncioTestCase):
    def _write_canonical_frozen_configuration(self, *, batch_size, thread_mode):
        variant_id = production_contract.selected_variant_id(
            batch_size=batch_size, thread_mode=thread_mode
        )
        runtime = production_contract.canonical_runtime_binding()
        arm_hashes = {
            variant: sha_bytes(f"arm:{variant}".encode())
            for variant in production_contract.EXPECTED_VARIANT_IDS
        }
        output_hashes = {
            variant: sha_bytes(f"output:{variant}".encode())
            for variant in production_contract.EXPECTED_VARIANT_IDS
        }
        label_hashes = {
            variant: sha_bytes(f"labels:{variant}".encode())
            for variant in production_contract.EXPECTED_VARIANT_IDS
        }
        self.matrix_lineage = self.root / "canonical-matrix-lineage.json"
        self.matrix_lineage.write_text(
            pretty(
                {
                    "schema_version": production_contract.MATRIX_RECEIPT_VERSION,
                    "state": "verified_full_six_arm_canonical_matrix",
                    "manifest_sha256": "1" * 64,
                    "context_set_sha256": "2" * 64,
                    "runtime_binding_sha256": runtime["matrix_binding_sha256"],
                    "context_control_overlay_sha256": runtime[
                        "context_control_overlay_sha256"
                    ],
                    "instruction_source_contract_sha256": runtime[
                        "instruction_source_contract_sha256"
                    ],
                    "capacity_policy_sha256": "3" * 64,
                    "precommit_sha256": "4" * 64,
                    "future_plan_binding_sha256": "5" * 64,
                    "future_execution_directive_sha256": "6" * 64,
                    "arm_envelope_sha256s": arm_hashes,
                    "full_output_validation_sha256": "7" * 64,
                    "opaque_case_order_sha256": "8" * 64,
                    "case_count": 12,
                    "arm_count": 6,
                    "per_arm_full_output_sha256s": output_hashes,
                    "per_arm_canonical_labels_sha256s": label_hashes,
                    "managed_chatgpt_auth_only": True,
                    "semantic_retry_count": 0,
                    "semantic_pruning": False,
                    "semantic_relabeling": False,
                    "production_mutated": False,
                }
            ),
            encoding="utf-8",
        )
        self.quality_lineage = self.root / "canonical-quality-lineage.json"
        self.quality_lineage.write_text(
            pretty(
                {
                    "schema_version": production_contract.QUALITY_RECEIPT_VERSION,
                    "state": "canonical_development_quality_passed",
                    "matrix_lineage_sha256": record(self.matrix_lineage)["sha256"],
                    "quality_result_sha256": "9" * 64,
                    "selected_variant_id": variant_id,
                    "selected_arm_envelope_sha256": arm_hashes[variant_id],
                    "selected_full_output_sha256": output_hashes[variant_id],
                    "selected_canonical_labels_sha256": label_hashes[variant_id],
                    "strict_full_field_macro": 0.99,
                    "baseline_strict_full_field_macro": 0.98,
                    "noninferiority_margin": 0.0,
                    "exact_evidence_rate": 1.0,
                    "exact_offset_rate": 1.0,
                    "exact_provenance_rate": 1.0,
                    "production_amortized_total_token_ratio": 0.21,
                    "usage_complete": True,
                    "evaluation_mode": "llm_only",
                    "embeddings_used": False,
                    "deterministic_semantic_matching": False,
                    "semantic_defaults": {},
                    "semantic_pruning": False,
                    "semantic_relabeling": False,
                    "production_mutated": False,
                }
            ),
            encoding="utf-8",
        )
        self.holdout_lineage = self.root / "canonical-holdout-lineage.json"
        self.holdout_lineage.write_text(
            pretty(
                {
                    "schema_version": production_contract.HOLDOUT_RECEIPT_VERSION,
                    "state": "untouched_canonical_holdout_passed",
                    "quality_lineage_sha256": record(self.quality_lineage)["sha256"],
                    "holdout_result_sha256": "a" * 64,
                    "selected_variant_id": variant_id,
                    "selected_arm_envelope_sha256": arm_hashes[variant_id],
                    "untouched_holdout": True,
                    "holdout_item_count": 60,
                    "holdout_passed": True,
                    "semantic_noninferior_or_better": True,
                    "exact_evidence_rate": 1.0,
                    "exact_offset_rate": 1.0,
                    "exact_provenance_rate": 1.0,
                    "managed_chatgpt_auth_only": True,
                    "semantic_retry_count": 0,
                    "semantic_pruning": False,
                    "semantic_relabeling": False,
                    "production_mutated": False,
                    "production_authorized": True,
                }
            ),
            encoding="utf-8",
        )
        self.frozen_configuration.write_text(
            pretty(
                production_contract.build_frozen_configuration(
                    batch_size=batch_size,
                    thread_mode=thread_mode,
                    matrix_lineage=record(self.matrix_lineage),
                    quality_lineage=record(self.quality_lineage),
                    holdout_lineage=record(self.holdout_lineage),
                )
            ),
            encoding="utf-8",
        )

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = self.root / "codex"
        self.binary.write_text("fixture pinned codex", encoding="utf-8")
        self.frozen_configuration = self.root / "frozen-configuration.json"
        self._write_canonical_frozen_configuration(
            batch_size=3, thread_mode="same_thread"
        )
        self.evaluation_root = self.root / "evaluation"
        self.evaluation_receipt = self._write_valid_evaluation_receipt()
        self.verified_evaluation = verify_evaluation_receipt(
            self.evaluation_receipt, self.evaluation_root
        )
        self.context_contract = self.root / "episode-context-contract.json"
        self.context_contract.write_text("{}\n", encoding="utf-8")
        self.context_runtime = self.root / "episode-context-runtime.json"
        self.context_runtime.write_text("{}\n", encoding="utf-8")
        self.context_provision_execution = (
            self.root / "episode-context-provision-execution.json"
        )
        self.context_provision_execution.write_text("{}\n", encoding="utf-8")
        self.context_required_manifest = self.root / "required-episodes.json"
        self.context_required_manifest.write_text("{}\n", encoding="utf-8")
        self.context_completion = self.root / "live-completion-receipt.json"
        self.production_promotion = self.root / "production-promotion.json"
        self.production_promotion.write_text("{}\n", encoding="utf-8")
        self.context_artifacts = {}
        self.context_label_pack = FROZEN_LABEL_PACK
        for episode_id in ("ep-a", "ep-b"):
            path = self.root / f"{episode_id}-context.json"
            path.write_text(
                pretty(
                    {
                        "episode_id": episode_id,
                        "episode_context": self._context_payload(episode_id),
                        "extraction_guidance": "Use every exact source unit.",
                        "excluded_source_context": ["ad reads only when semantically excluded"],
                    }
                ),
                encoding="utf-8",
            )
            self.context_artifacts[episode_id] = record(path)
        self.context_index_sha = sha_bytes(canonical(self.context_artifacts).encode())
        self.live_context_completion = {
            "schema_version": context_runner.LIVE_COMPLETION_RECEIPT_VERSION,
            "state": "passed",
            "phase": "episode_context",
            "execution_authority": "official_live",
            "test_only": False,
            "promotable": True,
            "managed_chatgpt_auth_only": True,
            "production_context_mutated": True,
            "label_queue_mutated": False,
            "contract": record(self.context_contract),
            "runtime_authorization": record(self.context_runtime),
            "provision_execution_receipt": record(
                self.context_provision_execution
            ),
            "context_artifact_index": self.context_artifacts,
            "context_artifact_index_sha256": self.context_index_sha,
        }
        self.context_completion.write_text(
            pretty(self.live_context_completion), encoding="utf-8"
        )
        self.fake_context_loaded = {
            "path": self.context_contract.resolve(),
            "sha256": record(self.context_contract)["sha256"],
            "contract": {
                "lineage": {"evaluation_receipt": record(self.evaluation_receipt)}
            },
            "configuration": {
                "lane": FROZEN_QUEUE_LANE,
                "label_pack": FROZEN_LABEL_PACK,
                "queue_payload_model": FROZEN_QUEUE_PAYLOAD_MODEL,
            },
            "configuration_sha256": "d" * 64,
            "verified_evaluation": self.verified_evaluation,
        }
        self.fake_context_runtime = {
            "path": self.context_runtime.resolve(),
            "provision_execution_path": self.context_provision_execution.resolve(),
            "provision_execution": {
                "required_episode_manifest": record(self.context_required_manifest)
            },
        }
        self.promotion_database_current = True
        self.contract = self.root / "contract.json"
        self.write_contract()
        self.real_context_runtime_binding = (
            production_contract.episode_context_runtime_binding()
        )
        self.real_verify_production_promotion = (
            production_contract.verify_production_promotion_authority
        )
        self.context_promotion_patch = patch(
            "research_factory.app_server_production_runner.production_contract."
            "verify_production_promotion_authority",
            side_effect=self._verify_production_promotion,
        )
        self.context_authority_patch = patch(
            "research_factory.app_server_production_runner."
            "_bind_same_sqlite_context_authority",
            side_effect=self._bind_test_context_authority,
        )
        self.context_artifact_patch = patch(
            "research_factory.app_server_production_runner.episode_context_runner."
            "_verify_managed_context_artifact",
            side_effect=self._verify_context_artifact,
        )
        self.context_promotion_mock = self.context_promotion_patch.start()
        self.context_authority_mock = self.context_authority_patch.start()
        self.context_artifact_mock = self.context_artifact_patch.start()
        self.items = []
        job_id = 1
        for episode_id, count in (("ep-a", 5), ("ep-b", 2)):
            for segment_index in range(count):
                text = f"Speaker {segment_index}:\nSystem {job_id} improves latency.\n"
                quality = fixture_segment_quality(text)
                segment_id = f"seg-{job_id}"
                self.items.append(
                    QueueItem(
                        job_id=job_id,
                        segment_id=segment_id,
                        episode_id=episode_id,
                        segment_index=segment_index,
                        segment_text=text,
                        label_run_id="preview",
                        output_path="preview",
                        source_name="Fixture Source",
                        episode_title=f"Fixture {episode_id}",
                        segment_text_sha256=sha_bytes(text.encode()),
                        segment_quality=quality,
                        expected_source_binding=fixture_source_binding(
                            segment_id=segment_id,
                            episode_id=episode_id,
                            segment_index=segment_index,
                            text=text,
                            quality=quality,
                        ),
                    )
                )
                job_id += 1

    def tearDown(self):
        self.context_artifact_patch.stop()
        self.context_authority_patch.stop()
        self.context_promotion_patch.stop()
        FakeClient.instances.clear()
        FakeClient.durable_threads.clear()
        FakeClient.durable_counter = 0
        FakeClient.turn_counter = 0
        self.temp.cleanup()

    def _context_payload(self, episode_id):
        return {
            "episode_id": episode_id,
            "source_name": "Fixture Source",
            "episode_title": f"Fixture {episode_id}",
            "context_summary": f"Context for {episode_id}",
            "speaker_map": [],
            "section_map": [],
            "entity_seed": {},
            "concept_seed": [],
        }

    def _sqlite_submission_fixture(self, output):
        database = self.root / f"submission-{len(list(self.root.glob('submission-*.sqlite')))}.sqlite"
        connection = factory_db.connect(database)
        factory_db.init_db(connection)
        timestamp = "2026-07-18T00:00:00+00:00"
        segment_id = str(output["segment_id"])
        episode_id = str(output["episode_id"])
        transcript_id = "tr-v31-submission"
        source_id = "src-v31-submission"
        segment_text = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "label_packs/ai_discourse_v3_1/examples.json"
            ).read_text(encoding="utf-8")
        )[0]["input"]["text"]
        segment_path = self.root / "corpus" / "segments" / f"{segment_id}.txt"
        segment_path.parent.mkdir(parents=True, exist_ok=True)
        segment_path.write_text(segment_text, encoding="utf-8")
        output_path = self.root / "runs" / "outputs" / f"{segment_id}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(pretty(output), encoding="utf-8")
        connection.execute(
            """
            INSERT INTO sources
              (id, name, policy, transcript_policy, enabled, metadata_json,
               created_at, updated_at)
            VALUES (?, 'Fixture Source', 'private_analysis_only',
                    'creator_rss_transcripts_only', 1, '{}', ?, ?)
            """,
            (source_id, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, created_at, updated_at)
            VALUES (?, ?, ?, 'Fixture Episode', ?, ?)
            """,
            (episode_id, source_id, episode_id, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
               status, word_count, created_at, updated_at)
            VALUES (?, ?, 'fixture', ?, ?, 'fetched', ?, ?, ?)
            """,
            (
                transcript_id,
                episode_id,
                str(segment_path.relative_to(self.root)),
                sha_bytes(segment_text.encode()),
                len(segment_text.split()),
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index,
               start_char, end_char, text_path, text_sha256, word_count,
               created_at)
            VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?)
            """,
            (
                segment_id,
                transcript_id,
                episode_id,
                source_id,
                len(segment_text),
                str(segment_path.relative_to(self.root)),
                sha_bytes(segment_text.encode()),
                len(segment_text.split()),
                timestamp,
            ),
        )
        label_run_id = f"run-{segment_id}"
        payload = {
            "label_pack": FROZEN_LABEL_PACK,
            "model": episode_batch.MODEL,
            "label_run_id": label_run_id,
            "output_path": str(output_path.resolve()),
            "prompt_path": str((self.root / "prompt.md").resolve()),
        }
        cursor = connection.execute(
            """
            INSERT INTO jobs
              (lane, job_type, target_id, payload_json, status, priority,
               attempts, max_attempts, lease_owner, leased_until, dedupe_key,
               created_at, updated_at)
            VALUES (?, 'label_segment', ?, ?, 'claimed', 1, 1, 3,
                    'fixture-worker', '2099-01-01T00:00:00+00:00', ?, ?, ?)
            """,
            (
                FROZEN_QUEUE_LANE,
                segment_id,
                canonical(payload),
                f"submission-{segment_id}",
                timestamp,
                timestamp,
            ),
        )
        job_id = int(cursor.lastrowid)
        connection.execute(
            """
            INSERT INTO label_runs
              (id, job_id, segment_id, label_pack, model, prompt_path,
               output_path, status, claimed_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'claimed', ?, ?, ?)
            """,
            (
                label_run_id,
                job_id,
                segment_id,
                FROZEN_LABEL_PACK,
                episode_batch.MODEL,
                payload["prompt_path"],
                str(output_path.resolve()),
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        connection.commit()
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="fixture-worker",
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            output_root=self.root / "sqlite-production",
        )
        item = QueueItem(
            job_id=job_id,
            segment_id=segment_id,
            episode_id=episode_id,
            segment_index=0,
            segment_text=segment_text,
            label_run_id=label_run_id,
            output_path=str(output_path.resolve()),
            segment_text_sha256=sha_bytes(segment_text.encode()),
        )
        return connection, queue, item, output_path

    def _sqlite_source_queue_fixture(self, specifications):
        fixture_index = len(list(self.root.glob("source-queue-*.sqlite")))
        database = self.root / f"source-queue-{fixture_index}.sqlite"
        connection = factory_db.connect(database)
        factory_db.init_db(connection)
        timestamp = "2026-07-18T00:00:00+00:00"
        episode_id = "ep-a"
        source_id = f"source-sqlite-{fixture_index}"
        connection.execute(
            """
            INSERT INTO sources
              (id, name, policy, transcript_policy, enabled, metadata_json,
               created_at, updated_at)
            VALUES (?, 'SQLite Fixture Source', 'private_analysis_only',
                    'creator_rss_transcripts_only', 1, '{}', ?, ?)
            """,
            (source_id, timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, published_at, created_at, updated_at)
            VALUES (?, ?, ?, 'SQLite Fixture Episode', '2026-07-18', ?, ?)
            """,
            (episode_id, source_id, f"guid-{fixture_index}", timestamp, timestamp),
        )
        records = []
        for segment_index, specification in enumerate(specifications):
            segment_id = str(specification["segment_id"])
            segment_text = str(specification["segment_text"])
            transcript_text = str(
                specification.get(
                    "transcript_text", f"Header.\n{segment_text}\nFooter.\n"
                )
            )
            cleaned = specification.get("cleaned", True) is True
            transcript_id = f"tr-sqlite-{fixture_index}-{segment_index}"
            preparation_id = f"prep-sqlite-{fixture_index}-{segment_index}"
            raw_path = (
                self.root
                / "corpus"
                / "raw"
                / f"{transcript_id}.txt"
            )
            cleaned_path = (
                self.root
                / "corpus"
                / "prepared"
                / f"{transcript_id}.txt"
            )
            segment_path = (
                self.root
                / "corpus"
                / "segments"
                / f"{segment_id}.txt"
            )
            for path in (raw_path, cleaned_path, segment_path):
                path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(transcript_text, encoding="utf-8")
            if cleaned:
                cleaned_path.write_text(transcript_text, encoding="utf-8")
            segment_path.write_text(segment_text, encoding="utf-8")
            start_char = transcript_text.index(segment_text)
            end_char = start_char + len(segment_text)
            raw_sha256 = sha_bytes(raw_path.read_bytes())
            cleaned_sha256 = (
                sha_bytes(cleaned_path.read_bytes()) if cleaned else None
            )
            segment_sha256 = sha_bytes(segment_path.read_bytes())
            connection.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
                   status, word_count, created_at, updated_at)
                VALUES (?, ?, 'fixture', ?, ?, 'fetched', ?, ?, ?)
                """,
                (
                    transcript_id,
                    episode_id,
                    str(raw_path.relative_to(self.root)),
                    raw_sha256,
                    len(transcript_text.split()),
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO transcript_preparations
                  (id, transcript_id, artifact_type, status, cleaned_text_path,
                   cleaned_text_sha256, original_word_count,
                   substantive_word_count, boilerplate_word_count,
                   boilerplate_ratio, speaker_turn_count, quality_score,
                   notes_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 0.5, '{}', ?, ?)
                """,
                (
                    preparation_id,
                    transcript_id,
                    specification.get("artifact_type", "dialogue_transcript"),
                    specification.get("status", "prepared"),
                    str(cleaned_path.relative_to(self.root)) if cleaned else None,
                    cleaned_sha256,
                    len(transcript_text.split()),
                    specification.get(
                        "substantive_word_count", len(segment_text.split())
                    ),
                    specification.get("boilerplate_ratio", 0.0),
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index,
                   start_char, end_char, text_path, text_sha256, word_count,
                   created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment_id,
                    transcript_id,
                    episode_id,
                    source_id,
                    segment_index,
                    start_char,
                    end_char,
                    str(segment_path.relative_to(self.root)),
                    segment_sha256,
                    len(segment_text.split()),
                    timestamp,
                ),
            )
            cursor = connection.execute(
                """
                INSERT INTO jobs
                  (lane, job_type, target_id, payload_json, status, priority,
                   attempts, max_attempts, dedupe_key, created_at, updated_at)
                VALUES (?, 'label_segment', ?, ?, 'pending', ?, 0, 3, ?, ?, ?)
                """,
                (
                    FROZEN_QUEUE_LANE,
                    segment_id,
                    canonical(
                        {
                            "label_pack": FROZEN_LABEL_PACK,
                            "model": FROZEN_QUEUE_PAYLOAD_MODEL,
                        }
                    ),
                    segment_index + 1,
                    f"source-queue-{fixture_index}-{segment_id}",
                    timestamp,
                    timestamp,
                ),
            )
            records.append(
                {
                    "job_id": int(cursor.lastrowid),
                    "segment_id": segment_id,
                    "raw_path": raw_path,
                    "cleaned_path": cleaned_path if cleaned else None,
                    "segment_path": segment_path,
                    "preparation_id": preparation_id,
                }
            )
        connection.commit()
        output_root = self.root / f"source-queue-output-{fixture_index}"
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="fixture-worker",
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            output_root=output_root,
            source_loader=VerifiedSourceLoader(connection, source_root=self.root),
        )
        return connection, queue, records

    def _verify_production_promotion(self, path, **kwargs):
        self.assertEqual(Path(path), self.production_promotion.resolve())
        self.assertEqual(
            Path(kwargs["frozen_configuration_path"]),
            self.frozen_configuration.resolve(),
        )
        self.assertEqual(
            Path(kwargs["episode_context_contract_path"]),
            self.context_contract.resolve(),
        )
        self.assertEqual(
            Path(kwargs["runtime_authorization_path"]),
            self.context_runtime.resolve(),
        )
        self.assertEqual(
            Path(kwargs["live_completion_receipt_path"]),
            self.context_completion.resolve(),
        )
        context_loaded = copy.deepcopy(self.fake_context_loaded)
        context_loaded["configuration"]["label_pack"] = self.context_label_pack
        return {
            "path": self.production_promotion.resolve(),
            "sha256": record(self.production_promotion)["sha256"],
            "authority": {"production_authorized": True},
            "frozen_configuration": json.loads(
                self.frozen_configuration.read_text(encoding="utf-8")
            ),
            "context_loaded": context_loaded,
            "runtime": copy.deepcopy(self.fake_context_runtime),
            "live_completion": json.loads(
                self.context_completion.read_text(encoding="utf-8")
            ),
            "database_current": (
                kwargs.get("queue") is not None
                and self.promotion_database_current
            ),
        }

    def _bind_test_context_authority(self, queue, **kwargs):
        self.assertIsInstance(queue, SQLiteWorkerQueue)
        self.assertEqual(
            Path(kwargs["context_contract_path"]), self.context_contract.resolve()
        )
        return SimpleNamespace(conn=queue.conn)

    def _verify_context_artifact(
        self, *, artifact_path, episode_id, label_pack, model, loaded
    ):
        self.assertEqual(label_pack, FROZEN_LABEL_PACK)
        self.assertEqual(model, FROZEN_QUEUE_PAYLOAD_MODEL)
        artifact = self.context_artifacts[episode_id]
        self.assertEqual(Path(artifact_path), Path(artifact["path"]))
        payload = json.loads(Path(artifact["path"]).read_text(encoding="utf-8"))
        lineage_path = Path(artifact["path"] + ".app-server-lineage.json")
        if not lineage_path.exists():
            lineage_path.write_text("{}\n", encoding="utf-8")
        return {
            "artifact": payload,
            "lineage": {"state": "verified"},
            "lineage_path": lineage_path,
        }

    def _write_valid_evaluation_receipt(self):
        self.evaluation_root.mkdir()
        winner_system_id = json.loads(
            self.frozen_configuration.read_text(encoding="utf-8")
        )["winner_system_id"]
        common = {
            "evaluation_id": "evaluation-1",
            "runtime_lock_sha256": "1" * 64,
            "frozen_configuration_sha256": sha_bytes(
                self.frozen_configuration.read_bytes()
            ),
            "reference_sha256": "3" * 64,
            "holdout_manifest_sha256": "4" * 64,
        }
        payloads = {
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
                "winner_system_id": winner_system_id,
                "window_count": 4,
                "context_chars": 900,
            },
            "untouched_holdout": {
                "untouched_holdout": True,
                "holdout_passed": True,
                "semantic_noninferior_or_better": True,
                "winner_system_id": winner_system_id,
                "baseline_system_id": "baseline-1",
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
                "winner_system_id": winner_system_id,
                "baseline_system_id": "baseline-1",
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
        for role, payload in payloads.items():
            path = self.evaluation_root / f"{role}.json"
            path.write_text(pretty({"artifact_role": role, **common, **payload}))
            bindings[role] = {"path": path.name, "sha256": sha_bytes(path.read_bytes())}
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

    def write_contract(self, **overrides):
        configuration = {
            "frozen_configuration_sha256": sha_bytes(
                self.frozen_configuration.read_bytes()
            ),
            "winner_system_id": json.loads(
                self.frozen_configuration.read_text(encoding="utf-8")
            )["winner_system_id"],
            "model": episode_batch.MODEL,
            "effort": episode_batch.EFFORT,
            "batch_size": 3,
            "thread_mode": "same_thread",
            "timeout_seconds": 600,
            "label_pack": FROZEN_LABEL_PACK,
            "lane": FROZEN_QUEUE_LANE,
            "queue_payload_model": "gpt-5.5",
            "window_count": 4,
            "context_chars": 900,
            **overrides,
        }
        payload = {
            "schema_version": CONTRACT_VERSION,
            "state": "frozen",
            "thread_id": "evaluation-thread-fixture",
            "cutover_id": "cutover-fixture",
            "transport": TRANSPORT,
            "auth": {
                "type": "chatgpt",
                "plan_type": "pro",
                "api_key_billing_allowed": False,
                "raw_session_token_access_allowed": False,
            },
            "evaluation": {
                "root": str(self.evaluation_root.resolve()),
                "receipt": record(self.evaluation_receipt),
                "evaluation_id": self.verified_evaluation["evaluation_id"],
                "frozen_configuration_sha256": configuration[
                    "frozen_configuration_sha256"
                ],
                "winner_system_id": configuration["winner_system_id"],
                "verified_artifact_hashes": self.verified_evaluation["artifact_hashes"],
            },
            "configuration": configuration,
            "artifacts": {
                "codex_binary": record(self.binary),
                "production_runner": record(
                    Path(__file__).parents[1]
                    / "research_factory"
                    / "app_server_production_runner.py"
                ),
                "canonical_production_contract": record(
                    Path(production_contract.__file__).resolve()
                ),
                "production_promotion_authority": record(
                    self.production_promotion
                ),
                "frozen_configuration": record(self.frozen_configuration),
                "episode_batch_adapter": record(
                    Path(episode_batch.__file__).resolve()
                ),
                "episode_context_verifier": record(
                    Path(context_runner.__file__).resolve()
                ),
                "windowing_source": record(Path(efficient_backtest.__file__).resolve()),
            },
            "episode_context": {
                "current_phase": "label_segment",
                "required_predecessor_phase": "episode_context",
                "context_contract": record(self.context_contract),
                "runtime_authorization": record(self.context_runtime),
                "provision_execution_receipt": record(
                    self.context_provision_execution
                ),
                "live_completion_receipt": record(self.context_completion),
                "completion_verifier": (
                    "verify_live_episode_context_completion_receipt"
                ),
                "artifact_verifier": "_verify_managed_context_artifact",
                "database_currentness": (
                    "same_sqlite_connection_queue_authority_at_execution"
                ),
                "overall_queue_completion_authorized": False,
            },
            "execution": {
                "persistent_app_server_processes": 1,
                "thread_mode": (
                    "one_same_thread_per_episode"
                    if configuration["thread_mode"] == "same_thread"
                    else "one_ephemeral_thread_per_batch"
                ),
                "same_thread_persistence": (
                    "durable_non_ephemeral_until_episode_drained"
                ),
                "same_thread_recovery": (
                    "exact_thread_resume_no_completed_turn_replay"
                ),
                "same_thread_archive": (
                    "after_durable_episode_completion_receipt"
                ),
                "new_thread_persistence": "ephemeral_per_batch",
                "claim_scope": "one_exact_episode_batch",
                "queue_lane": FROZEN_QUEUE_LANE,
                "payload_label_pack_match": "exact",
                "payload_model_policy": "null_or_exact_frozen_queue_model",
                "capacity_admission": production_contract.capacity_admission_contract(),
                "failed_label_reconciliation": FAILED_LABEL_RECONCILIATION_POLICY,
                "semantic_retry_count": 0,
                "ambiguous_retry_allowed": False,
                "deterministic_semantic_pruning": False,
            },
        }
        self.contract.write_text(pretty(payload), encoding="utf-8")

    def configure_arm(self, *, batch_size, thread_mode):
        self._write_canonical_frozen_configuration(
            batch_size=batch_size,
            thread_mode=thread_mode,
        )
        shutil.rmtree(self.evaluation_root)
        self.evaluation_receipt = self._write_valid_evaluation_receipt()
        self.verified_evaluation = verify_evaluation_receipt(
            self.evaluation_receipt, self.evaluation_root
        )
        self.fake_context_loaded["verified_evaluation"] = self.verified_evaluation
        self.fake_context_loaded["contract"]["lineage"]["evaluation_receipt"] = (
            record(self.evaluation_receipt)
        )
        self.write_contract(batch_size=batch_size, thread_mode=thread_mode)

    def factory(self, _binary):
        return FakeClient()

    def test_contract_binds_strict_evaluation_context_and_adapter(self):
        loaded = load_contract(self.contract)
        self.assertEqual(
            loaded["frozen_configuration"]["winner_system_id"],
            production_contract.selected_variant_id(
                batch_size=3, thread_mode="same_thread"
            ),
        )
        self.assertEqual(loaded["episode_context"]["context_artifact_index_sha256"], self.context_index_sha)
        self.assertTrue(loaded["episode_context_fixture_ready"])
        self.assertFalse(loaded["episode_context_ready"])
        self.assertEqual(ALLOWED_BATCH_SIZES, {3, 5, 8})
        self.assertEqual(ALLOWED_THREAD_MODES, {"new_thread", "same_thread"})
        self.context_promotion_mock.assert_called_once()
        payload = json.loads(self.contract.read_text())
        payload["artifacts"]["episode_batch_adapter"]["sha256"] = "0" * 64
        self.contract.write_text(pretty(payload))
        with self.assertRaisesRegex(ProductionRunnerError, "episode_batch_adapter artifact drifted"):
            load_contract(self.contract)

    def test_strict_evaluation_winner_drift_is_rejected(self):
        development = self.evaluation_root / "development_freeze.json"
        payload = json.loads(development.read_text())
        payload["winner_system_id"] = "wrong"
        development.write_text(pretty(payload))
        with self.assertRaisesRegex(ProductionRunnerError, "strict evaluation receipt"):
            load_contract(self.contract)

    def test_batch_size_outside_frozen_set_is_rejected(self):
        self.write_contract(batch_size=4)
        with self.assertRaisesRegex(ProductionRunnerError, "contract values drifted"):
            load_contract(self.contract)

    def test_window_parameters_must_match_verified_development_winner(self):
        self.write_contract(window_count=1, context_chars=0)
        with self.assertRaisesRegex(
            ProductionRunnerError,
            "configuration does not match accepted winner",
        ):
            load_contract(self.contract)

    def test_queue_payload_model_is_frozen_separately_from_semantic_winner(self):
        loaded = load_contract(self.contract)
        self.assertEqual(
            loaded["contract"]["configuration"]["queue_payload_model"],
            FROZEN_QUEUE_PAYLOAD_MODEL,
        )
        self.assertNotEqual(FROZEN_QUEUE_PAYLOAD_MODEL, episode_batch.MODEL)
        self.write_contract(queue_payload_model=episode_batch.MODEL)
        with self.assertRaisesRegex(ProductionRunnerError, "contract values drifted"):
            load_contract(self.contract)

    def test_v1_label_pack_is_rejected_before_context_queue_or_client(self):
        self.write_contract(label_pack="ai_discourse_v1")
        with self.assertRaisesRegex(ProductionRunnerError, "contract values drifted"):
            load_contract(self.contract)
        self.context_promotion_mock.assert_not_called()
        self.assertEqual(FakeClient.instances, [])

    def test_context_completion_label_pack_must_match_frozen_v31_lane(self):
        self.context_label_pack = "ai_discourse_v1"
        with self.assertRaisesRegex(
            ProductionRunnerError,
            "episode-context lane, label-pack, or evaluation lineage drifted",
        ):
            load_contract(self.contract)

    async def test_dry_run_prepares_exact_semantic_surface_without_mutation(self):
        queue = FakeQueue(self.items)
        clients = []
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "dry-run",
            queue=queue,
            limit=7,
            dry_run=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(receipt["state"], "dry_run")
        self.assertEqual(receipt["planned_batch_count"], 1)
        self.assertTrue(receipt["episode_context_verified"])
        self.assertEqual(
            receipt["semantic_surface"], episode_batch.CANDIDATE_SYSTEM_ID
        )
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertFalse(receipt["production_mutated"])
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(clients, [])

    async def test_failed_only_scope_cannot_mint_no_eligible_pass(self):
        queue = UnresolvedScopedQueue([], unresolved_after_pending=1)
        clients = []
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "failed-only-no-pass",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(
            receipt["terminal_reason"],
            "unresolved_scoped_jobs_require_reconciliation",
        )
        self.assertEqual(receipt["remaining_unresolved_scoped_job_count"], 1)
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertEqual(clients, [])

    async def test_queue_lane_drift_fails_before_preview_claim_or_client(self):
        queue = FakeQueue(self.items, lane="local")
        clients = []
        with self.assertRaisesRegex(
            ProductionRunnerError,
            "queue lane does not match the frozen production contract",
        ):
            await run_production_queue(
                contract_path=self.contract,
                output_root=self.root / "wrong-lane",
                queue=queue,
                limit=1,
                dry_run=True,
                client_factory=lambda _binary: clients.append(True),
            )
        self.assertEqual(queue.preview_calls, 0)
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(clients, [])

    async def test_fixture_run_uses_context_dynamic_schema_capacity_and_one_thread(self):
        queue = FakeQueue(self.items)
        capacity = FakeCapacityProvider(queue)
        with patch.dict(os.environ, {}, clear=True):
            receipt = await run_production_queue(
                contract_path=self.contract,
                output_root=self.root / "run",
                queue=queue,
                limit=7,
                dry_run=False,
                fixture_mode=True,
                client_factory=self.factory,
                capacity_provider=capacity,
            )
        self.assertEqual(receipt["state"], "passed")
        self.assertEqual(receipt["semantic_model_call_count"], 3)
        self.assertEqual(receipt["episode_thread_count"], 2)
        self.assertEqual(receipt["submitted_segment_count"], 7)
        self.assertEqual(receipt["usage"]["total_tokens"], 375)
        self.assertEqual(receipt["usage"]["cached_input_tokens"], 20)
        self.assertTrue(receipt["production_mutated"])
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(len(FakeClient.instances[0].started_threads), 2)
        self.assertTrue(
            all(not thread.ephemeral for thread in FakeClient.instances[0].started_threads)
        )
        self.assertEqual(len(FakeClient.instances[0].archived_threads), 2)
        self.assertEqual(len(receipt["episode_thread_archives"]), 2)
        self.assertEqual(len(capacity.calls), 3)
        self.assertEqual(queue.events[:3], ["preview", "capacity", "claim"])
        for request_path in (self.root / "run" / "turns").rglob(
            "prepared-request.private.json"
        ):
            request = json.loads(request_path.read_text())
            episode_batch.validate_prepared_request(request)
            self.assertEqual(
                request["candidate_system_id"], episode_batch.CANDIDATE_SYSTEM_ID
            )
            self.assertFalse(request["semantic_postprocessing"])
            self.assertNotIn("max_events_per_segment", request)
            self.assertNotIn("projection_schema", request)
            self.assertTrue(
                all(
                    "segment_quality" in segment
                    for segment in request["private_input"]["segments"]
                )
            )
            self.assertIn("speaker_map", request["episode_context"])
            context = json.loads(
                (request_path.parent / "episode-context-binding.private.json").read_text()
            )
            self.assertEqual(len(context["loaded_semantic_fields"]), 7)
            self.assertEqual(context["context_artifact_index_sha256"], self.context_index_sha)
        terminals = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.root / "run" / "turns").rglob("terminal.json")
        ]
        self.assertEqual(len(terminals), 3)
        for terminal in terminals:
            self.assertIn("source_packet", terminal)
            self.assertIn("canonical_labels", terminal)
            self.assertIn("evidence_provenance", terminal)
            self.assertIn("semantic_fidelity", terminal)
            self.assertNotIn("normalized", terminal)
            self.assertNotIn("diagnostics", terminal)
            labels = json.loads(Path(terminal["canonical_labels"]["path"]).read_text())
            provenance = json.loads(
                Path(terminal["evidence_provenance"]["path"]).read_text()
            )
            fidelity = json.loads(
                Path(terminal["semantic_fidelity"]["path"]).read_text()
            )
            self.assertEqual(len(labels), terminal["turn_batch_size"])
            self.assertTrue(
                all(label["schema_version"] == FROZEN_LABEL_PACK for label in labels)
            )
            self.assertTrue(provenance["unit_ids_removed_from_canonical_labels"])
            self.assertTrue(fidelity["all_emitted_semantic_values_preserved"])
        runtime = json.loads((self.root / "run" / "runtime-config.json").read_text())
        telemetry = json.loads((self.root / "run" / "usage-telemetry.json").read_text())
        self.assertEqual(runtime["schema_version"], RUNTIME_CONFIG_VERSION)
        self.assertEqual(
            runtime["context_control_overlay_sha256"],
            json.loads(self.frozen_configuration.read_text())[
                "runtime_binding"
            ]["context_control_overlay_sha256"],
        )
        overlay_path = self.root / "run" / "context-control-overlay.json"
        self.assertEqual(runtime["context_control_overlay"], record(overlay_path))
        self.assertEqual(telemetry["schema_version"], USAGE_TELEMETRY_VERSION)
        self.assertEqual(
            telemetry["semantic_surface"], episode_batch.CANDIDATE_SYSTEM_ID
        )

    async def test_same_thread_limit_does_not_split_or_underfill_started_episode(self):
        queue = FakeQueue(self.items[:5])
        capacity = FakeCapacityProvider(queue)
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "same-thread-drain",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=capacity,
        )
        self.assertEqual(receipt["state"], "passed")
        self.assertEqual(receipt["submitted_segment_count"], 5)
        self.assertEqual(receipt["semantic_model_call_count"], 2)
        self.assertEqual(receipt["episode_thread_count"], 1)
        self.assertEqual(queue.pending, [])
        self.assertEqual(len(FakeClient.instances), 1)
        client = FakeClient.instances[0]
        self.assertEqual(len(client.started_threads), 1)
        self.assertEqual(client.turn_batch_sizes, [3, 2])
        self.assertEqual(
            {thread_id for thread_id, _ in client.turns},
            {client.started_threads[0].thread_id},
        )
        self.assertFalse(client.started_threads[0].ephemeral)
        self.assertEqual(
            client.archived_threads, [client.started_threads[0].thread_id]
        )
        self.assertEqual(len(receipt["episode_thread_archives"]), 1)
        self.assertEqual([remaining for _, remaining in capacity.calls], [2, 1])

    async def test_same_thread_capacity_checkpoint_resumes_exact_durable_thread(self):
        run_root = self.root / "same-thread-crash"
        queue = FakeQueue(self.items[:4])
        admitted = FakeCapacityProvider(queue)

        def capacity_then_stop(*, request, remaining_batch_count, timeout_seconds):
            if admitted.calls:
                raise ProductionRunnerWaiting("fixture capacity stop before second batch")
            return admitted(
                request=request,
                remaining_batch_count=remaining_batch_count,
                timeout_seconds=timeout_seconds,
            )

        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=3,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=capacity_then_stop,
        )
        self.assertEqual(first["state"], "waiting")
        self.assertEqual(first["semantic_model_call_count"], 1)
        self.assertEqual(
            first["terminal_reason"],
            "same_thread_exact_resume_checkpoint_no_replay",
        )
        self.assertEqual(len(queue.pending), 1)
        self.assertFalse((run_root / "run-receipt.json").exists())
        first_client = FakeClient.instances[0]
        self.assertEqual(len(first_client.started_threads), 1)
        durable_thread_id = first_client.started_threads[0].thread_id
        self.assertFalse(first_client.started_threads[0].ephemeral)
        self.assertEqual(len(first_client.turns), 1)
        FakeClient.instances.clear()
        recovery_capacity = FakeCapacityProvider(queue)
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=3,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=recovery_capacity,
        )
        self.assertEqual(recovered["state"], "passed")
        self.assertEqual(recovered["semantic_model_call_count"], 2)
        self.assertEqual(recovered["submitted_segment_count"], 4)
        self.assertEqual(queue.pending, [])
        self.assertEqual(len(FakeClient.instances), 1)
        resumed_client = FakeClient.instances[0]
        self.assertEqual(resumed_client.started_threads, [])
        self.assertEqual(len(resumed_client.resumed_threads), 1)
        self.assertEqual(
            resumed_client.resumed_threads[0].thread_id, durable_thread_id
        )
        self.assertEqual(
            resumed_client.resumed_threads[0].prior_completed_turn_ids,
            (first_client.turns[0][1],),
        )
        self.assertEqual(
            [thread_id for thread_id, _turn_id in resumed_client.turns],
            [durable_thread_id],
        )
        self.assertEqual(resumed_client.archived_threads, [durable_thread_id])
        self.assertEqual(recovery_capacity.calls[0][1], 1)
        resumed_launch = next(
            launch
            for launch in (
                json.loads(path.read_text(encoding="utf-8"))
                for path in (run_root / "turns").rglob("launch.json")
            )
            if launch["thread_resumed"] is True
        )
        self.assertEqual(resumed_launch["managed_chatgpt_plan_type"], "pro")
        self.assertEqual(
            resumed_launch["context_control_overlay_sha256"],
            episode_batch.sha256_text(
                canonical(episode_batch.verified_context_control_overlay())
            ),
        )
        self.assertRegex(
            resumed_launch["resume_source_packet_records_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertRegex(resumed_launch["prior_usage_sha256"], r"^[0-9a-f]{64}$")

    async def test_same_thread_resume_rejects_persisted_path_drift_before_claim(self):
        run_root = self.root / "same-thread-persisted-path-drift"
        queue = FakeQueue(self.items[:4])
        admitted = FakeCapacityProvider(queue)

        def capacity_then_stop(*, request, remaining_batch_count, timeout_seconds):
            if admitted.calls:
                raise ProductionRunnerWaiting("fixture stop before residual turn")
            return admitted(
                request=request,
                remaining_batch_count=remaining_batch_count,
                timeout_seconds=timeout_seconds,
            )

        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=3,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=capacity_then_stop,
        )
        self.assertEqual(first["semantic_model_call_count"], 1)
        durable_thread_id = FakeClient.instances[0].started_threads[0].thread_id
        FakeClient.durable_threads[durable_thread_id][
            "persisted_path_sha256"
        ] = "f" * 64
        FakeClient.instances.clear()
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=3,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(recovered["state"], "waiting")
        self.assertEqual(
            recovered["terminal_reason"],
            "same_thread_exact_resume_checkpoint_no_replay",
        )
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        self.assertEqual(len(queue.pending), 1)
        self.assertEqual(queue.claim_calls, 2)
        self.assertEqual(FakeClient.instances[0].turns, [])

    def test_production_submission_rejects_unresolved_evidence_without_repair(self):
        example = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "label_packs/ai_discourse_v3_1/examples.json"
            ).read_text(encoding="utf-8")
        )[0]
        output = copy.deepcopy(example["output"])
        output["discourse_events"][0]["evidence"] = (
            "unsupported generated evidence that is absent from the segment"
        )
        output["discourse_events"][0]["evidence_start"] = 0
        output["discourse_events"][0]["evidence_end"] = len(
            output["discourse_events"][0]["evidence"]
        )
        original = copy.deepcopy(output)
        connection, queue, item, output_path = self._sqlite_submission_fixture(output)
        try:
            with patch(
                "research_factory.worker.corpus_dir",
                return_value=self.root / "corpus",
            ):
                with self.assertRaisesRegex(
                    ProductionRunnerWaiting,
                    "failed exact worker validation",
                ):
                    queue.submit(item, output_path)
            self.assertEqual(json.loads(output_path.read_text()), original)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM labels").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM jobs WHERE id = ?", (item.job_id,)
                ).fetchone()["status"],
                "claimed",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM label_runs WHERE id = ?",
                    (item.label_run_id,),
                ).fetchone()["status"],
                "claimed",
            )
        finally:
            connection.close()

    def test_production_submission_persists_valid_projected_json_exactly(self):
        example = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "label_packs/ai_discourse_v3_1/examples.json"
            ).read_text(encoding="utf-8")
        )[0]
        output = copy.deepcopy(example["output"])
        connection, queue, item, output_path = self._sqlite_submission_fixture(output)
        try:
            with patch(
                "research_factory.worker.corpus_dir",
                return_value=self.root / "corpus",
            ):
                submission = queue.submit(item, output_path)
            persisted = json.loads(
                connection.execute(
                    "SELECT output_json FROM labels WHERE id = ?",
                    (submission["label_id"],),
                ).fetchone()["output_json"]
            )
            self.assertEqual(canonical(persisted), canonical(output))
            self.assertEqual(json.loads(output_path.read_text()), output)
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM jobs WHERE id = ?", (item.job_id,)
                ).fetchone()["status"],
                "completed",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM label_runs WHERE id = ?",
                    (item.label_run_id,),
                ).fetchone()["status"],
                "completed",
            )
            for table in (
                "claims",
                "coded_observations",
                "entity_mentions",
                "concept_aliases",
                "discourse_events",
                "concept_candidates",
            ):
                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
                    0,
                    f"canonical production invented derivative rows in {table}",
                )
        finally:
            connection.close()

    def test_legacy_worker_submission_derives_semantics_by_default(self):
        example = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "label_packs/ai_discourse_v3_1/examples.json"
            ).read_text(encoding="utf-8")
        )[0]
        output = copy.deepcopy(example["output"])
        connection, _queue, item, output_path = self._sqlite_submission_fixture(output)
        try:
            with patch(
                "research_factory.worker.corpus_dir",
                return_value=self.root / "corpus",
            ):
                worker_module.submit_label_output(
                    connection,
                    job_id=item.job_id,
                    output_json_path=output_path,
                    worker_id="fixture-worker",
                    repair_output=False,
                )
            self.assertGreater(
                connection.execute("SELECT COUNT(*) FROM discourse_events").fetchone()[0],
                0,
            )
            self.assertGreater(
                connection.execute("SELECT COUNT(*) FROM concept_candidates").fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_real_sqlite_source_quality_uses_exact_db_values_and_raw_fallback(self):
        specifications = [
            {
                "segment_id": "seg-quality-zero",
                "segment_text": "Host: Explicit zero must remain zero.",
                "substantive_word_count": 0,
                "boilerplate_ratio": 0.0,
            },
            {
                "segment_id": "seg-quality-low-signal",
                "segment_text": "Host: Sparse source remains available.",
                "cleaned": False,
                "status": "low_signal",
                "substantive_word_count": 5,
                "boilerplate_ratio": 0.0,
            },
            {
                "segment_id": "seg-quality-low-threshold",
                "segment_text": "Host: Ratio below threshold.",
                "boilerplate_ratio": 0.1499,
            },
            {
                "segment_id": "seg-quality-medium-threshold",
                "segment_text": "Host: Ratio at medium threshold.",
                "boilerplate_ratio": 0.15,
            },
            {
                "segment_id": "seg-quality-high-threshold",
                "segment_text": "Host: Ratio at high threshold.",
                "boilerplate_ratio": 0.35,
            },
            {
                "segment_id": "seg-quality-boilerplate",
                "segment_text": "Host: Artifact type forces high risk.",
                "artifact_type": "boilerplate",
                "boilerplate_ratio": 0.0,
            },
        ]
        connection, queue, _records = self._sqlite_source_queue_fixture(
            specifications
        )
        try:
            items = queue.preview_episode_batch(limit=8)
            self.assertEqual([item.segment_id for item in items], [
                specification["segment_id"] for specification in specifications
            ])
            by_id = {item.segment_id: item for item in items}
            self.assertEqual(
                by_id["seg-quality-zero"].segment_quality[
                    "substantive_word_count"
                ],
                0,
            )
            self.assertEqual(
                by_id["seg-quality-low-signal"].expected_source_binding[
                    "transcript_source_basis"
                ]["source_basis"],
                "raw_transcript_for_low_signal",
            )
            self.assertEqual(
                {
                    segment_id: by_id[segment_id].segment_quality[
                        "boilerplate_risk"
                    ]
                    for segment_id in by_id
                },
                {
                    "seg-quality-zero": "low",
                    "seg-quality-low-signal": "high",
                    "seg-quality-low-threshold": "low",
                    "seg-quality-medium-threshold": "medium",
                    "seg-quality-high-threshold": "high",
                    "seg-quality-boilerplate": "high",
                },
            )
            prepared = production_runner._prepare_episode_batch(  # noqa: SLF001
                load_contract(self.contract), items[:2]
            )
            self.assertEqual(
                [
                    segment["segment_quality"]
                    for segment in prepared["request"]["private_input"]["segments"]
                ],
                [item.segment_quality for item in items[:2]],
            )
            self.assertEqual(
                [
                    segment["expected_source_binding"]
                    for segment in prepared["source_packet"]["segments"]
                ],
                [item.expected_source_binding for item in items[:2]],
            )
        finally:
            connection.close()

    def test_real_sqlite_source_drift_fails_before_claim_and_during_recovery(self):
        for drift_kind in ("offset", "segment_hash"):
            with self.subTest(drift_kind=drift_kind):
                connection, queue, records = self._sqlite_source_queue_fixture(
                    [
                        {
                            "segment_id": f"seg-drift-{drift_kind}",
                            "segment_text": "Host: Exact verified source bytes.",
                        }
                    ]
                )
                try:
                    if drift_kind == "offset":
                        connection.execute(
                            "UPDATE segments SET start_char = start_char + 1, "
                            "end_char = end_char + 1"
                        )
                        connection.commit()
                    else:
                        records[0]["segment_path"].write_text(
                            "tampered segment bytes", encoding="utf-8"
                        )
                    with self.assertRaisesRegex(
                        ProductionRunnerWaiting,
                        "source integrity failed before claim or transport",
                    ):
                        queue.preview_episode_batch(limit=1)
                    self.assertEqual(
                        connection.execute(
                            "SELECT status FROM jobs WHERE id = ?",
                            (records[0]["job_id"],),
                        ).fetchone()["status"],
                        "pending",
                    )
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM label_runs").fetchone()[0],
                        0,
                    )
                finally:
                    connection.close()

        connection, queue, records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-raw-lineage-drift",
                    "segment_text": "Host: Cleaned source with bound raw lineage.",
                }
            ]
        )
        try:
            items = queue.preview_episode_batch(limit=1)
            prepared = production_runner._prepare_episode_batch(  # noqa: SLF001
                load_contract(self.contract), items
            )
            request_path = self.root / "raw-drift-prepared-request.json"
            request_path.write_text(pretty(prepared["request"]), encoding="utf-8")
            records[0]["raw_path"].write_text(
                "tampered raw transcript lineage", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                ProductionRunnerWaiting, "raw transcript lineage failed"
            ):
                queue.claim_episode_batch(
                    items,
                    prepared_request_path=request_path,
                    claim_receipt_path=self.root / "raw-drift-claim.json",
                )
            with self.assertRaisesRegex(
                ProductionRunnerWaiting, "raw transcript lineage failed"
            ):
                queue.verify_source_packet(prepared["source_packet"])
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM jobs WHERE id = ?",
                    (records[0]["job_id"],),
                ).fetchone()["status"],
                "pending",
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM label_runs").fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_real_sqlite_coded_and_no_signal_project_full_canonical_outputs(self):
        example_text = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "label_packs/ai_discourse_v3_1/examples.json"
            ).read_text(encoding="utf-8")
        )[0]["input"]["text"]
        connection, queue, _records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-v31-coded",
                    "segment_text": example_text,
                    "substantive_word_count": 24,
                },
                {
                    "segment_id": "seg-v31-no-signal",
                    "segment_text": "Host: Welcome to the show. Guest: Thank you.",
                    "cleaned": False,
                    "status": "low_signal",
                    "substantive_word_count": 0,
                },
            ]
        )
        try:
            items = queue.preview_episode_batch(limit=3)
            prepared = production_runner._prepare_episode_batch(  # noqa: SLF001
                load_contract(self.contract), items
            )
            output = coded_and_no_signal_output(prepared["request"])
            original = copy.deepcopy(output)
            rows, projected = production_runner._validate_batch_output(  # noqa: SLF001
                output,
                request=prepared["request"],
                items=items,
            )
            self.assertEqual(output, original)
            self.assertEqual(
                [row["extraction_status"] for row in rows],
                ["coded", "no_signal"],
            )
            self.assertEqual(len(rows[0]["discourse_events"]), 2)
            self.assertEqual(rows[1]["discourse_events"], [])
            self.assertEqual(
                [row["segment_quality"] for row in rows],
                [item.segment_quality for item in items],
            )
            self.assertEqual(projected["fidelity"]["emitted_event_count"], 2)
            self.assertEqual(projected["fidelity"]["projected_event_count"], 2)
            self.assertTrue(
                projected["fidelity"]["all_emitted_semantic_values_preserved"]
            )
            self.assertTrue(
                projected["provenance"]["unit_ids_removed_from_canonical_labels"]
            )
            self.assertEqual(
                queue.verify_source_packet(prepared["source_packet"])["state"],
                "verified_current_source_packet",
            )
        finally:
            connection.close()

    async def test_new_thread_winner_uses_one_fresh_thread_per_batch(self):
        self.configure_arm(batch_size=3, thread_mode="new_thread")
        queue = FakeQueue(self.items)
        capacity = FakeCapacityProvider(queue)
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "new-thread-run",
            queue=queue,
            limit=7,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=capacity,
        )
        self.assertEqual(receipt["state"], "passed")
        self.assertEqual(receipt["thread_mode"], "new_thread")
        self.assertEqual(receipt["semantic_model_call_count"], 3)
        self.assertEqual(receipt["episode_thread_count"], 3)
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(len(FakeClient.instances[0].started_threads), 3)
        self.assertEqual(
            len({thread.thread_id for thread in FakeClient.instances[0].started_threads}),
            3,
        )
        telemetry = json.loads(
            (self.root / "new-thread-run" / "usage-telemetry.json").read_text()
        )
        self.assertEqual(telemetry["thread_mode"], "new_thread")

    def test_thread_lifecycle_contract_must_match_frozen_winner(self):
        self.configure_arm(batch_size=5, thread_mode="new_thread")
        payload = json.loads(self.contract.read_text())
        payload["execution"]["thread_mode"] = "one_same_thread_per_episode"
        self.contract.write_text(pretty(payload), encoding="utf-8")
        with self.assertRaisesRegex(
            ProductionRunnerError, "execution contract drifted"
        ):
            load_contract(self.contract)

    def test_default_client_injects_the_frozen_strict_config_overlay(self):
        loaded = load_contract(self.contract)
        overlay = _verified_context_control_overlay(loaded)
        sentinel = object()
        with patch.object(
            production_runner,
            "ProductionCodexAppServerClient",
            return_value=sentinel,
        ) as client:
            result = _default_client_factory(
                self.binary,
                config_overlay=overlay,
            )
        self.assertIs(result, sentinel)
        client.assert_called_once_with(
            config_overlay=overlay,
            command=[
                str(self.binary),
                "app-server",
                "--stdio",
                "--strict-config",
            ],
        )

    async def test_production_client_applies_exact_overlay_to_start_and_resume(self):
        overlay = _verified_context_control_overlay(load_contract(self.contract))
        client = production_runner.ProductionCodexAppServerClient(
            config_overlay=overlay,
            command=[str(self.binary), "app-server", "--stdio", "--strict-config"],
        )
        instruction_sources = episode_batch.expected_instruction_source_contract()[
            "effective_instruction_source_paths"
        ]
        transport = AsyncMock(
            side_effect=[{"instructionSources": instruction_sources}, {}]
        )
        with patch.object(
            episode_batch.codex_app_server.CodexAppServerClient,
            "_request",
            new=transport,
        ):
            await client._request("thread/start", {"model": episode_batch.MODEL})
            await client._request("thread/resume", {"threadId": "thread-fixture"})
        start_params = transport.await_args_list[0].args[1]
        resume_params = transport.await_args_list[1].args[1]
        for params in (start_params, resume_params):
            self.assertEqual(params["config"], overlay)
            self.assertEqual(params["personality"], "none")
            self.assertEqual(params["environments"], [])
            self.assertEqual(params["dynamicTools"], [])

    def test_context_overlay_drift_is_rejected_before_client_start(self):
        loaded = load_contract(self.contract)
        overlay = _verified_context_control_overlay(loaded)
        drifted = json.loads(json.dumps(overlay))
        drifted["project_doc_max_bytes"] = 1
        with patch.object(
            episode_batch,
            "verified_context_control_overlay",
            return_value=drifted,
        ):
            with self.assertRaisesRegex(
                ProductionRunnerError, "context contract drifted"
            ):
                _verified_context_control_overlay(loaded)

    async def test_instruction_source_drift_stops_before_semantic_turn(self):
        queue = FakeQueue(self.items[:1])
        capacity = FakeCapacityProvider(queue)
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "instruction-source-drift",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: FakeClient(
                instruction_source_drift=True
            ),
            capacity_provider=capacity,
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertTrue(receipt["production_mutated"])
        self.assertEqual(queue.pending, self.items[:1])
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(FakeClient.instances[0].turns, [])

    async def test_missing_capacity_waits_before_claim_or_turn(self):
        queue = FakeQueue(self.items[:1])
        output_root = self.root / "no-capacity"
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=output_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(FakeClient.instances[0].turns, [])
        checkpoints = list((output_root / "capacity-checkpoints").glob("*.json"))
        self.assertEqual(len(checkpoints), 1)
        checkpoint = json.loads(checkpoints[0].read_text(encoding="utf-8"))
        self.assertEqual(
            checkpoint["terminal_reason"],
            "capacity_admission_denied_before_claim_or_turn",
        )
        self.assertEqual(checkpoint["semantic_model_call_count"], 0)
        self.assertFalse(checkpoint["production_mutated"])
        self.assertEqual(list((output_root / "turns").rglob("terminal.json")), [])

    async def test_live_execution_remains_closed_without_current_promotion(self):
        connection, queue, _records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-live-closed",
                    "segment_text": "Host: Promotion currentness is mandatory.",
                }
            ]
        )
        self.promotion_database_current = False
        try:
            receipt = await run_production_queue(
                contract_path=self.contract,
                output_root=self.root / "live-closed",
                queue=queue,
                limit=1,
                dry_run=False,
                fixture_mode=False,
            )
        finally:
            connection.close()
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(
            receipt["terminal_reason"],
            "episode_context_production_contract_not_released",
        )
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertFalse(receipt["production_mutated"])

    async def test_partial_attempt_waits_without_claim_or_client_replay(self):
        run_root = self.root / "partial"
        turn_root = run_root / "turns" / "ep-a" / "batch"
        turn_root.mkdir(parents=True)
        (turn_root / "launch.json").write_text("{}", encoding="utf-8")
        queue = FakeQueue(self.items)
        clients = []
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=7,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["terminal_reason"], "partial_turn_preserved_without_replay")
        self.assertEqual(receipt["semantic_attempt_count"], 1)
        self.assertIsNone(receipt["semantic_model_call_count"])
        self.assertFalse(receipt["accounting_complete"])
        self.assertFalse(receipt["partial_turns"][0]["lineage_verified"])
        self.assertTrue(receipt["production_mutated"])
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(clients, [])

    async def test_valid_completed_sidecar_without_terminal_recovers_accounting_only(self):
        run_root = self.root / "valid-partial"
        first_queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=first_queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(first_queue),
        )
        self.assertEqual(first["state"], "passed")
        terminal = next((run_root / "turns").rglob("terminal.json"))
        terminal.unlink()
        (run_root / "run-receipt.json").unlink()
        clients = []
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=FakeQueue([]),
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(recovered["state"], "waiting")
        self.assertEqual(recovered["semantic_attempt_count"], 1)
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        self.assertTrue(recovered["accounting_complete"])
        self.assertEqual(recovered["usage"]["total_tokens"], 125)
        self.assertTrue(recovered["partial_turns"][0]["lineage_verified"])
        self.assertTrue(recovered["partial_turns"][0]["sidecar_lineage_verified"])
        self.assertEqual(clients, [])

    async def test_completed_terminal_and_submissions_rebuild_aggregate_without_replay(self):
        run_root = self.root / "completed-terminal-recovery"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        self.assertEqual(first["semantic_model_call_count"], 1)
        (run_root / "run-receipt.json").unlink()
        (run_root / "usage-telemetry.json").unlink()
        FakeClient.instances.clear()
        clients = []
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(recovered["state"], "passed")
        self.assertEqual(
            recovered["terminal_reason"],
            "completed_turns_reconciled_without_replay",
        )
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        self.assertEqual(recovered["recovery_new_semantic_model_call_count"], 0)
        self.assertEqual(recovered["submitted_segment_count"], 1)
        self.assertEqual(recovered["episode_thread_count"], 1)
        self.assertEqual(recovered["unique_turn_count"], 1)
        self.assertEqual(recovered["usage"]["total_tokens"], 125)
        self.assertEqual(recovered["wall_elapsed_seconds"], 1.25)
        self.assertTrue(recovered["production_mutated"])
        self.assertEqual(clients, [])
        self.assertEqual(FakeClient.instances, [])

        verified_again = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(verified_again, recovered)
        self.assertEqual(clients, [])

    async def test_completed_episode_recovers_completion_and_archive_without_replay(self):
        run_root = self.root / "completed-episode-archive-recovery"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        thread_id = FakeClient.instances[0].started_threads[0].thread_id
        (run_root / "run-receipt.json").unlink()
        (run_root / "usage-telemetry.json").unlink()
        lifecycle_root = run_root / "episodes" / "ep-a"
        (lifecycle_root / "thread-completion.json").unlink()
        (lifecycle_root / "thread-archive.json").unlink()
        FakeClient.durable_threads[thread_id]["archived"] = False
        FakeClient.instances.clear()

        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
        )
        self.assertEqual(recovered["state"], "passed")
        self.assertEqual(
            recovered["terminal_reason"],
            "completed_turns_reconciled_without_replay",
        )
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        self.assertEqual(len(FakeClient.instances), 1)
        recovery_client = FakeClient.instances[0]
        self.assertEqual(recovery_client.started_threads, [])
        self.assertEqual(recovery_client.resumed_threads, [])
        self.assertEqual(recovery_client.turns, [])
        self.assertEqual(recovery_client.archived_threads, [thread_id])
        self.assertTrue((lifecycle_root / "thread-completion.json").is_file())
        self.assertTrue((lifecycle_root / "thread-archive.json").is_file())

    async def test_archive_rpc_crash_recovers_from_observed_archived_state(self):
        run_root = self.root / "archive-rpc-crash"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: ArchiveAfterRpcCrashClient(),
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "waiting")
        self.assertEqual(
            first["terminal_reason"],
            "same_thread_archive_recovery_checkpoint_no_replay",
        )
        self.assertFalse((run_root / "run-receipt.json").exists())
        lifecycle_root = run_root / "episodes" / "ep-a"
        self.assertTrue((lifecycle_root / "thread-completion.json").is_file())
        self.assertFalse((lifecycle_root / "thread-archive.json").exists())
        thread_id = FakeClient.instances[0].started_threads[0].thread_id
        self.assertTrue(FakeClient.durable_threads[thread_id]["archived"])

        FakeClient.instances.clear()
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
        )
        self.assertEqual(recovered["state"], "passed")
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        recovery_client = FakeClient.instances[0]
        self.assertEqual(recovery_client.turns, [])
        self.assertEqual(recovery_client.archived_threads, [])
        self.assertEqual(recovery_client.archive_state_queries, [thread_id])
        archive = json.loads(
            (lifecycle_root / "thread-archive.json").read_text(encoding="utf-8")
        )
        self.assertFalse(archive["archive_rpc_performed"])
        self.assertEqual(
            archive["archive_state_observation"]["state"], "archived"
        )

    async def test_existing_archive_payload_and_aggregate_binding_are_reverified(self):
        run_root = self.root / "archive-reverification"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        archive_path = run_root / "episodes" / "ep-a" / "thread-archive.json"
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        archive["archive_state_observation"]["archived_match_count"] = 2
        archive_path.write_text(pretty(archive), encoding="utf-8")
        with self.assertRaisesRegex(
            ProductionRunnerError, "archive receipt drifted"
        ):
            await run_production_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                limit=1,
                dry_run=False,
                fixture_mode=True,
                client_factory=self.factory,
            )

    async def test_usage_telemetry_cannot_drop_verified_episode_archives(self):
        run_root = self.root / "archive-telemetry-binding"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        usage_path = run_root / "usage-telemetry.json"
        usage = json.loads(usage_path.read_text(encoding="utf-8"))
        usage["episode_thread_archives"] = []
        usage_path.write_text(pretty(usage), encoding="utf-8")
        receipt_path = run_root / "run-receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["usage_telemetry"] = record(usage_path)
        receipt_path.write_text(pretty(receipt), encoding="utf-8")
        with self.assertRaisesRegex(
            ProductionRunnerError, "usage receipt drifted"
        ):
            await run_production_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                limit=1,
                dry_run=False,
                fixture_mode=True,
                client_factory=self.factory,
            )

    async def test_multi_episode_resume_preserves_all_archive_records(self):
        run_root = self.root / "multi-episode-archive-resume"
        items = [self.items[0]]
        for index, source in enumerate(self.items[1:5]):
            binding = fixture_source_binding(
                segment_id=source.segment_id,
                episode_id="ep-b",
                segment_index=index,
                text=source.segment_text,
                quality=dict(source.segment_quality),
            )
            items.append(
                QueueItem(
                    **{
                        **source.__dict__,
                        "episode_id": "ep-b",
                        "segment_index": index,
                        "expected_source_binding": binding,
                    }
                )
            )
        queue = FakeQueue(items)
        admitted = FakeCapacityProvider(queue)

        def stop_before_final_batch(*, request, remaining_batch_count, timeout_seconds):
            if len(admitted.calls) == 2:
                raise ProductionRunnerWaiting(
                    "fixture stop before final episode batch"
                )
            return admitted(
                request=request,
                remaining_batch_count=remaining_batch_count,
                timeout_seconds=timeout_seconds,
            )

        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=5,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=stop_before_final_batch,
        )
        self.assertEqual(first["state"], "waiting")
        self.assertEqual(first["semantic_model_call_count"], 2)
        self.assertEqual(len(queue.pending), 1)
        self.assertTrue(
            (run_root / "episodes" / "ep-a" / "thread-archive.json").is_file()
        )
        FakeClient.instances.clear()
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=5,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(recovered["state"], "passed")
        self.assertEqual(recovered["semantic_model_call_count"], 3)
        self.assertEqual(len(recovered["episode_thread_archives"]), 2)
        completion_paths = {
            Path(item["completion"]["path"]).parent.name
            for item in recovered["episode_thread_archives"]
        }
        self.assertEqual(completion_paths, {"ep-a", "ep-b"})
        telemetry = json.loads(
            (run_root / "usage-telemetry.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            telemetry["episode_thread_archives"],
            recovered["episode_thread_archives"],
        )

    async def test_unresolved_scoped_job_prevents_episode_archive(self):
        run_root = self.root / "unresolved-episode-drain"
        queue = UnresolvedScopedQueue(self.items[:1])
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 1)
        self.assertFalse((run_root / "episodes" / "ep-a").exists())
        self.assertEqual(FakeClient.instances[0].archived_threads, [])

    async def test_finalization_rejects_terminal_persisted_path_drift(self):
        run_root = self.root / "finalization-persisted-path-drift"
        queue = PersistedPathTamperOnDrainQueue(
            self.items[:1], output_root=run_root
        )
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 1)
        self.assertTrue(queue.tampered)
        self.assertFalse((run_root / "episodes" / "ep-a").exists())
        self.assertEqual(FakeClient.instances[0].archived_threads, [])

    async def test_completed_terminal_context_tamper_waits_without_replay(self):
        run_root = self.root / "completed-context-tamper"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        (run_root / "run-receipt.json").unlink()
        (run_root / "usage-telemetry.json").unlink()
        turn_root = next((run_root / "turns").glob("*/*"))
        context_path = turn_root / "episode-context-binding.private.json"
        launch_path = turn_root / "launch.json"
        terminal_path = turn_root / "terminal.json"
        context = json.loads(context_path.read_text())
        context["semantic_fields_sha256"] = "f" * 64
        context_path.write_text(pretty(context), encoding="utf-8")
        launch = json.loads(launch_path.read_text())
        launch["episode_context_binding"] = record(context_path)
        launch_path.write_text(pretty(launch), encoding="utf-8")
        terminal = json.loads(terminal_path.read_text())
        terminal["context"] = record(context_path)
        terminal["launch"] = record(launch_path)
        terminal_path.write_text(pretty(terminal), encoding="utf-8")
        FakeClient.instances.clear()
        clients = []
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(recovered["state"], "waiting")
        self.assertEqual(
            recovered["terminal_reason"],
            "completed_turn_integrity_waiting_no_replay",
        )
        self.assertIsNone(recovered["semantic_model_call_count"])
        self.assertEqual(clients, [])
        self.assertEqual(FakeClient.instances, [])

    async def test_completed_terminal_submission_drift_waits_without_replay(self):
        run_root = self.root / "completed-submission-drift"
        queue = FakeQueue(self.items[:1])
        first = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(first["state"], "passed")
        (run_root / "run-receipt.json").unlink()
        (run_root / "usage-telemetry.json").unlink()
        queue.submissions.clear()
        FakeClient.instances.clear()
        clients = []
        recovered = await run_production_queue(
            contract_path=self.contract,
            output_root=run_root,
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
        )
        self.assertEqual(recovered["state"], "waiting")
        self.assertEqual(
            recovered["terminal_reason"],
            "completed_submission_reconciliation_waiting_no_replay",
        )
        self.assertEqual(recovered["semantic_model_call_count"], 1)
        self.assertEqual(recovered["recovery_new_semantic_model_call_count"], 0)
        self.assertEqual(clients, [])
        self.assertEqual(FakeClient.instances, [])

    async def test_fixture_capacity_cannot_authorize_production_recovery(self):
        run_root = self.root / "fixture-capacity-production-recovery"
        queue = FakeQueue(self.items[:1])
        with contextlib.nullcontext():
            first = await run_production_queue(
                contract_path=self.contract,
                output_root=run_root,
                queue=queue,
                limit=1,
                dry_run=False,
                fixture_mode=True,
                client_factory=self.factory,
                capacity_provider=FakeCapacityProvider(queue),
            )
            self.assertEqual(first["state"], "passed")
            (run_root / "run-receipt.json").unlink()
            (run_root / "usage-telemetry.json").unlink()
            FakeClient.instances.clear()
            clients = []
            with self.assertRaisesRegex(
                ProductionRunnerError,
                "live production rejects injected client or capacity providers",
            ):
                await run_production_queue(
                    contract_path=self.contract,
                    output_root=run_root,
                    queue=queue,
                    limit=1,
                    dry_run=False,
                    fixture_mode=False,
                    client_factory=lambda _binary: clients.append(True),
                )
        self.assertEqual(clients, [])
        self.assertEqual(FakeClient.instances, [])

    async def test_verified_stale_release_requires_fresh_root_without_model_call(self):
        queue = ReleasedRecoveryQueue(self.items[:1])
        clients = []
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "stale-release",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: clients.append(True),
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(
            receipt["terminal_reason"],
            "stale_prelaunch_claim_released_fresh_root_required",
        )
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertTrue(receipt["accounting_complete"])
        self.assertTrue(receipt["production_mutated"])
        self.assertEqual(queue.claim_calls, 0)
        self.assertEqual(clients, [])

    async def test_auth_drift_and_api_key_fail_closed(self):
        queue = FakeQueue(self.items[:1])
        capacity = FakeCapacityProvider(queue)
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "auth-drift",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: FakeClient(plan_type="free"),
            capacity_provider=capacity,
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        with patch.dict(os.environ, {"OPENAI_API_KEY": "synthetic"}, clear=True):
            with self.assertRaisesRegex(ProductionRunnerError, "OPENAI_API_KEY"):
                await run_production_queue(
                    contract_path=self.contract,
                    output_root=self.root / "api-key",
                    queue=FakeQueue(self.items[:1]),
                    limit=1,
                    dry_run=False,
                    fixture_mode=True,
                    client_factory=self.factory,
                    capacity_provider=capacity,
                )

    async def test_post_claim_failure_reports_production_mutation(self):
        queue = MutatingClaimFailureQueue(self.items[:1])
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "post-claim-failure",
            queue=queue,
            limit=1,
            dry_run=False,
            fixture_mode=True,
            client_factory=self.factory,
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 0)
        self.assertTrue(receipt["production_mutated"])

    async def test_duplicate_turn_id_stops_without_retry(self):
        queue = FakeQueue(self.items[:4])
        receipt = await run_production_queue(
            contract_path=self.contract,
            output_root=self.root / "duplicate-turn",
            queue=queue,
            limit=4,
            dry_run=False,
            fixture_mode=True,
            client_factory=lambda _binary: FakeClient(duplicate_turn_ids=True),
            capacity_provider=FakeCapacityProvider(queue),
        )
        self.assertEqual(receipt["state"], "waiting")
        self.assertEqual(receipt["semantic_model_call_count"], 2)
        self.assertEqual(receipt["semantic_retry_count"], 0)

    def test_sqlite_scope_includes_null_or_exact_v31_and_excludes_v1_or_other_model(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, target_id TEXT,
              payload_json TEXT, status TEXT, priority INTEGER, attempts INTEGER,
              max_attempts INTEGER, lease_owner TEXT, leased_until TEXT, updated_at TEXT
            );
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT, segment_index INTEGER);
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, job_id INTEGER, segment_id TEXT, label_pack TEXT,
              model TEXT, output_path TEXT, status TEXT, claimed_at TEXT,
              completed_at TEXT, error TEXT, created_at TEXT, updated_at TEXT,
              prompt_path TEXT
            );
            """
        )
        rows = [
            (1, "seg-1", "ep-a", 0, {"label_pack": "ai_discourse_v3_1"}),
            (2, "seg-2", "ep-a", 1, {"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"}),
            (3, "seg-3", "ep-a", 2, {"label_pack": "ai_discourse_v1"}),
            (4, "seg-4", "ep-a", 3, {"label_pack": "ai_discourse_v3_1", "model": "other"}),
        ]
        for job_id, segment_id, episode_id, index, payload in rows:
            connection.execute("INSERT INTO segments VALUES (?, ?, ?)", (segment_id, episode_id, index))
            connection.execute(
                "INSERT INTO jobs VALUES (?, 'podcast', 'label_segment', ?, ?, 'pending', 1, 0, 3, NULL, NULL, '')",
                (job_id, segment_id, canonical(payload)),
            )
        connection.commit()
        connection.execute(
            """
            INSERT INTO label_runs
              (id, job_id, segment_id, label_pack, model, output_path, status,
               claimed_at, completed_at, error, created_at, updated_at, prompt_path)
            VALUES ('stale-run', 1, 'seg-1', ?, ?, '/tmp/stale-output.json',
                    'claimed', '', NULL, NULL, '', '', '/tmp/stale-prompt.md')
            """,
            (FROZEN_LABEL_PACK, episode_batch.MODEL),
        )
        connection.commit()
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="fixture",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            output_root=self.root / "sqlite",
        )
        scoped = queue._scoped_episode_rows(limit=8, status="pending")  # noqa: SLF001
        self.assertEqual([row["id"] for row in scoped], [1, 2])
        prepared_request = self.root / "sqlite" / "prepared-request.json"
        prepared_request.parent.mkdir(parents=True, exist_ok=True)
        prepared_request.write_text(
            pretty(
                {
                    "episode_id": "ep-a",
                    "segment_ids": ["seg-1", "seg-2"],
                }
            ),
            encoding="utf-8",
        )
        claim_receipt = self.root / "sqlite" / "claim-receipt.json"
        claim_items = [
            QueueItem(
                job_id=job_id,
                segment_id=f"seg-{job_id}",
                episode_id="ep-a",
                segment_index=job_id - 1,
                segment_text=f"segment {job_id}",
                segment_text_sha256=sha_bytes(f"segment {job_id}".encode()),
                label_run_id="preview",
                output_path="preview",
            )
            for job_id in (1, 2)
        ]
        with patch.object(queue, "_assert_source_items_current"):
            claimed = queue.claim_episode_batch(
                claim_items,
                prepared_request_path=prepared_request,
                claim_receipt_path=claim_receipt,
            )
        self.assertEqual([item.job_id for item in claimed], [1, 2])
        self.assertEqual(
            queue.unresolved_episode_segment_count(episode_id="ep-a"), 2
        )
        connection.execute(
            "UPDATE jobs SET status = 'failed', attempts = max_attempts WHERE id = 2"
        )
        connection.commit()
        self.assertEqual(
            queue.unresolved_episode_segment_count(episode_id="ep-a"), 2
        )
        connection.execute(
            "UPDATE jobs SET status = 'claimed', attempts = 1 WHERE id = 2"
        )
        connection.commit()
        statuses = {
            row["id"]: row["status"]
            for row in connection.execute("SELECT id, status FROM jobs")
        }
        self.assertEqual(statuses, {1: "claimed", 2: "claimed", 3: "pending", 4: "pending"})
        persisted = json.loads(claim_receipt.read_text())
        self.assertEqual(persisted["job_ids"], [1, 2])
        self.assertEqual(persisted["payload_model_policy"], "null_or_exact_frozen_queue_model")
        connection.execute(
            "UPDATE jobs SET leased_until = '2000-01-01T00:00:00+00:00' WHERE id IN (1, 2)"
        )
        connection.commit()
        stale_rows = queue._scoped_episode_rows(limit=8, status="claimed")  # noqa: SLF001
        self.assertEqual([row["id"] for row in stale_rows], [1, 2])
        queue.stale_episode_batch = lambda *, limit: claimed
        launch_path = prepared_request.parent / "launch.json"
        launch_path.write_text("{}\n", encoding="utf-8")
        preserved = queue.recover_stale_prelaunch_batch(limit=8)
        self.assertEqual(preserved["state"], "semantic_attempt_preserved")
        self.assertIsNone(preserved["semantic_model_call_count"])
        self.assertFalse(preserved["accounting_complete"])
        claimed_statuses = {
            row["id"]: row["status"]
            for row in connection.execute("SELECT id, status FROM jobs WHERE id IN (1, 2)")
        }
        self.assertEqual(claimed_statuses, {1: "claimed", 2: "claimed"})
        launch_path.unlink()
        recovered = queue.recover_stale_prelaunch_batch(limit=8)
        self.assertEqual(recovered["state"], "released_verified_prelaunch_claim")
        self.assertEqual(recovered["semantic_model_call_count"], 0)
        released = {
            row["id"]: (row["status"], row["attempts"], json.loads(row["payload_json"]))
            for row in connection.execute(
                "SELECT id, status, attempts, payload_json FROM jobs WHERE id IN (1, 2)"
            )
        }
        self.assertEqual(released[1][0:2], ("pending", 0))
        self.assertNotIn("model", released[1][2])
        self.assertEqual(released[2][0:2], ("pending", 0))
        self.assertEqual(released[2][2]["model"], "gpt-5.5")
        failed_runs = connection.execute(
            "SELECT COUNT(*) FROM label_runs WHERE status = 'failed'"
        ).fetchone()[0]
        self.assertEqual(failed_runs, 2)
        connection.close()

    def test_failed_only_scoped_episode_resets_once_without_duplicate(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, target_id TEXT,
              payload_json TEXT, status TEXT, priority INTEGER, attempts INTEGER,
              max_attempts INTEGER, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT, created_at TEXT, updated_at TEXT,
              completed_at TEXT, error TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, segment_index INTEGER
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, job_id INTEGER, segment_id TEXT, label_pack TEXT,
              model TEXT, output_path TEXT, status TEXT, claimed_at TEXT,
              completed_at TEXT, error TEXT, created_at TEXT, updated_at TEXT,
              prompt_path TEXT
            );
            """
        )
        stale_payload = {
            "label_pack": FROZEN_LABEL_PACK,
            "model": episode_batch.MODEL,
            "queue_payload_model": FROZEN_QUEUE_PAYLOAD_MODEL,
            "queue_payload_model_original_present": False,
            "queue_payload_model_original": None,
            "label_run_id": "stale-run",
            "output_path": "/tmp/stale-output.json",
            "prompt_path": "/tmp/stale-prompt.md",
            "app_server_prepared_request_path": "/tmp/stale-request.json",
            "app_server_claim_receipt_path": "/tmp/stale-claim.json",
            "app_server_prepared_request_sha256": "a" * 64,
        }
        rows = [
            (1, FROZEN_QUEUE_LANE, "seg-1", "ep-failed", stale_payload),
            (
                2,
                "local",
                "seg-2",
                "ep-off-lane",
                {"label_pack": FROZEN_LABEL_PACK},
            ),
            (
                3,
                FROZEN_QUEUE_LANE,
                "seg-3",
                "ep-v1",
                {"label_pack": "ai_discourse_v1"},
            ),
            (
                4,
                FROZEN_QUEUE_LANE,
                "seg-4",
                "ep-other-model",
                {"label_pack": FROZEN_LABEL_PACK, "model": "other"},
            ),
        ]
        for job_id, lane, segment_id, episode_id, payload in rows:
            connection.execute(
                "INSERT INTO segments VALUES (?, ?, 0)",
                (segment_id, episode_id),
            )
            connection.execute(
                """
                INSERT INTO jobs
                  (id, lane, job_type, target_id, payload_json, status,
                   priority, attempts, max_attempts, lease_owner, leased_until,
                   dedupe_key, created_at, updated_at, completed_at, error)
                VALUES (?, ?, 'label_segment', ?, ?, 'failed', 1, 3, 3,
                        'stale-worker', '2000-01-01T00:00:00+00:00', ?, '', '',
                        '2026-07-18T00:00:00+00:00', 'fixture failure')
                """,
                (
                    job_id,
                    lane,
                    segment_id,
                    canonical(payload),
                    f"job-{job_id}",
                ),
            )
        connection.execute(
            """
            INSERT INTO label_runs
              (id, job_id, segment_id, label_pack, model, output_path, status,
               claimed_at, completed_at, error, created_at, updated_at, prompt_path)
            VALUES ('stale-run', 1, 'seg-1', ?, ?, '/tmp/stale-output.json',
                    'claimed', '', NULL, NULL, '', '', '/tmp/stale-prompt.md')
            """,
            (FROZEN_LABEL_PACK, episode_batch.MODEL),
        )
        connection.commit()
        output_root = self.root / "failed-label-reset"
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="fixture-worker",
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            output_root=output_root,
        )
        plan = queue.reconcile_failed_scoped_jobs(execute=False)
        self.assertEqual(plan["state"], "planned")
        self.assertEqual(plan["failed_job_count"], 1)
        self.assertEqual(plan["claimed_label_run_count"], 1)
        self.assertEqual(plan["managed_app_server_claimed_label_run_count"], 1)
        self.assertEqual(
            plan["legacy_semantic_attempts_failed_for_fresh_retry_count"], 0
        )
        self.assertEqual(
            connection.execute(
                "SELECT status FROM jobs WHERE id = 1"
            ).fetchone()[0],
            "failed",
        )
        connection.execute(
            """
            CREATE TRIGGER reject_fixture_label_run_reset
            BEFORE UPDATE ON label_runs
            WHEN OLD.id = 'stale-run' AND NEW.status = 'failed'
            BEGIN
              SELECT RAISE(ABORT, 'fixture atomic rollback');
            END
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            queue.reconcile_failed_scoped_jobs(execute=True)
        self.assertEqual(
            connection.execute(
                "SELECT status FROM jobs WHERE id = 1"
            ).fetchone()[0],
            "failed",
        )
        self.assertEqual(
            connection.execute(
                "SELECT status FROM label_runs WHERE id = 'stale-run'"
            ).fetchone()[0],
            "claimed",
        )
        connection.execute("DROP TRIGGER reject_fixture_label_run_reset")
        connection.commit()
        applied = queue.reconcile_failed_scoped_jobs(execute=True)
        self.assertEqual(applied["state"], "reconciled")
        self.assertEqual(applied["failed_job_count"], 1)
        reset = connection.execute(
            "SELECT * FROM jobs WHERE id = 1"
        ).fetchone()
        reset_payload = json.loads(reset["payload_json"])
        self.assertEqual(reset["status"], "pending")
        self.assertEqual(reset["attempts"], 0)
        self.assertIsNone(reset["lease_owner"])
        self.assertIsNone(reset["leased_until"])
        self.assertIsNone(reset["completed_at"])
        self.assertIsNone(reset["error"])
        self.assertNotIn("model", reset_payload)
        self.assertNotIn("queue_payload_model", reset_payload)
        self.assertNotIn("label_run_id", reset_payload)
        reconciled_run = connection.execute(
            "SELECT status, error FROM label_runs WHERE id = 'stale-run'"
        ).fetchone()
        self.assertEqual(reconciled_run["status"], "failed")
        self.assertEqual(
            reconciled_run["error"],
            "Reconciled failed app-server job for bounded retry.",
        )
        self.assertEqual(
            queue.reconcile_failed_scoped_jobs(execute=True)["state"],
            "no_change",
        )
        self.assertEqual(
            [
                row["id"]
                for row in queue._scoped_episode_rows(  # noqa: SLF001
                    limit=8,
                    status="pending",
                    episode_id="ep-failed",
                )
            ],
            [1],
        )
        prepared_request = output_root / "prepared-request.json"
        prepared_request.parent.mkdir(parents=True, exist_ok=True)
        prepared_request.write_text(
            pretty({"episode_id": "ep-failed", "segment_ids": ["seg-1"]}),
            encoding="utf-8",
        )
        with patch.object(queue, "_assert_source_items_current"):
            claimed = queue.claim_episode_batch(
                [
                    QueueItem(
                        job_id=1,
                        segment_id="seg-1",
                        episode_id="ep-failed",
                        segment_index=0,
                        segment_text="fixture",
                        label_run_id="preview",
                        output_path="preview",
                    )
                ],
                prepared_request_path=prepared_request,
                claim_receipt_path=output_root / "claim-receipt.json",
            )
        self.assertEqual([item.job_id for item in claimed], [1])
        self.assertEqual(
            queue.unresolved_episode_segment_count(episode_id="ep-failed"), 1
        )
        connection.execute(
            "UPDATE jobs SET status = 'completed', completed_at = '' WHERE id = 1"
        )
        connection.commit()
        self.assertEqual(
            queue.unresolved_episode_segment_count(episode_id="ep-failed"), 0
        )
        off_scope = {
            row["id"]: row["status"]
            for row in connection.execute(
                "SELECT id, status FROM jobs WHERE id IN (2, 3, 4)"
            )
        }
        self.assertEqual(off_scope, {2: "failed", 3: "failed", 4: "failed"})
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE target_id = 'seg-1'"
            ).fetchone()[0],
            1,
        )
        connection.close()

    def test_legacy_claimed_run_is_closed_without_relabeling_or_deleting_artifacts(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, target_id TEXT,
              payload_json TEXT, status TEXT, priority INTEGER, attempts INTEGER,
              max_attempts INTEGER, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT, created_at TEXT, updated_at TEXT,
              completed_at TEXT, error TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, segment_index INTEGER
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, job_id INTEGER, segment_id TEXT, label_pack TEXT,
              model TEXT, prompt_path TEXT, output_path TEXT, status TEXT,
              claimed_at TEXT, completed_at TEXT, error TEXT, created_at TEXT,
              updated_at TEXT
            );
            """
        )
        prompt_path = self.root / "legacy-prompt.md"
        output_path = self.root / "legacy-invalid-output.json"
        prompt_path.write_text("legacy prompt\n", encoding="utf-8")
        output_path.write_text(
            pretty(
                {
                    "schema_version": FROZEN_LABEL_PACK,
                    "segment_id": "seg-legacy",
                    "extraction_status": "not-coded",
                    "discourse_events": [{"event_type": "invalid-enum"}],
                }
            ),
            encoding="utf-8",
        )
        prompt_sha = sha_bytes(prompt_path.read_bytes())
        output_sha = sha_bytes(output_path.read_bytes())
        payload = {
            "label_pack": FROZEN_LABEL_PACK,
            "model": FROZEN_QUEUE_PAYLOAD_MODEL,
            "label_run_id": "legacy-run",
            "prompt_path": str(prompt_path),
            "output_path": str(output_path),
        }
        connection.execute(
            "INSERT INTO segments VALUES ('seg-legacy', 'ep-legacy', 0)"
        )
        connection.execute(
            """
            INSERT INTO jobs
              (id, lane, job_type, target_id, payload_json, status, priority,
               attempts, max_attempts, lease_owner, leased_until, dedupe_key,
               created_at, updated_at, completed_at, error)
            VALUES (1, ?, 'label_segment', 'seg-legacy', ?, 'failed', 1, 3, 3,
                    'legacy-worker', NULL, 'legacy-job', '', '', '', 'legacy failure')
            """,
            (FROZEN_QUEUE_LANE, canonical(payload)),
        )
        connection.execute(
            """
            INSERT INTO label_runs
              (id, job_id, segment_id, label_pack, model, prompt_path,
               output_path, status, claimed_at, completed_at, error,
               created_at, updated_at)
            VALUES ('legacy-run', 1, 'seg-legacy', ?, ?, ?, ?, 'claimed', '',
                    NULL, NULL, '', '')
            """,
            (
                FROZEN_LABEL_PACK,
                FROZEN_QUEUE_PAYLOAD_MODEL,
                str(prompt_path),
                str(output_path),
            ),
        )
        connection.commit()
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="fixture-worker",
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            semantic_model=episode_batch.MODEL,
            output_root=self.root / "legacy-reset",
        )
        plan = queue.reconcile_failed_scoped_jobs(execute=False)
        self.assertEqual(plan["failed_job_count"], 1)
        self.assertEqual(plan["claimed_label_run_count"], 1)
        self.assertEqual(plan["managed_app_server_claimed_label_run_count"], 0)
        self.assertEqual(
            plan["legacy_semantic_attempts_failed_for_fresh_retry_count"], 1
        )
        applied = queue.reconcile_failed_scoped_jobs(execute=True)
        self.assertEqual(applied["state"], "reconciled")
        reset_payload = json.loads(
            connection.execute(
                "SELECT payload_json FROM jobs WHERE id = 1"
            ).fetchone()[0]
        )
        self.assertEqual(reset_payload["model"], FROZEN_QUEUE_PAYLOAD_MODEL)
        self.assertNotIn("label_run_id", reset_payload)
        run = connection.execute(
            "SELECT * FROM label_runs WHERE id = 'legacy-run'"
        ).fetchone()
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["model"], FROZEN_QUEUE_PAYLOAD_MODEL)
        self.assertEqual(run["prompt_path"], str(prompt_path))
        self.assertEqual(run["output_path"], str(output_path))
        self.assertEqual(sha_bytes(prompt_path.read_bytes()), prompt_sha)
        self.assertEqual(sha_bytes(output_path.read_bytes()), output_sha)
        self.assertEqual(
            queue.reconcile_failed_scoped_jobs(execute=True)["state"],
            "no_change",
        )
        connection.close()

    def test_read_only_real_denominator_shape_plans_24_jobs_and_3_legacy_attempts(self):
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, target_id TEXT,
              payload_json TEXT, status TEXT, priority INTEGER, attempts INTEGER,
              max_attempts INTEGER, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT, created_at TEXT, updated_at TEXT,
              completed_at TEXT, error TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, segment_index INTEGER
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, job_id INTEGER, segment_id TEXT, label_pack TEXT,
              model TEXT, prompt_path TEXT, output_path TEXT, status TEXT,
              claimed_at TEXT, completed_at TEXT, error TEXT, created_at TEXT,
              updated_at TEXT
            );
            """
        )
        for job_id in range(1, 25):
            segment_id = f"seg-real-shape-{job_id:02d}"
            payload = {"label_pack": FROZEN_LABEL_PACK}
            if job_id <= 3:
                prompt_path = self.root / f"legacy-{job_id}.md"
                output_path = self.root / f"legacy-{job_id}.json"
                prompt_path.write_text(f"legacy prompt {job_id}\n", encoding="utf-8")
                output_path.write_text(
                    pretty(
                        {
                            "schema_version": FROZEN_LABEL_PACK,
                            "segment_id": segment_id,
                            "extraction_status": "not-coded",
                            "discourse_events": [{"event_type": "invalid-enum"}],
                        }
                    ),
                    encoding="utf-8",
                )
                payload.update(
                    {
                        "model": FROZEN_QUEUE_PAYLOAD_MODEL,
                        "label_run_id": f"legacy-run-{job_id}",
                        "prompt_path": str(prompt_path),
                        "output_path": str(output_path),
                    }
                )
                connection.execute(
                    """
                    INSERT INTO label_runs
                      (id, job_id, segment_id, label_pack, model, prompt_path,
                       output_path, status, claimed_at, completed_at, error,
                       created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'claimed', '', NULL, NULL, '', '')
                    """,
                    (
                        f"legacy-run-{job_id}",
                        job_id,
                        segment_id,
                        FROZEN_LABEL_PACK,
                        FROZEN_QUEUE_PAYLOAD_MODEL,
                        str(prompt_path),
                        str(output_path),
                    ),
                )
            connection.execute(
                "INSERT INTO segments VALUES (?, ?, ?)",
                (segment_id, f"ep-{(job_id - 1) // 3:02d}", job_id),
            )
            connection.execute(
                """
                INSERT INTO jobs
                  (id, lane, job_type, target_id, payload_json, status, priority,
                   attempts, max_attempts, lease_owner, leased_until, dedupe_key,
                   created_at, updated_at, completed_at, error)
                VALUES (?, ?, 'label_segment', ?, ?, 'failed', 1, 3, 3,
                        NULL, NULL, ?, '', '', '', 'fixture failure')
                """,
                (
                    job_id,
                    FROZEN_QUEUE_LANE,
                    segment_id,
                    canonical(payload),
                    f"real-shape-{job_id}",
                ),
            )
        connection.commit()
        connection.execute("PRAGMA query_only = ON")
        queue = SQLiteWorkerQueue(
            connection,
            lane=FROZEN_QUEUE_LANE,
            worker_id="read-only-plan",
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            semantic_model=episode_batch.MODEL,
            output_root=self.root / "read-only-real-shape",
        )
        plan = queue.reconcile_failed_scoped_jobs(execute=False)
        self.assertEqual(plan["state"], "planned")
        self.assertEqual(plan["failed_job_count"], 24)
        self.assertEqual(plan["claimed_label_run_count"], 3)
        self.assertEqual(plan["managed_app_server_claimed_label_run_count"], 0)
        self.assertEqual(
            plan["legacy_semantic_attempts_failed_for_fresh_retry_count"], 3
        )
        self.assertFalse(plan["production_mutated"])
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'failed'"
            ).fetchone()[0],
            24,
        )
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM label_runs WHERE status = 'claimed'"
            ).fetchone()[0],
            3,
        )
        connection.close()

    def test_cli_requires_immutable_plan_and_separate_reset_authorization(self):
        database = self.root / "failed-label-cli.sqlite"
        connection = sqlite3.connect(database)
        connection.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, target_id TEXT,
              payload_json TEXT, status TEXT, priority INTEGER, attempts INTEGER,
              max_attempts INTEGER, lease_owner TEXT, leased_until TEXT,
              dedupe_key TEXT, created_at TEXT, updated_at TEXT,
              completed_at TEXT, error TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY, episode_id TEXT, segment_index INTEGER
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY, job_id INTEGER, segment_id TEXT, label_pack TEXT,
              model TEXT, prompt_path TEXT, output_path TEXT, status TEXT,
              claimed_at TEXT, completed_at TEXT, error TEXT, created_at TEXT,
              updated_at TEXT
            );
            """
        )
        payload = {
            "label_pack": FROZEN_LABEL_PACK,
            "model": episode_batch.MODEL,
            "queue_payload_model": FROZEN_QUEUE_PAYLOAD_MODEL,
            "queue_payload_model_original_present": False,
            "queue_payload_model_original": None,
            "label_run_id": "cli-run",
        }
        connection.execute("INSERT INTO segments VALUES ('seg-cli', 'ep-cli', 0)")
        connection.execute(
            """
            INSERT INTO jobs
              (id, lane, job_type, target_id, payload_json, status, priority,
               attempts, max_attempts, lease_owner, leased_until, dedupe_key,
               created_at, updated_at, completed_at, error)
            VALUES (1, ?, 'label_segment', 'seg-cli', ?, 'failed', 1, 3, 3,
                    NULL, NULL, 'cli-job', '', '', '', 'fixture failure')
            """,
            (FROZEN_QUEUE_LANE, canonical(payload)),
        )
        connection.execute(
            """
            INSERT INTO label_runs
              (id, job_id, segment_id, label_pack, model, prompt_path,
               output_path, status, claimed_at, completed_at, error,
               created_at, updated_at)
            VALUES ('cli-run', 1, 'seg-cli', ?, ?, NULL, NULL, 'claimed', '',
                    NULL, NULL, '', '')
            """,
            (FROZEN_LABEL_PACK, episode_batch.MODEL),
        )
        connection.commit()
        connection.close()
        output_root = self.root / "failed-label-cli-root"
        loaded = {
            "sha256": "f" * 64,
            "contract": {
                "configuration": {
                    "lane": FROZEN_QUEUE_LANE,
                    "label_pack": FROZEN_LABEL_PACK,
                    "queue_payload_model": FROZEN_QUEUE_PAYLOAD_MODEL,
                    "model": episode_batch.MODEL,
                }
            },
        }
        common = [
            "--contract",
            str(self.contract),
            "--output-root",
            str(output_root),
            "--database",
            str(database),
        ]
        with (
            patch.object(production_runner, "load_contract", return_value=loaded),
            patch.object(
                production_runner,
                "run_production_queue",
                side_effect=AssertionError("semantic runner must remain closed"),
            ) as semantic_runner,
            patch("builtins.print"),
        ):
            self.assertEqual(
                production_runner.main(
                    [*common, "--failed-label-reconciliation", "plan"]
                ),
                0,
            )
            plan_path = output_root / "failed-label-reconciliation-plan.json"
            self.assertTrue(plan_path.is_file())
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["mode"], "plan")
            self.assertFalse(plan["model_call_authorized"])
            with self.assertRaisesRegex(SystemExit, "requires --execute"):
                production_runner.main(
                    [
                        *common,
                        "--failed-label-reconciliation",
                        "execute",
                    ]
                )
            self.assertEqual(
                production_runner.main(
                    [
                        *common,
                        "--failed-label-reconciliation",
                        "execute",
                        "--execute",
                        "--authorize-failed-label-reset",
                    ]
                ),
                0,
            )
            semantic_runner.assert_not_called()
        connection = sqlite3.connect(database)
        self.assertEqual(
            connection.execute("SELECT status FROM jobs WHERE id = 1").fetchone()[0],
            "pending",
        )
        self.assertEqual(
            connection.execute(
                "SELECT status FROM label_runs WHERE id = 'cli-run'"
            ).fetchone()[0],
            "failed",
        )
        connection.close()

    def test_expanded_cap_epoch4_freeze_cannot_authorize_canonical_production(self):
        legacy = self.root / "legacy-expanded-cap-freeze.json"
        legacy.write_text(
            pretty(
                {
                    "schema_version": legacy_production_contract.FROZEN_CONFIGURATION_VERSION,
                    "winner_system_id": legacy_production_contract.WINNER_SYSTEM_ID,
                    "batch_size": 3,
                    "thread_mode": "same_thread",
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ProductionRunnerError, "frozen winner configuration drifted"
        ):
            production_runner._validate_frozen_configuration(  # noqa: SLF001
                configuration_path=legacy,
                expected_sha256=record(legacy)["sha256"],
                expected_winner=legacy_production_contract.WINNER_SYSTEM_ID,
            )
        self.assertEqual(
            production_contract.verify_frozen_configuration(
                self.frozen_configuration
            )["schema_version"],
            production_contract.FROZEN_CONFIGURATION_VERSION,
        )

    def test_legacy_fixture_context_completion_cannot_authorize_production(self):
        self.context_completion.write_text(
            pretty(
                {
                    "schema_version": context_runner.COMPLETION_RECEIPT_VERSION,
                    "state": "passed",
                    "phase": "episode_context",
                    "fixture_only": True,
                    "production_ready": False,
                    "live_model_call_count": 0,
                }
            ),
            encoding="utf-8",
        )
        self.write_contract()
        self.context_promotion_mock.reset_mock()
        with self.assertRaisesRegex(
            ProductionRunnerError,
            "legacy, fixture-only, or nonpromotable context completion is forbidden",
        ):
            load_contract(self.contract)
        self.context_promotion_mock.assert_not_called()

    def test_real_temp_sqlite_live_completion_promotion_chain_is_required(self):
        database = self.root / "promotion-current.sqlite"
        connection = sqlite3.connect(database)
        other_connection = None
        try:
            queue = SimpleNamespace(conn=connection)
            database_identity = context_runner._database_identity(database)
            context_loaded = copy.deepcopy(self.fake_context_loaded)
            context_loaded["path"] = self.context_contract.resolve()
            context_loaded["sha256"] = record(self.context_contract)["sha256"]
            runtime = {
                "path": self.context_runtime.resolve(),
                "cutover": {"database": database_identity},
                "provision_plan": {
                    "database": database_identity,
                    "required_episode_ids": ["ep-a"],
                },
                "provision_execution": {"database": database_identity},
                "provision_execution_path": (
                    self.context_provision_execution.resolve()
                ),
            }
            artifact_index = {"ep-a": self.context_artifacts["ep-a"]}
            completion = {
                "schema_version": context_runner.LIVE_COMPLETION_RECEIPT_VERSION,
                "state": "passed",
                "phase": "episode_context",
                "database": database_identity,
                "contract": record(self.context_contract),
                "runtime_authorization": record(self.context_runtime),
                "provision_execution_receipt": record(
                    self.context_provision_execution
                ),
                "required_episode_count": 1,
                "completed_managed_attempt_count": 1,
                "unique_thread_count": 1,
                "unique_turn_count": 1,
                "context_artifact_index": artifact_index,
                "context_artifact_index_sha256": sha_bytes(
                    canonical(artifact_index).encode("utf-8")
                ),
                "execution_authority": "official_live",
                "semantic_retry_count": 0,
                "test_only": False,
                "promotable": True,
                "managed_chatgpt_auth_only": True,
                "production_context_mutated": True,
                "label_queue_mutated": False,
            }
            completion["receipt_sha256"] = sha_bytes(
                canonical(completion).encode("utf-8")
            )
            completion_path = self.root / "builder-live-completion.json"
            completion_path.write_text(pretty(completion), encoding="utf-8")
            runtime_binding = self.real_context_runtime_binding
            with (
                patch.object(
                    context_runner,
                    "load_episode_context_contract",
                    return_value=context_loaded,
                ),
                patch.object(
                    context_runner,
                    "load_live_episode_context_runtime_authorization",
                    return_value=runtime,
                ),
                patch.object(
                    context_runner,
                    "verify_live_episode_context_completion_receipt",
                    return_value=completion,
                ) as live_verifier,
                patch.object(
                    production_contract,
                    "episode_context_runtime_binding",
                    return_value=runtime_binding,
                ),
            ):
                authority = (
                    production_contract.build_production_promotion_authority(
                        frozen_configuration_path=self.frozen_configuration,
                        episode_context_contract_path=self.context_contract,
                        runtime_authorization_path=self.context_runtime,
                        live_completion_receipt_path=completion_path,
                        queue=queue,
                    )
                )
                authority_path = self.root / "builder-promotion.json"
                authority_path.write_text(pretty(authority), encoding="utf-8")
                verified = (
                    self.real_verify_production_promotion(
                        authority_path,
                        frozen_configuration_path=self.frozen_configuration,
                        episode_context_contract_path=self.context_contract,
                        runtime_authorization_path=self.context_runtime,
                        live_completion_receipt_path=completion_path,
                        queue=queue,
                    )
                )
                self.assertTrue(verified["database_current"])
                self.assertEqual(
                    authority["matrix_lineage"],
                    json.loads(self.frozen_configuration.read_text())[
                        "matrix_lineage"
                    ],
                )
                self.assertEqual(
                    authority["episode_context_database"], database_identity
                )
                self.assertEqual(
                    authority["episode_context_runtime_binding"][
                        "live_completion_receipt_version"
                    ],
                    context_runner.LIVE_COMPLETION_RECEIPT_VERSION,
                )
                self.assertGreaterEqual(live_verifier.call_count, 2)

                other_database = self.root / "promotion-other.sqlite"
                other_connection = sqlite3.connect(other_database)
                with self.assertRaisesRegex(
                    production_contract.CanonicalV31ProductionContractError,
                    "SQLite authority drifted",
                ):
                    self.real_verify_production_promotion(
                        authority_path,
                        frozen_configuration_path=self.frozen_configuration,
                        episode_context_contract_path=self.context_contract,
                        runtime_authorization_path=self.context_runtime,
                        live_completion_receipt_path=completion_path,
                        queue=SimpleNamespace(conn=other_connection),
                    )
        finally:
            if other_connection is not None:
                other_connection.close()
            connection.close()
        self.assertFalse(hasattr(production_runner, "LIVE_PRODUCTION_ENABLED"))

    async def test_every_credential_environment_gate_has_zero_activity(self):
        for name in production_runner.FORBIDDEN_CREDENTIAL_ENVIRONMENT:
            with self.subTest(name=name):
                queue = FakeQueue(self.items[:1])
                output_root = self.root / f"credential-gate-{name.lower()}"
                with patch.dict(os.environ, {name: "synthetic"}, clear=True):
                    with self.assertRaisesRegex(ProductionRunnerError, name):
                        await run_production_queue(
                            contract_path=self.contract,
                            output_root=output_root,
                            queue=queue,
                            limit=1,
                            dry_run=True,
                        )
                self.assertEqual(queue.preview_calls, 0)
                self.assertEqual(queue.claim_calls, 0)
                self.assertFalse(output_root.exists())
                self.assertEqual(FakeClient.instances, [])

    def test_process_writer_lock_is_a_genuine_real_sqlite_os_lock(self):
        connection, queue, _records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-lock-race",
                    "segment_text": "Host: The exact database lock is process wide.",
                }
            ]
        )
        output_root = queue.output_root
        script = """
import json
import sys
from pathlib import Path
from research_factory.app_server_production_runner import ProductionWriterLock
identity = json.loads(sys.argv[1])
with ProductionWriterLock(database_identity=identity, output_root=Path(sys.argv[2]), contract_path=Path(sys.argv[3])):
    print('locked', flush=True)
    sys.stdin.readline()
"""
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                canonical(queue.database_identity),
                str(output_root),
                str(self.contract),
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(process.stdout.readline().strip(), "locked")
            with self.assertRaisesRegex(
                ProductionRunnerWaiting, "writer lock is already held"
            ):
                with ProductionWriterLock(
                    database_identity=queue.database_identity,
                    output_root=output_root,
                    contract_path=self.contract,
                ):
                    self.fail("second process unexpectedly acquired the writer lock")
        finally:
            if process.stdin is not None:
                process.stdin.write("release\n")
                process.stdin.flush()
                process.stdin.close()
            process.wait(timeout=10)
            connection.close()
        stderr = process.stderr.read()
        process.stdout.close()
        process.stderr.close()
        self.assertEqual(process.returncode, 0, stderr)

    def test_writer_lock_rejects_symlink_without_truncating_target(self):
        connection, queue, _records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-lock-symlink",
                    "segment_text": "Host: Lock paths never follow symbolic links.",
                }
            ]
        )
        target = self.root / "must-not-be-truncated.txt"
        expected = b"preserve these exact bytes\n"
        target.write_bytes(expected)
        lock = ProductionWriterLock(
            database_identity=queue.database_identity,
            output_root=queue.output_root,
            contract_path=self.contract,
        )
        try:
            lock.path.unlink(missing_ok=True)
            lock.path.symlink_to(target)
            with self.assertRaisesRegex(
                ProductionRunnerError, "writer lock is not a real file"
            ):
                with lock:
                    self.fail("symbolic-link writer lock was unexpectedly acquired")
            self.assertEqual(target.read_bytes(), expected)
            self.assertTrue(lock.path.is_symlink())
        finally:
            lock.path.unlink(missing_ok=True)
            connection.close()

    async def test_postclaim_raw_cleaned_and_segment_drift_release_without_turn(self):
        for drift_kind in ("raw", "cleaned", "segment"):
            with self.subTest(drift_kind=drift_kind):
                connection, queue, records = self._sqlite_source_queue_fixture(
                    [
                        {
                            "segment_id": f"seg-postclaim-{drift_kind}",
                            "segment_text": "Host: Exact source bytes remain stable until dispatch.",
                        }
                    ]
                )
                original_claim = queue.claim_episode_batch

                def claim_then_mutate(*args, **kwargs):
                    claimed = original_claim(*args, **kwargs)
                    target = records[0][
                        {
                            "raw": "raw_path",
                            "cleaned": "cleaned_path",
                            "segment": "segment_path",
                        }[drift_kind]
                    ]
                    target.write_text(
                        f"postclaim {drift_kind} drift with different bytes",
                        encoding="utf-8",
                    )
                    return claimed

                FakeClient.instances.clear()
                try:
                    with patch.object(
                        queue,
                        "claim_episode_batch",
                        side_effect=claim_then_mutate,
                    ):
                        receipt = await run_production_queue(
                            contract_path=self.contract,
                            output_root=queue.output_root,
                            queue=queue,
                            limit=1,
                            dry_run=False,
                            fixture_mode=True,
                            client_factory=self.factory,
                            capacity_provider=FakeCapacityProvider(),
                        )
                    job = connection.execute(
                        "SELECT status, attempts FROM jobs WHERE id = ?",
                        (records[0]["job_id"],),
                    ).fetchone()
                    self.assertEqual(receipt["state"], "waiting")
                    self.assertEqual(receipt["semantic_model_call_count"], 0)
                    self.assertEqual(job["status"], "pending")
                    self.assertEqual(job["attempts"], 0)
                    self.assertEqual(
                        connection.execute(
                            "SELECT status FROM label_runs ORDER BY created_at DESC LIMIT 1"
                        ).fetchone()["status"],
                        "failed",
                    )
                    self.assertEqual(len(FakeClient.instances), 1)
                    self.assertEqual(FakeClient.instances[0].started_threads, [])
                    self.assertEqual(FakeClient.instances[0].resumed_threads, [])
                    self.assertEqual(FakeClient.instances[0].turns, [])
                    self.assertEqual(
                        list(queue.output_root.rglob("sidecar.json")), []
                    )
                finally:
                    connection.close()

    async def test_full_offline_transport_runs_against_real_sqlite_without_derivatives(self):
        connection, queue, records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-real-offline-transport",
                    "segment_text": "Host: Welcome to this exact offline transport test.",
                }
            ]
        )
        try:
            with patch(
                "research_factory.worker.corpus_dir",
                return_value=self.root / "corpus",
            ):
                receipt = await run_production_queue(
                    contract_path=self.contract,
                    output_root=queue.output_root,
                    queue=queue,
                    limit=1,
                    dry_run=False,
                    fixture_mode=True,
                    client_factory=self.factory,
                    capacity_provider=FakeCapacityProvider(),
                )
            self.assertEqual(receipt["state"], "passed")
            self.assertEqual(receipt["semantic_model_call_count"], 1)
            self.assertEqual(receipt["submitted_segment_count"], 1)
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM jobs WHERE id = ?", (records[0]["job_id"],)
                ).fetchone()["status"],
                "completed",
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM labels").fetchone()[0], 1
            )
            for table in (
                "claims",
                "coded_observations",
                "entity_mentions",
                "concept_aliases",
                "discourse_events",
                "concept_candidates",
            ):
                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
                    0,
                )
            capacity = json.loads(
                next(queue.output_root.rglob("capacity-admission.json")).read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(capacity["offline_fixture_non_promotable"])
            self.assertFalse(capacity["live_capacity_authority"])
        finally:
            connection.close()

    def test_real_cli_capacity_denial_precedes_claim_on_real_sqlite(self):
        connection, queue, records = self._sqlite_source_queue_fixture(
            [
                {
                    "segment_id": "seg-cli-capacity-boundary",
                    "segment_text": "Host: CLI capacity denial occurs before claim.",
                }
            ]
        )
        database = Path(queue.database_identity["path"])
        output_root = queue.output_root
        connection.close()

        class DeniedCapacity:
            fixture_only = True

            def __call__(self, **kwargs):
                raise ProductionRunnerWaiting("offline CLI numeric reserve denied")

        FakeClient.instances.clear()
        stdout = io.StringIO()
        with (
            patch.dict(
                os.environ,
                {"RESEARCH_FACTORY_ROOT": str(self.root)},
                clear=True,
            ),
            contextlib.redirect_stdout(stdout),
        ):
            code = production_runner.main(
                [
                    "--contract",
                    str(self.contract),
                    "--output-root",
                    str(output_root),
                    "--database",
                    str(database),
                    "--limit",
                    "1",
                    "--execute",
                ],
                _offline_test_client_factory=self.factory,
                _offline_test_capacity_provider=DeniedCapacity(),
            )
        self.assertEqual(code, 0)
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["state"], "waiting")
        self.assertEqual(result["semantic_model_call_count"], 0)
        reopened = sqlite3.connect(database)
        reopened.row_factory = sqlite3.Row
        try:
            job = reopened.execute(
                "SELECT status, attempts FROM jobs WHERE id = ?",
                (records[0]["job_id"],),
            ).fetchone()
            self.assertEqual(job["status"], "pending")
            self.assertEqual(job["attempts"], 0)
            self.assertEqual(
                reopened.execute("SELECT COUNT(*) FROM label_runs").fetchone()[0], 0
            )
        finally:
            reopened.close()
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(FakeClient.instances[0].started_threads, [])
        self.assertEqual(FakeClient.instances[0].turns, [])

    async def test_official_capacity_provider_uses_same_client_all_controls_and_deadline(self):
        loaded = load_contract(self.contract)
        prepared = production_runner._prepare_episode_batch(  # noqa: SLF001
            loaded, self.items[:1]
        )
        deadline = self.root / "operator-deadline.json"
        deadline.write_text(
            pretty(
                {
                    "schema_version": "pif_canonical_v31_operator_deadline_v1",
                    "deadline_at": (
                        datetime.now(timezone.utc) + timedelta(hours=3)
                    ).isoformat(),
                    "production_contract_sha256": loaded["sha256"],
                    "issued_by": "operator",
                    "semantic_authority": False,
                }
            ),
            encoding="utf-8",
        )

        class CapacityClient:
            def __init__(self, secondary_used):
                self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
                self.secondary_used = secondary_used
                self.calls = []

            async def _request(self, method, params):
                self.calls.append((method, params))
                snapshot = {
                    "limitId": "codex",
                    "primary": {
                        "usedPercent": 0,
                        "resetsAt": None,
                        "windowDurationMins": 300,
                    },
                    "secondary": {
                        "usedPercent": self.secondary_used,
                        "resetsAt": None,
                        "windowDurationMins": 10080,
                    },
                    "individualLimit": {
                        "limit": "1000",
                        "used": "0",
                        "remainingPercent": 100,
                        "resetsAt": 4_102_444_800,
                    },
                    "rateLimitReachedType": None,
                }
                return {
                    "rateLimits": snapshot,
                    "rateLimitsByLimitId": {"codex": snapshot},
                    "rateLimitResetCredits": {"availableCount": 99},
                }

        client = CapacityClient(secondary_used=1)
        provider = production_runner.OfficialManagedAuthCapacityProvider(
            client=client,
            deadline_path=deadline,
            production_contract_sha256=loaded["sha256"],
        )
        verified = await production_runner._verify_capacity_admission(  # noqa: SLF001
            provider=provider,
            request=prepared["request"],
            capacity_path=self.root / "official-capacity.json",
            fixture_mode=False,
            remaining_batch_count=1,
            timeout_seconds=600,
        )
        self.assertEqual(
            client.calls, [("account/rateLimits/read", {})]
        )
        self.assertEqual(
            [row["name"] for row in verified["receipt"]["applicable_controls"]],
            ["primary", "secondary", "individual_limit"],
        )
        self.assertFalse(
            verified["receipt"]["reset_credits_counted_as_capacity"]
        )
        self.assertFalse(verified["receipt"]["semantic_authority"])
        denied_client = CapacityClient(secondary_used=99)
        denied = production_runner.OfficialManagedAuthCapacityProvider(
            client=denied_client,
            deadline_path=deadline,
            production_contract_sha256=loaded["sha256"],
        )
        with self.assertRaisesRegex(
            ProductionRunnerWaiting, "numeric reserve is insufficient"
        ):
            await denied(
                request=prepared["request"],
                remaining_batch_count=1,
                timeout_seconds=600,
            )
        self.assertEqual(
            denied_client.calls, [("account/rateLimits/read", {})]
        )


if __name__ == "__main__":
    unittest.main()
