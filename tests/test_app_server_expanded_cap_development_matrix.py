from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import MagicMock, patch

import pytest

from research_factory import app_server_expanded_cap_development_matrix as matrix
from research_factory import app_server_expanded_cap_episode_batch as adapter
from research_factory import app_server_llm_judge
from research_factory.util import sha256_text


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return path


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_record(path: Path) -> dict[str, Any]:
    return {"path": str(path.resolve()), "sha256": sha_file(path), "size_bytes": path.stat().st_size}


def usage(input_tokens: int = 100, output_tokens: int = 20) -> dict[str, int]:
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": min(10, input_tokens),
        "output_tokens": output_tokens,
        "reasoning_output_tokens": min(5, output_tokens),
        "total_tokens": input_tokens + output_tokens,
    }


def sum_usage(values: list[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(value[field]) for value in values) for field in matrix.USAGE_FIELDS}


def event_for(text: str, *, exact: bool = True) -> dict[str, Any]:
    return {
        "event_type": "capability_claim",
        "event_subtype": "fixture_capability",
        "claim_type": "descriptive",
        "claim_text": text,
        "evidence": text if exact else "not present in the source",
        "stance": "neutral",
        "certainty": "high",
        "temporal_horizon": "present",
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": [],
    }


def build_fixture_bundle(root: Path) -> dict[str, Any]:
    records: dict[str, Any] = {}
    for index in range(10):
        records[f"artifact_{index:02d}"] = artifact_record(
            write_json(root / "epoch6" / "records" / f"record-{index:02d}.json", {"i": index})
        )
    epoch_receipt = {
        "schema_version": "pif_semantic_plan_step_receipt_v1",
        "plan_epoch": 6,
        "step_id": "expanded_cap_full_event_direct_reference_reserve_v6",
        "state": "passed",
        "terminal_reason": "epoch6_reserve_aware_direct_reference_quality_passed",
        "development_quality_passed": True,
        "candidate_strict_full_field_macro_f1": 0.981013,
        "exact_evidence_rate": 1.0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": usage(1000, 100),
        "semantic_attempt_count": 1,
        "semantic_model_call_count": 3,
        "semantic_model_call_cap": 3,
        "semantic_retry_count": 0,
        "semantic_total_token_cap": 2000,
        "strict_thread_turn_call_accounting": True,
        "failed_checks": [],
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout": False,
        "holdout_authorized": False,
        "production": False,
        "production_mutated": False,
        "predecessor_epoch5_mutated": False,
        "records": records,
    }
    epoch_path = write_json(root / "epoch6" / "plan-step-receipt.json", epoch_receipt)

    calibration_records: dict[str, Any] = {}
    for key in ("final_fields", "protocol", "reference", "score", "truth"):
        calibration_records[key] = artifact_record(
            write_json(root / "calibration" / f"{key}.json", {"key": key})
        )
    calibration = {
        "schema_version": "pif_app_server_judge_v5_4_v174_terminal_v1",
        "state": "completed",
        "terminal_reason": "v174_full_development_calibration_passed_selection_authorized",
        "development_judge_frozen": True,
        "selection_authorized": True,
        "failed_quality_gates": [],
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": usage(200, 20),
        "semantic_retry_count": 0,
        "production_mutated": False,
        "holdout_authorized": False,
        "metrics": {
            "order_bias": 0.0,
            "support_abstention_count": 0,
            "alignment_abstention_case_count": 0,
        },
        **calibration_records,
    }
    calibration_path = write_json(root / "calibration" / "terminal.json", calibration)

    episodes: list[dict[str, Any]] = []
    manifest_episodes: list[dict[str, Any]] = []
    references: list[dict[str, Any]] = []
    for episode_index in range(4):
        episode_id = f"episode-{episode_index}"
        source_id = f"source-{episode_index}"
        prepared_segments: list[dict[str, Any]] = []
        manifest_segments: list[dict[str, Any]] = []
        for segment_index in range(8):
            segment_id = f"segment-{episode_index}-{segment_index}"
            text = (
                f"Fixture episode {episode_index} segment {segment_index} states a verified capability."
            )
            density = "no_signal" if segment_index == 7 else "dense"
            events = [] if density == "no_signal" else [event_for(text)]
            golden = {
                "segment_id": segment_id,
                "episode_id": episode_id,
                "discourse_events": events,
            }
            golden_sha = sha256_text(canonical(golden))
            label_id = f"label-{episode_index}-{segment_index}"
            label_run_id = f"run-{episode_index}-{segment_index}"
            text_sha = sha256_text(text)
            spec = {
                "segment_id": segment_id,
                "segment_index": segment_index,
                "text_sha256": text_sha,
                "density_stratum": density,
                "golden_event_count": len(events),
                "golden_output_sha256": golden_sha,
                "label_id": label_id,
                "label_run_id": label_run_id,
                "label_run_output_sha256": sha256_text(f"raw-{segment_id}"),
                "shared_reference_seed_id": f"seed-{episode_index}-{segment_index}",
            }
            manifest_segments.append(spec)
            references.append(
                {
                    "segment_id": segment_id,
                    "episode_id": episode_id,
                    "text_sha256": text_sha,
                    "golden_output_sha256": golden_sha,
                    "label_id": label_id,
                    "label_run_id": label_run_id,
                    "shared_reference_seed_id": spec["shared_reference_seed_id"],
                    "golden_output": golden,
                }
            )
            prepared_segments.append(
                {
                    "segment_id": segment_id,
                    "segment_text": text,
                    "density_stratum": density,
                    "boundaries": [
                        {
                            "window_id": 0,
                            "owner_start": 0,
                            "owner_end": len(text),
                            "extract_start": 0,
                            "extract_end": len(text),
                        }
                    ],
                    "fixture_event": copy.deepcopy(events[0]) if events else None,
                }
            )
        context = {
            "episode_id": episode_id,
            "source_name": f"Source {episode_index}",
            "episode_title": f"Episode {episode_index}",
            "context_summary": "Fixture authority context.",
            "speaker_map": [],
            "section_map": [],
            "entity_seed": {},
            "concept_seed": [],
            "extraction_guidance": "Enumerate source-supported events.",
            "excluded_source_context": [],
        }
        episodes.append(
            {
                "episode_id": episode_id,
                "source_name": context["source_name"],
                "episode_title": context["episode_title"],
                "episode_context": context,
                "segments": prepared_segments,
            }
        )
        manifest_episodes.append(
            {
                "episode_id": episode_id,
                "episode_title": context["episode_title"],
                "source_id": source_id,
                "source_name": context["source_name"],
                "segments": manifest_segments,
            }
        )
    seed = {"schema_version": "pif_shared_reference_seed_v1", "references": references}
    seed_path = write_json(root / "manifest" / "reference-seed.json", seed)
    manifest = {
        "schema_version": matrix.evaluation.APP_SERVER_DEVELOPMENT_MANIFEST_V2,
        "evaluation_role": "retrospective_balanced_development_not_acceptance",
        "episode_count": 4,
        "source_count": 4,
        "segment_count": 32,
        "unique_episode_segment_index_count": 32,
        "unique_text_sha256_count": 32,
        "density_counts": {"dense": 28, "no_signal": 4},
        "selection_policy": "fixture_no_transcript_semantic_rules",
        "event_cap": 48,
        "shared_reference_seed": {
            "artifact_path": str(seed_path.resolve()),
            "artifact_sha256": sha_file(seed_path),
            "reference_count": 32,
            "schema_version": "pif_shared_reference_seed_v1",
        },
        "episodes": manifest_episodes,
    }
    manifest_path = write_json(root / "manifest" / "manifest.json", manifest)

    def episode_loader(_conn: Any, *, manifest: Mapping[str, Any], **_kwargs: Any) -> Any:
        assert manifest["segment_count"] == 32
        return copy.deepcopy(episodes)

    return {
        "epoch_path": epoch_path,
        "epoch_sha": sha_file(epoch_path),
        "calibration_path": calibration_path,
        "calibration_sha": sha_file(calibration_path),
        "manifest_path": manifest_path,
        "manifest_sha": sha_file(manifest_path),
        "episodes": episodes,
        "episode_loader": episode_loader,
    }


class FixtureCapacityProvider:
    def __init__(self, *, stop_after: int | None = None, mismatch: bool = False) -> None:
        self.stop_after = stop_after
        self.mismatch = mismatch
        self.calls: list[str] = []

    async def __call__(self, context: Mapping[str, Any]) -> dict[str, Any]:
        if self.stop_after is not None and len(self.calls) >= self.stop_after:
            raise matrix.ExpandedCapMatrixCapacityUnavailable("fixture reserve stop")
        variant = str(context["variant_id"])
        self.calls.append(variant)
        issued = datetime.now(timezone.utc)
        episode_ids = [str(value) for value in context["episode_ids"]]
        receipt = {
            "schema_version": adapter.CAPACITY_ADMISSION_VERSION,
            "admission_id": f"fixture-admission-{variant}",
            "issued_at": issued.isoformat(),
            "expires_at": (issued + timedelta(minutes=10)).isoformat(),
            "state": "admitted",
            "issued_by": "evaluation_coordinator",
            "verification_mode": "fixture_verified",
            "fixture": True,
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "batch_size": int(context["batch_size"]) + int(self.mismatch),
            "thread_mode": context["thread_mode"],
            "model": adapter.MODEL,
            "effort": adapter.EFFORT,
            "concurrency": 1,
            "episode_ids_sha256": sha256_text(canonical(episode_ids)),
            "episode_count": len(episode_ids),
            "segment_count": 32,
            "batch_count": context["batch_count"],
            "live_capacity_available": True,
            "managed_chatgpt_auth_only": True,
            "verification_evidence_sha256": "e" * 64,
        }
        return {
            "receipt": receipt,
            "sha256": adapter.capacity_admission_sha256(receipt),
            "fixture_mode": True,
        }


class FixtureContextPreparationRunner:
    def __init__(self, *, deny_first_clearance: bool = False) -> None:
        self.deny_first_clearance = deny_first_clearance
        self.denied = False
        self.capacity_calls: list[str] = []
        self.turn_calls: list[str] = []
        self._capacity_root: Path | None = None

    async def __call__(
        self,
        *,
        conn: Any,
        episodes: list[Mapping[str, Any]],
        manifest_info: Mapping[str, Any],
        plan: Mapping[str, Any],
        output_dir: Path,
        timeout_seconds: float,
        fixture_mode: bool,
    ) -> dict[str, Any]:
        assert conn is None
        assert fixture_mode is True
        self._capacity_root = output_dir / "fixture-capacity"
        return await matrix.run_development_context_preparation(
            conn,
            episodes=episodes,
            manifest_info=manifest_info,
            plan=plan,
            output_dir=output_dir,
            capacity_clearance_provider=self.capacity_clearance,
            turn_runner=self.turn,
            timeout_seconds=timeout_seconds,
            fixture_mode=True,
        )

    async def capacity_clearance(self, context: Mapping[str, Any]) -> dict[str, Any]:
        episode_id = str(context["episode_id"])
        self.capacity_calls.append(episode_id)
        if self.deny_first_clearance and not self.denied:
            self.denied = True
            raise matrix.ExpandedCapMatrixCapacityUnavailable("fixture context reserve stop")
        assert self._capacity_root is not None
        record_path = write_json(
            self._capacity_root / f"{episode_id}-{len(self.capacity_calls):02d}.json",
            {
                "episode_id": episode_id,
                "remaining_turn_count": context["remaining_turn_count"],
                "thread_started": False,
                "turn_started": False,
            },
        )
        return {
            "state": "cleared_before_semantic_client_start",
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "thread_started": False,
            "turn_started": False,
            "remaining_turn_count": context["remaining_turn_count"],
            "capacity_preflight": artifact_record(record_path),
        }

    async def turn(
        self,
        *,
        episode_id: str,
        output_schema: Mapping[str, Any],
        sidecar_path: Path,
        output_path: Path,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.turn_calls.append(episode_id)
        properties = output_schema["properties"]
        result: dict[str, Any] = {
            "schema_version": matrix.CONTEXT_PREPARATION_DELTA_SCHEMA_VERSION,
            "episode_id": episode_id,
            "original_context_sha256": properties["original_context_sha256"]["const"],
        }
        requested = set(output_schema["required"]) - set(result)
        for field in requested:
            if field == "section_map":
                result[field] = [{"section": "fixture substantive discussion"}]
            elif field == "entity_seed":
                result[field] = {"fixture": {"role": "subject"}}
            elif field == "concept_seed":
                result[field] = ["fixture concept"]
            elif field == "extraction_guidance":
                result[field] = "Extract only substantive, source-supported fixture discourse events."
            elif field == "excluded_source_context":
                result[field] = ["promotional setup and non-substantive show framing"]
            else:  # pragma: no cover - schema and test helper must evolve together
                raise AssertionError(field)
        write_json(output_path, result)
        measured = usage(40, 10)
        write_json(
            sidecar_path,
            {
                "state": "completed",
                "status": "completed",
                "usage": measured,
                "usage_complete": True,
                "wall_elapsed_seconds": 0.5,
            },
        )
        return {
            "status_ok": True,
            "managed_chatgpt_auth_verified": True,
            "official_managed_app_server_sidecar_valid": False,
            "thread_id": f"context-thread-{episode_id}",
            "turn_id": f"context-turn-{episode_id}",
            "usage": measured,
            "wall_elapsed_seconds": 0.5,
            "output": result,
        }


class FixtureArmRunner:
    def __init__(self, *, same_cost: bool = False, inexact_events: bool = False) -> None:
        self.same_cost = same_cost
        self.inexact_events = inexact_events
        self.calls: list[str] = []

    async def __call__(
        self,
        episodes: list[Mapping[str, Any]],
        *,
        output_dir: Path,
        batch_size: int,
        thread_mode: str,
        capacity_admission: Mapping[str, Any],
        capacity_admission_sha256: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        variant = f"batch_{batch_size}_{thread_mode}"
        assert variant not in self.calls
        self.calls.append(variant)
        output_dir.mkdir(parents=True, exist_ok=False)
        write_json(output_dir / "capacity-admission.json", capacity_admission)
        binding = {
            "schema_version": adapter.CAPACITY_BINDING_VERSION,
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "state": "verified_before_app_server_start",
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "contract_sha256": adapter.capacity_admission_contract()["contract_sha256"],
            "expected_canonical_sha256": capacity_admission_sha256,
            "observed_canonical_sha256": capacity_admission_sha256,
            "capacity_admission": artifact_record(output_dir / "capacity-admission.json"),
            "fixture": True,
        }
        binding_path = write_json(output_dir / "capacity-binding.json", binding)
        requests = [
            request
            for episode in episodes
            for request in adapter.prepare_episode_batches(
                episode, batch_size=batch_size, thread_mode=thread_mode
            )
        ]
        rank = list(matrix.EXPECTED_ARMS).index((batch_size, thread_mode))
        per_turn = usage(100 if self.same_cost else 100 + rank, 20)
        telemetry = []
        for index, request in enumerate(requests):
            telemetry.append(
                {
                    "batch_id": request["batch_id"],
                    "attempted": True,
                    "attempt_contract_valid": True,
                    "sidecar_contract_valid": True,
                    "thread_matches_attempt": True,
                    "usage_status": "complete",
                    "usage": copy.deepcopy(per_turn),
                    "wall_elapsed_seconds": 1.0,
                    "turn_id": f"turn-{variant}-{index}",
                    "thread_id": (
                        f"thread-{variant}-{request['episode_id']}"
                        if thread_mode == "same_thread"
                        else f"thread-{variant}-{index}"
                    ),
                }
            )
        aggregate = sum_usage([per_turn for _ in requests])
        cases = []
        for episode in episodes:
            for segment in episode["segments"]:
                fixture_event = segment.get("fixture_event")
                events = [] if fixture_event is None else [copy.deepcopy(fixture_event)]
                if events and self.inexact_events:
                    events[0]["evidence"] = "not present in the source"
                cases.append(
                    {
                        "segment_id": segment["segment_id"],
                        "status": "coded" if events else "no_signal",
                        "events": events,
                    }
                )
        return {
            "schema_version": adapter.RUN_REPORT_VERSION,
            "state": "passed",
            "development_regression_eligible": True,
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "model": adapter.MODEL,
            "effort": adapter.EFFORT,
            "max_events_per_segment": adapter.MAX_EVENTS_PER_SEGMENT,
            "semantic_postprocessing": False,
            "all_emitted_events_preserved": True,
            "exact_identity_duplicates_are_diagnostic_only": True,
            "retry_count": 0,
            "ambiguous_retry_count": 0,
            "ambiguous_outcome_attempts": 0,
            "requested_calls": len(requests),
            "attempted_calls": len(requests),
            "validated_calls": len(requests),
            "terminal_sidecars": len(requests),
            "usage_measured_attempts": len(requests),
            "usage_unknown_attempts": 0,
            "attempt_contract_failures": 0,
            "sidecar_contract_failures": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": aggregate,
            "cached_input_tokens": aggregate["cached_input_tokens"],
            "reasoning_output_tokens": aggregate["reasoning_output_tokens"],
            "turn_ids_unique": True,
            "thread_lineage_valid": True,
            "preflight_lineage_valid": True,
            "capacity_binding_valid": True,
            "capacity_binding": artifact_record(binding_path),
            "fixture_mode": True,
            "wall_elapsed_seconds": float(len(requests)),
            "turn_wall_elapsed_seconds_sum": float(len(requests)),
            "turn_telemetry": telemetry,
            "fixture_cases": cases,
        }


def fixture_output_loader(*, report: Mapping[str, Any], **_kwargs: Any) -> dict[str, Any]:
    return {"cases": copy.deepcopy(report["fixture_cases"]), "artifacts": []}


async def fixture_judge_runner(
    *, pool: Mapping[str, Any], calibration: Mapping[str, Any], **_kwargs: Any
) -> dict[str, Any]:
    cases = []
    for pool_case in pool["cases"]:
        witness_ids = [
            witness["witness_id"]
            for side in ("a", "b")
            for witness in pool_case[f"event_set_{side}"]
        ]
        cases.append(
            {
                "case_id": pool_case["case_id"],
                "status": "agreed",
                "support_results": [
                    {"witness_id": witness_id, "verdict": "supported"}
                    for witness_id in witness_ids
                ],
                "equivalence_groups": [[witness_id] for witness_id in witness_ids],
                "partition_abstained_witness_ids": [],
                "alignment_results": [],
                "alignment_abstained_witness_ids": [],
            }
        )
    consensus = {
        "schema_version": app_server_llm_judge.JUDGE_CONSENSUS_VERSION,
        "pool_sha256": sha256_text(canonical(pool)),
        "cases": cases,
        "abstentions": {},
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }
    return {
        "state": "completed",
        "calibration_receipt_sha256": calibration["sha256"],
        "orientations": ["ab", "ba"],
        "case_order": [case["case_id"] for case in pool["cases"]],
        "abstention_enabled": True,
        "capacity_admission_verified": True,
        "retry_count": 0,
        "accounting_complete": True,
        "usage": usage(1000, 100),
        "wall_elapsed_seconds": 2.0,
        "consensus": consensus,
    }


def run_fixture_matrix(
    bundle: Mapping[str, Any],
    output_dir: Path,
    provider: FixtureCapacityProvider,
    runner: FixtureArmRunner,
    *,
    context_runner: FixtureContextPreparationRunner | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        matrix.run_expanded_cap_development_matrix(
            None,
            output_dir=output_dir,
            capacity_admission_provider=provider,
            epoch6_receipt_path=bundle["epoch_path"],
            epoch6_receipt_sha256=bundle["epoch_sha"],
            manifest_path=bundle["manifest_path"],
            manifest_sha256=bundle["manifest_sha"],
            judge_calibration_path=bundle["calibration_path"],
            judge_calibration_sha256=bundle["calibration_sha"],
            cost_contract={
                "baseline_segments": 32,
                "baseline_usage": usage(1_000_000, 100_000),
                "production_amortized_context_usage": usage(0, 0),
                "formula": "fixture exact production amortization",
            },
            fixture_mode=True,
            episode_loader=bundle["episode_loader"],
            context_preparation_runner=context_runner,
            arm_runner=runner,
            arm_output_loader=fixture_output_loader,
            judge_runner=fixture_judge_runner,
            allowed_record_root=Path(bundle["epoch_path"]).parents[1],
        )
    )


def test_default_frozen_inputs_are_checksum_bound() -> None:
    assert matrix.verify_epoch6_passed_receipt()["verified_record_count"] == 32
    assert len(matrix.verify_development_manifest()["case_order"]) == 32
    assert matrix.verify_judge_calibration_receipt()["verified_record_count"] == 5


def test_dry_preflight_is_six_arm_no_model_surface(tmp_path: Path) -> None:
    bundle = build_fixture_bundle(tmp_path)
    preflight = matrix.build_matrix_dry_preflight(
        None,
        epoch6_receipt_path=bundle["epoch_path"],
        epoch6_receipt_sha256=bundle["epoch_sha"],
        manifest_path=bundle["manifest_path"],
        manifest_sha256=bundle["manifest_sha"],
        judge_calibration_path=bundle["calibration_path"],
        judge_calibration_sha256=bundle["calibration_sha"],
        episode_loader=bundle["episode_loader"],
        allowed_record_root=tmp_path,
    )
    assert preflight["arm_count"] == 6
    assert preflight["exact_extraction_turn_count"] == 48
    assert [row["request_count"] for row in preflight["arms"]] == [12, 12, 8, 8, 4, 4]
    assert preflight["capacity_probe_performed"] is False
    assert preflight["semantic_client_started"] is False


def test_dry_preflight_reports_exact_context_delta_plan_without_adapter_calls(
    tmp_path: Path,
) -> None:
    bundle = build_fixture_bundle(tmp_path)
    for episode in bundle["episodes"]:
        episode["episode_context"].pop("excluded_source_context")
    with patch.object(
        matrix.adapter,
        "prepare_episode_batches",
        side_effect=AssertionError("adapter preparation must wait for context authority"),
    ):
        preflight = matrix.build_matrix_dry_preflight(
            None,
            epoch6_receipt_path=bundle["epoch_path"],
            epoch6_receipt_sha256=bundle["epoch_sha"],
            manifest_path=bundle["manifest_path"],
            manifest_sha256=bundle["manifest_sha"],
            judge_calibration_path=bundle["calibration_path"],
            judge_calibration_sha256=bundle["calibration_sha"],
            episode_loader=bundle["episode_loader"],
            allowed_record_root=tmp_path,
        )
    assert preflight["state"] == (
        "context_preparation_required_no_capacity_probe_or_semantic_work"
    )
    assert preflight["context_semantic_turn_count"] == 4
    assert preflight["context_preparation"]["episode_ids_requiring_semantic_turn"] == [
        f"episode-{index}" for index in range(4)
    ]
    assert all(
        row["missing_fields"] == ["excluded_source_context"]
        for row in preflight["context_preparation"]["episodes"]
    )
    assert [row["request_count"] for row in preflight["arms"]] == [12, 12, 8, 8, 4, 4]
    assert preflight["exact_extraction_turn_count"] == 48
    assert preflight["capacity_probe_performed"] is False
    assert preflight["semantic_client_started"] is False
    assert preflight["extraction_turn_identity_frozen"] is False


def test_context_delta_phase_is_no_retry_resumable_and_preserves_existing_authority(
    tmp_path: Path,
) -> None:
    bundle = build_fixture_bundle(tmp_path / "bundle")
    originals: dict[str, dict[str, Any]] = {}
    for episode in bundle["episodes"]:
        context = episode["episode_context"]
        context.pop("excluded_source_context")
        originals[str(episode["episode_id"])] = copy.deepcopy(context)
    manifest_info = matrix.verify_development_manifest(
        bundle["manifest_path"], expected_sha256=bundle["manifest_sha"]
    )
    episodes = matrix.prepare_development_episodes(
        None,
        manifest=manifest_info["payload"],
        manifest_rows=manifest_info["rows"],
        episode_loader=bundle["episode_loader"],
    )
    plan = matrix.inspect_development_context_preparation(
        episodes=episodes,
        manifest_info=manifest_info,
        require_manifest_artifacts=False,
    )
    output = tmp_path / "matrix" / "context-preparation"
    runner = FixtureContextPreparationRunner(deny_first_clearance=True)
    with pytest.raises(matrix.ExpandedCapMatrixCapacityUnavailable):
        asyncio.run(
            runner(
                conn=None,
                episodes=episodes,
                manifest_info=manifest_info,
                plan=plan,
                output_dir=output,
                timeout_seconds=10,
                fixture_mode=True,
            )
        )
    assert list((output / "episodes").glob("*/attempt.json")) == []
    assert runner.turn_calls == []

    report = asyncio.run(
        runner(
            conn=None,
            episodes=episodes,
            manifest_info=manifest_info,
            plan=plan,
            output_dir=output,
            timeout_seconds=10,
            fixture_mode=True,
        )
    )
    assert report["attempted_calls"] == 4
    assert report["retry_count"] == 0
    expected_context_usage = sum_usage([usage(40, 10) for _ in range(4)])
    assert report["usage"] == expected_context_usage
    assert report["cost_treatment"]["included_in_epoch7_total_accounting"] is True
    assert (
        report["cost_treatment"]["added_to_production_amortized_context_numerator"]
        is False
    )
    validated = matrix._validate_context_preparation_report(
        report,
        plan=plan,
        episodes=episodes,
        output_dir=output,
        fixture_mode=True,
    )
    for episode in validated["episodes"]:
        episode_id = str(episode["episode_id"])
        overlay = episode["episode_context"]
        for field, value in originals[episode_id].items():
            assert canonical(overlay[field]) == canonical(value)
        assert overlay["excluded_source_context"] == [
            "promotional setup and non-substantive show framing"
        ]
    calls_before_adoption = list(runner.turn_calls)
    adopted = asyncio.run(
        runner(
            conn=None,
            episodes=episodes,
            manifest_info=manifest_info,
            plan=plan,
            output_dir=output,
            timeout_seconds=10,
            fixture_mode=True,
        )
    )
    assert adopted == report
    assert runner.turn_calls == calls_before_adoption
    accounting = matrix._partial_matrix_accounting(tmp_path / "matrix")
    assert accounting["semantic_model_call_count"] == 4
    assert accounting["usage"] == expected_context_usage
    assert accounting["completed_context_report_count"] == 1


def test_context_delta_never_replaces_present_invalid_semantic_authority(
    tmp_path: Path,
) -> None:
    bundle = build_fixture_bundle(tmp_path)
    bundle["episodes"][0]["episode_context"]["excluded_source_context"] = None
    with pytest.raises(
        matrix.ExpandedCapDevelopmentMatrixError,
        match="invalid and cannot be replaced",
    ):
        matrix.build_matrix_dry_preflight(
            None,
            epoch6_receipt_path=bundle["epoch_path"],
            epoch6_receipt_sha256=bundle["epoch_sha"],
            manifest_path=bundle["manifest_path"],
            manifest_sha256=bundle["manifest_sha"],
            judge_calibration_path=bundle["calibration_path"],
            judge_calibration_sha256=bundle["calibration_sha"],
            episode_loader=bundle["episode_loader"],
            allowed_record_root=tmp_path,
        )


def test_matrix_run_accounts_context_delta_without_changing_production_allocation(
    tmp_path: Path,
) -> None:
    bundle = build_fixture_bundle(tmp_path / "bundle")
    for episode in bundle["episodes"]:
        episode["episode_context"].pop("excluded_source_context")
    context_runner = FixtureContextPreparationRunner()
    output = tmp_path / "matrix"
    result = run_fixture_matrix(
        bundle,
        output,
        FixtureCapacityProvider(),
        FixtureArmRunner(),
        context_runner=context_runner,
    )
    expected_context_usage = sum_usage([usage(40, 10) for _ in range(4)])
    context = result["development_context_preparation"]
    assert context["required"] is True
    assert context["semantic_turn_count"] == 4
    assert context["usage"] == expected_context_usage
    treatment = context["cost_treatment"]
    assert treatment["measured_development_schema_delta_validation_usage"] == (
        expected_context_usage
    )
    assert treatment["added_to_production_amortized_context_numerator"] is False
    assert treatment["frozen_production_amortized_context_usage_retained"] == usage(0, 0)
    assert result["ambient_instruction_isolation"][
        "project_instruction_content_byte_budget"
    ] == 0
    assert result["ambient_instruction_isolation"][
        "project_instruction_content_included"
    ] is False
    assert len(context_runner.turn_calls) == 4
    assert all(
        f"development_context_overlay_episode-{index}"
        in result["frozen_artifact_hashes"]
        for index in range(4)
    )
    accounting = matrix._partial_matrix_accounting(output)
    arm_usage = [
        json.loads(path.read_text())["usage"]
        for path in sorted((output / "arms").glob("*/report.json"))
    ]
    assert accounting["usage"] == sum_usage([expected_context_usage, *arm_usage])
    assert accounting["semantic_model_call_count"] == 4 + sum(
        json.loads(path.read_text())["attempted_calls"]
        for path in sorted((output / "arms").glob("*/report.json"))
    )


def test_receipt_manifest_and_admission_mismatch_fail_before_arm(tmp_path: Path) -> None:
    bundle = build_fixture_bundle(tmp_path)
    with pytest.raises(matrix.ExpandedCapDevelopmentMatrixError, match="checksum binding"):
        matrix.verify_epoch6_passed_receipt(
            bundle["epoch_path"], expected_sha256="0" * 64, allowed_record_root=tmp_path
        )
    with pytest.raises(matrix.ExpandedCapDevelopmentMatrixError, match="checksum binding"):
        matrix.verify_development_manifest(bundle["manifest_path"], expected_sha256="0" * 64)

    runner = FixtureArmRunner()
    with pytest.raises(matrix.ExpandedCapDevelopmentMatrixError, match="admission failed"):
        run_fixture_matrix(
            bundle,
            tmp_path / "mismatch-matrix",
            FixtureCapacityProvider(mismatch=True),
            runner,
        )
    assert runner.calls == []


def test_capacity_stop_after_two_arms_resumes_without_replay(tmp_path: Path) -> None:
    bundle = build_fixture_bundle(tmp_path)
    output = tmp_path / "matrix"
    runner = FixtureArmRunner()
    first_provider = FixtureCapacityProvider(stop_after=2)
    with pytest.raises(matrix.ExpandedCapMatrixCapacityUnavailable):
        run_fixture_matrix(bundle, output, first_provider, runner)
    assert runner.calls == ["batch_3_new_thread", "batch_3_same_thread"]
    first_turn_ids = {
        row["turn_id"]
        for report_path in sorted((output / "arms").glob("*/report.json"))
        for row in json.loads(report_path.read_text())["turn_telemetry"]
    }

    second_provider = FixtureCapacityProvider()
    result = run_fixture_matrix(bundle, output, second_provider, runner)
    assert result["schema_version"] == matrix.FROZEN_WINNER_VERSION
    assert result["winner_frozen"] is True
    assert result["holdout_inspected"] is False
    assert result["production_mutated"] is False
    assert runner.calls == [
        "batch_3_new_thread",
        "batch_3_same_thread",
        "batch_5_new_thread",
        "batch_5_same_thread",
        "batch_8_new_thread",
        "batch_8_same_thread",
    ]
    assert second_provider.calls == [
        "batch_5_new_thread",
        "batch_5_same_thread",
        "batch_8_new_thread",
        "batch_8_same_thread",
    ]
    all_turn_ids = [
        row["turn_id"]
        for report_path in sorted((output / "arms").glob("*/report.json"))
        for row in json.loads(report_path.read_text())["turn_telemetry"]
    ]
    assert len(all_turn_ids) == len(set(all_turn_ids))
    assert first_turn_ids.issubset(all_turn_ids)
    loaded = matrix.load_frozen_winner(output / "frozen-winner-v3.json")
    assert loaded["payload"] == result


def test_quality_exactness_and_unique_cost_tie_fail_closed(tmp_path: Path) -> None:
    bundle = build_fixture_bundle(tmp_path / "inexact")
    with pytest.raises(matrix.ExpandedCapDevelopmentMatrixError, match="no arm passed"):
        run_fixture_matrix(
            bundle,
            tmp_path / "inexact-matrix",
            FixtureCapacityProvider(),
            FixtureArmRunner(inexact_events=True),
        )

    bundle = build_fixture_bundle(tmp_path / "tie")
    with pytest.raises(matrix.ExpandedCapDevelopmentMatrixError, match="winner is not unique"):
        run_fixture_matrix(
            bundle,
            tmp_path / "tie-matrix",
            FixtureCapacityProvider(),
            FixtureArmRunner(same_cost=True),
        )


def test_live_capacity_denial_is_no_thread_and_emits_no_admission(tmp_path: Path) -> None:
    async def denied_probe(**_kwargs: Any) -> dict[str, Any]:
        return {
            "schema_version": "pif_app_server_rate_limit_probe_v1",
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "limit_id": "codex",
            "primary_used_percent": 99,
            "primary_resets_at": None,
            "rate_limit_reached_type": None,
            "maximum_primary_used_percent": 100,
            "cleared_for_semantic_work": True,
            "thread_started": False,
            "turn_started": False,
        }

    coordinator = matrix.LiveMatrixCapacityCoordinator(
        control_dir=tmp_path / "control",
        rate_limit_probe=denied_probe,
        semantic_client_factory=lambda: (_ for _ in ()).throw(
            AssertionError("semantic client must not start")
        ),
    )
    with pytest.raises(matrix.ExpandedCapMatrixCapacityUnavailable):
        asyncio.run(
            coordinator.arm_capacity_admission(
                {
                    "variant_id": "batch_3_new_thread",
                    "batch_size": 3,
                    "thread_mode": "new_thread",
                    "winner_system_id": adapter.WINNER_SYSTEM_ID,
                    "model": adapter.MODEL,
                    "effort": adapter.EFFORT,
                    "concurrency": 1,
                    "episode_ids": ["a", "b", "c", "d"],
                    "episode_count": 4,
                    "segment_count": 32,
                    "batch_count": 12,
                    "fixture_mode": False,
                }
            )
        )
    assert list((tmp_path / "control" / "admissions").glob("*.json")) == []
    probe = json.loads(next((tmp_path / "control" / "probes").glob("*/*.json")).read_text())
    assert probe["state"] == "waiting_before_semantic_client_start"
    assert probe["semantic_client_started"] is False


def test_cli_dry_preflight_does_not_construct_live_coordinator(tmp_path: Path, capsys: Any) -> None:
    fake_preflight = {
        "schema_version": matrix.MATRIX_VERSION,
        "state": "dry_preflight_passed_no_capacity_probe_or_semantic_work",
        "arm_count": 6,
        "exact_extraction_turn_count": 48,
        "episodes": [{"private": True}],
        "manifest_info": {"private": True},
    }
    connection = MagicMock()
    with (
        patch.object(matrix, "_reject_external_auth_material"),
        patch.object(matrix, "_open_read_only_db", return_value=connection),
        patch.object(matrix, "build_matrix_dry_preflight", return_value=fake_preflight),
        patch.object(
            matrix,
            "LiveMatrixCapacityCoordinator",
            side_effect=AssertionError("dry preflight must not construct live capacity"),
        ),
    ):
        assert matrix.main(["preflight", "--db", str(tmp_path / "unused.sqlite")]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["arm_count"] == 6
    assert "episodes" not in output
    assert "manifest_info" not in output
