from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_development_matrix as matrix
from research_factory import app_server_canonical_v31_episode_batch as adapter
from research_factory.util import sha256_text


def _write(path: Path, value: object) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n")
    return matrix._record(path)


def _quality(text: str, index: int) -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": len(text.split()),
        "transcript_preparation_id": f"prep_matrix_{index}",
    }


def _no_signal_label(*, episode_id: str, segment_id: str, quality: dict) -> dict:
    return {
        "schema_version": adapter.CANONICAL_LABEL_PACK,
        "segment_id": segment_id,
        "episode_id": episode_id,
        "extraction_status": "no_signal",
        "segment_quality": copy.deepcopy(quality),
        "segment_source_context": {
            "kind": "show_setup",
            "confidence": 1.0,
            "rationale": "This fixture contains only neutral show setup.",
        },
        "discourse_events": [],
        "concept_candidates": [],
        "rejected_candidates": [],
        "no_signal_reason": "No grounded canonical discourse event is present.",
        "overall_confidence": 1.0,
        "needs_review": False,
        "review_reason": None,
    }


def _fixture(tmp_path: Path) -> dict:
    episode_id = "ep_matrix_fixture"
    example = json.loads(adapter.PROJECT_ROOT.joinpath(
        "label_packs/ai_discourse_v3_1/examples.json"
    ).read_text())[0]
    coded_text = example["input"]["text"]
    segments = []
    labels = []
    for index in range(9):
        segment_id = f"seg_matrix_{index}"
        text = coded_text if index == 0 else f"HOST: Welcome to neutral setup number {index}."
        quality = _quality(text, index)
        segments.append(
            {
                "segment_id": segment_id,
                "segment_text": text,
                "segment_quality": quality,
                "density_stratum": "coded" if index == 0 else "no_signal",
                "boundaries": [
                    {
                        "window_id": 0,
                        "chunk_index": 0,
                        "owner_start": 0,
                        "owner_end": len(text),
                        "extract_start": 0,
                        "extract_end": len(text),
                    }
                ],
            }
        )
        if index == 0:
            label = copy.deepcopy(example["output"])
            label["segment_id"] = segment_id
            label["episode_id"] = episode_id
            label["segment_quality"] = copy.deepcopy(quality)
        else:
            label = _no_signal_label(
                episode_id=episode_id,
                segment_id=segment_id,
                quality=quality,
            )
        labels.append(label)
    episode = {
        "episode_id": episode_id,
        "source_name": "Canonical Matrix Fixture",
        "episode_title": "Full v3.1 coded and no-signal development packet",
        "context_summary": "One coded example and eight neutral setup segments.",
        "speaker_map": [
            {"name": "HOST", "role": "host"},
            {"name": "GUEST", "role": "guest"},
        ],
        "section_map": [
            {
                "section_id": "fixture",
                "segment_ids": [segment["segment_id"] for segment in segments],
            }
        ],
        "entity_seed": {"organizations": ["Google"]},
        "concept_seed": ["frontier_ai_end_state_language"],
        "extraction_guidance": "Evaluate every canonical v3.1 field from source evidence.",
        "excluded_source_context": ["show setup when unsupported"],
        "segments": segments,
    }
    context_payload = {
        "schema_version": matrix.CONTEXT_ARTIFACT_VERSION,
        "episode_id": episode_id,
        "episode_context": {
            field: copy.deepcopy(episode[field]) for field in matrix._CONTEXT_FIELDS
        },
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
    }
    context_record = _write(tmp_path / "context.json", context_payload)
    case_order = [f"case_{index:02d}_opaque" for index in range(9)]
    references = [
        {
            "opaque_case_id": case_order[index],
            "segment_id": segment["segment_id"],
            "episode_id": episode_id,
            "text_sha256": sha256_text(segment["segment_text"]),
            "label": copy.deepcopy(labels[index]),
        }
        for index, segment in enumerate(segments)
    ]
    reference_payload = {
        "schema_version": matrix.REFERENCE_SEED_VERSION,
        "case_order": case_order,
        "one_shared_reference_for_all_arms": True,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "references": references,
    }
    reference_record = _write(tmp_path / "reference-seed.json", reference_payload)
    manifest_segments = [
        {
            "segment_id": segment["segment_id"],
            "segment_index": index,
            "opaque_case_id": case_order[index],
            "text_sha256": sha256_text(segment["segment_text"]),
            "segment_quality_sha256": sha256_text(
                matrix._canonical_json(segment["segment_quality"])
            ),
            "density_stratum": segment["density_stratum"],
        }
        for index, segment in enumerate(segments)
    ]
    manifest_payload = {
        "schema_version": matrix.MANIFEST_VERSION,
        "evaluation_role": "full_canonical_v31_development_only",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "case_count": 9,
        "episode_count": 1,
        "holdout_excluded": True,
        "production_excluded": True,
        "selection_policy": "opaque_identity_only_no_transcript_semantic_selection_rules",
        "semantic_deterministic_pruning": False,
        "semantic_deterministic_defaults": {},
        "episodes": [
            {
                "episode_id": episode_id,
                "source_id": "source_matrix_fixture",
                "context_artifact": context_record,
                "segments": manifest_segments,
            }
        ],
        "opaque_case_order": case_order,
        "shared_reference_seed": reference_record,
    }
    manifest_record = _write(tmp_path / "manifest.json", manifest_payload)
    capacity_payload = {
        "schema_version": matrix.CAPACITY_POLICY_VERSION,
        "state": "frozen_offline_policy",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "official_app_server_initialized_before_capacity": True,
        "admission_required_before_thread_start": True,
        "admission_required_before_thread_resume": True,
        "admission_required_before_turn_start": True,
        "admission_must_bind_future_plan_and_directive": True,
        "complete_usage_cache_reasoning_wall_required": True,
        "unknown_usage_hard_stop": True,
        "minimum_remaining_reserve_percent": 20,
        "capacity_safety_margin_percent": 10,
        "quota_points_per_million_tokens": 1_250,
        "quota_calibration_id": "canonical_v31_epoch7_conservative_calibration_v1",
        "quota_calibration_conservative_uplift_applied": True,
        "maximum_total_tokens_per_turn": 2_000,
        "maximum_wall_seconds_per_turn": 10.0,
        "maximum_rate_limit_snapshot_age_seconds": 30,
        "operator_wall_deadline_safety_margin_seconds": 1.0,
        "token_capacity_is_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_required": True,
        "postturn_measured_stop_required": True,
        "semantic_retry_count": 0,
        "required_arms": [
            {"batch_size": size, "thread_mode": mode}
            for size, mode in matrix.EXPECTED_ARMS
        ],
        "full_canonical_v31_outputs_required": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    capacity_record = _write(tmp_path / "capacity-policy.json", capacity_payload)
    return {
        "episodes": [episode],
        "labels": labels,
        "case_order": case_order,
        "references": references,
        "manifest_record": manifest_record,
        "capacity_record": capacity_record,
        "context_record": context_record,
        "reference_record": reference_record,
        "root": tmp_path,
    }


def _preflight(data: dict) -> dict:
    return matrix.build_matrix_dry_preflight(
        manifest_path=Path(data["manifest_record"]["path"]),
        manifest_sha256=data["manifest_record"]["sha256"],
        episodes=data["episodes"],
        capacity_policy_path=Path(data["capacity_record"]["path"]),
        capacity_policy_sha256=data["capacity_record"]["sha256"],
        allowed_root=data["root"],
    )


def _directive(plan: dict, now: datetime) -> dict:
    binding = plan["binding"]
    return {
        "schema_version": matrix.FUTURE_DIRECTIVE_VERSION,
        "state": "explicit_live_development_matrix_authorization",
        "authorized_by": "offline-fixture-operator",
        "issued_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=30)).isoformat(),
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "future_plan_binding_sha256": plan["binding_sha256"],
        "precommit_sha256": binding["precommit_sha256"],
        "manifest_sha256": binding["manifest_sha256"],
        "context_set_sha256": binding["context_set_sha256"],
        "runtime_binding_sha256": binding["runtime_binding_sha256"],
        "context_control_overlay_sha256": binding["context_control_overlay_sha256"],
        "instruction_source_contract_sha256": binding[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": binding["capacity_policy_sha256"],
        "quality_evaluator_contract_sha256": binding[
            "quality_evaluator_contract_sha256"
        ],
        "arms": copy.deepcopy(binding["arms"]),
        "live_semantic_dispatch_authorized": True,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "full_canonical_v31_outputs_required": True,
        "semantic_retry_count": 0,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _outputs(data: dict) -> dict:
    return {
        matrix._variant_id(size, mode): [
            {"opaque_case_id": case_id, "label": copy.deepcopy(label)}
            for case_id, label in zip(data["case_order"], data["labels"])
        ]
        for size, mode in matrix.EXPECTED_ARMS
    }


def test_dry_preflight_materializes_exact_six_arms_without_dispatch(
    tmp_path: Path, monkeypatch
) -> None:
    data = _fixture(tmp_path)

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("live model path must not be called")

    monkeypatch.setattr(adapter, "run_episode_batch_arm", _forbidden)
    monkeypatch.setattr(adapter, "_client_factory", _forbidden)
    preflight = _preflight(data)
    receipt = preflight["receipt"]
    assert receipt["state"] == "passed_offline_no_auth_capacity_probe_thread_or_turn"
    assert receipt["arm_count"] == 6
    assert receipt["case_count"] == 9
    assert receipt["capacity_probe_performed"] is False
    assert receipt["semantic_client_started"] is False
    assert receipt["thread_started"] is False
    assert receipt["turn_started"] is False
    assert receipt["live_dispatch_authorized"] is False
    assert [row["request_count"] for row in receipt["arms"]] == [3, 3, 2, 2, 2, 2]
    assert all(
        row["opaque_case_order_sha256"] == receipt["opaque_case_order_sha256"]
        for row in receipt["arms"]
    )


def test_manifest_context_reference_capacity_and_overlay_tamper_fail_closed(
    tmp_path: Path, monkeypatch
) -> None:
    data = _fixture(tmp_path)
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="manifest"):
        matrix.build_matrix_dry_preflight(
            manifest_path=Path(data["manifest_record"]["path"]),
            manifest_sha256="0" * 64,
            episodes=data["episodes"],
            capacity_policy_path=Path(data["capacity_record"]["path"]),
            capacity_policy_sha256=data["capacity_record"]["sha256"],
            allowed_root=tmp_path,
        )
    Path(data["context_record"]["path"]).write_text("{}\n")
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="context"):
        _preflight(data)
    data = _fixture(tmp_path / "reference")
    Path(data["reference_record"]["path"]).write_text("{}\n")
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="reference"):
        _preflight(data)
    data = _fixture(tmp_path / "capacity")
    capacity = json.loads(Path(data["capacity_record"]["path"]).read_text())
    capacity["managed_chatgpt_plan_type"] = "plus"
    data["capacity_record"] = _write(Path(data["capacity_record"]["path"]), capacity)
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="capacity"):
        _preflight(data)
    data = _fixture(tmp_path / "old-capacity-boundary")
    capacity = json.loads(Path(data["capacity_record"]["path"]).read_text())
    capacity["admission_required_before_app_server_start"] = True
    data["capacity_record"] = _write(Path(data["capacity_record"]["path"]), capacity)
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="capacity"):
        _preflight(data)
    data = _fixture(tmp_path / "overlay")
    monkeypatch.setitem(adapter.CONTEXT_CONTROL_OVERLAY, "project_doc_max_bytes", 1)
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError, match="zero-byte"):
        _preflight(data)


def test_precommit_is_immutable_and_future_plan_binding_creates_nothing(
    tmp_path: Path,
) -> None:
    data = _fixture(tmp_path / "fixture")
    bundle = matrix.build_matrix_precommit(_preflight(data))
    target = tmp_path / "frozen" / "matrix-precommit.json"
    frozen = matrix.freeze_matrix_precommit(target, bundle)
    assert frozen["record"]["path"] == str(target.resolve())
    assert matrix.freeze_matrix_precommit(target, bundle)["record"] == frozen["record"]
    drifted = copy.deepcopy(bundle)
    drifted["precommit"]["case_count"] += 1
    drifted["precommit_sha256"] = sha256_text(
        matrix._canonical_json(drifted["precommit"])
    )
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="immutable"):
        matrix.freeze_matrix_precommit(target, drifted)
    output_root = tmp_path / "future-output-must-not-exist"
    plan = matrix.build_future_plan_binding(bundle, output_root=output_root)
    assert plan["binding"]["plan_file_created"] is False
    assert plan["binding"]["plan_installed"] is False
    assert plan["binding"]["live_dispatch_authorized"] is False
    assert not output_root.exists()


def test_future_dispatch_requires_fresh_exact_checksum_bound_directive(
    tmp_path: Path,
) -> None:
    data = _fixture(tmp_path / "fixture")
    precommit = matrix.build_matrix_precommit(_preflight(data))
    plan = matrix.build_future_plan_binding(precommit, output_root=tmp_path / "future")
    now = datetime(2026, 7, 18, 18, 0, tzinfo=timezone.utc)
    with pytest.raises(matrix.FutureExecutionDirectiveRequired):
        matrix.require_future_execution_directive(
            None, expected_sha256=None, plan_binding=plan, now=now
        )
    directive = _directive(plan, now)
    checksum = sha256_text(matrix._canonical_json(directive))
    verified = matrix.require_future_execution_directive(
        directive,
        expected_sha256=checksum,
        plan_binding=plan,
        now=now,
    )
    assert verified["directive_sha256"] == checksum
    drifted = copy.deepcopy(directive)
    drifted["managed_chatgpt_plan_type"] = "plus"
    with pytest.raises(matrix.FutureExecutionDirectiveRequired):
        matrix.validate_future_execution_directive(
            drifted,
            expected_sha256=sha256_text(matrix._canonical_json(drifted)),
            plan_binding=plan,
            now=now,
        )
    old_boundary = copy.deepcopy(directive)
    for field in (
        "capacity_admission_required_before_thread_start",
        "capacity_admission_required_before_thread_resume",
        "capacity_admission_required_before_turn_start",
    ):
        old_boundary.pop(field)
    old_boundary["capacity_admission_required_before_app_server_start"] = True
    with pytest.raises(matrix.FutureExecutionDirectiveRequired):
        matrix.validate_future_execution_directive(
            old_boundary,
            expected_sha256=sha256_text(matrix._canonical_json(old_boundary)),
            plan_binding=plan,
            now=now,
        )


def test_all_six_outputs_must_be_full_grounded_v31_in_identical_opaque_order(
    tmp_path: Path,
) -> None:
    data = _fixture(tmp_path)
    preflight = _preflight(data)
    outputs = _outputs(data)
    receipt = matrix.validate_full_v31_outputs(
        outputs, manifest_info=preflight["manifest_info"]
    )
    assert receipt["arm_count"] == 6
    assert receipt["case_count_per_arm"] == 9
    drifted = copy.deepcopy(outputs)
    first = next(iter(drifted))
    drifted[first][0]["label"].pop("overall_confidence")
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="full grounded"):
        matrix.validate_full_v31_outputs(
            drifted, manifest_info=preflight["manifest_info"]
        )
    reordered = copy.deepcopy(outputs)
    reordered[first][0], reordered[first][1] = reordered[first][1], reordered[first][0]
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="order"):
        matrix.validate_full_v31_outputs(
            reordered, manifest_info=preflight["manifest_info"]
        )


def test_quality_contract_covers_every_field_with_llm_only_ab_ba_stages() -> None:
    contract = matrix.quality_evaluator_contract()
    final_fields = set(contract["canonical_final_field_paths"])
    semantic_fields = set(contract["model_semantic_field_paths"])
    provenance_fields = set(contract["deterministic_provenance_field_paths"])
    assert semantic_fields.isdisjoint(provenance_fields)
    assert semantic_fields | provenance_fields == final_fields
    assert contract["every_canonical_final_field_covered"] is True
    assert contract["evaluation_mode"] == "llm_only"
    assert contract["embeddings_used"] is False
    assert contract["deterministic_semantic_matching"] is False
    assert contract["stage_order"] == ["support_first", "alignment"]
    assert contract["orientations_per_stage"] == ["ab", "ba"]
    assert contract["abstention_enabled_per_stage"] is True
    assert contract["exact_evidence_rate_required"] == 1.0
    assert contract["exact_offset_rate_required"] == 1.0
    assert contract["exact_provenance_rate_required"] == 1.0


def _augmented_reference(data: dict, root: Path) -> dict:
    payload = {
        "schema_version": matrix.AUGMENTED_REFERENCE_VERSION,
        "state": "frozen_one_shared_reference_before_judging",
        "source_reference_seed_sha256": data["reference_record"]["sha256"],
        "case_order": copy.deepcopy(data["case_order"]),
        "case_order_sha256": sha256_text(matrix._canonical_json(data["case_order"])),
        "one_shared_reference_for_all_six_arms": True,
        "augmentation_mode": "llm_only",
        "embeddings_used": False,
        "deterministic_semantic_matching": False,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "references": copy.deepcopy(data["references"]),
    }
    return _write(root / "augmented-reference.json", payload)


def _usage(total: int) -> dict:
    output = total // 6
    return {
        "input_tokens": total - output,
        "cached_input_tokens": min(100, total - output),
        "output_tokens": output,
        "reasoning_output_tokens": min(50, output),
        "total_tokens": total,
    }


def _orientation(name: str, stage: str, order_hash: str, reference_sha: str) -> dict:
    del stage
    return {
        "orientation": name,
        "state": "completed",
        "case_order_sha256": order_hash,
        "shared_augmented_reference_sha256": reference_sha,
        "evaluation_mode": "llm_only",
        "abstention_enabled": True,
        "abstention_count": 0,
        "managed_chatgpt_auth_verified": True,
        "managed_chatgpt_plan_type": "pro",
        "usage_status": "complete",
        "usage": _usage(120),
        "wall_elapsed_seconds": 1.0,
        "retry_count": 0,
        "deterministic_semantic_matching": False,
    }


def _stage(name: str, order_hash: str, reference_sha: str) -> dict:
    orientations = [
        _orientation("ab", name, order_hash, reference_sha),
        _orientation("ba", name, order_hash, reference_sha),
    ]
    return {
        "stage": name,
        "state": "completed",
        "orientations": ["ab", "ba"],
        "orientation_receipts": orientations,
        "case_order_sha256": order_hash,
        "shared_augmented_reference_sha256": reference_sha,
        "evaluation_mode": "llm_only",
        "abstention_enabled": True,
        "accounting_complete": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "retry_count": 0,
        "deterministic_semantic_matching": False,
        "usage": matrix._sum_usage([row["usage"] for row in orientations]),
        "wall_elapsed_seconds": 2.0,
    }


def _cost(usage: dict, *, cases: int, baseline_rate: float = 1000.0) -> dict:
    production_segments = 90
    production_episodes = 10
    context = 0
    candidate = usage["total_tokens"] / cases * production_segments
    baseline = baseline_rate * production_segments
    return {
        "formula": matrix.PRODUCTION_COST_FORMULA,
        "measured_extraction_total_tokens": usage["total_tokens"],
        "development_segment_count": cases,
        "production_segment_count": production_segments,
        "production_episode_count": production_episodes,
        "recurring_context_tokens_per_episode": context,
        "baseline_tokens_per_segment": baseline_rate,
        "candidate_production_amortized_total_tokens": candidate,
        "baseline_production_total_tokens": baseline,
        "production_amortized_total_token_ratio": candidate / baseline,
    }


def _quality_result(data: dict, preflight: dict, precommit: dict, root: Path) -> tuple[dict, dict]:
    contract = matrix.quality_evaluator_contract()
    augmented = _augmented_reference(data, root)
    order_hash = preflight["manifest_info"]["opaque_case_order_sha256"]
    stages = [
        _stage("support_first", order_hash, augmented["sha256"]),
        _stage("alignment", order_hash, augmented["sha256"]),
    ]
    envelope_hashes = {
        matrix._variant_id(size, mode): sha256_text(f"envelope:{size}:{mode}")
        for size, mode in matrix.EXPECTED_ARMS
    }
    macro_values = [0.980, 0.981, 0.982, 0.990, 0.979, 0.978]
    arms = {}
    for index, variant_id in enumerate(envelope_hashes):
        usage = _usage(1800 + index * 30)
        arms[variant_id] = {
            "arm_envelope_sha256": envelope_hashes[variant_id],
            "case_order_sha256": order_hash,
            "shared_augmented_reference_sha256": augmented["sha256"],
            "evaluated_model_semantic_field_paths_sha256": contract[
                "model_semantic_field_paths_sha256"
            ],
            "evaluated_deterministic_provenance_field_paths_sha256": contract[
                "deterministic_provenance_field_paths_sha256"
            ],
            "every_canonical_final_field_evaluated": True,
            "strict_full_field_macro": macro_values[index],
            "baseline_strict_full_field_macro": 0.97,
            "exact_evidence_rate": 1.0,
            "exact_offset_rate": 1.0,
            "exact_provenance_rate": 1.0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "cached_input_tokens": usage["cached_input_tokens"],
            "reasoning_output_tokens": usage["reasoning_output_tokens"],
            "wall_elapsed_seconds": 3.0,
            "semantic_defaults_used": False,
            "semantic_pruning": False,
            "retry_count": 0,
            "production_cost": _cost(usage, cases=9),
        }
    result = {
        "schema_version": matrix.QUALITY_RESULT_VERSION,
        "state": "completed_full_canonical_shared_reference_evaluation",
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": precommit["precommit"]["manifest_sha256"],
        "context_set_sha256": precommit["precommit"]["context_set_sha256"],
        "runtime_binding_sha256": precommit["precommit"]["runtime_binding_sha256"],
        "context_control_overlay_sha256": precommit["precommit"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": precommit["precommit"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": precommit["precommit"]["capacity_policy_sha256"],
        "quality_evaluator_contract_sha256": sha256_text(
            matrix._canonical_json(contract)
        ),
        "shared_augmented_reference": augmented,
        "arm_envelope_sha256s": envelope_hashes,
        "case_order": copy.deepcopy(data["case_order"]),
        "case_order_sha256": order_hash,
        "stage_order": ["support_first", "alignment"],
        "orientations_per_stage": ["ab", "ba"],
        "abstention_enabled_per_stage": True,
        "one_shared_augmented_reference_for_all_six_arms": True,
        "baseline_system_id": "canonical_v31_frozen_baseline_fixture",
        "baseline_full_v31_output_sha256": sha256_text("baseline-full-v31-fixture"),
        "baseline_strict_full_field_macro": 0.97,
        "baseline_scored_in_same_shared_reference_evaluation": True,
        "evaluation_mode": "llm_only",
        "embeddings_used": False,
        "deterministic_semantic_matching": False,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "stages": stages,
        "judge_usage": matrix._sum_usage([stage["usage"] for stage in stages]),
        "judge_wall_elapsed_seconds": 4.0,
        "judge_accounting_complete": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "arms": arms,
    }
    return result, envelope_hashes


def test_quality_result_selects_unique_strict_gate_winner(tmp_path: Path) -> None:
    data = _fixture(tmp_path / "fixture")
    preflight = _preflight(data)
    precommit = matrix.build_matrix_precommit(preflight)
    result, envelope_hashes = _quality_result(data, preflight, precommit, tmp_path)
    checksum = sha256_text(matrix._canonical_json(result))
    selection = matrix.score_and_select_winner(
        result,
        expected_sha256=checksum,
        precommit_bundle=precommit,
        manifest_info=preflight["manifest_info"],
        expected_arm_envelope_sha256s=envelope_hashes,
        allowed_root=tmp_path,
    )
    assert selection["winner_variant_id"] == "canonical_v31_batch_5_same_thread"
    assert selection["winner_is_development_only"] is True
    assert selection["holdout_authorized"] is False
    assert selection["production_mutation_allowed"] is False


@pytest.mark.parametrize(
    "gate",
    ["macro", "noninferiority", "evidence", "offset", "provenance", "ratio"],
)
def test_each_quality_gate_is_fail_closed(tmp_path: Path, gate: str) -> None:
    data = _fixture(tmp_path / "fixture")
    preflight = _preflight(data)
    precommit = matrix.build_matrix_precommit(preflight)
    result, envelope_hashes = _quality_result(data, preflight, precommit, tmp_path)
    for metrics in result["arms"].values():
        if gate == "macro":
            metrics["strict_full_field_macro"] = 0.96
        elif gate == "noninferiority":
            metrics["strict_full_field_macro"] = 0.98
            metrics["baseline_strict_full_field_macro"] = 0.99
        elif gate == "evidence":
            metrics["exact_evidence_rate"] = 0.99
        elif gate == "offset":
            metrics["exact_offset_rate"] = 0.99
        elif gate == "provenance":
            metrics["exact_provenance_rate"] = 0.99
        else:
            metrics["production_cost"] = _cost(
                metrics["usage"], cases=9, baseline_rate=500.0
            )
    if gate == "noninferiority":
        result["baseline_strict_full_field_macro"] = 0.99
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="no development arm"):
        matrix.score_and_select_winner(
            result,
            expected_sha256=sha256_text(matrix._canonical_json(result)),
            precommit_bundle=precommit,
            manifest_info=preflight["manifest_info"],
            expected_arm_envelope_sha256s=envelope_hashes,
            allowed_root=tmp_path,
        )


def test_capacity_admission_binds_exact_future_plan_directive_and_arm(tmp_path: Path) -> None:
    data = _fixture(tmp_path / "fixture")
    precommit = matrix.build_matrix_precommit(_preflight(data))
    plan = matrix.build_future_plan_binding(precommit, output_root=tmp_path / "future")
    now = datetime(2026, 7, 18, 18, 0, tzinfo=timezone.utc)
    directive = _directive(plan, now)
    directive_info = matrix.validate_future_execution_directive(
        directive,
        expected_sha256=sha256_text(matrix._canonical_json(directive)),
        plan_binding=plan,
        now=now,
    )
    arm = precommit["precommit"]["arms"][0]
    admission = {
        "schema_version": matrix.CAPACITY_ADMISSION_VERSION,
        "state": "admitted_after_official_app_server_initialization_before_semantic_thread_or_turn",
        "issued_at": (now - timedelta(seconds=30)).isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "variant_id": arm["variant_id"],
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "request_count": arm["request_count"],
        "request_set_sha256": arm["request_set_sha256"],
        "future_plan_binding_sha256": plan["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": plan["binding"]["manifest_sha256"],
        "runtime_binding_sha256": plan["binding"]["runtime_binding_sha256"],
        "context_control_overlay_sha256": plan["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan["binding"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["binding"]["capacity_policy_sha256"],
        "capacity_measurement_sha256": "a" * 64,
        "requested_total_token_ceiling": 10000,
        "available_total_tokens": 20000,
        "requested_wall_seconds_ceiling": 600.0,
        "available_wall_seconds": 1200.0,
        "capacity_available": True,
        "capacity_unknown": False,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    checksum = sha256_text(matrix._canonical_json(admission))
    verified = matrix.validate_capacity_admission(
        admission,
        expected_sha256=checksum,
        plan_binding=plan,
        directive_info=directive_info,
        precommit_bundle=precommit,
        variant_id=arm["variant_id"],
        now=now,
    )
    assert verified["admission_sha256"] == checksum
    drifted = copy.deepcopy(admission)
    drifted["available_total_tokens"] = 1
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="headroom"):
        matrix.validate_capacity_admission(
            drifted,
            expected_sha256=sha256_text(matrix._canonical_json(drifted)),
            plan_binding=plan,
            directive_info=directive_info,
            precommit_bundle=precommit,
            variant_id=arm["variant_id"],
            now=now,
        )
    old_boundary = copy.deepcopy(admission)
    old_boundary["app_server_started"] = False
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="lineage"):
        matrix.validate_capacity_admission(
            old_boundary,
            expected_sha256=sha256_text(matrix._canonical_json(old_boundary)),
            plan_binding=plan,
            directive_info=directive_info,
            precommit_bundle=precommit,
            variant_id=arm["variant_id"],
            now=now,
        )


def test_offline_arm_envelope_requires_complete_adapter_lifecycle_and_full_outputs(
    tmp_path: Path,
) -> None:
    data = _fixture(tmp_path / "fixture")
    preflight = _preflight(data)
    precommit = matrix.build_matrix_precommit(preflight)
    plan = matrix.build_future_plan_binding(precommit, output_root=tmp_path / "future")
    now = datetime(2026, 7, 18, 18, 0, tzinfo=timezone.utc)
    directive = _directive(plan, now)
    directive_info = matrix.validate_future_execution_directive(
        directive,
        expected_sha256=sha256_text(matrix._canonical_json(directive)),
        plan_binding=plan,
        now=now,
    )
    arm = precommit["precommit"]["arms"][0]
    admission = {
        "schema_version": matrix.CAPACITY_ADMISSION_VERSION,
        "state": "admitted_after_official_app_server_initialization_before_semantic_thread_or_turn",
        "issued_at": (now - timedelta(seconds=30)).isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "variant_id": arm["variant_id"],
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "request_count": arm["request_count"],
        "request_set_sha256": arm["request_set_sha256"],
        "future_plan_binding_sha256": plan["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": plan["binding"]["manifest_sha256"],
        "runtime_binding_sha256": plan["binding"]["runtime_binding_sha256"],
        "context_control_overlay_sha256": plan["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan["binding"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["binding"]["capacity_policy_sha256"],
        "capacity_measurement_sha256": "b" * 64,
        "requested_total_token_ceiling": 10000,
        "available_total_tokens": 20000,
        "requested_wall_seconds_ceiling": 600.0,
        "available_wall_seconds": 1200.0,
        "capacity_available": True,
        "capacity_unknown": False,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    admission_info = matrix.validate_capacity_admission(
        admission,
        expected_sha256=sha256_text(matrix._canonical_json(admission)),
        plan_binding=plan,
        directive_info=directive_info,
        precommit_bundle=precommit,
        variant_id=arm["variant_id"],
        now=now,
    )
    turn_usage = _usage(120)
    results = [
        {
            "batch_id": batch_id,
            "state": "validated",
            "thread_id": f"thread-{index}",
            "turn_id": f"turn-{index}",
            "usage": copy.deepcopy(turn_usage),
        }
        for index, batch_id in enumerate(arm["batch_ids"])
    ]
    usage = matrix._sum_usage([row["usage"] for row in results])
    instruction_sources = adapter.expected_instruction_source_contract()
    report = {
        "schema_version": adapter.RUN_REPORT_VERSION,
        "state": "passed",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "model": adapter.MODEL,
        "effort": adapter.EFFORT,
        "requested_calls": arm["request_count"],
        "attempted_calls": arm["request_count"],
        "validated_calls": arm["request_count"],
        "rejected_calls": 0,
        "failed_calls": 0,
        "usage_status": "complete",
        "usage": usage,
        "cached_input_tokens": usage["cached_input_tokens"],
        "reasoning_output_tokens": usage["reasoning_output_tokens"],
        "wall_elapsed_seconds": 3.0,
        "retry_count": 0,
        "ambiguous_retry_count": 0,
        "semantic_postprocessing": False,
        "all_emitted_semantic_values_preserved": True,
        "thread_lineage_valid": True,
        "turn_ids_unique": True,
        "observed_thread_count": arm["request_count"],
        "observed_turn_count": arm["request_count"],
        "managed_chatgpt_auth_verified": True,
        "managed_chatgpt_plan_type": "pro",
        "instruction_source_contract": instruction_sources,
        "instruction_source_contract_sha256": sha256_text(
            matrix._canonical_json(instruction_sources)
        ),
        "project_instruction_content_byte_budget": 0,
        "production_mutated": False,
        "holdout_authorized": False,
        "results": results,
    }
    envelope = {
        "schema_version": matrix.ARM_ENVELOPE_VERSION,
        "state": "completed_full_canonical_v31_arm",
        "variant_id": arm["variant_id"],
        "precommit_sha256": precommit["precommit_sha256"],
        "future_plan_binding_sha256": plan["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "capacity_admission_sha256": admission_info["admission_sha256"],
        "manifest_sha256": plan["binding"]["manifest_sha256"],
        "context_set_sha256": plan["binding"]["context_set_sha256"],
        "runtime_binding_sha256": plan["binding"]["runtime_binding_sha256"],
        "context_control_overlay_sha256": plan["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan["binding"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["binding"]["capacity_policy_sha256"],
        "adapter_report": report,
        "outputs": _outputs(data)[arm["variant_id"]],
    }
    verified = matrix.validate_arm_envelope(
        envelope,
        expected_sha256=sha256_text(matrix._canonical_json(envelope)),
        precommit_bundle=precommit,
        plan_binding=plan,
        directive_info=directive_info,
        admission_info=admission_info,
        manifest_info=preflight["manifest_info"],
    )
    assert verified["variant_id"] == arm["variant_id"]
    assert verified["usage"] == usage
    drifted = copy.deepcopy(envelope)
    drifted["adapter_report"]["results"][0]["batch_id"] = "wrong-batch"
    with pytest.raises(matrix.CanonicalV31DevelopmentMatrixError, match="batch order"):
        matrix.validate_arm_envelope(
            drifted,
            expected_sha256=sha256_text(matrix._canonical_json(drifted)),
            precommit_bundle=precommit,
            plan_binding=plan,
            directive_info=directive_info,
            admission_info=admission_info,
            manifest_info=preflight["manifest_info"],
        )


def test_matrix_module_has_no_live_dispatch_or_historical_compact_authority() -> None:
    source = Path(matrix.__file__).read_text()
    assert "run_episode_batch_arm(" not in source
    assert "CodexAppServerClient" not in source
    assert "app_server_expanded_cap_development_matrix" not in source
    assert "verify_epoch6" not in source
    assert "DEFAULT_EPOCH6" not in source
