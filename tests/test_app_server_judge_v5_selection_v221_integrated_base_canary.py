from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_evaluation as app_eval
from research_factory import (
    app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic as v86,
)
from research_factory import (
    app_server_judge_v5_selection_v221_integrated_base_canary as v221,
)
from research_factory.paths import db_path


def _event(evidence: str) -> dict:
    return {
        "window_id": 0,
        "event_type": "capability_claim",
        "event_subtype": "",
        "claim_type": "descriptive",
        "actor_name": "Speaker",
        "actor_type": "person",
        "speaker_name": "Speaker",
        "speaker_role": "speaker",
        "reported_actor_name": "",
        "reported_actor_type": "none",
        "source_context_kind": "substantive_dialogue",
        "target_concept": "speed",
        "claim_text": "Alpha improves speed.",
        "stance": "supportive",
        "certainty": "high",
        "temporal_horizon": "present",
        "causal_mechanism": "",
        "counterclaim": "",
        "metric_value": "",
        "metric_unit": "",
        "metric_comparator": "",
        "metric_direction": "not_applicable",
        "metric_raw_text": "",
        "signal_reason": "A concrete capability is asserted.",
        "evidence": evidence,
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
        "confidence": 0.9,
    }


def _prepared_segment(segment_id: str, text: str, *, dense: bool) -> dict:
    windows, boundaries = app_eval.build_windowed_segment_packet(
        text, window_count=4, context_chars=900
    )
    return {
        "segment_id": segment_id,
        "segment_index": 0 if dense else 1,
        "segment_text": text,
        "windows": windows,
        "boundaries": boundaries,
        "density_stratum": "dense" if dense else "no_signal",
        "golden_event_count": 1 if dense else 0,
    }


def _requests() -> list[dict]:
    result = []
    for index in range(4):
        episode_id = f"episode-{index}"
        dense = _prepared_segment(
            f"dense-{index}", "Alpha improves speed.", dense=True
        )
        no_signal = _prepared_segment(
            f"no-signal-{index}", "Welcome to the show.", dense=False
        )
        segments = [dense, no_signal]
        segment_ids = [row["segment_id"] for row in segments]
        schema = v221.v220.integrated_episode_batch_schema(
            episode_id=episode_id, segment_ids=segment_ids
        )
        base = f"base-{index}"
        prompt = f"prompt-{index}"
        schema_text = json.dumps(
            schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        result.append(
            {
                "turn_name": f"turn_{index}",
                "episode_id": episode_id,
                "segment_ids": segment_ids,
                "base_instructions_sha256": v221.sha256_text(base),
                "base_instructions_bytes": len(base),
                "prompt_sha256": v221.sha256_text(prompt),
                "prompt_bytes": len(prompt),
                "schema_sha256": v221.sha256_text(schema_text),
                "schema_bytes": len(schema_text.encode()),
                "total_request_bytes": len(base)
                + len(prompt)
                + len(schema_text.encode()),
                "episode": {"episode_id": episode_id},
                "segments": segments,
                "base_instructions": base,
                "prompt": prompt,
                "schema": schema,
            }
        )
    return result


def _fake_predecessor(root: Path, requests: list[dict]) -> dict:
    names = (
        "terminal",
        "runtime_lock",
        "spec",
        "design",
        "selection",
        "manifest",
        "reference",
        "reference_noise",
        "request_fingerprints",
        "instructions",
    )
    paths = {}
    for name in names:
        path = root / f"v220-{name}.json"
        path.write_text(json.dumps({"name": name}) + "\n", encoding="utf-8")
        paths[name] = path
    episodes = []
    for index, request in enumerate(requests):
        episodes.append(
            {
                "episode_id": request["episode_id"],
                "source_id": f"source-{index}",
                "segments": [
                    {
                        "segment_id": request["segment_ids"][0],
                        "density_stratum": "dense",
                        "golden_event_count": 1,
                    },
                    {
                        "segment_id": request["segment_ids"][1],
                        "density_stratum": "no_signal",
                        "golden_event_count": 0,
                    },
                ],
            }
        )
    return {
        "root": root,
        "paths": paths,
        "records": {name: v86._record(path) for name, path in paths.items()},
        "instructions": "instructions",
        "terminal": {
            "cumulative_known_usage_lower_bound": {
                "input_tokens": 100,
                "cached_input_tokens": 0,
                "output_tokens": 20,
                "reasoning_output_tokens": 10,
                "total_tokens": 120,
            },
            "cumulative_unknown_usage_turn_count": 3,
            "cumulative_conservative_unknown_usage_upper_bound": 245_000,
        },
        "manifest": {
            "episode_count": 4,
            "segment_count": 8,
            "density_counts": {"dense": 4, "no_signal": 4},
            "episodes": episodes,
        },
        "request_fingerprints": {"turns": []},
        "spec": {},
        "design": {},
        "selection": {},
        "reference": {},
        "reference_noise": {},
        "runtime_lock": {},
    }


def _payload(request: dict) -> dict:
    dense_id, no_signal_id = request["segment_ids"]
    return {
        "episode_id": request["episode_id"],
        "segments": [
            {
                "segment_id": dense_id,
                "status": "coded",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "Substantive claim.",
                },
                "no_signal_reason": "",
                "events": [_event("Alpha improves speed.")],
                "coverage_receipt": {
                    "coverage_status": "complete",
                    "followup_required": False,
                    "gap_event_types": [],
                    "gap_evidence": [],
                    "rationale": "All eligible claims are represented.",
                },
            },
            {
                "segment_id": no_signal_id,
                "status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 0.9,
                    "rationale": "Only show setup.",
                },
                "no_signal_reason": "No eligible proposition.",
                "events": [],
                "coverage_receipt": {
                    "coverage_status": "no_eligible_events",
                    "followup_required": False,
                    "gap_event_types": [],
                    "gap_evidence": [],
                    "rationale": "No eligible claim is present.",
                },
            },
        ],
    }


class _FakeClient:
    def __init__(self, root: Path, requests: list[dict]):
        self.root = root
        self.requests = requests
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        assert (self.root / "launch-receipt.json").exists()
        request = self.requests[self.calls]
        self.calls += 1
        payload = _payload(request)
        sidecar = Path(kwargs["sidecar_path"])
        output = Path(kwargs["output_path"])
        capacity = Path(kwargs["capacity_checkpoint_path"])
        capacity.write_text(json.dumps({"cleared": True}) + "\n")
        usage = {
            "input_tokens": 80,
            "cached_input_tokens": 0,
            "output_tokens": 20,
            "reasoning_output_tokens": 10,
            "total_tokens": 100,
        }
        sidecar.write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_complete": True,
                    "usage_status": "measured",
                    "usage": usage,
                }
            )
            + "\n"
        )
        output.write_text(json.dumps(payload) + "\n")
        return SimpleNamespace(status_ok=True, output=payload)


def test_v221_receipt_contract_rejects_nonexact_gap_evidence():
    errors = v221._receipt_errors(
        receipt={
            "coverage_status": "material_gaps",
            "followup_required": True,
            "gap_event_types": ["capability_claim"],
            "gap_evidence": ["not present"],
            "rationale": "A gap remains.",
        },
        status="coded",
        output_event_count=1,
        segment_text="Alpha improves speed.",
    )
    assert errors == ["nonexact_gap_evidence"]


def test_v221_validate_and_normalize_accepts_complete_and_no_signal_pair():
    request = _requests()[0]
    normalized, diagnostics = v221.validate_and_normalize_output(
        payload=_payload(request), request=request
    )
    assert len(normalized["segments"][0]["events"]) == 1
    assert normalized["segments"][1]["events"] == []
    assert all(row["coverage_receipt_valid"] for row in diagnostics)
    assert all(row["exactness_pruned_events"] == 0 for row in diagnostics)


def test_v221_structural_gate_passes_complete_synthetic_cohort(
    tmp_path: Path,
):
    requests = _requests()
    predecessor = _fake_predecessor(tmp_path, requests)
    diagnostics = []
    for request in requests:
        _normalized, rows = v221.validate_and_normalize_output(
            payload=_payload(request), request=request
        )
        diagnostics.extend(rows)
    usage = {
        "input_tokens": 320,
        "cached_input_tokens": 0,
        "output_tokens": 80,
        "reasoning_output_tokens": 40,
        "total_tokens": 400,
    }
    gate, _private = v221.evaluate_structural_gate(
        predecessor=predecessor,
        diagnostics_rows=diagnostics,
        usage=usage,
        measured_turn_count=4,
    )
    assert gate["passed"] is True
    assert gate["failed_checks"] == []
    assert gate["followup_required_episode_count"] == 0
    assert gate["fresh_frozen_judge_authorized"] is True


def test_v221_freezes_before_launch_and_runs_once_with_complete_usage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    requests = _requests()
    predecessor = _fake_predecessor(tmp_path, requests)
    monkeypatch.setattr(v221, "_validate_v220_authorization", lambda: predecessor)
    monkeypatch.setattr(
        v221,
        "_build_turn_requests",
        lambda conn, predecessor: requests,
    )
    monkeypatch.setattr(
        v221, "_expected_runtime_paths", lambda: (Path(v221.__file__).resolve(),)
    )
    root = tmp_path / "attempt"
    frozen = v221.freeze_v221(output_dir=root, database_path=db_path())
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.glob("turns/*/capacity.json"))
    client = _FakeClient(root, requests)
    terminal = asyncio.run(
        v221.run_v221(
            output_dir=root,
            database_path=db_path(),
            client_factory=lambda policy_path: client,
        )
    )
    assert client.calls == 4
    assert terminal["state"] == "completed"
    assert terminal["fresh_judge_authorized"] is True
    assert terminal["bounded_gap_followup_authorized"] is False
    assert terminal["usage_status"] == "complete"
    assert terminal["accounting_complete"] is True
    assert terminal["usage"]["total_tokens"] == 400
    assert terminal["extraction_rerun_performed"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert v221.freeze_v221(output_dir=root)["terminal"] == terminal
