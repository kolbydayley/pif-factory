from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from research_factory import app_server_candidate_evaluation_bundle as evaluator
from research_factory import app_server_candidate_evaluation_runner as runner
from research_factory import app_server_judge_v5 as judge
from research_factory import app_server_capacity_reserve as reserve


PIPELINE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
)
V249_ROOT = PIPELINE_ROOT / "development-selection-v5_4-v249-explicit-applicability"
SOURCE_PATH = (
    V249_ROOT
    / "turns"
    / "v249-explicit-applicability-d7c914bc1cee2c432b36"
    / "input.private.json"
)
REFERENCE_PATH = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v220-fresh-integrated-base-design"
    / "shared-reference-seed-v1.json"
)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _alignment_output(
    value: Mapping[str, Any], mapping: Mapping[str, Any], *, drop_one: bool = False
) -> dict[str, Any]:
    origin_by_id = {
        str(row["witness_id"]): str(row["origin"])
        for row in mapping["rows"]
        if row["witness_id"]
    }
    cases = []
    for input_case in value["cases"]:
        witness_ids = [str(row["witness_id"]) for row in input_case["witnesses"]]
        references = sorted(
            witness_id
            for witness_id in witness_ids
            if origin_by_id[witness_id] == "reference"
        )
        candidates = sorted(
            witness_id
            for witness_id in witness_ids
            if origin_by_id[witness_id] == "candidate"
        )
        pair_count = min(len(references), len(candidates))
        if drop_one and pair_count:
            pair_count -= 1
        pairs = list(zip(references[:pair_count], candidates[:pair_count]))
        paired = {witness_id for pair in pairs for witness_id in pair}
        unpaired = sorted(set(witness_ids) - paired)
        cases.append(
            {
                "case_id": input_case["case_id"],
                "equivalence_groups": [
                    {
                        "witness_ids": [first, second],
                        "rationale": "Equivalent synthetic full-field pair.",
                    }
                    for first, second in pairs
                ]
                + [
                    {
                        "witness_ids": [witness_id],
                        "rationale": "Unpaired synthetic witness.",
                    }
                    for witness_id in unpaired
                ],
                "alignment_pairs": [
                    {
                        "witness_id_1": first,
                        "witness_id_2": second,
                        "relation": "equivalent",
                        "checklist": [
                            {
                                "field": field,
                                "decision": "same",
                                "source_evidence_spans": [],
                                "witness_evidence_ids": [first, second],
                                "rationale": "No material difference.",
                            }
                            for field in judge.CHECKLIST_FIELDS
                        ],
                        "rationale": "Equivalent across all frozen checklist rows.",
                    }
                    for first, second in pairs
                ],
                "unpaired_witness_ids": unpaired,
            }
        )
    return {"cases": cases}


class FakeCandidateEvaluatorClient:
    def __init__(self, *, disagree: bool = False, unknown_first: bool = False) -> None:
        self.disagree = disagree
        self.unknown_first = unknown_first
        self.calls: list[str] = []

    async def __aenter__(self) -> "FakeCandidateEvaluatorClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        sidecar_path = Path(str(kwargs["sidecar_path"]))
        output_path = Path(str(kwargs["output_path"]))
        capacity_path = Path(str(kwargs["capacity_checkpoint_path"]))
        turn_name = sidecar_path.parent.name
        self.calls.append(turn_name)
        input_value = _load(sidecar_path.parent / "input.private.json")
        if turn_name == "pointwise-support":
            output = {
                "units": [
                    {
                        "case_id": row["case_id"],
                        "witness_id": row["witness_id"],
                        "support_status": "supported",
                        "source_evidence_spans": [
                            str(row["source_excerpt"])[
                                : min(40, len(str(row["source_excerpt"])))
                            ]
                        ],
                        "rationale": "Synthetic exact source support.",
                    }
                    for row in input_value["units"]
                ]
            }
        else:
            alignment_input = input_value.get("alignment_input", input_value)
            mapping_root = (
                sidecar_path.parents[3] / "alignment"
                if sidecar_path.parents[2].name == "adjudication"
                else sidecar_path.parents[2]
            )
            mapping = _load(mapping_root / "origin-map.private.json")
            output = _alignment_output(
                alignment_input,
                mapping,
                drop_one=(self.disagree and turn_name.endswith("balanced-canary")),
            )
        capacity_path.parent.mkdir(parents=True, exist_ok=True)
        capacity_path.write_text(
            json.dumps(
                {
                    "schema_version": reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
                    "managed_chatgpt_auth_verified": True,
                    "cleared_for_semantic_turn": True,
                    "rate_limit_reached_type": None,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        output_path.write_text(json.dumps(output, sort_keys=True), encoding="utf-8")
        usage_status = "unknown" if self.unknown_first and len(self.calls) == 1 else "measured"
        sidecar = {
            "state": "failed" if usage_status == "unknown" else "completed",
            "status": "failed" if usage_status == "unknown" else "completed",
            "usage_status": usage_status,
            "usage_complete": usage_status == "measured",
            "auth_type": "chatgpt",
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "usage": (
                None
                if usage_status == "unknown"
                else {
                    "input_tokens": 50,
                    "cached_input_tokens": 0,
                    "output_tokens": 40,
                    "reasoning_output_tokens": 10,
                    "total_tokens": 100,
                }
            ),
        }
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True), encoding="utf-8")
        return SimpleNamespace(output=output)


def _copied(path: Path, root: Path) -> Path:
    target = root / path.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(path.read_bytes())
    return target


def _config(tmp_path: Path, *, local_inputs: bool = False) -> Path:
    protocol_root = tmp_path / "protocol"
    evaluator.freeze_protocol(protocol_root)
    input_root = tmp_path / "inputs"
    source_path = _copied(SOURCE_PATH, input_root) if local_inputs else SOURCE_PATH
    candidate_path = (
        _copied(V249_ROOT / "normalized-output.private.json", input_root)
        if local_inputs
        else V249_ROOT / "normalized-output.private.json"
    )
    provenance_path = (
        _copied(V249_ROOT / "evidence-provenance.private.json", input_root)
        if local_inputs
        else V249_ROOT / "evidence-provenance.private.json"
    )
    reference_path = (
        _copied(REFERENCE_PATH, input_root) if local_inputs else REFERENCE_PATH
    )
    terminal_path = (
        _copied(V249_ROOT / "terminal.json", input_root)
        if local_inputs
        else V249_ROOT / "terminal.json"
    )
    gate_path = (
        _copied(V249_ROOT / "architecture-structural-gate.json", input_root)
        if local_inputs
        else V249_ROOT / "architecture-structural-gate.json"
    )
    return runner.create_config(
        output_dir=tmp_path / "run",
        evaluation_id="candidate_evaluator_runner_fixture",
        source_path=source_path,
        candidate_path=candidate_path,
        provenance_path=provenance_path,
        shared_reference_path=reference_path,
        candidate_terminal_path=terminal_path,
        candidate_gate_path=gate_path,
        production_amortized_total_token_ratio=0.192218,
        protocol_root=protocol_root,
    )


def test_candidate_evaluator_runs_three_turns_and_freezes_winner(tmp_path: Path) -> None:
    config = _config(tmp_path)
    fake = FakeCandidateEvaluatorClient()
    terminal = asyncio.run(
        runner.run_evaluation(config, client_factory=lambda _policy: fake)
    )
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["production_mutated"] is False
    assert terminal["usage"]["total_tokens"] == 300
    assert fake.calls == [
        "pointwise-support",
        "neutral-alignment-base",
        "neutral-alignment-balanced-canary",
    ]
    calls = list(fake.calls)
    repeated = asyncio.run(
        runner.run_evaluation(config, client_factory=lambda _policy: fake)
    )
    assert repeated == terminal
    assert fake.calls == calls


def test_disagreement_runs_one_adjudication_then_fails_order_gate(tmp_path: Path) -> None:
    config = _config(tmp_path)
    fake = FakeCandidateEvaluatorClient(disagree=True)
    terminal = asyncio.run(
        runner.run_evaluation(config, client_factory=lambda _policy: fake)
    )
    assert terminal["terminal_reason"] == "candidate_semantic_quality_gate_not_passed"
    assert terminal["accounting_complete"] is True
    assert fake.calls[-1] == "observable-disagreement-adjudication"
    assert len(fake.calls) == 4
    score = _load(config.parent / "alignment" / "alignment-score.json")
    assert score["checks"]["observable_disagreement_adjudication_complete"] is True
    assert score["checks"]["unresolved_alignment_cases_0"] is True
    assert score["checks"]["permutation_projection_exact"] is False


def test_unknown_usage_is_terminal_and_never_retried(tmp_path: Path) -> None:
    config = _config(tmp_path)
    fake = FakeCandidateEvaluatorClient(unknown_first=True)
    terminal = asyncio.run(
        runner.run_evaluation(config, client_factory=lambda _policy: fake)
    )
    assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
    assert terminal["accounting_complete"] is False
    assert terminal["unknown_usage_turn_count"] == 1
    assert fake.calls == ["pointwise-support"]
    repeated = asyncio.run(
        runner.run_evaluation(config, client_factory=lambda _policy: fake)
    )
    assert repeated == terminal
    assert fake.calls == ["pointwise-support"]


def test_runtime_lock_rejects_direct_candidate_mutation(tmp_path: Path) -> None:
    config = _config(tmp_path, local_inputs=True)
    frozen = runner.freeze_run(config)
    assert runner.verify_run_lock(frozen["runtime_lock"]) == frozen["runtime_lock"]
    candidate = Path(str(_load(config)["candidate"]["path"]))
    candidate.write_text(candidate.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(runner.CandidateEvaluationRunError):
        runner.verify_run_lock(frozen["runtime_lock"])
