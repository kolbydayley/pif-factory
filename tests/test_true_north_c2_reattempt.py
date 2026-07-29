from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from research_factory import true_north
from research_factory import true_north_c2_reattempt as c2


def _packet() -> dict:
    return {
        "schema_version": "old",
        "input": {
            "candidates": [
                {
                    "candidate_id": "c1",
                    "pass_a_glm": {
                        "atomic_claims": [{"claim_text": "A."}]
                    },
                    "pass_b_sol": {
                        "atomic_claims": [
                            {"claim_text": "B."},
                            {"claim_text": "C."},
                        ]
                    },
                    "union_claim_texts": ["A.", "B.", "C."],
                }
            ]
        },
    }


def test_flat_schema_omits_provider_unsupported_keywords() -> None:
    schema = c2.flat_output_schema(c2.flat_packet(_packet()))
    rendered = str(schema)

    assert "oneOf" not in rendered
    assert "uniqueItems" not in rendered
    item = schema["properties"]["items"]["items"]
    assert item["properties"]["decision"]["enum"] == [
        "chose_a",
        "chose_b",
        "merged",
    ]


def test_local_validator_enforces_decision_and_uniqueness() -> None:
    packet = c2.flat_packet(_packet())
    c2.validate_flat_output(
        {
            "schema_version": c2.SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "c1",
                    "decision": "chose_b",
                    "resulting_claim_texts": ["B.", "C."],
                }
            ],
        },
        packet,
    )

    with pytest.raises(c2.C2ReattemptError, match="duplicate claims"):
        c2.validate_flat_output(
            {
                "schema_version": c2.SCHEMA_VERSION,
                "items": [
                    {
                        "candidate_id": "c1",
                        "decision": "merged",
                        "resulting_claim_texts": ["A.", "A."],
                    }
                ],
            },
            packet,
        )


def test_reattempt_reservation_includes_smoke_within_ceiling() -> None:
    assert 19 <= c2.MAX_CALLS
    assert (
        19 * c2.RESERVED_TOKENS_PER_ENVELOPE
        <= c2.MAX_TOKENS
    )


def test_codex_transport_omits_provider_schema(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    packet = c2.flat_packet(_packet())
    packet_path = tmp_path / "packet.private.json"
    packet_path.write_text(json.dumps(packet), encoding="utf-8")
    captured: list[str] = []

    def fake_run(command: list[str], **_kwargs: object) -> object:
        captured.extend(command)
        output = Path(
            command[command.index("--output-last-message") + 1]
        )
        output.write_text(
            json.dumps(
                {
                    "schema_version": c2.SCHEMA_VERSION,
                    "items": [
                        {
                            "candidate_id": "c1",
                            "decision": "chose_a",
                            "resulting_claim_texts": ["A."],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    receipt = true_north._run_codex_gold_packet(
        packet_path=packet_path,
        schema_path=None,
        output_dir=tmp_path / "output",
        timeout_seconds=10,
        codex_binary="codex",
        validator=c2.validate_flat_output,
        prompt_prefix=c2.SYSTEM_PROMPT,
    )

    assert receipt["ok"] is True
    assert "--output-schema" not in captured
