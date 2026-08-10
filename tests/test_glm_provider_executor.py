"""GLM opencode executor path in the provider seam (durability plan Phase 2).

The GLM provider must honor the exact same contract as the codex path:
rendered prompt in, output file written at the recorded path, JSONL log with a
usage event the budget ledger can meter, and the same status strings so
post-processing (validation, submission, finalization) is untouched.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research_factory import headless_codex as hc


def _stream(answer: str, *, tokens: dict | None = None) -> str:
    events = [
        {"type": "text", "part": {"text": answer}},
        {
            "type": "step_finish",
            "part": {
                "tokens": tokens
                or {
                    "input": 1000,
                    "output": 200,
                    "total": 1200,
                    "cache": {"read": 400},
                }
            },
        },
    ]
    return "\n".join(json.dumps(event) for event in events) + "\n"


def _run(tmp_path: Path, stdout: str, returncode: int = 0):
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("LABEL THIS SEGMENT", encoding="utf-8")
    output_path = tmp_path / "out" / "label.json"
    log_path = tmp_path / "run.log"
    completed = SimpleNamespace(stdout=stdout, stderr="", returncode=returncode)
    with patch.object(hc.subprocess, "run", return_value=completed) as run:
        item = hc._run_glm_opencode(
            prompt_path=prompt_path,
            output_path=output_path,
            log_path=log_path,
            model="opencode-go/glm-5.2",
            timeout_seconds=900,
        )
    return item, output_path, log_path, run


def test_valid_answer_writes_output_and_completes(tmp_path: Path) -> None:
    payload = {"labels": [{"claim_text": "a claim"}]}
    item, output_path, log_path, run = _run(
        tmp_path, _stream(json.dumps(payload))
    )
    assert item["status"] == "codex_exec_completed"
    assert item["provider"] == "glm_opencode"
    assert item["provider_call_started"] is True
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    # The command ran opencode, not codex.
    argv = run.call_args.args[0]
    assert "opencode" in Path(argv[0]).name
    assert "--model" in argv


def test_log_carries_a_meterable_usage_event(tmp_path: Path) -> None:
    from research_factory.efficient_backtest import _codex_usage_from_jsonl

    payload = {"labels": []}
    _, _, log_path, _ = _run(tmp_path, _stream(json.dumps(payload)))
    usage = _codex_usage_from_jsonl(log_path)
    assert usage is not None
    assert usage["total_tokens"] == 1200
    assert usage["input_tokens"] == 1000
    assert usage["cached_input_tokens"] == 400


def test_fenced_answer_is_unfenced(tmp_path: Path) -> None:
    payload = {"labels": []}
    fenced = "```json\n" + json.dumps(payload) + "\n```"
    item, output_path, _, _ = _run(tmp_path, _stream(fenced))
    assert item["status"] == "codex_exec_completed"
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_non_json_answer_fails_without_writing_output(tmp_path: Path) -> None:
    item, output_path, _, _ = _run(tmp_path, _stream("I cannot do that."))
    assert item["status"] == "codex_exec_failed"
    assert not output_path.exists()
    assert item["provider_call_started"] is True


def test_nonzero_exit_fails(tmp_path: Path) -> None:
    item, output_path, _, _ = _run(
        tmp_path, _stream(json.dumps({"labels": []})), returncode=1
    )
    assert item["status"] == "codex_exec_failed"
    assert not output_path.exists()


def test_timeout_is_reported_like_the_codex_path(tmp_path: Path) -> None:
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("LABEL THIS SEGMENT", encoding="utf-8")
    output_path = tmp_path / "label.json"
    log_path = tmp_path / "run.log"
    with patch.object(
        hc.subprocess,
        "run",
        side_effect=hc.subprocess.TimeoutExpired(cmd="opencode", timeout=900),
    ):
        item = hc._run_glm_opencode(
            prompt_path=prompt_path,
            output_path=output_path,
            log_path=log_path,
            model="opencode-go/glm-5.2",
            timeout_seconds=900,
        )
    assert item["status"] == "codex_exec_timeout"
    assert item["timed_out"] is True
    assert item["provider_call_started"] is True
