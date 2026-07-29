"""Offline GLM-5.2 candidate-extraction canary orchestrated through OpenCode.

This module is intentionally disconnected from the canonical SQLite queue.  It
reads an already-frozen private holdout, writes private lab artifacts outside
the repository, and never publishes labels or mutates production state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .labels import _validate_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOT = PROJECT_ROOT / "work" / "efficiency-windowed-final-holdout2-20260711"
DEFAULT_LAB_ROOT = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Podcast Intelligence Factory"
    / "glm-workhorse"
)
DEFAULT_MODEL = "opencode-go/glm-5.2"
DEFAULT_SPARK_MODEL = "gpt-5.3-codex-spark"
DEFAULT_JUDGE_MODEL = "gpt-5.6-terra"
EVENT_TYPES = (
    "term_usage",
    "frame_usage",
    "stance_position",
    "forecast",
    "causal_mechanism",
    "capability_claim",
    "product_signal",
    "market_signal",
    "risk_signal",
    "counterclaim",
    "uncertainty",
    "adoption_signal",
    "actor_mention",
    "entity_reference",
)
RUN_MESSAGE = (
    "Perform the private podcast semantic-extraction job in the attached JSON file. "
    "Read all instructions and context, silently perform the inventory and pruning "
    "passes, then return only one JSON object matching output_schema."
)
EXTRACTION_AGENT_NAME = "pif-extractor"
EXTRACTION_AGENT_PROMPT = """You are a systematic, topic-general semantic event reader
for a private podcast research corpus. This is a structured reading task, not a
coding task. Do not use tools. Read the complete attached job, distinguish
evidence-eligible text from context-only material, and finish in one response.

Silently work in three passes:
1. inventory every distinct research-useful proposition in the evidence text;
2. audit omissions across all event families and assign the best suggested type;
3. prune unsupported claims, ads, setup, banter, bare mentions, semantic duplicates,
   and low-signal fragments.

Return only schema-valid JSON, with no Markdown or commentary."""
CANARY_SCHEMA_VERSION = "pif_glm52_candidate_canary_v2"
JUDGE_SCHEMA_VERSION = "pif_glm52_candidate_judge_v1"
SOURCE_AUDIT_SCHEMA_VERSION = "pif_glm52_source_aware_audit_v1"


class GlmWorkhorseError(RuntimeError):
    """A fail-closed GLM workhorse error."""


@dataclass(frozen=True)
class CanaryCase:
    segment_id: str
    source_name: str
    chunk_index: int
    extract_text: str
    left_context: str
    right_context: str
    episode_context: dict[str, Any]
    adjacent_segments: tuple[dict[str, Any], ...]
    baseline_events: tuple[dict[str, Any], ...]
    prompt_path: Path
    baseline_path: Path


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_json(path: Path, value: Any) -> None:
    _write_text(path, json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _parse_private_packet(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    marker = "\n\n# Input packet\n"
    _instructions, separator, packet_text = text.partition(marker)
    if not separator:
        raise GlmWorkhorseError(f"frozen prompt has no input packet: {path.name}")
    try:
        value = json.loads(packet_text.strip())
    except json.JSONDecodeError as exc:
        raise GlmWorkhorseError(f"frozen prompt packet is invalid: {path.name}") from exc
    if not isinstance(value, dict):
        raise GlmWorkhorseError(f"frozen prompt packet is not an object: {path.name}")
    return value


def _baseline_events(payload: Mapping[str, Any], chunk_index: int) -> list[dict[str, Any]]:
    events = payload.get("events")
    if isinstance(events, list):
        return [
            dict(event)
            for event in events
            if isinstance(event, dict) and int(event.get("window_id", -1)) == chunk_index
        ]
    return [
        dict(event)
        for window in payload.get("windows") or []
        if isinstance(window, dict) and int(window.get("chunk_index", -1)) == chunk_index
        for event in window.get("events") or []
        if isinstance(event, dict)
    ]


def select_blinded_cases(
    source_root: Path,
    *,
    limit: int = 20,
) -> list[CanaryCase]:
    """Select one window per source without consulting golden event density."""
    root = Path(source_root).expanduser().resolve()
    manifest_path = root / "windowed_core_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GlmWorkhorseError("cannot read frozen windowed core manifest") from exc
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise GlmWorkhorseError("frozen windowed core manifest has no entries")
    cases: list[CanaryCase] = []
    seen_sources: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        source_name = str(entry.get("source_name") or "")
        if not source_name or source_name in seen_sources:
            continue
        segment_id = str(entry.get("segment_id") or entry.get("chunk_id") or "")
        prompt_path = root / "windowed_core_prompts" / f"{segment_id}.md"
        baseline_path = root / "windowed_core_outputs" / f"{segment_id}.json"
        if not prompt_path.is_file() or not baseline_path.is_file():
            continue
        packet = _parse_private_packet(prompt_path)
        windows = [window for window in packet.get("windows") or [] if isinstance(window, dict)]
        if not windows:
            continue
        # Longest source window is a source-only selection.  Golden labels do
        # not influence which material GLM sees.
        selected = max(
            windows,
            key=lambda window: (
                len(str(window.get("extract_text") or "")),
                -int(window.get("chunk_index", window.get("window_id", 0))),
            ),
        )
        chunk_index = int(selected.get("chunk_index", selected.get("window_id", 0)))
        try:
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise GlmWorkhorseError(f"invalid frozen baseline: {baseline_path.name}") from exc
        cases.append(
            CanaryCase(
                segment_id=segment_id,
                source_name=source_name,
                chunk_index=chunk_index,
                extract_text=str(selected.get("extract_text") or ""),
                left_context=str(selected.get("left_context") or ""),
                right_context=str(selected.get("right_context") or ""),
                episode_context=dict(packet.get("episode_context") or {}),
                adjacent_segments=tuple(
                    dict(segment)
                    for segment in packet.get("adjacent_segments") or []
                    if isinstance(segment, dict)
                ),
                baseline_events=tuple(_baseline_events(baseline, chunk_index)),
                prompt_path=prompt_path,
                baseline_path=baseline_path,
            )
        )
        seen_sources.add(source_name)
        if len(cases) >= limit:
            break
    if len(cases) < limit:
        raise GlmWorkhorseError(f"only {len(cases)} distinct frozen sources are available")
    return cases


def candidate_schema(segment_id: str, chunk_index: int, *, max_events: int) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["segment_id", "chunk_index", "events"],
        "properties": {
            "segment_id": {"type": "string", "enum": [segment_id]},
            "chunk_index": {"type": "integer", "enum": [chunk_index]},
            "events": {
                "type": "array",
                "maxItems": max_events,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["event_type", "evidence", "claim_text"],
                    "properties": {
                        "event_type": {"type": "string", "enum": list(EVENT_TYPES)},
                        "evidence": {"type": "string", "minLength": 1},
                        "claim_text": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def build_private_job(case: CanaryCase, *, max_events: int) -> dict[str, Any]:
    return {
        "task": "Extract research-useful discourse-event candidates from one podcast window.",
        "instructions": [
            (
                "Read every word of evidence_eligible_extract_text. All extraction must come "
                "from semantic understanding, never keyword, regex, or phrase rules."
            ),
            f"Return at most {max_events} distinct events; the ceiling is not a target.",
            (
                "Event families: term_usage=notable wording/category; "
                "frame_usage=interpretive lens; stance_position=actor position; "
                "forecast=future prediction; causal_mechanism=cause/constraint/enabler/consequence; "
                "capability_claim=ability or limitation; "
                "product_signal=release/roadmap/access/packaging/pricing/integration/distribution; "
                "market_signal=demand/investment/economics/competition/labor/commercial movement; "
                "risk_signal=safety/reliability/security/governance/regulatory/deployment risk; "
                "counterclaim=disagreement/rebuttal/exception; "
                "uncertainty=hedging/evidence weakness/method caveat; "
                "adoption_signal=workflow/user/customer/institutional uptake; "
                "actor_mention=explicit attribution/affiliation/influence/who-discusses-whom; "
                "entity_reference=graph-useful relation to a product/model/dataset/benchmark/"
                "paper/standard/institution, never plain presence."
            ),
            (
                "First inventory independently useful propositions, including distinct premises, "
                "mechanisms, capabilities, constraints, comparisons, outcomes, and alternatives. "
                "Split clauses only when each performs independent analytical work."
            ),
            (
                "Classify by analytical function and main predicate: preserve causal relations, "
                "questions that express unresolved alternatives as uncertainty, interpretive "
                "analogies as frames, and challenges as counterclaims."
            ),
            (
                "Track speaker, asserting source, acting entity, and discussed entity separately. "
                "Do not turn a mentioned organization, artifact, population, or third party into "
                "the claimant without explicit support."
            ),
            (
                "Keep inseparable comparisons, alternatives, causal chains, and joint outcomes "
                "together; split independently meaningful capabilities, policies, risks, adoption "
                "facts, or market implications."
            ),
            (
                "Retain concise propositions about identity, affiliation, classification, "
                "existence, availability, scope, limitation, policy, observed state, or explicit "
                "absence only when they add distinct research-useful information."
            ),
            (
                "Separate present state from forecast, capability from observed adoption, "
                "terminology from substantive framing, and uncertainty from qualified affirmation."
            ),
            (
                "Before returning, prune semantic restatements, bare name-drops, ordinary technical "
                "terms, ads, setup, banter, and low-signal fragments. actor_mention, entity_reference, "
                "and term_usage require a specific graph-useful or analytically notable relation."
            ),
            (
                "For every event, evidence must be copied verbatim as one exact contiguous "
                "substring of evidence_eligible_extract_text. Context-only text can resolve "
                "attribution but can never supply evidence or a new proposition."
            ),
            (
                "Use the smallest self-contained evidence span that supports the proposition, "
                "normally 5-40 words and one sentence or less. Before returning, locate each "
                "evidence string again in evidence_eligible_extract_text and copy its characters "
                "exactly; do not reconstruct the quote from memory."
            ),
            "Never paraphrase, normalize, stitch, shorten with ellipses, or repair the evidence string.",
            "claim_text may summarize the supported proposition, but must not add an unstated actor, target, cause, or outcome.",
            "Return exactly the JSON object described by output_schema and nothing else.",
        ],
        "output_schema": candidate_schema(case.segment_id, case.chunk_index, max_events=max_events),
        "input": {
            "segment_id": case.segment_id,
            "chunk_index": case.chunk_index,
            "evidence_eligible_extract_text": case.extract_text,
            "context_only": {
                "left_context": case.left_context,
                "right_context": case.right_context,
                "episode_context": case.episode_context,
                "adjacent_segments": list(case.adjacent_segments),
            },
        },
    }


def opencode_config(model: str) -> dict[str, Any]:
    """Return an isolated non-coding agent configuration for one lab call."""
    return {
        "$schema": "https://opencode.ai/config.json",
        "share": "disabled",
        "snapshot": False,
        "default_agent": EXTRACTION_AGENT_NAME,
        "permission": {"*": "deny"},
        "agent": {
            EXTRACTION_AGENT_NAME: {
                "description": "Private structured podcast candidate extraction",
                "mode": "primary",
                "model": model,
                "temperature": 0.1,
                "steps": 8,
                "prompt": EXTRACTION_AGENT_PROMPT,
                "permission": {"*": "deny"},
            }
        },
    }


def _timeout_output(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else ""


def _parse_opencode_stream(value: str) -> tuple[str, dict[str, Any] | None, int]:
    parts: list[str] = []
    finish = None
    count = 0
    for line in value.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        count += 1
        part = event.get("part")
        if event.get("type") == "text" and isinstance(part, dict) and isinstance(part.get("text"), str):
            parts.append(part["text"])
        if event.get("type") == "step_finish" and isinstance(part, dict):
            finish = part
    return "".join(parts).strip(), finish, count


def _decode_answer(answer: str) -> tuple[dict[str, Any], bool]:
    value = answer.strip()
    fence_removed = False
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        stripped = re.sub(r"^```(?:json)?[ \t\r\n]*", "", value, flags=re.IGNORECASE)
        stripped = re.sub(r"[ \t\r\n]*```$", "", stripped)
        if stripped == value:
            raise
        payload = json.loads(stripped)
        fence_removed = True
    if not isinstance(payload, dict):
        raise ValueError("model answer is not a JSON object")
    return payload, fence_removed


def _normalize_char(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return (
        normalized.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2013", "-")
        .replace("\u2014", "-")
    )


def _normalized_with_map(value: str) -> tuple[str, list[int]]:
    rendered: list[str] = []
    positions: list[int] = []
    in_space = False
    for index, original in enumerate(value):
        for char in _normalize_char(original):
            if char.isspace():
                if rendered and not in_space:
                    rendered.append(" ")
                    positions.append(index)
                in_space = True
            else:
                rendered.append(char)
                positions.append(index)
                in_space = False
    if rendered and rendered[-1] == " ":
        rendered.pop()
        positions.pop()
    return "".join(rendered), positions


def recover_exact_evidence(source: str, evidence: str) -> tuple[str | None, str]:
    if evidence in source:
        return evidence, "exact"
    normalized_source, positions = _normalized_with_map(source)
    normalized_evidence, _unused = _normalized_with_map(evidence)
    if not normalized_evidence:
        return None, "empty"
    hits: list[int] = []
    cursor = normalized_source.find(normalized_evidence)
    while cursor >= 0:
        hits.append(cursor)
        cursor = normalized_source.find(normalized_evidence, cursor + 1)
    if len(hits) != 1:
        return None, "ambiguous" if hits else "not_found"
    start = positions[hits[0]]
    end = positions[hits[0] + len(normalized_evidence) - 1] + 1
    recovered = source[start:end]
    if _normalized_with_map(recovered)[0] != normalized_evidence:
        return None, "mapping_failed"
    return recovered, "normalized_unique"


def validate_and_repair_candidate(
    payload: dict[str, Any],
    *,
    case: CanaryCase,
    max_events: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _validate_schema(candidate_schema(case.segment_id, case.chunk_index, max_events=max_events), payload, path="$")
    repaired = json.loads(json.dumps(payload))
    repair_modes: Counter[str] = Counter()
    invalid = 0
    returned_events = list(repaired.get("events") or [])
    grounded_events = []
    for event in returned_events:
        evidence = str(event.get("evidence") or "")
        recovered, mode = recover_exact_evidence(case.extract_text, evidence)
        repair_modes[mode] += 1
        if recovered is None:
            invalid += 1
        else:
            event["evidence"] = recovered
            grounded_events.append(event)
    repaired["events"] = grounded_events
    return repaired, {
        "returned_event_count": len(returned_events),
        "event_count": len(grounded_events),
        "invalid_evidence_events_dropped": invalid,
        "repair_modes": dict(sorted(repair_modes.items())),
        "all_returned_events_grounded": invalid == 0,
        "exact_grounding_after_repair": True,
        "usable_after_repair": bool(grounded_events) or not returned_events,
    }


def _usage(finish: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(finish, Mapping):
        return None
    tokens = finish.get("tokens") if isinstance(finish.get("tokens"), Mapping) else {}
    cache = tokens.get("cache") if isinstance(tokens.get("cache"), Mapping) else {}
    return {
        "input_tokens": tokens.get("input"),
        "cached_input_tokens": cache.get("read"),
        "output_tokens": tokens.get("output"),
        "reasoning_tokens": tokens.get("reasoning"),
        "total_tokens": tokens.get("total"),
        "estimated_cost_usd": finish.get("cost"),
    }


def _exact_pair_score(candidate: Sequence[Mapping[str, Any]], baseline: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidate_pairs = Counter((str(event.get("event_type") or ""), str(event.get("evidence") or "")) for event in candidate)
    baseline_pairs = Counter((str(event.get("event_type") or ""), str(event.get("evidence") or "")) for event in baseline)
    shared = sum((candidate_pairs & baseline_pairs).values())
    candidate_count = sum(candidate_pairs.values())
    baseline_count = sum(baseline_pairs.values())
    precision = shared / candidate_count if candidate_count else (1.0 if not baseline_count else 0.0)
    recall = shared / baseline_count if baseline_count else (1.0 if not candidate_count else 0.0)
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "shared_pairs": shared,
        "candidate_events": candidate_count,
        "baseline_events": baseline_count,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def _run_case_opencode(
    case: CanaryCase,
    *,
    output_root: Path,
    model: str,
    timeout_seconds: int,
    retry_count: int,
    max_events: int,
    opencode_binary: str,
) -> dict[str, Any]:
    job = build_private_job(case, max_events=max_events)
    job_path = output_root / "jobs" / f"{case.segment_id}.private.json"
    _write_json(job_path, job)
    env = os.environ.copy()
    env["OPENCODE_CONFIG_CONTENT"] = _canonical_json(opencode_config(model))
    worker_state_root = output_root / "worker-state"
    worker_state_root.mkdir(exist_ok=True)
    auth_source = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if not auth_source.is_file():
        raise GlmWorkhorseError("OpenCode authentication material is unavailable")
    attempts: list[dict[str, Any]] = []
    final_payload: dict[str, Any] | None = None
    final_validation: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory(
        prefix=f"{case.segment_id}-",
        dir=str(worker_state_root),
    ) as worker_data:
        private_data_root = Path(worker_data)
        private_opencode_root = private_data_root / "opencode"
        private_opencode_root.mkdir()
        auth_target = private_opencode_root / "auth.json"
        shutil.copy2(auth_source, auth_target)
        os.chmod(auth_target, 0o600)
        env["XDG_DATA_HOME"] = str(private_data_root)
        for attempt_index in range(retry_count + 1):
            attempt_number = attempt_index + 1
            command = [
                opencode_binary,
                "run",
                "--pure",
                "--dir",
                str(output_root / "scratch"),
                "--model",
                model,
                "--agent",
                EXTRACTION_AGENT_NAME,
                "--format",
                "json",
                "--title",
                f"pif-glm52-canary-{case.segment_id}-a{attempt_number}",
                RUN_MESSAGE,
                "--file",
                str(job_path),
            ]
            started = time.monotonic()
            timed_out = False
            try:
                completed = subprocess.run(
                    command,
                    cwd=str(PROJECT_ROOT),
                    env=env,
                    text=True,
                    errors="replace",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=timeout_seconds,
                    check=False,
                )
                stdout = completed.stdout
                stderr = completed.stderr
                exit_code = completed.returncode
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                stdout = _timeout_output(exc.stdout)
                stderr = _timeout_output(exc.stderr)
                exit_code = None
            elapsed = round(time.monotonic() - started, 3)
            stem = f"{case.segment_id}-attempt-{attempt_number}"
            _write_text(output_root / "raw-events" / f"{stem}.private.jsonl", stdout)
            _write_text(output_root / "stderr" / f"{stem}.private.txt", stderr)
            answer, finish, stream_count = _parse_opencode_stream(stdout)
            _write_text(output_root / "raw-answers" / f"{stem}.private.txt", answer + "\n")
            attempt: dict[str, Any] = {
                "attempt": attempt_number,
                "exit_code": exit_code,
                "timed_out": timed_out,
                "elapsed_seconds": elapsed,
                "stream_event_count": stream_count,
                "raw_answer_bytes": len(answer.encode("utf-8")),
                "usage": _usage(finish),
                "schema_valid": False,
                "exact_grounding_after_repair": False,
            }
            if exit_code == 0 and not timed_out:
                try:
                    payload, fence_removed = _decode_answer(answer)
                    attempt["outer_fence_stripped"] = fence_removed
                    repaired, validation = validate_and_repair_candidate(
                        payload,
                        case=case,
                        max_events=max_events,
                    )
                    attempt["schema_valid"] = True
                    attempt.update(validation)
                    if validation["usable_after_repair"]:
                        final_payload = repaired
                        final_validation = validation
                except Exception as exc:
                    attempt["validation_error"] = type(exc).__name__
            attempts.append(attempt)
            if final_payload is not None:
                break
    if final_payload is not None:
        _write_json(output_root / "validated" / f"{case.segment_id}.private.json", final_payload)
    candidate_events = final_payload.get("events") if final_payload else []
    return {
        "segment_id": case.segment_id,
        "source_name": case.source_name,
        "chunk_index": case.chunk_index,
        "job_sha256": _sha256_bytes(job_path.read_bytes()),
        "job_bytes": job_path.stat().st_size,
        "baseline_event_count": len(case.baseline_events),
        "attempts": attempts,
        "attempt_count": len(attempts),
        "retry_attempts": max(0, len(attempts) - 1),
        "usable": final_payload is not None,
        "event_count": len(candidate_events),
        "validation": final_validation,
        "exact_pair_score": _exact_pair_score(candidate_events, case.baseline_events),
    }


def _run_case_codex(
    case: CanaryCase,
    *,
    output_root: Path,
    model: str,
    timeout_seconds: int,
    retry_count: int,
    max_events: int,
    codex_binary: str,
) -> dict[str, Any]:
    """Run the identical semantic packet through an ephemeral Codex model."""
    job = build_private_job(case, max_events=max_events)
    job_path = output_root / "jobs" / f"{case.segment_id}.private.json"
    schema_path = output_root / "schemas" / f"{case.segment_id}.json"
    _write_json(job_path, job)
    _write_json(schema_path, job["output_schema"])
    attempts: list[dict[str, Any]] = []
    final_payload: dict[str, Any] | None = None
    final_validation: dict[str, Any] | None = None
    for attempt_index in range(retry_count + 1):
        attempt_number = attempt_index + 1
        stem = f"{case.segment_id}-attempt-{attempt_number}"
        output_path = output_root / "raw-answers" / f"{stem}.private.txt"
        command = [
            codex_binary,
            "exec",
            "-m",
            model,
            "-c",
            'model_reasoning_effort="low"',
            "-C",
            str(output_root / "scratch"),
            "--skip-git-repo-check",
            "--ignore-rules",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(output_path),
            "--json",
            "-",
        ]
        prompt = (
            EXTRACTION_AGENT_PROMPT
            + "\n\n"
            + RUN_MESSAGE
            + "\n\n# Private job\n"
            + _canonical_json(job)
        )
        started = time.monotonic()
        timed_out = False
        try:
            completed = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                input=prompt,
                text=True,
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_seconds,
                check=False,
            )
            stdout = completed.stdout
            stderr = completed.stderr
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            stdout = _timeout_output(exc.stdout)
            stderr = _timeout_output(exc.stderr)
            exit_code = None
        elapsed = round(time.monotonic() - started, 3)
        _write_text(output_root / "raw-events" / f"{stem}.private.jsonl", stdout)
        _write_text(output_root / "stderr" / f"{stem}.private.txt", stderr)
        answer = output_path.read_text(encoding="utf-8") if output_path.is_file() else ""
        if not output_path.is_file():
            _write_text(output_path, "")
        usage = _codex_usage_from_jsonl(stdout)
        attempt: dict[str, Any] = {
            "attempt": attempt_number,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "elapsed_seconds": elapsed,
            "stream_event_count": sum(1 for line in stdout.splitlines() if line.strip()),
            "raw_answer_bytes": len(answer.encode("utf-8")),
            "usage": {
                "input_tokens": usage.get("input_tokens"),
                "cached_input_tokens": usage.get("cached_input_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "reasoning_tokens": usage.get("reasoning_output_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "estimated_cost_usd": None,
            },
            "schema_valid": False,
            "exact_grounding_after_repair": False,
        }
        if exit_code == 0 and not timed_out:
            try:
                payload, fence_removed = _decode_answer(answer)
                attempt["outer_fence_stripped"] = fence_removed
                repaired, validation = validate_and_repair_candidate(
                    payload,
                    case=case,
                    max_events=max_events,
                )
                attempt["schema_valid"] = True
                attempt.update(validation)
                if validation["usable_after_repair"]:
                    final_payload = repaired
                    final_validation = validation
            except Exception as exc:
                attempt["validation_error"] = type(exc).__name__
        attempts.append(attempt)
        if final_payload is not None:
            break
    if final_payload is not None:
        _write_json(output_root / "validated" / f"{case.segment_id}.private.json", final_payload)
    candidate_events = final_payload.get("events") if final_payload else []
    return {
        "segment_id": case.segment_id,
        "source_name": case.source_name,
        "chunk_index": case.chunk_index,
        "job_sha256": _sha256_bytes(job_path.read_bytes()),
        "job_bytes": job_path.stat().st_size,
        "baseline_event_count": len(case.baseline_events),
        "attempts": attempts,
        "attempt_count": len(attempts),
        "retry_attempts": max(0, len(attempts) - 1),
        "usable": final_payload is not None,
        "event_count": len(candidate_events),
        "validation": final_validation,
        "exact_pair_score": _exact_pair_score(candidate_events, case.baseline_events),
    }


def run_canary(
    *,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    output_root: Path,
    limit: int = 20,
    concurrency: int = 2,
    model: str = DEFAULT_MODEL,
    timeout_seconds: int = 180,
    retry_count: int = 0,
    max_events: int = 10,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    transport: str = "opencode",
    codex_binary: str = "codex",
) -> dict[str, Any]:
    if (
        limit < 1
        or concurrency < 1
        or timeout_seconds < 1
        or retry_count < 0
        or max_events < 1
        or transport not in {"opencode", "codex"}
    ):
        raise GlmWorkhorseError("invalid bounded canary settings")
    root = Path(output_root).expanduser().resolve()
    if (root / "receipt.json").exists():
        raise GlmWorkhorseError("output root already contains a completed receipt")
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    for name in ("jobs", "schemas", "raw-events", "raw-answers", "stderr", "validated", "scratch"):
        (root / name).mkdir(exist_ok=True)
    cases = select_blinded_cases(source_root, limit=limit)
    selection = {
        "schema_version": "pif_glm52_blinded_selection_v1",
        "selection_rule": "one distinct source in frozen manifest order; longest source window; golden density unavailable to selector",
        "source_root_sha256": _sha256_bytes((Path(source_root) / "windowed_core_manifest.json").read_bytes()),
        "cases": [
            {
                "segment_id": case.segment_id,
                "source_name": case.source_name,
                "chunk_index": case.chunk_index,
                "extract_bytes": len(case.extract_text.encode("utf-8")),
                "prompt_sha256": _sha256_bytes(case.prompt_path.read_bytes()),
                "baseline_sha256": _sha256_bytes(case.baseline_path.read_bytes()),
            }
            for case in cases
        ],
        "golden_used_for_window_selection": False,
        "production_mutation": False,
        "queue_mutation": False,
    }
    _write_json(root / "selection.json", selection)
    wall_started = time.monotonic()
    results_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(concurrency, len(cases))) as executor:
        futures = {}
        for case in cases:
            print(_canonical_json({"status": "starting", "segment_id": case.segment_id, "source_name": case.source_name}), flush=True)
            runner = _run_case_opencode if transport == "opencode" else _run_case_codex
            runner_arguments: dict[str, Any] = {
                "output_root": root,
                "model": model,
                "timeout_seconds": timeout_seconds,
                "retry_count": retry_count,
                "max_events": max_events,
            }
            if transport == "opencode":
                runner_arguments["opencode_binary"] = opencode_binary
            else:
                runner_arguments["codex_binary"] = codex_binary
            future = executor.submit(runner, case, **runner_arguments)
            futures[future] = case
        for future in as_completed(futures):
            case = futures[future]
            result = future.result()
            results_by_id[case.segment_id] = result
            print(
                _canonical_json(
                    {
                        "status": "finished",
                        "segment_id": case.segment_id,
                        "source_name": case.source_name,
                        "usable": result["usable"],
                        "attempt_count": result["attempt_count"],
                        "event_count": result["event_count"],
                    }
                ),
                flush=True,
            )
    results = [results_by_id[case.segment_id] for case in cases]
    attempts = [attempt for result in results for attempt in result["attempts"]]
    usage_rows = [attempt["usage"] for attempt in attempts if isinstance(attempt.get("usage"), dict)]
    aggregate_pairs = Counter()
    for result in results:
        score = result["exact_pair_score"]
        aggregate_pairs.update(
            shared=score["shared_pairs"],
            candidate=score["candidate_events"],
            baseline=score["baseline_events"],
        )
    exact_precision = aggregate_pairs["shared"] / aggregate_pairs["candidate"] if aggregate_pairs["candidate"] else 0.0
    exact_recall = aggregate_pairs["shared"] / aggregate_pairs["baseline"] if aggregate_pairs["baseline"] else 0.0
    exact_f1 = (
        2 * exact_precision * exact_recall / (exact_precision + exact_recall)
        if exact_precision + exact_recall
        else 0.0
    )
    summary = {
        "case_count": len(results),
        "source_count": len({result["source_name"] for result in results}),
        "usable_case_count": sum(bool(result["usable"]) for result in results),
        "failed_case_count": sum(not result["usable"] for result in results),
        "attempted_calls": len(attempts),
        "retry_attempts": sum(result["retry_attempts"] for result in results),
        "timeout_attempts": sum(bool(attempt["timed_out"]) for attempt in attempts),
        "schema_valid_attempts": sum(bool(attempt["schema_valid"]) for attempt in attempts),
        "candidate_events": sum(result["event_count"] for result in results),
        "baseline_events": sum(result["baseline_event_count"] for result in results),
        "grounding_repaired_events": sum(
            int(((result.get("validation") or {}).get("repair_modes") or {}).get("normalized_unique") or 0)
            for result in results
        ),
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
        "total_attempt_elapsed_seconds": round(
            sum(float(attempt.get("elapsed_seconds") or 0) for attempt in attempts), 3
        ),
        "input_tokens": sum(int(row.get("input_tokens") or 0) for row in usage_rows),
        "cached_input_tokens": sum(int(row.get("cached_input_tokens") or 0) for row in usage_rows),
        "output_tokens": sum(int(row.get("output_tokens") or 0) for row in usage_rows),
        "total_tokens": sum(int(row.get("total_tokens") or 0) for row in usage_rows),
        "estimated_cost_usd": round(sum(float(row.get("estimated_cost_usd") or 0) for row in usage_rows), 8),
        "exact_pair_score": {
            "shared_pairs": aggregate_pairs["shared"],
            "precision": round(exact_precision, 6),
            "recall": round(exact_recall, 6),
            "f1": round(exact_f1, 6),
        },
    }
    receipt = {
        "schema_version": CANARY_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "privacy": "private_analysis_only",
        "production_enabled": False,
        "production_mutation": False,
        "queue_mutation": False,
        "canonical_db_opened": False,
        "promotion_authorized": False,
        "model": model,
        "transport": transport,
        "opencode_binary": str(Path(opencode_binary).resolve()),
        "opencode_pure_mode": True,
        "opencode_worker_state": "isolated_ephemeral_per_case",
        "credential_copy_persisted": False,
        "subscription_metering": (
            "opencode_reported_nominal_cost"
            if transport == "opencode"
            else "codex_subscription_no_per_call_cost_reported"
        ),
        "opencode_agent": EXTRACTION_AGENT_NAME,
        "agent_role": "non_coding_private_semantic_extractor",
        "tool_permission": "deny",
        "context_restored": True,
        "reference_alignment_semantics": "advisory_recall_and_agreement_only_not_precision",
        "source_root": str(Path(source_root).resolve()),
        "selection_path": str((root / "selection.json").resolve()),
        "concurrency": concurrency,
        "timeout_seconds": timeout_seconds,
        "retry_count": retry_count,
        "max_events": max_events,
        "summary": summary,
        "results": results,
    }
    _write_json(root / "receipt.json", receipt)
    print(_canonical_json({"status": "complete", "receipt_path": str(root / "receipt.json"), "summary": summary}), flush=True)
    return receipt


def revalidate_receipt(receipt_path: Path) -> dict[str, Any]:
    """Apply current event-granular grounding rules without another model call."""
    receipt_file = Path(receipt_path).expanduser().resolve()
    original = json.loads(receipt_file.read_text(encoding="utf-8"))
    if original.get("schema_version") != CANARY_SCHEMA_VERSION:
        raise GlmWorkhorseError("receipt is not a current GLM candidate canary")
    root = receipt_file.parent
    cases = {
        case.segment_id: case
        for case in select_blinded_cases(
            Path(str(original["source_root"])),
            limit=len(original["results"]),
        )
    }
    results = json.loads(json.dumps(original["results"]))
    for result in results:
        segment_id = str(result["segment_id"])
        case = cases[segment_id]
        final_payload = None
        final_validation = None
        for attempt in result["attempts"]:
            attempt_number = int(attempt["attempt"])
            answer_path = root / "raw-answers" / f"{segment_id}-attempt-{attempt_number}.private.txt"
            try:
                payload, fence_removed = _decode_answer(answer_path.read_text(encoding="utf-8"))
                repaired, validation = validate_and_repair_candidate(
                    payload,
                    case=case,
                    max_events=int(original["max_events"]),
                )
                attempt["outer_fence_stripped"] = fence_removed
                attempt["schema_valid"] = True
                attempt.update(validation)
                if validation["usable_after_repair"]:
                    final_payload = repaired
                    final_validation = validation
                    break
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        if final_payload is not None:
            _write_json(root / "validated" / f"{segment_id}.private.json", final_payload)
        candidate_events = final_payload.get("events") if final_payload else []
        result["usable"] = final_payload is not None
        result["event_count"] = len(candidate_events)
        result["validation"] = final_validation
        result["exact_pair_score"] = _exact_pair_score(candidate_events, case.baseline_events)
    attempts = [attempt for result in results for attempt in result["attempts"]]
    aggregate_pairs = Counter()
    for result in results:
        score = result["exact_pair_score"]
        aggregate_pairs.update(
            shared=score["shared_pairs"],
            candidate=score["candidate_events"],
            baseline=score["baseline_events"],
        )
    exact_precision = aggregate_pairs["shared"] / aggregate_pairs["candidate"] if aggregate_pairs["candidate"] else 0.0
    exact_recall = aggregate_pairs["shared"] / aggregate_pairs["baseline"] if aggregate_pairs["baseline"] else 0.0
    exact_f1 = (
        2 * exact_precision * exact_recall / (exact_precision + exact_recall)
        if exact_precision + exact_recall
        else 0.0
    )
    summary = dict(original["summary"])
    summary.update(
        {
            "usable_case_count": sum(bool(result["usable"]) for result in results),
            "failed_case_count": sum(not result["usable"] for result in results),
            "schema_valid_attempts": sum(bool(attempt["schema_valid"]) for attempt in attempts),
            "candidate_events": sum(int(result["event_count"]) for result in results),
            "grounding_repaired_events": sum(
                int(((result.get("validation") or {}).get("repair_modes") or {}).get("normalized_unique") or 0)
                for result in results
            ),
            "ungrounded_events_dropped": sum(
                int((result.get("validation") or {}).get("invalid_evidence_events_dropped") or 0)
                for result in results
            ),
            "exact_pair_score": {
                "shared_pairs": aggregate_pairs["shared"],
                "precision": round(exact_precision, 6),
                "recall": round(exact_recall, 6),
                "f1": round(exact_f1, 6),
            },
        }
    )
    updated = dict(original)
    updated.update(
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "revalidated_from": str(receipt_file),
            "revalidation_model_calls": 0,
            "grounding_failure_scope": "individual_event",
            "summary": summary,
            "results": results,
        }
    )
    output_path = root / "receipt.revalidated.json"
    _write_json(output_path, updated)
    print(_canonical_json({"status": "revalidated", "receipt_path": str(output_path), "summary": summary}), flush=True)
    return updated


def build_judge_packet(
    receipt: Mapping[str, Any],
    *,
    source_root: Path,
    validated_root: Path,
) -> dict[str, Any]:
    cases = {case.segment_id: case for case in select_blinded_cases(source_root, limit=len(receipt["results"]))}
    rendered_cases = []
    for result in receipt["results"]:
        segment_id = str(result["segment_id"])
        candidate_path = validated_root / f"{segment_id}.private.json"
        candidates = json.loads(candidate_path.read_text(encoding="utf-8")).get("events") if candidate_path.exists() else []
        case = cases[segment_id]
        rendered_cases.append(
            {
                "case_id": segment_id,
                "reference": [
                    {
                        "id": index,
                        "event_type": event.get("event_type"),
                        "claim": event.get("claim_text"),
                        "evidence": event.get("evidence"),
                    }
                    for index, event in enumerate(case.baseline_events)
                ],
                "candidate": [
                    {
                        "id": index,
                        "event_type": event.get("event_type"),
                        "claim": event.get("claim_text"),
                        "evidence": event.get("evidence"),
                    }
                    for index, event in enumerate(candidates or [])
                ],
            }
        )
    return {
        "instructions": (
            "Strictly align candidate and reference podcast discourse events one-to-one within each case. "
            "Use equivalent only when they express the same specific research signal with materially the same "
            "actor or source, target, proposition, stance or direction, timing, and numeric detail. A defensible "
            "neighboring event type is allowed when the analytical meaning is the same. Use partial for related "
            "but materially different events. Omit unrelated pairs. Do not favor either side."
        ),
        "cases": rendered_cases,
    }


def judge_schema(case_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["evaluations"],
        "properties": {
            "evaluations": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["case_id", "pairs"],
                    "properties": {
                        "case_id": {"type": "string", "enum": list(case_ids)},
                        "pairs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["reference_id", "candidate_id", "relation"],
                                "properties": {
                                    "reference_id": {"type": "integer", "minimum": 0},
                                    "candidate_id": {"type": "integer", "minimum": 0},
                                    "relation": {"type": "string", "enum": ["equivalent", "partial"]},
                                },
                            },
                        },
                    },
                },
            }
        },
    }


def _codex_usage_from_jsonl(value: str) -> dict[str, int]:
    latest: dict[str, int] | None = None
    for line in value.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        candidates = []
        if isinstance(event, dict):
            candidates.append(event.get("usage"))
            item = event.get("item")
            if isinstance(item, dict):
                candidates.append(item.get("usage"))
        for usage in candidates:
            if isinstance(usage, dict) and any(
                field in usage for field in ("input_tokens", "output_tokens", "total_tokens")
            ):
                latest = {
                    field: int(usage.get(field) or 0)
                    for field in (
                        "input_tokens",
                        "cached_input_tokens",
                        "output_tokens",
                        "reasoning_output_tokens",
                        "total_tokens",
                    )
                }
    if latest is None:
        return {}
    if not latest["total_tokens"]:
        latest["total_tokens"] = latest["input_tokens"] + latest["output_tokens"]
    return latest


def aggregate_case_scores(
    case_scores: Sequence[Mapping[str, Any]],
    *,
    case_ids: set[str] | None = None,
) -> dict[str, Any]:
    selected = [
        score
        for score in case_scores
        if case_ids is None or str(score.get("case_id") or "") in case_ids
    ]
    reference = sum(int(score.get("reference_events") or 0) for score in selected)
    candidate = sum(int(score.get("candidate_events") or 0) for score in selected)
    equivalent = sum(int(score.get("equivalent_pairs") or 0) for score in selected)
    partial = sum(int(score.get("partial_pairs") or 0) for score in selected)
    precision = equivalent / candidate if candidate else 0.0
    recall = equivalent / reference if reference else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "case_count": len(selected),
        "reference_events": reference,
        "candidate_events": candidate,
        "equivalent_pairs": equivalent,
        "partial_pairs": partial,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def score_judge_output(packet: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    cases = {str(case["case_id"]): case for case in packet["cases"]}
    evaluations = output.get("evaluations") if isinstance(output, Mapping) else None
    if not isinstance(evaluations, list):
        raise GlmWorkhorseError("judge output has no evaluations")
    seen_cases: set[str] = set()
    aggregate = Counter()
    case_scores = []
    for evaluation in evaluations:
        case_id = str(evaluation.get("case_id") or "")
        if case_id not in cases or case_id in seen_cases:
            raise GlmWorkhorseError("judge output case coverage is invalid")
        seen_cases.add(case_id)
        case = cases[case_id]
        reference_count = len(case["reference"])
        candidate_count = len(case["candidate"])
        valid = []
        invalid_pairs = 0
        for pair in evaluation.get("pairs") or []:
            reference_id = pair.get("reference_id")
            candidate_id = pair.get("candidate_id")
            relation = pair.get("relation")
            if (
                not isinstance(reference_id, int)
                or not isinstance(candidate_id, int)
                or not 0 <= reference_id < reference_count
                or not 0 <= candidate_id < candidate_count
                or relation not in {"equivalent", "partial"}
            ):
                invalid_pairs += 1
                continue
            valid.append((relation, reference_id, candidate_id))
        valid.sort(key=lambda row: (row[0] != "equivalent", row[1], row[2]))
        used_reference: set[int] = set()
        used_candidate: set[int] = set()
        counts = Counter()
        duplicate_pairs = 0
        for relation, reference_id, candidate_id in valid:
            if reference_id in used_reference or candidate_id in used_candidate:
                duplicate_pairs += 1
                continue
            used_reference.add(reference_id)
            used_candidate.add(candidate_id)
            counts[relation] += 1
        equivalent = counts["equivalent"]
        precision = equivalent / candidate_count if candidate_count else (1.0 if not reference_count else 0.0)
        recall = equivalent / reference_count if reference_count else (1.0 if not candidate_count else 0.0)
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        case_scores.append(
            {
                "case_id": case_id,
                "reference_events": reference_count,
                "candidate_events": candidate_count,
                "equivalent_pairs": equivalent,
                "partial_pairs": counts["partial"],
                "invalid_pairs": invalid_pairs,
                "duplicate_pairs": duplicate_pairs,
                "precision": round(precision, 6),
                "recall": round(recall, 6),
                "f1": round(f1, 6),
            }
        )
        aggregate.update(
            reference=reference_count,
            candidate=candidate_count,
            equivalent=equivalent,
            partial=counts["partial"],
            invalid=invalid_pairs,
            duplicate=duplicate_pairs,
        )
    if seen_cases != set(cases):
        raise GlmWorkhorseError("judge output omitted canary cases")
    precision = aggregate["equivalent"] / aggregate["candidate"] if aggregate["candidate"] else 0.0
    recall = aggregate["equivalent"] / aggregate["reference"] if aggregate["reference"] else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "case_count": len(case_scores),
        "reference_events": aggregate["reference"],
        "candidate_events": aggregate["candidate"],
        "equivalent_pairs": aggregate["equivalent"],
        "partial_pairs": aggregate["partial"],
        "invalid_pairs": aggregate["invalid"],
        "duplicate_pairs": aggregate["duplicate"],
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "case_scores": case_scores,
    }


def build_source_audit_packet(
    receipt: Mapping[str, Any],
    *,
    source_root: Path,
    validated_root: Path,
) -> dict[str, Any]:
    cases = {case.segment_id: case for case in select_blinded_cases(source_root, limit=len(receipt["results"]))}
    rendered = []
    for result in receipt["results"]:
        segment_id = str(result["segment_id"])
        case = cases[segment_id]
        candidate_path = validated_root / f"{segment_id}.private.json"
        candidates = json.loads(candidate_path.read_text(encoding="utf-8")).get("events") if candidate_path.exists() else []
        rendered.append(
            {
                "case_id": segment_id,
                "evidence_eligible_extract_text": case.extract_text,
                "candidates": [
                    {
                        "candidate_id": index,
                        "event_type": event.get("event_type"),
                        "claim_text": event.get("claim_text"),
                        "evidence": event.get("evidence"),
                    }
                    for index, event in enumerate(candidates or [])
                ],
            }
        )
    return {
        "instructions": (
            "Audit every candidate directly against its evidence-eligible transcript. "
            "support=supported only when the claim is fully entailed, partial when directionally "
            "grounded but overstated or missing a qualification, unsupported otherwise. "
            "distinctness=duplicate for semantic restatements of another candidate in the same case "
            "and fragment when it is not independently useful. signal=useful only when the proposition "
            "has concrete downstream research or graph value rather than being setup, banter, a bare "
            "mention, or an ordinary term. type_fit=neighbor when a defensible adjacent ontology family "
            "fits but the chosen type is not best. verdict=keep for usable as-is, revise for a grounded "
            "candidate needing claim/type cleanup, and drop for unsupported, duplicate, fragmentary, or "
            "low-signal candidates. Judge the transcript, not the frozen reference set."
        ),
        "cases": rendered,
    }


def source_audit_schema(case_ids: Sequence[str], candidate_count: int) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["evaluations"],
        "properties": {
            "evaluations": {
                "type": "array",
                "minItems": candidate_count,
                "maxItems": candidate_count,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "case_id",
                        "candidate_id",
                        "support",
                        "distinctness",
                        "signal",
                        "type_fit",
                        "verdict",
                    ],
                    "properties": {
                        "case_id": {"type": "string", "enum": list(case_ids)},
                        "candidate_id": {"type": "integer", "minimum": 0},
                        "support": {"type": "string", "enum": ["supported", "partial", "unsupported"]},
                        "distinctness": {"type": "string", "enum": ["distinct", "duplicate", "fragment"]},
                        "signal": {"type": "string", "enum": ["useful", "low_signal"]},
                        "type_fit": {"type": "string", "enum": ["correct", "neighbor", "wrong"]},
                        "verdict": {"type": "string", "enum": ["keep", "revise", "drop"]},
                    },
                },
            }
        },
    }


def score_source_audit(packet: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        (str(case["case_id"]), int(candidate["candidate_id"]))
        for case in packet["cases"]
        for candidate in case["candidates"]
    }
    evaluations = output.get("evaluations") if isinstance(output, Mapping) else None
    if not isinstance(evaluations, list):
        raise GlmWorkhorseError("source audit output has no evaluations")
    observed: set[tuple[str, int]] = set()
    counts: dict[str, Counter[str]] = {
        field: Counter()
        for field in ("support", "distinctness", "signal", "type_fit", "verdict")
    }
    clean_keep = 0
    for evaluation in evaluations:
        key = (str(evaluation.get("case_id") or ""), int(evaluation.get("candidate_id", -1)))
        if key not in expected or key in observed:
            raise GlmWorkhorseError("source audit candidate coverage is invalid")
        observed.add(key)
        for field in counts:
            counts[field][str(evaluation.get(field) or "")] += 1
        if (
            evaluation.get("support") == "supported"
            and evaluation.get("distinctness") == "distinct"
            and evaluation.get("signal") == "useful"
            and evaluation.get("type_fit") in {"correct", "neighbor"}
            and evaluation.get("verdict") == "keep"
        ):
            clean_keep += 1
    if observed != expected:
        raise GlmWorkhorseError("source audit omitted candidates")
    total = len(expected)
    return {
        "candidate_count": total,
        "support": dict(sorted(counts["support"].items())),
        "distinctness": dict(sorted(counts["distinctness"].items())),
        "signal": dict(sorted(counts["signal"].items())),
        "type_fit": dict(sorted(counts["type_fit"].items())),
        "verdict": dict(sorted(counts["verdict"].items())),
        "supported_or_partial_fraction": round(
            (counts["support"]["supported"] + counts["support"]["partial"]) / total if total else 1.0, 6
        ),
        "actionable_fraction": round(
            (counts["verdict"]["keep"] + counts["verdict"]["revise"]) / total if total else 1.0, 6
        ),
        "clean_keep_count": clean_keep,
        "clean_keep_fraction": round(clean_keep / total if total else 1.0, 6),
    }


def run_source_audit(
    *,
    receipt_path: Path,
    judge_model: str = DEFAULT_JUDGE_MODEL,
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
    codex_binary: str = "codex",
) -> dict[str, Any]:
    receipt_file = Path(receipt_path).expanduser().resolve()
    receipt = json.loads(receipt_file.read_text(encoding="utf-8"))
    if receipt.get("schema_version") != CANARY_SCHEMA_VERSION:
        raise GlmWorkhorseError("receipt is not a GLM candidate canary")
    output_root = receipt_file.parent
    packet = build_source_audit_packet(
        receipt,
        source_root=Path(str(receipt["source_root"])),
        validated_root=output_root / "validated",
    )
    case_ids = [str(case["case_id"]) for case in packet["cases"]]
    candidate_count = sum(len(case["candidates"]) for case in packet["cases"])
    schema = source_audit_schema(case_ids, candidate_count)
    audit_root = output_root / "source-aware-audit"
    audit_root.mkdir(exist_ok=True)
    job_path = audit_root / "job.private.json"
    schema_path = audit_root / "schema.json"
    output_path = audit_root / "output.private.json"
    _write_json(job_path, packet)
    _write_json(schema_path, schema)
    command = [
        codex_binary,
        "exec",
        "-m",
        judge_model,
        "-c",
        f'model_reasoning_effort="{reasoning_effort}"',
        "-C",
        str(audit_root),
        "--skip-git-repo-check",
        "--ignore-rules",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--json",
        "-",
    ]
    prompt = (
        "Perform the source-aware private candidate audit in the attached packet. "
        "Evaluate every candidate exactly once and return only schema-valid JSON.\n\n"
        "# Private packet\n" + _canonical_json(packet)
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            input=prompt,
            text=True,
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
        timed_out = False
        stdout = completed.stdout
        stderr = completed.stderr
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = _timeout_output(exc.stdout)
        stderr = _timeout_output(exc.stderr)
        exit_code = None
    elapsed = round(time.monotonic() - started, 3)
    _write_text(audit_root / "run.private.jsonl", stdout)
    _write_text(audit_root / "stderr.private.txt", stderr)
    if timed_out or exit_code != 0 or not output_path.is_file():
        raise GlmWorkhorseError("Codex source-aware audit did not complete")
    output = json.loads(output_path.read_text(encoding="utf-8"))
    _validate_schema(schema, output, path="$")
    score = score_source_audit(packet, output)
    gate = {
        "all_cases_usable": int(receipt["summary"]["usable_case_count"]) == int(receipt["summary"]["case_count"]),
        "supported_or_partial_at_least_0_95": score["supported_or_partial_fraction"] >= 0.95,
        "actionable_at_least_0_75": score["actionable_fraction"] >= 0.75,
        "clean_keep_at_least_0_60": score["clean_keep_fraction"] >= 0.60,
    }
    gate["passed"] = all(gate.values())
    result = {
        "schema_version": SOURCE_AUDIT_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "canary_receipt_path": str(receipt_file),
        "judge_model": judge_model,
        "reasoning_effort": reasoning_effort,
        "elapsed_seconds": elapsed,
        "usage": _codex_usage_from_jsonl(stdout),
        "score": score,
        "gate": gate,
        "reference_set_used_for_candidate_validity": False,
        "production_enabled": False,
        "production_mutation": False,
        "queue_mutation": False,
        "canonical_db_opened": False,
    }
    _write_json(audit_root / "receipt.json", result)
    print(_canonical_json({"status": "source_audit_complete", "receipt_path": str(audit_root / "receipt.json"), "score": score, "gate": gate}), flush=True)
    return result


def run_codex_judge(
    *,
    receipt_path: Path,
    judge_model: str = DEFAULT_JUDGE_MODEL,
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
    codex_binary: str = "codex",
) -> dict[str, Any]:
    receipt_file = Path(receipt_path).expanduser().resolve()
    receipt = json.loads(receipt_file.read_text(encoding="utf-8"))
    if receipt.get("schema_version") != CANARY_SCHEMA_VERSION:
        raise GlmWorkhorseError("receipt is not a GLM candidate canary")
    output_root = receipt_file.parent
    packet = build_judge_packet(
        receipt,
        source_root=Path(str(receipt["source_root"])),
        validated_root=output_root / "validated",
    )
    case_ids = [str(case["case_id"]) for case in packet["cases"]]
    schema = judge_schema(case_ids)
    judge_root = output_root / "codex-judge"
    judge_root.mkdir(exist_ok=True)
    job_path = judge_root / "job.private.json"
    schema_path = judge_root / "schema.json"
    output_path = judge_root / "output.private.json"
    log_path = judge_root / "run.private.jsonl"
    _write_json(job_path, packet)
    _write_json(schema_path, schema)
    command = [
        codex_binary,
        "exec",
        "-m",
        judge_model,
        "-c",
        f'model_reasoning_effort="{reasoning_effort}"',
        "-C",
        str(judge_root),
        "--skip-git-repo-check",
        "--ignore-rules",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--json",
        "-",
    ]
    prompt = (
        "Perform the strict blinded semantic alignment job in the attached private JSON packet. "
        "Return only schema-valid JSON.\n\n# Private packet\n" + _canonical_json(packet)
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            input=prompt,
            text=True,
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
        timed_out = False
        stdout = completed.stdout
        stderr = completed.stderr
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        exit_code = None
    elapsed = round(time.monotonic() - started, 3)
    _write_text(log_path, stdout)
    _write_text(judge_root / "stderr.private.txt", stderr)
    if timed_out or exit_code != 0 or not output_path.is_file():
        raise GlmWorkhorseError("Codex semantic judge did not complete")
    output = json.loads(output_path.read_text(encoding="utf-8"))
    _validate_schema(schema, output, path="$")
    score = score_judge_output(packet, output)
    usable_cases = int(receipt["summary"]["usable_case_count"])
    case_count = int(receipt["summary"]["case_count"])
    usable_case_ids = {
        str(result["segment_id"])
        for result in receipt["results"]
        if bool(result.get("usable"))
    }
    successful_transport_score = aggregate_case_scores(
        score["case_scores"],
        case_ids=usable_case_ids,
    )
    grounding_pass = usable_cases == case_count
    partial_coverage = (
        (score["equivalent_pairs"] + score["partial_pairs"]) / score["reference_events"]
        if score["reference_events"]
        else 1.0
    )
    gate = {
        "all_cases_usable": grounding_pass,
        "equivalent_reference_coverage_at_least_0_70": score["recall"] >= 0.70,
        "equivalent_or_partial_reference_coverage_at_least_0_80": partial_coverage >= 0.80,
        "invalid_judge_pairs_zero": score["invalid_pairs"] == 0,
        "passed": bool(
            grounding_pass
            and score["recall"] >= 0.70
            and partial_coverage >= 0.80
            and score["invalid_pairs"] == 0
        ),
    }
    result = {
        "schema_version": JUDGE_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "canary_receipt_path": str(receipt_file),
        "canary_receipt_sha256": _sha256_bytes(receipt_file.read_bytes()),
        "judge_model": judge_model,
        "reasoning_effort": reasoning_effort,
        "elapsed_seconds": elapsed,
        "usage": _codex_usage_from_jsonl(stdout),
        "score": score,
        "successful_transport_score": successful_transport_score,
        "reference_alignment_is_advisory_not_precision": True,
        "equivalent_or_partial_reference_coverage": round(partial_coverage, 6),
        "gate": gate,
        "codex_work_reduction_proxy": {
            "definition": "fraction of frozen reference events already nominated as semantically equivalent GLM candidates",
            "fraction": score["recall"],
            "equivalent_candidate_events": score["equivalent_pairs"],
            "reference_events": score["reference_events"],
        },
        "production_enabled": False,
        "production_mutation": False,
        "queue_mutation": False,
        "canonical_db_opened": False,
        "promotion_authorized": False,
    }
    _write_json(judge_root / "receipt.json", result)
    print(_canonical_json({"status": "judge_complete", "receipt_path": str(judge_root / "receipt.json"), "score": score, "gate": gate}), flush=True)
    return result


def _default_run_root() -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return DEFAULT_LAB_ROOT / f"glm52-canary-{stamp}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pif lab glm-workhorse")
    sub = parser.add_subparsers(dest="command", required=True)
    status = sub.add_parser("status")
    status.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    status.add_argument("--opencode-binary", default="/opt/homebrew/bin/opencode")

    canary = sub.add_parser("canary")
    canary.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    canary.add_argument("--output-root", type=Path)
    canary.add_argument("--limit", type=int, default=20)
    canary.add_argument("--concurrency", type=int, default=2)
    canary.add_argument("--model", default=DEFAULT_MODEL)
    canary.add_argument("--timeout-seconds", type=int, default=180)
    canary.add_argument("--retry-count", type=int, default=0)
    canary.add_argument("--max-events", type=int, default=10)
    canary.add_argument("--opencode-binary", default="/opt/homebrew/bin/opencode")
    canary.add_argument("--transport", choices=("opencode", "codex"), default="opencode")
    canary.add_argument("--codex-binary", default="codex")
    canary.add_argument("--execute", action="store_true")

    judge = sub.add_parser("judge")
    judge.add_argument("--receipt", type=Path, required=True)
    judge.add_argument("--model", default=DEFAULT_JUDGE_MODEL)
    judge.add_argument("--reasoning-effort", default="low")
    judge.add_argument("--timeout-seconds", type=int, default=600)
    judge.add_argument("--codex-binary", default="codex")
    judge.add_argument("--execute", action="store_true")

    audit = sub.add_parser("audit")
    audit.add_argument("--receipt", type=Path, required=True)
    audit.add_argument("--model", default=DEFAULT_JUDGE_MODEL)
    audit.add_argument("--reasoning-effort", default="low")
    audit.add_argument("--timeout-seconds", type=int, default=600)
    audit.add_argument("--codex-binary", default="codex")
    audit.add_argument("--execute", action="store_true")

    revalidate = sub.add_parser("revalidate")
    revalidate.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "status":
            binary = Path(args.opencode_binary).expanduser()
            cases = select_blinded_cases(args.source_root, limit=20)
            result = {
                "ok": binary.is_file() and len(cases) == 20,
                "status": "ready" if binary.is_file() and len(cases) == 20 else "setup_required",
                "binary": str(binary.resolve()) if binary.exists() else str(binary),
                "model": DEFAULT_MODEL,
                "frozen_case_count": len(cases),
                "source_count": len({case.source_name for case in cases}),
                "production_enabled": False,
                "queue_mutation": False,
                "canonical_db_opened": False,
                "model_call_made": False,
            }
            print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
            return 0 if result["ok"] else 2
        if args.command == "canary":
            output_root = args.output_root or _default_run_root()
            if not args.execute:
                cases = select_blinded_cases(args.source_root, limit=args.limit)
                print(
                    json.dumps(
                        {
                            "status": "planned",
                            "external_model_call": False,
                            "case_count": len(cases),
                            "source_count": len({case.source_name for case in cases}),
                            "output_root": str(output_root.expanduser().resolve()),
                            "model": args.model,
                            "transport": args.transport,
                            "concurrency": args.concurrency,
                            "timeout_seconds": args.timeout_seconds,
                            "retry_count": args.retry_count,
                            "production_enabled": False,
                            "queue_mutation": False,
                            "canonical_db_opened": False,
                            "next": "repeat with --execute to make the bounded hosted model calls",
                        },
                        ensure_ascii=True,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            run_canary(
                source_root=args.source_root,
                output_root=output_root,
                limit=args.limit,
                concurrency=args.concurrency,
                model=args.model,
                timeout_seconds=args.timeout_seconds,
                retry_count=args.retry_count,
                max_events=args.max_events,
                opencode_binary=args.opencode_binary,
                transport=args.transport,
                codex_binary=args.codex_binary,
            )
            return 0
        if args.command == "judge":
            if not args.execute:
                print(
                    json.dumps(
                        {
                            "status": "planned",
                            "external_model_call": False,
                            "receipt": str(args.receipt.expanduser().resolve()),
                            "judge_model": args.model,
                            "production_enabled": False,
                            "queue_mutation": False,
                            "canonical_db_opened": False,
                            "next": "repeat with --execute to run the blinded Codex semantic judge",
                        },
                        ensure_ascii=True,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            run_codex_judge(
                receipt_path=args.receipt,
                judge_model=args.model,
                reasoning_effort=args.reasoning_effort,
                timeout_seconds=args.timeout_seconds,
                codex_binary=args.codex_binary,
            )
            return 0
        if args.command == "audit":
            if not args.execute:
                print(
                    json.dumps(
                        {
                            "status": "planned",
                            "external_model_call": False,
                            "receipt": str(args.receipt.expanduser().resolve()),
                            "judge_model": args.model,
                            "production_enabled": False,
                            "queue_mutation": False,
                            "canonical_db_opened": False,
                            "next": "repeat with --execute to run the source-aware Codex audit",
                        },
                        ensure_ascii=True,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0
            run_source_audit(
                receipt_path=args.receipt,
                judge_model=args.model,
                reasoning_effort=args.reasoning_effort,
                timeout_seconds=args.timeout_seconds,
                codex_binary=args.codex_binary,
            )
            return 0
        if args.command == "revalidate":
            revalidate_receipt(args.receipt)
            return 0
    except (GlmWorkhorseError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "production_enabled": False,
                    "queue_mutation": False,
                    "canonical_db_opened": False,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
