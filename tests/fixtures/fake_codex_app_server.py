from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any


def emit(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def usage(turn_index: int) -> dict[str, int]:
    input_tokens = 100 + turn_index
    output_tokens = 10
    return {
        "inputTokens": input_tokens,
        "cachedInputTokens": 20,
        "outputTokens": output_tokens,
        "reasoningOutputTokens": 3,
        "totalTokens": input_tokens + output_tokens,
    }


def agent_text(thread_id: str, turn_id: str) -> str:
    return json.dumps(
        {"ok": True, "thread_id": thread_id, "turn_id": turn_id},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def emit_success(thread_id: str, turn_id: str, turn_index: int, *, invalid_output: bool = False) -> None:
    text = "not-json" if invalid_output else agent_text(thread_id, turn_id)
    emit(
        {
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": 1,
                "item": {
                    "id": f"item-{turn_id}",
                    "type": "agentMessage",
                    "text": text,
                    "phase": "final_answer",
                },
            },
        }
    )
    token_usage = usage(turn_index)
    emit(
        {
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "tokenUsage": {"last": token_usage, "total": token_usage},
            },
        }
    )
    emit(
        {
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {
                    "id": turn_id,
                    "items": [],
                    "status": "completed",
                    "durationMs": 12,
                },
            },
        }
    )


def emit_episode_batch_success(
    thread_id: str,
    turn_id: str,
    turn_index: int,
    *,
    output_schema: dict[str, Any],
) -> None:
    properties = output_schema.get("properties") or {}
    episode_ids = (properties.get("episode_id") or {}).get("enum") or []
    segment_item = ((properties.get("segments") or {}).get("items") or {}).get("properties") or {}
    segment_ids = (segment_item.get("segment_id") or {}).get("enum") or []
    payload = {
        "episode_id": episode_ids[0],
        "segments": [
            {
                "segment_id": segment_id,
                "status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 0.9,
                    "rationale": "Synthetic fixture output contains no durable event.",
                },
                "no_signal_reason": "Synthetic fixture output contains no durable event.",
                "events": [],
            }
            for segment_id in segment_ids
        ],
    }
    text = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    emit(
        {
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": 1,
                "item": {
                    "id": f"item-{turn_id}",
                    "type": "agentMessage",
                    "text": text,
                    "phase": "final_answer",
                },
            },
        }
    )
    token_usage = usage(turn_index)
    emit(
        {
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "tokenUsage": {"last": token_usage, "total": token_usage},
            },
        }
    )
    emit(
        {
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {"id": turn_id, "items": [], "status": "completed", "durationMs": 12},
            },
        }
    )


def _text_metadata(text: str) -> dict[str, Any]:
    return {
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "bytes": len(text.encode("utf-8")),
        "contains_shared_episode_context": "# Shared episode title, context, and extraction guidance" in text,
        "contains_segment_packets": "# Segment-specific IDs and fixed evidence windows" in text,
    }


def write_log(path: Path | None, method: str, params: dict[str, Any]) -> None:
    if path is None:
        return
    record: dict[str, Any] = {"method": method}
    if method == "thread/start":
        record["base_instructions"] = _text_metadata(str(params.get("baseInstructions") or ""))
    elif method == "turn/start":
        inputs = params.get("input") or []
        prompt = "".join(
            str(item.get("text") or "")
            for item in inputs
            if isinstance(item, dict) and item.get("type") == "text"
        )
        record["prompt"] = _text_metadata(prompt)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=[
            "success",
            "interleaved",
            "timeout",
            "death",
            "malformed",
            "failed",
            "invalid_output",
            "episode_batch",
            "large_notification",
        ],
        default="success",
    )
    parser.add_argument("--log")
    args = parser.parse_args()
    log_path = Path(args.log) if args.log else None
    initialized = False
    thread_count = 0
    turn_count = 0
    pending_interleaved: list[tuple[str, str, int]] = []
    threads: dict[str, dict[str, Any]] = {}
    thread_turns: dict[str, list[dict[str, Any]]] = {}

    def thread_response(thread_id: str) -> dict[str, Any]:
        metadata = threads[thread_id]
        ephemeral = bool(metadata["ephemeral"])
        return {
            "thread": {
                "id": thread_id,
                "cwd": metadata["cwd"],
                "ephemeral": ephemeral,
                "path": None if ephemeral else f"/tmp/fake-codex-home/sessions/{thread_id}.jsonl",
                "turns": list(thread_turns[thread_id]),
            },
            "model": metadata["model"],
            "modelProvider": "openai",
            "cwd": metadata["cwd"],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": {"type": "readOnly", "networkAccess": False},
            "instructionSources": ["/tmp/fake/AGENTS.md"],
            "reasoningEffort": metadata.get("reasoning_effort"),
        }

    for line in sys.stdin:
        message = json.loads(line)
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") or {}
        write_log(log_path, str(method), params)
        if method == "initialize":
            client = params.get("clientInfo") or {}
            if client.get("name") != "podcast-intelligence-factory":
                emit({"id": request_id, "error": {"code": -32602, "message": "bad client"}})
                continue
            emit(
                {
                    "id": request_id,
                    "result": {
                        "codexHome": "/tmp/fake-codex-home",
                        "platformFamily": "unix",
                        "platformOs": "macos",
                        "userAgent": "fake-codex-app-server/0.144.1",
                    },
                }
            )
        elif method == "initialized":
            initialized = True
        elif method == "account/read":
            if not initialized:
                emit({"id": request_id, "error": {"code": -32000, "message": "not initialized"}})
                continue
            emit(
                {
                    "id": request_id,
                    "result": {
                        "account": {
                            "type": "chatgpt",
                            "email": "must-not-appear@example.test",
                            "planType": "pro",
                        },
                        "requiresOpenaiAuth": True,
                    },
                }
            )
        elif method == "account/rateLimits/read":
            emit(
                {
                    "id": request_id,
                    "result": {
                        "rateLimits": {
                            "primary": {
                                "usedPercent": 17,
                                "resetsAt": 1_788_748_260,
                                "windowDurationMins": 10_080,
                            }
                        }
                    },
                }
            )
        elif method == "model/list":
            emit(
                {
                    "id": request_id,
                    "result": {
                        "data": [
                            {
                                "id": model,
                                "model": model,
                                "displayName": model,
                                "description": "fixture",
                                "hidden": False,
                                "isDefault": model == "gpt-5.6-sol",
                                "defaultReasoningEffort": "low",
                                "supportedReasoningEfforts": [],
                            }
                            for model in ("gpt-5.6-sol", "gpt-5.4-mini", "gpt-5.3-codex-spark")
                        ],
                        "nextCursor": None,
                    },
                }
            )
        elif method == "thread/start":
            valid = bool(
                isinstance(params.get("ephemeral"), bool)
                and params.get("sandbox") == "read-only"
                and params.get("approvalPolicy") == "never"
                and params.get("dynamicTools") == []
                and params.get("allowProviderModelFallback") is False
            )
            if not valid:
                emit({"id": request_id, "error": {"code": -32602, "message": "unsafe thread"}})
                continue
            thread_count += 1
            thread_id = f"thread-{thread_count}"
            threads[thread_id] = {
                "model": params.get("model"),
                "cwd": params.get("cwd"),
                "ephemeral": params.get("ephemeral"),
                "base_instructions": params.get("baseInstructions"),
                "reasoning_effort": None,
                "archived": False,
            }
            thread_turns[thread_id] = []
            emit(
                {
                    "id": request_id,
                    "result": thread_response(thread_id),
                }
            )
        elif method == "thread/resume":
            thread_id = str(params.get("threadId") or "")
            metadata = threads.get(thread_id)
            valid = bool(
                metadata is not None
                and metadata["ephemeral"] is False
                and params.get("model") == metadata["model"]
                and params.get("cwd") == metadata["cwd"]
                and params.get("baseInstructions") == metadata["base_instructions"]
                and params.get("sandbox") == "read-only"
                and params.get("approvalPolicy") == "never"
                and params.get("excludeTurns") is False
            )
            if not valid:
                emit({"id": request_id, "error": {"code": -32602, "message": "unsafe resume"}})
                continue
            emit({"id": request_id, "result": thread_response(thread_id)})
        elif method == "thread/archive":
            thread_id = str(params.get("threadId") or "")
            if (
                thread_id not in threads
                or threads[thread_id]["ephemeral"] is not False
                or threads[thread_id]["archived"] is True
            ):
                emit({"id": request_id, "error": {"code": -32602, "message": "unknown durable thread"}})
                continue
            threads[thread_id]["archived"] = True
            emit({"id": request_id, "result": {}})
        elif method == "thread/list":
            archived = params.get("archived") is True
            data = [
                {"id": thread_id}
                for thread_id, metadata in threads.items()
                if metadata.get("archived") is archived
            ]
            emit(
                {
                    "id": request_id,
                    "result": {"data": data, "nextCursor": None},
                }
            )
        elif method == "turn/start":
            if "outputSchema" not in params or params.get("approvalPolicy") != "never":
                emit({"id": request_id, "error": {"code": -32602, "message": "unsafe turn"}})
                continue
            turn_count += 1
            thread_id = str(params["threadId"])
            turn_id = f"turn-{turn_count}"
            if thread_id in threads:
                threads[thread_id]["reasoning_effort"] = params.get("effort")
            emit(
                {
                    "id": request_id,
                    "result": {
                        "turn": {"id": turn_id, "items": [], "status": "inProgress"}
                    },
                }
            )
            if args.scenario == "death":
                os._exit(7)
            if args.scenario == "malformed":
                sys.stdout.write("{malformed-json\n")
                sys.stdout.flush()
                continue
            if args.scenario == "timeout":
                continue
            if args.scenario == "failed":
                token_usage = usage(turn_count)
                emit(
                    {
                        "method": "thread/tokenUsage/updated",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "tokenUsage": {"last": token_usage, "total": token_usage},
                        },
                    }
                )
                emit(
                    {
                        "method": "turn/completed",
                        "params": {
                            "threadId": thread_id,
                            "turn": {
                                "id": turn_id,
                                "items": [],
                                "status": "failed",
                                "error": {"message": "fixture failure"},
                            },
                        },
                    }
                )
                continue
            if args.scenario == "interleaved":
                pending_interleaved.append((thread_id, turn_id, turn_count))
                if len(pending_interleaved) == 2:
                    second, first = pending_interleaved[1], pending_interleaved[0]
                    emit_success(*second)
                    emit_success(*first)
                continue
            if args.scenario == "episode_batch":
                emit_episode_batch_success(
                    thread_id,
                    turn_id,
                    turn_count,
                    output_schema=params["outputSchema"],
                )
                continue
            if args.scenario == "large_notification":
                emit(
                    {
                        "method": "mcpServer/startupStatus/updated",
                        "params": {"blob": "x" * (128 * 1024)},
                    }
                )
            emit_success(
                thread_id,
                turn_id,
                turn_count,
                invalid_output=args.scenario == "invalid_output",
            )
            if args.scenario != "invalid_output" and thread_id in thread_turns:
                thread_turns[thread_id].append(
                    {"id": turn_id, "items": [], "status": "completed"}
                )
        elif method == "turn/interrupt":
            emit({"id": request_id, "result": {}})
        else:
            emit({"id": request_id, "error": {"code": -32601, "message": "unknown method"}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
