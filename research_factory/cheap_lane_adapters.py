"""Cheap-lane drafting adapters: Grok Build CLI and OpenCode GLM.

Both adapters return ``{"ok", "label", "elapsed", "error"}``. ``validate_label``
enforces the label schema and drops ungrounded events event-granularly (a bad
evidence span removes that event only, never the whole case).

Spec: docs/superpowers/specs/2026-08-13-glm-primary-labeling-budget-governor-design.md
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

GROK_BINARY = str(Path.home() / ".grok" / "bin" / "grok")
OPENCODE_BINARY = str(Path.home() / ".opencode" / "bin" / "opencode")
GROK_MODEL = "grok-4.6"
GROK_REASONING_EFFORT = "low"
GLM_MODEL = "opencode-go/glm-5.2"

CLAIM_TYPES = {"assessment", "product_observation", "prediction",
               "recommendation", "factual_assertion"}
STANCES = {"bullish", "bearish", "neutral", "mixed"}

LABEL_SCHEMA: Dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["claims", "entities", "topics", "summary", "needs_review", "overall_confidence"],
    "properties": {
        "claims": {"type": "array", "items": {"type": "object", "additionalProperties": False,
            "required": ["claim_text", "claim_type", "evidence", "confidence"],
            "properties": {
                "claim_text": {"type": "string"},
                "claim_type": {"type": "string", "enum": sorted(CLAIM_TYPES)},
                "evidence": {"type": "string"},
                "confidence": {"type": "number"}}}},
        "entities": {"type": "object", "additionalProperties": False,
            "required": ["people", "organizations", "products"],
            "properties": {key: {"type": "array", "items": {"type": "string"}}
                           for key in ("people", "organizations", "products")}},
        "topics": {"type": "array", "items": {"type": "object", "additionalProperties": False,
            "required": ["topic", "stance", "intensity", "evidence"],
            "properties": {
                "topic": {"type": "string"},
                "stance": {"type": "string", "enum": sorted(STANCES)},
                "intensity": {"type": "number"},
                "evidence": {"type": "string"}}}},
        "summary": {"type": "string"},
        "needs_review": {"type": "boolean"},
        "overall_confidence": {"type": "number"},
    },
}


def unwrap_grok_response(stdout: str) -> Dict[str, Any]:
    """Grok headless JSON mode wraps the payload as {"text": "<json>"}."""
    raw = json.loads(stdout)
    if isinstance(raw, dict) and "text" in raw and isinstance(raw["text"], str):
        return json.loads(raw["text"])
    return raw


def extract_json_lenient(text: str) -> Dict[str, Any]:
    """Parse a JSON object out of possibly prose- or fence-wrapped output."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        return json.loads(fenced.group(1))
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start:end + 1])
    raise ValueError("no JSON object found in model output")


def _valid_claim(claim: Any) -> bool:
    return (isinstance(claim, dict)
            and isinstance(claim.get("claim_text"), str)
            and claim.get("claim_type") in CLAIM_TYPES
            and isinstance(claim.get("evidence"), str)
            and isinstance(claim.get("confidence"), (int, float)))


def _valid_topic(topic: Any) -> bool:
    return (isinstance(topic, dict)
            and isinstance(topic.get("topic"), str)
            and topic.get("stance") in STANCES
            and isinstance(topic.get("intensity"), (int, float))
            and isinstance(topic.get("evidence"), str))


def validate_label(label: Any, segment_text: str) -> Dict[str, Any]:
    """Schema-check a label and drop ungrounded events event-granularly."""
    if not isinstance(label, dict):
        return {"schema_ok": False, "label": None, "dropped": 0, "reason": "not_object"}
    missing = [key for key in LABEL_SCHEMA["required"] if key not in label]
    if missing:
        return {"schema_ok": False, "label": None, "dropped": 0,
                "reason": f"missing_keys:{','.join(missing)}"}
    entities = label.get("entities")
    if (not isinstance(entities, dict)
            or any(not isinstance(entities.get(k), list)
                   for k in ("people", "organizations", "products"))):
        return {"schema_ok": False, "label": None, "dropped": 0, "reason": "bad_entities"}
    if not isinstance(label.get("claims"), list) or not isinstance(label.get("topics"), list):
        return {"schema_ok": False, "label": None, "dropped": 0, "reason": "bad_lists"}
    if not all(_valid_claim(c) for c in label["claims"]):
        return {"schema_ok": False, "label": None, "dropped": 0, "reason": "bad_claim"}
    if not all(_valid_topic(t) for t in label["topics"]):
        return {"schema_ok": False, "label": None, "dropped": 0, "reason": "bad_topic"}

    dropped = 0
    kept_claims = []
    for claim in label["claims"]:
        if claim["evidence"] and claim["evidence"] in segment_text:
            kept_claims.append(claim)
        else:
            dropped += 1
    kept_topics = []
    for topic in label["topics"]:
        if topic["evidence"] and topic["evidence"] in segment_text:
            kept_topics.append(topic)
        else:
            dropped += 1
    cleaned = dict(label)
    cleaned["claims"] = kept_claims
    cleaned["topics"] = kept_topics
    return {"schema_ok": True, "label": cleaned, "dropped": dropped, "reason": None}


def draft_grok(prompt: str, *, timeout: int = 240) -> Dict[str, Any]:
    """One headless Grok drafting call. ``prompt`` is the fully rendered prompt."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
        handle.write(prompt)
        prompt_path = handle.name
    started = time.monotonic()
    try:
        proc = subprocess.run(
            [GROK_BINARY, "--prompt-file", prompt_path,
             "-m", GROK_MODEL, "--reasoning-effort", GROK_REASONING_EFFORT,
             "--json-schema", json.dumps(LABEL_SCHEMA),
             "--disable-web-search", "--no-subagents", "--no-memory", "--no-plan",
             "--max-turns", "1"],
            capture_output=True, text=True, timeout=timeout)
        elapsed = time.monotonic() - started
        if proc.returncode != 0:
            return {"ok": False, "label": None, "elapsed": elapsed,
                    "error": proc.stderr[-500:]}
        return {"ok": True, "label": unwrap_grok_response(proc.stdout),
                "elapsed": elapsed, "error": None}
    except subprocess.TimeoutExpired:
        return {"ok": False, "label": None, "elapsed": float(timeout), "error": "timeout"}
    except (json.JSONDecodeError, ValueError) as exc:
        return {"ok": False, "label": None,
                "elapsed": time.monotonic() - started, "error": f"parse: {exc}"}
    finally:
        os.unlink(prompt_path)


def draft_glm(prompt: str, state_root: Path, *, timeout: int = 300) -> Dict[str, Any]:
    """One OpenCode GLM drafting call in an isolated ephemeral data dir."""
    auth_source = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if not auth_source.is_file():
        return {"ok": False, "label": None, "elapsed": 0.0,
                "error": "opencode auth material unavailable"}
    state_root = Path(state_root)
    state_root.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(dir=str(state_root)) as workdir:
            data_root = Path(workdir)
            (data_root / "opencode").mkdir()
            auth_target = data_root / "opencode" / "auth.json"
            shutil.copy2(auth_source, auth_target)
            os.chmod(auth_target, 0o600)
            scratch = data_root / "scratch"
            scratch.mkdir()
            env = os.environ.copy()
            env["XDG_DATA_HOME"] = str(data_root)
            proc = subprocess.run(
                [OPENCODE_BINARY, "run", "--pure", "--dir", str(scratch),
                 "--model", GLM_MODEL, prompt],
                capture_output=True, text=True, timeout=timeout, env=env)
            elapsed = time.monotonic() - started
            if proc.returncode != 0:
                return {"ok": False, "label": None, "elapsed": elapsed,
                        "error": (proc.stderr or proc.stdout)[-500:]}
            return {"ok": True, "label": extract_json_lenient(proc.stdout),
                    "elapsed": elapsed, "error": None}
    except subprocess.TimeoutExpired:
        return {"ok": False, "label": None, "elapsed": float(timeout), "error": "timeout"}
    except (json.JSONDecodeError, ValueError) as exc:
        return {"ok": False, "label": None,
                "elapsed": time.monotonic() - started, "error": f"parse: {exc}"}
