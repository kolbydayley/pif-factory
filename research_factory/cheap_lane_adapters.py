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


GLM_JSON_INSTRUCTION = """
Your ENTIRE reply must be the JSON object itself: start with "{" and end with "}". No prose before or after, no markdown fences, no commentary.
Output ONLY a single JSON object with exactly these keys:
claims (array of {claim_text, claim_type, evidence, confidence}),
entities ({people, organizations, products} arrays of strings),
topics (array of {topic, stance, intensity, evidence}),
summary (string), needs_review (boolean), overall_confidence (number).
Include every key even when its value is an empty array.
"""


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


_SPEAKER_TAG = re.compile(r"Speaker \d+:\s*")
_WHITESPACE = re.compile(r"\s+")


def _normalize_with_map(text: str) -> tuple:
    """Collapse speaker tags + whitespace; keep normalized→original offset map."""
    stripped = []
    offset_map = []
    i = 0
    while i < len(text):
        tag = _SPEAKER_TAG.match(text, i)
        if tag:
            i = tag.end()
            continue
        stripped.append(text[i])
        offset_map.append(i)
        i += 1
    normalized_chars = []
    normalized_map = []
    prev_space = False
    for ch, orig in zip(stripped, offset_map):
        if ch.isspace():
            if prev_space:
                continue
            normalized_chars.append(" ")
            normalized_map.append(orig)
            prev_space = True
        else:
            normalized_chars.append(ch.lower())
            normalized_map.append(orig)
            prev_space = False
    return "".join(normalized_chars), normalized_map


def ground_span(evidence: str, segment_text: str):
    """Map evidence to an exact contiguous substring of segment_text.

    Exact matches pass through. Otherwise match in speaker-tag-stripped,
    whitespace-collapsed, case-folded space and map back to the original
    exact span. Returns the exact original substring, or None.
    """
    if evidence and evidence in segment_text:
        return evidence
    if not evidence:
        return None
    norm_seg, offset_map = _normalize_with_map(segment_text)
    norm_ev = _WHITESPACE.sub(" ", evidence).strip().lower()
    if not norm_ev:
        return None
    pos = norm_seg.find(norm_ev)
    if pos < 0:
        return None
    start = offset_map[pos]
    end = offset_map[pos + len(norm_ev) - 1] + 1
    return segment_text[start:end]


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
    recovered = 0
    kept_claims = []
    for claim in label["claims"]:
        span = ground_span(claim["evidence"], segment_text)
        if span is None:
            dropped += 1
            continue
        if span != claim["evidence"]:
            claim = dict(claim, evidence=span)
            recovered += 1
        kept_claims.append(claim)
    kept_topics = []
    for topic in label["topics"]:
        span = ground_span(topic["evidence"], segment_text)
        if span is None:
            dropped += 1
            continue
        if span != topic["evidence"]:
            topic = dict(topic, evidence=span)
            recovered += 1
        kept_topics.append(topic)
    cleaned = dict(label)
    cleaned["claims"] = kept_claims
    cleaned["topics"] = kept_topics
    return {"schema_ok": True, "label": cleaned, "dropped": dropped,
            "recovered": recovered, "reason": None}


def window_text(text: str, *, max_chars: int = 6000, overlap_chars: int = 500):
    """Split long text into <=max_chars windows on line boundaries with overlap.

    Every window is a contiguous substring of the original text, so evidence
    grounded in a window is grounded in the full segment.
    """
    if len(text) <= max_chars:
        return [text]
    windows = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        if end < len(text):
            newline = text.rfind("\n", start, end)
            if newline > start + max_chars // 2:
                end = newline
        windows.append(text[start:end])
        if end >= len(text):
            break
        next_start = end - overlap_chars
        newline = text.find("\n", next_start)
        if 0 <= newline < end:
            next_start = newline + 1
        start = max(next_start, start + 1)
    return windows


def merge_window_labels(labels) -> Dict[str, Any]:
    """Merge per-window labels: dedupe claims/topics by evidence, union entities."""
    merged: Dict[str, Any] = {
        "claims": [], "topics": [],
        "entities": {"people": [], "organizations": [], "products": []},
        "summary": "", "needs_review": False, "overall_confidence": 1.0,
    }
    seen_claims, seen_topics = set(), set()
    summaries = []
    for label in labels:
        for claim in label.get("claims") or []:
            key = _WHITESPACE.sub(" ", claim.get("evidence", "")).strip().lower()
            if key and key not in seen_claims:
                seen_claims.add(key)
                merged["claims"].append(claim)
        for topic in label.get("topics") or []:
            key = (topic.get("topic", "").lower(),
                   _WHITESPACE.sub(" ", topic.get("evidence", "")).strip().lower())
            if key not in seen_topics:
                seen_topics.add(key)
                merged["topics"].append(topic)
        for kind in ("people", "organizations", "products"):
            for name in (label.get("entities") or {}).get(kind) or []:
                if name not in merged["entities"][kind]:
                    merged["entities"][kind].append(name)
        if label.get("summary"):
            summaries.append(label["summary"])
        merged["needs_review"] = merged["needs_review"] or bool(label.get("needs_review"))
        conf = label.get("overall_confidence")
        if isinstance(conf, (int, float)):
            merged["overall_confidence"] = min(merged["overall_confidence"], float(conf))
    merged["summary"] = " ".join(summaries)[:600]
    return merged


def draft_with_omission(draft_fn, template: str, window: str, passes: int,
                        omission_suffix: str) -> Dict[str, Any]:
    """Draft one window via ``draft_fn(prompt)``, then run up to ``passes``
    omission-audit passes, merging genuinely new claims into the label.

    Omission passes are best-effort: a failed pass keeps the first-pass label.
    """
    result = draft_fn(template.replace("{SEGMENT_TEXT}", window))
    calls = result.get("calls", 1)
    if not result["ok"]:
        return dict(result, calls=calls)
    label, elapsed = result["label"], result["elapsed"]
    for _ in range(passes):
        prior = "\n".join("- " + (c.get("claim_text") or "")
                          for c in (label.get("claims") or [])
                          if isinstance(c, dict)) or "(none)"
        omission_template = template + omission_suffix.replace("{PRIOR_CLAIMS}", prior)
        second = draft_fn(omission_template.replace("{SEGMENT_TEXT}", window))
        calls += second.get("calls", 1)
        if not second["ok"]:
            break
        elapsed += second["elapsed"]
        extra = (second["label"] or {}).get("claims") or []
        if not extra:
            break
        label = dict(label, claims=list(label.get("claims") or []) + extra)
    return {"ok": True, "label": label, "elapsed": elapsed, "error": None, "calls": calls}


def draft_grok(prompt: str, *, timeout: int = 240,
               reasoning_effort: str = GROK_REASONING_EFFORT) -> Dict[str, Any]:
    """One headless Grok drafting call. ``prompt`` is the fully rendered prompt."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
        handle.write(prompt)
        prompt_path = handle.name
    started = time.monotonic()
    try:
        proc = subprocess.run(
            [GROK_BINARY, "--prompt-file", prompt_path,
             "-m", GROK_MODEL, "--reasoning-effort", reasoning_effort,
             "--json-schema", json.dumps(LABEL_SCHEMA),
             "--disable-web-search", "--no-subagents", "--no-memory", "--no-plan",
             "--max-turns", "1"],
            capture_output=True, text=True, timeout=timeout)
        elapsed = time.monotonic() - started
        if proc.returncode != 0:
            return {"ok": False, "label": None, "elapsed": elapsed,
                    "error": proc.stderr[-500:], "error_class": "provider", "calls": 1}
        return {"ok": True, "label": unwrap_grok_response(proc.stdout),
                "elapsed": elapsed, "error": None, "calls": 1}
    except subprocess.TimeoutExpired:
        return {"ok": False, "label": None, "elapsed": float(timeout),
                "error": "timeout", "error_class": "timeout", "calls": 1}
    except (json.JSONDecodeError, ValueError) as exc:
        return {"ok": False, "label": None,
                "elapsed": time.monotonic() - started,
                "error": f"parse: {exc}", "error_class": "parse", "calls": 1}
    finally:
        os.unlink(prompt_path)


def draft_glm(prompt: str, state_root: Path, *, timeout: int = 300) -> Dict[str, Any]:
    """One OpenCode GLM drafting call in an isolated ephemeral data dir."""
    auth_source = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if not auth_source.is_file():
        return {"ok": False, "label": None, "elapsed": 0.0,
                "error": "opencode auth material unavailable",
                "error_class": "provider", "calls": 0}
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
                        "error": (proc.stderr or proc.stdout)[-500:],
                        "error_class": "provider", "calls": 1}
            return {"ok": True, "label": extract_json_lenient(proc.stdout),
                    "elapsed": elapsed, "error": None, "calls": 1}
    except subprocess.TimeoutExpired:
        return {"ok": False, "label": None, "elapsed": float(timeout),
                "error": "timeout", "error_class": "timeout", "calls": 1}
    except (json.JSONDecodeError, ValueError) as exc:
        return {"ok": False, "label": None,
                "elapsed": time.monotonic() - started,
                "error": f"parse: {exc}", "error_class": "parse", "calls": 1}
