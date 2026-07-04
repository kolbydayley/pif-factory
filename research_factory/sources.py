from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .util import parse_bool


def _coerce_value(value: str) -> Any:
    value = value.strip()
    if value in {"", "null", "Null", "NULL"}:
        return None
    if value.lower() in {"true", "false", "yes", "no", "on", "off"}:
        return parse_bool(value)
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def _load_simple_yaml(text: str) -> dict[str, Any]:
    """Parse the narrow YAML subset used by config/sources.yaml.

    This avoids making PyYAML a hard dependency. It supports a top-level
    `sources:` list of scalar key/value mappings.
    """
    sources: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    in_sources = False
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.strip() == "sources:":
            in_sources = True
            continue
        if not in_sources:
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            if current:
                sources.append(current)
            current = {}
            rest = stripped[2:].strip()
            if rest and ":" in rest:
                key, value = rest.split(":", 1)
                current[key.strip()] = _coerce_value(value)
            continue
        if current is not None and ":" in stripped:
            key, value = stripped.split(":", 1)
            current[key.strip()] = _coerce_value(value)
    if current:
        sources.append(current)
    return {"sources": sources}


def load_source_list(path: str | Path) -> list[dict[str, Any]]:
    source_path = Path(path).expanduser().resolve()
    text = source_path.read_text(encoding="utf-8")
    if source_path.suffix.lower() == ".json":
        data = json.loads(text)
    else:
        data = _load_simple_yaml(text)
    sources = data.get("sources", data if isinstance(data, list) else [])
    if not isinstance(sources, list):
        raise ValueError(f"Source list must contain a sources list: {source_path}")
    normalized = []
    for source in sources:
        if not source.get("enabled", True):
            continue
        if not source.get("name"):
            raise ValueError(f"Source entry missing name in {source_path}")
        normalized.append(source)
    return normalized

