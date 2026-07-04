from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc


def now_iso() -> str:
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_id(*parts: str, prefix: str = "") -> str:
    digest = sha256_text("\n".join(parts))[:24]
    return f"{prefix}{digest}" if prefix else digest


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", normalized).strip("-").lower()
    return normalized or "item"


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def loads_json(data: str | bytes | None, default: Any = None) -> Any:
    if not data:
        return default
    return json.loads(data)


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = value.strip()
    try:
        from email.utils import parsedate_to_datetime

        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except Exception:
        pass
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except Exception:
        return None


def parse_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.date.fromisoformat(value)
        return dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
    except ValueError:
        return parse_datetime(value)

