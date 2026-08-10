from __future__ import annotations

import concurrent.futures
import hashlib
import json
import random
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

from . import db
from .headless_codex import resolve_codex_binary
from .paths import root
from .util import dumps_json, now_iso, stable_id, write_text_atomic


def build_campaign_stratified_manifest(
    campaign_manifest_paths: list[Path],
    *,
    destination: Path,
) -> dict[str, Any]:
    """Combine immutable campaign manifests without changing their contents."""

    label_ids: list[str] = []
    strata_by_label_id: dict[str, str] = {}
    sources: list[dict[str, Any]] = []
    for path in campaign_manifest_paths:
        value = json.loads(path.read_text(encoding="utf-8"))
        run_id = str(value["run_id"])
        ids = [str(item) for item in value["label_ids"]]
        if any(label_id in strata_by_label_id for label_id in ids):
            raise RuntimeError(f"duplicate label id across campaign strata: {run_id}")
        label_ids.extend(ids)
        strata_by_label_id.update({label_id: run_id for label_id in ids})
        sources.append(
            {
                "path": str(path.resolve()),
                "run_id": run_id,
                "label_count": len(ids),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    manifest = {
        "schema_version": "pif_direction_audit_campaign_strata_v1",
        "created_at": now_iso(),
        "label_ids": label_ids,
        "strata_by_label_id": strata_by_label_id,
        "source_manifests": sources,
    }
    manifest["content_sha256"] = hashlib.sha256(
        dumps_json(manifest).encode("utf-8")
    ).hexdigest()
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(destination, dumps_json(manifest) + "\n")
    return {**manifest, "path": str(destination)}


def _population(manifest_path: Path) -> list[dict[str, Any]]:
    source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    label_ids = source_manifest["label_ids"]
    strata_by_label_id = source_manifest.get("strata_by_label_id") or {}
    conn = db.connect()
    rows = []
    for offset in range(0, len(label_ids), 300):
        chunk = label_ids[offset : offset + 300]
        marks = ",".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"SELECT id, segment_id, output_json FROM labels WHERE id IN ({marks})",
                chunk,
            ).fetchall()
        )
    conn.close()
    result = []
    for row in rows:
        payload = json.loads(row["output_json"])
        for event_index, event in enumerate(payload.get("discourse_events") or []):
            metric = event.get("metric") or {}
            if metric.get("direction") in (None, "not_applicable"):
                continue
            if any(metric.get(key) not in (None, "") for key in ("raw_text", "value", "unit", "comparator")):
                continue
            if not metric.get("direction_evidence"):
                continue
            result.append(
                {
                    "label_id": row["id"],
                    "segment_id": row["segment_id"],
                    "event_index": event_index,
                    "claim_text": event.get("claim_text"),
                    "evidence": event.get("evidence"),
                    "direction": metric.get("direction"),
                    "direction_evidence": metric.get("direction_evidence"),
                    "stratum": str(
                        strata_by_label_id.get(row["id"])
                        or metric.get("direction")
                    ),
                }
            )
    return sorted(result, key=lambda item: (item["label_id"], item["event_index"]))


def _seeded_stratified_sample(
    population: list[dict[str, Any]],
    *,
    max_calls: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    if len(population) <= max_calls:
        result = list(population)
        rng.shuffle(result)
        return result
    strata: dict[str, list[dict[str, Any]]] = {}
    for item in population:
        strata.setdefault(str(item["stratum"]), []).append(item)
    names = sorted(strata)
    if len(names) > max_calls:
        raise ValueError("max_calls must cover every declared audit stratum")
    quotas = {name: max_calls // len(names) for name in names}
    for name in sorted(names, key=lambda value: (-len(strata[value]), value))[
        : max_calls % len(names)
    ]:
        quotas[name] += 1
    selected: list[dict[str, Any]] = []
    for name in names:
        by_direction: dict[str, list[dict[str, Any]]] = {}
        for item in strata[name]:
            by_direction.setdefault(str(item["direction"]), []).append(item)
        for items in by_direction.values():
            rng.shuffle(items)
        directions = sorted(by_direction)
        while len([item for item in selected if item["stratum"] == name]) < min(
            quotas[name], len(strata[name])
        ):
            progressed = False
            for direction in directions:
                if by_direction[direction]:
                    selected.append(by_direction[direction].pop())
                    progressed = True
                    if len(
                        [item for item in selected if item["stratum"] == name]
                    ) >= min(quotas[name], len(strata[name])):
                        break
            if not progressed:
                break
    rng.shuffle(selected)
    return selected


def _prompt(row: dict[str, Any]) -> str:
    return f"""Independently audit one direction-only podcast metric. Judge only whether the direction enum is explicitly supported by the verbatim direction_evidence in context. Do not reward plausibility. Magnitude alone is not increase; a current attribute is not stable; uncertainty about a number is not unknown. Unknown is supported only when the speaker makes a directional claim but leaves its direction indeterminate.

Claim: {row['claim_text']}
Direction: {row['direction']}
Direction evidence: {row['direction_evidence']}
Full event evidence: {row['evidence']}

Return JSON only: {{"supported":true,"failure_kind":null,"explanation":"brief reason","filler":false}}. If unsupported, failure_kind must be one of wrong_direction, stable_filler, unknown_filler, no_directional_claim, ambiguous_direction.
"""


def _usage(path: Path) -> dict[str, int]:
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        usage = item.get("usage") or (item.get("event") or {}).get("usage")
        if isinstance(usage, dict) and isinstance(usage.get("input_tokens"), (int, float)):
            events.append(usage)
    if not events:
        return {"billed": 0, "unique": 0}
    last = events[-1]
    return {
        "billed": int((last.get("input_tokens") or 0) + (last.get("output_tokens") or 0)),
        "unique": int((last.get("input_tokens") or 0) - (last.get("cached_input_tokens") or 0) + (last.get("output_tokens") or 0)),
    }


def run(manifest_path: Path, *, max_calls: int = 15) -> dict[str, Any]:
    population = _population(manifest_path)
    if not population:
        raise RuntimeError("no_retained_direction_metrics")
    seed = f"{manifest_path.stem}-grounded-direction-audit-v1"
    rng = random.Random(seed)
    sample = _seeded_stratified_sample(
        population, max_calls=max_calls, rng=rng
    )
    audit_id = stable_id(now_iso(), seed, prefix="doa_")
    out = root() / "work/pif-ops/direction-only-audit" / audit_id
    out.mkdir(parents=True, exist_ok=False)
    ledger = {
        "schema_version": "pif_direction_only_metric_audit_budget_v2",
        "audit_id": audit_id,
        "declared_at": now_iso(),
        "provider_lane": "codex_subscription",
        "model": "gpt-5.5",
        "bounds": {"max_provider_calls": len(sample), "max_total_tokens": 3_000_000, "max_runtime_seconds": 3_600, "concurrency": 5},
        "cumulative_spend_before_first_call": {"provider_calls": 0, "tokens": 0},
    }
    write_text_atomic(out / "budget-ledger.json", dumps_json(ledger) + "\n")
    manifest = {
        "seed": seed,
        "selection": "seeded_equal_campaign_strata_direction_balanced",
        "population_size": len(population),
        "population_sha256": hashlib.sha256(dumps_json(population).encode()).hexdigest(),
        "population_direction_distribution": dict(Counter(item["direction"] for item in population)),
        "population_stratum_distribution": dict(Counter(item["stratum"] for item in population)),
        "sample_stratum_distribution": dict(Counter(item["stratum"] for item in sample)),
        "sample_direction_distribution": dict(Counter(item["direction"] for item in sample)),
        "sample": sample,
    }
    write_text_atomic(out / "sample-manifest.json", dumps_json(manifest) + "\n")
    codex = resolve_codex_binary()

    def one(index: int, row: dict[str, Any]) -> dict[str, Any]:
        last = out / f"sample-{index:02d}.last.json"
        log = out / f"sample-{index:02d}.log.jsonl"
        started = time.monotonic()
        proc = subprocess.run(
            [codex, "exec", "--ephemeral", "-m", "gpt-5.5", "-C", str(root()), "--sandbox", "danger-full-access", "--output-last-message", str(last), "--json", "-"],
            input=_prompt(row), text=True, capture_output=True, timeout=900,
        )
        write_text_atomic(log, proc.stdout + proc.stderr)
        if proc.returncode:
            return {**row, "call_index": index, "ok": False, "returncode": proc.returncode, "wall_seconds": time.monotonic() - started}
        raw = last.read_text(encoding="utf-8").strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        try:
            verdict = json.loads(raw)
        except json.JSONDecodeError as exc:
            return {**row, "call_index": index, "ok": False, "parse_error": str(exc), "raw": raw, "wall_seconds": time.monotonic() - started}
        return {**row, "call_index": index, "ok": True, "verdict": verdict, "wall_seconds": time.monotonic() - started}

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, len(sample))) as pool:
        results = list(pool.map(lambda pair: one(*pair), enumerate(sample, 1)))
    usages = [_usage(out / f"sample-{index:02d}.log.jsonl") for index in range(1, len(sample) + 1)]
    valid = [item for item in results if item.get("ok")]
    supported = sum(bool(item["verdict"].get("supported")) for item in valid)
    report = {
        "schema_version": "pif_direction_only_metric_audit_report_v2",
        "audit_id": audit_id,
        **{key: manifest[key] for key in ("seed", "selection", "population_size", "population_sha256", "population_direction_distribution", "population_stratum_distribution", "sample_stratum_distribution", "sample_direction_distribution")},
        "sample_size": len(sample),
        "provider_calls": len(sample),
        "valid_verdicts": len(valid),
        "supported": supported,
        "supported_rate": supported / len(valid) if valid else 0.0,
        "failure_distribution": dict(Counter(item["verdict"].get("failure_kind") for item in valid if not item["verdict"].get("supported"))),
        "tokens": {"billed": sum(item["billed"] for item in usages), "last_turn_unique": sum(item["unique"] for item in usages)},
        "wall_seconds": time.monotonic() - started,
        "results": sorted(results, key=lambda item: item["call_index"]),
    }
    write_text_atomic(out / "report.json", dumps_json(report) + "\n")
    return {**report, "report_path": str(out / "report.json"), "ledger_path": str(out / "budget-ledger.json")}
