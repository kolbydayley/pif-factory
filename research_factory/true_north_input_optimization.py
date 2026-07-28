"""Fail-closed input-optimization campaigns for the true-north benchmark.

This module deliberately does not import or open the production database.  It
operates only on a frozen suite manifest and its immutable development bundles.
Gold is used only by the scoring process; work packets and public residuals
never contain item-level gold or validation details.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import true_north


SCHEMA_VERSION = "pif_true_north_input_optimization_v1"
FACTORS = (
    "system_prompt",
    "task_rules",
    "surrounding_context",
    "candidate_priors",
    "output_schema",
)
COMBINED_FACTOR = "combined_stack"
CAMPAIGN_FACTORS = FACTORS + (COMBINED_FACTOR,)
FACTOR_ALIASES = {"context": "surrounding_context"}
PHASES = (
    "train",
    "replication",
    "locked_validation",
    "final_confirmation",
    "complete",
)
DEFAULT_FOLDS = {
    "train": ("ep_90c3b5c995bce501c9aef55c", "ep_7ec9f808a3955c720aeb94ff"),
    "replication": ("ep_903cc763f0f106d7f4610f17",),
    "locked_validation": ("ep_8a0c6919d7cfe6bcd9cacd30",),
    "final_confirmation": ("ep_e7630540911fc5dea9850204",),
}
MAX_LIFETIME_ATTEMPTS_PER_PACKET = 3
ALLOWED_TRANSFORMS = {
    "system_prompt": {"identity", "append_system_directives"},
    "task_rules": {"identity", "rewrite_task_rules"},
    "surrounding_context": {"identity", "select_context_strategy"},
    "candidate_priors": {"identity", "present_candidate_priors"},
    "output_schema": {"identity", "emit_output_schema"},
    COMBINED_FACTOR: {"identity", "compose_transforms"},
}
MANDATORY_CANDIDATE_FIELDS = {
    "candidate_id",
    "label_id",
    "segment_id",
    "segment_index",
    "event_index",
    "evidence_text",
    "evidence_start",
    "evidence_end",
    "provenance",
}


class CampaignError(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: Any) -> str:
    raw = value if isinstance(value, bytes) else _canonical(value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _serialized(value: Any) -> bytes:
    return (
        json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _presentation_sha(value: Any) -> str:
    return hashlib.sha256(_serialized(value)).hexdigest()


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _immutable_write(path: Path, value: Any) -> None:
    # Packet property order is an experimental variable.  Hashes remain
    # canonical, while on-disk JSON preserves the validated insertion order.
    payload = _serialized(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise CampaignError(f"immutable artifact differs: {path}")
        return
    with os.fdopen(fd, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


@contextlib.contextmanager
def _exclusive_lease(path: Path, *, ttl_seconds: float = 3600.0):
    path.parent.mkdir(parents=True, exist_ok=True)
    owner_token = uuid.uuid4().hex
    payload = {
        "pid": os.getpid(),
        "owner_token": owner_token,
        "claimed_at_epoch": time.time(),
        "expires_at_epoch": time.time() + ttl_seconds,
    }
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        try:
            existing = _read(path)
        except Exception as exc:
            raise CampaignError(f"lease is unreadable: {path}") from exc
        owner_alive = False
        try:
            os.kill(int(existing.get("pid", -1)), 0)
            owner_alive = True
        except (OSError, ValueError):
            pass
        if owner_alive or float(existing.get("expires_at_epoch", 0)) >= time.time():
            raise CampaignError(f"active lease prevents duplicate work: {path}")
        try:
            path.unlink()
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except (FileNotFoundError, FileExistsError) as exc:
            raise CampaignError(f"lease reclamation raced: {path}") from exc
    with os.fdopen(fd, "wb") as handle:
        handle.write(_serialized(payload))
        handle.flush()
        os.fsync(handle.fileno())
    stopped = threading.Event()

    def heartbeat() -> None:
        interval = min(30.0, max(0.05, ttl_seconds / 3))
        while not stopped.wait(interval):
            try:
                current = _read(path)
                if current.get("owner_token") != owner_token:
                    return
                current["expires_at_epoch"] = time.time() + ttl_seconds
                temporary = path.with_name(
                    f".{path.name}.{owner_token}.heartbeat"
                )
                temporary.write_bytes(_serialized(current))
                os.replace(temporary, path)
            except FileNotFoundError:
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=max(0.1, min(ttl_seconds, 1.0)))
        try:
            current = _read(path)
            if current.get("owner_token") == owner_token:
                path.unlink()
        except FileNotFoundError:
            pass


@contextlib.contextmanager
def _wait_for_exclusive_lease(
    path: Path, *, ttl_seconds: float = 60.0, wait_seconds: float = 30.0
):
    """Acquire a short shared-state mutex, waiting through normal contention."""
    deadline = time.monotonic() + wait_seconds
    manager = None
    while manager is None:
        candidate = _exclusive_lease(path, ttl_seconds=ttl_seconds)
        try:
            candidate.__enter__()
        except CampaignError as exc:
            if (
                "active lease prevents duplicate work" not in str(exc)
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.05)
        else:
            manager = candidate
    try:
        yield
    finally:
        manager.__exit__(None, None, None)


def _identifier(value: str, label: str) -> str:
    if not value or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for ch in value):
        raise CampaignError(f"invalid {label}: {value!r}")
    return value


@dataclass(frozen=True)
class Budget:
    max_calls: int
    max_tokens: int
    max_wall_seconds: float

    def validate(self) -> None:
        if self.max_calls < 1 or self.max_tokens < 1 or self.max_wall_seconds <= 0:
            raise CampaignError("all campaign budgets must be positive")


def _fixed_config(config: Mapping[str, Any]) -> dict[str, Any]:
    required = ("model", "temperature", "steps", "timeout_seconds", "workers", "evaluator_hashes")
    missing = [key for key in required if key not in config]
    if missing:
        raise CampaignError(f"fixed configuration is missing: {missing}")
    if int(config["workers"]) != 1:
        raise CampaignError("campaign execution is serialized; workers must equal 1")
    if float(config["temperature"]) != 0.1 or int(config["steps"]) != 12:
        raise CampaignError("current OpenCode runner is fixed to temperature=0.1 and steps=12")
    result = {key: copy.deepcopy(config[key]) for key in required}
    result["max_attempts_per_packet"] = int(config.get("max_attempts_per_packet", 3))
    result["reserved_output_tokens"] = int(
        config.get("reserved_output_tokens", 16_000)
    )
    result["max_total_tokens_per_attempt"] = int(
        config.get(
            "max_total_tokens_per_attempt",
            config.get("max_tokens_per_attempt", 64_000),
        )
    )
    if (
        result["max_attempts_per_packet"] < 1
        or result["reserved_output_tokens"] < 1
        or result["max_total_tokens_per_attempt"] < 1
        or result["reserved_output_tokens"] >= result["max_total_tokens_per_attempt"]
    ):
        raise CampaignError("attempt and token ceilings must be positive")
    result["fixed_configuration_sha256"] = _sha(result)
    return result


def _nonnegative_finite(value: Any, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CampaignError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (positive and number <= 0):
        raise CampaignError(f"{label} must be finite and {'positive' if positive else 'nonnegative'}")
    return number


def _validate_receipts(
    receipts: Sequence[Mapping[str, Any]],
    *,
    model: str,
    attempt_ceiling: int,
) -> tuple[int, float]:
    if not receipts or len(receipts) > attempt_ceiling:
        raise CampaignError("one or more bounded attempt receipts are required")
    total_tokens = 0
    total_elapsed = 0.0
    for index, receipt in enumerate(receipts):
        if str(receipt.get("provider_model") or "") != model:
            raise CampaignError("receipt provider model differs from fixed model")
        attempt_value = _nonnegative_finite(
            receipt.get("attempt"), f"receipt[{index}].attempt", positive=True
        )
        if not attempt_value.is_integer():
            raise CampaignError("receipt attempt must be an integer")
        elapsed = _nonnegative_finite(
            receipt.get("elapsed_seconds", 0),
            f"receipt[{index}].elapsed_seconds",
        )
        usage = receipt.get("usage")
        if not isinstance(usage, Mapping):
            raise CampaignError("receipt usage must be an object")

        def validate_costs(value: Any, path: str = "usage") -> None:
            if isinstance(value, Mapping):
                for key, item in value.items():
                    child = f"{path}.{key}"
                    if "cost" in str(key).lower() and not isinstance(item, Mapping):
                        _nonnegative_finite(item, child)
                    else:
                        validate_costs(item, child)

        validate_costs(usage)
        token_fields = {
            str(key): value for key, value in usage.items()
            if "token" in str(key).lower()
        }
        for key, value in token_fields.items():
            _nonnegative_finite(value, f"receipt[{index}].usage.{key}")
        if "total_tokens" in usage:
            tokens = int(_nonnegative_finite(
                usage["total_tokens"], f"receipt[{index}].usage.total_tokens"
            ))
        else:
            tokens = sum(
                int(_nonnegative_finite(usage.get(key, 0), f"receipt[{index}].usage.{key}"))
                for key in ("input_tokens", "output_tokens", "reasoning_tokens")
            )
        total_tokens += tokens
        total_elapsed += elapsed
    return total_tokens, total_elapsed


def _validate_manifest(manifest: Mapping[str, Any]) -> tuple[dict[str, Mapping[str, Any]], set[str]]:
    if manifest.get("production_mutation_allowed") is not False:
        raise CampaignError("suite does not explicitly prohibit production mutation")
    development = {
        str(row["episode_id"]): row
        for row in manifest.get("bundles", [])
        if row.get("partition") == "development"
    }
    holdout = {
        str(row["episode_id"])
        for row in manifest.get("bundles", [])
        if row.get("partition") == "holdout"
    }
    if not development:
        raise CampaignError("suite has no development bundles")
    return development, holdout


def _bundle_hash_matches(bundle: Mapping[str, Any], manifest_hash: str) -> bool:
    declared = str(bundle.get("bundle_sha256") or "")
    body = {key: value for key, value in bundle.items() if key != "bundle_sha256"}
    computed = true_north.sha256_text(true_north.dumps_json(body))
    return bool(declared) and declared == str(manifest_hash) == computed


def _manifest_hash_matches(manifest: Mapping[str, Any]) -> bool:
    declared = str(manifest.get("manifest_sha256") or "")
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    return declared == true_north.sha256_text(true_north.dumps_json(body))


def _gold_binding(suite_root: Path) -> dict[str, Any]:
    final = suite_root / "gold" / "development" / "final"
    consensus_path = final / "consensus.private.json"
    gold_path = final / "gold.private.json"
    consensus = _read(consensus_path)
    gold = _read(gold_path)
    consensus_body = {
        key: value for key, value in consensus.items()
        if key != "consensus_sha256"
    }
    gold_body = {
        key: value for key, value in gold.items()
        if key != "gold_sha256"
    }
    if str(consensus.get("consensus_sha256") or "") != true_north.sha256_text(
        true_north.dumps_json(consensus_body)
    ):
        raise CampaignError("consensus gold self-hash mismatch")
    if str(gold.get("gold_sha256") or "") != true_north.sha256_text(
        true_north.dumps_json(gold_body)
    ):
        raise CampaignError("preferred gold self-hash mismatch")
    if consensus.get("source_gold_sha256") != gold["gold_sha256"]:
        raise CampaignError("consensus gold is not bound to preferred gold")
    return {
        "consensus_path": str(consensus_path),
        "consensus_file_sha256": true_north._sha256_file(consensus_path),
        "consensus_sha256": consensus["consensus_sha256"],
        "source_gold_sha256": consensus["source_gold_sha256"],
        "gold_path": str(gold_path),
        "gold_file_sha256": true_north._sha256_file(gold_path),
        "gold_sha256": gold["gold_sha256"],
    }


def _verify_gold_binding(suite_root: Path, expected: Mapping[str, Any]) -> None:
    if _gold_binding(suite_root) != dict(expected):
        raise CampaignError("frozen gold binding drift")


def _hash_field_matches(value: Mapping[str, Any], field: str) -> bool:
    return str(value.get(field) or "") == _sha({
        key: item for key, item in value.items() if key != field
    })


def _validate_folds(
    folds: Mapping[str, Sequence[str]],
    development: Mapping[str, Any],
    holdout: set[str],
) -> dict[str, tuple[str, ...]]:
    normalized: dict[str, tuple[str, ...]] = {}
    seen: set[str] = set()
    for phase in PHASES[:-1]:
        values = tuple(str(value) for value in folds.get(phase, ()))
        if not values:
            raise CampaignError(f"fold is empty: {phase}")
        if set(values) & holdout:
            raise CampaignError("sealed holdout episode requested")
        unknown = set(values) - set(development)
        if unknown:
            raise CampaignError(f"fold contains non-development episodes: {sorted(unknown)}")
        overlap = seen & set(values)
        if overlap:
            raise CampaignError(f"episodes appear in multiple folds: {sorted(overlap)}")
        seen.update(values)
        normalized[phase] = values
    return normalized


def _factor_diff(baseline: Mapping[str, Any], variant: Mapping[str, Any]) -> str:
    changed = [factor for factor in FACTORS if baseline.get(factor) != variant.get(factor)]
    extra = (set(variant) | set(baseline)) - set(FACTORS)
    if extra:
        raise CampaignError(f"unknown input components: {sorted(extra)}")
    if len(changed) != 1:
        raise CampaignError(f"variant must change exactly one input factor; changed={changed}")
    return changed[0]


def _validate_transform(factor: str, transform: Mapping[str, Any]) -> None:
    op = str(transform.get("operation", ""))
    if op not in ALLOWED_TRANSFORMS[factor]:
        raise CampaignError(f"unsupported {factor} transform: {op}")
    params = transform.get("parameters", {})
    if not isinstance(params, Mapping):
        raise CampaignError("transform parameters must be an object")
    if op == "compose_transforms":
        steps = params.get("steps")
        if not isinstance(steps, list) or not (2 <= len(steps) <= len(FACTORS)):
            raise CampaignError("combined stack requires two to five transform steps")
        step_factors: list[str] = []
        for step in steps:
            if not isinstance(step, Mapping):
                raise CampaignError("combined stack steps must be objects")
            step_factor = str(step.get("factor", ""))
            step_transform = step.get("transform")
            if step_factor not in FACTORS or not isinstance(step_transform, Mapping):
                raise CampaignError("combined stack step is outside the five input factors")
            if str(step_transform.get("operation", "")) == "identity":
                raise CampaignError("combined stack cannot contain identity steps")
            _validate_transform(step_factor, step_transform)
            step_factors.append(step_factor)
        if len(step_factors) != len(set(step_factors)):
            raise CampaignError("combined stack may transform each input factor only once")
    elif op == "append_system_directives":
        value = params.get("directives")
        if not isinstance(value, list) or not value or not all(isinstance(x, str) and x.strip() for x in value):
            raise CampaignError("append_system_directives requires non-empty directives[]")
    elif op == "rewrite_task_rules":
        value = params.get("rules")
        if not isinstance(value, list) or not value or not all(isinstance(x, str) and x.strip() for x in value):
            raise CampaignError("rewrite_task_rules requires non-empty rules[]")
    elif op == "select_context_strategy":
        strategy = params.get("strategy")
        if strategy == "episode_context_fields":
            live = {
                "artifact_path", "concept_seed", "context_run_id", "entity_seed",
                "extraction_guidance", "model", "section_map", "speaker_map",
            }
            fields = params.get("ordered_fields")
            if not isinstance(fields, list) or len(fields) != len(set(fields)) or not set(fields) <= live:
                raise CampaignError("ordered_fields must be a unique subset of live context fields")
        elif strategy == "evidence_union_crop":
            if params.get("margin_chars") not in {128, 256, 512}:
                raise CampaignError("crop margin_chars must be 128, 256, or 512")
            if params.get("offset_basis") != "original_segment_text" or params.get("preserve_segment_identity") is not True:
                raise CampaignError("crop must preserve original offsets and segment identity")
        else:
            raise CampaignError("unknown context strategy")
    elif op == "present_candidate_priors":
        strategy = params.get("strategy")
        if strategy == "candidate_field_projection":
            present = [key for key in ("drop_fields", "keep_fields") if key in params]
            if len(present) != 1:
                raise CampaignError("candidate field projection requires exactly one of drop_fields or keep_fields")
            fields = params[present[0]]
            if not isinstance(fields, list) or not fields or len(fields) != len(set(fields)) or not all(isinstance(value, str) and value for value in fields):
                raise CampaignError("candidate projection fields must be unique non-empty strings")
            roots = {value.split(".", 1)[0] for value in fields}
            if present[0] == "drop_fields" and (
                roots & MANDATORY_CANDIDATE_FIELDS
                or any(value.startswith("_source_") for value in fields)
            ):
                raise CampaignError("candidate projection cannot drop evidence, lineage, or embedded source fields")
        elif strategy == "confidence_projection":
            if params.get("mode") != "three_buckets" or params.get("cut_points") != [0.34, 0.67]:
                raise CampaignError("confidence projection must use frozen three-bucket cut points")
        elif strategy == "candidate_field_order":
            fields = params.get("ordered_fields")
            if not isinstance(fields, list) or not fields or len(fields) != len(set(fields)):
                raise CampaignError("candidate field order requires unique ordered_fields[]")
        elif strategy == "candidate_order":
            allowed = {"candidate_id", "segment_id", "segment_index", "event_index", "evidence_start"}
            keys = params.get("sort_keys")
            if not isinstance(keys, list) or not keys or len(keys) != len(set(keys)) or not set(keys) <= allowed:
                raise CampaignError("candidate sort keys are outside the frozen allowlist")
            if not isinstance(params.get("descending"), bool):
                raise CampaignError("candidate order requires boolean descending")
        elif strategy == "candidate_policy":
            if params.get("policy") not in {
                "upstream_candidate_fields_are_nonbinding",
                "exact_evidence_overrides_conflicting_prior_fields",
            }:
                raise CampaignError("unknown candidate-prior policy marker")
        else:
            raise CampaignError("unknown candidate-prior strategy")
    elif op == "emit_output_schema":
        if params.get("normalization_contract") != "canonical_atomic_output_v1":
            raise CampaignError("output schema normalization contract is required")
        styles = {
            "reversible_property_order", "reversible_wrapper",
            "reversible_aliases", "reversible_candidate_keyed",
            "reversible_description_level", "reversible_wrapper_aliases",
            "reversible_candidate_keyed_aliases",
        }
        if params.get("schema_style") not in styles:
            raise CampaignError("unknown reversible output schema style")
        canonical_fields = {
            "schema_version", "items", "candidate_id", "disposition",
            "reason_code", "atomic_claims",
        }
        if set(params.get("required_fields", ())) != canonical_fields:
            raise CampaignError("schema required_fields must be the six canonical transport fields")
        aliases = params.get("aliases")
        if not isinstance(aliases, Mapping) or not set(aliases) <= canonical_fields:
            raise CampaignError("schema aliases must map canonical transport fields")
        if len(set(aliases.values())) != len(aliases):
            raise CampaignError("schema aliases must be reversible")
        if params.get("shape") not in {"canonical", "wrapped", "candidate_keyed"}:
            raise CampaignError("unknown reversible schema shape")


def _ordered_subset(value: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    return {key: copy.deepcopy(value[key]) for key in keys if key in value}


def _drop_dotted(value: dict[str, Any], path: str) -> None:
    parts = path.split(".")
    cursor: Any = value
    for part in parts[:-1]:
        if not isinstance(cursor, dict) or part not in cursor:
            return
        cursor = cursor[part]
    if isinstance(cursor, dict):
        cursor.pop(parts[-1], None)


def _get_dotted(value: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    cursor: Any = value
    for part in path.split("."):
        if not isinstance(cursor, Mapping) or part not in cursor:
            return False, None
        cursor = cursor[part]
    return True, copy.deepcopy(cursor)


def _set_dotted(value: dict[str, Any], path: str, item: Any) -> None:
    parts = path.split(".")
    cursor = value
    for part in parts[:-1]:
        child = cursor.setdefault(part, {})
        if not isinstance(child, dict):
            raise CampaignError(f"dotted projection conflicts at {path}")
        cursor = child
    cursor[parts[-1]] = item


def _mandatory_candidate_copy(candidate: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        key: copy.deepcopy(candidate[key])
        for key in candidate
        if key in MANDATORY_CANDIDATE_FIELDS or key.startswith("_source_")
    }
    missing = MANDATORY_CANDIDATE_FIELDS - set(result)
    if missing:
        raise CampaignError(f"candidate lacks mandatory evidence/lineage fields: {sorted(missing)}")
    return result


def _apply_context_crop(packet: dict[str, Any], margin: int) -> None:
    candidates = packet["input"]["candidates"]
    composite = bool(packet["input"]["episode_context"].get("composite"))
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for candidate in candidates:
        grouped.setdefault((
            str(candidate.get("_source_episode_id", "")),
            str(candidate["segment_id"]),
        ), []).append(candidate)
    segment_index = {
        (str(row["episode_id"]), str(row["segment_id"])): row
        for row in packet["input"].get("segments", [])
    }
    for key, rows in grouped.items():
        segment = segment_index[key] if composite else packet["input"]["segment"]
        text = str(segment["text"])
        start = max(0, min(int(row["evidence_start"]) for row in rows) - margin)
        end = min(len(text), max(int(row["evidence_end"]) for row in rows) + margin)
        segment["text"] = text[start:end]
        segment["crop_original_start"] = start
        segment["crop_original_end"] = end
        segment["offset_basis"] = "original_segment_text"


def _surface_schema(canonical: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(canonical)
    aliases = dict(params["aliases"])
    alias = lambda key: str(aliases.get(key, key))
    item_schema = schema["properties"]["items"]["items"]
    item_props = item_schema["properties"]
    item_order = list(params["item_order"])
    item_schema["properties"] = {
        alias(key): item_props[key] for key in item_order if key in item_props
    } | {
        alias(key): item_props[key] for key in item_props if key not in item_order
    }
    item_schema["required"] = [alias(key) for key in item_schema["required"]]
    items_key = alias("items")
    version_key = alias("schema_version")
    root_props = {
        version_key: schema["properties"]["schema_version"],
        items_key: schema["properties"]["items"],
    }
    schema["properties"] = {
        alias(key): root_props[alias(key)]
        for key in params["root_order"]
        if alias(key) in root_props
    } | {
        key: value for key, value in root_props.items()
        if key not in {alias(raw) for raw in params["root_order"]}
    }
    schema["required"] = [version_key, items_key]
    if params["shape"] == "candidate_keyed":
        candidate_ids = list(canonical["properties"]["items"]["items"]["properties"]["candidate_id"]["enum"])
        keyed_item = copy.deepcopy(item_schema)
        candidate_key = alias("candidate_id")
        keyed_item["properties"].pop(candidate_key, None)
        keyed_item["required"] = [key for key in keyed_item["required"] if key != candidate_key]
        schema["properties"][items_key] = {
            "type": "object",
            # The canonical validator below enforces the exact candidate-ID
            # enum and complete scope after reversible normalization. Avoid
            # duplicating a large item schema once per candidate on the wire.
            "additionalProperties": keyed_item,
            "description": (
                "Keys must be exactly the packet candidate IDs; complete scope "
                f"contains {len(candidate_ids)} entries."
            ),
        }
    if params["shape"] == "wrapped":
        wrapper = str(params["wrapper"])
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": [wrapper],
            "properties": {wrapper: schema},
        }
    schema["title"] = str(params["schema_style"])
    if params["descriptions"]:
        schema["description"] = "Reversible presentation of the canonical atomic output contract."
    return schema


def _apply_transform(job: Mapping[str, Any], factor: str, transform: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
    """Apply the finite transform DSL; return packet and optional system prompt."""
    _validate_transform(factor, transform)
    packet = copy.deepcopy(job)
    op = str(transform["operation"])
    params = copy.deepcopy(dict(transform.get("parameters", {})))
    system_prompt: str | None = None
    if op == "identity":
        return packet, None
    if factor == COMBINED_FACTOR:
        system_prompts: list[str] = []
        for step in params["steps"]:
            packet, step_system_prompt = _apply_transform(
                packet,
                str(step["factor"]),
                step["transform"],
            )
            if step_system_prompt:
                system_prompts.append(step_system_prompt)
        if len(system_prompts) > 1:
            raise CampaignError("combined stack produced multiple system prompts")
        system_prompt = system_prompts[0] if system_prompts else None
    elif factor == "system_prompt":
        system_prompt = true_north.BASE_SYSTEM_PROMPT + " " + " ".join(params["directives"])
    elif factor == "task_rules":
        packet["instructions"] = list(params["rules"])
    elif factor == "surrounding_context":
        if params["strategy"] == "episode_context_fields":
            fields = params["ordered_fields"]
            if packet["input"]["episode_context"].get("composite"):
                for row in packet["input"]["episode_contexts"]:
                    row["context"] = _ordered_subset(row["context"], fields)
            else:
                packet["input"]["episode_context"] = _ordered_subset(
                    packet["input"]["episode_context"], fields
                )
        else:
            _apply_context_crop(packet, int(params["margin_chars"]))
    elif factor == "candidate_priors":
        candidates = packet["input"]["candidates"]
        strategy = params["strategy"]
        if strategy == "candidate_field_projection":
            if "drop_fields" in params:
                for row in candidates:
                    for field in params["drop_fields"]:
                        _drop_dotted(row, str(field))
            else:
                projected = []
                for row in candidates:
                    result = _mandatory_candidate_copy(row)
                    for field in params["keep_fields"]:
                        found, item = _get_dotted(row, str(field))
                        if found:
                            _set_dotted(result, str(field), item)
                    projected.append(result)
                packet["input"]["candidates"] = candidates = projected
        elif strategy == "confidence_projection":
            low, high = params["cut_points"]
            for row in candidates:
                confidence = float(row.get("confidence", 0.0))
                row["confidence"] = (
                    "low" if confidence < low
                    else "medium" if confidence < high
                    else "high"
                )
        elif strategy == "candidate_field_order":
            ordered = []
            for row in candidates:
                fields = list(params["ordered_fields"])
                ordered.append(
                    {key: row[key] for key in fields if key in row}
                    | {key: value for key, value in row.items() if key not in fields}
                )
            packet["input"]["candidates"] = candidates = ordered
        elif strategy == "candidate_order":
            keys = list(params["sort_keys"])
            packet["input"]["candidates"] = sorted(
                candidates,
                key=lambda row: tuple(row.get(key) for key in keys),
                reverse=bool(params["descending"]),
            )
        else:
            packet["input"]["candidate_prior_policy"] = params["policy"]
    else:
        packet["output_schema"] = _surface_schema(packet["output_schema"], params)
        packet["output_schema"]["x-pif-normalization"] = params
    return packet, system_prompt


def normalize_output(output: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    params = packet.get("output_schema", {}).get("x-pif-normalization")
    if not params:
        return copy.deepcopy(dict(output))
    value = copy.deepcopy(dict(output))
    if params["shape"] == "wrapped":
        wrapper = str(params["wrapper"])
        if set(value) != {wrapper} or not isinstance(value[wrapper], Mapping):
            raise CampaignError("schema-wrapped output does not match declared wrapper")
        value = dict(value[wrapper])
    aliases = dict(params["aliases"])
    reverse = {surface: canonical for canonical, surface in aliases.items()}
    value = {reverse.get(key, key): item for key, item in value.items()}
    items = value.get("items")
    if params["shape"] == "candidate_keyed":
        if not isinstance(items, Mapping):
            raise CampaignError("candidate-keyed output must be an object")
        rows = []
        for candidate_id, item in items.items():
            if not isinstance(item, Mapping):
                raise CampaignError("candidate-keyed item must be an object")
            row = {reverse.get(key, key): entry for key, entry in item.items()}
            row["candidate_id"] = candidate_id
            rows.append(row)
        items = rows
    if not isinstance(items, list):
        raise CampaignError("normalized items must be a list")
    value["items"] = [
        {reverse.get(key, key): entry for key, entry in row.items()}
        for row in items
    ]
    if set(value) != {"schema_version", "items"}:
        raise CampaignError("normalization produced non-canonical top-level fields")
    required_item = {"candidate_id", "disposition", "reason_code", "atomic_claims"}
    if any(set(row) != required_item for row in value["items"]):
        raise CampaignError("normalization produced non-canonical item fields")
    return value


def _validate_surface_and_canonical(output: Mapping[str, Any], packet: Mapping[str, Any]) -> None:
    true_north._validate_schema(packet["output_schema"], output, path="$")
    normalized = normalize_output(output, packet)
    canonical = {
        **packet,
        "output_schema": true_north.atomic_output_schema(
            [str(row["candidate_id"]) for row in packet["input"]["candidates"]]
        ),
    }
    true_north._validate_atomic_output(normalized, canonical)


def _screening_selection(
    suite_root: Path,
    episode_ids: Sequence[str],
    *,
    phase: str,
    target: int = 60,
) -> tuple[list[str], dict[str, int]]:
    """Select frozen strata internally without putting truth labels in packets."""
    path = suite_root / "gold" / "development" / "final" / "consensus.private.json"
    document = _read(path)
    rows = [
        row for row in document["items"]
        if str(row["episode_id"]) in set(episode_ids)
    ]
    required_states = (
        {"consensus_junk", "contested"}
        if phase == "train"
        else {"consensus_junk", "consensus_hold", "contested"}
    )
    required = sorted(
        [row for row in rows if row["consensus_state"] in required_states],
        key=lambda row: _sha(str(row["candidate_id"])),
    )
    selected = [str(row["candidate_id"]) for row in required]
    selected_set = set(selected)
    strata: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        if row["consensus_state"] != "consensus_value":
            continue
        count = int(row.get("preferred_atomic_count") or 0)
        band = "0" if count == 0 else "1" if count == 1 else "2" if count == 2 else "3+"
        strata.setdefault((str(row["episode_id"]), band), []).append(row)
    queues = [
        sorted(values, key=lambda row: _sha(str(row["candidate_id"])))
        for _, values in sorted(strata.items())
    ]
    while len(selected) < target and any(queues):
        for queue in queues:
            while queue and str(queue[0]["candidate_id"]) in selected_set:
                queue.pop(0)
            if queue and len(selected) < target:
                candidate_id = str(queue.pop(0)["candidate_id"])
                selected.append(candidate_id)
                selected_set.add(candidate_id)
    counts = {
        "selected": len(selected),
        "junk": sum(row["consensus_state"] == "consensus_junk" for row in required),
        "hold": sum(row["consensus_state"] == "consensus_hold" for row in required),
        "contested": sum(row["consensus_state"] == "contested" for row in required),
        "value": len(selected) - len(required),
    }
    return selected, counts


def _composite_jobs(
    development: Mapping[str, Mapping[str, Any]],
    episode_ids: Sequence[str],
    *,
    selected_candidate_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Build two deterministic, stratified screening packets (~25 items each)."""
    episode_contexts: dict[str, dict[str, Any]] = {}
    queues: list[list[dict[str, Any]]] = []
    for episode_id in episode_ids:
        row = development[episode_id]
        bundle = _read(Path(str(row["bundle_path"])))
        if not _bundle_hash_matches(bundle, str(row["bundle_sha256"])):
            raise CampaignError(f"bundle hash drift: {episode_id}")
        episode_contexts[episode_id] = copy.deepcopy(bundle["episode_context"])
        segments = {str(item["segment_id"]): item for item in bundle["segments"]}
        grouped: dict[str, list[dict[str, Any]]] = {}
        for candidate in bundle["candidates"]:
            enriched = copy.deepcopy(candidate)
            enriched["_source_episode_id"] = episode_id
            grouped.setdefault(str(candidate["segment_id"]), []).append(enriched)
        groups = [{
            "episode_id": episode_id,
            "segment": copy.deepcopy(segments[segment_id]),
            "candidates": sorted(values, key=lambda item: _sha(str(item["candidate_id"]))),
        } for segment_id, values in grouped.items()]
        queues.append(sorted(
            groups,
            key=lambda group: (
                -len(group["candidates"]),
                _sha(str(group["segment"]["segment_id"])),
            ),
        ))
    selected_ids = set(selected_candidate_ids)
    selected: list[dict[str, Any]] = []
    for queue in queues:
        for group in queue:
            candidates = [
                row for row in group["candidates"]
                if str(row["candidate_id"]) in selected_ids
            ]
            if candidates:
                selected.append({**group, "candidates": candidates})
    found = {
        str(candidate["candidate_id"])
        for group in selected
        for candidate in group["candidates"]
    }
    if found != selected_ids:
        raise CampaignError(
            f"screening selection/bundle mismatch: missing={len(selected_ids - found)}"
        )
    selected.sort(key=lambda group: (
        -len(group["candidates"]),
        _sha(str(group["segment"]["segment_id"])),
    ))
    halves: list[list[dict[str, Any]]] = [[], []]
    counts = [0, 0]
    for group in selected:
        index = 0 if counts[0] <= counts[1] else 1
        halves[index].append(group)
        counts[index] += len(group["candidates"])
    jobs = []
    for index, groups in enumerate(halves, start=1):
        if not groups:
            continue
        candidates = [
            candidate for group in groups for candidate in group["candidates"]
        ]
        candidate_ids = [str(row["candidate_id"]) for row in candidates]
        jobs.append({
            "schema_version": true_north.RUN_SCHEMA_VERSION,
            "suite_id": true_north.SUITE_ID,
            "task": "Adjudicate and atomize a deterministic composite development screening batch.",
            "instructions": true_north._atomic_instructions(gold=False),
            "output_schema": true_north.atomic_output_schema(candidate_ids),
            "input": {
                "episode": {"episode_id": f"composite-{index}", "source_episode_ids": list(episode_ids)},
                "episode_context": {"composite": True},
                "episode_contexts": [
                    {"episode_id": episode_id, "context": episode_contexts[episode_id]}
                    for episode_id in episode_ids
                    if any(group["episode_id"] == episode_id for group in groups)
                ],
                "segment": {"segment_id": f"composite-{index}", "segment_index": index - 1},
                "segments": [
                    {"episode_id": group["episode_id"], **group["segment"]}
                    for group in groups
                ],
                "candidates": candidates,
            },
        })
    return jobs


def plan_campaign(
    *,
    suite_root: str | Path,
    campaign_root: str | Path,
    campaign_id: str,
    factor: str,
    baseline: Mapping[str, Any],
    variants: Sequence[Mapping[str, Any]],
    fixed_config: Mapping[str, Any],
    budget: Budget,
    replicates: int = 1,
    folds: Mapping[str, Sequence[str]] | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Validate and optionally materialize an immutable campaign registry/packets."""
    campaign_id = _identifier(campaign_id, "campaign_id")
    factor = FACTOR_ALIASES.get(factor, factor)
    if factor not in CAMPAIGN_FACTORS:
        raise CampaignError(f"unknown factor: {factor}")
    budget.validate()
    if replicates < 1:
        raise CampaignError("replicates must be positive")
    suite_root = Path(suite_root).resolve()
    root = Path(campaign_root).resolve() / campaign_id
    manifest_path = suite_root / "manifest.json"
    manifest = _read(manifest_path)
    if not _manifest_hash_matches(manifest):
        raise CampaignError("suite manifest self-hash mismatch")
    development, holdout = _validate_manifest(manifest)
    fold_map = _validate_folds(folds or DEFAULT_FOLDS, development, holdout)
    fixed = _fixed_config(fixed_config)
    gold_binding = _gold_binding(suite_root)
    screening: dict[str, dict[str, Any]] = {}
    for phase in ("train", "replication"):
        ids, counts = _screening_selection(
            suite_root, fold_map[phase], phase=phase
        )
        screening[phase] = {
            "candidate_ids": ids,
            "counts": counts,
            "candidate_ids_sha256": _sha(ids),
        }
    normalized_variants: list[dict[str, Any]] = []
    for raw in variants:
        variant_id = _identifier(str(raw.get("id", raw.get("variant_id", ""))), "variant_id")
        if str(raw.get("factor", factor)) != factor:
            raise CampaignError(f"variant {variant_id} declares the wrong factor")
        baseline_arm = bool(raw.get("baseline", False))
        declared_change = raw.get("changes_only")
        transform = copy.deepcopy(dict(raw["transform"]))
        _validate_transform(factor, transform)
        if baseline_arm != (transform["operation"] == "identity"):
            raise CampaignError("only a baseline variant may use identity transform")
        if baseline_arm:
            if declared_change != factor:
                raise CampaignError("baseline must still declare its isolated factor")
            components = copy.deepcopy(dict(baseline))
        else:
            if declared_change != factor:
                raise CampaignError("changes_only must name exactly the campaign factor")
            components = copy.deepcopy(dict(baseline))
            if factor == COMBINED_FACTOR:
                for step in transform["parameters"]["steps"]:
                    step_factor = str(step["factor"])
                    components[step_factor] = {
                        "transform_sha256": _sha(step["transform"])
                    }
                changed = [
                    name for name in FACTORS
                    if baseline.get(name) != components.get(name)
                ]
                if len(changed) < 2:
                    raise CampaignError(
                        f"variant {variant_id} is not a multi-factor interaction"
                    )
            else:
                components[factor] = {"transform_sha256": _sha(transform)}
                changed = _factor_diff(baseline, components)
                if changed != factor:
                    raise CampaignError(f"variant {variant_id} does not materially change {factor}")
        normalized_variants.append({
            "variant_id": variant_id,
            "factor": factor,
            "baseline": baseline_arm,
            "changes_only": declared_change,
            "hypothesis": str(raw.get("hypothesis", "")),
            "risk": str(raw.get("risk", "")),
            "expected_tradeoff": str(raw.get("expected_tradeoff", "")),
            "components": components,
            "components_sha256": _sha(components),
            "transform": transform,
            "transform_sha256": _sha(transform),
        })
    if len({row["variant_id"] for row in normalized_variants}) != len(normalized_variants):
        raise CampaignError("variant IDs must be unique")
    registry = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": campaign_id,
        "factor": factor,
        "suite_manifest_path": str(manifest_path),
        "suite_manifest_sha256": _sha(manifest),
        "production_database_open_allowed": False,
        "holdout_access_allowed": False,
        "gold_binding": gold_binding,
        "baseline": copy.deepcopy(dict(baseline)),
        "baseline_sha256": _sha(baseline),
        "variants": normalized_variants,
        "fixed_config": fixed,
        "budget": {
            "max_calls": budget.max_calls,
            "max_tokens": budget.max_tokens,
            "max_wall_seconds": budget.max_wall_seconds,
        },
        "replicates": replicates,
        "folds": {key: list(value) for key, value in fold_map.items()},
        "initial_phase": "train",
        "agent_feedback_policy": "aggregate_train_residuals_only",
        "sampling": {
            phase: {
                "policy_sha256": _sha({
                    "policy": "frozen_consensus_stratified_v1",
                    "phase": phase,
                    "target": 60,
                }),
                "selected_candidate_ids_sha256": screening[phase]["candidate_ids_sha256"],
                "selected_count": len(screening[phase]["candidate_ids"]),
            }
            for phase in ("train", "replication")
        },
    }
    registry["registry_sha256"] = _sha(registry)
    packets: list[dict[str, Any]] = []
    for phase, episode_ids in fold_map.items():
        if phase != "train":
            continue
        composite_jobs = _composite_jobs(
            development,
            episode_ids,
            selected_candidate_ids=screening[phase]["candidate_ids"],
        )
        for variant in normalized_variants:
            for replicate_index in range(1, replicates + 1):
                replicate_id = f"r{replicate_index:02d}"
                for job in composite_jobs:
                    packet_id = str(job["input"]["segment"]["segment_id"])
                    episode_id = "+".join(episode_ids)
                    transformed, system_prompt = _apply_transform(job, factor, variant["transform"])
                    locator = Path(factor) / variant["variant_id"] / replicate_id / phase / episode_id / packet_id
                    packet_path = root / "packets" / locator.with_suffix(".private.json")
                    output_dir = root / "outputs" / locator
                    record = {
                        "phase": phase,
                        "factor": factor,
                        "variant_id": variant["variant_id"],
                        "replicate_id": replicate_id,
                        "fold_id": phase,
                        "episode_id": episode_id,
                        "packet_id": packet_id,
                        "packet_path": str(packet_path),
                        "output_dir": str(output_dir),
                        "checkpoint_namespace": _sha(str(locator)),
                        "packet_sha256": _presentation_sha(transformed),
                        "packet_semantic_sha256": _sha(transformed),
                        "system_prompt_sha256": _sha(system_prompt) if system_prompt else None,
                    }
                    packets.append(record)
                    if not dry_run:
                        _immutable_write(packet_path, transformed)
    presentation_signatures = {
        variant["variant_id"]: _sha([
            {
                "packet_sha256": row["packet_sha256"],
                "system_prompt_sha256": row["system_prompt_sha256"],
            }
            for row in packets
            if row["variant_id"] == variant["variant_id"]
        ])
        for variant in normalized_variants
    }
    reverse_signatures: dict[str, list[str]] = {}
    for variant_id, signature in presentation_signatures.items():
        reverse_signatures.setdefault(signature, []).append(variant_id)
    duplicates = [
        values for values in reverse_signatures.values() if len(values) > 1
    ]
    if duplicates:
        raise CampaignError(
            f"factor contains serialized-identical variant arms: {duplicates}"
        )
    screening_calls = (
        len(_composite_jobs(
            development,
            fold_map["train"],
            selected_candidate_ids=screening["train"]["candidate_ids"],
        ))
        * len(normalized_variants)
        * replicates
    )
    replication_calls = len(_composite_jobs(
        development,
        fold_map["replication"],
        selected_candidate_ids=screening["replication"]["candidate_ids"],
    )) * replicates
    lockbox_calls = sum(
        int(development[episode_id].get("segment_count", 1))
        for phase in ("locked_validation", "final_confirmation")
        for episode_id in fold_map[phase]
    ) * replicates
    estimated_calls = screening_calls + replication_calls + lockbox_calls
    estimated_attempts = estimated_calls * fixed["max_attempts_per_packet"]
    estimated_tokens = estimated_attempts * fixed["max_total_tokens_per_attempt"]
    estimated_wall = estimated_attempts * float(fixed["timeout_seconds"])
    plan = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": campaign_id,
        "dry_run": dry_run,
        "campaign_root": str(root),
        "registry_sha256": registry["registry_sha256"],
        "packet_count": len(packets),
        "packets": packets,
        "variant_presentation_sha256": presentation_signatures,
        "phase_specs": {
            phase: list(fold_map[phase])
            for phase in ("replication", "locked_validation", "final_confirmation")
        },
        "estimated_call_ceiling": estimated_calls,
        "budget_feasible": (
            estimated_attempts <= budget.max_calls
            and estimated_tokens <= budget.max_tokens
            and estimated_wall <= budget.max_wall_seconds
        ),
        "estimated_attempt_ceiling": estimated_attempts,
        "estimated_token_ceiling": estimated_tokens,
        "estimated_wall_ceiling": estimated_wall,
        "estimated_five_factor_attempt_ceiling": (
            estimated_attempts * len(FACTORS)
        ),
    }
    plan["plan_sha256"] = _sha(plan)
    if not plan["budget_feasible"]:
        raise CampaignError("worst-case attempts, tokens, or wall time exceed campaign budget")
    if not dry_run:
        with _exclusive_lease(root / ".plan.lock"):
            _immutable_write(root / "registry.json", registry)
            _immutable_write(root / "plan.json", plan)
            state_path = root / "state.json"
            if not state_path.exists():
                _atomic_state_write(state_path, {
                    "campaign_id": campaign_id,
                    "phase": "train",
                    "registry_sha256": registry["registry_sha256"],
                    "completed_locators": [],
                    "usage": {"calls": 0, "tokens": 0, "wall_seconds": 0.0},
                })
            else:
                _load_campaign(root)
    return plan


def _load_campaign(root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    registry = _read(root / "registry.json")
    if not _hash_field_matches(registry, "registry_sha256"):
        raise CampaignError("campaign registry hash mismatch")
    manifest = _read(Path(registry["suite_manifest_path"]))
    if (
        not _manifest_hash_matches(manifest)
        or _sha(manifest) != registry["suite_manifest_sha256"]
    ):
        raise CampaignError("suite manifest hash mismatch")
    _verify_gold_binding(
        Path(registry["suite_manifest_path"]).parent,
        registry["gold_binding"],
    )
    plan = _read(root / "plan.json")
    if not _hash_field_matches(plan, "plan_sha256"):
        raise CampaignError("campaign plan hash mismatch")
    if plan["registry_sha256"] != registry["registry_sha256"]:
        raise CampaignError("plan is not bound to the registry")
    for phase in ("replication", "locked_validation", "final_confirmation"):
        phase_path = root / "phase-plans" / f"{phase}.json"
        if phase_path.is_file():
            phase_plan = _read(phase_path)
            if not _hash_field_matches(phase_plan, "phase_plan_sha256"):
                raise CampaignError(f"{phase} plan hash mismatch")
            plan["packets"].extend(phase_plan["packets"])
    state = _read(root / "state.json")
    if not _hash_field_matches(state, "state_sha256"):
        raise CampaignError("campaign state hash mismatch")
    if state["registry_sha256"] != registry["registry_sha256"]:
        raise CampaignError("state is not bound to the registry")
    return registry, plan, state


def _materialize_phase(root: Path, registry: Mapping[str, Any], phase: str) -> None:
    """Materialize a selected-survivor phase only when the machine reaches it."""
    destination = root / "phase-plans" / f"{phase}.json"
    if destination.is_file():
        return
    manifest = _read(Path(registry["suite_manifest_path"]))
    if (
        not _manifest_hash_matches(manifest)
        or _sha(manifest) != registry["suite_manifest_sha256"]
    ):
        raise CampaignError("suite manifest hash mismatch during phase materialization")
    _verify_gold_binding(
        Path(registry["suite_manifest_path"]).parent,
        registry["gold_binding"],
    )
    development, holdout = _validate_manifest(manifest)
    episode_ids = tuple(registry["folds"][phase])
    if set(episode_ids) & holdout:
        raise CampaignError("sealed holdout episode requested")
    packets: list[dict[str, Any]] = []
    selected_id = _read(root / "state.json").get("selected_variant_id")
    variants = [
        row for row in registry["variants"] if row["variant_id"] == selected_id
    ]
    if len(variants) != 1:
        raise CampaignError("one registered survivor must be selected before phase materialization")
    for variant in variants:
        for replicate_index in range(1, int(registry["replicates"]) + 1):
            replicate_id = f"r{replicate_index:02d}"
            if phase == "replication":
                selected_ids, _ = _screening_selection(
                    Path(registry["suite_manifest_path"]).parent,
                    episode_ids,
                    phase=phase,
                )
                if _sha(selected_ids) != registry["sampling"][phase]["selected_candidate_ids_sha256"]:
                    raise CampaignError("replication sampling selection hash mismatch")
                jobs_and_episodes = [
                    (job, "+".join(episode_ids))
                    for job in _composite_jobs(
                        development,
                        episode_ids,
                        selected_candidate_ids=selected_ids,
                    )
                ]
            else:
                jobs_and_episodes = []
                for episode_id in episode_ids:
                    row = development[episode_id]
                    bundle = _read(Path(row["bundle_path"]))
                    if not _bundle_hash_matches(bundle, str(row["bundle_sha256"])):
                        raise CampaignError(f"bundle hash drift: {episode_id}")
                    jobs_and_episodes.extend(
                        (job, episode_id)
                        for job in true_north._segment_jobs(bundle, gold=False)
                    )
            for job, episode_id in jobs_and_episodes:
                packet_id = str(job["input"]["segment"]["segment_id"])
                transformed, system_prompt = _apply_transform(
                    job, str(registry["factor"]), variant["transform"]
                )
                locator = Path(str(registry["factor"])) / variant["variant_id"] / replicate_id / phase / episode_id / packet_id
                packet_path = root / "packets" / locator.with_suffix(".private.json")
                _immutable_write(packet_path, transformed)
                packets.append({
                    "phase": phase,
                    "factor": registry["factor"],
                    "variant_id": variant["variant_id"],
                    "replicate_id": replicate_id,
                    "fold_id": phase,
                    "episode_id": episode_id,
                    "packet_id": packet_id,
                    "packet_path": str(packet_path),
                    "output_dir": str(root / "outputs" / locator),
                    "checkpoint_namespace": _sha(str(locator)),
                    "packet_sha256": _presentation_sha(transformed),
                    "packet_semantic_sha256": _sha(transformed),
                    "system_prompt_sha256": _sha(system_prompt) if system_prompt else None,
                })
    phase_plan = {
        "campaign_id": registry["campaign_id"],
        "phase": phase,
        "packets": packets,
        "opened_only_after_prior_phase_score": True,
    }
    phase_plan["phase_plan_sha256"] = _sha(phase_plan)
    _immutable_write(destination, phase_plan)


def _atomic_state_write(path: Path, value: Any) -> None:
    value = {key: item for key, item in value.items() if key != "state_sha256"}
    value["state_sha256"] = _sha(value)
    payload = _canonical(value) + "\n"
    temp = path.with_suffix(".tmp")
    temp.write_text(payload, encoding="utf-8")
    os.replace(temp, path)


def _cohort_budget_reserve(
    *,
    campaign_root: Path,
    campaign_id: str,
    locator: str,
    calls: int,
    tokens: int,
    wall_seconds: float,
) -> str | None:
    """Reserve worst-case shared capacity across concurrently running factors.

    A cohort manifest is optional so older standalone campaigns retain their
    behavior.  When present, every member is fail-closed against one aggregate
    call/token/wall envelope, including in-flight reservations from other
    processes.
    """
    cohort_path = campaign_root.parent / "cohort.json"
    if not cohort_path.is_file():
        return None
    cohort = _read(cohort_path)
    if not _hash_field_matches(cohort, "cohort_sha256"):
        raise CampaignError("cohort manifest hash mismatch")
    members = list(cohort.get("campaign_ids", []))
    if campaign_id not in members:
        return None
    reservation_id = _sha(
        {
            "campaign_id": campaign_id,
            "locator": locator,
            "nonce": uuid.uuid4().hex,
        }
    )
    state_path = campaign_root.parent / "cohort-state.json"
    with _wait_for_exclusive_lease(
        campaign_root.parent / ".cohort-budget.lock"
    ):
        state = (
            _read(state_path)
            if state_path.is_file()
            else {"reservations": {}}
        )
        if state_path.is_file() and not _hash_field_matches(
            state, "state_sha256"
        ):
            raise CampaignError("cohort state hash mismatch")
        actual = {"calls": 0, "tokens": 0, "wall_seconds": 0.0}
        for member in members:
            member_state_path = campaign_root.parent / member / "state.json"
            if not member_state_path.is_file():
                raise CampaignError(f"cohort member state missing: {member}")
            member_state = _read(member_state_path)
            if not _hash_field_matches(member_state, "state_sha256"):
                raise CampaignError(f"cohort member state hash mismatch: {member}")
            usage = member_state["usage"]
            actual["calls"] += int(usage["calls"])
            actual["tokens"] += int(usage["tokens"])
            actual["wall_seconds"] += float(usage["wall_seconds"])
        reserved = {"calls": 0, "tokens": 0, "wall_seconds": 0.0}
        for row in state.get("reservations", {}).values():
            reserved["calls"] += int(row["calls"])
            reserved["tokens"] += int(row["tokens"])
            reserved["wall_seconds"] += float(row["wall_seconds"])
        limits = cohort["budget"]
        proposed = {
            "calls": actual["calls"] + reserved["calls"] + calls,
            "tokens": actual["tokens"] + reserved["tokens"] + tokens,
            "wall_seconds": (
                actual["wall_seconds"]
                + reserved["wall_seconds"]
                + wall_seconds
            ),
        }
        if (
            proposed["calls"] > int(limits["max_calls"])
            or proposed["tokens"] > int(limits["max_tokens"])
            or proposed["wall_seconds"] > float(limits["max_wall_seconds"])
        ):
            raise CampaignError("shared cohort budget cannot cover one worst-case packet")
        state.setdefault("reservations", {})[reservation_id] = {
            "campaign_id": campaign_id,
            "locator_sha256": _sha(locator),
            "calls": calls,
            "tokens": tokens,
            "wall_seconds": wall_seconds,
        }
        _atomic_state_write(state_path, state)
    return reservation_id


def _cohort_budget_release(
    *, campaign_root: Path, reservation_id: str | None
) -> None:
    if reservation_id is None:
        return
    state_path = campaign_root.parent / "cohort-state.json"
    with _wait_for_exclusive_lease(
        campaign_root.parent / ".cohort-budget.lock"
    ):
        state = _read(state_path)
        if not _hash_field_matches(state, "state_sha256"):
            raise CampaignError("cohort state hash mismatch")
        if reservation_id not in state.get("reservations", {}):
            raise CampaignError("cohort reservation is missing")
        del state["reservations"][reservation_id]
        _atomic_state_write(state_path, state)


def reconcile_completed_cohort_reservations(
    *, campaign_parent: str | Path
) -> dict[str, Any]:
    """Remove only reservations whose exact locator is already committed."""
    parent = Path(campaign_parent).resolve()
    state_path = parent / "cohort-state.json"
    removed: list[str] = []
    with _wait_for_exclusive_lease(parent / ".cohort-budget.lock"):
        state = _read(state_path)
        if not _hash_field_matches(state, "state_sha256"):
            raise CampaignError("cohort state hash mismatch")
        completed_hashes: dict[str, set[str]] = {}
        for reservation_id, reservation in list(
            state.get("reservations", {}).items()
        ):
            campaign_id = str(reservation["campaign_id"])
            if campaign_id not in completed_hashes:
                campaign_state = _read(parent / campaign_id / "state.json")
                if not _hash_field_matches(campaign_state, "state_sha256"):
                    raise CampaignError(
                        f"cohort member state hash mismatch: {campaign_id}"
                    )
                completed_hashes[campaign_id] = {
                    _sha(locator)
                    for locator in campaign_state["completed_locators"]
                }
            if (
                str(reservation["locator_sha256"])
                in completed_hashes[campaign_id]
            ):
                del state["reservations"][reservation_id]
                removed.append(reservation_id)
        _atomic_state_write(state_path, state)
    return {
        "removed_count": len(removed),
        "remaining_count": len(state.get("reservations", {})),
        "removed_reservation_ids_sha256": _sha(sorted(removed)),
    }


def _terminalize_decodable_validation_failure(
    *,
    root: Path,
    registry: Mapping[str, Any],
    row: Mapping[str, Any],
    locator: str,
    packet: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Convert a prior decodable-but-invalid answer into a scored failure.

    Retrying schema/semantic noncompliance until it validates would introduce
    survivorship bias.  Transport truncation remains retryable because it
    cannot be decoded; a complete JSON answer that fails surface,
    normalization, or canonical validation is a terminal quality observation.
    """
    output_dir = Path(row["output_dir"])
    if (output_dir / "campaign-result.private.json").is_file():
        return None
    failures = sorted(output_dir.glob("failure-call-*.private.json"))
    checkpoint = output_dir / "attempt-1.private.jsonl"
    if not failures or not checkpoint.is_file():
        return None
    stdout = checkpoint.read_text(encoding="utf-8")
    answer, finish, stream_count = true_north._parse_opencode_stream(stdout)
    try:
        decoded = true_north._decode_json_answer(answer)
    except json.JSONDecodeError:
        return None
    try:
        _validate_surface_and_canonical(decoded, packet)
        normalized = normalize_output(decoded, packet)
        true_north._validate_atomic_output(
            normalized,
            {
                **packet,
                "output_schema": true_north.atomic_output_schema(
                    [
                        str(candidate["candidate_id"])
                        for candidate in packet["input"]["candidates"]
                    ]
                ),
            },
        )
    except Exception as validation_error:
        validation_error_type = type(validation_error).__name__
        validation_error_sha256 = _sha(str(validation_error))
    else:
        return None
    failure = _read(failures[-1])
    stderr_path = output_dir / "attempt-1.stderr.private.txt"
    stderr = (
        stderr_path.read_text(encoding="utf-8")
        if stderr_path.is_file()
        else ""
    )
    receipt = {
        "packet_sha256": row["packet_sha256"],
        "provider_model": registry["fixed_config"]["model"],
        "system_prompt_sha256": (
            row.get("system_prompt_sha256")
            or true_north.sha256_text(true_north.BASE_SYSTEM_PROMPT)
        ),
        "attempt": 10,
        "operational_failure": False,
        "fallback_reason": None,
        "timed_out": False,
        "exit_code": 0,
        "elapsed_seconds": float(failure["elapsed_seconds"]),
        "stream_event_count": stream_count,
        "usage": true_north._usage(finish),
        "stderr_sha256": true_north._sha256_bytes(stderr.encode("utf-8")),
    }
    record = {
        "locator": locator,
        "provider_model": registry["fixed_config"]["model"],
        "packet_sha256": row["packet_sha256"],
        "packet_semantic_sha256": row["packet_semantic_sha256"],
        "output": None,
        "output_sha256": _sha(None),
        "receipts": [receipt],
        "terminal_validation_failure": {
            "kind": "decodable_schema_or_semantic_noncompliance",
            "error_type": validation_error_type,
            "error_sha256": validation_error_sha256,
            "candidate_count": len(packet["input"]["candidates"]),
        },
    }
    _immutable_write(output_dir / "campaign-result.private.json", record)
    return record


def _execute_campaign_serialized(
    *,
    campaign_dir: str | Path,
    runner: Callable[..., tuple[dict[str, Any], list[dict[str, Any]], str]] | None = None,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    max_packets: int | None = None,
) -> dict[str, Any]:
    """Execute/resume only the current phase with unique replicate output dirs."""
    root = Path(campaign_dir).resolve()
    registry, plan, state = _load_campaign(root)
    if state["phase"] == "complete":
        return status_campaign(campaign_dir=root)
    using_default_runner = runner is None
    runner = runner or true_north._run_opencode_packet
    completed = set(state["completed_locators"])
    budget = registry["budget"]
    selected = [row for row in plan["packets"] if row["phase"] == state["phase"]]
    if state["phase"] != "train" and state.get("selected_variant_id"):
        selected = [row for row in selected if row["variant_id"] == state["selected_variant_id"]]
    ran = 0
    for row in selected:
        locator = "/".join((row["factor"], row["variant_id"], row["replicate_id"], row["fold_id"], row["episode_id"], row["packet_id"]))
        if locator in completed:
            continue
        if max_packets is not None and ran >= max_packets:
            break
        attempts = int(registry["fixed_config"]["max_attempts_per_packet"])
        token_ceiling = attempts * int(
            registry["fixed_config"]["max_total_tokens_per_attempt"]
        )
        wall_ceiling = attempts * float(registry["fixed_config"]["timeout_seconds"])
        if (
            state["usage"]["calls"] + attempts > budget["max_calls"]
            or state["usage"]["tokens"] + token_ceiling > budget["max_tokens"]
            or state["usage"]["wall_seconds"] + wall_ceiling > budget["max_wall_seconds"]
        ):
            raise CampaignError("remaining budget cannot cover one worst-case packet")
        packet_path = Path(row["packet_path"])
        if true_north._sha256_file(packet_path) != row["packet_sha256"]:
            raise CampaignError("packet presentation hash mismatch")
        packet = _read(packet_path)
        terminal = _terminalize_decodable_validation_failure(
            root=root,
            registry=registry,
            row=row,
            locator=locator,
            packet=packet,
        )
        if terminal is not None:
            completed.add(locator)
            state["completed_locators"] = sorted(completed)
            _atomic_state_write(root / "state.json", state)
            ran += 1
            continue
        prior_failed_attempts = sum(
            int(_read(path).get("conservative_attempts_accounted", 0))
            for path in Path(row["output_dir"]).glob("failure-call-*.private.json")
        )
        if (
            prior_failed_attempts + attempts
            > MAX_LIFETIME_ATTEMPTS_PER_PACKET
        ):
            raise CampaignError("packet lifetime attempt ceiling exhausted")
        variant = next(item for item in registry["variants"] if item["variant_id"] == row["variant_id"])
        system_prompt = None
        if (
            registry["factor"] == "system_prompt"
            and variant["transform"]["operation"] != "identity"
        ):
            system_prompt = true_north.BASE_SYSTEM_PROMPT + " " + " ".join(
                variant["transform"]["parameters"]["directives"]
            )
        input_token_estimate = (packet_path.stat().st_size + 3) // 4
        if (
            input_token_estimate
            + int(registry["fixed_config"]["reserved_output_tokens"])
            > int(registry["fixed_config"]["max_total_tokens_per_attempt"])
        ):
            raise CampaignError(
                "estimated input plus reserved output exceeds model context ceiling"
            )
        cohort_reservation = _cohort_budget_reserve(
            campaign_root=root,
            campaign_id=str(registry["campaign_id"]),
            locator=locator,
            calls=attempts,
            tokens=token_ceiling,
            wall_seconds=wall_ceiling,
        )
        started = time.monotonic()
        try:
            with _exclusive_lease(
                Path(row["output_dir"]) / ".packet-claim.json",
                ttl_seconds=(
                    attempts * float(registry["fixed_config"]["timeout_seconds"])
                    + 300
                ),
            ):
                runner_kwargs = {
                    "packet_path": packet_path,
                    "output_dir": Path(row["output_dir"]),
                    "models": (registry["fixed_config"]["model"],),
                    "stage": f"input-optimization-{registry['factor']}",
                    "timeout_seconds": int(
                        registry["fixed_config"]["timeout_seconds"]
                    ),
                    "opencode_binary": opencode_binary,
                    "system_prompt": system_prompt,
                    "validator": _validate_surface_and_canonical,
                }
                if using_default_runner:
                    runner_kwargs["_semantic_retry_remaining"] = attempts - 1
                output, receipts, model = runner(
                    **runner_kwargs
                )
                if model != registry["fixed_config"]["model"]:
                    raise CampaignError("runner model violates fixed model identity")
                usage_tokens, receipt_elapsed = _validate_receipts(
                    receipts,
                    model=registry["fixed_config"]["model"],
                    attempt_ceiling=attempts,
                )
                if any(
                    bool(receipt["usage"].get("checkpoint_reuse"))
                    for receipt in receipts
                ):
                    raise CampaignError("checkpoint reuse across campaign replicates is forbidden")
                _validate_surface_and_canonical(output, packet)
                canonical_output = normalize_output(output, packet)
                true_north._validate_atomic_output(
                    canonical_output,
                    {
                        **packet,
                        "output_schema": true_north.atomic_output_schema([
                            str(candidate["candidate_id"])
                            for candidate in packet["input"]["candidates"]
                        ]),
                    },
                )
        except Exception as exc:
            elapsed = round(time.monotonic() - started, 3)
            failure = {
                "locator": locator,
                "error_type": type(exc).__name__,
                "error_sha256": _sha(str(exc)),
                "conservative_attempts_accounted": attempts,
                "conservative_total_tokens_accounted": (
                    int(registry["fixed_config"]["max_total_tokens_per_attempt"])
                    * attempts
                ),
                "elapsed_seconds": elapsed,
            }
            _immutable_write(
                Path(row["output_dir"]) / (
                    f"failure-call-{state['usage']['calls'] + attempts}.private.json"
                ),
                failure,
            )
            state["usage"]["calls"] += attempts
            state["usage"]["tokens"] += (
                int(registry["fixed_config"]["max_total_tokens_per_attempt"])
                * attempts
            )
            state["usage"]["wall_seconds"] = round(
                state["usage"]["wall_seconds"] + elapsed, 3
            )
            _atomic_state_write(root / "state.json", state)
            _cohort_budget_release(
                campaign_root=root, reservation_id=cohort_reservation
            )
            raise
        elapsed = time.monotonic() - started
        result = {
            "locator": locator,
            "provider_model": model,
            "packet_sha256": row["packet_sha256"],
            "packet_semantic_sha256": row["packet_semantic_sha256"],
            "output": canonical_output,
            "output_sha256": _sha(canonical_output),
            "receipts": receipts,
        }
        _immutable_write(Path(row["output_dir"]) / "campaign-result.private.json", result)
        completed.add(locator)
        state["completed_locators"] = sorted(completed)
        state["usage"]["calls"] += len(receipts)
        state["usage"]["tokens"] += usage_tokens
        state["usage"]["wall_seconds"] = round(
            state["usage"]["wall_seconds"] + max(elapsed, receipt_elapsed), 3
        )
        _atomic_state_write(root / "state.json", state)
        _cohort_budget_release(
            campaign_root=root, reservation_id=cohort_reservation
        )
        if state["usage"]["calls"] > budget["max_calls"] or state["usage"]["tokens"] > budget["max_tokens"] or state["usage"]["wall_seconds"] > budget["max_wall_seconds"]:
            raise CampaignError("paid result persisted but receipt exceeded campaign budget")
        if len(receipts) > attempts:
            raise CampaignError("paid result persisted but attempt ceiling was exceeded")
        ran += 1
    return status_campaign(campaign_dir=root)


def execute_campaign(
    *,
    campaign_dir: str | Path,
    runner: Callable[..., tuple[dict[str, Any], list[dict[str, Any]], str]] | None = None,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    max_packets: int | None = None,
) -> dict[str, Any]:
    root = Path(campaign_dir).resolve()
    with _exclusive_lease(root / ".campaign-state.lock"):
        return _execute_campaign_serialized(
            campaign_dir=root,
            runner=runner,
            opencode_binary=opencode_binary,
            max_packets=max_packets,
        )


resume_campaign = execute_campaign


def _import_result_serialized(
    *,
    campaign_dir: str | Path,
    locator: str,
    output: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Import an externally executed packet without permitting checkpoint reuse."""
    root = Path(campaign_dir).resolve()
    registry, plan, state = _load_campaign(root)
    row = next((item for item in plan["packets"] if "/".join((item["factor"], item["variant_id"], item["replicate_id"], item["fold_id"], item["episode_id"], item["packet_id"])) == locator), None)
    if row is None or row["phase"] != state["phase"]:
        raise CampaignError("result locator is unknown or outside the current phase")
    if state["phase"] != "train" and state.get("selected_variant_id") != row["variant_id"]:
        raise CampaignError("result does not belong to the selected survivor")
    attempts = int(registry["fixed_config"]["max_attempts_per_packet"])
    usage_tokens, receipt_elapsed = _validate_receipts(
        receipts,
        model=registry["fixed_config"]["model"],
        attempt_ceiling=attempts,
    )
    if any(bool(receipt["usage"].get("checkpoint_reuse")) for receipt in receipts):
        raise CampaignError("checkpoint reuse is forbidden")
    packet_path = Path(row["packet_path"])
    if true_north._sha256_file(packet_path) != row["packet_sha256"]:
        raise CampaignError("packet presentation hash mismatch")
    packet = _read(packet_path)
    if _sha(packet) != row["packet_semantic_sha256"]:
        raise CampaignError("packet semantic hash mismatch")
    _validate_surface_and_canonical(output, packet)
    normalized = normalize_output(output, packet)
    canonical_job = {**packet, "output_schema": true_north.atomic_output_schema([str(c["candidate_id"]) for c in packet["input"]["candidates"]])}
    true_north._validate_atomic_output(normalized, canonical_job)
    budget = registry["budget"]
    if (
        state["usage"]["calls"] + len(receipts) > budget["max_calls"]
        or state["usage"]["tokens"] + usage_tokens > budget["max_tokens"]
        or state["usage"]["wall_seconds"] + receipt_elapsed > budget["max_wall_seconds"]
    ):
        raise CampaignError("import receipt exceeds remaining campaign budget")
    record = {
        "locator": locator,
        "provider_model": registry["fixed_config"]["model"],
        "packet_sha256": row["packet_sha256"],
        "packet_semantic_sha256": row["packet_semantic_sha256"],
        "output": normalized,
        "output_sha256": _sha(normalized),
        "receipts": list(receipts),
    }
    _immutable_write(Path(row["output_dir"]) / "campaign-result.private.json", record)
    completed = set(state["completed_locators"])
    completed.add(locator)
    state["completed_locators"] = sorted(completed)
    state["usage"]["calls"] += len(receipts)
    state["usage"]["tokens"] += usage_tokens
    state["usage"]["wall_seconds"] += receipt_elapsed
    _atomic_state_write(root / "state.json", state)
    return record


def import_result(
    *,
    campaign_dir: str | Path,
    locator: str,
    output: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    root = Path(campaign_dir).resolve()
    with _exclusive_lease(root / ".campaign-state.lock"):
        return _import_result_serialized(
            campaign_dir=root,
            locator=locator,
            output=output,
            receipts=receipts,
        )


def score_campaign(
    *,
    campaign_dir: str | Path,
    scorer: Callable[..., Mapping[str, Any]] | None = None,
    gold_path: str | Path | None = None,
) -> dict[str, Any]:
    """Score current phase privately and publish only aggregate train residuals."""
    root = Path(campaign_dir).resolve()
    registry, plan, state = _load_campaign(root)
    phase_rows = [row for row in plan["packets"] if row["phase"] == state["phase"]]
    if state["phase"] != "train" and state.get("selected_variant_id"):
        phase_rows = [row for row in phase_rows if row["variant_id"] == state["selected_variant_id"]]
    missing = [row for row in phase_rows if not (Path(row["output_dir"]) / "campaign-result.private.json").is_file()]
    if missing:
        raise CampaignError(f"current phase has {len(missing)} incomplete packets")
    using_default_scorer = scorer is None
    if scorer is None:
        try:
            from .true_north_semantic_scoring import score_campaign as scorer
        except ImportError:
            scorer = None
    if scorer is None:
        raise CampaignError("semantic scorer is unavailable")
    if gold_path is None:
        gold_path = Path(registry["suite_manifest_path"]).parent / "gold" / "development" / "final" / "consensus.private.json"
    consensus_document = _read(Path(gold_path))
    consensus_items = list(consensus_document["items"])
    preferred_path = Path(gold_path).with_name("gold.private.json")
    preferred_items = list(_read(preferred_path)["items"]) if preferred_path.is_file() else []
    by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    speaker_maps_by_group: dict[
        tuple[str, str, str], dict[str, Any]
    ] = {}
    schema_counts: dict[tuple[str, str, str], dict[str, int]] = {}
    completed = set(state["completed_locators"])
    for row in phase_rows:
        locator = "/".join((
            row["factor"], row["variant_id"], row["replicate_id"],
            row["fold_id"], row["episode_id"], row["packet_id"],
        ))
        if locator not in completed:
            raise CampaignError("result is not recorded complete in campaign state")
        packet_path = Path(row["packet_path"])
        if (
            true_north._sha256_file(packet_path) != row["packet_sha256"]
            or _sha(_read(packet_path)) != row["packet_semantic_sha256"]
        ):
            raise CampaignError("score packet hash mismatch")
        result = _read(Path(row["output_dir"]) / "campaign-result.private.json")
        if (
            result.get("locator") != locator
            or result.get("packet_sha256") != row["packet_sha256"]
            or result.get("packet_semantic_sha256") != row["packet_semantic_sha256"]
            or result.get("provider_model") != registry["fixed_config"]["model"]
            or result.get("output_sha256") != _sha(result.get("output"))
        ):
            raise CampaignError("campaign result identity or hash mismatch")
        _validate_receipts(
            result.get("receipts", []),
            model=registry["fixed_config"]["model"],
            attempt_ceiling=int(registry["fixed_config"]["max_attempts_per_packet"]),
        )
        packet = _read(packet_path)
        group_key = (
            row["variant_id"], row["replicate_id"], row["episode_id"]
        )
        candidate_ids = [
            str(candidate["candidate_id"])
            for candidate in packet["input"]["candidates"]
        ]
        episode_context = packet["input"].get("episode_context", {})
        context_by_episode = {
            str(entry["episode_id"]): entry.get("context", {})
            for entry in packet["input"].get("episode_contexts", [])
            if isinstance(entry, Mapping) and entry.get("episode_id")
        }
        group_maps = speaker_maps_by_group.setdefault(group_key, {})
        for candidate in packet["input"]["candidates"]:
            source_episode_id = str(
                candidate.get("_source_episode_id")
                or packet["input"].get("episode", {}).get("episode_id")
                or ""
            )
            context = (
                context_by_episode.get(source_episode_id, {})
                if context_by_episode
                else episode_context
            )
            if isinstance(context, Mapping) and context.get("speaker_map"):
                group_maps[str(candidate["candidate_id"])] = context["speaker_map"]
        schema_counts.setdefault(group_key, {"valid": 0, "total": 0})
        schema_counts[group_key]["total"] += len(candidate_ids)
        if result.get("terminal_validation_failure"):
            by_group.setdefault(group_key, []).extend(
                {
                    "candidate_id": candidate_id,
                    "disposition": "invalid_output",
                    "value_state": "invalid_output",
                    "atomic_claims": [],
                }
                for candidate_id in candidate_ids
            )
            continue
        schema_counts[group_key]["valid"] += len(candidate_ids)
        canonical_output = copy.deepcopy(result["output"])
        true_north._validate_atomic_output(
            canonical_output,
            {
                **packet,
                "output_schema": true_north.atomic_output_schema([
                    str(candidate["candidate_id"])
                    for candidate in packet["input"]["candidates"]
                ]),
            },
        )
        by_group.setdefault(group_key, []).extend(result["output"]["items"])
    private: dict[str, Any] = {"phase": state["phase"], "variants": {}}
    public: dict[str, Any] = {"phase": state["phase"], "variants": {}}
    scored_groups: dict[str, list[dict[str, Any]]] = {}
    for (variant_id, replicate_id, episode_id), predictions in by_group.items():
        ids = {str(row["candidate_id"]) for row in predictions}
        consensus_subset = [row for row in consensus_items if str(row["candidate_id"]) in ids]
        preferred_subset = [row for row in preferred_items if str(row["candidate_id"]) in ids]
        if {str(row["candidate_id"]) for row in consensus_subset} != ids:
            raise CampaignError("predictions lack consensus contracts")
        scorer_kwargs = (
            {
                "speaker_maps_by_candidate": speaker_maps_by_group.get(
                    (variant_id, replicate_id, episode_id), {}
                )
            }
            if using_default_scorer
            else {}
        )
        score = dict(
            scorer(
                predictions,
                consensus_subset,
                preferred_subset,
                **scorer_kwargs,
            )
        )
        counts = schema_counts[(variant_id, replicate_id, episode_id)]
        score["aggregate"]["schema_parse_success_rate"] = round(
            counts["valid"] / counts["total"], 6
        )
        score["aggregate"]["terminal_validation_failure_rate"] = round(
            1.0 - score["aggregate"]["schema_parse_success_rate"], 6
        )
        if counts["valid"] == counts["total"]:
            core_metrics = true_north._consensus_atomic_metrics(
                consensus_document,
                {str(row["candidate_id"]): row for row in predictions},
                require_complete_scope=False,
            )
            score["aggregate"].update({
                str(row["metric"]): row["value"] for row in core_metrics
            })
        scored_groups.setdefault(variant_id, []).append({
            "replicate_id": replicate_id,
            "episode_id": episode_id,
            "score": score,
        })
    for variant_id, groups in scored_groups.items():
        aggregate_keys = sorted({
            key for group in groups for key, value in group["score"]["aggregate"].items()
            if isinstance(value, (int, float)) and value is not None
        })
        aggregate = {
            key: sum(float(group["score"]["aggregate"][key]) for group in groups if group["score"]["aggregate"].get(key) is not None)
            / sum(group["score"]["aggregate"].get(key) is not None for group in groups)
            for key in aggregate_keys
        }
        private["variants"][variant_id] = {"groups": groups, "aggregate": aggregate}
        public["variants"][variant_id] = {
            "aggregate_metrics": aggregate,
            "item_details_withheld": True,
            "gold_withheld": True,
        }
    gate_rules = {
        "consensus_candidate_state_macro_f1": (">=", 0.90),
        "retained_value_recall": (">=", 0.90),
        "consensus_junk_escape_rate": ("<=", 0.02),
        "acceptable_atomic_count_rate": (">=", 0.90),
        "claim_text_faithfulness_proxy": (">=", 0.90),
        "speaker_exactness": (">=", 0.97),
        "reported_actor_exactness": (">=", 0.95),
        "hallucination_rate_proxy": ("<=", 0.02),
        "schema_parse_success_rate": (">=", 0.99),
    }
    eligible: list[str] = []
    gate_records: dict[str, Any] = {}
    for variant_id, value in private["variants"].items():
        aggregate = value["aggregate"]
        exact_metrics_present = set(gate_rules) <= set(aggregate)
        checks = {
            metric: (
                aggregate[metric] >= threshold
                if comparison == ">="
                else aggregate[metric] <= threshold
            )
            for metric, (comparison, threshold) in gate_rules.items()
            if metric in aggregate
        }
        passed = exact_metrics_present and all(checks.values())
        gate_records[variant_id] = {
            "passed": passed,
            "checks": checks,
            "evaluated_gate_count": len(checks),
            "exact_required_metric_set_present": exact_metrics_present,
        }
        if passed:
            eligible.append(variant_id)

    pareto_directions = {
        "consensus_candidate_state_macro_f1": "maximize",
        "retained_value_recall": "maximize",
        "consensus_junk_escape_rate": "minimize",
        "acceptable_atomic_count_rate": "maximize",
        "claim_text_faithfulness_proxy": "maximize",
        "speaker_exactness": "maximize",
        "reported_actor_exactness": "maximize",
        "hallucination_rate_proxy": "minimize",
    }

    def dominates(left: str, right: str) -> bool:
        left_metrics = private["variants"][left]["aggregate"]
        right_metrics = private["variants"][right]["aggregate"]
        shared = sorted(pareto_directions)
        at_least = True
        strict = False
        for metric in shared:
            minimize = pareto_directions[metric] == "minimize"
            left_value = float(left_metrics[metric])
            right_value = float(right_metrics[metric])
            at_least &= left_value <= right_value if minimize else left_value >= right_value
            strict |= left_value < right_value if minimize else left_value > right_value
        return at_least and strict

    pareto = [
        variant_id for variant_id in eligible
        if not any(
            dominates(other, variant_id)
            for other in eligible
            if other != variant_id
        )
    ]
    private["selection"] = {
        "hard_gate_policy": gate_rules,
        "hard_gate_records": gate_records,
        "pareto_metric_directions": pareto_directions,
        "eligible_variant_ids": sorted(eligible),
        "pareto_variant_ids": sorted(pareto),
        "selection_is_score_bound": True,
    }
    public["selection_summary"] = {
        "eligible_variant_ids": sorted(eligible),
        "pareto_variant_ids": sorted(pareto),
    }
    private["score_sha256"] = _sha(private)
    public["score_sha256"] = _sha(public)
    _immutable_write(root / "scores" / f"{state['phase']}.private.json", private)
    if state["phase"] == "train":
        _immutable_write(root / "feedback" / "train.aggregate.json", public)
        return public
    return {
        "phase": state["phase"],
        "score_sha256": private["score_sha256"],
        "validation_details_withheld_from_agents": True,
    }


def _advance_campaign_serialized(*, campaign_dir: str | Path, selected_variant_id: str) -> dict[str, Any]:
    root = Path(campaign_dir).resolve()
    registry, plan, state = _load_campaign(root)
    if state.get("selected_variant_id") and state["selected_variant_id"] != selected_variant_id:
        raise CampaignError("survivor cannot be switched after selection")
    score_path = root / "scores" / f"{state['phase']}.private.json"
    if not score_path.is_file():
        raise CampaignError("current phase must be completed and scored before advancing")
    score = _read(score_path)
    if not _hash_field_matches(score, "score_sha256"):
        raise CampaignError("private score hash mismatch")
    selection = score.get("selection", {})
    if (
        selected_variant_id not in selection.get("eligible_variant_ids", [])
        or selected_variant_id not in selection.get("pareto_variant_ids", [])
    ):
        raise CampaignError("selected variant is not a score-gated Pareto survivor")
    next_phase = PHASES[PHASES.index(state["phase"]) + 1]
    state["selected_variant_id"] = selected_variant_id
    _atomic_state_write(root / "state.json", state)
    if next_phase in {"replication", "locked_validation", "final_confirmation"}:
        _materialize_phase(root, registry, next_phase)
    state["phase"] = next_phase
    state.setdefault("phase_history", []).append({"phase": next_phase, "selected_variant_id": selected_variant_id})
    _atomic_state_write(root / "state.json", state)
    return status_campaign(campaign_dir=root)


def advance_campaign(*, campaign_dir: str | Path, selected_variant_id: str) -> dict[str, Any]:
    root = Path(campaign_dir).resolve()
    with _exclusive_lease(root / ".campaign-state.lock"):
        return _advance_campaign_serialized(
            campaign_dir=root,
            selected_variant_id=selected_variant_id,
        )


def status_campaign(*, campaign_dir: str | Path) -> dict[str, Any]:
    root = Path(campaign_dir).resolve()
    registry, plan, state = _load_campaign(root)
    phase_rows = [row for row in plan["packets"] if row["phase"] == state["phase"]]
    if state["phase"] != "train" and state.get("selected_variant_id"):
        phase_rows = [row for row in phase_rows if row["variant_id"] == state["selected_variant_id"]]
    complete = sum((Path(row["output_dir"]) / "campaign-result.private.json").is_file() for row in phase_rows)
    return {
        "campaign_id": registry["campaign_id"],
        "factor": registry["factor"],
        "phase": state["phase"],
        "phase_packets_complete": complete,
        "phase_packet_count": len(phase_rows),
        "usage": state["usage"],
        "budget": registry["budget"],
        "production_database_opened": False,
        "holdout_opened": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    status = sub.add_parser("status")
    status.add_argument("campaign_dir")
    execute = sub.add_parser("execute")
    execute.add_argument("campaign_dir")
    execute.add_argument("--max-packets", type=int)
    score = sub.add_parser("score")
    score.add_argument("campaign_dir")
    args = parser.parse_args(argv)
    if args.command == "status":
        result = status_campaign(campaign_dir=args.campaign_dir)
    elif args.command == "execute":
        result = execute_campaign(campaign_dir=args.campaign_dir, max_packets=args.max_packets)
    else:
        result = score_campaign(campaign_dir=args.campaign_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
