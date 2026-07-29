"""Small, isolated command line surface for Research Radar v1.

This module deliberately does not import the historical PIF CLI.  Its commands
operate only on the new Research Radar database and frozen contract bundle.  No
command starts a scheduler, invokes a model transport, or reads a legacy queue.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence
from urllib.parse import urlsplit


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "research_radar"
CONFIG_FILES = (
    "product_contract_v1.json",
    "schema_contract_v1.json",
    "budget_policy_v1.json",
    "benchmark_v1.json",
    "workspace_ai_technology_v1.json",
    "demo_five_items_v1.json",
)
COMMANDS = (
    "init",
    "seed-demo",
    "status",
    "run-cycle",
    "serve",
    "validate-contract",
)
EXPECTED_PIPELINE = [
    "discover",
    "fetch",
    "normalize",
    "extract_evidence",
    "reconcile_document",
    "reconcile_graph",
    "score_significance",
    "publish_provisional",
    "verify_amend_retract",
]
EXPECTED_NODE_TYPES = [
    "workspace",
    "source",
    "content_item",
    "evidence_span",
    "entity",
    "topic",
    "development",
    "claim",
    "position_observation",
    "briefing",
]
EXPECTED_EDGE_FIELDS = [
    "evidence_span_id",
    "observed_at",
    "confidence",
    "release_status",
    "extractor_version",
    "release_id",
]
EXPECTED_RELEASE_STATUSES = [
    "candidate",
    "provisional",
    "verified",
    "amended",
    "retracted",
    "dismissed",
]


class ContractError(ValueError):
    """Raised when the frozen Research Radar contract bundle is inconsistent."""


def fallback_radar_db_path() -> Path:
    """Return a local default that is outside Documents/FileProvider storage."""

    return (
        Path.home()
        / "Library"
        / "Application Support"
        / "Research Radar"
        / "research-radar.sqlite3"
    )


def default_db_path() -> Path:
    """Resolve the core service default without importing legacy PIF modules."""

    try:
        from .radar import default_radar_db_path
    except ImportError:
        return fallback_radar_db_path()
    return Path(default_radar_db_path()).expanduser()


def _db_path(value: str | None) -> Path:
    selected = Path(value).expanduser() if value else default_db_path()
    return selected.resolve()


def _service(database: Path) -> Any:
    from .radar import ResearchRadar

    return ResearchRadar(database)


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContractError(f"missing contract artifact: {path.name}") from exc
    except json.JSONDecodeError as exc:
        raise ContractError(f"invalid JSON in {path.name}: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"{path.name} must contain a JSON object")
    return value


def load_contract_bundle(config_dir: Path = CONFIG_DIR) -> Dict[str, Dict[str, Any]]:
    """Load the complete, versioned contract bundle from one directory."""

    root = Path(config_dir)
    return {name: _read_json(root / name) for name in CONFIG_FILES}


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _ids_for_item(item: Mapping[str, Any]) -> Dict[str, str]:
    fields = {
        "source": "source_id",
        "content": "content_item_id",
        "evidence": "evidence_span_id",
        "entity": "entity_id",
        "development": "development_id",
        "claim": "claim_id",
        "position": "position_observation_id",
        "brief": "briefing_id",
    }
    result: Dict[str, str] = {}
    for section, field in fields.items():
        value = item.get(section)
        _expect(isinstance(value, dict), f"demo item is missing {section}")
        identifier = value.get(field)
        _expect(isinstance(identifier, str) and bool(identifier), f"demo {section} is missing {field}")
        result[field] = identifier
    return result


def _all_mapping_keys(value: Any) -> list[str]:
    keys: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            keys.append(str(key))
            keys.extend(_all_mapping_keys(nested))
    elif isinstance(value, list):
        for nested in value:
            keys.extend(_all_mapping_keys(nested))
    return keys


def validate_contract_bundle(config_dir: Path = CONFIG_DIR) -> Dict[str, Any]:
    """Validate invariants shared by product, schema, budgets, and demo data."""

    bundle = load_contract_bundle(config_dir)
    product = bundle["product_contract_v1.json"]
    schema = bundle["schema_contract_v1.json"]
    budget = bundle["budget_policy_v1.json"]
    benchmark = bundle["benchmark_v1.json"]
    workspace = bundle["workspace_ai_technology_v1.json"]
    fixture = bundle["demo_five_items_v1.json"]

    for filename, document in bundle.items():
        _expect(document.get("schema_version") == "1", f"{filename} must use schema_version 1")

    _expect(product.get("status") == "frozen_pilot", "product contract must remain frozen_pilot")
    _expect(
        product.get("audience") == "private_local_single_user",
        "product audience must remain private and local",
    )
    _expect(product.get("pipeline") == EXPECTED_PIPELINE, "product pipeline order changed")
    _expect(product.get("semantic_authority") == "managed_auth_llm_only", "semantic authority changed")
    _expect(product.get("embeddings_allowed") is False, "embeddings must remain disabled")
    _expect(
        product.get("deterministic_semantic_matching_allowed") is False,
        "deterministic semantic matching must remain disabled",
    )
    _expect(product.get("scheduler_enabled") is False, "scheduler must remain disabled during pilot")
    _expect(product.get("legacy_queue_access") == "forbidden", "legacy queue access must remain forbidden")

    _expect(schema.get("status") == "frozen_pilot", "schema contract must remain frozen_pilot")
    _expect(schema.get("node_types") == EXPECTED_NODE_TYPES, "bounded node type list changed")
    _expect(
        schema.get("release_statuses") == EXPECTED_RELEASE_STATUSES,
        "release status lifecycle changed",
    )
    _expect(
        schema.get("semantic_edge_required_fields") == EXPECTED_EDGE_FIELDS,
        "semantic edge provenance fields changed",
    )
    _expect(
        schema.get("source_trust_tiers") == product.get("source_trust_tiers"),
        "source trust tiers differ between contracts",
    )
    extension = schema.get("ai_technology_extension")
    _expect(isinstance(extension, dict), "AI and technology extension is missing")
    _expect(extension.get("model_may_invent_relation_types") is False, "models may not invent relation types")
    authority = schema.get("authority_rules")
    _expect(isinstance(authority, dict), "schema authority rules are missing")
    _expect(authority.get("provisional_overlay_separate") is True, "provisional data must remain separate")
    _expect(authority.get("legacy_pending_queue_import") is False, "legacy pending queue import is forbidden")

    item_budget = budget.get("item")
    cycle_budget = budget.get("cycle")
    source_budget = budget.get("sources")
    _expect(isinstance(item_budget, dict), "item budget is missing")
    _expect(isinstance(cycle_budget, dict), "cycle budget is missing")
    _expect(isinstance(source_budget, dict), "source budget is missing")
    _expect(item_budget.get("max_input_tokens") == 75000, "item token cap must be 75000")
    _expect(item_budget.get("max_wall_seconds") == 900, "item wall cap must be 900 seconds")
    _expect(item_budget.get("max_coherent_windows") == 4, "item window cap must be four")
    _expect(cycle_budget.get("max_new_items") == 25, "cycle item cap must be 25")
    _expect(cycle_budget.get("max_wall_seconds") == 7200, "cycle wall cap must be two hours")
    _expect(cycle_budget.get("scheduler_enabled") is False, "automatic scheduler must remain disabled")
    _expect(source_budget.get("max_active_during_pilot") == 50, "pilot source cap must be 50")
    _expect(source_budget.get("max_promotions_per_day") == 3, "daily source promotion cap must be three")

    splits = benchmark.get("splits")
    format_counts = benchmark.get("format_counts")
    _expect(isinstance(splits, dict), "benchmark splits are missing")
    _expect(isinstance(format_counts, dict), "benchmark format counts are missing")
    _expect(benchmark.get("total_items") == 30, "benchmark must contain 30 items")
    _expect(sum(int(value) for value in splits.values()) == 30, "benchmark split counts must total 30")
    _expect(splits.get("development") == 20 and splits.get("holdout") == 10, "benchmark must use a 20/10 split")
    _expect(sum(int(value) for value in format_counts.values()) == 30, "benchmark format counts must total 30")
    _expect(set(format_counts.values()) == {6}, "benchmark must contain six items in each format group")

    _expect(workspace.get("workspace_id") == product.get("first_workspace_id"), "workspace ID differs from product contract")
    questions = workspace.get("strategic_questions")
    _expect(isinstance(questions, list) and len(questions) == 4, "workspace must retain four strategic questions")
    discovery = workspace.get("source_discovery")
    _expect(isinstance(discovery, dict), "workspace source discovery policy is missing")
    _expect(
        discovery.get("active_source_cap") == source_budget.get("max_active_during_pilot"),
        "workspace and budget source caps differ",
    )

    _expect(
        fixture.get("fixture_kind") == "synthetic_public_safe_paraphrases",
        "demo fixture must remain synthetic and public-safe",
    )
    _expect(
        not any("transcript" in key.lower() for key in _all_mapping_keys(fixture)),
        "demo fixture may not contain transcript-shaped fields",
    )
    fixture_workspace = fixture.get("workspace")
    _expect(isinstance(fixture_workspace, dict), "demo workspace is missing")
    _expect(fixture_workspace.get("workspace_id") == workspace.get("workspace_id"), "demo workspace ID differs")
    items = fixture.get("items")
    _expect(isinstance(items, list) and len(items) == 5, "demo fixture must contain exactly five items")

    allowed_formats = set(product.get("allowed_source_formats") or [])
    allowed_tiers = set(product.get("source_trust_tiers") or [])
    allowed_statement_kinds = set(product.get("statement_kinds") or [])
    all_ids: set[str] = set()
    for index, raw_item in enumerate(items, start=1):
        _expect(isinstance(raw_item, dict), f"demo item {index} must be an object")
        ids = _ids_for_item(raw_item)
        for identifier in ids.values():
            _expect(identifier not in all_ids, f"duplicate demo identifier: {identifier}")
            all_ids.add(identifier)

        source = raw_item["source"]
        content = raw_item["content"]
        evidence = raw_item["evidence"]
        development = raw_item["development"]
        claim = raw_item["claim"]
        position = raw_item["position"]
        brief = raw_item["brief"]

        _expect(source.get("source_type") in allowed_formats, f"demo item {index} uses a forbidden source format")
        _expect(source.get("trust_tier") in allowed_tiers, f"demo item {index} uses an unknown trust tier")
        _expect("status" not in source, f"demo item {index} may not override the probation source default")
        parsed_url = urlsplit(str(source.get("canonical_url") or ""))
        _expect(
            parsed_url.scheme == "https" and parsed_url.hostname is not None and parsed_url.hostname.endswith(".example.invalid"),
            f"demo item {index} must use a reserved synthetic source URL",
        )
        _expect(content.get("source_id") == ids["source_id"], f"demo item {index} source reference is broken")
        normalized = content.get("normalized_text")
        _expect(isinstance(normalized, str) and len(normalized) <= 240, f"demo item {index} normalized text is not short")
        _expect(evidence.get("content_item_id") == ids["content_item_id"], f"demo item {index} evidence content reference is broken")
        start = evidence.get("start_char")
        end = evidence.get("end_char")
        _expect(isinstance(start, int) and isinstance(end, int), f"demo item {index} evidence offsets must be integers")
        _expect(0 <= start < end <= len(normalized), f"demo item {index} evidence offsets are out of range")
        _expect(bool(normalized[start:end].strip()), f"demo item {index} evidence does not resolve exactly")
        _expect(evidence.get("status") == "provisional", f"demo item {index} evidence must be provisional")
        _expect(development.get("status") == "provisional", f"demo item {index} development must be provisional")
        _expect(claim.get("claim_type") in allowed_statement_kinds, f"demo item {index} has an unknown statement kind")
        _expect(claim.get("status") == "provisional", f"demo item {index} claim must be provisional")
        _expect(position.get("claim_id") == ids["claim_id"], f"demo item {index} position claim reference is broken")
        _expect(position.get("entity_id") == ids["entity_id"], f"demo item {index} position entity reference is broken")
        _expect(position.get("evidence_span_id") == ids["evidence_span_id"], f"demo item {index} position evidence reference is broken")
        for field in EXPECTED_EDGE_FIELDS:
            fixture_field = "status" if field == "release_status" else field
            _expect(position.get(fixture_field) is not None, f"demo item {index} position is missing {fixture_field}")
        _expect(brief.get("status") == "provisional", f"demo item {index} brief must be provisional")
        _expect(brief.get("development_id") == ids["development_id"], f"demo item {index} brief development reference is broken")
        _expect(brief.get("evidence_span_id") == ids["evidence_span_id"], f"demo item {index} brief lacks resolving evidence")

    canonical = json.dumps(bundle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return {
        "ok": True,
        "contract_version": "1",
        "bundle_sha256": digest,
        "artifacts": list(CONFIG_FILES),
        "demo_items": len(items),
        "scheduler_enabled": False,
        "legacy_queue_access": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="research-radar",
        description="Private, local Research Radar v1 control surface.",
    )
    parser.add_argument(
        "--db",
        help="Research Radar SQLite path (default: local Application Support, outside Documents).",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init", help="Initialize only the isolated Research Radar database.")

    seed = subparsers.add_parser("seed-demo", help="Seed five deterministic synthetic briefings.")
    seed.add_argument(
        "--fixture",
        type=Path,
        default=CONFIG_DIR / "demo_five_items_v1.json",
        help="Synthetic demo fixture path.",
    )

    subparsers.add_parser("status", help="Report isolated database and pilot-gate status.")

    cycle = subparsers.add_parser(
        "run-cycle",
        help="Manually run one bounded deterministic queue cycle; never starts a scheduler or model.",
    )
    cycle.add_argument("--max-items", type=int, default=25)

    serve = subparsers.add_parser("serve", help="Serve the private dashboard on loopback only.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8767)

    validate = subparsers.add_parser("validate-contract", help="Validate frozen v1 contracts and demo evidence.")
    validate.add_argument("--config-dir", type=Path, default=CONFIG_DIR)
    return parser


def _print(payload: Mapping[str, Any], *, stream: Any = None) -> None:
    target = sys.stdout if stream is None else stream
    print(json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True, default=str), file=target)


def _result_payload(command: str, database: Path, result: Any = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": True,
        "command": command,
        "db": str(database),
        "scheduler_enabled": False,
        "legacy_queue_access": False,
    }
    if result is not None:
        if isinstance(result, Mapping):
            payload["result"] = dict(result)
        else:
            payload["result"] = result
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.command:
        parser.print_help()
        return 0

    try:
        if args.command == "validate-contract":
            _print(validate_contract_bundle(args.config_dir))
            return 0

        database = _db_path(args.db)
        if args.command == "init":
            validation = validate_contract_bundle()
            with _service(database) as service:
                result = service.init()
            payload = _result_payload("init", database, result)
            payload["contract_sha256"] = validation["bundle_sha256"]
            _print(payload)
            return 0

        if args.command == "seed-demo":
            validation = validate_contract_bundle()
            fixture = _read_json(Path(args.fixture))
            _expect(fixture.get("fixture_id") == "research-radar-five-item-demo-v1", "unrecognized demo fixture")
            canonical_fixture = _read_json(CONFIG_DIR / "demo_five_items_v1.json")
            _expect(fixture == canonical_fixture, "demo fixture differs from the frozen public-safe fixture")
            with _service(database) as service:
                service.init()
                result = service.seed_demo(fixture)
            payload = _result_payload("seed-demo", database, result)
            payload["contract_sha256"] = validation["bundle_sha256"]
            payload["fixture_id"] = fixture["fixture_id"]
            _print(payload)
            return 0

        if args.command == "status":
            if not database.is_file():
                _print(
                    {
                        "ok": True,
                        "command": "status",
                        "db": str(database),
                        "initialized": False,
                        "scheduler_enabled": False,
                        "legacy_queue_access": False,
                        "next_action": "research-radar init",
                    }
                )
                return 0
            with _service(database) as service:
                result = service.status()
            _print(_result_payload("status", database, result))
            return 0

        if args.command == "run-cycle":
            cycle_cap = int(_read_json(CONFIG_DIR / "budget_policy_v1.json")["cycle"]["max_new_items"])
            _expect(1 <= args.max_items <= cycle_cap, f"max-items must be between 1 and {cycle_cap}")
            _expect(database.is_file(), "Research Radar database is not initialized")
            with _service(database) as service:
                result = service.run_cycle(max_items=args.max_items)
            payload = _result_payload("run-cycle", database, result)
            payload["manual"] = True
            payload["model_transport_started"] = False
            _print(payload)
            return 0

        if args.command == "serve":
            _expect(database.is_file(), "Research Radar database is not initialized")
            from . import radar_ui

            return int(
                radar_ui.main(
                    [
                        "--db",
                        str(database),
                        "--host",
                        str(args.host),
                        "--port",
                        str(args.port),
                    ]
                )
            )
    except (ContractError, FileNotFoundError, ImportError, RuntimeError) as exc:
        _print(
            {
                "ok": False,
                "command": args.command,
                "error": type(exc).__name__,
                "message": str(exc),
                "scheduler_enabled": False,
                "legacy_queue_access": False,
            },
            stream=sys.stderr,
        )
        return 2

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
