from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from . import db
from .observer import publish_snapshot, write_snapshot
from .research_queue import queue_status, run_worker, sync_queue_envelopes
from .scale_gate import build_scale_gate_report
from .scale_ops import cluster_claims, judge_claim_edges
from .identity_graph import groom_identity_graph
from .util import now_iso


DEFAULT_OBSERVER_URL = "https://observer-ui-production.up.railway.app"
DEFAULT_PILOT_ID = "scale-gate-v31-2026-07-02"


def production_cycle(
    conn,
    *,
    pilot_id: str = DEFAULT_PILOT_ID,
    model: str = "gpt-5.5",
    label_pack: str = "ai_discourse_v3_1",
    worker_mode: str = "none",
    acquisition_limit: int = 4,
    extractor_limit: int = 4,
    reviewer_limit: int = 2,
    run_graph_jobs: bool = False,
    graph_limit: int = 100,
    snapshot_output: str | Path | None = None,
    publish: bool = False,
    observer_url: str = DEFAULT_OBSERVER_URL,
    token_file: str | Path | None = None,
) -> dict[str, Any]:
    db.init_db(conn)
    started_at = now_iso()
    steps: list[dict[str, Any]] = []

    steps.append({"step": "sync_queue_envelopes", **sync_queue_envelopes(conn)})
    steps.append({"step": "queue_status", **queue_status(conn, group_by=["lane", "content_type", "role", "status"])})

    if worker_mode not in {"none", "bounded"}:
        raise ValueError("--worker-mode must be none or bounded")
    if worker_mode == "bounded":
        steps.append(
            {
                "step": "worker_acquisition",
                **run_worker(
                    conn,
                    role="acquisition",
                    lane="podcast",
                    limit=acquisition_limit,
                    model=model,
                    label_pack=label_pack,
                    worker_id="prod-local-acquisition",
                    claim_prompts=False,
                ),
            }
        )
        steps.append(
            {
                "step": "worker_extractor",
                **run_worker(
                    conn,
                    role="extractor",
                    lane="podcast",
                    limit=extractor_limit,
                    model=model,
                    label_pack=label_pack,
                    worker_id="prod-local-extractor",
                    claim_prompts=True,
                ),
            }
        )
        steps.append(
            {
                "step": "worker_reviewer",
                **run_worker(
                    conn,
                    role="reviewer",
                    lane="quality",
                    limit=reviewer_limit,
                    model=model,
                    label_pack=label_pack,
                    worker_id="prod-local-reviewer",
                    claim_prompts=False,
                ),
            }
        )

    if run_graph_jobs:
        steps.append({"step": "groom_identities", **groom_identity_graph(conn, model=model, pilot_id=pilot_id, limit=graph_limit)})
        steps.append({"step": "cluster_claims", **cluster_claims(conn, pilot_id=pilot_id, model=model, limit=graph_limit)})
        steps.append({"step": "judge_claim_edges", **judge_claim_edges(conn, pilot_id=pilot_id, model=model, limit=graph_limit)})

    scale_report = build_scale_gate_report(conn, pilot_id=pilot_id)
    steps.append({"step": "scale_batch_report", "gate_state": scale_report.get("gate_state"), "failed_checks": scale_report.get("failed_checks", [])})

    snapshot_path = write_snapshot(conn, str(snapshot_output) if snapshot_output else None)
    steps.append({"step": "snapshot", "path": str(snapshot_path)})

    privacy = privacy_scan_path(snapshot_path.parent)
    steps.append({"step": "privacy_scan", **privacy})
    if not privacy["ok"]:
        return {
            "ok": False,
            "started_at": started_at,
            "completed_at": now_iso(),
            "mode": worker_mode,
            "published": False,
            "error": "privacy_scan_failed",
            "steps": steps,
        }

    published = False
    if publish:
        token = None
        if token_file:
            token = Path(token_file).expanduser().read_text(encoding="utf-8").strip()
        publish_result = publish_snapshot(str(snapshot_path), url=observer_url, token=token)
        steps.append({"step": "publish_snapshot", **publish_result})
        published = int(publish_result.get("status", 0)) == 200

    return {
        "ok": True,
        "started_at": started_at,
        "completed_at": now_iso(),
        "mode": worker_mode,
        "published": published,
        "local_source_of_truth": True,
        "railway_compute_enabled": False,
        "mcp_remote_worker_enabled": False,
        "steps": steps,
    }


def railway_cost_guard(
    *,
    status_json: dict[str, Any] | None = None,
    status_json_path: str | Path | None = None,
    project_name: str = "podcast-intelligence-observer",
    service_name: str = "observer-ui",
    max_replicas: int = 1,
) -> dict[str, Any]:
    status = status_json or _load_railway_status(status_json_path)
    issues: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    if status.get("name") != project_name:
        issues.append({"severity": "critical", "message": f"Wrong Railway project: {status.get('name')!r}"})
    if _edge_count(status.get("buckets")):
        issues.append({"severity": "critical", "message": "Railway buckets are present; observer should not use object storage unless explicitly approved."})

    services = _service_nodes(status)
    service_names = sorted({item.get("serviceName") or item.get("name") for item in services if item})
    unexpected_services = [name for name in service_names if name != service_name]
    if unexpected_services:
        issues.append({"severity": "critical", "message": "Unexpected Railway services: " + ", ".join(unexpected_services)})
    if service_name not in service_names:
        issues.append({"severity": "critical", "message": f"Expected Railway service {service_name!r} was not found."})

    for service in services:
        current_name = service.get("serviceName") or service.get("name")
        if service.get("nextCronRunAt"):
            issues.append({"severity": "critical", "message": f"{current_name} has a cron schedule."})
        if _edge_count(service.get("volumeInstances")):
            issues.append({"severity": "critical", "message": f"{current_name} has Railway volume instances."})
        latest = service.get("latestDeployment") or {}
        meta = latest.get("meta") or {}
        manifests = [meta.get("serviceManifest") or {}, meta.get("fileServiceManifest") or {}]
        for manifest in manifests:
            deploy = manifest.get("deploy") or {}
            if deploy.get("cronSchedule"):
                issues.append({"severity": "critical", "message": f"{current_name} deploy manifest has cronSchedule."})
            replicas = _replica_count(deploy)
            if replicas > max_replicas:
                issues.append({"severity": "critical", "message": f"{current_name} uses {replicas} replicas; max allowed is {max_replicas}."})
            start_command = str(deploy.get("startCommand") or "")
            if start_command and start_command != "python3 -m research_factory.ui_server":
                issues.append({"severity": "critical", "message": f"{current_name} start command is not observer-only: {start_command}"})
            if deploy.get("preDeployCommand"):
                issues.append({"severity": "critical", "message": f"{current_name} has a preDeployCommand."})
            if deploy.get("sleepApplication") is False:
                warnings.append({"severity": "info", "message": f"{current_name} sleepApplication is false; acceptable only while live observer availability is preferred over minimum idle cost."})
        if meta.get("volumeMounts"):
            issues.append({"severity": "critical", "message": f"{current_name} deployment has volume mounts."})

    return {
        "ok": not issues,
        "project": status.get("name"),
        "expected_project": project_name,
        "services": service_names,
        "issues": issues,
        "warnings": warnings,
        "policy": {
            "railway_role": "sanitized_observer_storage_only",
            "compute_allowed": False,
            "max_replicas": max_replicas,
            "allowed_service": service_name,
        },
    }


def privacy_scan_path(path: Path) -> dict[str, object]:
    import re

    suspicious = []
    patterns = [
        ("absolute_local_path", re.compile(r"/Users/[^\\s\"']+")),
        ("bearer_token", re.compile(r"Bearer\\s+[A-Za-z0-9._~+/-]{12,}", re.I)),
        ("api_key_like", re.compile(r"(?i)\b(?:api[_-]?key|token|secret)\b\s*[\"':=]+\s*[\"']?[A-Za-z0-9._~+/-]{16,}")),
        ("openai_key_like", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
        ("raw_transcript_marker", re.compile(r"BEGIN RAW TRANSCRIPT")),
    ]
    for item in path.expanduser().resolve().rglob("*"):
        if not item.is_file() or item.suffix.lower() not in {".md", ".json", ".html", ".txt"}:
            continue
        text = item.read_text(encoding="utf-8", errors="ignore")
        reasons = []
        if len(text.split()) > 5000 and ("raw_text" in item.name or "transcript" in item.name):
            reasons.append("long_raw_text_like_file")
        for name, pattern in patterns:
            if pattern.search(text):
                reasons.append(name)
        if _has_long_copied_excerpt(text):
            reasons.append("long_copied_excerpt_like_text")
        if reasons:
            suspicious.append({"path": str(item), "reasons": sorted(set(reasons))})
    return {"ok": not suspicious, "suspicious": suspicious}


def _load_railway_status(status_json_path: str | Path | None) -> dict[str, Any]:
    if status_json_path:
        return json.loads(Path(status_json_path).expanduser().read_text(encoding="utf-8"))
    completed = subprocess.run(["railway", "status", "--json"], capture_output=True, text=True, timeout=45, check=True)
    return json.loads(completed.stdout)


def _service_nodes(status: dict[str, Any]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for edge in ((status.get("services") or {}).get("edges") or []):
        node = edge.get("node")
        if isinstance(node, dict):
            nodes.append(node)
    for env_edge in ((status.get("environments") or {}).get("edges") or []):
        env = env_edge.get("node") or {}
        for service_edge in ((env.get("serviceInstances") or {}).get("edges") or []):
            node = service_edge.get("node")
            if isinstance(node, dict):
                nodes.append(node)
    return nodes


def _edge_count(value: Any) -> int:
    if not isinstance(value, dict):
        return 0
    edges = value.get("edges")
    return len(edges) if isinstance(edges, list) else 0


def _replica_count(deploy: dict[str, Any]) -> int:
    if not isinstance(deploy, dict):
        return 0
    replicas = int(deploy.get("numReplicas") or 0)
    multi = deploy.get("multiRegionConfig") or {}
    if isinstance(multi, dict):
        replicas = max(replicas, sum(int((region or {}).get("numReplicas") or 0) for region in multi.values()))
    return replicas


def _has_long_copied_excerpt(text: str) -> bool:
    compact_lines = [line.strip() for line in text.splitlines() if line.strip()]
    return any(len(line.split()) >= 120 and not line.lstrip().startswith(("{", "[", "#", "-", "|")) for line in compact_lines)
