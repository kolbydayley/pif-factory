from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from . import db
from .claim_canonicalizer import canonicalize_claims
from .claim_subjects import build_claim_subjects
from .headless_codex import execute_claimed_label_runs, execute_pending_reviewer_audits
from .identity_graph import groom_identity_graph
from .mcp_bridge import publish_broker_snapshot, publish_broker_status
from .observer import publish_snapshot, write_snapshot
from .paths import db_path, exports_dir
from .research_queue import run_worker, sync_queue_envelopes


@contextmanager
def pipeline_lock(*, wait: bool = False):
    lock_path = db_path().with_suffix(db_path().suffix + ".pipeline.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        flags = fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(lock_file.fileno(), flags)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def run_cycle(
    conn,
    *,
    cycle: str,
    model: str = "gpt-5.5",
    label_pack: str = "ai_discourse_v3_1",
    limit: int = 4,
    broker_url: str | None = None,
    broker_token: str | None = None,
    observer_url: str | None = None,
    observer_token: str | None = None,
    wait_for_lock: bool = False,
    initialize_db: bool = True,
) -> dict[str, Any]:
    if initialize_db:
        db.init_db(conn)
    with pipeline_lock(wait=wait_for_lock) as acquired:
        if not acquired:
            return {"ok": False, "skipped": True, "reason": "pipeline_lock_busy", "cycle": cycle}
        steps: list[dict[str, Any]] = []
        if cycle in {"bridge_fast", "bridge_sync"}:
            steps.append(
                {
                    "step": "sync_queue_envelopes",
                    **sync_queue_envelopes(conn, statuses=("pending", "claimed", "running"), job_types=("episode_context",)),
                }
            )
            if broker_url and broker_token:
                steps.append(
                    {
                        "step": "remote_import_disabled",
                        "ok": True,
                        "reason": "chatgpt_scheduled_extraction_abandoned_local_codex_only",
                    }
                )
                steps.append(
                    {
                        "step": "source_card_buffer_disabled",
                        "ok": True,
                        "reason": "chatgpt_scheduled_extraction_abandoned_local_codex_only",
                    }
                )
                steps.append({"step": "publish_mcp_broker_status", **publish_broker_status(conn, broker_url=broker_url, token=broker_token)})
            if cycle == "bridge_fast":
                return {"ok": True, "cycle": cycle, "steps": steps}
        if cycle in {"bridge_sync", "observer_publish"}:
            steps.append(
                {
                    "step": "identity_groom",
                    **groom_identity_graph(conn, model=model, limit=limit),
                }
            )
            steps.append(
                {
                    "step": "build_claim_subjects",
                    **build_claim_subjects(conn, scope="last_18_months", model=model),
                }
            )
            snapshot_path = write_snapshot(conn, str(exports_dir() / "observer-snapshot.json"))
            steps.append({"step": "snapshot", "path": str(snapshot_path)})
            if observer_url and observer_token:
                steps.append({"step": "publish_observer", **publish_snapshot(snapshot_path, url=observer_url, token=observer_token)})
            if broker_url and broker_token:
                steps.append({"step": "publish_mcp_broker", **publish_broker_snapshot(conn, broker_url=broker_url, token=broker_token)})
                steps.append({"step": "publish_mcp_broker_status", **publish_broker_status(conn, broker_url=broker_url, token=broker_token)})
            return {"ok": True, "cycle": cycle, "steps": steps}
        if cycle == "local_extractor":
            worker_id = "pif-local-extractor"
            steps.append(
                {
                    "step": "worker_extractor",
                    **run_worker(
                        conn,
                        role="extractor",
                        lane="podcast",
                        limit=limit,
                        model=model,
                        label_pack=label_pack,
                        worker_id=worker_id,
                    ),
                }
            )
            steps.append({"step": "headless_exec", **execute_claimed_label_runs(conn, lease_owner=worker_id, limit=limit, model=model)})
            return {"ok": True, "cycle": cycle, "steps": steps}
        if cycle == "local_reviewer":
            steps.append(
                {
                    "step": "headless_review",
                    **execute_pending_reviewer_audits(
                        conn,
                        patch_tag=None,
                        limit=limit,
                        model=model,
                        concurrency=min(2, max(1, limit)),
                    ),
                }
            )
            return {"ok": True, "cycle": cycle, "steps": steps}
        if cycle == "claim_judge":
            steps.append(
                {
                    "step": "canonicalize_claims",
                    **canonicalize_claims(conn, scope="last_18_months", model=model, limit=limit, mode="semantic"),
                }
            )
            steps.append(
                {
                    "step": "build_claim_subjects",
                    **build_claim_subjects(conn, scope="last_18_months", model=model, limit=limit),
                }
            )
            return {"ok": True, "cycle": cycle, "steps": steps}
        raise ValueError("--cycle must be bridge_fast, bridge_sync, observer_publish, local_extractor, local_reviewer, or claim_judge")
