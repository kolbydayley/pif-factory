from __future__ import annotations

import json
import os
import datetime as dt
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


STATE_DIR = Path(os.environ.get("RAILWAY_UI_STATE_DIR", "ui_state")).resolve()
SNAPSHOT_PATH = STATE_DIR / "factory-state.json"
EXPORT_SNAPSHOT_PATH = Path(os.environ.get("RAILWAY_UI_EXPORT_SNAPSHOT_PATH", "exports/observer-snapshot.json")).resolve()
INGEST_TOKEN = os.environ.get("RAILWAY_UI_INGEST_TOKEN", "")
MAX_SNAPSHOT_AGE_SECONDS = int(os.environ.get("RAILWAY_UI_MAX_SNAPSHOT_AGE_SECONDS", "7200"))


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Research Intelligence Factory</title>
  <style>
    :root { color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif; }
    body { margin: 0; background: #0f1418; color: #edf2f4; }
    header { padding: 22px 28px; border-bottom: 1px solid #26323a; background: #151c21; }
    h1 { margin: 0 0 4px; font-size: 22px; letter-spacing: 0; }
    main { padding: 22px 28px 42px; display: grid; gap: 18px; }
    section { border: 1px solid #26323a; border-radius: 6px; background: #151c21; padding: 16px; }
    h2 { margin: 0 0 12px; font-size: 16px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
    .metric { border: 1px solid #2d3a43; border-radius: 6px; padding: 12px; background: #10161a; }
    .metric b { display: block; font-size: 24px; margin-top: 4px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; border-bottom: 1px solid #26323a; padding: 8px; vertical-align: top; }
    th { color: #a9bbc7; font-weight: 600; }
    .flag { border-left: 4px solid #f3b44e; padding: 8px 10px; background: #241d12; margin: 6px 0; }
    .flag.critical { border-left-color: #ff6b6b; background: #2a1717; }
    .muted { color: #a9bbc7; }
    a { color: #8cc8ff; }
  </style>
</head>
<body>
  <header>
    <h1>Research Intelligence Factory</h1>
    <div class="muted" id="generated">Loading snapshot...</div>
  </header>
  <main>
    <section><h2>Intervention Flags</h2><div id="flags"></div></section>
    <section><h2>Corpus Counts</h2><div class="grid" id="counts"></div></section>
    <section><h2>Research Queue</h2><div class="grid" id="researchQueue"></div><table id="queueRoles"></table><table id="workerRuns"></table></section>
    <section><h2>Content Coverage</h2><div class="grid" id="contentCoverage"></div><table id="contentTypes"></table><table id="artifactTypes"></table></section>
    <section><h2>Dense Coding Metrics</h2><div class="grid" id="coding"></div></section>
    <section><h2>Useful Signals</h2><div class="grid" id="signals"></div><table id="signalTypes"></table><table id="burstTerms"></table></section>
    <section><h2>Derived Tables</h2><div class="grid" id="derived"></div></section>
    <section><h2>Transcript Acquisition</h2><div class="grid" id="acquisition"></div><table id="acquisitionAttempts"></table><table id="transcriptionRuns"></table></section>
    <section><h2>Transcript Preparation</h2><table id="preparation"></table></section>
    <section><h2>Source Yield</h2><table id="sourceYield"></table></section>
    <section><h2>Queue By Type</h2><table id="queue"></table></section>
    <section><h2>Active And Failed Jobs</h2><table id="jobs"></table></section>
    <section><h2>Recent Label Runs</h2><table id="runs"></table></section>
    <section><h2>Artifacts</h2><table id="artifacts"></table></section>
  </main>
  <script>
    const esc = (value) => String(value ?? "").replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
    const cell = (value) => `<td>${esc(value)}</td>`;
    const table = (el, headers, rows) => {
      el.innerHTML = `<tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr>` +
        rows.map(r => `<tr>${r.map(cell).join("")}</tr>`).join("");
    };
    fetch('/state.json', {cache: 'no-store'}).then(r => r.json()).then(data => {
      const health = data.snapshot_health || {};
      const ageText = health.snapshot_age_seconds != null ? ` · age ${health.snapshot_age_seconds}s` : '';
      document.getElementById('generated').textContent = `${data.generated_at || 'No snapshot'} · ${data.privacy || ''}${ageText}`;
      document.getElementById('counts').innerHTML = Object.entries(data.counts || {}).map(([k,v]) =>
        `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      const researchQueue = data.research_queue_metrics || {};
      document.getElementById('researchQueue').innerHTML = [
        ['mode', researchQueue.project_mode || 'research_intelligence_factory'],
        ['local source of truth', researchQueue.local_source_of_truth ? 'yes' : 'unknown'],
        ['headless Codex workers', researchQueue.headless_codex_workers_enabled ? 'enabled' : 'disabled'],
        ['MCP remote workers', researchQueue.mcp_remote_worker_enabled ? 'enabled' : 'disabled'],
        ['remote contract', researchQueue.remote_worker_contract_ready ? 'ready-disabled' : 'not ready'],
        ['remote claimable', Object.entries(researchQueue.remote_claimable_by_privacy_tier || {}).map(([k,v]) => `${k}:${v}`).join(' · ') || '0']
      ].map(([k,v]) => `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      table(document.getElementById('queueRoles'), ['Role', 'Content Type', 'Status', 'Count'],
        (researchQueue.depth_by_role || []).map(x => [x.worker_role, x.content_type, x.status, x.count]));
      table(document.getElementById('workerRuns'), ['Role', 'Status', 'Runs', 'Claimed', 'Completed', 'Failed'],
        (researchQueue.worker_runs || []).map(x => [x.worker_role, x.status, x.count, x.claimed_jobs, x.completed_jobs, x.failed_jobs]));
      const content = data.content_metrics || {};
      document.getElementById('contentCoverage').innerHTML = [
        ['supported types', (content.supported_content_types || []).length],
        ['sources', (content.source_types || []).reduce((a,x) => a + Number(x.count || 0), 0)],
        ['items', (content.content_types || []).reduce((a,x) => a + Number(x.count || 0), 0)],
        ['artifacts', (content.artifact_types || []).reduce((a,x) => a + Number(x.count || 0), 0)]
      ].map(([k,v]) => `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      table(document.getElementById('contentTypes'), ['Content Type', 'Acquisition', 'Privacy', 'Count'],
        (content.content_types || []).map(x => [x.content_type, x.acquisition_status, x.privacy_tier, x.count]));
      table(document.getElementById('artifactTypes'), ['Artifact Type', 'Source Kind', 'Status', 'Count'],
        (content.artifact_types || []).map(x => [x.artifact_type, x.source_kind, x.status, x.count]));
      const coding = data.coding_metrics || {};
      document.getElementById('coding').innerHTML = [
        ['v2 labels', coding.v2_labels || 0],
        ['v2 observations', coding.v2_observations || 0],
        ['observations / 1k words', coding.v2_observations_per_1000_segment_words || 0],
        ['v3 labels', coding.v3_labels || 0],
        ['v3 discourse events', coding.v3_discourse_events || 0],
        ['v3 events / 1k words', coding.v3_events_per_1000_segment_words || 0],
        ['v3.1 labels', coding.v3_1_labels || 0],
        ['v3.1 discourse events', coding.v3_1_discourse_events || 0],
        ['v3.1 events / 1k words', coding.v3_1_events_per_1000_segment_words || 0],
        ['v3.1 full episode context', coding.v3_1_full_episode_context_required ? 'required' : 'unknown'],
        ['v3.1 local draft', coding.v3_1_local_draft_disabled ? 'disabled' : 'enabled'],
        ['label packs', (coding.labels_by_pack || []).map(x => `${x.label_pack}:${x.count}`).join(' · ') || 'none']
      ].map(([k,v]) => `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      const signals = data.useful_signal_metrics || {};
      document.getElementById('signals').innerHTML = [
        ['candidate concepts', Object.values(signals.candidate_concepts || {}).reduce((a,b) => a + Number(b || 0), 0)],
        ['needs adjudication', (signals.candidate_concepts || {}).needs_adjudication || 0],
        ['promoted aliases', (signals.concept_aliases || {}).active || 0],
        ['shift alerts', (signals.shift_signals_by_type || []).reduce((a,x) => a + Number(x.count || 0), 0)],
        ['actor stance changes', signals.actor_stance_changes || 0],
        ['v3 audit avg', signals.v3_avg_audit_score || 0],
        ['v3.1 audit avg', signals.v3_1_avg_audit_score || 0],
        ['v3.1 scale state', signals.v3_1_scale_state || 'unknown']
      ].map(([k,v]) => `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      table(document.getElementById('signalTypes'), ['Signal Type', 'Count', 'Avg Score'],
        (signals.shift_signals_by_type || []).map(x => [x.signal_type, x.count, x.avg_score]));
      table(document.getElementById('burstTerms'), ['Burst Term', 'Count', 'Max Score'],
        (signals.burst_terms || []).map(x => [x.term, x.count, x.max_score]));
      document.getElementById('derived').innerHTML = Object.entries(data.derived_table_counts || {}).map(([k,v]) =>
        `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
      const acquisition = data.transcript_acquisition_metrics || {};
      document.getElementById('acquisition').innerHTML = Object.entries(acquisition.status_counts || {}).map(([k,v]) =>
        `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("") || '<div class="muted">No acquisition attempts recorded yet.</div>';
      table(document.getElementById('acquisitionAttempts'), ['Method', 'Status', 'Count'],
        (acquisition.attempt_counts || []).map(x => [x.method, x.status, x.count]));
      table(document.getElementById('transcriptionRuns'), ['Provider', 'Status', 'Count'],
        (acquisition.transcription_runs || []).map(x => [x.provider, x.status, x.count]));
      table(document.getElementById('preparation'), ['Artifact Type', 'Status', 'Count', 'Avg Boilerplate', 'Avg Quality'],
        (data.transcript_preparation_metrics || []).map(x => [x.artifact_type, x.status, x.count, x.avg_boilerplate_ratio, x.avg_quality_score]));
      table(document.getElementById('sourceYield'), ['Source', 'Category', 'Transcripts', 'Segments', 'Labels', 'Observations', 'Events', 'Obs / 1k Words'],
        (data.source_yield || []).map(x => [x.source_name, x.category, x.transcripts, x.segments, x.labels, x.observations, x.discourse_events, x.observations_per_1000_words]));
      const flags = [...(data.intervention_flags || [])];
      if (health.ok === false) flags.unshift({severity: 'critical', message: `Observer snapshot health: ${health.error || 'not ok'}`});
      document.getElementById('flags').innerHTML = flags.length
        ? flags.map(f => `<div class="flag ${esc(f.severity)}"><b>${esc(f.severity)}</b> ${esc(f.message)}</div>`).join("")
        : '<div class="muted">No intervention flags.</div>';
      table(document.getElementById('queue'), ['Lane', 'Type', 'Status', 'Count'],
        (data.job_type_counts || []).map(x => [x.lane, x.job_type, x.status, x.count]));
      table(document.getElementById('jobs'), ['Lane', 'Type', 'Status', 'Worker', 'Attempts', 'Lease', 'Updated', 'Error'],
        (data.active_jobs || []).map(x => [x.lane, x.job_type, x.status, x.lease_owner, x.attempts, x.leased_until, x.updated_at, x.error]));
      table(document.getElementById('runs'), ['Pack', 'Model', 'Status', 'Prompt', 'Output', 'Output Exists'],
        (data.recent_runs || []).map(x => [x.label_pack, x.model, x.status, x.prompt_artifact, x.output_artifact, x.output_exists]));
      table(document.getElementById('artifacts'), ['Name', 'Bytes'],
        (data.artifacts || []).map(x => [x.name, x.bytes]));
    }).catch(err => {
      document.getElementById('generated').textContent = `Snapshot unavailable: ${err}`;
    });
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:
        if self.path in {"/", "/index.html"}:
            self._send(HTTPStatus.OK, "text/html; charset=utf-8", b"", include_body=False)
        elif self.path == "/state.json":
            self._send(HTTPStatus.OK, "application/json", b"", include_body=False)
        elif self.path == "/readyz":
            self._send(HTTPStatus.OK, "application/json", b"", include_body=False)
        elif self.path == "/healthz":
            status, payload = _health()
            self._send(status, "application/json", b"", include_body=False)
        elif self.path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, "image/x-icon", b"", include_body=False)
        else:
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"", include_body=False)

    def do_GET(self) -> None:
        if self.path in {"/", "/index.html"}:
            self._send(HTTPStatus.OK, "text/html; charset=utf-8", HTML.encode("utf-8"))
        elif self.path == "/state.json":
            if SNAPSHOT_PATH.exists():
                self._send(HTTPStatus.OK, "application/json", _state_body())
            else:
                self._send(HTTPStatus.OK, "application/json", _state_body())
        elif self.path == "/healthz":
            status, payload = _health()
            self._send(status, "application/json", json.dumps(payload, ensure_ascii=True).encode("utf-8"))
        elif self.path == "/readyz":
            self._send(HTTPStatus.OK, "application/json", b'{"ok":true}')
        elif self.path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, "image/x-icon", b"")
        else:
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"not found")

    def do_POST(self) -> None:
        if self.path != "/ingest-snapshot":
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"not found")
            return
        if not INGEST_TOKEN:
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, "application/json", b'{"ok":false,"error":"ingest token not configured"}')
            return
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {INGEST_TOKEN}":
            self._send(HTTPStatus.UNAUTHORIZED, "application/json", b'{"ok":false,"error":"unauthorized"}')
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_000_000:
            self._send(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "application/json", b'{"ok":false,"error":"snapshot too large"}')
            return
        body = self.rfile.read(length)
        try:
            parsed = json.loads(body)
            if parsed.get("privacy") != "sanitized_operational_snapshot_no_raw_transcripts":
                raise ValueError("snapshot privacy marker missing")
            if not isinstance(parsed.get("counts"), dict) or not isinstance(parsed.get("job_counts"), dict):
                raise ValueError("snapshot missing required operational counts")
            parsed = _sanitize_snapshot(parsed)
        except Exception as exc:
            self._send(HTTPStatus.BAD_REQUEST, "application/json", json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))
            return
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_bytes(json.dumps(parsed, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8"))
        self._send(HTTPStatus.OK, "application/json", b'{"ok":true}')

    def log_message(self, format: str, *args) -> None:
        return

    def _send(self, status: HTTPStatus, content_type: str, body: bytes, *, include_body: bool = True) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def main() -> int:
    if not INGEST_TOKEN and os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("RAILWAY_UI_INGEST_TOKEN must be set in Railway")
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
    return 0


def _health() -> tuple[HTTPStatus, dict[str, object]]:
    snapshot_path = _best_snapshot_path()
    if not snapshot_path.exists():
        return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_missing"}
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        generated_at = payload.get("generated_at")
        if not generated_at:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_generated_at_missing"}
        parsed = dt.datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        age_seconds = (dt.datetime.now(dt.timezone.utc) - parsed.astimezone(dt.timezone.utc)).total_seconds()
        if age_seconds > MAX_SNAPSHOT_AGE_SECONDS:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_stale", "age_seconds": int(age_seconds)}
        return HTTPStatus.OK, {"ok": True, "snapshot_age_seconds": int(age_seconds)}
    except Exception as exc:
        return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": f"snapshot_invalid: {exc}"}


def _state_body() -> bytes:
    status, health = _health()
    snapshot_path = _best_snapshot_path()
    if snapshot_path.exists():
        try:
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception as exc:
            payload = {"generated_at": None, "counts": {}, "intervention_flags": []}
            health = {"ok": False, "error": f"snapshot_invalid: {exc}"}
    else:
        payload = {
            "generated_at": None,
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {},
            "job_counts": {},
            "job_type_counts": [],
            "active_jobs": [],
            "recent_runs": [],
            "artifacts": [],
            "intervention_flags": [{"severity": "info", "message": "No snapshot has been published yet."}],
        }
    payload["snapshot_health"] = health
    payload = _sanitize_snapshot(payload)
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")


def _best_snapshot_path() -> Path:
    candidates = [SNAPSHOT_PATH, EXPORT_SNAPSHOT_PATH]
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return SNAPSHOT_PATH
    return max(existing, key=_snapshot_sort_key)


def _snapshot_sort_key(path: Path) -> tuple[float, float]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        generated_at = payload.get("generated_at")
        if generated_at:
            parsed = dt.datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return (parsed.timestamp(), path.stat().st_mtime)
    except Exception:
        pass
    return (0.0, path.stat().st_mtime)


def _sanitize_snapshot(payload: dict) -> dict:
    allowed = {
        "generated_at",
        "privacy",
        "counts",
        "derived_table_counts",
        "coding_metrics",
        "useful_signal_metrics",
        "transcript_preparation_metrics",
        "transcript_acquisition_metrics",
        "research_queue_metrics",
        "content_metrics",
        "source_yield",
        "job_counts",
        "job_type_counts",
        "active_jobs",
        "recent_runs",
        "artifacts",
        "intervention_flags",
        "snapshot_health",
    }
    sanitized = {key: _sanitize_value(value) for key, value in payload.items() if key in allowed}
    sanitized.setdefault("privacy", "sanitized_operational_snapshot_no_raw_transcripts")
    sanitized.setdefault("counts", {})
    sanitized.setdefault("derived_table_counts", {})
    sanitized.setdefault("coding_metrics", {})
    sanitized.setdefault("useful_signal_metrics", {})
    sanitized.setdefault("transcript_preparation_metrics", [])
    sanitized.setdefault("transcript_acquisition_metrics", {})
    sanitized.setdefault("research_queue_metrics", {})
    sanitized.setdefault("content_metrics", {})
    sanitized.setdefault("source_yield", [])
    sanitized.setdefault("job_counts", {})
    sanitized.setdefault("job_type_counts", [])
    sanitized.setdefault("active_jobs", [])
    sanitized.setdefault("recent_runs", [])
    sanitized.setdefault("artifacts", [])
    sanitized.setdefault("intervention_flags", [])
    return sanitized


def _sanitize_value(value):
    if isinstance(value, dict):
        return {str(key): _sanitize_value(child) for key, child in value.items() if _allowed_public_key(str(key))}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value[:200]]
    if isinstance(value, str):
        return _sanitize_text(value)
    return value


def _allowed_public_key(key: str) -> bool:
    blocked = {"target_id", "segment_id", "episode_id", "label_id", "job_id", "id", "prompt_path", "output_path", "source_url", "url"}
    return key not in blocked


def _sanitize_text(value: str) -> str:
    text = re.sub(r"/Users/[^\s\"']+", "<local-path>", value)
    text = re.sub(r"https?://[^\s\"')]+", "<url>", text)
    text = re.sub(r"\b(ep|seg|tr|lbl|run|aud)_[a-f0-9]{12,}\b", r"\1_<id>", text)
    return text[:500]


if __name__ == "__main__":
    raise SystemExit(main())
