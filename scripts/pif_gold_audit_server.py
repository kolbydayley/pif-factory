#!/usr/bin/env python3
"""Local, read-only audit UI for gold-extracted claims.

Serves a single-page app for browsing every claim in the final adjudicated
gold C outputs (development + validation; the sealed holdout is excluded) and
recording legit / not-legit / unsure verdicts into an isolated SQLite file.
Nothing here writes to a campaign database or result file.

    python3 -B scripts/pif_gold_audit_server.py [--port 8787] [--host 127.0.0.1]

Then open http://127.0.0.1:8787 .  Verdicts land in
work/pif-ops/audit/gold_audit.sqlite; read them back with --dump-illegitimate.
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.gold_audit import (  # noqa: E402
    AUDITABLE_SPLITS,
    all_flags,
    connect_audit_db,
    flag_summary,
    illegitimate_flags,
    load_claims,
    load_window_shows,
    load_window_texts,
    set_flag,
)
from research_factory.util import now_iso  # noqa: E402

GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
AUDIT_DB = PIF_ROOT / "work/pif-ops/audit/gold_audit.sqlite"
INDEX_HTML = Path(__file__).resolve().parent / "gold_audit_assets" / "index.html"

_lock = threading.Lock()
_window_texts: dict[str, str] = {}
_window_shows: dict[str, str] = {}


def _load_window_texts() -> dict[str, str]:
    global _window_texts, _window_shows
    if not _window_texts:
        _window_texts = load_window_texts(MANIFEST, project_root=PIF_ROOT, splits=AUDITABLE_SPLITS)
        _window_shows = load_window_shows(MANIFEST)
    return _window_texts


def _claims_payload() -> dict:
    conn = connect_audit_db(AUDIT_DB)
    try:
        claims = load_claims(GOLD_ROOT, window_texts=_load_window_texts(),
                             window_shows=_window_shows, flags=all_flags(conn))
        summary = flag_summary(conn)
    finally:
        conn.close()
    shows = sorted({c["show"] for c in claims if c.get("show")})
    return {
        "claims": claims,
        "summary": {**summary, "total": len(claims)},
        "splits": list(AUDITABLE_SPLITS),
        "shows": shows,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # quiet
        pass

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj) -> None:
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._send(200, INDEX_HTML.read_bytes(), "text/html; charset=utf-8")
        elif self.path.startswith("/api/claims"):
            with _lock:
                self._json(200, _claims_payload())
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/api/flag":
            self._json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self._json(400, {"error": "invalid json"})
            return
        conn = connect_audit_db(AUDIT_DB)
        try:
            with _lock:
                row = set_flag(
                    conn,
                    event_id=str(data["event_id"]),
                    window_id=str(data["window_id"]),
                    split=str(data.get("split") or "validation"),
                    verdict=str(data["verdict"]),
                    note=str(data.get("note") or ""),
                    now=now_iso(),
                )
                summary = flag_summary(conn)
        except (KeyError, ValueError) as exc:
            self._json(400, {"error": str(exc)})
            return
        finally:
            conn.close()
        self._json(200, {"flag": row, "summary": summary})


def _dump_illegitimate() -> int:
    conn = connect_audit_db(AUDIT_DB)
    try:
        print(json.dumps(illegitimate_flags(conn), indent=2))
    finally:
        conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--dump-illegitimate", action="store_true",
                        help="Print all illegitimate-flagged claims as JSON and exit.")
    args = parser.parse_args(argv)
    if args.dump_illegitimate:
        return _dump_illegitimate()
    print(f"Loading gold windows (read-only)…", flush=True)
    _load_window_texts()
    payload = _claims_payload()
    print(f"Ready: {payload['summary']['total']} claims across {AUDITABLE_SPLITS}.", flush=True)
    print(f"Audit verdicts -> {AUDIT_DB}", flush=True)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Open http://{args.host}:{args.port}  (Ctrl-C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
