#!/usr/bin/env python3
"""Localhost receiver for browser-harvested YouTube caption transcripts.

The authenticated Chrome tab runs a paced harvest loop (installed by the
Claude session): it asks GET /work for the next episode, fetches the
transcript via YouTube's own get_panel endpoint in the page context, and
POSTs it back to /save. This server hands out work from
work/pif-ops/youtube-captions/worklist.json and writes <video_id>.txt
cache files that research_factory.ingest serves instead of the (blocked)
youtube-transcript-api.

Loopback-only by construction. Run: python3 scripts/pif_caption_receiver.py
"""
from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
CACHE = PIF_ROOT / "work" / "pif-ops" / "youtube-captions"
WORKLIST = CACHE / "worklist.json"
PORT = 8917
MIN_CHARS = 2000  # a real podcast transcript is far larger; short = junk


class Handler(BaseHTTPRequestHandler):
    def _send(self, code=200, body=b"", ctype="application/json"):
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin", "https://www.youtube.com")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "content-type")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Access-Control-Allow-Local-Network", "true")
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    protocol_version = "HTTP/1.1"

    def do_OPTIONS(self):
        self._send(200)


    def do_GET(self):
        if self.path.startswith("/work"):
            work = json.loads(WORKLIST.read_text())
            pending = [w for w in work
                       if not (CACHE / f"{w['video_id']}.txt").exists()
                       and not (CACHE / f"{w['video_id']}.skip").exists()]
            self._send(200, json.dumps(
                {"next": pending[:1], "remaining": len(pending)}).encode())
        elif self.path == "/status":
            done = len(list(CACHE.glob("*.txt")))
            skipped = len(list(CACHE.glob("*.skip")))
            self._send(200, json.dumps(
                {"harvested": done, "skipped": skipped}).encode())
        else:
            self._send(404)

    def do_POST(self):
        if self.path != "/save":
            self._send(404)
            return
        length = int(self.headers.get("content-length", 0))
        if length > 3_000_000:
            self._send(413)
            return
        payload = json.loads(self.rfile.read(length))
        vid = str(payload.get("video_id", ""))[:11]
        text = payload.get("text") or ""
        err = payload.get("error")
        if not vid or not vid.replace("-", "").replace("_", "").isalnum():
            self._send(400)
            return
        if err or len(text) < MIN_CHARS:
            (CACHE / f"{vid}.skip").write_text(
                json.dumps({"error": err, "chars": len(text),
                            "at": time.strftime("%F %T")}))
            print(f"skip {vid}: {err or 'too short'} ({len(text)} chars)",
                  flush=True)
        else:
            (CACHE / f"{vid}.txt").write_text(text)
            print(f"saved {vid}: {len(text)} chars", flush=True)
        self._send(200, b'{"ok": true}')

    def log_message(self, fmt, *a):  # echo requests for diagnostics
        print(f"req {self.command} {self.path} from {self.client_address[0]}",
              flush=True)


if __name__ == "__main__":
    CACHE.mkdir(parents=True, exist_ok=True)
    print(f"caption receiver on 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
