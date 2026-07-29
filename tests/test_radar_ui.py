from __future__ import annotations

import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from research_factory.radar import ResearchRadar
from research_factory.radar_cli import CONFIG_DIR
from research_factory.radar_ui import (
    create_server,
    render_route,
    validate_loopback_host,
)


class FakeRadar:
    def __init__(self) -> None:
        self.feedback: list[tuple[str, str]] = []
        self.source_updates: list[tuple[str, str, str]] = []

    def list_workspaces(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            {
                "id": "workspace_ai",
                "name": "AI & Technology Radar",
                "topic": "AI <script>alert('topic')</script>",
                "source_count": 3,
            }
        ][:limit]

    def list_briefings(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        unread_only: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        del workspace_id, status, unread_only
        return [
            {
                "id": "brief_one",
                "workspace_id": "workspace_ai",
                "headline": '<script>alert("brief")</script> A model changed',
                "what_changed": "A public result changed.",
                "status": "provisional",
                "event_at": "2026-07-20T10:00:00Z",
                "read_at": None,
                "transcript_text": "SECRET TRANSCRIPT BODY",
            }
        ][:limit]

    def get_briefing(self, briefing_id: str) -> dict[str, Any]:
        return {
            "id": briefing_id,
            "workspace_id": "workspace_ai",
            "headline": '<script>alert("brief")</script> A model changed',
            "what_changed": "A public result changed.",
            "why_it_matters": "It changes a previous comparison.",
            "competing_interpretations": ["The gain is durable", "The test is narrow"],
            "position_changes": [{"entity": "Lab A", "change": "More cautious"}],
            "unresolved_questions": ["Will independent tests reproduce it?"],
            "watch_next": ["Independent benchmark"],
            "status": "provisional",
            "event_at": "2026-07-20T10:00:00Z",
            "transcript": "SECRET TRANSCRIPT BODY",
            "evidence": [
                {
                    "id": "evidence_one",
                    "quote_text": '<b>Exact evidence</b> & context',
                    "support_kind": "primary_evidence",
                    "source_name": "Official Lab",
                    "trust_tier": "primary",
                    "canonical_url": "https://example.com/result",
                    "start_char": 12,
                    "end_char": 48,
                    "raw_content": "SECRET RAW CONTENT",
                }
            ],
            "status_history": [
                {
                    "from_status": "candidate",
                    "to_status": "provisional",
                    "recorded_at": "2026-07-20T10:05:00Z",
                }
            ],
        }

    def workspace_timeline(
        self,
        workspace_id: str,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        del status
        return [
            {
                "id": "development_one",
                "workspace_id": workspace_id,
                "briefing_id": "brief_one",
                "title": "A model changed",
                "summary": "A chronological development.",
                "status": "amended",
                "event_at": "2026-07-20T10:00:00Z",
            }
        ][:limit]

    def entity_positions(self, entity_id: str, limit: int = 50) -> dict[str, Any]:
        return {
            "entity": {"id": entity_id, "canonical_name": "Lab A"},
            "positions": [
                {
                    "id": "position_one",
                    "entity_id": entity_id,
                    "position_text": "The system is not ready for broad deployment.",
                    "stance": "cautious",
                    "changed_position": True,
                    "observed_at": "2026-07-20T10:00:00Z",
                    "evidence_span_id": "evidence_one",
                }
            ][:limit],
        }

    def list_sources(
        self,
        workspace_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        del workspace_id, status
        return [
            {
                "id": "source_one",
                "name": "Official Lab",
                "canonical_url": "https://example.com/feed",
                "trust_tier": "primary",
                "status": "active",
                "discovery_reason": "Referenced by a monitored paper.",
                "content_count": 4,
                "consecutive_fetch_failures": 0,
                "last_relevant_at": "2026-07-20T10:00:00Z",
            }
        ][:limit]

    def operations_summary(self) -> dict[str, Any]:
        return {
            "scheduler_enabled": False,
            "queue": {"queued": 2, "in_progress": 0},
            "dead_letters": {"open": 1},
            "budgets": {"max_items_per_cycle": 25, "max_input_tokens_per_item": 75_000},
            "last_successful_cycle": {"completed_at": "2026-07-20T09:00:00Z"},
            "normalized_text": "SECRET NORMALIZED BODY",
        }

    def record_feedback(self, briefing_id: str, rating: str) -> dict[str, Any]:
        self.feedback.append((briefing_id, rating))
        return {"id": "feedback_one", "briefing_id": briefing_id, "rating": rating}

    def set_source_status(self, source_id: str, status: str, *, reason: str) -> dict[str, Any]:
        self.source_updates.append((source_id, status, reason))
        return {"id": source_id, "status": status, "paused_reason": reason}


@contextmanager
def running_server(service: FakeRadar) -> Iterator[str]:
    server = create_server(host="127.0.0.1", port=0, service_factory=lambda: service)
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@contextmanager
def running_database_server(database: Path) -> Iterator[str]:
    server = create_server(database, host="127.0.0.1", port=0)
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class RadarUIUnitTests(unittest.TestCase):
    def test_non_loopback_binds_are_rejected(self) -> None:
        self.assertEqual(validate_loopback_host("localhost"), "localhost")
        self.assertEqual(validate_loopback_host("127.99.0.1"), "127.99.0.1")
        self.assertEqual(validate_loopback_host("[::1]"), "::1")
        for host in ("0.0.0.0", "::", "192.168.1.10", "example.com", ""):
            with self.subTest(host=host), self.assertRaises(ValueError):
                create_server(host=host, port=0, service_factory=FakeRadar)

    def test_detail_escapes_dynamic_html_and_drops_transcript_bodies(self) -> None:
        document = render_route(FakeRadar(), "/brief", {"briefing_id": ["brief_one"]})
        self.assertNotIn("<script>", document)
        self.assertIn("&lt;script&gt;alert(&quot;brief&quot;)&lt;/script&gt;", document)
        self.assertIn("&lt;b&gt;Exact evidence&lt;/b&gt; &amp; context", document)
        self.assertIn("Primary Evidence", document)
        self.assertIn("Primary", document)
        self.assertIn("characters 12–48", document)
        self.assertNotIn("SECRET TRANSCRIPT BODY", document)
        self.assertNotIn("SECRET RAW CONTENT", document)
        self.assertIn("Evidence briefing", document)
        self.assertIn("Competing interpretations", document)
        self.assertIn("Unresolved questions", document)


class RadarUIHTTPTests(unittest.TestCase):
    def test_canonical_five_item_fixture_serves_through_real_core(self) -> None:
        fixture = json.loads((CONFIG_DIR / "demo_five_items_v1.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "research-radar.sqlite3"
            with ResearchRadar(database) as radar:
                seeded = radar.seed_demo(fixture)
            self.assertEqual(seeded["inserted_items"], 5)
            workspace_id = fixture["workspace"]["workspace_id"]
            briefing_id = fixture["items"][0]["brief"]["briefing_id"]
            with running_database_server(database) as base:
                inbox = urlopen(base + "/inbox?unread_only=0", timeout=3).read().decode("utf-8")
                detail = urlopen(base + f"/api/briefs/{briefing_id}", timeout=3).read().decode("utf-8")
                timeline = json.loads(
                    urlopen(base + f"/api/timeline?workspace_id={workspace_id}", timeout=3)
                    .read()
                    .decode("utf-8")
                )
            self.assertIn("A smaller model changes the cost discussion", inbox)
            self.assertIn("quote_text", detail)
            self.assertNotIn("normalized_text", detail)
            self.assertNotIn("transcript", detail.lower())
            self.assertTrue(timeline["ok"])
            self.assertEqual(len(timeline["data"]), 5)

    def test_all_dashboard_pages_are_private_html(self) -> None:
        service = FakeRadar()
        with running_server(service) as base:
            routes_and_text = {
                "/inbox": "Radar inbox",
                "/timeline": "Workspace timeline",
                "/brief?briefing_id=brief_one": "Supporting evidence",
                "/positions?entity_id=entity_one": "Entity positions",
                "/sources": "Source ledger",
                "/operations": "Operations",
            }
            for route, expected in routes_and_text.items():
                with self.subTest(route=route):
                    response = urlopen(base + route, timeout=3)
                    body = response.read().decode("utf-8")
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.headers["Cache-Control"], "no-store")
                    self.assertIn("default-src 'none'", response.headers["Content-Security-Policy"])
                    self.assertEqual(response.headers["Cross-Origin-Resource-Policy"], "same-origin")
                    self.assertIn(expected, body)
                    self.assertIn("Local · Private · Evidence-linked", body)
                    self.assertIn("No raw transcripts", body)
                    self.assertNotIn("SECRET TRANSCRIPT BODY", body)
                    self.assertNotIn("SECRET NORMALIZED BODY", body)
                    self.assertNotIn("<script>alert", body)

            health = json.loads(urlopen(base + "/healthz", timeout=3).read().decode("utf-8"))
            self.assertTrue(health["ok"])
            self.assertTrue(health["loopback_only"])
            self.assertFalse(health["railway_content_used"])
            self.assertFalse(health["transcripts_exposed"])

    def test_json_endpoints_cover_complete_radar_surface_and_redact_bodies(self) -> None:
        service = FakeRadar()
        with running_server(service) as base:
            routes = (
                "/api/workspaces",
                "/api/briefs?workspace_id=workspace_ai&unread_only=true",
                "/api/briefs/brief_one",
                "/api/timeline?workspace_id=workspace_ai",
                "/api/entity-positions?entity_id=entity_one",
                "/api/entities/entity_one/positions",
                "/api/sources?workspace_id=workspace_ai",
                "/api/operations",
            )
            for route in routes:
                with self.subTest(route=route):
                    response = urlopen(base + route, timeout=3)
                    raw = response.read().decode("utf-8")
                    payload = json.loads(raw)
                    self.assertEqual(response.status, 200)
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["privacy"], "local_private")
                    self.assertNotIn("SECRET TRANSCRIPT BODY", raw)
                    self.assertNotIn("SECRET RAW CONTENT", raw)
                    self.assertNotIn("SECRET NORMALIZED BODY", raw)

            with self.assertRaises(HTTPError) as missing_workspace:
                urlopen(base + "/api/timeline", timeout=3)
            self.assertEqual(missing_workspace.exception.code, 400)

            with self.assertRaises(HTTPError) as invalid_route:
                urlopen(base + "/api/briefs/brief_one/extra", timeout=3)
            self.assertEqual(invalid_route.exception.code, 400)

    def test_feedback_is_bounded_local_and_does_not_rewrite_briefing(self) -> None:
        service = FakeRadar()
        with running_server(service) as base:
            request = Request(
                base + "/api/feedback",
                data=json.dumps({"briefing_id": "brief_one", "rating": "useful"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            response = urlopen(request, timeout=3)
            payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(response.status, 201)
            self.assertEqual(payload["data"]["rating"], "useful")
            self.assertEqual(service.feedback, [("brief_one", "useful")])

            invalid = Request(
                base + "/api/feedback",
                data=json.dumps({"briefing_id": "brief_one", "rating": "rewrite"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(HTTPError) as raised:
                urlopen(invalid, timeout=3)
            self.assertEqual(raised.exception.code, 400)

            cross_origin = Request(
                base + "/api/feedback",
                data=json.dumps({"briefing_id": "brief_one", "rating": "follow"}).encode("utf-8"),
                headers={"Content-Type": "application/json", "Origin": "https://attacker.example"},
                method="POST",
            )
            with self.assertRaises(HTTPError) as raised_origin:
                urlopen(cross_origin, timeout=3)
            self.assertEqual(raised_origin.exception.code, 403)
            self.assertEqual(service.feedback, [("brief_one", "useful")])

    def test_source_control_only_permits_nondestructive_pause_or_block(self) -> None:
        service = FakeRadar()
        with running_server(service) as base:
            request = Request(
                base + "/api/source-status",
                data=json.dumps(
                    {"source_id": "source_one", "status": "blocked", "reason": "not relevant"}
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            response = urlopen(request, timeout=3)
            payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(response.status, 200)
            self.assertEqual(payload["data"]["status"], "blocked")
            self.assertEqual(service.source_updates, [("source_one", "blocked", "not relevant")])

            destructive = Request(
                base + "/api/source-status",
                data=json.dumps(
                    {"source_id": "source_one", "status": "deleted", "reason": "remove"}
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(HTTPError) as raised:
                urlopen(destructive, timeout=3)
            self.assertEqual(raised.exception.code, 400)
            self.assertEqual(service.source_updates, [("source_one", "blocked", "not relevant")])

    def test_host_header_and_unsupported_mutations_are_rejected(self) -> None:
        with running_server(FakeRadar()) as base:
            hostile_host = Request(base + "/api/operations", headers={"Host": "attacker.example"})
            with self.assertRaises(HTTPError) as bad_host:
                urlopen(hostile_host, timeout=3)
            self.assertEqual(bad_host.exception.code, 400)

            put = Request(base + "/api/workspaces", data=b"{}", method="PUT")
            with self.assertRaises(HTTPError) as method:
                urlopen(put, timeout=3)
            self.assertEqual(method.exception.code, 405)


if __name__ == "__main__":
    unittest.main()
