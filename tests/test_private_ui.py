from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from research_factory import db
from research_factory.cli import main as cli_main
from research_factory.private_ui import (
    create_server,
    private_query_service,
    render_route,
    validate_loopback_host,
)


class _EscapingService:
    def search(self, query: str, *, limit: int = 50) -> dict[str, Any]:
        return {
            "ok": True,
            "surface": "search",
            "privacy": "local_private_read_only",
            "data": [
                {
                    "result_type": "atomic_claim",
                    "id": "claim<&",
                    "text": '<script>alert("private")</script>',
                    "evidence_unit_id": "segment&one",
                    "pipeline_run_id": "run one",
                    "corpus_release_id": "release/one",
                }
            ],
        }


@contextmanager
def _temporary_database() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "factory.sqlite"
        conn = db.connect(path)
        try:
            db.init_db(conn)
            conn.execute("CREATE TABLE private_ui_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            conn.execute("INSERT INTO private_ui_probe (value) VALUES ('unchanged')")
            conn.commit()
        finally:
            conn.close()
        yield path


@contextmanager
def _running_server(database_path: Path) -> Iterator[str]:
    server = create_server(database_path, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class PrivateUIUnitTests(unittest.TestCase):
    def test_non_loopback_bind_is_rejected_before_server_creation(self) -> None:
        self.assertEqual(validate_loopback_host("localhost"), "localhost")
        self.assertEqual(validate_loopback_host("127.42.0.1"), "127.42.0.1")
        self.assertEqual(validate_loopback_host("[::1]"), "::1")
        for host in ("0.0.0.0", "::", "192.168.1.10", "example.com", ""):
            with self.subTest(host=host), self.assertRaises(ValueError):
                create_server("does-not-matter.sqlite", host=host, port=0)

    def test_dynamic_html_is_escaped_and_provenance_identifiers_are_links(self) -> None:
        document = render_route(
            _EscapingService(),  # type: ignore[arg-type]
            "/search",
            {"q": ['"><img src=x onerror=alert(1)>'], "limit": ["5"]},
        )
        self.assertNotIn("<script>alert", document)
        self.assertNotIn("<img src=x", document)
        self.assertIn("&lt;script&gt;alert(&quot;private&quot;)&lt;/script&gt;", document)
        self.assertIn("&quot;&gt;&lt;img src=x onerror=alert(1)&gt;", document)
        self.assertIn("/lineage?kind=claim&amp;id=claim%3C%26", document)
        self.assertIn("/lineage?kind=pipeline_run&amp;id=run+one", document)
        self.assertIn("/lineage?kind=corpus_release&amp;id=release%2Fone", document)
        self.assertIn("/lineage?kind=claim&amp;id=claim%3C%26", document)
        self.assertIn("segment&amp;one", document)
        for label in (
            "Search",
            "Subjects / Propositions",
            "People",
            "Show / Network graph",
            "Consensus / Contrarian",
            "Outcomes",
            "Quality",
            "Lineage",
        ):
            self.assertIn(label, document)

    def test_private_query_service_is_strict_and_database_rejects_writes(self) -> None:
        with _temporary_database() as database_path:
            with private_query_service(database_path) as service:
                capabilities = service.capabilities()
                self.assertTrue(capabilities["private_local_only"])
                self.assertTrue(capabilities["read_only"])
                self.assertEqual(capabilities["authority"], "current_accepted_only")
                self.assertFalse(capabilities["legacy_fallbacks"]["enabled"])
                with self.assertRaises(sqlite3.OperationalError):
                    service.conn.execute("INSERT INTO private_ui_probe (value) VALUES ('mutated')")

    def test_top_level_pif_ui_dispatches_without_opening_a_mutable_connection(self) -> None:
        with patch("research_factory.private_ui.main", return_value=0) as private_main:
            result = cli_main(
                [
                    "--db",
                    "/tmp/private-ui-does-not-need-to-exist.sqlite",
                    "ui",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "0",
                ]
            )
        self.assertEqual(result, 0)
        private_main.assert_called_once_with(
            [
                "--db",
                "/tmp/private-ui-does-not-need-to-exist.sqlite",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
            ]
        )


class PrivateUIHTTPTests(unittest.TestCase):
    def test_all_pages_are_local_html_and_requests_do_not_mutate_sqlite(self) -> None:
        with _temporary_database() as database_path:
            with _running_server(database_path) as base:
                routes = (
                    "/search?q=%3Cscript%3Eprivate%3C%2Fscript%3E",
                    "/subjects",
                    "/people",
                    "/network",
                    "/consensus",
                    "/outcomes",
                    "/quality",
                    "/lineage",
                )
                for route in routes:
                    with self.subTest(route=route):
                        response = urlopen(base + route, timeout=3)
                        body = response.read().decode("utf-8")
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.headers["Cache-Control"], "no-store")
                        self.assertIn("default-src 'none'", response.headers["Content-Security-Policy"])
                        self.assertIn("Loopback · Read only", body)
                        self.assertIn("No Railway observer data", body)
                        self.assertNotIn("<script>private</script>", body)

                health = json.loads(urlopen(base + "/healthz", timeout=3).read().decode("utf-8"))
                self.assertTrue(health["ok"])
                self.assertFalse(health["railway_observer_data_used"])
                self.assertTrue(health["capabilities"]["read_only"])

                with self.assertRaises(HTTPError) as raised:
                    urlopen(Request(base + "/search", data=b"q=write", method="POST"), timeout=3)
                self.assertEqual(raised.exception.code, 405)
                self.assertEqual(raised.exception.headers["Allow"], "GET, HEAD")

            conn = sqlite3.connect(database_path)
            try:
                count, value = conn.execute("SELECT COUNT(*), MIN(value) FROM private_ui_probe").fetchone()
                self.assertEqual((count, value), (1, "unchanged"))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
