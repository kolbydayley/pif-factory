from __future__ import annotations

import unittest
from unittest.mock import patch

from research_factory import pif_cli


class PifRadarDispatchTest(unittest.TestCase):
    def test_radar_dispatch_is_isolated_from_legacy_cli(self) -> None:
        with patch("research_factory.radar_cli.main", return_value=0) as radar_main, patch.object(
            pif_cli.legacy_cli,
            "main",
            side_effect=AssertionError("Research Radar must not enter the legacy CLI"),
        ):
            self.assertEqual(pif_cli.main(["radar", "validate-contract"]), 0)
        radar_main.assert_called_once_with(["validate-contract"])

    def test_explicit_global_database_is_forwarded_to_radar(self) -> None:
        with patch("research_factory.radar_cli.main", return_value=0) as radar_main:
            self.assertEqual(
                pif_cli.main(["--db", "/tmp/radar.sqlite3", "radar", "status"]),
                0,
            )
        radar_main.assert_called_once_with(
            ["--db", "/tmp/radar.sqlite3", "status"]
        )

    def test_radar_owned_database_argument_is_preserved(self) -> None:
        with patch("research_factory.radar_cli.main", return_value=0) as radar_main:
            self.assertEqual(
                pif_cli.main(["radar", "--db", "/tmp/radar.sqlite3", "status"]),
                0,
            )
        radar_main.assert_called_once_with(
            ["--db", "/tmp/radar.sqlite3", "status"]
        )


if __name__ == "__main__":
    unittest.main()
