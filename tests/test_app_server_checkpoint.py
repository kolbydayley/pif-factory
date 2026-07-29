from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_checkpoint import (
    CURRENT_INSTRUCTION_CONTRACT_PATH,
    CURRENT_INSTRUCTION_CONTRACT_SHA256,
    CURRENT_INSTRUCTION_CONTRACT_SIZE_BYTES,
    INSTRUCTION_CONTRACT_ARTIFACT_VERSION,
    canonical_json,
    verify_instruction_contract,
)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class AppServerInstructionContractTest(unittest.TestCase):
    def test_default_binds_the_current_versioned_contract_exactly(self) -> None:
        contract = verify_instruction_contract()

        self.assertEqual(
            contract["artifact_schema_version"],
            INSTRUCTION_CONTRACT_ARTIFACT_VERSION,
        )
        self.assertEqual(
            contract["contract_artifact_path"],
            str(CURRENT_INSTRUCTION_CONTRACT_PATH.resolve()),
        )
        self.assertNotEqual(
            Path(contract["contract_artifact_path"]).name,
            "run-spec-v2.json",
        )
        self.assertEqual(
            contract["contract_artifact_sha256"],
            CURRENT_INSTRUCTION_CONTRACT_SHA256,
        )
        self.assertEqual(
            contract["contract_artifact_size_bytes"],
            CURRENT_INSTRUCTION_CONTRACT_SIZE_BYTES,
        )
        self.assertEqual(contract["project_doc_max_bytes"], 0)
        self.assertEqual(contract["model_visible_project_instruction_bytes"], 0)
        self.assertEqual(contract["strict_config_overlay"]["project_doc_max_bytes"], 0)

    def test_default_does_not_hash_zero_budget_global_instruction_content(self) -> None:
        from research_factory import app_server_checkpoint as checkpoint

        source_path = (Path.home() / ".codex" / "AGENTS.md").resolve()
        actual_sha256_file = checkpoint.sha256_file

        def guarded_sha256_file(path: Path) -> str:
            if Path(path).resolve() == source_path:
                raise AssertionError("zero-budget global instruction bytes were hashed")
            return actual_sha256_file(path)

        with patch.object(checkpoint, "sha256_file", side_effect=guarded_sha256_file):
            verified = checkpoint.verify_instruction_contract()
        self.assertEqual(verified["instruction_source_paths"], [str(source_path)])
        self.assertEqual(verified["model_visible_project_instruction_bytes"], 0)

    def test_explicit_contract_verifies_artifact_hash_size_and_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "AGENTS.md"
            source.write_text("fixture instructions\n", encoding="utf-8")
            source_sha256 = sha256_bytes(source.read_bytes())
            source_path = str(source.resolve())
            payload = {
                "schema_version": "pif_app_server_instruction_contract_v1",
                "instruction_contract": {
                    "expected_path_set_sha256": sha256_bytes(
                        canonical_json([source_path]).encode("utf-8")
                    ),
                    "sources": [
                        {
                            "path": source_path,
                            "content_sha256": source_sha256,
                            "size_bytes": source.stat().st_size,
                        }
                    ],
                },
            }
            artifact = root / "instruction-contract-v99.json"
            artifact.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
            artifact_sha256 = sha256_bytes(artifact.read_bytes())
            artifact_size = artifact.stat().st_size

            verified = verify_instruction_contract(
                artifact,
                expected_artifact_sha256=artifact_sha256,
                expected_artifact_size_bytes=artifact_size,
            )
            self.assertEqual(verified["instruction_sources_count"], 1)
            self.assertEqual(
                verified["source_content_sha256s"], [source_sha256]
            )

            with self.assertRaisesRegex(ValueError, "artifact hash drift"):
                verify_instruction_contract(
                    artifact,
                    expected_artifact_sha256="0" * 64,
                    expected_artifact_size_bytes=artifact_size,
                )
            with self.assertRaisesRegex(ValueError, "artifact size drift"):
                verify_instruction_contract(
                    artifact,
                    expected_artifact_sha256=artifact_sha256,
                    expected_artifact_size_bytes=artifact_size + 1,
                )

            source.write_text("fixture instructionz\n", encoding="utf-8")
            self.assertEqual(source.stat().st_size, len("fixture instructions\n"))
            with self.assertRaisesRegex(ValueError, "source content drift"):
                verify_instruction_contract(
                    artifact,
                    expected_artifact_sha256=artifact_sha256,
                    expected_artifact_size_bytes=artifact_size,
                )

    def test_v2_nonzero_overlay_contract_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = json.loads(CURRENT_INSTRUCTION_CONTRACT_PATH.read_text(encoding="utf-8"))
            payload["instruction_contract"]["project_doc_max_bytes"] = 1
            artifact = root / "instruction-contract-v2-nonzero.json"
            artifact.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "not a zero-byte overlay"):
                verify_instruction_contract(
                    artifact,
                    expected_artifact_sha256=sha256_bytes(artifact.read_bytes()),
                    expected_artifact_size_bytes=artifact.stat().st_size,
                )

    def test_v2_overlay_artifact_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = json.loads(CURRENT_INSTRUCTION_CONTRACT_PATH.read_text(encoding="utf-8"))
            overlay_record = payload["instruction_contract"]["strict_config_overlay"]
            overlay = root / "config-overlay.json"
            overlay.write_text(
                json.dumps(
                    overlay_record["config"],
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            overlay_record["artifact_path"] = str(overlay)
            overlay_record["artifact_sha256"] = sha256_bytes(overlay.read_bytes())
            overlay_record["size_bytes"] = overlay.stat().st_size
            artifact = root / "instruction-contract-v2-overlay-tamper.json"
            artifact.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            artifact_sha256 = sha256_bytes(artifact.read_bytes())
            artifact_size = artifact.stat().st_size
            overlay.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "strict overlay artifact drift"):
                verify_instruction_contract(
                    artifact,
                    expected_artifact_sha256=artifact_sha256,
                    expected_artifact_size_bytes=artifact_size,
                )


if __name__ == "__main__":
    unittest.main()
