#!/opt/homebrew/bin/python3
"""Build the minimal home-scoped PIF supervision zipapp."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import zipapp
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path.home() / ".codex" / "lib" / "pif-pipeline-runtime.pyz"
CHECKSUM = OUTPUT.with_suffix(OUTPUT.suffix + ".sha256")
FILES = {
    PROJECT_ROOT / "automation" / "pif-watchdog-runtime" / "__main__.py": Path("__main__.py"),
    PROJECT_ROOT / "research_factory" / "__init__.py": Path("research_factory/__init__.py"),
    PROJECT_ROOT / "research_factory" / "pipeline_babysitter.py": Path("research_factory/pipeline_babysitter.py"),
    PROJECT_ROOT / "research_factory" / "pipeline_watchdog.py": Path("research_factory/pipeline_watchdog.py"),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pif-watchdog-build-") as raw_stage:
        stage = Path(raw_stage)
        for source, relative in FILES.items():
            destination = stage / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        temporary_output = OUTPUT.with_suffix(OUTPUT.suffix + ".tmp")
        temporary_output.unlink(missing_ok=True)
        zipapp.create_archive(
            stage,
            target=temporary_output,
            interpreter="/opt/homebrew/bin/python3",
            compressed=True,
        )
        temporary_output.chmod(0o755)
        temporary_output.replace(OUTPUT)
    digest = sha256_file(OUTPUT)
    temporary_checksum = CHECKSUM.with_suffix(CHECKSUM.suffix + ".tmp")
    temporary_checksum.write_text(digest + "\n", encoding="ascii")
    temporary_checksum.replace(CHECKSUM)
    print(f"{OUTPUT} {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
