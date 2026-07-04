from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def root() -> Path:
    return Path(os.environ.get("RESEARCH_FACTORY_ROOT", PROJECT_ROOT)).resolve()


def data_dir() -> Path:
    path = root() / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def db_path() -> Path:
    return Path(os.environ.get("RESEARCH_FACTORY_DB", data_dir() / "factory.sqlite")).resolve()


def corpus_dir() -> Path:
    path = root() / "corpus"
    path.mkdir(parents=True, exist_ok=True)
    return path


def runs_dir() -> Path:
    path = root() / "runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def reports_dir() -> Path:
    path = root() / "reports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def exports_dir() -> Path:
    path = root() / "exports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def label_pack_dir(label_pack: str) -> Path:
    return root() / "label_packs" / label_pack

