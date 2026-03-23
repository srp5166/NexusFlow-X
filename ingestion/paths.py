"""Resolved data and config paths for NexusFlow-X (Docker + local)."""
from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def data_root() -> Path:
    env = os.getenv("NEXUSFLOW_DATA_ROOT")
    if env:
        return Path(env)
    if Path("/.dockerenv").exists():
        return Path("/app/data")
    return project_root() / "data"


def quality_rules_path() -> Path:
    env = os.getenv("QUALITY_RULES_PATH")
    if env:
        return Path(env)
    return project_root() / "ingestion" / "quality_rules.yaml"
