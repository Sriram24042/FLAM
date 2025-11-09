"""Utility helpers for queuectl."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__, constants


def ensure_directories() -> None:
    """Ensure queuectl data directories exist."""

    constants.refresh_paths()
    for directory in (constants.DATA_DIR, constants.WORKER_DIR, constants.LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    """Return the current UTC datetime."""

    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    """Return an ISO formatted UTC timestamp."""

    return utc_now().isoformat().replace("+00:00", "Z")


def parse_json_argument(raw: str) -> dict[str, Any]:
    """Parse a JSON string from CLI input."""

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - simple error wrapping
        raise ValueError(f"Invalid JSON payload: {exc.msg}") from exc


def load_json(path: Path, default: Any) -> Any:
    """Load JSON from path or return default."""

    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, payload: Any) -> None:
    """Write JSON payload to disk atomically."""

    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def generate_worker_id() -> str:
    """Generate a unique worker identifier."""

    suffix = uuid.uuid4().hex[:8]
    return f"worker-{suffix}"


def generate_job_id() -> str:
    """Generate a unique job identifier."""

    return f"job-{uuid.uuid4().hex[:8]}"


def user_agent() -> str:
    """Return a simple user agent string."""

    return f"queuectl/{__version__}"

