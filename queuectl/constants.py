"""Constants used across queuectl."""

from __future__ import annotations

import os
from pathlib import Path


STATE_PENDING = "pending"
STATE_PROCESSING = "processing"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_DEAD = "dead"

CONFIG_MAX_RETRIES = "max_retries_default"
CONFIG_BACKOFF_BASE = "backoff_base"
CONFIG_POLL_INTERVAL = "poll_interval"

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2
DEFAULT_POLL_INTERVAL = 2.0

def refresh_paths() -> None:
    base = Path(os.getenv("QUEUECTL_HOME", Path.home() / ".queuectl"))
    globals()["DATA_DIR"] = base
    globals()["DB_FILE"] = base / "queue.db"
    globals()["WORKER_DIR"] = base / "workers"
    globals()["LOG_DIR"] = base / "logs"
    globals()["WORKER_REGISTRY_FILE"] = globals()["WORKER_DIR"] / "registry.json"


refresh_paths()

