"""SQLite-backed persistence layer for queuectl."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from . import constants
from .utils import ensure_directories, utc_now_str


Row = sqlite3.Row


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    """Yield a SQLite connection with proper defaults."""

    ensure_directories()
    constants.refresh_paths()
    conn = sqlite3.connect(
        constants.DB_FILE, timeout=30, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Initialise database schema if required."""

    constants.refresh_paths()
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                available_at TEXT NOT NULL,
                processing_started_at TEXT,
                completed_at TEXT,
                last_error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("COMMIT")

    for key, value in (
        (constants.CONFIG_MAX_RETRIES, str(constants.DEFAULT_MAX_RETRIES)),
        (constants.CONFIG_BACKOFF_BASE, str(constants.DEFAULT_BACKOFF_BASE)),
        (constants.CONFIG_POLL_INTERVAL, str(constants.DEFAULT_POLL_INTERVAL)),
    ):
        ensure_config_default(key, value)


def ensure_config_default(key: str, value: str) -> None:
    """Ensure a config key exists; set default if missing."""

    with get_connection() as conn:
        conn.execute("BEGIN")
        existing = conn.execute("SELECT 1 FROM config WHERE key = ?", (key,)).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO config(key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, utc_now_str()),
            )
        conn.execute("COMMIT")


def set_config(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO config(key, value, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, value, utc_now_str()),
        )
        conn.execute("COMMIT")


def get_config(key: str) -> Optional[str]:
    with get_connection() as conn:
        row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def list_config() -> list[Row]:
    with get_connection() as conn:
        return conn.execute("SELECT key, value, updated_at FROM config ORDER BY key").fetchall()


def enqueue_job(job: dict[str, Any]) -> None:
    now = utc_now_str()
    available_at = job.get("available_at") or now
    record = (
        job["id"],
        job["command"],
        job.get("state", constants.STATE_PENDING),
        int(job.get("attempts", 0)),
        int(job.get("max_retries", constants.DEFAULT_MAX_RETRIES)),
        job.get("created_at", now),
        job.get("updated_at", now),
        available_at,
        job.get("processing_started_at"),
        job.get("completed_at"),
        job.get("last_error"),
    )

    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO jobs(
                id, command, state, attempts, max_retries,
                created_at, updated_at, available_at, processing_started_at,
                completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            record,
        )
        conn.execute("COMMIT")


def get_job(job_id: str) -> Optional[Row]:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def list_jobs(state: Optional[str] = None) -> list[Row]:
    query = "SELECT * FROM jobs"
    params: tuple[Any, ...] = ()
    if state:
        query += " WHERE state = ?"
        params = (state,)
    query += " ORDER BY created_at"
    with get_connection() as conn:
        return conn.execute(query, params).fetchall()


def count_jobs_by_state() -> list[Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT state, COUNT(*) AS count FROM jobs GROUP BY state ORDER BY state"
        ).fetchall()


@dataclass
class AcquiredJob:
    """A job record obtained for processing."""

    id: str
    command: str
    attempts: int
    max_retries: int


def acquire_next_job(now: str) -> Optional[AcquiredJob]:
    with get_connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        # Acquire jobs that are pending or failed (ready for retry)
        # Failed jobs become available after exponential backoff delay
        row = conn.execute(
            """
            SELECT id, command, attempts, max_retries
            FROM jobs
            WHERE state IN (?, ?) AND available_at <= ?
            ORDER BY created_at
            LIMIT 1
            """,
            (constants.STATE_PENDING, constants.STATE_FAILED, now),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None

        conn.execute(
            """
            UPDATE jobs
            SET state = ?, processing_started_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (constants.STATE_PROCESSING, now, now, row["id"]),
        )
        conn.execute("COMMIT")

    return AcquiredJob(
        id=row["id"],
        command=row["command"],
        attempts=row["attempts"],
        max_retries=row["max_retries"],
    )


def mark_job_completed(job_id: str, attempts: int, now: str) -> None:
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            UPDATE jobs
            SET state = ?, attempts = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (constants.STATE_COMPLETED, attempts, now, now, job_id),
        )
        conn.execute("COMMIT")


def mark_job_failed(job_id: str, attempts: int, max_retries: int, error: str, now: str, backoff_seconds: float) -> None:
    next_available = utc_now_str() if backoff_seconds <= 0 else _add_seconds(now, backoff_seconds)
    new_state = constants.STATE_FAILED if attempts < max_retries else constants.STATE_DEAD

    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            UPDATE jobs
            SET state = ?, attempts = ?, updated_at = ?, available_at = ?, last_error = ?
            WHERE id = ?
            """,
            (new_state, attempts, now, next_available, error[:512], job_id),
        )
        conn.execute("COMMIT")


def reset_job(job_id: str, now: str) -> None:
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            UPDATE jobs
            SET state = ?, attempts = 0, updated_at = ?, available_at = ?
            WHERE id = ?
            """,
            (constants.STATE_PENDING, now, now, job_id),
        )
        conn.execute("COMMIT")


def delete_job(job_id: str) -> None:
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.execute("COMMIT")


def _add_seconds(iso_timestamp: str, seconds: float) -> str:
    from datetime import datetime, timedelta

    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    dt += timedelta(seconds=seconds)
    return dt.isoformat().replace("+00:00", "Z")

