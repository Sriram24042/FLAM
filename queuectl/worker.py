"""Worker process implementation."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import constants
from .storage import (
    acquire_next_job,
    get_config,
    mark_job_completed,
    mark_job_failed,
    init_db,
)
from .utils import ensure_directories, utc_now_str


@dataclass
class WorkerSettings:
    worker_id: str
    poll_interval: float
    backoff_base: float


class Worker:
    """Long-running worker loop."""

    def __init__(self, settings: WorkerSettings) -> None:
        constants.refresh_paths()
        self.settings = settings
        self.control_file = constants.WORKER_DIR / f"{settings.worker_id}.json"
        ensure_directories()
        init_db()
        signal.signal(signal.SIGTERM, self._handle_signal)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, self._handle_signal)
        self._stop_requested = False

    def _handle_signal(self, signum: int, _: Optional[object]) -> None:  # pragma: no cover - signal path
        self._stop_requested = True

    def _should_stop(self) -> bool:
        if self._stop_requested:
            return True
        if not self.control_file.exists():
            return False
        try:
            with self.control_file.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except json.JSONDecodeError:
            return False
        return bool(payload.get("stop"))

    def run(self) -> None:
        log_path = constants.LOG_DIR / f"{self.settings.worker_id}.log"
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"[{utc_now_str()}] worker {self.settings.worker_id} started\n")
            while not self._should_stop():
                now = utc_now_str()
                job = acquire_next_job(now)
                if job is None:
                    time.sleep(self.settings.poll_interval)
                    continue

                log.write(f"[{utc_now_str()}] picked job {job.id}\n")
                log.flush()

                attempts = job.attempts + 1
                error_message = None
                exit_code = None
                
                try:
                    result = subprocess.run(
                        job.command, 
                        shell=True, 
                        capture_output=True, 
                        text=True,
                        timeout=3600  # 1 hour timeout to prevent hanging jobs
                    )
                    exit_code = result.returncode
                    stdout = result.stdout or ""
                    stderr = result.stderr or ""
                    
                    if exit_code == 0:
                        mark_job_completed(job.id, attempts, utc_now_str())
                        log.write(f"[{utc_now_str()}] job {job.id} completed\n")
                        if stdout:
                            log.write(stdout + "\n")
                    else:
                        # Command failed with non-zero exit code
                        error_message = stderr or stdout or f"Command exited with code {exit_code}"
                        if not error_message.strip():
                            error_message = f"Command failed with exit code {exit_code} (no error output)"
                        
                except subprocess.TimeoutExpired:
                    # Command timed out
                    error_message = f"Command execution timed out after 1 hour"
                    exit_code = -1
                    
                except FileNotFoundError:
                    # Command not found (invalid command)
                    error_message = f"Command not found: '{job.command}'. The command or executable does not exist."
                    exit_code = 127  # Standard "command not found" exit code
                    
                except PermissionError:
                    # Permission denied
                    error_message = f"Permission denied executing command: '{job.command}'"
                    exit_code = 126  # Standard "permission denied" exit code
                    
                except Exception as exc:
                    # Any other exception during command execution
                    error_message = f"Error executing command: {str(exc)}"
                    exit_code = -1
                
                # Handle failures (non-zero exit code or exceptions)
                if exit_code != 0:
                    # Calculate exponential backoff delay
                    backoff_seconds = self.settings.backoff_base ** attempts
                    
                    # Determine if we should retry or move to DLQ
                    should_retry = attempts < job.max_retries
                    
                    # Mark job as failed or dead (DLQ)
                    mark_job_failed(
                        job.id,
                        attempts,
                        job.max_retries,
                        error_message or f"Command failed with exit code {exit_code}",
                        utc_now_str(),
                        backoff_seconds if should_retry else 0,
                    )
                    
                    # Determine final state
                    state = constants.STATE_FAILED if should_retry else constants.STATE_DEAD
                    
                    log.write(
                        f"[{utc_now_str()}] job {job.id} {state} (attempt {attempts}/{job.max_retries})\n"
                    )
                    if error_message:
                        log.write(f"Error: {error_message}\n")
                    
                    if state == constants.STATE_DEAD:
                        log.write(
                            f"[{utc_now_str()}] job {job.id} moved to Dead Letter Queue (DLQ) after {attempts} failed attempts\n"
                        )
                
                log.flush()

            log.write(f"[{utc_now_str()}] worker {self.settings.worker_id} stopping\n")

        if self.control_file.exists():  # best-effort cleanup
            try:
                self.control_file.unlink()
            except OSError:
                pass


def load_worker_settings(worker_id: str) -> WorkerSettings:
    init_db()
    poll_interval = float(get_config(constants.CONFIG_POLL_INTERVAL) or 2.0)
    backoff_base = float(get_config(constants.CONFIG_BACKOFF_BASE) or 2.0)
    return WorkerSettings(worker_id=worker_id, poll_interval=poll_interval, backoff_base=backoff_base)


def run_worker(worker_id: str) -> None:
    worker = Worker(load_worker_settings(worker_id))
    worker.run()

