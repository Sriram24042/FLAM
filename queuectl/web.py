"""Web interface for queuectl."""

from __future__ import annotations

import json
from typing import Any, Optional

from flask import Flask, jsonify, render_template, request

from . import constants
from .storage import (
    count_jobs_by_state,
    delete_job,
    enqueue_job,
    get_config,
    get_job,
    init_db,
    list_config,
    list_jobs,
    reset_job,
    set_config,
)
from .utils import ensure_directories, generate_job_id, generate_worker_id, load_json, utc_now_str, write_json
import os
import subprocess
import sys
from pathlib import Path

# Set template folder to queuectl/templates
template_dir = Path(__file__).parent / "templates"
app = Flask(__name__, template_folder=str(template_dir))


def _initialise() -> None:
    """Initialize database and directories."""
    constants.refresh_paths()
    ensure_directories()
    init_db()


def _load_worker_registry() -> list[dict[str, Any]]:
    """Load worker registry from file."""
    if not constants.WORKER_REGISTRY_FILE.exists():
        return []
    return load_json(constants.WORKER_REGISTRY_FILE, [])


def _pid_alive(pid: Optional[int]) -> bool:
    """Check if a process ID is alive."""
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def _spawn_worker(worker_id: str) -> dict[str, Any]:
    """Spawn a new worker process."""
    now = utc_now_str()
    control_payload = {"id": worker_id, "stop": False, "created_at": now}
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    write_json(control_file, control_payload)

    log_file = constants.LOG_DIR / f"{worker_id}.log"
    log_file.touch(exist_ok=True)
    args = [sys.executable, "-m", "queuectl", "_worker", "--worker-id", worker_id]
    creationflags = 0
    if os.name == "nt":  # Windows specific
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    stdout = log_file.open("a", encoding="utf-8")
    process = subprocess.Popen(args, stdout=stdout, stderr=stdout, creationflags=creationflags)
    stdout.close()

    registry = _load_worker_registry()
    registry.append({"id": worker_id, "pid": process.pid, "started_at": now})
    write_json(constants.WORKER_REGISTRY_FILE, registry)
    return {"id": worker_id, "pid": process.pid}


def _request_worker_stop(worker_id: str) -> None:
    """Request a worker to stop."""
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    payload = load_json(control_file, {"id": worker_id})
    payload["stop"] = True
    write_json(control_file, payload)


def _cleanup_worker_registry() -> None:
    """Remove dead workers from registry."""
    registry = [entry for entry in _load_worker_registry() if _pid_alive(entry.get("pid"))]
    write_json(constants.WORKER_REGISTRY_FILE, registry)


@app.route("/")
def index() -> str:
    """Main dashboard page."""
    _initialise()
    return render_template("dashboard.html")


@app.route("/api/debug")
def api_debug() -> dict[str, Any]:
    """Debug endpoint to check database connection and data."""
    try:
        _initialise()
        jobs = list_jobs()
        return jsonify({
            "db_file": str(constants.DB_FILE),
            "data_dir": str(constants.DATA_DIR),
            "job_count": len(jobs),
            "sample_jobs": [
                {
                    "id": str(job["id"]),
                    "state": str(job["state"]),
                    "command": str(job["command"])[:50],
                }
                for job in jobs[:3]
            ] if jobs else []
        })
    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "db_file": str(constants.DB_FILE) if hasattr(constants, 'DB_FILE') else "unknown",
        }), 500


@app.route("/api/status")
def api_status() -> dict[str, Any]:
    """Get queue status and worker information."""
    _initialise()
    
    job_counts = {row["state"]: row["count"] for row in count_jobs_by_state()}
    
    registry = _load_worker_registry()
    workers = []
    for worker in registry:
        pid = worker.get("pid")
        workers.append({
            "id": worker.get("id", "?"),
            "pid": pid,
            "started_at": worker.get("started_at", "-"),
            "alive": _pid_alive(pid) if pid else False,
        })
    
    return jsonify({
        "job_counts": job_counts,
        "workers": workers,
    })


@app.route("/api/jobs")
def api_jobs() -> dict[str, Any]:
    """Get list of jobs, optionally filtered by state."""
    try:
        _initialise()
        state = request.args.get("state")
        
        # Special handling: "processing" filter should include "pending", "processing", and "failed" states
        # (failed jobs are waiting for retry after backoff delay)
        # Also exclude "dead" (DLQ) jobs from main jobs list
        if state == "processing":
            # Get pending, processing, and failed (retryable) jobs
            pending_jobs = list_jobs("pending")
            processing_jobs = list_jobs("processing")
            failed_jobs = list_jobs("failed")
            jobs = list(pending_jobs) + list(processing_jobs) + list(failed_jobs)
        elif state:
            jobs = list_jobs(state)
        else:
            # For "All States", exclude dead (DLQ) jobs - they're shown in DLQ section
            all_jobs = list_jobs(None)
            jobs = [job for job in all_jobs if str(job["state"]) != constants.STATE_DEAD]
        
        # Convert SQLite Row objects to dictionaries
        jobs_list = []
        for job in jobs:
            # SQLite Row objects use [] access, not .get()
            completed_at = job["completed_at"] if "completed_at" in job.keys() and job["completed_at"] else None
            last_error = job["last_error"] if "last_error" in job.keys() and job["last_error"] else None
            jobs_list.append({
                "id": str(job["id"]),
                "command": str(job["command"]),
                "state": str(job["state"]),
                "attempts": int(job["attempts"]),
                "max_retries": int(job["max_retries"]),
                "created_at": str(job["created_at"]),
                "updated_at": str(job["updated_at"]),
                "completed_at": str(completed_at) if completed_at else None,
                "last_error": str(last_error) if last_error else None,
            })
        
        return jsonify({"jobs": jobs_list})
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return jsonify({"error": error_msg, "jobs": []}), 500


@app.route("/api/dlq")
def api_dlq() -> dict[str, Any]:
    """Get dead letter queue jobs."""
    try:
        _initialise()
        jobs = list_jobs(constants.STATE_DEAD)
        
        # Convert SQLite Row objects to dictionaries
        jobs_list = []
        for job in jobs:
            # SQLite Row objects use [] access, not .get()
            last_error = job["last_error"] if "last_error" in job.keys() and job["last_error"] else None
            jobs_list.append({
                "id": str(job["id"]),
                "command": str(job["command"]),
                "state": str(job["state"]),
                "attempts": int(job["attempts"]),
                "max_retries": int(job["max_retries"]),
                "created_at": str(job["created_at"]),
                "updated_at": str(job["updated_at"]),
                "last_error": str(last_error) if last_error else None,
            })
        
        return jsonify({"jobs": jobs_list})
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return jsonify({"error": error_msg, "jobs": []}), 500


@app.route("/api/jobs/<job_id>")
def api_job_detail(job_id: str) -> dict[str, Any]:
    """Get details of a specific job."""
    _initialise()
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    # SQLite Row objects use [] access, not .get()
    def get_row_value(row, key, default=None):
        return row[key] if key in row.keys() and row[key] else default
    
    return jsonify({
        "id": str(job["id"]),
        "command": str(job["command"]),
        "state": str(job["state"]),
        "attempts": int(job["attempts"]),
        "max_retries": int(job["max_retries"]),
        "created_at": str(job["created_at"]),
        "updated_at": str(job["updated_at"]),
        "available_at": str(get_row_value(job, "available_at")) if get_row_value(job, "available_at") else None,
        "processing_started_at": str(get_row_value(job, "processing_started_at")) if get_row_value(job, "processing_started_at") else None,
        "completed_at": str(get_row_value(job, "completed_at")) if get_row_value(job, "completed_at") else None,
        "last_error": str(get_row_value(job, "last_error")) if get_row_value(job, "last_error") else None,
    })


@app.route("/api/jobs", methods=["POST"])
def api_enqueue_job() -> dict[str, Any]:
    """Enqueue a new job."""
    _initialise()
    data = request.get_json() or {}
    
    job_id = data.get("id") or generate_job_id()
    command = data.get("command")
    max_retries = data.get("max_retries")
    
    if not command:
        return jsonify({"error": "Command is required"}), 400
    
    existing = get_job(job_id)
    if existing:
        return jsonify({"error": f"Job {job_id} already exists"}), 400
    
    now = utc_now_str()
    payload = {
        "id": job_id,
        "command": command,
        "state": constants.STATE_PENDING,
        "attempts": 0,
        "max_retries": max_retries or int(get_config(constants.CONFIG_MAX_RETRIES) or constants.DEFAULT_MAX_RETRIES),
        "created_at": now,
        "updated_at": now,
        "available_at": now,
    }
    
    enqueue_job(payload)
    return jsonify({"id": job_id, "message": "Job enqueued"})


@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def api_delete_job(job_id: str) -> dict[str, Any]:
    """Delete a job from the queue."""
    _initialise()
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    delete_job(job_id)
    return jsonify({"message": f"Job {job_id} deleted"})


@app.route("/api/jobs/<job_id>/retry", methods=["POST"])
def api_retry_job(job_id: str) -> dict[str, Any]:
    """Retry a job from DLQ."""
    _initialise()
    job = get_job(job_id)
    if not job or job["state"] != constants.STATE_DEAD:
        return jsonify({"error": "Job not found in DLQ"}), 404
    
    now = utc_now_str()
    reset_job(job_id, now)
    return jsonify({"message": f"Job {job_id} requeued"})


@app.route("/api/workers", methods=["GET"])
def api_list_workers() -> dict[str, Any]:
    """List all workers."""
    _initialise()
    registry = _load_worker_registry()
    workers = []
    for worker in registry:
        pid = worker.get("pid")
        workers.append({
            "id": worker.get("id", "?"),
            "pid": pid,
            "started_at": worker.get("started_at", "-"),
            "alive": _pid_alive(pid) if pid else False,
        })
    return jsonify({"workers": workers})


@app.route("/api/workers", methods=["POST"])
def api_start_worker() -> dict[str, Any]:
    """Start worker(s)."""
    _initialise()
    data = request.get_json() or {}
    count = data.get("count", 1)
    
    if count < 1:
        return jsonify({"error": "Count must be >= 1"}), 400
    
    started = []
    for _ in range(count):
        worker_id = generate_worker_id()
        worker_info = _spawn_worker(worker_id)
        started.append(worker_info)
    
    return jsonify({"message": f"Started {len(started)} worker(s)", "workers": started})


@app.route("/api/workers/<worker_id>", methods=["DELETE"])
def api_stop_worker(worker_id: str) -> dict[str, Any]:
    """Stop a specific worker."""
    _initialise()
    registry = _load_worker_registry()
    worker = next((w for w in registry if w.get("id") == worker_id), None)
    
    if not worker:
        return jsonify({"error": f"Worker {worker_id} not found"}), 404
    
    _request_worker_stop(worker_id)
    _cleanup_worker_registry()
    return jsonify({"message": f"Stop signal sent to worker {worker_id}"})


@app.route("/api/config")
def api_get_config() -> dict[str, Any]:
    """Get all configuration."""
    _initialise()
    configs = list_config()
    return jsonify({
        "config": {row["key"]: row["value"] for row in configs}
    })


@app.route("/api/config/<key>", methods=["PUT"])
def api_set_config(key: str) -> dict[str, Any]:
    """Set a configuration value."""
    _initialise()
    data = request.get_json() or {}
    value = data.get("value")
    
    if value is None:
        return jsonify({"error": "Value is required"}), 400
    
    set_config(key, str(value))
    return jsonify({"message": f"Updated {key} -> {value}"})


def run_web_server(host: str = "127.0.0.1", port: int = 5000, debug: bool = False) -> None:
    """Run the Flask web server."""
    app.run(host=host, port=port, debug=debug)

