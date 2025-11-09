"""Typer-based CLI for queuectl."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import Optional

import typer
from rich import box
from rich.console import Console
from rich.table import Table

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
from .utils import (
    ensure_directories,
    generate_job_id,
    generate_worker_id,
    load_json,
    parse_json_argument,
    utc_now_str,
    write_json,
)
from .worker import run_worker


console = Console()
app = typer.Typer(help="queuectl - minimal background job queue", no_args_is_help=True)
worker_app = typer.Typer(help="Manage worker processes")
config_app = typer.Typer(help="Manage queue configuration")
dlq_app = typer.Typer(help="Dead letter queue operations")

app.add_typer(worker_app, name="worker")
app.add_typer(config_app, name="config")
app.add_typer(dlq_app, name="dlq")


@app.command("web")
def web_server(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind to"),
    port: int = typer.Option(5000, "--port", help="Port to bind to"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode"),
) -> None:
    """Start the web interface server."""
    from .web import run_web_server
    console.print(f"Starting web server at http://{host}:{port}")
    run_web_server(host=host, port=port, debug=debug)


def _initialise() -> None:
    constants.refresh_paths()
    ensure_directories()
    init_db()


@app.callback()
def main() -> None:
    """CLI entry point."""

    _initialise()


@app.command()
def enqueue(
    job_json: Optional[str] = typer.Argument(None, help="Job payload as JSON"),
    id: Optional[str] = typer.Option(None, "--id", help="Job identifier"),
    command: Optional[str] = typer.Option(None, "--command", help="Command to execute"),
    max_retries: Optional[int] = typer.Option(None, "--max-retries", help="Override retry limit"),
) -> None:
    """Add a new job to the queue."""

    payload: dict[str, object] = {}
    if job_json:
        payload = parse_json_argument(job_json)

    if id:
        payload["id"] = id
    if command:
        payload["command"] = command
    if "command" not in payload:
        raise typer.BadParameter("Command is required")

    if "id" not in payload:
        payload["id"] = generate_job_id()

    existing = get_job(str(payload["id"]))
    if existing:
        raise typer.BadParameter(f"Job {payload['id']} already exists")

    payload.setdefault("state", constants.STATE_PENDING)
    payload.setdefault("attempts", 0)
    if max_retries is not None:
        payload["max_retries"] = max_retries
    else:
        payload.setdefault(
            "max_retries",
            int(get_config(constants.CONFIG_MAX_RETRIES) or constants.DEFAULT_MAX_RETRIES),
        )
    payload.setdefault("created_at", utc_now_str())
    payload.setdefault("updated_at", payload["created_at"])
    payload.setdefault("available_at", payload["created_at"])

    enqueue_job({str(key): value for key, value in payload.items()})
    console.print(f"Enqueued job [bold]{payload['id']}[/bold]")


@app.command()
def status() -> None:
    """Show queue status and active workers."""

    table = Table(title="Job States", box=box.MINIMAL_DOUBLE_HEAD, show_lines=False)
    table.add_column("State")
    table.add_column("Count", justify="right")
    for row in count_jobs_by_state():
        table.add_row(row["state"], str(row["count"]))

    console.print(table)

    registry = _load_worker_registry()
    if not registry:
        console.print("No active workers registered")
        return

    worker_table = Table(title="Workers", box=box.MINIMAL_DOUBLE_HEAD)
    worker_table.add_column("ID")
    worker_table.add_column("PID", justify="right")
    worker_table.add_column("Started")
    worker_table.add_column("Alive")

    for worker in registry:
        alive = "yes" if _pid_alive(worker.get("pid")) else "no"
        worker_table.add_row(
            worker.get("id", "?"),
            str(worker.get("pid", "-")),
            worker.get("started_at", "-"),
            alive,
        )

    console.print(worker_table)


@app.command("list")
def list_jobs_cli(
    state: Optional[str] = typer.Option(None, "--state", help="Filter by state"),
) -> None:
    """List jobs with optional state filter."""

    rows = list_jobs(state)
    if not rows:
        console.print("No jobs found")
        return

    table = Table(title="Jobs", box=box.MINIMAL_DOUBLE_HEAD)
    table.add_column("ID")
    table.add_column("State")
    table.add_column("Attempts", justify="right")
    table.add_column("Max Retries", justify="right")
    table.add_column("Command")

    for row in rows:
        table.add_row(
            row["id"],
            row["state"],
            str(row["attempts"]),
            str(row["max_retries"]),
            row["command"],
        )

    console.print(table)


@app.command("delete")
def delete_job_cli(job_id: str = typer.Argument(..., help="Job ID to delete")) -> None:
    """Delete a job from the queue (any state)."""
    job = get_job(job_id)
    if not job:
        raise typer.BadParameter(f"Job {job_id} not found")
    delete_job(job_id)
    console.print(f"Deleted job [bold]{job_id}[/bold]")


@dlq_app.command("list")
def dlq_list() -> None:
    rows = list_jobs(constants.STATE_DEAD)
    if not rows:
        console.print("Dead letter queue empty")
        return

    table = Table(title="Dead Letter Queue", box=box.MINIMAL_DOUBLE_HEAD)
    table.add_column("ID")
    table.add_column("Attempts", justify="right")
    table.add_column("Command")
    table.add_column("Error")

    for row in rows:
        table.add_row(
            row["id"],
            str(row["attempts"]),
            row["command"],
            row["last_error"] or "",
        )

    console.print(table)


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(..., help="Job id to retry")) -> None:
    job = get_job(job_id)
    if not job or job["state"] != constants.STATE_DEAD:
        raise typer.BadParameter("Job not found in DLQ")
    now = utc_now_str()
    reset_job(job_id, now)
    console.print(f"Requeued job [bold]{job_id}[/bold]")


@dlq_app.command("delete")
def dlq_delete(job_id: str = typer.Argument(..., help="Remove job from DLQ")) -> None:
    job = get_job(job_id)
    if not job or job["state"] != constants.STATE_DEAD:
        raise typer.BadParameter("Job not found in DLQ")
    delete_job(job_id)
    console.print(f"Deleted job {job_id} from DLQ")


@config_app.command("get")
def config_get(key: str) -> None:
    value = get_config(key)
    if value is None:
        console.print(f"No config value for {key}")
    else:
        console.print(f"{key} = {value}")


@config_app.command("set")
def config_set(key: str, value: str) -> None:
    set_config(key, value)
    console.print(f"Updated {key} -> {value}")


@config_app.command("list")
def config_list() -> None:
    rows = list_config()
    table = Table(title="Config", box=box.MINIMAL_DOUBLE_HEAD)
    table.add_column("Key")
    table.add_column("Value")
    table.add_column("Updated")
    for row in rows:
        table.add_row(row["key"], row["value"], row["updated_at"])
    console.print(table)


@worker_app.command("start")
def worker_start(
    count: int = typer.Option(1, "--count", help="Number of workers to start"),
) -> None:
    if count < 1:
        raise typer.BadParameter("Count must be >= 1")

    started = []
    for _ in range(count):
        worker_id = generate_worker_id()
        started.append(_spawn_worker(worker_id))

    console.print(f"Started {len(started)} worker(s)")


@worker_app.command("stop")
def worker_stop(
    worker_id: Optional[str] = typer.Option(None, "--id", help="Specific worker id"),
    wait: bool = typer.Option(False, "--wait", help="Wait for workers to exit"),
    timeout: float = typer.Option(10.0, "--timeout", help="Wait timeout in seconds"),
) -> None:
    registry = _load_worker_registry()
    targets = registry
    if worker_id:
        targets = [w for w in registry if w.get("id") == worker_id]
        if not targets:
            raise typer.BadParameter(f"Worker {worker_id} not found")

    if not targets:
        console.print("No workers registered")
        return

    for worker in targets:
        _request_worker_stop(worker.get("id"))

    if wait:
        deadline = time.time() + timeout
        remaining = {w.get("id"): w.get("pid") for w in targets}
        while remaining and time.time() < deadline:
            finished = [wid for wid, pid in remaining.items() if not _pid_alive(pid)]
            for wid in finished:
                remaining.pop(wid, None)
            if remaining:
                time.sleep(0.5)
        if remaining:
            console.print(f"Timed out waiting for: {', '.join(remaining)}")

    _cleanup_worker_registry()
    console.print("Stop signal sent")


@worker_app.command("logs")
def worker_logs(worker_id: str, lines: int = typer.Option(50, "--lines")) -> None:
    log_path = constants.LOG_DIR / f"{worker_id}.log"
    if not log_path.exists():
        raise typer.BadParameter("Log file not found")
    with log_path.open("r", encoding="utf-8") as fh:
        content = fh.readlines()[-lines:]
    console.print("".join(content))


@app.command("_worker")
def worker_internal(worker_id: str = typer.Option(..., "--worker-id")) -> None:
    run_worker(worker_id)


def _spawn_worker(worker_id: str) -> dict[str, object]:
    now = utc_now_str()
    control_payload = {"id": worker_id, "stop": False, "created_at": now}
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    write_json(control_file, control_payload)

    log_file = constants.LOG_DIR / f"{worker_id}.log"
    log_file.touch(exist_ok=True)
    args = [sys.executable, "-m", "queuectl", "_worker", "--worker-id", worker_id]
    creationflags = 0
    if os.name == "nt":  # pragma: no cover - windows specific
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    stdout = log_file.open("a", encoding="utf-8")
    process = subprocess.Popen(args, stdout=stdout, stderr=stdout, creationflags=creationflags)
    stdout.close()

    registry = _load_worker_registry()
    registry.append({"id": worker_id, "pid": process.pid, "started_at": now})
    write_json(constants.WORKER_REGISTRY_FILE, registry)
    return {"id": worker_id, "pid": process.pid}


def _request_worker_stop(worker_id: Optional[str]) -> None:
    if not worker_id:
        return
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    payload = load_json(control_file, {"id": worker_id})
    payload["stop"] = True
    write_json(control_file, payload)


def _cleanup_worker_registry() -> None:
    registry = [entry for entry in _load_worker_registry() if _pid_alive(entry.get("pid"))]
    write_json(constants.WORKER_REGISTRY_FILE, registry)


def _load_worker_registry() -> list[dict[str, object]]:
    if not constants.WORKER_REGISTRY_FILE.exists():
        return []
    return load_json(constants.WORKER_REGISTRY_FILE, [])


def _pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True

