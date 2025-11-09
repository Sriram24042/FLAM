from __future__ import annotations

import os
import shutil
import sys
import threading
import time
from pathlib import Path

from typer.testing import CliRunner

_default_home = Path.cwd() / "test-data"
os.environ.setdefault("QUEUECTL_HOME", str(_default_home))
if _default_home.exists():
    shutil.rmtree(_default_home)

from queuectl.cli import app  # noqa: E402
import queuectl.constants as constants
from queuectl.storage import get_job
from queuectl.utils import write_json
from queuectl.worker import Worker, WorkerSettings


runner = CliRunner()


def test_job_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUECTL_HOME", str(tmp_path))
    constants.refresh_paths()
    
    # Ensure database is initialized
    from queuectl.storage import init_db
    from queuectl.utils import ensure_directories
    ensure_directories()
    init_db()

    runner.invoke(app, ["config", "set", "poll_interval", "0.1"], env={"QUEUECTL_HOME": str(tmp_path)})
    # Use a simpler command that works on all platforms
    if os.name == "nt":  # Windows
        command = f'"{sys.executable}" -c "print(\\"ok\\")"'
    else:
        command = f'{sys.executable} -c "print(\\"ok\\")"'
    result = runner.invoke(
        app,
        ["enqueue", "--command", command, "--id", "job-test"],
        env={"QUEUECTL_HOME": str(tmp_path)},
    )
    assert result.exit_code == 0

    # Verify job was enqueued
    job = get_job("job-test")
    assert job is not None, "Job was not enqueued"
    assert job["state"] == "pending", f"Job state is {job['state']}, expected pending"

    worker_id = "test-worker"
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    control_file.parent.mkdir(parents=True, exist_ok=True)
    write_json(control_file, {"id": worker_id, "stop": False})

    worker = Worker(WorkerSettings(worker_id, poll_interval=0.1, backoff_base=2))

    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    
    # Give worker a moment to start
    time.sleep(0.5)

    deadline = time.time() + 15  # Increased timeout
    while time.time() < deadline:
        job = get_job("job-test")
        if job and job["state"] == "completed":
            break
        time.sleep(0.2)
    else:
        # Get final job state for debugging
        final_job = get_job("job-test")
        final_state = final_job["state"] if final_job else "not found"
        raise AssertionError(f"job did not complete within timeout. Final state: {final_state}")

    write_json(control_file, {"id": worker_id, "stop": True})
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_dlq_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUECTL_HOME", str(tmp_path))
    constants.refresh_paths()
    command = f'{sys.executable} -c "import sys; sys.exit(1)"'
    runner.invoke(app, ["config", "set", "poll_interval", "0.1"], env={"QUEUECTL_HOME": str(tmp_path)})
    runner.invoke(
        app,
        ["enqueue", "--command", command, "--id", "job-fail", "--max-retries", "1"],
        env={"QUEUECTL_HOME": str(tmp_path)},
    )

    worker_id = "fail-worker"
    control_file = constants.WORKER_DIR / f"{worker_id}.json"
    control_file.parent.mkdir(parents=True, exist_ok=True)
    write_json(control_file, {"id": worker_id, "stop": False})

    worker = Worker(WorkerSettings(worker_id, poll_interval=0.1, backoff_base=1.5))
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()

    deadline = time.time() + 10
    while time.time() < deadline:
        job = get_job("job-fail")
        if job and job["state"] == "dead":
            break
        time.sleep(0.2)
    else:
        raise AssertionError("job did not reach DLQ")

    write_json(control_file, {"id": worker_id, "stop": True})
    thread.join(timeout=5)

    result = runner.invoke(
        app,
        ["dlq", "retry", "job-fail"],
        env={"QUEUECTL_HOME": str(tmp_path)},
    )
    assert result.exit_code == 0
    job = get_job("job-fail")
    assert job["state"] == "pending"

