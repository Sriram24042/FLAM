# 🚀 QueueCTL -Background Job Queue

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**QueueCTL** is a lightweight, CLI-driven background job queue system with durable storage, automatic retry semantics, and worker management. Built with Python, it provides a simple yet powerful solution for managing asynchronous job processing with SQLite-backed persistence.

## ✨ Features

- **🔄 Automatic Retry Logic** - Exponential backoff retry mechanism for failed jobs
- **💾 Durable Storage** - SQLite database with WAL mode for data persistence
- **📊 Dead Letter Queue (DLQ)** - Failed jobs after max retries are moved to DLQ for manual inspection
- **👷 Worker Management** - Start, stop, and monitor multiple worker processes
- **🌐 Web Dashboard** - Beautiful web interface for monitoring and managing jobs
- **⚙️ Runtime Configuration** - Adjust settings without restarting workers
- **🔍 Comprehensive Logging** - Per-worker logs for debugging and monitoring
- **🛡️ Error Handling** - Robust error handling for invalid commands and execution failures

## 📋 Table of Contents

- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Usage Examples](#-usage-examples)
- [Web Dashboard](#-web-dashboard)
- [Architecture](#-architecture)
- [Job Lifecycle](#-job-lifecycle)
- [Configuration](#-configuration)
- [Testing](#-testing)
- [Contributing](#-contributing)

## 🚀 Installation

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)

### Install from Source

```bash

git clone https://github.com/Sriram24042/FLAM.git
cd FLAM


python -m pip install -e .


python -m pip install pytest

By default, data is stored in `~/.queuectl/` (or `%USERPROFILE%\.queuectl\` on Windows).

## 🎯 Quick Start

1. **Enqueue your first job:**
   ```bash
   python -m queuectl enqueue --command "echo 'Hello, QueueCTL!'" --id my-first-job
   ```

2. **Start a worker:**
   ```bash
   python -m queuectl worker start --count 1
   ```

3. **Check job status:**
   ```bash
   python -m queuectl status
   ```

4. **Launch web dashboard:**
   ```bash
   python -m queuectl web
   ```
   Then open http://127.0.0.1:5000 in your browser.

## 📖 Usage Examples

### Job Management

```bash
# Enqueue a job with JSON
python -m queuectl enqueue '{"command":"sleep 5", "max_retries":5}'

# Enqueue with command-line flags
python -m queuectl enqueue --command "python script.py" --id job-123 --max-retries 3

# List all jobs
python -m queuectl list

# List jobs by state
python -m queuectl list --state processing

# Delete a job
python -m queuectl delete job-123
```

### Worker Management

```bash
# Start multiple workers
python -m queuectl worker start --count 3

# Stop all workers
python -m queuectl worker stop

# Stop a specific worker
python -m queuectl worker stop --id worker-abc123

# Stop workers and wait for completion
python -m queuectl worker stop --wait --timeout 10

# View worker logs
python -m queuectl worker logs worker-abc123 --lines 50
```

### Dead Letter Queue (DLQ)

```bash
# List all failed jobs in DLQ
python -m queuectl dlq list

# Retry a job from DLQ
python -m queuectl dlq retry job-123

# Delete a job from DLQ
python -m queuectl dlq delete job-123
```

### Configuration

```bash
# List all configuration
python -m queuectl config list

# Get a specific config value
python -m queuectl config get max_retries_default

# Set configuration values
python -m queuectl config set max_retries_default 5
python -m queuectl config set backoff_base 3
python -m queuectl config set poll_interval 1.5
```

## 🌐 Web Dashboard

QueueCTL includes a modern web dashboard for visual job management:

```bash
python -m queuectl web --host 0.0.0.0 --port 5000
```

**Features:**
- 📊 Real-time job statistics
- 📝 Job listing with filtering
- ➕ Enqueue new jobs via web interface
- 🔄 Retry failed jobs from DLQ
- 🗑️ Delete jobs
- 👷 Worker management

The dashboard auto-refreshes every 3 seconds to show the latest job status.

## 🏗️ Architecture

QueueCTL is built with a modular architecture:

- **`queuectl.cli`** - Typer-based CLI interface for all operations
- **`queuectl.storage`** - SQLite persistence layer with WAL mode for concurrency
- **`queuectl.worker`** - Worker process implementation with job execution
- **`queuectl.web`** - Flask-based web dashboard and REST API
- **`queuectl.utils`** - Utility functions for timestamps, IDs, and JSON handling
- **`queuectl.constants`** - Configuration constants and path management

## 🔄 Job Lifecycle

1. **Enqueue** → Job is created in `pending` state
2. **Acquisition** → Worker atomically acquires job, transitions to `processing`
3. **Execution** → Command runs via system shell
4. **Success** → Job marked as `completed` with timestamp
5. **Failure** → Job marked as `failed`, retry scheduled with exponential backoff
6. **Retry** → After backoff delay, job becomes available again
7. **Max Retries Exceeded** → Job moved to `dead` state (DLQ)

### Exponential Backoff

Retry delays follow the formula: `delay = backoff_base ^ attempts`

- Attempt 1: `2^1 = 2 seconds`
- Attempt 2: `2^2 = 4 seconds`
- Attempt 3: `2^3 = 8 seconds`

## ⚙️ Configuration

### Default Settings

- `max_retries_default`: 3 (maximum retry attempts)
- `backoff_base`: 2 (exponential backoff multiplier)
- `poll_interval`: 2.0 (seconds between worker polls)

### Runtime Configuration

All settings can be adjusted at runtime without restarting workers:

```bash
python -m queuectl config set max_retries_default 5
python -m queuectl config set backoff_base 3
python -m queuectl config set poll_interval 1.0
```

## 🧪 Testing

Run the test suite:

```bash
# Install test dependencies
python -m pip install pytest

# Run all tests
python -m pytest

# Run with verbose output
python -m pytest -v
```

Tests use an isolated `QUEUECTL_HOME` directory and cover:
- Job lifecycle (enqueue → process → complete)
- Retry mechanism with exponential backoff
- Dead letter queue operations
- Worker management

## 🔧 Error Handling

QueueCTL handles various error scenarios:

- **Invalid Commands** - Commands that don't exist are caught and retried
- **Permission Errors** - Permission denied errors are logged and retried
- **Timeouts** - Commands exceeding 1 hour are automatically terminated
- **Execution Failures** - Non-zero exit codes trigger retry logic
- **Max Retries** - Jobs exceeding max retries are moved to DLQ

## 📁 Project Structure

```
FLAM/
├── queuectl/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py              
│   ├── constants.py        
│   ├── storage.py          # SQLite persistence
│   ├── utils.py            # Utility functions
│   ├── web.py              # Web dashboard
│   ├── worker.py           # Worker implementation
│   └── templates/
│       └── dashboard.html   # Web UI
├── tests/
│   └── test_basic.py       
├── pyproject.toml          # Project configuration
└── README.md               # This file
```

