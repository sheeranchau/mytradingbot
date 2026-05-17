"""
Process supervisor for the trading system.
Start, stop, monitor, and restart individual processes.

Usage:
    python supervisor.py start all
    python supervisor.py start gateway_okx
    python supervisor.py stop risk_engine
    python supervisor.py stop all
    python supervisor.py status
    python supervisor.py restart strategy_container
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PIDFILE_DIR = Path(__file__).parent / ".pids"
LOG_DIR = Path(__file__).parent / "logs"

# Process registry: name -> module entry point
PROCESS_REGISTRY = {
    "gateway_okx": "gateway.okx_runner",
    "gateway_futu": "gateway.futu_runner",
    "strategy_container": "strategy.runner",
    "risk_engine": "risk.runner",
    "view_telegram": "view.telegram_runner",
}

# Startup order matters: risk first, then gateways, then strategy, then view
STARTUP_ORDER = [
    "risk_engine",
    "gateway_okx",
    "gateway_futu",
    "strategy_container",
    "view_telegram",
]


def ensure_dirs():
    PIDFILE_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)


def get_pidfile(name: str) -> Path:
    return PIDFILE_DIR / f"{name}.pid"


def read_pid(name: str) -> int | None:
    pidfile = get_pidfile(name)
    if pidfile.exists():
        try:
            return int(pidfile.read_text().strip())
        except ValueError:
            return None
    return None


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def process_status(name: str) -> dict:
    """Get status of a single process."""
    pid = read_pid(name)
    if pid and is_running(pid):
        # Read uptime from log or calculate from pidfile mtime
        pidfile = get_pidfile(name)
        uptime = time.time() - pidfile.stat().st_mtime
        return {"name": name, "pid": pid, "status": "running", "uptime": uptime}
    else:
        # Clean stale pidfile
        pidfile = get_pidfile(name)
        if pidfile.exists():
            pidfile.unlink()
        return {"name": name, "pid": None, "status": "stopped", "uptime": 0}


def start_process(name: str) -> bool:
    """Start a single process in the background."""
    if name not in PROCESS_REGISTRY:
        print(f"Unknown process: {name}")
        return False

    info = process_status(name)
    if info["status"] == "running":
        print(f"{name} is already running (pid={info['pid']})")
        return True

    module = PROCESS_REGISTRY[name]
    log_file = LOG_DIR / f"{name}.log"

    # Start as a detached subprocess
    with open(log_file, "a") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", module],
            cwd=str(Path(__file__).parent),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    # Write pidfile
    get_pidfile(name).write_text(str(proc.pid))
    print(f"Started {name} (pid={proc.pid}), log: {log_file}")
    return True


def stop_process(name: str, timeout: int = 10) -> bool:
    """Stop a process gracefully (SIGTERM), then SIGKILL if needed."""
    info = process_status(name)
    if info["status"] != "running":
        print(f"{name} is not running")
        return True

    pid = info["pid"]
    print(f"Stopping {name} (pid={pid})...", end=" ")

    # Send SIGTERM
    os.kill(pid, signal.SIGTERM)

    # Wait for exit
    for _ in range(timeout * 10):
        if not is_running(pid):
            break
        time.sleep(0.1)

    if is_running(pid):
        print("force killing...", end=" ")
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)

    # Clean pidfile
    pidfile = get_pidfile(name)
    if pidfile.exists():
        pidfile.unlink()

    print("stopped")
    return True


def show_status():
    """Show status of all processes in a table."""
    print(f"\n{'Process':<25} {'PID':<10} {'Status':<12} {'Uptime':<15}")
    print("-" * 62)

    all_healthy = True
    for name in STARTUP_ORDER:
        info = process_status(name)
        pid_str = str(info["pid"]) if info["pid"] else "-"
        uptime_str = _format_uptime(info["uptime"]) if info["status"] == "running" else "-"
        status = info["status"]

        if status == "running":
            status_display = "running"
        else:
            status_display = "stopped"
            all_healthy = False

        print(f"{name:<25} {pid_str:<10} {status_display:<12} {uptime_str:<15}")

    print("-" * 62)

    # Also check Redis for heartbeat info
    try:
        import redis
        r = redis.Redis(decode_responses=True)
        r.ping()
        heartbeat_keys = r.keys("heartbeat:*")
        if heartbeat_keys:
            print(f"\nRedis heartbeats ({len(heartbeat_keys)}):")
            for key in sorted(heartbeat_keys):
                hb = r.hgetall(key)
                name = key.replace("heartbeat:", "")
                ttl = r.ttl(key)
                print(f"  {name}: pid={hb.get('pid', '?')}, "
                      f"uptime={hb.get('uptime', '?')}s, "
                      f"ttl={ttl}s")
        r.close()
    except Exception:
        print("\nRedis: not reachable (heartbeat data unavailable)")

    print()
    return all_healthy


def _format_uptime(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    elif seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    else:
        return f"{seconds / 86400:.1f}d"


def main():
    ensure_dirs()

    parser = argparse.ArgumentParser(description="Trading system process supervisor")
    parser.add_argument("action", choices=["start", "stop", "restart", "status"],
                        help="Action to perform")
    parser.add_argument("process", nargs="?", default=None,
                        help="Process name or 'all'")
    args = parser.parse_args()

    if args.action == "status":
        show_status()
        return

    if not args.process:
        parser.error(f"'{args.action}' requires a process name or 'all'")

    if args.process == "all":
        processes = STARTUP_ORDER
    elif args.process in PROCESS_REGISTRY:
        processes = [args.process]
    else:
        print(f"Unknown process: {args.process}")
        print(f"Available: {', '.join(STARTUP_ORDER)} or 'all'")
        sys.exit(1)

    if args.action == "start":
        for name in processes:
            start_process(name)
    elif args.action == "stop":
        # Stop in reverse order
        for name in reversed(processes):
            stop_process(name)
    elif args.action == "restart":
        for name in reversed(processes):
            stop_process(name)
        time.sleep(1)
        for name in processes:
            start_process(name)


if __name__ == "__main__":
    main()
