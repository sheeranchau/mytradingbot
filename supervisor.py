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


def _load_env_file(path: Path) -> dict:
    """Parse a .env file and return a dict of key=value pairs."""
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip()
    return env

PIDFILE_DIR = Path(__file__).parent / ".pids"
LOG_DIR = Path(__file__).parent / "logs"

# Process registry: name -> module entry point
PROCESS_REGISTRY = {
    "gateway_okx": "gateway.okx_runner",
    "gateway_futu": "gateway.futu_runner",
    "gateway_webull": "gateway.webull_runner",
    "strategy_container": "strategy.runner",
    "risk_engine": "risk.runner",
    "view_telegram": "view.telegram_runner",
    "view_web": "view.web.runner",
}

# Startup order matters: risk first, then gateways, then strategy, then view
STARTUP_ORDER = [
    "risk_engine",
    "gateway_okx",
    "gateway_futu",
    "gateway_webull",
    "strategy_container",
    "view_telegram",
    "view_web",
]


def ensure_dirs():
    PIDFILE_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)


def get_pidfile(name: str) -> Path:
    return PIDFILE_DIR / f"{name}.pid"


def read_pid(name: str):
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

    # Build env: current environment + .env file overrides
    child_env = os.environ.copy()
    dot_env = _load_env_file(Path(__file__).parent / ".env")
    child_env.update(dot_env)

    # Start as a detached subprocess
    with open(log_file, "a") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", module],
            cwd=str(Path(__file__).parent),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=child_env,
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


# ── ANSI colour helpers ────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_DIM    = "\033[2m"
_RED    = "\033[91m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_CYAN   = "\033[96m"
_WHITE  = "\033[97m"

def _c(text, *codes):
    return "".join(codes) + str(text) + _RESET

def _ttl_color(ttl: int) -> str:
    """Colour TTL: green ≥ 20s, yellow 10–19s, red < 10s."""
    if ttl >= 20:
        return _c(f"{ttl}s", _GREEN)
    elif ttl >= 10:
        return _c(f"{ttl}s", _YELLOW)
    else:
        return _c(f"{ttl}s", _RED)


def show_status():
    """Show status of all processes in a colour table."""
    header = (f"\n{_c('Process', _BOLD, _CYAN):<34} "
              f"{_c('PID', _BOLD, _CYAN):<19} "
              f"{_c('Status', _BOLD, _CYAN):<21} "
              f"{_c('Uptime', _BOLD, _CYAN)}")
    divider = _c("─" * 62, _DIM)
    print(header)
    print(divider)

    all_healthy = True
    for name in STARTUP_ORDER:
        info = process_status(name)
        pid_str    = _c(str(info["pid"]), _YELLOW) if info["pid"] else _c("-", _DIM)
        uptime_str = _c(_format_uptime(info["uptime"]), _WHITE) if info["status"] == "running" else _c("-", _DIM)

        if info["status"] == "running":
            status_display = _c("● running", _GREEN, _BOLD)
        else:
            status_display = _c("○ stopped", _RED)
            all_healthy = False

        # pad accounting: colour codes add invisible chars, adjust manually
        name_col   = f"{_c(name, _WHITE):<{25 + len(_WHITE) + len(_RESET)}}"
        pid_col    = f"{pid_str:<{10 + len(_YELLOW) + len(_RESET)}}"
        status_col = f"{status_display:<{12 + len(_GREEN) + len(_BOLD) + len(_RESET)}}"
        print(f"  {name_col} {pid_col} {status_col} {uptime_str}")

    print(divider)

    # ── Redis heartbeat section ────────────────────────────────────────────────
    try:
        import redis
        _redis_pw  = os.environ.get("REDIS_PASSWORD", "")
        _redis_url = (
            f"redis://default:{_redis_pw}@127.0.0.1:6379"
            if _redis_pw else "redis://127.0.0.1:6379"
        )
        r = redis.from_url(_redis_url, decode_responses=True)
        r.ping()
        redis_label = _c("Redis", _BOLD, _GREEN) + _c(" ✓  heartbeats:", _GREEN)
        heartbeat_keys = r.keys("heartbeat:*")
        if heartbeat_keys:
            print(f"\n{redis_label}")
            for key in sorted(heartbeat_keys):
                hb  = r.hgetall(key)
                hb_name = key.replace("heartbeat:", "")
                ttl = r.ttl(key)
                uptime_raw = hb.get("uptime", "?")
                try:
                    uptime_fmt = _format_uptime(float(uptime_raw))
                except (ValueError, TypeError):
                    uptime_fmt = f"{uptime_raw}s"
                print(f"  {_c(hb_name, _CYAN):<{20 + len(_CYAN) + len(_RESET)}}  "
                      f"pid={_c(hb.get('pid', '?'), _YELLOW)}  "
                      f"up={_c(uptime_fmt, _WHITE)}  "
                      f"ttl={_ttl_color(ttl)}")
        else:
            print(f"\n{redis_label} {_c('(no heartbeat keys yet)', _DIM)}")
        r.close()
    except Exception as exc:
        print(f"\n{_c('Redis', _BOLD, _RED)} {_c('✗  not reachable', _RED)} "
              f"{_c(f'({exc})', _DIM)}")

    print()


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
