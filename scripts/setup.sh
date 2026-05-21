#!/usr/bin/env bash
#
# Setup script for mytradingbot.
# Works on macOS (Homebrew) and Ubuntu/Debian (apt).
#
# Usage:
#   bash scripts/setup.sh          # full setup
#   bash scripts/setup.sh --check  # verify environment only
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[OK]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; }

# ── Detect OS ──────────────────────────────────────────────
detect_os() {
    case "$(uname -s)" in
        Darwin) echo "macos" ;;
        Linux)  echo "linux" ;;
        *)      echo "unknown" ;;
    esac
}

OS=$(detect_os)

# ── Check / Install Python ─────────────────────────────────
check_python() {
    if command -v "$PYTHON" &>/dev/null; then
        PY_VERSION=$("$PYTHON" --version 2>&1 | awk '{print $2}')
        info "Python $PY_VERSION found at $(command -v $PYTHON)"
    else
        fail "Python3 not found. Install Python 3.9+ first."
        exit 1
    fi
}

# ── Check / Install Redis ──────────────────────────────────
install_redis() {
    if command -v redis-server &>/dev/null; then
        info "Redis already installed: $(redis-server --version | head -1)"
    else
        warn "Redis not found, installing..."
        if [ "$OS" = "macos" ]; then
            brew install redis
        elif [ "$OS" = "linux" ]; then
            sudo apt-get update -qq && sudo apt-get install -y redis-server
        else
            fail "Unknown OS. Install Redis manually."
            exit 1
        fi
        info "Redis installed."
    fi
}

start_redis() {
    if redis-cli ping &>/dev/null 2>&1; then
        info "Redis is running."
    else
        warn "Starting Redis..."
        if [ "$OS" = "macos" ]; then
            brew services start redis
        elif [ "$OS" = "linux" ]; then
            sudo systemctl enable redis-server
            sudo systemctl start redis-server
        fi
        sleep 1
        if redis-cli ping &>/dev/null 2>&1; then
            info "Redis started successfully."
        else
            fail "Failed to start Redis."
            exit 1
        fi
    fi
}

# ── Install Python dependencies ────────────────────────────
install_python_deps() {
    info "Installing Python dependencies..."
    "$PYTHON" -m pip install --upgrade pip -q
    "$PYTHON" -m pip install -r "$REPO_DIR/requirements.txt" -q
    info "Python dependencies installed."
}

# ── Verify all imports work ────────────────────────────────
verify_imports() {
    "$PYTHON" -c "
import zmq, msgpack, redis, aiohttp, yaml
print('All core packages OK')
" 2>&1 && info "All Python packages importable." || fail "Package import check failed."
}

# ── Create .env template ───────────────────────────────────
create_env_template() {
    ENV_FILE="$REPO_DIR/.env"
    if [ -f "$ENV_FILE" ]; then
        info ".env file already exists. Skipping."
    else
        cat > "$ENV_FILE" <<'ENVEOF'
# OKX API (demo trading)
OKX_API_KEY=
OKX_SECRET_KEY=
OKX_PASSPHRASE=

# FUTU OpenD (optional)
FUTU_UNLOCK_PWD=

# Webull (optional — market-data-only without these)
WEBULL_EMAIL=
WEBULL_PASSWORD=
WEBULL_TRADE_PIN=
WEBULL_MFA_SEED=
WEBULL_DID=

# Telegram Bot (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
ENVEOF
        warn ".env file created at $ENV_FILE — fill in your credentials."
    fi
}

# ── Create required directories ────────────────────────────
create_dirs() {
    mkdir -p "$REPO_DIR/logs" "$REPO_DIR/.pids"
    info "Created logs/ and .pids/ directories."
}

# ── Run tests ──────────────────────────────────────────────
run_tests() {
    info "Running test suite..."
    cd "$REPO_DIR"
    "$PYTHON" -m pytest tests/ -v --tb=short
}

# ── Check-only mode ────────────────────────────────────────
check_only() {
    echo "=== Environment Check ==="
    check_python

    if command -v redis-server &>/dev/null; then
        info "Redis installed."
    else
        fail "Redis not installed."
    fi

    if redis-cli ping &>/dev/null 2>&1; then
        info "Redis running."
    else
        fail "Redis not running."
    fi

    verify_imports

    if [ -f "$REPO_DIR/.env" ]; then
        info ".env file exists."
        # Check if credentials are filled
        if grep -q "^OKX_API_KEY=$" "$REPO_DIR/.env"; then
            warn "OKX_API_KEY is empty in .env"
        fi
    else
        warn ".env file not found."
    fi

    echo ""
    echo "=== Process Status ==="
    cd "$REPO_DIR"
    "$PYTHON" supervisor.py status 2>/dev/null || warn "Supervisor not runnable (check Python path)."
}

# ── Main ───────────────────────────────────────────────────
main() {
    echo "=============================="
    echo " mytradingbot setup"
    echo " OS: $OS"
    echo "=============================="
    echo ""

    if [ "${1:-}" = "--check" ]; then
        check_only
        exit 0
    fi

    check_python
    install_redis
    start_redis
    install_python_deps
    verify_imports
    create_env_template
    create_dirs
    run_tests

    echo ""
    echo "=============================="
    echo " Setup complete!"
    echo "=============================="
    echo ""
    echo "Next steps:"
    echo "  1. Edit .env with your OKX demo API keys"
    echo "  2. Source env:     source .env && export \$(grep -v '^#' .env | xargs)"
    echo "  3. Start system:  python3 supervisor.py start all"
    echo "  4. Check status:  python3 supervisor.py status"
    echo ""
}

main "$@"
