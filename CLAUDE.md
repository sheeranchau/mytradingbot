# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-process Python trading system for crypto (OKX) and equities (FUTU — US/HK markets). Processes communicate via ZeroMQ (hot path: ticks, orders, kills) and Redis (warm path: positions, config, trade log, PnL).

## Setup & Deployment

```bash
# Full setup (works on macOS and Ubuntu/Debian)
bash scripts/setup.sh

# Verify environment without installing
bash scripts/setup.sh --check
```

The setup script handles: Python check, Redis install+start, pip dependencies, .env template, directory creation, and runs tests. See `scripts/setup.sh`.

## Commands

```bash

# Process management (supervisor)
python supervisor.py start all          # start all processes in order
python supervisor.py stop all           # stop all (reverse order)
python supervisor.py start gateway_okx  # start single process
python supervisor.py stop risk_engine
python supervisor.py restart strategy_container
python supervisor.py status             # show running/stopped + uptime

# Run a single process directly (for debugging)
python -m risk.runner
python -m gateway.okx_runner
python -m strategy.runner
python -m view.telegram_runner

# Tests
python3 -m pytest tests/ -v              # all tests
python3 -m pytest tests/ -v -k "not redis"  # skip Redis-dependent tests
python3 -m pytest tests/test_risk_engine.py -v  # single file
```

## Trading Mode

Configured per gateway in `config/settings.yaml`:
- **OKX**: `simulated: true` = demo (`wspap.okx.com`), `false` = REAL MONEY
- **FUTU**: `trade_env: "SIMULATE"` = paper, `"REAL"` = REAL MONEY (requires unlock password)

## Architecture

### Process Topology & ZMQ Ports

```
gateway_okx/futu ──PUB:15555──> [market data] ──SUB──> strategy_container, risk_engine, view
strategy_container ──PUSH:15556──> [signals] ──PULL──> risk_engine
risk_engine ──PUSH:15557──> [orders] ──PULL──> gateway_okx/futu
risk_engine ──PUB:15558──> [kill signals] ──SUB──> strategy_container, view
all processes ──PUB:15559──> [heartbeat] ──SUB──> supervisor (via Redis TTL)
```

### Key Data Flow

1. Gateway receives exchange WS data → normalizes → publishes `tick.<gateway>.<symbol>` on ZMQ PUB
2. Strategy container subscribes, routes to matching strategies, gets signal back
3. Signal pushed to risk engine via PUSH/PULL pipeline
4. Risk engine validates (limits from Redis) → forwards as order to gateway, or kills strategy
5. Gateway executes order → publishes fill event
6. View subscribes to all events for Telegram/web reporting

### Component Responsibilities

- **core/process_base.py** — Base class all processes inherit. Handles heartbeat (ZMQ + Redis), signal handlers, graceful shutdown.
- **core/zmq_channels.py** — Publisher, Subscriber, Pusher, Puller wrappers. Port constants defined here.
- **core/redis_store.py** — Shared state: positions, risk limits, strategy enabled/disabled, trade log (Streams), PnL snapshots.
- **gateway/base.py** — Abstract gateway. Publishes ticks/orderbooks/fills, pulls orders.
- **strategy/base.py** — Abstract strategy. Receives ticks, returns signal dicts.
- **strategy/container.py** — Routes market data to strategies by (gateway, symbol). Listens for kill signals.
- **risk/engine.py** — Pre-trade checks + real-time PnL/delta monitoring. Can kill strategies and flatten.
- **view/telegram.py** — Reports fills, risk kills, periodic summaries. Supports /status /positions /stop commands.
- **supervisor.py** — Start/stop/restart/status for all processes. Uses pidfiles + `os.kill` for monitoring.

### Configuration

All config in `config/settings.yaml`. Secrets via environment variables (OKX_API_KEY, TELEGRAM_BOT_TOKEN, etc).

### Adding a New Strategy

1. Create class inheriting `strategy.base.BaseStrategy`
2. Implement `on_tick()` returning signal dict or None
3. Register in `config/settings.yaml` under `strategies`
4. Optionally add risk limits under `risk.<strategy_name>`
