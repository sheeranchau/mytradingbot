"""
Web dashboard API server.
Serves REST endpoints and WebSocket for live updates.
"""
import asyncio
import json
import os
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import redis.asyncio as aioredis

from core.config import load_env

load_env()

STATIC_DIR = Path(__file__).parent / "static"
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

app = FastAPI(title="Trading Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_redis = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


@app.on_event("shutdown")
async def shutdown():
    global _redis
    if _redis:
        await _redis.aclose()


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/strategies")
async def get_strategies():
    """Get all strategy states."""
    r = await get_redis()
    raw = await r.get("dashboard:strategies")
    return json.loads(raw) if raw else {}


@app.get("/api/prices")
async def get_prices():
    """Get latest market prices."""
    r = await get_redis()
    raw = await r.get("dashboard:prices")
    return json.loads(raw) if raw else {}


@app.get("/api/positions")
async def get_positions():
    """Get all positions from Redis."""
    r = await get_redis()
    keys = await r.keys("position:*")
    positions = []
    for key in keys:
        pos = await r.hgetall(key)
        pos["_key"] = key
        positions.append(pos)
    return positions


@app.get("/api/trades")
async def get_trades(count: int = 50):
    """Get recent trade history."""
    r = await get_redis()
    trades = await r.xrange("trades", "-", "+", count=count)
    return [{"id": t[0], **t[1]} for t in trades]


@app.get("/api/heartbeats")
async def get_heartbeats():
    """Get process heartbeats."""
    r = await get_redis()
    keys = await r.keys("heartbeat:*")
    result = {}
    for key in keys:
        name = key.replace("heartbeat:", "")
        hb = await r.hgetall(key)
        ttl = await r.ttl(key)
        hb["ttl"] = ttl
        result[name] = hb
    return result


@app.get("/api/pnl/{strategy}")
async def get_pnl(strategy: str, count: int = 100):
    """Get PnL history for a strategy."""
    r = await get_redis()
    key = f"pnl:{strategy}"
    history = await r.xrange(key, "-", "+", count=count)
    return [{"id": h[0], **h[1]} for h in history]


@app.post("/api/strategy/{name}/params")
async def update_params(name: str, params: dict):
    """Update strategy parameters at runtime."""
    r = await get_redis()
    cmd = json.dumps({
        "action": "update_params",
        "strategy": name,
        "params": params,
    })
    await r.publish("strategy:command", cmd)
    return {"status": "ok", "message": f"Params update sent to {name}"}


@app.post("/api/strategy/{name}/enable")
async def enable_strategy(name: str):
    r = await get_redis()
    cmd = json.dumps({"action": "enable", "strategy": name})
    await r.publish("strategy:command", cmd)
    return {"status": "ok", "message": f"{name} enabled"}


@app.post("/api/strategy/{name}/disable")
async def disable_strategy(name: str):
    r = await get_redis()
    cmd = json.dumps({"action": "disable", "strategy": name})
    await r.publish("strategy:command", cmd)
    return {"status": "ok", "message": f"{name} disabled"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for live dashboard updates. Pushes state every 2 seconds."""
    await websocket.accept()
    r = await get_redis()
    try:
        while True:
            strategies = await r.get("dashboard:strategies")
            prices = await r.get("dashboard:prices")
            heartbeat_keys = await r.keys("heartbeat:*")
            heartbeats = {}
            for key in heartbeat_keys:
                name = key.replace("heartbeat:", "")
                heartbeats[name] = await r.hgetall(key)

            await websocket.send_json({
                "strategies": json.loads(strategies) if strategies else {},
                "prices": json.loads(prices) if prices else {},
                "heartbeats": heartbeats,
                "timestamp": asyncio.get_event_loop().time(),
            })
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
