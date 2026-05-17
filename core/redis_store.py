"""
Redis shared state store.
Used for: positions, config, trade log, PnL snapshots.
"""
import json
from typing import Optional
import redis.asyncio as aioredis


class RedisStore:
    """Async Redis wrapper for shared trading state."""

    def __init__(self, url: str = "redis://127.0.0.1:6379", db: int = 0):
        self.url = url
        self.db = db
        self._client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self.url, db=self.db, decode_responses=True)
        await self._client.ping()

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> aioredis.Redis:
        if not self._client:
            raise RuntimeError("RedisStore not connected")
        return self._client

    # --- Position state ---
    async def set_position(self, gateway: str, symbol: str, strategy: str,
                           position: dict) -> None:
        key = f"position:{gateway}:{symbol}:{strategy}"
        await self.client.hset(key, mapping=position)

    async def get_position(self, gateway: str, symbol: str, strategy: str) -> dict:
        key = f"position:{gateway}:{symbol}:{strategy}"
        return await self.client.hgetall(key)

    async def get_all_positions(self) -> list[dict]:
        keys = await self.client.keys("position:*")
        positions = []
        for key in keys:
            pos = await self.client.hgetall(key)
            pos["_key"] = key
            positions.append(pos)
        return positions

    # --- Trade log (Redis Streams) ---
    async def log_trade(self, trade: dict) -> str:
        return await self.client.xadd("trades", trade, maxlen=10000)

    async def get_trades(self, count: int = 100, start: str = "-",
                         end: str = "+") -> list:
        return await self.client.xrange("trades", start, end, count=count)

    # --- Risk config ---
    async def set_risk_limits(self, strategy: str, limits: dict) -> None:
        key = f"risk_limits:{strategy}"
        await self.client.hset(key, mapping={
            k: json.dumps(v) if not isinstance(v, str) else v
            for k, v in limits.items()
        })

    async def get_risk_limits(self, strategy: str) -> dict:
        key = f"risk_limits:{strategy}"
        raw = await self.client.hgetall(key)
        return {k: json.loads(v) if v.startswith(("{", "[")) else v
                for k, v in raw.items()} if raw else {}

    # --- Strategy state ---
    async def set_strategy_enabled(self, strategy: str, enabled: bool) -> None:
        await self.client.hset("strategy_state", strategy, str(int(enabled)))

    async def is_strategy_enabled(self, strategy: str) -> bool:
        val = await self.client.hget("strategy_state", strategy)
        return val == "1" if val else True

    # --- PnL snapshots ---
    async def snapshot_pnl(self, strategy: str, pnl: dict) -> None:
        key = f"pnl:{strategy}"
        await self.client.xadd(key, pnl, maxlen=5000)

    async def get_pnl_history(self, strategy: str, count: int = 100) -> list:
        key = f"pnl:{strategy}"
        return await self.client.xrange(key, "-", "+", count=count)

    # --- Heartbeat tracking ---
    async def set_heartbeat(self, process_name: str, info: dict) -> None:
        key = f"heartbeat:{process_name}"
        await self.client.hset(key, mapping=info)
        await self.client.expire(key, 30)  # expires if no heartbeat for 30s

    async def get_all_heartbeats(self) -> dict[str, dict]:
        keys = await self.client.keys("heartbeat:*")
        result = {}
        for key in keys:
            name = key.replace("heartbeat:", "")
            result[name] = await self.client.hgetall(key)
        return result
