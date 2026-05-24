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

    async def _hmset(self, key: str, mapping: dict) -> None:
        """Pipeline individual HSET calls — compatible with Redis 3.2+."""
        pipe = self.client.pipeline()
        for field, value in mapping.items():
            pipe.hset(key, field, value)
        await pipe.execute()

    # --- Position state ---
    async def set_position(self, gateway: str, symbol: str, strategy: str,
                           position: dict) -> None:
        key = f"position:{gateway}:{symbol}:{strategy}"
        await self._hmset(key, position)

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
        await self._hmset(key, {
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

    async def get_all_pnl_snapshots(self) -> list[dict]:
        """Return the latest PnL snapshot for every strategy (written by risk engine)."""
        keys = await self.client.keys("pnl:*")
        snapshots = []
        for key in keys:
            raw = await self.client.get(key)
            if raw:
                try:
                    snapshots.append(json.loads(raw))
                except (json.JSONDecodeError, TypeError):
                    pass
        return snapshots

    async def get_account_positions(self) -> list[dict]:
        """Return raw exchange positions from all account:*:positions keys.

        Each entry is the raw dict the gateway wrote, augmented with a
        '_gateway' field derived from the key name.
        """
        keys = await self.client.keys("account:*:positions")
        positions = []
        for key in keys:
            raw = await self.client.get(key)
            if not raw:
                continue
            try:
                entries = json.loads(raw)
                # key format: account:{gateway_name}:positions
                gateway = key.split(":")[1] if key.count(":") >= 2 else key
                for entry in entries:
                    if float(entry.get("pos", 0)) != 0:
                        entry["_gateway"] = gateway
                        positions.append(entry)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        return positions

    # --- Order history ---

    async def get_order_updates(self, gateway: str = "okx",
                                count: int = 200,
                                start: str = "-", end: str = "+") -> list[dict]:
        """Full audit log — every state change for every order (open, partial, filled…)."""
        key = f"order_updates:{gateway}"
        entries = await self.client.xrange(key, start, end, count=count)
        return [{"_stream_id": e[0], **e[1]} for e in entries]

    async def get_finished_orders(self, gateway: str = "okx",
                                  count: int = 100) -> list[dict]:
        """Terminal orders only (filled / cancelled / rejected)."""
        entries = await self.client.xrange("finished_orders", "-", "+", count=count)
        return [{"_stream_id": e[0], **e[1]} for e in entries]

    async def get_fills(self, gateway: str = "okx",
                        count: int = 200) -> list[dict]:
        """Individual executions — each entry has a unique fill_id."""
        key = f"fills:{gateway}"
        entries = await self.client.xrange(key, "-", "+", count=count)
        return [{"_stream_id": e[0], **e[1]} for e in entries]

    async def get_pending_orders(self, gateway: str = "okx") -> list[dict]:
        """Live snapshot of non-terminal orders."""
        raw = await self.client.get(f"pending_orders:{gateway}")
        return json.loads(raw) if raw else []

    # --- Heartbeat tracking ---
    async def set_heartbeat(self, process_name: str, info: dict) -> None:
        key = f"heartbeat:{process_name}"
        await self._hmset(key, info)
        await self.client.expire(key, 30)  # expires if no heartbeat for 30s

    async def get_all_heartbeats(self) -> dict[str, dict]:
        keys = await self.client.keys("heartbeat:*")
        result = {}
        for key in keys:
            name = key.replace("heartbeat:", "")
            result[name] = await self.client.hgetall(key)
        return result
