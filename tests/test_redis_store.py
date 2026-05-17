"""
Tests for Redis shared state store.
Requires a running Redis instance. Skipped if Redis is unavailable.
Uses db=15 to avoid clobbering real data.
"""
import pytest
from tests.conftest import requires_redis


@requires_redis
class TestRedisStorePositions:

    @pytest.mark.asyncio
    async def test_set_and_get_position(self, redis_store):
        await redis_store.set_position("okx", "BTC-USDT-SWAP", "momentum", {
            "quantity": "0.5",
            "avg_price": "50000",
            "unrealized_pnl": "100",
        })
        pos = await redis_store.get_position("okx", "BTC-USDT-SWAP", "momentum")
        assert pos["quantity"] == "0.5"
        assert pos["avg_price"] == "50000"

    @pytest.mark.asyncio
    async def test_get_all_positions(self, redis_store):
        await redis_store.set_position("okx", "BTC", "s1", {"quantity": "1"})
        await redis_store.set_position("futu", "AAPL", "s2", {"quantity": "100"})

        positions = await redis_store.get_all_positions()
        assert len(positions) == 2

    @pytest.mark.asyncio
    async def test_get_position_nonexistent(self, redis_store):
        pos = await redis_store.get_position("okx", "FAKE", "none")
        assert pos == {}


@requires_redis
class TestRedisStoreTradeLog:

    @pytest.mark.asyncio
    async def test_log_and_get_trades(self, redis_store):
        await redis_store.log_trade({"symbol": "BTC", "side": "buy", "quantity": "0.1"})
        await redis_store.log_trade({"symbol": "BTC", "side": "sell", "quantity": "0.1"})

        trades = await redis_store.get_trades(count=10)
        assert len(trades) == 2

    @pytest.mark.asyncio
    async def test_trade_log_ordering(self, redis_store):
        await redis_store.log_trade({"seq": "1"})
        await redis_store.log_trade({"seq": "2"})
        await redis_store.log_trade({"seq": "3"})

        trades = await redis_store.get_trades(count=10)
        seqs = [t[1]["seq"] for t in trades]
        assert seqs == ["1", "2", "3"]


@requires_redis
class TestRedisStoreRiskLimits:

    @pytest.mark.asyncio
    async def test_set_and_get_risk_limits(self, redis_store):
        await redis_store.set_risk_limits("momentum", {
            "max_notional": "50000",
            "max_daily_loss": "1000",
        })
        limits = await redis_store.get_risk_limits("momentum")
        assert limits["max_notional"] == "50000"
        assert limits["max_daily_loss"] == "1000"

    @pytest.mark.asyncio
    async def test_get_risk_limits_nonexistent(self, redis_store):
        limits = await redis_store.get_risk_limits("no_such_strategy")
        assert limits == {}


@requires_redis
class TestRedisStoreStrategyState:

    @pytest.mark.asyncio
    async def test_enable_disable_strategy(self, redis_store):
        await redis_store.set_strategy_enabled("momentum", False)
        assert await redis_store.is_strategy_enabled("momentum") is False

        await redis_store.set_strategy_enabled("momentum", True)
        assert await redis_store.is_strategy_enabled("momentum") is True

    @pytest.mark.asyncio
    async def test_default_is_enabled(self, redis_store):
        # Never set => default True
        assert await redis_store.is_strategy_enabled("unknown") is True


@requires_redis
class TestRedisStorePnL:

    @pytest.mark.asyncio
    async def test_snapshot_and_get_pnl(self, redis_store):
        await redis_store.snapshot_pnl("momentum", {"daily_pnl": "100"})
        await redis_store.snapshot_pnl("momentum", {"daily_pnl": "150"})

        history = await redis_store.get_pnl_history("momentum", count=10)
        assert len(history) == 2
        assert history[0][1]["daily_pnl"] == "100"
        assert history[1][1]["daily_pnl"] == "150"


@requires_redis
class TestRedisStoreHeartbeat:

    @pytest.mark.asyncio
    async def test_set_and_get_heartbeats(self, redis_store):
        await redis_store.set_heartbeat("gateway_okx", {
            "pid": "12345", "uptime": "3600",
        })
        await redis_store.set_heartbeat("risk_engine", {
            "pid": "12346", "uptime": "3500",
        })

        heartbeats = await redis_store.get_all_heartbeats()
        assert "gateway_okx" in heartbeats
        assert heartbeats["gateway_okx"]["pid"] == "12345"
        assert "risk_engine" in heartbeats

    @pytest.mark.asyncio
    async def test_heartbeat_has_ttl(self, redis_store):
        await redis_store.set_heartbeat("test_proc", {"pid": "1"})
        ttl = await redis_store.client.ttl("heartbeat:test_proc")
        assert 0 < ttl <= 30
