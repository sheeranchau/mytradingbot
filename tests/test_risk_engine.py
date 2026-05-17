"""
Tests for risk engine signal validation.
Extracts _check_signal logic to test without ZMQ/Redis by mocking the redis store.
"""
import time
import pytest
from unittest.mock import AsyncMock, MagicMock
from risk.engine import RiskEngine, StrategyRiskState


class MockRedisStore:
    """Minimal mock that returns configurable risk limits."""

    def __init__(self, limits: dict = None):
        self._limits = limits or {}
        self._enabled = {}
        self._killed = []

    async def get_risk_limits(self, strategy: str) -> dict:
        return self._limits.get(strategy, {})

    async def set_strategy_enabled(self, strategy: str, enabled: bool) -> None:
        self._enabled[strategy] = enabled

    async def is_strategy_enabled(self, strategy: str) -> bool:
        return self._enabled.get(strategy, True)

    async def log_trade(self, trade: dict) -> str:
        return "mock-id"

    async def snapshot_pnl(self, strategy: str, pnl: dict) -> None:
        pass

    async def connect(self):
        pass

    async def close(self):
        pass


def make_engine(limits: dict = None) -> RiskEngine:
    """Create a RiskEngine with mocked Redis."""
    engine = RiskEngine.__new__(RiskEngine)
    engine.process_name = "risk_engine"
    engine.running = True
    engine.redis = MockRedisStore(limits)
    engine._strategy_states = {}
    return engine


def make_signal(strategy="test_strat", price=50000.0, quantity=0.01,
                side="buy", gateway="okx", symbol="BTC-USDT-SWAP") -> dict:
    return {
        "event_type": "signal",
        "strategy": strategy,
        "gateway": gateway,
        "symbol": symbol,
        "side": side,
        "order_type": "limit",
        "price": price,
        "quantity": quantity,
        "timestamp": time.time(),
    }


class TestRiskCheckSignal:
    """Test pre-trade risk checks."""

    @pytest.mark.asyncio
    async def test_pass_through_when_no_limits_configured(self):
        engine = make_engine(limits={})
        kill_pub = AsyncMock()
        signal = make_signal()

        result = await engine._check_signal(signal, kill_pub)
        assert result is True

    @pytest.mark.asyncio
    async def test_reject_when_strategy_disabled(self):
        engine = make_engine()
        state = engine._get_state("test_strat")
        state.enabled = False
        kill_pub = AsyncMock()

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False

    @pytest.mark.asyncio
    async def test_reject_notional_exceeds_limit(self):
        engine = make_engine(limits={
            "test_strat": {"max_notional": "1000"}
        })
        kill_pub = AsyncMock()
        # notional = 50000 * 0.1 = 5000 > 1000
        signal = make_signal(price=50000.0, quantity=0.1)

        result = await engine._check_signal(signal, kill_pub)
        assert result is False

    @pytest.mark.asyncio
    async def test_pass_notional_within_limit(self):
        engine = make_engine(limits={
            "test_strat": {"max_notional": "10000"}
        })
        kill_pub = AsyncMock()
        # notional = 50000 * 0.01 = 500 < 10000
        signal = make_signal(price=50000.0, quantity=0.01)

        result = await engine._check_signal(signal, kill_pub)
        assert result is True

    @pytest.mark.asyncio
    async def test_kill_on_daily_loss_breach(self):
        engine = make_engine(limits={
            "test_strat": {"max_daily_loss": "500"}
        })
        kill_pub = AsyncMock()

        # Simulate that PnL already dropped below limit
        state = engine._get_state("test_strat")
        state.daily_pnl = -600

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False
        assert state.enabled is False  # strategy killed

    @pytest.mark.asyncio
    async def test_pass_when_daily_loss_within_limit(self):
        engine = make_engine(limits={
            "test_strat": {"max_daily_loss": "500"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.daily_pnl = -100  # within limit

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is True

    @pytest.mark.asyncio
    async def test_kill_on_drawdown_breach(self):
        engine = make_engine(limits={
            "test_strat": {"max_drawdown_pct": "10"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.peak_pnl = 1000
        state.daily_pnl = 800  # 20% drawdown from peak

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False
        assert state.enabled is False

    @pytest.mark.asyncio
    async def test_pass_drawdown_within_limit(self):
        engine = make_engine(limits={
            "test_strat": {"max_drawdown_pct": "10"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.peak_pnl = 1000
        state.daily_pnl = 950  # 5% drawdown

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is True

    @pytest.mark.asyncio
    async def test_kill_on_order_rate_exceeded(self):
        engine = make_engine(limits={
            "test_strat": {"max_order_rate": "3"}
        })
        kill_pub = AsyncMock()

        # Send 3 signals (allowed), 4th should kill
        for _ in range(3):
            result = await engine._check_signal(make_signal(), kill_pub)
            assert result is True

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False

    @pytest.mark.asyncio
    async def test_order_rate_resets_after_window(self):
        engine = make_engine(limits={
            "test_strat": {"max_order_rate": "2"}
        })
        kill_pub = AsyncMock()

        # Use up the limit
        await engine._check_signal(make_signal(), kill_pub)
        await engine._check_signal(make_signal(), kill_pub)

        # Simulate time window passed
        state = engine._get_state("test_strat")
        state.order_count_reset_time = time.time() - 61  # >60s ago

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is True  # rate counter reset

    @pytest.mark.asyncio
    async def test_kill_on_delta_breach(self):
        engine = make_engine(limits={
            "test_strat": {"max_delta": "5.0"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.net_delta = 6.0  # exceeds limit

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False
        assert state.enabled is False

    @pytest.mark.asyncio
    async def test_pass_delta_within_limit(self):
        engine = make_engine(limits={
            "test_strat": {"max_delta": "5.0"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.net_delta = 3.0

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is True

    @pytest.mark.asyncio
    async def test_negative_delta_also_checked(self):
        engine = make_engine(limits={
            "test_strat": {"max_delta": "5.0"}
        })
        kill_pub = AsyncMock()

        state = engine._get_state("test_strat")
        state.net_delta = -6.0  # abs exceeds limit

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_limits_all_checked(self):
        """Signal must pass ALL limits, not just one."""
        engine = make_engine(limits={
            "test_strat": {
                "max_notional": "100000",
                "max_daily_loss": "500",
                "max_delta": "10",
                "max_order_rate": "100",
            }
        })
        kill_pub = AsyncMock()

        # Everything within limits
        state = engine._get_state("test_strat")
        state.daily_pnl = -100
        state.net_delta = 2.0

        result = await engine._check_signal(make_signal(), kill_pub)
        assert result is True


class TestRiskOnFill:
    """Test PnL updates on fills."""

    @pytest.mark.asyncio
    async def test_fill_deducts_commission(self):
        engine = make_engine()
        fill = {
            "strategy": "test_strat",
            "commission": -0.5,
            "symbol": "BTC-USDT-SWAP",
        }
        await engine._on_fill(fill)

        state = engine._get_state("test_strat")
        assert state.daily_pnl == -0.5

    @pytest.mark.asyncio
    async def test_fill_no_strategy_ignored(self):
        engine = make_engine()
        fill = {"strategy": "", "commission": -0.5}
        await engine._on_fill(fill)
        # Should not create any state
        assert len(engine._strategy_states) == 0

    @pytest.mark.asyncio
    async def test_multiple_fills_accumulate(self):
        engine = make_engine()
        for _ in range(3):
            await engine._on_fill({
                "strategy": "test_strat",
                "commission": -0.1,
                "symbol": "BTC",
            })

        state = engine._get_state("test_strat")
        assert state.daily_pnl == pytest.approx(-0.3)
