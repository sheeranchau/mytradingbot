"""
Tests for strategy logic.
These are pure unit tests — no ZMQ, no Redis, no async.
"""
import pytest
from strategy.base import BaseStrategy
from strategy.examples.momentum import MomentumStrategy
from strategy.examples.spread_arb import SpreadArbStrategy


class TestBaseStrategy:
    """Test position tracking in base strategy."""

    def _make_strategy(self):
        return MomentumStrategy(
            name="test", gateway="okx", symbols=["BTC-USDT-SWAP"],
            lookback=5, threshold=0.01, order_size=0.1,
        )

    def test_on_fill_buy_updates_position(self):
        s = self._make_strategy()
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "buy", "quantity": 0.5})
        assert s.positions["BTC-USDT-SWAP"] == 0.5

    def test_on_fill_sell_updates_position(self):
        s = self._make_strategy()
        s.positions["BTC-USDT-SWAP"] = 1.0
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "sell", "quantity": 0.3})
        assert s.positions["BTC-USDT-SWAP"] == pytest.approx(0.7)

    def test_on_fill_accumulates(self):
        s = self._make_strategy()
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "buy", "quantity": 0.1})
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "buy", "quantity": 0.2})
        assert s.positions["BTC-USDT-SWAP"] == pytest.approx(0.3)

    def test_on_fill_net_to_short(self):
        s = self._make_strategy()
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "buy", "quantity": 0.1})
        s.on_fill({"symbol": "BTC-USDT-SWAP", "side": "sell", "quantity": 0.3})
        assert s.positions["BTC-USDT-SWAP"] == pytest.approx(-0.2)

    def test_on_position_reconciles(self):
        s = self._make_strategy()
        s.positions["BTC-USDT-SWAP"] = 999.0  # stale
        s.on_position({"symbol": "BTC-USDT-SWAP", "quantity": 0.5})
        assert s.positions["BTC-USDT-SWAP"] == 0.5


class TestMomentumStrategy:
    """Test momentum signal generation."""

    def _make(self, lookback=5, threshold=0.02, order_size=0.1):
        return MomentumStrategy(
            name="test_mom", gateway="okx", symbols=["BTC-USDT-SWAP"],
            lookback=lookback, threshold=threshold, order_size=order_size,
        )

    def _tick(self, last, ask=None, bid=None):
        return {"last": last, "ask": ask or last + 1, "bid": bid or last - 1}

    def test_no_signal_during_warmup(self):
        s = self._make(lookback=5)
        for price in [100, 101, 102, 103]:  # only 4 ticks, need 5
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(price))
            assert signal is None

    def test_long_signal_on_upward_momentum(self):
        s = self._make(lookback=5, threshold=0.02)
        # Feed 5 ticks: 100 -> 103 (3% up)
        prices = [100, 100.5, 101, 102, 103]
        signal = None
        for p in prices:
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(p))

        assert signal is not None
        assert signal["side"] == "buy"
        assert signal["quantity"] == 0.1
        assert "momentum" in signal["reason"]

    def test_short_signal_on_downward_momentum(self):
        s = self._make(lookback=5, threshold=0.02)
        prices = [100, 99.5, 99, 98.5, 97]
        signal = None
        for p in prices:
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(p))

        assert signal is not None
        assert signal["side"] == "sell"

    def test_no_signal_within_threshold(self):
        s = self._make(lookback=5, threshold=0.05)
        # 2% move, but threshold is 5%
        prices = [100, 100.5, 101, 101.5, 102]
        signal = None
        for p in prices:
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(p))

        assert signal is None

    def test_no_duplicate_long_signal_when_already_long(self):
        s = self._make(lookback=5, threshold=0.02)
        s.positions["BTC-USDT-SWAP"] = 0.1  # already long

        prices = [100, 100.5, 101, 102, 103]
        signal = None
        for p in prices:
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(p))

        assert signal is None  # should not signal again

    def test_reverse_from_short_to_long_includes_flatten(self):
        s = self._make(lookback=5, threshold=0.02, order_size=0.1)
        s.positions["BTC-USDT-SWAP"] = -0.2  # short 0.2

        prices = [100, 100.5, 101, 102, 103]
        signal = None
        for p in prices:
            signal = s.on_tick("BTC-USDT-SWAP", self._tick(p))

        assert signal is not None
        assert signal["side"] == "buy"
        # Should flatten the short (0.2) + new long (0.1) = 0.3
        assert signal["quantity"] == pytest.approx(0.3)

    def test_zero_price_tick_ignored(self):
        s = self._make(lookback=5)
        signal = s.on_tick("BTC-USDT-SWAP", self._tick(0))
        assert signal is None

    def test_negative_price_tick_ignored(self):
        s = self._make(lookback=5)
        signal = s.on_tick("BTC-USDT-SWAP", self._tick(-100))
        assert signal is None


class TestSpreadArbStrategy:
    """Test spread arbitrage signal generation."""

    def _make(self, lookback=10, z_threshold=2.0, order_size=0.01):
        return SpreadArbStrategy(
            name="test_spread", gateway="okx", symbols=["ETH-USDT-SWAP"],
            lookback=lookback, z_threshold=z_threshold, order_size=order_size,
        )

    def _book(self, best_bid, best_ask):
        return {
            "bids": [[best_bid, 10.0]],
            "asks": [[best_ask, 10.0]],
        }

    def test_no_signal_during_warmup(self):
        s = self._make(lookback=10)
        for i in range(9):
            signal = s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))
            assert signal is None

    def test_no_signal_on_normal_spread(self):
        s = self._make(lookback=10, z_threshold=2.0)
        # Feed 10 identical spreads — std=0, no signal
        for _ in range(10):
            signal = s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))

        assert signal is None  # std=0, returns None

    def test_buy_signal_on_wide_spread(self):
        s = self._make(lookback=10, z_threshold=2.0)
        # Feed 9 tight spreads, then 1 wide one
        for _ in range(9):
            s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))
        # Now a much wider spread
        signal = s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3010))

        assert signal is not None
        assert signal["side"] == "buy"
        assert signal["price"] == 3000  # buy at bid
        assert "spread_z" in signal["reason"]

    def test_sell_to_take_profit_when_spread_normalizes(self):
        s = self._make(lookback=10, z_threshold=2.0)
        s.positions["ETH-USDT-SWAP"] = 0.01  # already long from previous entry

        # Feed spreads that make current z < 0.5
        for _ in range(10):
            s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))
        signal = s.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))

        # z should be ~0 (all same spread), and we have position => take profit
        # But std=0 means z calculation returns None
        # Need variation for std > 0
        # Reset with some variation
        s2 = self._make(lookback=10, z_threshold=2.0)
        s2.positions["ETH-USDT-SWAP"] = 0.05

        # Build up spread history with some variation
        spreads_sequence = [
            (3000, 3002), (3000, 3003), (3000, 3001), (3000, 3004),
            (3000, 3002), (3000, 3003), (3000, 3001), (3000, 3002),
            (3000, 3003), (3000, 3002),
        ]
        for bid, ask in spreads_sequence:
            s2.on_orderbook("ETH-USDT-SWAP", self._book(bid, ask))

        # Now send a tight spread (z should be negative, < 0.5)
        signal = s2.on_orderbook("ETH-USDT-SWAP", self._book(3000, 3001))

        assert signal is not None
        assert signal["side"] == "sell"
        assert signal["quantity"] == 0.05  # sell entire position

    def test_empty_orderbook_no_crash(self):
        s = self._make(lookback=10)
        signal = s.on_orderbook("ETH-USDT-SWAP", {"bids": [], "asks": []})
        assert signal is None

    def test_tick_returns_none(self):
        s = self._make()
        signal = s.on_tick("ETH-USDT-SWAP", {"last": 3000})
        assert signal is None
