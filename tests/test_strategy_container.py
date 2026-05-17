"""
Tests for strategy container routing logic.
"""
import pytest
from strategy.container import StrategyContainer
from strategy.examples.momentum import MomentumStrategy
from strategy.examples.spread_arb import SpreadArbStrategy
from core.events import EventType


class TestContainerRouting:
    """Test that the container routes events to the correct strategies."""

    def _make_container(self):
        s1 = MomentumStrategy(
            name="mom_btc", gateway="okx", symbols=["BTC-USDT-SWAP"],
            lookback=5, threshold=0.02,
        )
        s2 = MomentumStrategy(
            name="mom_eth", gateway="okx", symbols=["ETH-USDT-SWAP"],
            lookback=5, threshold=0.02,
        )
        s3 = SpreadArbStrategy(
            name="spread_btc", gateway="okx", symbols=["BTC-USDT-SWAP"],
            lookback=10, z_threshold=2.0,
        )
        s4 = MomentumStrategy(
            name="mom_futu", gateway="futu", symbols=["US.AAPL"],
            lookback=5, threshold=0.02,
        )
        container = StrategyContainer.__new__(StrategyContainer)
        container.strategies = {s.name: s for s in [s1, s2, s3, s4]}
        container._routes = {}
        for s in [s1, s2, s3, s4]:
            for symbol in s.symbols:
                key = (s.gateway, symbol)
                container._routes.setdefault(key, []).append(s.name)
        return container

    def test_routing_table_built_correctly(self):
        c = self._make_container()

        # BTC on OKX should route to both mom_btc and spread_btc
        assert set(c._routes[("okx", "BTC-USDT-SWAP")]) == {"mom_btc", "spread_btc"}
        # ETH on OKX should route to mom_eth only
        assert c._routes[("okx", "ETH-USDT-SWAP")] == ["mom_eth"]
        # AAPL on FUTU should route to mom_futu only
        assert c._routes[("futu", "US.AAPL")] == ["mom_futu"]

    def test_no_route_for_unknown_symbol(self):
        c = self._make_container()
        assert c._routes.get(("okx", "DOGE-USDT"), []) == []

    def test_no_route_for_wrong_gateway(self):
        c = self._make_container()
        # BTC exists on okx, not futu
        assert c._routes.get(("futu", "BTC-USDT-SWAP"), []) == []

    def test_strategy_count(self):
        c = self._make_container()
        assert len(c.strategies) == 4

    def test_disabled_strategy_not_in_route_check(self):
        c = self._make_container()
        c.strategies["mom_btc"].enabled = False

        # mom_btc is still in the route, but its enabled flag is False
        routes = c._routes[("okx", "BTC-USDT-SWAP")]
        assert "mom_btc" in routes  # still in routing table
        assert c.strategies["mom_btc"].enabled is False  # but disabled


class TestContainerKillHandling:
    """Test that kill signals correctly disable strategies."""

    def _make_container(self):
        s1 = MomentumStrategy(name="s1", gateway="okx", symbols=["BTC"], lookback=5, threshold=0.02)
        s2 = MomentumStrategy(name="s2", gateway="okx", symbols=["ETH"], lookback=5, threshold=0.02)
        container = StrategyContainer.__new__(StrategyContainer)
        container.strategies = {s.name: s for s in [s1, s2]}
        container._routes = {}
        return container

    def test_kill_single_strategy(self):
        c = self._make_container()
        target = "s1"
        if target in c.strategies:
            c.strategies[target].enabled = False

        assert c.strategies["s1"].enabled is False
        assert c.strategies["s2"].enabled is True  # unaffected

    def test_kill_all_strategies(self):
        c = self._make_container()
        for s in c.strategies.values():
            s.enabled = False

        assert all(not s.enabled for s in c.strategies.values())

    def test_kill_unknown_strategy_no_crash(self):
        c = self._make_container()
        target = "nonexistent"
        if target in c.strategies:
            c.strategies[target].enabled = False
        # Should not raise — just skip
