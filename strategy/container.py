"""
Strategy container process.
Manages multiple strategies, routes market data to them, and emits signals.
"""
import asyncio
import time

from core.process_base import ProcessBase
from core.zmq_channels import (
    Subscriber, Pusher, MARKET_DATA_PORT, SIGNAL_PORT, RISK_KILL_PORT
)
from core.events import EventType
from core.logger import get_logger
from strategy.base import BaseStrategy


class StrategyContainer(ProcessBase):
    """
    Runs all strategies in a single process.
    - Subscribes to market data from all gateways
    - Routes ticks/books to appropriate strategies
    - Pushes signals to risk engine
    - Listens for kill signals from risk
    """

    def __init__(self, strategies: list[BaseStrategy], **kwargs):
        super().__init__(process_name="strategy_container", **kwargs)
        self.logger = get_logger("strategy_container")
        self.strategies = {s.name: s for s in strategies}
        # Build routing table: (gateway, symbol) -> [strategy_name, ...]
        self._routes: dict[tuple[str, str], list[str]] = {}
        for s in strategies:
            for symbol in s.symbols:
                key = (s.gateway, symbol)
                self._routes.setdefault(key, []).append(s.name)

    async def run(self) -> None:
        # Subscribe to all market data
        market_sub = Subscriber(MARKET_DATA_PORT, topics=["tick.", "orderbook.", "fill."])
        # Push signals to risk engine
        signal_push = Pusher(SIGNAL_PORT, bind=False)
        # Listen for risk kill signals
        kill_sub = Subscriber(RISK_KILL_PORT, topics=["risk."])

        asyncio.create_task(self._listen_kills(kill_sub))

        while self.running:
            try:
                topic, data = await market_sub.receive()
                await self._dispatch(topic, data, signal_push)
            except Exception as e:
                self.logger.error("Dispatch error: %s", e)
                await asyncio.sleep(0.1)

    async def _dispatch(self, topic: str, data: dict, signal_push: Pusher) -> None:
        gateway = data.get("gateway", "")
        symbol = data.get("symbol", "")
        event_type = data.get("event_type", "")

        # Route to strategies
        key = (gateway, symbol)
        strategy_names = self._routes.get(key, [])

        for name in strategy_names:
            strategy = self.strategies[name]
            if not strategy.enabled:
                continue

            # Check Redis for enabled state
            enabled = await self.redis.is_strategy_enabled(name)
            if not enabled:
                strategy.enabled = False
                continue

            signal = None
            if event_type == EventType.TICK:
                signal = strategy.on_tick(symbol, data)
            elif event_type == EventType.ORDERBOOK:
                signal = strategy.on_orderbook(symbol, data)
            elif event_type == EventType.FILL:
                if data.get("strategy") == name:
                    strategy.on_fill(data)

            if signal:
                signal["event_type"] = EventType.SIGNAL
                signal["strategy"] = name
                signal["gateway"] = gateway
                signal["symbol"] = symbol
                signal["timestamp"] = time.time()
                await signal_push.push(signal)

    async def _listen_kills(self, kill_sub: Subscriber) -> None:
        """Listen for risk kill signals and disable strategies."""
        while self.running:
            try:
                topic, data = await kill_sub.receive()
                target = data.get("strategy", "")
                reason = data.get("reason", "unknown")

                if target:
                    if target in self.strategies:
                        self.strategies[target].enabled = False
                        self.logger.warning("Strategy '%s' killed: %s", target, reason)
                else:
                    # Kill all
                    for s in self.strategies.values():
                        s.enabled = False
                    self.logger.critical("ALL strategies killed: %s", reason)
            except Exception as e:
                self.logger.error("Kill listener error: %s", e)
                await asyncio.sleep(1)
