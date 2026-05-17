"""
Strategy container process.
Manages multiple strategies, routes market data to them, and emits signals.
"""
import asyncio
import json
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
    - Publishes strategy state to Redis for the dashboard
    - Listens for param update commands from Redis
    """

    def __init__(self, strategies: list[BaseStrategy], **kwargs):
        super().__init__(process_name="strategy_container", **kwargs)
        self.logger = get_logger("strategy_container")
        self.strategies = {s.name: s for s in strategies}
        self._latest_prices: dict[str, dict] = {}  # symbol -> last tick
        # Build routing table: (gateway, symbol) -> [strategy_name, ...]
        self._routes: dict[tuple[str, str], list[str]] = {}
        for s in strategies:
            for symbol in s.symbols:
                key = (s.gateway, symbol)
                self._routes.setdefault(key, []).append(s.name)

    async def run(self) -> None:
        market_sub = Subscriber(MARKET_DATA_PORT, topics=["tick.", "orderbook.", "fill."])
        signal_push = Pusher(SIGNAL_PORT, bind=False)
        kill_sub = Subscriber(RISK_KILL_PORT, topics=["risk."])

        asyncio.create_task(self._listen_kills(kill_sub))
        asyncio.create_task(self._publish_state_loop())
        asyncio.create_task(self._listen_commands())

        self.logger.info("Loaded %d strategies: %s", len(self.strategies),
                         list(self.strategies.keys()))
        self.logger.info("Routing table: %s", {
            f"{gw}.{sym}": names for (gw, sym), names in self._routes.items()
        })

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

        # Track latest prices for dashboard
        if event_type == EventType.TICK:
            self._latest_prices[f"{gateway}:{symbol}"] = {
                "bid": data.get("bid", 0),
                "ask": data.get("ask", 0),
                "last": data.get("last", 0),
                "timestamp": data.get("timestamp", 0),
            }

        key = (gateway, symbol)
        strategy_names = self._routes.get(key, [])

        for name in strategy_names:
            strategy = self.strategies[name]
            if not strategy.enabled:
                continue

            enabled = await self.redis.is_strategy_enabled(name)
            if not enabled:
                strategy.enabled = False
                continue

            signal = None
            if event_type == EventType.TICK:
                strategy._tick_count += 1
                signal = strategy.on_tick(symbol, data)
            elif event_type == EventType.ORDERBOOK:
                signal = strategy.on_orderbook(symbol, data)
            elif event_type == EventType.FILL:
                if data.get("strategy") == name:
                    strategy.on_fill(data)
                    self.logger.info("Fill received: %s %s %s @ %s",
                                     name, data.get("side"), symbol, data.get("price"))

            if signal:
                strategy._signal_count += 1
                strategy._last_signal = {
                    **signal,
                    "timestamp": time.time(),
                    "symbol": symbol,
                }
                signal["event_type"] = EventType.SIGNAL
                signal["strategy"] = name
                signal["gateway"] = gateway
                signal["symbol"] = symbol
                signal["timestamp"] = time.time()
                self.logger.info("Signal: %s %s %s %s @ %s qty=%s reason=%s",
                                 name, signal["side"], gateway, symbol,
                                 signal.get("price"), signal.get("quantity"),
                                 signal.get("reason", ""))
                await signal_push.push(signal)

    async def _publish_state_loop(self) -> None:
        """Publish strategy states to Redis every 5 seconds for the dashboard."""
        while self.running:
            await asyncio.sleep(5)
            try:
                states = {}
                for name, strategy in self.strategies.items():
                    state = strategy.get_state()
                    states[name] = state

                # Store as JSON in Redis
                await self.redis.client.set(
                    "dashboard:strategies",
                    json.dumps(states, default=str),
                    ex=30,
                )
                await self.redis.client.set(
                    "dashboard:prices",
                    json.dumps(self._latest_prices, default=str),
                    ex=30,
                )
            except Exception as e:
                self.logger.error("State publish error: %s", e)

    async def _listen_commands(self) -> None:
        """Listen for commands from the dashboard via Redis pub/sub."""
        pubsub = self.redis.client.pubsub()
        await pubsub.subscribe("strategy:command")

        while self.running:
            try:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if msg and msg["type"] == "message":
                    await self._handle_command(json.loads(msg["data"]))
            except Exception as e:
                self.logger.error("Command listener error: %s", e)
                await asyncio.sleep(1)

    async def _handle_command(self, cmd: dict) -> None:
        """Handle commands from the dashboard."""
        action = cmd.get("action", "")
        strategy_name = cmd.get("strategy", "")

        if action == "update_params" and strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            applied = strategy.set_params(cmd.get("params", {}))
            self.logger.info("Params updated for '%s': %s", strategy_name, applied)

        elif action == "enable" and strategy_name in self.strategies:
            self.strategies[strategy_name].enabled = True
            await self.redis.set_strategy_enabled(strategy_name, True)
            self.logger.info("Strategy '%s' enabled", strategy_name)

        elif action == "disable" and strategy_name in self.strategies:
            self.strategies[strategy_name].enabled = False
            await self.redis.set_strategy_enabled(strategy_name, False)
            self.logger.info("Strategy '%s' disabled", strategy_name)

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
                    for s in self.strategies.values():
                        s.enabled = False
                    self.logger.critical("ALL strategies killed: %s", reason)
            except Exception as e:
                self.logger.error("Kill listener error: %s", e)
                await asyncio.sleep(1)
