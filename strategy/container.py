"""
Strategy container process.
Manages multiple strategies, routes market data to them, and emits signals.
Provides strategies with order management, depth, balance, and position access.
"""
import asyncio
import json
import time

from core.process_base import ProcessBase
from core.zmq_channels import (
    Subscriber, MultiSubscriber, Pusher,
    SIGNAL_PORT, RISK_KILL_PORT, GATEWAY_MARKET_DATA_PORTS,
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
    - Provides order/depth/balance/position access to strategies
    - Publishes strategy state to Redis for the dashboard
    - Listens for commands from the dashboard via Redis pub/sub
    """

    def __init__(self, strategies: list[BaseStrategy], **kwargs):
        super().__init__(process_name="strategy_container", **kwargs)
        self.logger = get_logger("strategy_container")
        self.strategies = {s.name: s for s in strategies}
        self._latest_prices: dict[str, dict] = {}
        self._depth_cache: dict[str, dict] = {}
        self._signal_push: Pusher = None

        self._routes: dict[tuple[str, str], list[str]] = {}
        for s in strategies:
            s.set_container(self)
            for symbol in s.symbols:
                key = (s.gateway, symbol)
                self._routes.setdefault(key, []).append(s.name)

    async def run(self) -> None:
        market_sub = MultiSubscriber(GATEWAY_MARKET_DATA_PORTS,
                                     topics=["tick.", "orderbook.", "fill."])
        self._signal_push = Pusher(SIGNAL_PORT, bind=False)
        kill_sub = Subscriber(RISK_KILL_PORT, topics=["risk."])

        asyncio.create_task(self._listen_kills(kill_sub))
        asyncio.create_task(self._publish_state_loop())
        asyncio.create_task(self._listen_commands())
        asyncio.create_task(self._run_periodic_tasks())

        self.logger.info("Loaded %d strategies: %s", len(self.strategies),
                         list(self.strategies.keys()))
        self.logger.info("Routing table: %s", {
            f"{gw}.{sym}": names for (gw, sym), names in self._routes.items()
        })

        while self.running:
            try:
                topic, data = await market_sub.receive()
                await self._dispatch(topic, data)
            except Exception as e:
                self.logger.error("Dispatch error: %s", e)
                await asyncio.sleep(0.1)

    # ── Market data dispatch ─────────────────────────────────────────────

    async def _dispatch(self, topic: str, data: dict) -> None:
        gateway = data.get("gateway", "")
        symbol = data.get("symbol", "")
        event_type = data.get("event_type", "")

        if event_type == EventType.TICK:
            self._latest_prices[f"{gateway}:{symbol}"] = {
                "bid": data.get("bid", 0),
                "ask": data.get("ask", 0),
                "last": data.get("last", 0),
                "timestamp": data.get("timestamp", 0),
            }

        if event_type == EventType.ORDERBOOK:
            self._depth_cache[f"{gateway}:{symbol}"] = data

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
                signal = strategy.on_depth(symbol, data)
                if signal is None:
                    signal = strategy.on_orderbook(symbol, data)
            elif event_type == EventType.FILL:
                if data.get("strategy") == name:
                    strategy.on_fill(data)
                    self.logger.info("Fill received: %s %s %s @ %s",
                                     name, data.get("side"), symbol, data.get("price"))

            if signal:
                await self._emit_signal(name, gateway, symbol, signal)

    async def _emit_signal(self, name: str, gateway: str, symbol: str,
                           signal: dict) -> None:
        strategy = self.strategies[name]
        strategy._signal_count += 1
        strategy._last_signal = {**signal, "timestamp": time.time(), "symbol": symbol}
        signal["event_type"] = EventType.SIGNAL
        signal["strategy"] = name
        signal["gateway"] = gateway
        signal["symbol"] = symbol
        signal["timestamp"] = time.time()
        self.logger.info("Signal: %s %s %s %s @ %s qty=%s reason=%s",
                         name, signal["side"], gateway, symbol,
                         signal.get("price"), signal.get("quantity"),
                         signal.get("reason", ""))
        await self._signal_push.push(signal)

    # ── Strategy-callable capabilities ───────────────────────────────────

    async def strategy_place_order(self, strategy_name: str, gateway: str,
                                   symbol: str, side: str, price: float,
                                   quantity: float, order_type: str = "limit") -> None:
        order = {
            "event_type": EventType.ORDER_NEW,
            "strategy": strategy_name,
            "gateway": gateway,
            "symbol": symbol,
            "side": side,
            "price": price,
            "quantity": quantity,
            "order_type": order_type,
            "timestamp": time.time(),
        }
        await self._signal_push.push(order)

    async def strategy_cancel_order(self, strategy_name: str, gateway: str,
                                    order_id: str, symbol: str) -> None:
        order = {
            "event_type": EventType.ORDER_CANCEL,
            "strategy": strategy_name,
            "gateway": gateway,
            "order_id": order_id,
            "symbol": symbol,
            "timestamp": time.time(),
        }
        await self._signal_push.push(order)

    async def strategy_amend_order(self, strategy_name: str, gateway: str,
                                   order_id: str, symbol: str,
                                   new_price: float = None,
                                   new_qty: float = None) -> None:
        order = {
            "event_type": EventType.ORDER_AMEND,
            "strategy": strategy_name,
            "gateway": gateway,
            "order_id": order_id,
            "symbol": symbol,
            "timestamp": time.time(),
        }
        if new_price is not None:
            order["price"] = new_price
        if new_qty is not None:
            order["quantity"] = new_qty
        await self._signal_push.push(order)

    async def strategy_get_positions(self, gateway: str) -> list[dict]:
        try:
            acct_id = f"{gateway}_demo"
            raw = await self.redis.client.get(f"account:{acct_id}:positions")
            if not raw:
                acct_id = f"{gateway}_live"
                raw = await self.redis.client.get(f"account:{acct_id}:positions")
            return json.loads(raw) if raw else []
        except Exception:
            return []

    async def strategy_get_balance(self, gateway: str) -> dict:
        try:
            acct_id = f"{gateway}_demo"
            raw = await self.redis.client.get(f"account:{acct_id}:trading")
            if not raw:
                acct_id = f"{gateway}_live"
                raw = await self.redis.client.get(f"account:{acct_id}:trading")
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    def strategy_get_depth(self, gateway: str, symbol: str) -> dict:
        return self._depth_cache.get(f"{gateway}:{symbol}",
                                     {"bids": [], "asks": []})

    # ── Periodic task runner ─────────────────────────────────────────────

    async def _run_periodic_tasks(self):
        tasks = []
        for name, strategy in self.strategies.items():
            for pt in strategy._periodic_tasks:
                tasks.append(self._periodic_loop(name, pt["fn"], pt["interval"]))
        if tasks:
            await asyncio.gather(*tasks)

    async def _periodic_loop(self, strategy_name: str, fn, interval: float):
        while self.running:
            try:
                strategy = self.strategies[strategy_name]
                if strategy.enabled:
                    result = fn()
                    if asyncio.iscoroutine(result):
                        await result
            except Exception as e:
                self.logger.error("Periodic task error (%s): %s", strategy_name, e)
            await asyncio.sleep(interval)

    # ── Dashboard state publishing ───────────────────────────────────────

    async def _publish_state_loop(self) -> None:
        while self.running:
            await asyncio.sleep(5)
            try:
                states = {}
                for name, strategy in self.strategies.items():
                    states[name] = strategy.get_state()
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

    # ── Command handling ─────────────────────────────────────────────────

    async def _listen_commands(self) -> None:
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

    # ── Kill signal handling ─────────────────────────────────────────────

    async def _listen_kills(self, kill_sub: Subscriber) -> None:
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
