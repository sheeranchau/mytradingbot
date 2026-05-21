"""
Risk engine process.
Sits between strategy signals and order execution.
Monitors PnL, delta, and enforces limits.
"""
import asyncio
import time
from dataclasses import dataclass

from core.process_base import ProcessBase
from core.zmq_channels import (
    Puller, Pusher, Publisher, MultiSubscriber,
    SIGNAL_PORT, RISK_KILL_PORT,
    GATEWAY_ORDER_PORTS, GATEWAY_MARKET_DATA_PORTS,
)
from core.events import EventType
from core.logger import get_logger


@dataclass
class StrategyRiskState:
    """Real-time risk state per strategy."""
    name: str
    daily_pnl: float = 0.0
    peak_pnl: float = 0.0
    drawdown: float = 0.0
    net_delta: float = 0.0
    gross_delta: float = 0.0
    order_count: int = 0
    order_count_reset_time: float = 0.0
    enabled: bool = True


class RiskEngine(ProcessBase):
    """
    Risk engine responsibilities:
    - Pull signals from strategies
    - Validate against risk limits (from Redis)
    - Forward valid orders to gateway
    - Monitor PnL and delta continuously
    - Publish kill signals when limits breached
    """

    def __init__(self, **kwargs):
        super().__init__(process_name="risk_engine", **kwargs)
        self.logger = get_logger("risk_engine")
        self._strategy_states: dict[str, StrategyRiskState] = {}

    async def run(self) -> None:
        # Pull signals from strategies
        signal_puller = Puller(SIGNAL_PORT, bind=True)
        # One Pusher per gateway — each gateway binds its own orders port.
        order_pushers: dict[str, Pusher] = {
            gw: Pusher(port, bind=False) for gw, port in GATEWAY_ORDER_PORTS.items()
        }
        self.logger.info("Order routers ready for gateways: %s", list(order_pushers))
        # Publish kill signals
        kill_pub = Publisher(RISK_KILL_PORT)
        # Subscribe to fills/ticks from every gateway
        market_sub = MultiSubscriber(GATEWAY_MARKET_DATA_PORTS, topics=["fill.", "tick."])

        asyncio.create_task(self._monitor_market(market_sub, kill_pub))

        while self.running:
            try:
                signal = await signal_puller.pull()
                approved = await self._check_signal(signal, kill_pub)
                if approved:
                    gw = signal["gateway"]
                    pusher = order_pushers.get(gw)
                    if not pusher:
                        self.logger.error("Signal for unknown gateway '%s' — dropped", gw)
                        continue
                    # Convert signal to order
                    order = {
                        "event_type": EventType.ORDER_NEW,
                        "strategy": signal["strategy"],
                        "gateway": gw,
                        "symbol": signal["symbol"],
                        "side": signal["side"],
                        "order_type": signal.get("order_type", "limit"),
                        "price": signal.get("price", 0),
                        "quantity": signal.get("quantity", 0),
                        "timestamp": time.time(),
                    }
                    await pusher.push(order)
            except Exception as e:
                self.logger.error("Signal processing error: %s", e)
                await asyncio.sleep(0.1)

    async def _check_signal(self, signal: dict, kill_pub: Publisher) -> bool:
        """Pre-trade risk checks. Returns True if signal is approved."""
        strategy_name = signal.get("strategy", "")
        state = self._get_state(strategy_name)

        if not state.enabled:
            return False

        # Load limits from Redis
        limits = await self.redis.get_risk_limits(strategy_name)
        if not limits:
            # No limits configured — pass through (be permissive during setup)
            return True

        # Check order rate
        now = time.time()
        if now - state.order_count_reset_time > 60:
            state.order_count = 0
            state.order_count_reset_time = now

        max_order_rate = float(limits.get("max_order_rate", 100))
        if state.order_count >= max_order_rate:
            await self._kill_strategy(kill_pub, strategy_name, "Order rate limit exceeded")
            return False
        state.order_count += 1

        # Check max position / notional
        quantity = signal.get("quantity", 0)
        price = signal.get("price", 0)
        notional = quantity * price

        max_notional = float(limits.get("max_notional", 0))
        if max_notional > 0 and notional > max_notional:
            self.logger.warning("Signal rejected: notional %s > %s", notional, max_notional)
            return False

        # Check daily loss
        max_daily_loss = float(limits.get("max_daily_loss", 0))
        if max_daily_loss > 0 and state.daily_pnl < -max_daily_loss:
            await self._kill_strategy(kill_pub, strategy_name,
                                      f"Max daily loss breached: {state.daily_pnl}")
            return False

        # Check drawdown
        max_drawdown_pct = float(limits.get("max_drawdown_pct", 0))
        if max_drawdown_pct > 0 and state.peak_pnl > 0:
            dd_pct = (state.peak_pnl - state.daily_pnl) / state.peak_pnl * 100
            if dd_pct > max_drawdown_pct:
                await self._kill_strategy(kill_pub, strategy_name,
                                          f"Drawdown {dd_pct:.1f}% > {max_drawdown_pct}%")
                return False

        # Check delta limits
        max_delta = float(limits.get("max_delta", 0))
        if max_delta > 0 and abs(state.net_delta) > max_delta:
            await self._kill_strategy(kill_pub, strategy_name,
                                      f"Delta {state.net_delta} exceeds limit {max_delta}")
            return False

        return True

    async def _monitor_market(self, market_sub: MultiSubscriber, kill_pub: Publisher) -> None:
        """Continuously update PnL and delta from market data and fills."""
        while self.running:
            try:
                topic, data = await market_sub.receive()
                event_type = data.get("event_type", "")

                if event_type == EventType.FILL:
                    await self._on_fill(data)
                elif event_type == EventType.TICK:
                    await self._on_tick(data, kill_pub)
            except Exception as e:
                self.logger.error("Monitor error: %s", e)
                await asyncio.sleep(0.1)

    async def _on_fill(self, fill: dict) -> None:
        """Update PnL on fill."""
        strategy = fill.get("strategy", "")
        if not strategy:
            return
        state = self._get_state(strategy)
        # Simplified: track realized PnL from commission
        commission = fill.get("commission", 0)
        state.daily_pnl -= abs(commission)

        # Log to Redis
        await self.redis.log_trade(fill)
        await self.redis.snapshot_pnl(strategy, {
            "daily_pnl": str(state.daily_pnl),
            "timestamp": str(time.time()),
        })

    async def _on_tick(self, tick: dict, kill_pub: Publisher) -> None:
        """Update unrealized PnL based on market prices."""
        # In a full implementation, iterate positions and mark to market
        # For now, positions are tracked per strategy in Redis
        pass

    async def _kill_strategy(self, kill_pub: Publisher, strategy: str, reason: str) -> None:
        """Disable a strategy and publish kill signal."""
        state = self._get_state(strategy)
        state.enabled = False

        # Persist to Redis
        await self.redis.set_strategy_enabled(strategy, False)

        # Publish kill signal
        await kill_pub.publish(f"risk.kill.{strategy}", {
            "event_type": EventType.RISK_KILL,
            "strategy": strategy,
            "reason": reason,
            "timestamp": time.time(),
            "flatten": True,
        })
        self.logger.critical("KILLED strategy '%s': %s", strategy, reason)

    async def kill_all(self, kill_pub: Publisher, reason: str) -> None:
        """Emergency: kill all strategies."""
        for state in self._strategy_states.values():
            state.enabled = False
            await self.redis.set_strategy_enabled(state.name, False)

        await kill_pub.publish("risk.kill.all", {
            "event_type": EventType.RISK_KILL,
            "strategy": "",
            "reason": reason,
            "timestamp": time.time(),
            "flatten": True,
        })
        self.logger.critical("KILLED ALL STRATEGIES: %s", reason)

    def _get_state(self, strategy_name: str) -> StrategyRiskState:
        if strategy_name not in self._strategy_states:
            self._strategy_states[strategy_name] = StrategyRiskState(name=strategy_name)
        return self._strategy_states[strategy_name]
