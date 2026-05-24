"""
Risk engine process.
Sits between strategy signals and order execution.
Monitors PnL, delta, and enforces limits.
"""
import asyncio
import json
import time
from dataclasses import dataclass, field

from core.process_base import ProcessBase
from core.zmq_channels import (
    Puller, Pusher, Publisher, MultiSubscriber,
    SIGNAL_PORT, RISK_KILL_PORT,
    GATEWAY_ORDER_PORTS, GATEWAY_MARKET_DATA_PORTS,
)
from core.events import EventType
from core.logger import get_logger
from core.models import OrderUpdate, OrderState, SymbolPosition


@dataclass
class StrategyRiskState:
    """
    Real-time risk state per strategy.

    Positions are tracked per symbol (key = "{gateway}:{symbol}").
    Aggregate PnL/delta are derived from those positions on demand.
    ``daily_pnl`` is a cached aggregate refreshed after every fill/tick so
    that ``_check_signal`` can read it cheaply.
    """
    name: str
    enabled: bool = True
    order_count: int = 0
    order_count_reset_time: float = 0.0

    # Per-symbol positions; key = "{gateway}:{symbol}"
    positions: dict = field(default_factory=dict)  # str → SymbolPosition

    # Cached aggregates — call refresh_pnl() to update
    daily_pnl: float = 0.0   # total_pnl across all positions
    peak_pnl: float  = 0.0   # high-watermark for drawdown calculation

    # ── Derived aggregates ────────────────────────────────────────────────

    @property
    def net_delta(self) -> float:
        """Total USD net directional exposure across all open positions."""
        return sum(p.dollar_delta for p in self.positions.values())

    @property
    def realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self.positions.values())

    @property
    def unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def total_fees(self) -> float:
        return sum(p.total_fees for p in self.positions.values())

    def refresh_pnl(self) -> None:
        """Recompute cached daily_pnl and advance peak_pnl high-watermark."""
        self.daily_pnl = sum(p.total_pnl for p in self.positions.values())
        if self.daily_pnl > self.peak_pnl:
            self.peak_pnl = self.daily_pnl


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
        self._last_snapshot: dict[str, float] = {}  # strategy → last snapshot timestamp

    async def run(self) -> None:
        await self._restore_pnl_from_redis()

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
                event_type = signal.get("event_type", EventType.SIGNAL)

                # Cancel and amend bypass risk checks
                if event_type in (EventType.ORDER_CANCEL, EventType.ORDER_AMEND):
                    gw = signal.get("gateway", "")
                    pusher = order_pushers.get(gw)
                    if not pusher:
                        self.logger.error("%s for unknown gateway '%s' — dropped", event_type, gw)
                        continue
                    await pusher.push(signal)
                    self.logger.info("Forwarded %s for order %s to %s",
                                     event_type, signal.get("order_id", "?"), gw)
                    continue

                approved, reason = await self._check_signal(signal, kill_pub)

                # Write result to a short-lived Redis key so the caller
                # (web server) can pick it up synchronously.
                request_id = signal.get("request_id")
                if request_id:
                    r = self.redis.client
                    await r.set(
                        f"order_response:{request_id}",
                        json.dumps({"approved": approved, "reason": reason}),
                        ex=30,
                    )

                if not approved:
                    self.logger.warning(
                        "Signal rejected [%s/%s]: %s",
                        signal.get("strategy", "?"), signal.get("symbol", "?"), reason,
                    )
                    # Record rejected manual orders in finished_orders so they
                    # appear in the dashboard order history with the reason.
                    if request_id:
                        now_ms = int(time.time() * 1000)
                        update = OrderUpdate(
                            client_order_id=request_id,
                            gateway=signal.get("gateway", ""),
                            strategy=signal.get("strategy", ""),
                            symbol=signal.get("symbol", ""),
                            side=signal.get("side", ""),
                            order_type=signal.get("order_type", "limit"),
                            price=float(signal.get("price", 0)),
                            quantity=float(signal.get("quantity", 0)),
                            state=OrderState.REJECTED,
                            reject_reason=reason,
                            update_time_ms=now_ms,
                            create_time_ms=now_ms,
                            update_source="risk",
                        )
                        await self.redis.client.xadd(
                            "finished_orders", update.to_redis_dict(), maxlen=500
                        )
                    continue

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

    async def _check_signal(self, signal: dict, kill_pub: Publisher) -> tuple[bool, str]:
        """Pre-trade risk checks. Returns (approved, reason).

        reason is empty string when approved; a human-readable explanation
        when rejected (used for UI feedback and order history).
        """
        strategy_name = signal.get("strategy", "")
        state = self._get_state(strategy_name)

        if not state.enabled:
            return False, f"Strategy '{strategy_name}' is disabled"

        # Load limits from Redis
        limits = await self.redis.get_risk_limits(strategy_name)
        if not limits:
            # No limits configured — pass through (be permissive during setup)
            return True, ""

        # Check order rate
        now = time.time()
        if now - state.order_count_reset_time > 60:
            state.order_count = 0
            state.order_count_reset_time = now

        max_order_rate = float(limits.get("max_order_rate", 100))
        if state.order_count >= max_order_rate:
            reason = f"Order rate limit exceeded ({state.order_count}/{int(max_order_rate)} per min)"
            await self._kill_strategy(kill_pub, strategy_name, reason)
            return False, reason
        state.order_count += 1

        # Check max notional
        quantity = float(signal.get("quantity", 0))
        price    = float(signal.get("price", 0))
        notional = quantity * price

        max_notional = float(limits.get("max_notional", 0))
        if max_notional > 0 and notional > max_notional:
            return False, f"Notional {notional:.2f} exceeds limit {max_notional:.2f}"

        # Check daily loss
        max_daily_loss = float(limits.get("max_daily_loss", 0))
        if max_daily_loss > 0 and state.daily_pnl < -max_daily_loss:
            reason = f"Daily loss limit breached ({state.daily_pnl:.2f} / -{max_daily_loss:.2f})"
            await self._kill_strategy(kill_pub, strategy_name, reason)
            return False, reason

        # Check drawdown
        max_drawdown_pct = float(limits.get("max_drawdown_pct", 0))
        if max_drawdown_pct > 0 and state.peak_pnl > 0:
            dd_pct = (state.peak_pnl - state.daily_pnl) / state.peak_pnl * 100
            if dd_pct > max_drawdown_pct:
                reason = f"Drawdown {dd_pct:.1f}% exceeds limit {max_drawdown_pct:.1f}%"
                await self._kill_strategy(kill_pub, strategy_name, reason)
                return False, reason

        # Check delta limits
        max_delta = float(limits.get("max_delta", 0))
        if max_delta > 0 and abs(state.net_delta) > max_delta:
            reason = f"Net delta {state.net_delta:.4f} exceeds limit {max_delta:.4f}"
            await self._kill_strategy(kill_pub, strategy_name, reason)
            return False, reason

        return True, ""

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
        """Apply fill to the matching SymbolPosition and refresh PnL."""
        gateway  = fill.get("gateway", "")
        strategy = fill.get("strategy", "")
        symbol   = fill.get("symbol", "")
        side     = fill.get("side", "")
        price    = float(fill.get("price", 0) or 0)
        qty      = float(fill.get("quantity", 0) or 0)
        # commission is already abs-valued in publish_fill; use abs() as belt-and-suspenders
        fee      = abs(float(fill.get("commission", 0) or fill.get("fill_fee", 0) or 0))

        if not strategy or not symbol or qty <= 0 or price <= 0:
            return

        state   = self._get_state(strategy)
        pos_key = f"{gateway}:{symbol}"
        if pos_key not in state.positions:
            state.positions[pos_key] = SymbolPosition(
                symbol=symbol, gateway=gateway, strategy=strategy
            )
        state.positions[pos_key].apply_fill(side, qty, price, fee)
        state.refresh_pnl()

        await self.redis.log_trade(fill)
        await self._snapshot_pnl(strategy, state, force=True)

    # Stablecoins whose USD price is always treated as 1.0
    _STABLES = frozenset({"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD"})

    async def _on_tick(self, tick: dict, kill_pub: Publisher) -> None:
        """Mark open positions to market, update unrealized PnL, and maintain
        a per-currency price cache used by the exchange-based PnL calculation."""
        gateway = tick.get("gateway", "")
        symbol  = tick.get("symbol", "")
        # Normalise price field across gateways (okx uses "price", others may use "last")
        price   = float(tick.get("price", 0) or tick.get("last", 0) or 0)

        if not symbol or price <= 0:
            return

        # ── Price cache ──────────────────────────────────────────────────────
        # For pairs like "BTC-USDT", "ETH-USD", "BTC-USDC":
        #   base = "BTC" / "ETH"  → stored at px:BTC in USD
        #   quote must be a USD-equivalent to be a reliable anchor
        # For equity symbols like "AAPL" (no dash): price is already in USD
        try:
            if "-" in symbol:
                base, quote = symbol.split("-", 1)
                # Strip trailing instrument type  (e.g. "BTC-USDT-SWAP" → quote="USDT")
                quote = quote.split("-")[0]
                if quote in self._STABLES or quote == "USD":
                    await self.redis.client.set(f"px:{base}", str(price), ex=3600)
                    # Ensure stablecoins themselves have a recorded price
                    for s in self._STABLES:
                        await self.redis.client.set(f"px:{s}", "1.0", ex=86400)
            else:
                # Equity / single-currency instrument already denominated in USD
                await self.redis.client.set(f"px:{symbol}", str(price), ex=3600)
        except Exception:
            pass

        # ── Mark to market ────────────────────────────────────────────────────
        pos_key  = f"{gateway}:{symbol}"
        now_ms   = int(time.time() * 1000)
        affected = []

        for strat_name, state in self._strategy_states.items():
            pos = state.positions.get(pos_key)
            if pos and pos.net_qty != 0:
                pos.last_price    = price
                pos.last_update_ms = now_ms
                state.refresh_pnl()
                affected.append(strat_name)

        # Throttled snapshot — at most once per second per strategy
        for strat_name in affected:
            await self._snapshot_pnl(strat_name, self._strategy_states[strat_name])

    async def _snapshot_pnl(self, strategy: str, state: StrategyRiskState,
                            force: bool = False) -> None:
        """Write a PnL/positions snapshot to Redis.

        Throttled to at most once per second per strategy unless force=True
        (used after fills to ensure immediate visibility).
        """
        now = time.time()
        if not force and now - self._last_snapshot.get(strategy, 0) < 1.0:
            return
        self._last_snapshot[strategy] = now

        positions_data = {
            key: pos.to_dict()
            for key, pos in state.positions.items()
            if pos.net_qty != 0 or pos.realized_pnl != 0
        }
        snapshot = {
            "strategy":       strategy,
            "realized_pnl":   round(state.realized_pnl, 8),
            "unrealized_pnl": round(state.unrealized_pnl, 8),
            "total_pnl":      round(state.daily_pnl, 8),
            "total_fees":     round(state.total_fees, 8),
            "net_delta":      round(state.net_delta, 2),
            "positions":      positions_data,
            "timestamp":      now,
        }
        try:
            await self.redis.client.set(
                f"pnl:{strategy}", json.dumps(snapshot), ex=86400
            )
        except Exception as e:
            self.logger.debug("PnL snapshot error: %s", e)

    async def _restore_pnl_from_redis(self) -> None:
        """On restart: reload position state from the last Redis snapshot.

        This restores realized_pnl, net_qty, and avg_entry_price so that
        risk limits remain accurate across process restarts.
        Unrealized PnL rebuilds naturally as ticks arrive.
        """
        try:
            keys = await self.redis.client.keys("pnl:*")
            for key in keys:
                raw = await self.redis.client.get(key)
                if not raw:
                    continue
                data    = json.loads(raw)
                strategy = data.get("strategy", "")
                if not strategy:
                    continue
                state = self._get_state(strategy)
                for pos_key, pd in data.get("positions", {}).items():
                    pos = SymbolPosition(
                        symbol          = pd.get("symbol", ""),
                        gateway         = pd.get("gateway", ""),
                        strategy        = strategy,
                        net_qty         = float(pd.get("net_qty", 0)),
                        avg_entry_price = float(pd.get("avg_entry_price", 0)),
                        realized_pnl    = float(pd.get("realized_pnl", 0)),
                        total_fees      = float(pd.get("total_fees", 0)),
                        last_price      = float(pd.get("last_price", 0)),
                    )
                    state.positions[pos_key] = pos
                state.refresh_pnl()
                self.logger.info(
                    "Restored PnL for '%s': realized=%.4f unrealized≈%.4f",
                    strategy, state.realized_pnl, state.unrealized_pnl,
                )
        except Exception as e:
            self.logger.warning("Could not restore PnL from Redis: %s", e)

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
