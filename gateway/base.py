"""
Base gateway class. Each exchange implements this interface.
"""
import json
import secrets
import time
from abc import ABC, abstractmethod
from dataclasses import asdict

from core.process_base import ProcessBase
from core.zmq_channels import Publisher, Puller, GATEWAY_PORTS
from core.events import EventType
from core.models import OrderUpdate, OrderState
from core.logger import get_logger


class BaseGateway(ProcessBase, ABC):
    """
    Gateway process responsibilities:
    - Connect to exchange (WS/TCP)
    - Publish market data to ZMQ PUB
    - Listen for orders on ZMQ PULL and execute them
    - Report fills back via ZMQ PUB
    """

    def __init__(self, gateway_name: str, **kwargs):
        super().__init__(process_name=f"gateway_{gateway_name}", **kwargs)
        self.gateway_name = gateway_name
        self.logger = get_logger(f"gateway_{gateway_name}")
        self._market_pub = None
        self._order_puller = None
        # clOrdId ↔ ordId correlation maps (populated by each gateway)
        self._cl_to_ord: dict[str, str] = {}
        self._ord_to_cl: dict[str, str] = {}

    async def run(self) -> None:
        if self.gateway_name not in GATEWAY_PORTS:
            raise ValueError(
                f"Gateway '{self.gateway_name}' not registered in GATEWAY_PORTS "
                f"(core/zmq_channels.py). Known: {list(GATEWAY_PORTS)}"
            )
        ports = GATEWAY_PORTS[self.gateway_name]
        self._market_pub = Publisher(ports["market_data"])
        # Each gateway binds its own orders port; risk engine connects per-gateway.
        self._order_puller = Puller(ports["orders"], bind=True)
        self.logger.info("ZMQ ports: market_data=%d, orders=%d",
                         ports["market_data"], ports["orders"])

        await self.connect_exchange()

        await asyncio.gather(
            self._listen_orders(),
            self._stream_market_data(),
        )

    async def _listen_orders(self) -> None:
        """Pull orders from risk engine and execute. Each gateway has its own
        order port, so no gateway-name filtering is required."""
        import asyncio
        while self.running:
            try:
                order_data = await self._order_puller.pull()
                event_type = order_data.get("event_type")
                if event_type == EventType.ORDER_NEW:
                    await self.send_order(order_data)
                elif event_type == EventType.ORDER_CANCEL:
                    await self.cancel_order(order_data)
                elif event_type == EventType.ORDER_AMEND:
                    await self.amend_order(order_data)
            except Exception as e:
                self.logger.error("Order listener error: %s", e)
                await asyncio.sleep(1)

    async def publish_tick(self, symbol: str, tick: dict) -> None:
        topic = f"tick.{self.gateway_name}.{symbol}"
        tick["event_type"] = EventType.TICK
        tick["gateway"] = self.gateway_name
        tick["symbol"] = symbol
        await self._market_pub.publish(topic, tick)

    async def publish_orderbook(self, symbol: str, book: dict) -> None:
        topic = f"orderbook.{self.gateway_name}.{symbol}"
        book["event_type"] = EventType.ORDERBOOK
        book["gateway"] = self.gateway_name
        book["symbol"] = symbol
        await self._market_pub.publish(topic, book)

    async def publish_funding_rate(self, symbol: str, data: dict) -> None:
        topic = f"funding_rate.{self.gateway_name}.{symbol}"
        data["event_type"] = EventType.FUNDING_RATE
        data["gateway"] = self.gateway_name
        data["symbol"] = symbol
        await self._market_pub.publish(topic, data)

    async def publish_fill(self, fill: dict) -> None:
        topic = f"fill.{self.gateway_name}.{fill.get('symbol', '')}"
        fill["event_type"] = EventType.FILL
        fill["gateway"] = self.gateway_name
        await self._market_pub.publish(topic, fill)

    async def publish_order_update(self, update: OrderUpdate) -> None:
        """Publish an order lifecycle event (open, partial, cancelled…) on ZMQ.

        Only non-fill state changes are published here; fills go through
        publish_fill so subscribers don't receive duplicate notifications.
        Topic: ``order_update.{gateway}.{symbol}``
        """
        topic = f"order_update.{self.gateway_name}.{update.symbol}"
        await self._market_pub.publish(topic, {
            "event_type":       EventType.ORDER_UPDATE,
            "gateway":          self.gateway_name,
            "strategy":         update.strategy,
            "symbol":           update.symbol,
            "client_order_id":  update.client_order_id,
            "order_id":         update.order_id,
            "side":             update.side,
            "order_type":       update.order_type,
            "price":            update.price,
            "quantity":         update.quantity,
            "state":            update.state,
            "filled_qty":       update.filled_qty,
            "avg_fill_price":   update.avg_fill_price,
            "update_time_ms":   update.update_time_ms,
            "timestamp":        time.time(),
        })

    # ── Order lifecycle helpers (shared by all gateways) ─────────────────

    def _gen_cl_ord_id(self, strategy: str = "") -> str:
        """Generate a unique client order ID.

        Format: ``{prefix}-{ts_ms}-{rand4hex}`` (max 32 chars).
        Safe to send as ``clOrdId`` on exchanges that support it (OKX).
        For gateways that don't accept clOrdId it is still used internally
        as the primary key for order tracking.
        """
        prefix = "".join(c for c in (strategy or "manual")[:8] if c.isalnum())
        ts = int(time.time() * 1000)
        rand = secrets.token_hex(2)
        # OKX (and most exchanges) only accept alphanumeric clOrdIds — no hyphens.
        return f"{prefix}{ts}{rand}"[:32]

    async def _process_order_update(self, update: OrderUpdate) -> None:
        """Central handler for all order lifecycle events — shared by all gateways.

        Redis writes per call:
          ``order_updates:{gateway}``  — every state change (full audit trail)
          ``pending_orders:{gateway}`` — live snapshot of non-terminal orders
          ``finished_orders``          — terminal orders, written exactly once
          ``fills:{gateway}``          — individual executions (fill_id present)

        Exactly-once guarantee for ``finished_orders``
        -----------------------------------------------
        The ``was_pending`` flag alone is not sufficient.  Both a REST sync and
        a WS push can read the pending list *before either writes back*, so both
        see ``was_pending=True`` and both would write a duplicate entry.

        We use Redis ``SET NX`` (set-if-not-exists) on a short-lived lock key as
        an atomic one-shot gate: only the first caller to acquire the lock writes
        to ``finished_orders``; the second finds the key already set and skips.
        The 60 s TTL ensures cleanup without manual housekeeping.
        """
        try:
            gw = self.gateway_name
            pending_key = f"pending_orders:{gw}"
            r = self.redis.client
            raw = await r.get(pending_key)
            orders: list[dict] = json.loads(raw) if raw else []

            def _matches(o: dict) -> bool:
                if update.client_order_id:
                    return o.get("client_order_id") == update.client_order_id
                return bool(update.order_id and o.get("order_id") == update.order_id)

            was_pending = any(_matches(o) for o in orders)
            orders = [o for o in orders if not _matches(o)]

            update_dict = update.to_redis_dict()

            # 1. Full audit log — always, for every state transition
            await r.xadd(f"order_updates:{gw}", update_dict, maxlen=5000)

            # 2. Live snapshot
            if not update.is_terminal:
                orders.append(update_dict)
            elif was_pending:
                # 3. Terminal history — exactly once via atomic SET NX lock.
                #    was_pending filters out orders we never tracked (e.g. placed
                #    on another instance).  SET NX prevents the race where both a
                #    WS push and a REST sync read was_pending=True before either
                #    has written the updated pending list back to Redis.
                order_key = update.client_order_id or update.order_id
                lock_key  = f"order_terminal:{gw}:{order_key}"
                acquired  = await r.set(lock_key, "1", nx=True, ex=60)
                if acquired:
                    await r.xadd("finished_orders", update_dict, maxlen=500)

            await r.set(pending_key, json.dumps(orders))

            # 4. Fill-level stream — same NX guard to prevent duplicate fill events
            #    (e.g. if a fill push and a poll arrive within the same tick).
            if update.is_fill:
                fill_lock = f"fill_written:{gw}:{update.fill_id}"
                if await r.set(fill_lock, "1", nx=True, ex=60):
                    await r.xadd(f"fills:{gw}", update_dict, maxlen=2000)

        except Exception as e:
            self.logger.debug("Failed to process order update in Redis: %s", e)

        # ZMQ publish (outside try so errors remain visible)
        # Always publish order lifecycle events so strategies can track state.
        # Fills get their own richer publish_fill call below.
        await self.publish_order_update(update)

        if update.is_fill:
            await self.publish_fill({
                "client_order_id": update.client_order_id,
                "order_id":        update.order_id,
                "fill_id":         update.fill_id,
                "strategy":        update.strategy,
                "symbol":          update.symbol,
                "side":            update.side,
                "price":           update.fill_price,
                "quantity":        update.fill_qty,
                "commission":      update.fill_fee,
                "fill_fee":        update.fill_fee,
                "fill_fee_ccy":    update.fill_fee_ccy,
                "fill_pnl":        update.fill_pnl,
                "fill_time_ms":    update.fill_time_ms,
                "filled_qty":      update.filled_qty,
                "avg_fill_price":  update.avg_fill_price,
                "timestamp":       time.time(),
            })

    @abstractmethod
    async def connect_exchange(self) -> None:
        """Establish connection to exchange."""
        ...

    @abstractmethod
    async def _stream_market_data(self) -> None:
        """Subscribe and stream market data. Calls publish_tick/publish_orderbook."""
        ...

    @abstractmethod
    async def send_order(self, order: dict) -> str:
        """Send order to exchange. Returns exchange order ID."""
        ...

    @abstractmethod
    async def cancel_order(self, order: dict) -> None:
        """Cancel order on exchange."""
        ...

    @abstractmethod
    async def amend_order(self, order: dict) -> None:
        """Amend order on exchange (change price/quantity)."""
        ...

    @abstractmethod
    async def query_positions(self) -> list[dict]:
        """Query current positions from exchange."""
        ...

    async def on_stop(self) -> None:
        if self._market_pub:
            self._market_pub.close()
        if self._order_puller:
            self._order_puller.close()
        await self.disconnect_exchange()

    async def disconnect_exchange(self) -> None:
        """Override if exchange needs explicit disconnect."""
        pass


# Fix missing import in run()
import asyncio
