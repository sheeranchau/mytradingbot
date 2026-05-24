"""
FUTU (OpenD) gateway for US and HK stock trading.
Uses futu-api package which connects to a local OpenD daemon.

Order lifecycle tracking
------------------------
FUTU's OpenD API does not provide a per-fill ID.  Order state is tracked
by polling ``order_list_query()`` every ``ORDER_POLL_INTERVAL`` seconds for
all active orders.  When an increase in ``dealt_qty`` is detected a fill
event is synthesised with a local fill_id of ``{order_id}-{now_ms}``.

FUTU OrderStatus → OrderState mapping:
  SUBMITTED               → OPEN
  FILLED_PART             → PARTIAL
  FILLED_ALL              → FILLED
  CANCELLED_ALL / _PART   → CANCELLED
  FAILED / SUBMIT_FAILED  → REJECTED
  DISABLED / DELETED      → CANCELLED
"""
import asyncio
import time
from typing import Optional

from gateway.base import BaseGateway
from core.models import OrderUpdate, OrderState

try:
    from futu import (
        OpenQuoteContext, OpenSecTradeContext,
        RET_OK, TrdSide, OrderType, TrdEnv,
        StockQuoteHandlerBase, OrderBookHandlerBase,
    )
    FUTU_AVAILABLE = True
except ImportError:
    FUTU_AVAILABLE = False


# FUTU order status strings → normalised OrderState.
# futu-api returns the enum name as a string from the DataFrame.
_FUTU_STATE: dict[str, str] = {
    "SUBMITTED":       OrderState.OPEN,
    "FILLED_PART":     OrderState.PARTIAL,
    "FILLED_ALL":      OrderState.FILLED,
    "CANCELLED_ALL":   OrderState.CANCELLED,
    "CANCELLED_PART":  OrderState.CANCELLED,
    "FAILED":          OrderState.REJECTED,
    "SUBMIT_FAILED":   OrderState.REJECTED,
    "TIME_OUT":        OrderState.REJECTED,
    "DISABLED":        OrderState.CANCELLED,
    "DELETED":         OrderState.CANCELLED,
    # Transient states — treat as still open
    "UNSUBMITTED":     OrderState.PENDING_SUBMIT,
    "WAITING_SUBMIT":  OrderState.PENDING_SUBMIT,
    "SUBMITTING":      OrderState.PENDING_SUBMIT,
    "CANCELLING_PART": OrderState.OPEN,
    "CANCELLING_ALL":  OrderState.OPEN,
}

# Seconds between order-status polls
ORDER_POLL_INTERVAL = 2.0


class FUTUGateway(BaseGateway):
    """
    FUTU OpenD gateway.
    Requires OpenD running locally (default: 127.0.0.1:11111).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 11111,
                 symbols: list[str] = None, market: str = "US",
                 trade_env: str = "SIMULATE",
                 unlock_password: str = "", **kwargs):
        super().__init__(gateway_name="futu", **kwargs)
        self.host = host
        self.port = port
        self.symbols = symbols or []
        self.market = market
        self.trade_env = trade_env
        self.unlock_password = unlock_password

        mode = "PAPER" if trade_env == "SIMULATE" else "REAL"
        self.logger.info("Initialized in %s mode, market=%s", mode, market)
        self._quote_ctx: Optional[object] = None
        self._trade_ctx: Optional[object] = None

        # Track last-seen dealt_qty per order_id to detect incremental fills
        self._last_dealt_qty: dict[str, float] = {}
        # Tracks the previous avg_fill_price so we can back-calculate the
        # marginal execution price for each incremental fill slice.
        self._last_avg_price: dict[str, float] = {}

    async def connect_exchange(self) -> None:
        if not FUTU_AVAILABLE:
            raise ImportError("futu-api not installed. Run: pip install futu-api")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._connect_sync)

        # Reconcile pending orders from last session, then start polling
        await self._sync_pending_orders()
        asyncio.create_task(self._poll_orders_loop())

    def _connect_sync(self) -> None:
        self._quote_ctx = OpenQuoteContext(host=self.host, port=self.port)
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        self._trade_ctx = OpenSecTradeContext(
            host=self.host, port=self.port, filter_trdmarket=self.market
        )
        if self.trade_env == "REAL":
            if not self.unlock_password:
                raise ValueError("[FUTU] unlock_password required for REAL trading")
            ret, msg = self._trade_ctx.unlock_trade(password=self.unlock_password)
            if ret != RET_OK:
                raise ConnectionError(f"[FUTU] Failed to unlock trading: {msg}")

    # ──────────────────────────────────────────────────────────────────────
    # Market data
    # ──────────────────────────────────────────────────────────────────────
    async def _stream_market_data(self) -> None:
        """Poll market data (FUTU push is callback-based, we adapt to async)."""
        loop = asyncio.get_event_loop()
        while self.running:
            for symbol in self.symbols:
                try:
                    quote = await loop.run_in_executor(None, self._get_quote, symbol)
                    if quote:
                        await self.publish_tick(symbol, quote)
                except Exception as e:
                    self.logger.error("Quote error for %s: %s", symbol, e)
            await asyncio.sleep(0.5)

    def _get_quote(self, symbol: str) -> Optional[dict]:
        ret, data = self._quote_ctx.get_stock_quote([symbol])
        if ret == RET_OK and not data.empty:
            row = data.iloc[0]
            return {
                "bid":       float(row.get("bid_price", 0)),
                "ask":       float(row.get("ask_price", 0)),
                "bid_size":  float(row.get("bid_vol", 0)),
                "ask_size":  float(row.get("ask_vol", 0)),
                "last":      float(row.get("last_price", 0)),
                "last_size": float(row.get("volume", 0)),
                "timestamp": time.time(),
            }
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Startup reconciliation + polling
    # ──────────────────────────────────────────────────────────────────────
    async def _sync_pending_orders(self) -> None:
        """Reconcile Redis state against FUTU OpenD on startup / reconnect.

        Three things happen here:
        1. Rebuild ``_cl_to_ord`` / ``_ord_to_cl`` maps from persisted Redis
           data (FUTU doesn't store clOrdId, so the maps are always lost on
           restart and must be recovered from Redis).
        2. Seed ``_last_dealt_qty`` from the persisted ``filled_qty`` value so
           the first poll does not emit spurious fill events for quantity that
           was already dealt before the restart.
        3. Query each order from FUTU to catch any state changes (fills,
           cancels) that happened while the gateway was offline.
        """
        try:
            import json
            loop = asyncio.get_event_loop()
            r = self.redis.client
            raw = await r.get(f"pending_orders:{self.gateway_name}")
            pending: list[dict] = json.loads(raw) if raw else []

            for o in pending:
                order_id  = o.get("order_id", "")
                cl_ord_id = o.get("client_order_id", "")
                # 1. Rebuild ID maps
                if order_id and cl_ord_id:
                    self._cl_to_ord[cl_ord_id] = order_id
                    self._ord_to_cl[order_id]  = cl_ord_id
                # 2. Seed last_dealt_qty / last_avg_price to prevent spurious
                #    fills and wrong marginal prices on the first poll.
                if order_id:
                    self._last_dealt_qty[order_id] = float(o.get("filled_qty", 0) or 0)
                    self._last_avg_price[order_id] = float(o.get("avg_fill_price", 0) or 0)

            # 3. Query exchange for each order to catch offline changes
            checked = 0
            for o in pending:
                order_id = o.get("order_id", "")
                if not order_id:
                    continue
                try:
                    result = await loop.run_in_executor(
                        None, self._query_single_order_sync, order_id
                    )
                    if result:
                        await self._handle_order_poll(o, result)
                        checked += 1
                except Exception as exc:
                    self.logger.debug("Startup sync order %s: %s", order_id, exc)

            self.logger.info("Startup sync: %d pending orders, %d queried from exchange",
                             len(pending), checked)
        except Exception as e:
            self.logger.error("Failed to sync pending orders on startup: %s", e)

    async def _poll_orders_loop(self) -> None:
        """Periodically query FUTU OpenD for the status of all tracked orders."""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                await asyncio.sleep(ORDER_POLL_INTERVAL)
                import json
                r = self.redis.client
                raw = await r.get("pending_orders:futu")
                pending: list[dict] = json.loads(raw) if raw else []
                for cached in pending:
                    order_id = cached.get("order_id", "")
                    if not order_id:
                        continue
                    try:
                        result = await loop.run_in_executor(
                            None, self._query_single_order_sync, order_id
                        )
                        if result:
                            await self._handle_order_poll(cached, result)
                    except Exception as exc:
                        self.logger.debug("Poll order %s error: %s", order_id, exc)
            except Exception as exc:
                self.logger.debug("_poll_orders_loop error: %s", exc)

    def _query_single_order_sync(self, order_id: str) -> Optional[dict]:
        """Query a single order from FUTU OpenD. Returns a flat dict or None."""
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        try:
            ret, data = self._trade_ctx.order_list_query(
                order_id=order_id, trd_env=trd_env
            )
            if ret == RET_OK and not data.empty:
                row = data.iloc[0]
                return {
                    "order_id":    str(row.get("order_id", "")),
                    "order_status": str(row.get("order_status", "")),
                    "dealt_qty":   float(row.get("dealt_qty", 0) or 0),
                    "dealt_avg_price": float(row.get("dealt_avg_price", 0) or 0),
                    "qty":         float(row.get("qty", 0) or 0),
                    "price":       float(row.get("price", 0) or 0),
                    "updated_time": str(row.get("updated_time", "")),
                }
        except Exception as exc:
            self.logger.debug("_query_single_order_sync(%s) error: %s", order_id, exc)
        return None

    async def _handle_order_poll(self, cached: dict, result: dict) -> None:
        """Process a single polled order result and emit an OrderUpdate if changed."""
        order_id   = result["order_id"] or cached.get("order_id", "")
        cl_ord_id  = cached.get("client_order_id", self._ord_to_cl.get(order_id, ""))
        raw_status = result["order_status"]
        state      = _FUTU_STATE.get(raw_status, OrderState.OPEN)

        new_dealt  = result["dealt_qty"]
        avg_price  = result["dealt_avg_price"]
        prev_dealt = self._last_dealt_qty.get(order_id, 0.0)
        fill_delta = new_dealt - prev_dealt

        fill_id = ""
        marginal_price = 0.0
        if fill_delta > 0:
            fill_id = f"{order_id}-{int(time.time() * 1000)}"
            # Back-calculate the marginal execution price for this slice so that
            # SymbolPosition.apply_fill gets the correct per-fill price rather
            # than the cumulative VWAP (which corrupts cost basis on partial fills).
            prev_avg = self._last_avg_price.get(order_id, 0.0)
            if prev_dealt > 0 and prev_avg > 0:
                marginal_price = (avg_price * new_dealt - prev_avg * prev_dealt) / fill_delta
            else:
                marginal_price = avg_price  # first fill: avg == marginal
            self._last_dealt_qty[order_id] = new_dealt
            self._last_avg_price[order_id] = avg_price

        update = OrderUpdate(
            client_order_id = cl_ord_id,
            order_id        = order_id,
            gateway         = self.gateway_name,
            strategy        = cached.get("strategy", ""),
            symbol          = cached.get("symbol", ""),
            side            = cached.get("side", ""),
            order_type      = cached.get("order_type", "limit"),
            price           = float(cached.get("price", 0) or 0),
            quantity        = float(cached.get("quantity", 0) or 0),
            state           = state,
            filled_qty      = new_dealt,
            avg_fill_price  = avg_price,
            fill_id         = fill_id,
            fill_price      = marginal_price if fill_id else 0.0,
            fill_qty        = fill_delta if fill_id else 0.0,
            update_time_ms  = int(time.time() * 1000),
            update_source   = "rest_poll",
        )
        await self._process_order_update(update)

    # ──────────────────────────────────────────────────────────────────────
    # Orders
    # ──────────────────────────────────────────────────────────────────────
    async def send_order(self, order: dict) -> str:
        loop = asyncio.get_event_loop()

        strategy  = order.get("strategy", "")
        cl_ord_id = order.get("client_order_id") or self._gen_cl_ord_id(strategy)
        now_ms    = int(time.time() * 1000)

        # Record PENDING_SUBMIT before hitting the exchange
        await self._process_order_update(OrderUpdate(
            client_order_id = cl_ord_id,
            order_id        = "",
            gateway         = self.gateway_name,
            strategy        = strategy,
            symbol          = order["symbol"],
            side            = order["side"],
            order_type      = order.get("order_type", "limit"),
            price           = float(order.get("price", 0)),
            quantity        = float(order["quantity"]),
            state           = OrderState.PENDING_SUBMIT,
            create_time_ms  = now_ms,
            update_time_ms  = now_ms,
            update_source   = "rest",
        ))

        order_id = await loop.run_in_executor(None, self._place_order_sync, order)

        if not order_id:
            await self._process_order_update(OrderUpdate(
                client_order_id = cl_ord_id,
                order_id        = "",
                gateway         = self.gateway_name,
                strategy        = strategy,
                symbol          = order["symbol"],
                side            = order["side"],
                order_type      = order.get("order_type", "limit"),
                price           = float(order.get("price", 0)),
                quantity        = float(order["quantity"]),
                state           = OrderState.REJECTED,
                update_time_ms  = int(time.time() * 1000),
                update_source   = "rest",
            ))
            return ""

        self._cl_to_ord[cl_ord_id] = order_id
        self._ord_to_cl[order_id]  = cl_ord_id
        self._last_dealt_qty[order_id] = 0.0
        self._last_avg_price[order_id] = 0.0

        self.logger.info("Order placed: %s %s %s @ %s -> clOrdId=%s ordId=%s",
                         order["side"], order["quantity"], order["symbol"],
                         order.get("price", "MKT"), cl_ord_id, order_id)

        await self._process_order_update(OrderUpdate(
            client_order_id = cl_ord_id,
            order_id        = order_id,
            gateway         = self.gateway_name,
            strategy        = strategy,
            symbol          = order["symbol"],
            side            = order["side"],
            order_type      = order.get("order_type", "limit"),
            price           = float(order.get("price", 0)),
            quantity        = float(order["quantity"]),
            state           = OrderState.OPEN,
            create_time_ms  = now_ms,
            update_time_ms  = int(time.time() * 1000),
            update_source   = "rest",
        ))
        return order_id

    def _place_order_sync(self, order: dict) -> str:
        side     = TrdSide.BUY if order["side"] == "buy" else TrdSide.SELL
        ord_type = OrderType.NORMAL if order.get("order_type") == "limit" else OrderType.MARKET
        trd_env  = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        ret, data = self._trade_ctx.place_order(
            price=order.get("price", 0),
            qty=order["quantity"],
            code=order["symbol"],
            trd_side=side,
            order_type=ord_type,
            trd_env=trd_env,
        )
        if ret == RET_OK:
            return str(data.iloc[0]["order_id"])
        else:
            self.logger.error("Order failed: %s", data)
            return ""

    async def cancel_order(self, order: dict) -> None:
        order_id  = order.get("order_id", "")
        cl_ord_id = order.get("client_order_id", "") or self._ord_to_cl.get(order_id, "")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._cancel_sync, order_id)

        # Retrieve cached order for metadata
        import json
        r = self.redis.client
        raw = await r.get("pending_orders:futu")
        orders: list[dict] = json.loads(raw) if raw else []
        cached = next((o for o in orders
                       if o.get("client_order_id") == cl_ord_id
                       or o.get("order_id") == order_id), {})

        await self._process_order_update(OrderUpdate(
            client_order_id = cl_ord_id,
            order_id        = order_id,
            gateway         = self.gateway_name,
            strategy        = cached.get("strategy", ""),
            symbol          = order.get("symbol", cached.get("symbol", "")),
            side            = cached.get("side", ""),
            order_type      = cached.get("order_type", "limit"),
            price           = float(cached.get("price", 0) or 0),
            quantity        = float(cached.get("quantity", 0) or 0),
            filled_qty      = float(cached.get("filled_qty", 0) or 0),
            avg_fill_price  = float(cached.get("avg_fill_price", 0) or 0),
            state           = OrderState.CANCELLED,
            update_time_ms  = int(time.time() * 1000),
            update_source   = "rest",
        ))

    def _cancel_sync(self, order_id: str) -> None:
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        self._trade_ctx.modify_order(
            modify_order_op=2,  # cancel
            order_id=order_id,
            qty=0, price=0,
            trd_env=trd_env,
        )

    async def amend_order(self, order: dict) -> None:
        order_id = order.get("order_id", "")
        price    = order.get("price", 0)
        qty      = order.get("quantity", 0)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._amend_sync, order_id, price, qty)

    def _amend_sync(self, order_id: str, price: float, qty: float) -> None:
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        self._trade_ctx.modify_order(
            modify_order_op=1,  # amend
            order_id=order_id,
            qty=qty, price=price,
            trd_env=trd_env,
        )

    async def query_positions(self) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._query_positions_sync)

    def _query_positions_sync(self) -> list[dict]:
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        ret, data = self._trade_ctx.position_list_query(trd_env=trd_env)
        if ret == RET_OK and not data.empty:
            positions = []
            for _, row in data.iterrows():
                positions.append({
                    "symbol":          row["code"],
                    "quantity":        float(row["qty"]),
                    "avg_price":       float(row["cost_price"]),
                    "market_price":    float(row["market_val"]),
                    "unrealized_pnl":  float(row["pl_val"]),
                })
            return positions
        return []

    async def disconnect_exchange(self) -> None:
        if self._quote_ctx:
            self._quote_ctx.close()
        if self._trade_ctx:
            self._trade_ctx.close()
