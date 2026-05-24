"""
Base strategy class (v2).

Strategies are dataclass-driven: Params (user-editable) and Status (read-only output).
The container provides rich access: market depth, order management, balance, positions.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields, asdict, field
from typing import Optional, Any
import time
import asyncio


@dataclass
class BaseParams:
    """Subclass this with your strategy's tunable parameters."""
    pass


@dataclass
class BaseStatus:
    """Subclass this with your strategy's observable output."""
    pass


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.

    Subclasses must define:
        Params: a dataclass inheriting BaseParams
        Status: a dataclass inheriting BaseStatus
        on_tick / on_depth / on_order_update: callbacks
    """

    Params: type = BaseParams
    Status: type = BaseStatus

    def __init__(self, name: str, gateway: str, symbols: list, **init_kwargs):
        self.name = name
        self.gateway = gateway
        self.symbols = symbols if isinstance(symbols, list) else [symbols]
        self.enabled = True

        self.params = self._build_dataclass(self.Params, init_kwargs)
        self.status = self.Status()

        self._container = None
        self._periodic_tasks: list[dict] = []
        self._tick_count = 0
        self._signal_count = 0
        self._last_signal: Optional[dict] = None

        # In-strategy position tracking (accumulated from on_fill)
        self.positions: dict[str, float] = {}
        # Open order tracking (maintained by on_order_update)
        self._open_orders: dict[str, dict] = {}
        # Multi-symbol state caches (populated by container before dispatch)
        self._latest_ticks: dict[str, dict] = {}
        self._latest_books: dict[str, dict] = {}

        self._register_periodic_methods()

    def _build_dataclass(self, cls, overrides: dict):
        valid = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in overrides.items() if k in valid}
        return cls(**filtered)

    def _register_periodic_methods(self):
        for attr_name in dir(self):
            attr = getattr(self, attr_name, None)
            if callable(attr) and hasattr(attr, "_periodic_interval"):
                self._periodic_tasks.append({
                    "fn": attr,
                    "interval": attr._periodic_interval,
                })

    def set_container(self, container):
        self._container = container

    # ── Params / Status serialization ────────────────────────────────────

    def get_params_dict(self) -> dict:
        return asdict(self.params)

    def get_status_dict(self) -> dict:
        return asdict(self.status)

    def get_params_schema(self) -> list[dict]:
        result = []
        for f in fields(self.Params):
            entry = {"name": f.name, "type": f.type if isinstance(f.type, str) else f.type.__name__}
            if f.metadata:
                entry["desc"] = f.metadata.get("desc", "")
            result.append(entry)
        return result

    def get_status_schema(self) -> list[dict]:
        result = []
        for f in fields(self.Status):
            entry = {"name": f.name, "type": f.type if isinstance(f.type, str) else f.type.__name__}
            if f.metadata:
                entry["desc"] = f.metadata.get("desc", "")
            result.append(entry)
        return result

    def set_params(self, updates: dict) -> dict:
        applied = {}
        for f in fields(self.Params):
            if f.name in updates:
                val = updates[f.name]
                ftype = f.type if isinstance(f.type, str) else f.type.__name__
                if ftype == "int":
                    val = int(val)
                elif ftype == "float":
                    val = float(val)
                elif ftype == "bool":
                    val = val if isinstance(val, bool) else str(val).lower() in ("true", "1")
                elif ftype == "str":
                    val = str(val)
                setattr(self.params, f.name, val)
                applied[f.name] = val
        return applied

    def get_state(self) -> dict:
        return {
            "name": self.name,
            "gateway": self.gateway,
            "symbols": self.symbols,
            "enabled": self.enabled,
            "params": self.get_params_dict(),
            "params_schema": self.get_params_schema(),
            "status": self.get_status_dict(),
            "status_schema": self.get_status_schema(),
            "tick_count": self._tick_count,
            "signal_count": self._signal_count,
            "last_signal": self._last_signal,
        }

    # ── Container-provided capabilities (proxied) ────────────────────────

    async def place_order(self, symbol: str, side: str, price: float,
                          quantity: float, order_type: str = "limit") -> None:
        if self._container:
            await self._container.strategy_place_order(
                self.name, self.gateway, symbol, side, price, quantity, order_type)

    async def cancel_order(self, order_id: str, symbol: str) -> None:
        if self._container:
            await self._container.strategy_cancel_order(
                self.name, self.gateway, order_id, symbol)

    async def amend_order(self, order_id: str, symbol: str,
                          new_price: float = None, new_qty: float = None) -> None:
        if self._container:
            await self._container.strategy_amend_order(
                self.name, self.gateway, order_id, symbol, new_price, new_qty)

    async def get_positions(self) -> list[dict]:
        if self._container:
            return await self._container.strategy_get_positions(self.gateway)
        return []

    async def get_balance(self) -> dict:
        if self._container:
            return await self._container.strategy_get_balance(self.gateway)
        return {}

    def get_depth(self, symbol: str) -> dict:
        if self._container:
            return self._container.strategy_get_depth(self.gateway, symbol)
        return {"bids": [], "asks": []}

    # ── Callbacks (override in subclass) ─────────────────────────────────

    def on_tick(self, symbol: str, tick: dict) -> Optional[dict]:
        """Called on every tick. Return a signal dict or None."""
        return None

    def on_depth(self, symbol: str, book: dict) -> Optional[dict]:
        """Called on orderbook update. Return a signal dict or None."""
        return None

    def on_order_update(self, order: dict) -> None:
        """Called when an order belonging to this strategy changes state.
        Default maintains _open_orders. Subclasses should call super() first."""
        cl_id = order.get("client_order_id", "")
        if not cl_id:
            return
        state = order.get("state", "")
        if state in ("filled", "cancelled", "rejected", "expired"):
            self._open_orders.pop(cl_id, None)
        else:
            self._open_orders[cl_id] = order

    def on_fill(self, fill: dict) -> None:
        """Called when a fill belongs to this strategy.
        Default accumulates positions. Subclasses should call super() first."""
        symbol = fill.get("symbol", "")
        side = fill.get("side", "")
        qty = float(fill.get("quantity", 0) or 0)
        if not symbol or qty <= 0:
            return
        current = self.positions.get(symbol, 0.0)
        if side == "buy":
            self.positions[symbol] = current + qty
        elif side == "sell":
            self.positions[symbol] = current - qty

    def on_position(self, position: dict) -> None:
        """Reconcile in-memory position with exchange truth."""
        symbol = position.get("symbol", "")
        qty = float(position.get("quantity", 0) or 0)
        if symbol:
            self.positions[symbol] = qty

    def on_funding_rate(self, data: dict) -> None:
        """Called when a funding rate update arrives for a subscribed symbol."""
        pass

    async def on_pause(self) -> None:
        """Called when the strategy is disabled (paused) via dashboard/command.
        Strategy may cancel orders, reduce exposure, etc. Will be re-enabled later."""
        pass

    async def on_stop(self) -> None:
        """Called when the strategy is killed by the risk engine.
        Strategy should cancel all orders and clean up. Not expected to resume."""
        pass

    # back-compat alias
    def on_orderbook(self, symbol: str, book: dict) -> Optional[dict]:
        return self.on_depth(symbol, book)

    def on_kline(self, symbol: str, kline: dict) -> Optional[dict]:
        return None

    # ── Open order helpers ───────────────────────────────────────────────

    def get_open_orders(self, symbol: str = None, side: str = None) -> list[dict]:
        orders = list(self._open_orders.values())
        if symbol:
            orders = [o for o in orders if o.get("symbol") == symbol]
        if side:
            orders = [o for o in orders if o.get("side") == side]
        return orders

    def has_open_order(self, symbol: str, side: str = None) -> bool:
        return len(self.get_open_orders(symbol=symbol, side=side)) > 0

    # ── Position helpers ─────────────────────────────────────────────────

    def get_position(self, symbol: str) -> float:
        return self.positions.get(symbol, 0.0)

    # ── Multi-symbol state helpers ───────────────────────────────────────

    def get_cached_tick(self, symbol: str) -> dict:
        return self._latest_ticks.get(symbol, {})

    def get_cached_book(self, symbol: str) -> dict:
        return self._latest_books.get(symbol, {})

    def all_books_ready(self, *symbols: str) -> bool:
        return all(s in self._latest_books for s in symbols)

    # ── Instrument & fee helpers ─────────────────────────────────────────

    async def get_instrument(self, symbol: str) -> dict:
        if self._container:
            return await self._container.strategy_get_instrument(self.gateway, symbol)
        return {}

    async def get_instruments(self) -> dict:
        if self._container:
            return await self._container.strategy_get_instruments(self.gateway)
        return {}

    async def get_fee_rates(self) -> dict:
        if self._container:
            return await self._container.strategy_get_fee_rates(self.gateway)
        return {}


def periodic(interval: float):
    """Decorator to mark a method as a periodic task."""
    def decorator(fn):
        fn._periodic_interval = interval
        return fn
    return decorator
