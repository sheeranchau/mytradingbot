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
        """Called when an order belonging to this strategy changes state."""
        pass

    def on_fill(self, fill: dict) -> None:
        """Called when a fill belongs to this strategy."""
        pass

    # back-compat alias
    def on_orderbook(self, symbol: str, book: dict) -> Optional[dict]:
        return self.on_depth(symbol, book)

    def on_kline(self, symbol: str, kline: dict) -> Optional[dict]:
        return None


def periodic(interval: float):
    """Decorator to mark a method as a periodic task."""
    def decorator(fn):
        fn._periodic_interval = interval
        return fn
    return decorator
