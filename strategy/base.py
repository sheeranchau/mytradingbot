"""
Base strategy class.
"""
from abc import ABC, abstractmethod
from typing import Optional


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.
    Strategies receive market data and emit signals — they never send orders directly.
    """

    # Subclasses declare which params are tunable at runtime.
    # Format: {"param_name": {"type": "float"|"int"|"bool", "desc": "..."}}
    PARAMS_SCHEMA: dict = {}

    def __init__(self, name: str, gateway: str, symbols: list[str]):
        self.name = name
        self.gateway = gateway
        self.symbols = symbols
        self.enabled = True
        self.positions: dict[str, float] = {}  # symbol -> qty
        self._last_signal: Optional[dict] = None
        self._signal_count = 0
        self._tick_count = 0

    def get_params(self) -> dict:
        """Return current tunable parameters and their values."""
        return {key: getattr(self, key) for key in self.PARAMS_SCHEMA}

    def set_params(self, updates: dict) -> dict:
        """Update parameters at runtime. Returns the applied changes."""
        applied = {}
        for key, value in updates.items():
            if key not in self.PARAMS_SCHEMA:
                continue
            schema = self.PARAMS_SCHEMA[key]
            ptype = schema.get("type", "float")
            if ptype == "float":
                value = float(value)
            elif ptype == "int":
                value = int(value)
            elif ptype == "bool":
                value = value if isinstance(value, bool) else str(value).lower() in ("true", "1")
            setattr(self, key, value)
            applied[key] = value
        return applied

    def get_state(self) -> dict:
        """Return full strategy state for the dashboard."""
        return {
            "name": self.name,
            "gateway": self.gateway,
            "symbols": self.symbols,
            "enabled": self.enabled,
            "positions": dict(self.positions),
            "params": self.get_params(),
            "params_schema": self.PARAMS_SCHEMA,
            "tick_count": self._tick_count,
            "signal_count": self._signal_count,
            "last_signal": self._last_signal,
        }

    @abstractmethod
    def on_tick(self, symbol: str, tick: dict) -> Optional[dict]:
        """
        Called on every tick. Return a signal dict or None.
        Signal format: {"side": "buy"|"sell", "symbol": ..., "price": ..., "quantity": ..., "reason": ...}
        """
        ...

    def on_orderbook(self, symbol: str, book: dict) -> Optional[dict]:
        """Called on orderbook update. Override if needed."""
        return None

    def on_kline(self, symbol: str, kline: dict) -> Optional[dict]:
        """Called on kline update. Override if needed."""
        return None

    def on_fill(self, fill: dict) -> None:
        """Called when a fill belongs to this strategy. Update internal state."""
        symbol = fill.get("symbol", "")
        qty = fill.get("quantity", 0)
        side = fill.get("side", "")
        if side == "buy":
            self.positions[symbol] = self.positions.get(symbol, 0) + qty
        else:
            self.positions[symbol] = self.positions.get(symbol, 0) - qty

    def on_position(self, position: dict) -> None:
        """Called on position snapshot from gateway. Reconcile state."""
        symbol = position.get("symbol", "")
        self.positions[symbol] = position.get("quantity", 0)
