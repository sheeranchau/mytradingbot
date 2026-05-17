"""
Base strategy class and strategy container.
"""
from abc import ABC, abstractmethod
from typing import Optional


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.
    Strategies receive market data and emit signals — they never send orders directly.
    """

    def __init__(self, name: str, gateway: str, symbols: list[str]):
        self.name = name
        self.gateway = gateway
        self.symbols = symbols
        self.enabled = True
        self.positions: dict[str, float] = {}  # symbol -> qty

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
