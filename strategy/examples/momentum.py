"""
Simple momentum strategy example.
Tracks recent price changes and enters when momentum exceeds threshold.
"""
from collections import deque
from typing import Optional

from strategy.base import BaseStrategy


class MomentumStrategy(BaseStrategy):
    """
    Enters long when price moves up by `threshold` pct over `lookback` ticks.
    Enters short when price drops by `threshold` pct.
    Flat position before reversing.
    """

    def __init__(self, name: str, gateway: str, symbols: list[str],
                 lookback: int = 20, threshold: float = 0.02,
                 order_size: float = 0.01):
        super().__init__(name=name, gateway=gateway, symbols=symbols)
        self.lookback = lookback
        self.threshold = threshold
        self.order_size = order_size
        # Per-symbol price history
        self._prices: dict[str, deque] = {
            s: deque(maxlen=lookback) for s in symbols
        }

    def on_tick(self, symbol: str, tick: dict) -> Optional[dict]:
        last = tick.get("last", 0)
        if last <= 0:
            return None

        prices = self._prices[symbol]
        prices.append(last)

        if len(prices) < self.lookback:
            return None

        # Momentum = pct change from oldest to newest
        oldest = prices[0]
        momentum = (last - oldest) / oldest
        current_pos = self.positions.get(symbol, 0)

        # Long signal
        if momentum > self.threshold and current_pos <= 0:
            # Flatten short first if needed
            qty = self.order_size + abs(min(current_pos, 0))
            return {
                "side": "buy",
                "price": tick.get("ask", last),
                "quantity": qty,
                "order_type": "limit",
                "reason": f"momentum={momentum:.4f} > {self.threshold}",
            }

        # Short signal
        if momentum < -self.threshold and current_pos >= 0:
            qty = self.order_size + abs(max(current_pos, 0))
            return {
                "side": "sell",
                "price": tick.get("bid", last),
                "quantity": qty,
                "order_type": "limit",
                "reason": f"momentum={momentum:.4f} < -{self.threshold}",
            }

        return None
