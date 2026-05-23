"""
Simple momentum strategy example.
Tracks recent price changes and enters when momentum exceeds threshold.
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from strategy.base import BaseStrategy, BaseParams, BaseStatus, periodic


@dataclass
class Params(BaseParams):
    lookback: int = 20
    threshold: float = 0.02
    order_size: float = 0.01


@dataclass
class Status(BaseStatus):
    momentum: float = 0.0
    position: float = 0.0
    tick_count: int = 0


class MomentumStrategy(BaseStrategy):

    Params = Params
    Status = Status

    def __init__(self, name: str, gateway: str, symbols: list, **kwargs):
        super().__init__(name=name, gateway=gateway, symbols=symbols, **kwargs)
        self._prices: dict[str, deque] = {
            s: deque(maxlen=self.params.lookback) for s in self.symbols
        }

    def on_tick(self, symbol: str, tick: dict) -> Optional[dict]:
        last = tick.get("last", 0)
        if last <= 0:
            return None

        prices = self._prices.get(symbol)
        if prices is None:
            self._prices[symbol] = deque(maxlen=self.params.lookback)
            prices = self._prices[symbol]
        prices.append(last)

        self.status.tick_count = self._tick_count

        if len(prices) < self.params.lookback:
            return None

        oldest = prices[0]
        momentum = (last - oldest) / oldest
        self.status.momentum = round(momentum, 6)
        self.status.position = 0.0

        if momentum > self.params.threshold:
            return {
                "side": "buy",
                "price": tick.get("ask", last),
                "quantity": self.params.order_size,
                "order_type": "limit",
                "reason": f"momentum={momentum:.4f} > {self.params.threshold}",
            }

        if momentum < -self.params.threshold:
            return {
                "side": "sell",
                "price": tick.get("bid", last),
                "quantity": self.params.order_size,
                "order_type": "limit",
                "reason": f"momentum={momentum:.4f} < -{self.params.threshold}",
            }

        return None
