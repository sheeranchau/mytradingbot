"""
Orderbook spread strategy example.
Demonstrates using on_depth() instead of on_tick().
Trades when bid-ask spread widens beyond a threshold (mean-reversion on spread).
"""
from collections import deque
from dataclasses import dataclass
from typing import Optional

from strategy.base import BaseStrategy, BaseParams, BaseStatus


@dataclass
class Params(BaseParams):
    lookback: int = 100
    z_threshold: float = 2.0
    order_size: float = 0.01


@dataclass
class Status(BaseStatus):
    spread_bps: float = 0.0
    spread_z: float = 0.0
    position: float = 0.0


class SpreadArbStrategy(BaseStrategy):
    """
    Monitors the bid-ask spread. When spread is abnormally wide (> mean + n*std),
    posts limit orders on both sides expecting reversion.
    """

    Params = Params
    Status = Status

    def __init__(self, name: str, gateway: str, symbols: list, **kwargs):
        super().__init__(name=name, gateway=gateway, symbols=symbols, **kwargs)
        self._spreads: dict[str, deque] = {
            s: deque(maxlen=self.params.lookback) for s in self.symbols
        }

    def on_depth(self, symbol: str, book: dict) -> Optional[dict]:
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid

        if mid <= 0:
            return None

        spread_bps = spread / mid * 10000
        spreads = self._spreads.get(symbol)
        if spreads is None:
            self._spreads[symbol] = deque(maxlen=self.params.lookback)
            spreads = self._spreads[symbol]
        spreads.append(spread_bps)

        self.status.spread_bps = round(spread_bps, 2)

        if len(spreads) < self.params.lookback:
            return None

        mean = sum(spreads) / len(spreads)
        variance = sum((s - mean) ** 2 for s in spreads) / len(spreads)
        std = variance ** 0.5

        if std == 0:
            return None

        z = (spread_bps - mean) / std
        self.status.spread_z = round(z, 3)

        if z > self.params.z_threshold:
            return {
                "side": "buy",
                "price": best_bid,
                "quantity": self.params.order_size,
                "order_type": "limit",
                "reason": f"spread_z={z:.2f} > {self.params.z_threshold}, spread={spread_bps:.1f}bps",
            }

        return None
