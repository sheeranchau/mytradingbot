"""
Orderbook spread strategy example.
Demonstrates using on_orderbook() instead of on_tick().
Trades when bid-ask spread widens beyond a threshold (mean-reversion on spread).
"""
from collections import deque
from typing import Optional

from strategy.base import BaseStrategy


class SpreadArbStrategy(BaseStrategy):
    """
    Monitors the bid-ask spread. When spread is abnormally wide (> mean + n*std),
    posts limit orders on both sides expecting reversion.
    Useful on liquid crypto pairs where spread typically stays tight.
    """

    PARAMS_SCHEMA = {
        "lookback": {"type": "int", "desc": "Spread history window size"},
        "z_threshold": {"type": "float", "desc": "Z-score threshold to trigger entry"},
        "order_size": {"type": "float", "desc": "Order size per trade"},
    }

    def __init__(self, name: str, gateway: str, symbols: list[str],
                 lookback: int = 100, z_threshold: float = 2.0,
                 order_size: float = 0.01):
        super().__init__(name=name, gateway=gateway, symbols=symbols)
        self.lookback = lookback
        self.z_threshold = z_threshold
        self.order_size = order_size
        self._spreads: dict[str, deque] = {
            s: deque(maxlen=lookback) for s in symbols
        }

    def on_tick(self, symbol: str, tick: dict) -> Optional[dict]:
        # We use on_orderbook for this strategy, tick is a no-op
        return None

    def on_orderbook(self, symbol: str, book: dict) -> Optional[dict]:
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

        spread_bps = spread / mid * 10000  # in basis points
        spreads = self._spreads[symbol]
        spreads.append(spread_bps)

        if len(spreads) < self.lookback:
            return None

        # Calculate z-score of current spread
        mean = sum(spreads) / len(spreads)
        variance = sum((s - mean) ** 2 for s in spreads) / len(spreads)
        std = variance ** 0.5

        if std == 0:
            return None

        z = (spread_bps - mean) / std
        current_pos = self.positions.get(symbol, 0)

        # Spread is wide — buy at bid (expect spread to narrow, price to rise from bid)
        if z > self.z_threshold and current_pos <= 0:
            return {
                "side": "buy",
                "price": best_bid,
                "quantity": self.order_size,
                "order_type": "limit",
                "reason": f"spread_z={z:.2f} > {self.z_threshold}, spread={spread_bps:.1f}bps",
            }

        # Already long and spread normalized — take profit
        if z < 0.5 and current_pos > 0:
            return {
                "side": "sell",
                "price": best_ask,
                "quantity": current_pos,
                "order_type": "limit",
                "reason": f"spread normalized z={z:.2f}, taking profit",
            }

        return None
