"""
ETH Basis / Carry + Market Making Strategy.

Quotes passively on quarterly futures, hedges on perpetual swap.
Captures: basis convergence, funding carry, maker spread.
"""
import time
from dataclasses import dataclass, field
from typing import Optional

from strategy.base import BaseStrategy, BaseParams, BaseStatus, periodic


@dataclass
class BasisParams(BaseParams):
    futures_symbol: str = "ETH-USDT-260626"
    perp_symbol: str = "ETH-USDT-SWAP"
    entry_basis_bps: float = 15.0
    exit_basis_bps: float = 5.0
    max_position: float = 5.0
    quote_offset_ticks: int = 1
    min_dte_days: int = 3
    max_delta_imbalance: float = 0.5
    hedge_slippage_ticks: int = 2


@dataclass
class BasisStatus(BaseStatus):
    basis_bps: float = 0.0
    annualized_bps: float = 0.0
    dte_days: float = 0.0
    funding_rate: float = 0.0
    funding_earned: float = 0.0
    fut_position: float = 0.0
    perp_position: float = 0.0
    net_delta: float = 0.0
    last_quote_time: float = 0.0
    trades_locked: int = 0


class BasisStrategy(BaseStrategy):
    Params = BasisParams
    Status = BasisStatus

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tick_sz: float = 0.0
        self._ct_val: float = 0.0
        self._expiry_ms: int = 0
        self._instrument_loaded = False

    # ── Market data callbacks ────────────────────────────────────────────

    def on_depth(self, symbol: str, book: dict) -> Optional[dict]:
        if not self._instrument_loaded:
            return None

        p = self.params
        if not self.all_books_ready(p.futures_symbol, p.perp_symbol):
            return None

        fut_book = self.get_cached_book(p.futures_symbol)
        perp_book = self.get_cached_book(p.perp_symbol)

        fut_bids = fut_book.get("bids", [])
        fut_asks = fut_book.get("asks", [])
        perp_bids = perp_book.get("bids", [])
        perp_asks = perp_book.get("asks", [])

        if not fut_bids or not fut_asks or not perp_bids or not perp_asks:
            return None

        fut_mid = (fut_bids[0][0] + fut_asks[0][0]) / 2
        perp_mid = (perp_bids[0][0] + perp_asks[0][0]) / 2

        if perp_mid <= 0:
            return None

        basis = (fut_mid - perp_mid) / perp_mid
        dte_days = self._get_dte_days()
        annualized = basis * (365 / max(dte_days, 0.1))

        self.status.basis_bps = basis * 10000
        self.status.annualized_bps = annualized * 10000
        self.status.dte_days = dte_days
        self.status.fut_position = self.get_position(p.futures_symbol)
        self.status.perp_position = self.get_position(p.perp_symbol)
        self.status.net_delta = self.status.fut_position + self.status.perp_position

        # Decision logic — bidirectional:
        #   Contango (basis > 0): buy futures, short perp
        #   Backwardation (basis < 0): sell futures, buy perp
        fut_pos = self.status.fut_position
        abs_pos = abs(fut_pos)
        ann_bps = annualized * 10000  # signed

        if abs_pos >= p.max_position:
            return None

        if dte_days < p.min_dte_days:
            return self._unwind_signal(fut_book, perp_book)

        # Unwind: basis weakened below exit threshold for current direction
        if fut_pos > 0 and ann_bps < p.exit_basis_bps:
            return self._unwind_signal(fut_book, perp_book)
        if fut_pos < 0 and ann_bps > -p.exit_basis_bps:
            return self._unwind_signal(fut_book, perp_book)

        # Entry: basis strong enough in either direction
        if ann_bps >= p.entry_basis_bps:
            return self._quote_contango(fut_book)
        if ann_bps <= -p.entry_basis_bps:
            return self._quote_backwardation(fut_book)

        return None

    def _quote_contango(self, fut_book: dict) -> Optional[dict]:
        """Contango: buy futures passively (then short perp as hedge)."""
        p = self.params
        if self.has_open_order(p.futures_symbol, side="buy"):
            return None

        fut_best_bid = fut_book["bids"][0][0]
        quote_price = fut_best_bid + self._tick_sz * p.quote_offset_ticks

        self.status.last_quote_time = time.time()
        return {
            "side": "buy",
            "symbol": p.futures_symbol,
            "price": quote_price,
            "quantity": self._ct_val,
            "order_type": "limit",
            "reason": f"contango_entry ann={self.status.annualized_bps:.1f}bps",
        }

    def _quote_backwardation(self, fut_book: dict) -> Optional[dict]:
        """Backwardation: sell futures passively (then buy perp as hedge)."""
        p = self.params
        if self.has_open_order(p.futures_symbol, side="sell"):
            return None

        fut_best_ask = fut_book["asks"][0][0]
        quote_price = fut_best_ask - self._tick_sz * p.quote_offset_ticks

        self.status.last_quote_time = time.time()
        return {
            "side": "sell",
            "symbol": p.futures_symbol,
            "price": quote_price,
            "quantity": self._ct_val,
            "order_type": "limit",
            "reason": f"backwardation_entry ann={self.status.annualized_bps:.1f}bps",
        }

    def _unwind_signal(self, fut_book: dict, perp_book: dict) -> Optional[dict]:
        """Close the futures leg (direction-aware)."""
        p = self.params
        fut_pos = self.get_position(p.futures_symbol)

        if fut_pos == 0:
            return None

        if fut_pos > 0:
            if self.has_open_order(p.futures_symbol, side="sell"):
                return None
            fut_best_ask = fut_book["asks"][0][0]
            return {
                "side": "sell",
                "symbol": p.futures_symbol,
                "price": fut_best_ask - self._tick_sz * p.quote_offset_ticks,
                "quantity": min(fut_pos, self._ct_val),
                "order_type": "limit",
                "reason": f"unwind_long ann={self.status.annualized_bps:.1f}bps",
            }
        else:
            if self.has_open_order(p.futures_symbol, side="buy"):
                return None
            fut_best_bid = fut_book["bids"][0][0]
            return {
                "side": "buy",
                "symbol": p.futures_symbol,
                "price": fut_best_bid + self._tick_sz * p.quote_offset_ticks,
                "quantity": min(abs(fut_pos), self._ct_val),
                "order_type": "limit",
                "reason": f"unwind_short ann={self.status.annualized_bps:.1f}bps",
            }

    # ── Fill handling: hedge the other leg ───────────────────────────────

    def on_fill(self, fill: dict) -> None:
        super().on_fill(fill)

        p = self.params
        symbol = fill.get("symbol", "")
        side = fill.get("side", "")
        qty = float(fill.get("quantity", 0) or 0)

        if symbol == p.futures_symbol and qty > 0:
            self.status.trades_locked += 1
        # No explicit hedge tracking needed — execute_hedge reads net_delta
        # directly from positions and always converges toward neutral.

    def on_order_update(self, order: dict) -> None:
        super().on_order_update(order)

    async def on_pause(self) -> None:
        await self._cancel_all_open()

    async def on_stop(self) -> None:
        await self._cancel_all_open()

    async def _cancel_all_open(self) -> None:
        for order in self.get_open_orders():
            await self.cancel_order(
                order.get("order_id", "") or order.get("client_order_id", ""),
                order.get("symbol", ""),
            )

    # ── Funding rate tracking ────────────────────────────────────────────

    def on_funding_rate(self, data: dict) -> None:
        rate = data.get("funding_rate", 0)
        self.status.funding_rate = rate
        perp_pos = self.get_position(self.params.perp_symbol)
        if perp_pos != 0:
            # Approximate: funding earned = -position × rate × notional
            # (short earns positive funding, long pays)
            perp_tick = self.get_cached_tick(self.params.perp_symbol)
            mark = float(perp_tick.get("last", 0) or 0)
            if mark > 0:
                self.status.funding_earned += -perp_pos * rate * mark

    # ── Periodic tasks ───────────────────────────────────────────────────

    @periodic(3)
    async def execute_hedge(self):
        """Reconcile perp hedge to match futures position.

        Uses net_delta (fut_pos + perp_pos) to determine how much hedge is
        needed, not a fragile pending flag. Survives restarts, partial fills,
        and rejected orders.
        """
        p = self.params
        fut_pos = self.get_position(p.futures_symbol)
        perp_pos = self.get_position(p.perp_symbol)
        net_delta = fut_pos + perp_pos

        # No imbalance — nothing to do
        if abs(net_delta) < self._ct_val * 0.5:
            return

        # Don't hedge if we already have an open order on perp
        if self.has_open_order(p.perp_symbol):
            return

        perp_book = self.get_cached_book(p.perp_symbol)
        if not perp_book.get("asks") or not perp_book.get("bids"):
            return

        hedge_qty = abs(net_delta)
        if net_delta > 0:
            # Long-heavy → sell perp to hedge
            price = perp_book["bids"][0][0] - self._tick_sz * p.hedge_slippage_ticks
            side = "sell"
        else:
            # Short-heavy → buy perp to hedge
            price = perp_book["asks"][0][0] + self._tick_sz * p.hedge_slippage_ticks
            side = "buy"

        await self.place_order(
            symbol=p.perp_symbol,
            side=side,
            price=price,
            quantity=hedge_qty,
            order_type="limit",
        )

    @periodic(30)
    async def check_risk(self):
        """Delta-neutral check — cancel futures quotes if dangerously imbalanced."""
        p = self.params
        net_delta = self.get_position(p.futures_symbol) + self.get_position(p.perp_symbol)
        self.status.net_delta = net_delta

        if abs(net_delta) > p.max_delta_imbalance:
            # Cancel futures quotes to prevent widening the gap further;
            # execute_hedge (runs every 3s) will close the perp gap.
            for order in self.get_open_orders(symbol=p.futures_symbol):
                await self.cancel_order(order.get("order_id", ""),
                                        order.get("symbol", ""))

    @periodic(300)
    async def load_instrument(self):
        """Load/refresh instrument metadata."""
        p = self.params
        inst = await self.get_instrument(p.futures_symbol)
        if inst:
            self._tick_sz = float(inst.get("tickSz", 0) or 0)
            self._ct_val = float(inst.get("ctVal", 0) or 0)
            exp_time = inst.get("expTime", "")
            if exp_time:
                self._expiry_ms = int(exp_time)
            self._instrument_loaded = True

    # ── Helpers ──────────────────────────────────────────────────────────

    def _get_dte_days(self) -> float:
        if self._expiry_ms <= 0:
            return 999.0
        now_ms = int(time.time() * 1000)
        return max((self._expiry_ms - now_ms) / 86400000, 0.0)
