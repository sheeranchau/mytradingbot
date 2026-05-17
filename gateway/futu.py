"""
FUTU (OpenD) gateway for US and HK stock trading.
Uses futu-api package which connects to a local OpenD daemon.
"""
import asyncio
import time
from typing import Optional

from gateway.base import BaseGateway

try:
    from futu import (
        OpenQuoteContext, OpenSecTradeContext,
        RET_OK, TrdSide, OrderType, TrdEnv,
        StockQuoteHandlerBase, OrderBookHandlerBase,
    )
    FUTU_AVAILABLE = True
except ImportError:
    FUTU_AVAILABLE = False


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

    async def connect_exchange(self) -> None:
        if not FUTU_AVAILABLE:
            raise ImportError("futu-api not installed. Run: pip install futu-api")

        # FUTU API is synchronous, run in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._connect_sync)

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

    async def _stream_market_data(self) -> None:
        """Poll market data (FUTU push is callback-based, we adapt to async)."""
        loop = asyncio.get_event_loop()

        while self.running:
            for symbol in self.symbols:
                try:
                    quote = await loop.run_in_executor(
                        None, self._get_quote, symbol
                    )
                    if quote:
                        await self.publish_tick(symbol, quote)
                except Exception as e:
                    self.logger.error("Quote error for %s: %s", symbol, e)
            await asyncio.sleep(0.5)  # polling interval

    def _get_quote(self, symbol: str) -> Optional[dict]:
        ret, data = self._quote_ctx.get_stock_quote([symbol])
        if ret == RET_OK and not data.empty:
            row = data.iloc[0]
            return {
                "bid": float(row.get("bid_price", 0)),
                "ask": float(row.get("ask_price", 0)),
                "bid_size": float(row.get("bid_vol", 0)),
                "ask_size": float(row.get("ask_vol", 0)),
                "last": float(row.get("last_price", 0)),
                "last_size": float(row.get("volume", 0)),
                "timestamp": time.time(),
            }
        return None

    async def send_order(self, order: dict) -> str:
        loop = asyncio.get_event_loop()
        order_id = await loop.run_in_executor(None, self._place_order_sync, order)
        return order_id

    def _place_order_sync(self, order: dict) -> str:
        side = TrdSide.BUY if order["side"] == "buy" else TrdSide.SELL
        ord_type = OrderType.NORMAL if order.get("order_type") == "limit" else OrderType.MARKET

        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
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

    async def cancel_order(self, order_id: str) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._cancel_sync, order_id)

    def _cancel_sync(self, order_id: str) -> None:
        trd_env = TrdEnv.SIMULATE if self.trade_env == "SIMULATE" else TrdEnv.REAL
        self._trade_ctx.modify_order(
            modify_order_op=2,  # cancel
            order_id=order_id,
            qty=0, price=0,
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
                    "symbol": row["code"],
                    "quantity": float(row["qty"]),
                    "avg_price": float(row["cost_price"]),
                    "market_price": float(row["market_val"]),
                    "unrealized_pnl": float(row["pl_val"]),
                })
            return positions
        return []

    async def disconnect_exchange(self) -> None:
        if self._quote_ctx:
            self._quote_ctx.close()
        if self._trade_ctx:
            self._trade_ctx.close()
