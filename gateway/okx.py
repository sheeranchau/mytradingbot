"""
OKX exchange gateway.
Uses WebSocket for market data and private channels, REST for account queries.
"""
import asyncio
import hashlib
import hmac
import base64
import time
import json
from dataclasses import asdict
from typing import Optional

import aiohttp

from gateway.base import BaseGateway
from core.models import OrderUpdate, OrderState


# ── OKX state → normalised OrderState ────────────────────────────────────────
_OKX_STATE: dict[str, str] = {
    "live":             OrderState.OPEN,
    "partially_filled": OrderState.PARTIAL,
    "filled":           OrderState.FILLED,
    "canceled":         OrderState.CANCELLED,
    "cancelled":        OrderState.CANCELLED,
    "mmp_canceled":     OrderState.CANCELLED,
    "rejected":         OrderState.REJECTED,
    "expired":          OrderState.EXPIRED,
}


class OKXGateway(BaseGateway):
    """OKX WebSocket + REST gateway."""

    # Production endpoints
    LIVE_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
    LIVE_WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"
    LIVE_REST_BASE = "https://www.okx.com"

    # Demo trading endpoints
    DEMO_WS_PUBLIC = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
    DEMO_WS_PRIVATE = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
    DEMO_REST_BASE = "https://www.okx.com"

    def __init__(self, api_key: str, secret_key: str, passphrase: str,
                 symbols: list[str], simulated: bool = False, **kwargs):
        super().__init__(gateway_name="okx", **kwargs)
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.symbols = symbols
        self.simulated = simulated
        self.acct_id = "okx_demo" if simulated else "okx_live"
        # Local order book state per symbol for incremental update management
        self._orderbooks: dict = {}
        self._last_pos_update: float = 0

        # Select endpoints based on mode
        if simulated:
            self.WS_PUBLIC = self.DEMO_WS_PUBLIC
            self.WS_PRIVATE = self.DEMO_WS_PRIVATE
            self.REST_BASE = self.DEMO_REST_BASE
        else:
            self.WS_PUBLIC = self.LIVE_WS_PUBLIC
            self.WS_PRIVATE = self.LIVE_WS_PRIVATE
            self.REST_BASE = self.LIVE_REST_BASE

        mode = "DEMO" if simulated else "LIVE"
        self.logger.info("Initialized in %s mode", mode)
        self._ws_public: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_private: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        # In-memory map: clOrdId → ordId (populated once exchange ACKs the order)
        # Used to correlate REST sync responses back to our client_order_id
        self._cl_to_ord: dict[str, str] = {}
        # Reverse map: ordId → clOrdId (for WS pushes that only carry ordId)
        self._ord_to_cl: dict[str, str] = {}
        # clOrdId → strategy name — OKX WS never sends the strategy field so
        # we record it at send_order time and look it up on every WS fill push.
        self._cl_to_strategy: dict[str, str] = {}

    def _has_credentials(self) -> bool:
        # Treat unresolved YAML placeholders (e.g. "${OKX_API_KEY}") as missing
        def _set(val):
            return bool(val) and not str(val).startswith("${")
        return _set(self.api_key) and _set(self.secret_key) and _set(self.passphrase)

    async def connect_exchange(self) -> None:
        headers = {}
        if self.simulated:
            headers["x-simulated-trading"] = "1"
        self._session = aiohttp.ClientSession(headers=headers)

        # Connect public websocket (no credentials required)
        self.logger.info("Connecting public WS: %s", self.WS_PUBLIC)
        self._ws_public = await self._session.ws_connect(self.WS_PUBLIC)
        self.logger.info("Public WS connected")

        # Instruments are a public endpoint — fetch before auth check so
        # market-data-only mode still has instrument metadata available.
        await self._fetch_and_store_instruments()

        if self._has_credentials():
            # Connect and authenticate private websocket for order/fill streams
            self.logger.info("Connecting private WS: %s", self.WS_PRIVATE)
            self._ws_private = await self._session.ws_connect(self.WS_PRIVATE)
            self.logger.info("Private WS connected, authenticating...")
            await self._authenticate()
            self.logger.info("Authentication successful")
            await self._subscribe_private()
            await self._sync_pending_orders()
            await self._sync_account_info()
            await self._fetch_fee_rates()
            asyncio.create_task(self._cod_keepalive_loop())
        else:
            self.logger.info("No API credentials — running in market-data-only mode")

        self.logger.info("Subscribed to %d symbols: %s", len(self.symbols), self.symbols)

    async def _authenticate(self) -> None:
        timestamp = str(int(time.time()))
        sign_str = timestamp + "GET" + "/users/self/verify"
        signature = base64.b64encode(
            hmac.new(self.secret_key.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()

        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": signature,
            }]
        }
        await self._ws_private.send_json(login_msg)
        resp = await self._ws_private.receive_json()
        if resp.get("code") != "0":
            raise ConnectionError(f"OKX auth failed: {resp}")

    async def _subscribe_private(self) -> None:
        """Subscribe to order, position, and account updates for all instruments."""
        args = [
            {"channel": "orders", "instType": "ANY"},
            {"channel": "positions", "instType": "ANY"},
            {"channel": "account"},
        ]
        await self._ws_private.send_json({"op": "subscribe", "args": args})

    async def _stream_market_data(self) -> None:
        """Subscribe to tickers and orderbook, then stream."""
        # Subscribe
        args = []
        for symbol in self.symbols:
            args.append({"channel": "tickers", "instId": symbol})
            args.append({"channel": "books", "instId": symbol})
            if symbol.endswith("-SWAP"):
                args.append({"channel": "funding-rate", "instId": symbol})
        await self._ws_public.send_json({"op": "subscribe", "args": args})

        # Listen and publish (private task only when credentials were provided)
        if self._has_credentials():
            asyncio.create_task(self._listen_private())
        asyncio.create_task(self._refresh_instruments_loop())

        while self.running:
            try:
                msg = await self._ws_public.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_public_msg(data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
            except Exception as e:
                self.logger.error("Public WS error: %s", e)
                await asyncio.sleep(1)

    @staticmethod
    def _compute_checksum(bids: dict, asks: dict) -> int:
        import zlib
        sorted_bids = sorted(bids.items(), key=lambda x: float(x[0]), reverse=True)[:25]
        sorted_asks = sorted(asks.items(), key=lambda x: float(x[0]))[:25]
        parts = []
        for i in range(max(len(sorted_bids), len(sorted_asks))):
            if i < len(sorted_bids):
                parts.append(f"{sorted_bids[i][0]}:{sorted_bids[i][1]}")
            if i < len(sorted_asks):
                parts.append(f"{sorted_asks[i][0]}:{sorted_asks[i][1]}")
        crc = zlib.crc32(":".join(parts).encode())
        if crc >= 0x80000000:
            crc -= 0x100000000
        return crc

    async def _resubscribe_books(self, symbol: str):
        if self._ws_public and not self._ws_public.closed:
            await self._ws_public.send_json({
                "op": "unsubscribe",
                "args": [{"channel": "books", "instId": symbol}],
            })
            await asyncio.sleep(0.1)
            await self._ws_public.send_json({
                "op": "subscribe",
                "args": [{"channel": "books", "instId": symbol}],
            })

    async def _handle_public_msg(self, data: dict) -> None:
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        action = data.get("action", "snapshot")
        symbol = arg.get("instId", "")
        records = data.get("data", [])

        for record in records:
            if channel == "tickers":
                last_px = record.get("last", "0")
                await self.publish_tick(symbol, {
                    "bid": float(record.get("bidPx", 0)),
                    "ask": float(record.get("askPx", 0)),
                    "bid_size": float(record.get("bidSz", 0)),
                    "ask_size": float(record.get("askSz", 0)),
                    "last": float(last_px),
                    "last_size": float(record.get("lastSz", 0)),
                    "timestamp": time.time(),
                })
                now = time.time()
                if now - self._last_pos_update >= 2:
                    self._last_pos_update = now
                    await self._update_position_mark_price(symbol, last_px)
            elif channel == "books":
                if symbol not in self._orderbooks:
                    self._orderbooks[symbol] = {"bids": {}, "asks": {}}
                book = self._orderbooks[symbol]

                if action == "snapshot":
                    book["bids"] = {e[0]: e[1] for e in record.get("bids", [])}
                    book["asks"] = {e[0]: e[1] for e in record.get("asks", [])}
                else:
                    for e in record.get("bids", []):
                        price_str, size_str = e[0], e[1]
                        if size_str == "0":
                            book["bids"].pop(price_str, None)
                        else:
                            book["bids"][price_str] = size_str
                    for e in record.get("asks", []):
                        price_str, size_str = e[0], e[1]
                        if size_str == "0":
                            book["asks"].pop(price_str, None)
                        else:
                            book["asks"][price_str] = size_str

                expected_crc = record.get("checksum")
                if expected_crc is not None:
                    actual_crc = self._compute_checksum(book["bids"], book["asks"])
                    if actual_crc != expected_crc:
                        self.logger.warning("Checksum mismatch for %s: expected=%s got=%s, resubscribing",
                                            symbol, expected_crc, actual_crc)
                        self._orderbooks.pop(symbol, None)
                        asyncio.create_task(self._resubscribe_books(symbol))
                        return

                top_bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:25]
                top_asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:25]
                await self.publish_orderbook(symbol, {
                    "bids": [[float(p), float(s)] for p, s in top_bids],
                    "asks": [[float(p), float(s)] for p, s in top_asks],
                    "timestamp": time.time(),
                })

            elif channel == "funding-rate":
                await self.publish_funding_rate(symbol, {
                    "funding_rate": float(record.get("fundingRate", 0) or 0),
                    "next_funding_rate": float(record.get("nextFundingRate", 0) or 0),
                    "funding_time": int(record.get("fundingTime", 0) or 0),
                    "next_funding_time": int(record.get("nextFundingTime", 0) or 0),
                    "timestamp": time.time(),
                })

    async def _listen_private(self) -> None:
        """Listen for fills and position updates from private WS."""
        while self.running:
            try:
                msg = await self._ws_private.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_private_msg(data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
            except Exception as e:
                self.logger.error("Private WS error: %s", e)
                await asyncio.sleep(1)

    def _build_order_update_from_ws(self, record: dict) -> OrderUpdate:
        """Map a single OKX orders-channel WS record to an OrderUpdate."""
        ord_id    = record.get("ordId", "")
        cl_id     = record.get("clOrdId", "") or self._ord_to_cl.get(ord_id, "")
        raw_state = record.get("state", "")

        # Recover strategy — OKX WS never sends it, so look up from the map
        # populated at send_order time (survives restart via Redis seed).
        strategy = self._cl_to_strategy.get(cl_id, "")

        # Per-fill fields — only present when fillSz > 0
        fill_sz  = float(record.get("fillSz", 0) or 0)
        fill_id  = record.get("fillId", "") if fill_sz > 0 else ""

        # OKX sometimes sends a terminal state=filled push with fillSz=0 / no
        # fillId (the per-fill detail was in an earlier push, or OKX batched it).
        # When that happens synthesize a fill from the aggregate accFillSz + avgPx
        # so the risk engine and Telegram are always notified.  We use the ordId
        # as the synthetic fill_id — the fill_lock NX guard in
        # _process_order_update will de-duplicate if a real fill push arrived first.
        filled_qty = float(record.get("accFillSz", 0) or 0)
        avg_px     = float(record.get("avgPx", 0) or 0)
        if not fill_id and raw_state == "filled" and filled_qty > 0 and avg_px > 0:
            fill_id    = f"syn{ord_id}"   # synthetic — prefixed to avoid clash with real fillIds
            fill_sz    = filled_qty
            avg_px     = avg_px           # use aggregate avg as fill price

        # fillFee is per-fill; fee is cumulative. Prefer fillFee, fall back to
        # fee (negated — OKX sends fees as negative numbers) for older API versions.
        if fill_id:
            raw_fee  = record.get("fillFee") or record.get("fee", 0)
            fill_fee = abs(float(raw_fee or 0))
        else:
            fill_fee = 0.0

        fill_price = float(record.get("fillPx", 0) or 0) if fill_id else 0.0
        # For synthetic fills, fillPx is absent — use avgPx instead
        if fill_id and fill_price == 0.0:
            fill_price = avg_px

        return OrderUpdate(
            client_order_id = cl_id,
            order_id        = ord_id,
            gateway         = self.gateway_name,
            strategy        = strategy,
            symbol          = record.get("instId", ""),
            side            = record.get("side", ""),
            order_type      = record.get("ordType", "limit"),
            price           = float(record.get("px", 0) or 0),
            quantity        = float(record.get("sz", 0) or 0),
            state           = _OKX_STATE.get(raw_state, raw_state),
            filled_qty      = filled_qty,
            avg_fill_price  = avg_px,
            # Fill-level fields
            fill_id         = fill_id,
            fill_price      = fill_price,
            fill_qty        = fill_sz if fill_id else 0.0,
            fill_fee        = fill_fee,
            fill_fee_ccy    = record.get("fillFeeCcy", "") if fill_id else "",
            fill_pnl        = float(record.get("pnl", 0) or 0) if fill_id else 0.0,
            fill_time_ms    = int(record.get("fillTime", 0) or 0) if fill_id else 0,
            create_time_ms  = int(record.get("cTime", 0) or 0),
            update_time_ms  = int(record.get("uTime", 0) or 0),
            update_source   = "ws",
        )

    def _build_order_update_from_rest(self, record: dict, *,
                                      cl_ord_id: str = "") -> OrderUpdate:
        """Map an OKX REST order record to an OrderUpdate.
        REST does not carry per-fill fields; those come only via WS.
        """
        ord_id    = record.get("ordId", "")
        cl_id     = record.get("clOrdId", "") or cl_ord_id or self._ord_to_cl.get(ord_id, "")
        raw_state = record.get("state", "")
        # Recover strategy the same way as the WS builder.
        strategy  = self._cl_to_strategy.get(cl_id, "")

        return OrderUpdate(
            client_order_id = cl_id,
            order_id        = ord_id,
            gateway         = self.gateway_name,
            strategy        = strategy,
            symbol          = record.get("instId", ""),
            side            = record.get("side", ""),
            order_type      = record.get("ordType", "limit"),
            price           = float(record.get("px", 0) or 0),
            quantity        = float(record.get("sz", 0) or 0),
            state           = _OKX_STATE.get(raw_state, raw_state),
            filled_qty      = float(record.get("accFillSz", 0) or 0),
            avg_fill_price  = float(record.get("avgPx", 0) or 0),
            create_time_ms  = int(record.get("cTime", 0) or 0),
            update_time_ms  = int(record.get("uTime", 0) or 0),
            update_source   = "rest",
        )

    async def _handle_private_msg(self, data: dict) -> None:
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        records = data.get("data", [])

        for record in records:
            if channel == "orders":
                update = self._build_order_update_from_ws(record)
                # Keep id-maps in sync
                if update.order_id and update.client_order_id:
                    self._cl_to_ord[update.client_order_id] = update.order_id
                    self._ord_to_cl[update.order_id] = update.client_order_id
                await self._process_order_update(update)

            elif channel == "account":
                await self._store_trading_account(record)

            elif channel == "positions":
                await self._store_positions(record)

    async def _store_positions(self, record: dict) -> None:
        """Store position update from WS push. Replaces per-instrument entry."""
        try:
            r = self.redis.client
            key = f"account:{self.acct_id}:positions"
            raw = await r.get(key)
            positions = json.loads(raw) if raw else []
            inst_id = record.get("instId", "")
            pos_side = record.get("posSide", "net")
            positions = [p for p in positions
                         if not (p["instId"] == inst_id and p["posSide"] == pos_side)]
            pos_amt = float(record.get("pos", 0))
            if pos_amt != 0:
                positions.append({
                    "instId": record.get("instId", ""),
                    "posSide": pos_side,
                    "pos": record.get("pos", "0"),
                    "avgPx": record.get("avgPx", "0"),
                    "upl": record.get("upl", "0"),
                    "uplRatio": record.get("uplRatio", "0"),
                    "lever": record.get("lever", "0"),
                    "liqPx": record.get("liqPx", ""),
                    "markPx": record.get("markPx", "0"),
                    "margin": record.get("margin", "0"),
                    "mgnMode": record.get("mgnMode", ""),
                    "notionalUsd": record.get("notionalUsd", "0"),
                    "uTime": record.get("uTime", ""),
                })
            await r.set(key, json.dumps(positions))
        except Exception as e:
            self.logger.debug("Failed to store position: %s", e)

    async def _update_position_mark_price(self, symbol: str, last_px: str) -> None:
        """Update mark price and UPL for positions matching this symbol."""
        try:
            r = self.redis.client
            key = f"account:{self.acct_id}:positions"
            raw = await r.get(key)
            if not raw:
                return
            positions = json.loads(raw)
            changed = False
            for p in positions:
                if p["instId"] == symbol:
                    p["markPx"] = last_px
                    avg = float(p.get("avgPx", 0))
                    pos = float(p.get("pos", 0))
                    mark = float(last_px)
                    if avg > 0 and pos != 0:
                        upl = (mark - avg) * pos
                        p["upl"] = str(upl)
                        p["uplRatio"] = str(upl / (avg * abs(pos))) if avg * abs(pos) != 0 else "0"
                    changed = True
            if changed:
                await r.set(key, json.dumps(positions))
        except Exception:
            pass

    async def _sync_account_info(self) -> None:
        """Fetch account config, trading balance, and funding balance on startup."""
        try:
            r = self.redis.client
            acct_id = self.acct_id

            config_resp = await self._rest_request("GET", "/api/v5/account/config")
            if config_resp and config_resp.get("code") == "0" and config_resp["data"]:
                cfg = config_resp["data"][0]
                level_map = {"1": "cash", "2": "single_ccy_margin",
                             "3": "multi_ccy_margin", "4": "portfolio_margin"}
                acct_config = {
                    "acct_id": acct_id,
                    "acct_level": cfg.get("acctLv", ""),
                    "acct_level_name": level_map.get(cfg.get("acctLv", ""), "unknown"),
                    "pos_mode": cfg.get("posMode", ""),
                    "uid": cfg.get("uid", ""),
                }
                await r.set(f"account:{acct_id}:config", json.dumps(acct_config))

            balance_resp = await self._rest_request("GET", "/api/v5/account/balance")
            if balance_resp and balance_resp.get("code") == "0" and balance_resp["data"]:
                await self._store_trading_account(balance_resp["data"][0])

            funding_resp = await self._rest_request("GET", "/api/v5/asset/balances")
            if funding_resp and funding_resp.get("code") == "0":
                funding = []
                for item in funding_resp.get("data", []):
                    bal = float(item.get("bal", 0))
                    if bal > 0:
                        funding.append({
                            "ccy": item.get("ccy", ""),
                            "bal": item.get("bal", "0"),
                            "availBal": item.get("availBal", "0"),
                            "frozenBal": item.get("frozenBal", "0"),
                        })
                await r.set(f"account:{acct_id}:funding", json.dumps(funding))

            earn_resp = await self._rest_request("GET", "/api/v5/finance/savings/balance")
            if earn_resp and earn_resp.get("code") == "0":
                earning = []
                for item in earn_resp.get("data", []):
                    amt = float(item.get("amt", 0))
                    if amt > 0:
                        earning.append({
                            "ccy": item.get("ccy", ""),
                            "amt": item.get("amt", "0"),
                            "earnings": item.get("earnings", "0"),
                            "rate": item.get("rate", "0"),
                        })
                await r.set(f"account:{acct_id}:earning", json.dumps(earning))

            pos_resp = await self._rest_request("GET", "/api/v5/account/positions")
            if pos_resp and pos_resp.get("code") == "0":
                positions = []
                for p in pos_resp.get("data", []):
                    if float(p.get("pos", 0)) != 0:
                        positions.append({
                            "instId": p.get("instId", ""),
                            "posSide": p.get("posSide", "net"),
                            "pos": p.get("pos", "0"),
                            "avgPx": p.get("avgPx", "0"),
                            "upl": p.get("upl", "0"),
                            "uplRatio": p.get("uplRatio", "0"),
                            "lever": p.get("lever", "0"),
                            "liqPx": p.get("liqPx", ""),
                            "markPx": p.get("markPx", "0"),
                            "margin": p.get("margin", "0"),
                            "mgnMode": p.get("mgnMode", ""),
                            "notionalUsd": p.get("notionalUsd", "0"),
                            "uTime": p.get("uTime", ""),
                        })
                await r.set(f"account:{acct_id}:positions", json.dumps(positions))

            await r.hset("accounts", acct_id, json.dumps({
                "acct_id": acct_id,
                "exchange": "OKX",
                "mode": "DEMO" if self.simulated else "LIVE",
                "gateway": self.gateway_name,
            }))
            self.logger.info("Synced account info for %s", acct_id)
        except Exception as e:
            self.logger.error("Failed to sync account info: %s", e)

    async def _store_trading_account(self, data: dict) -> None:
        """Store trading account snapshot from REST or WS push."""
        try:
            r = self.redis.client
            acct_id = self.acct_id
            summary = {
                "acct_id": acct_id,
                "totalEq": data.get("totalEq", "0"),
                "adjEq": data.get("adjEq", "0"),
                "isoEq": data.get("isoEq", "0"),
                "ordFroz": data.get("ordFroz", "0"),
                "imr": data.get("imr", "0"),
                "mmr": data.get("mmr", "0"),
                "mgnRatio": data.get("mgnRatio", ""),
                "notionalUsd": data.get("notionalUsd", "0"),
                "uTime": data.get("uTime", ""),
            }
            details = []
            for d in data.get("details", []):
                eq = float(d.get("eq", 0) or 0)
                if eq > 0:
                    details.append({
                        "ccy": d.get("ccy", ""),
                        "eq": d.get("eq", "0"),
                        "cashBal": d.get("cashBal", "0"),
                        "availEq": d.get("availEq", "0"),
                        "frozenBal": d.get("frozenBal", "0"),
                        "ordFrozen": d.get("ordFrozen", "0"),
                        "upl": d.get("upl", "0"),
                        "crossLiab": d.get("crossLiab", "0"),
                        "eqUsd": d.get("eqUsd", "0"),
                    })
            await r.set(f"account:{acct_id}:trading", json.dumps({
                "summary": summary, "details": details
            }))
        except Exception as e:
            self.logger.debug("Failed to store trading account: %s", e)

    async def _sync_pending_orders(self) -> None:
        """On startup, reconcile Redis active orders with OKX actual state.

        Three things happen here:
        1. Rebuild ``_cl_to_ord`` / ``_ord_to_cl`` maps from Redis data first
           (covers orders that were pending before restart even if they are now
           terminal — needed before any _sync_single_order calls below).
        2. Fetch live orders from OKX and rebuild the pending list.
        3. Any order tracked in Redis but no longer live on exchange is synced
           via REST to get its terminal state written to finished_orders.
        """
        try:
            r = self.redis.client
            raw = await r.get("pending_orders:okx")
            old_orders = json.loads(raw) if raw else []

            # 1. Rebuild maps from Redis first — ensures _sync_single_order below
            #    can resolve cl_id even if the order is no longer on the exchange.
            for old in old_orders:
                oid      = old.get("order_id", "")
                cl_id    = old.get("client_order_id", "")
                strategy = old.get("strategy", "")
                if oid and cl_id:
                    self._cl_to_ord[cl_id] = oid
                    self._ord_to_cl[oid]   = cl_id
                if cl_id and strategy:
                    self._cl_to_strategy[cl_id] = strategy

            # 2. Fetch live orders from OKX and overwrite maps with fresh data
            live_orders = await self.query_pending_orders()
            live_ids = {o.get("ordId", "") for o in live_orders}
            synced: list[dict] = []
            for o in live_orders:
                update = self._build_order_update_from_rest(o)
                if update.order_id and update.client_order_id:
                    self._cl_to_ord[update.client_order_id] = update.order_id
                    self._ord_to_cl[update.order_id] = update.client_order_id
                synced.append(update.to_redis_dict())

            # 3. Orders tracked in Redis but no longer live → get terminal state
            for old in old_orders:
                oid   = old.get("order_id", "")
                cl_id = old.get("client_order_id", "")
                if oid and oid not in live_ids:
                    await self._sync_single_order(oid, old.get("symbol", ""),
                                                  cl_ord_id=cl_id)

            await r.set("pending_orders:okx", json.dumps(synced))
            self.logger.info("Startup sync: %d live orders on OKX, %d checked from Redis",
                             len(synced), len(old_orders))
        except Exception as e:
            self.logger.error("Failed to sync pending orders: %s", e)

    async def send_order(self, order: dict) -> str:
        """Place order via REST API. Returns exchange ordId, or "" on failure."""
        path = "/api/v5/trade/order"

        # Generate clOrdId and register it before the REST call so any
        # concurrent WS push that arrives first can still be correlated.
        strategy    = order.get("strategy", "")
        cl_ord_id   = order.get("client_order_id") or self._gen_cl_ord_id(strategy)
        now_ms      = int(time.time() * 1000)
        # Record strategy so WS fill pushes (which carry no strategy field) can
        # recover it via _build_order_update_from_ws.
        if cl_ord_id and strategy:
            self._cl_to_strategy[cl_ord_id] = strategy

        body = {
            "instId":  order["symbol"],
            "tdMode":  "cross",              # cross margin
            "side":    order["side"],
            "ordType": order.get("order_type", "limit"),
            "sz":      str(order["quantity"]),
            "clOrdId": cl_ord_id,
        }
        if order.get("order_type", "limit") == "limit":
            body["px"] = str(order["price"])

        # Create a pending_submit entry immediately so we track the order from
        # the first instant, before the exchange ACK or any WS push.
        pending_update = OrderUpdate(
            client_order_id = cl_ord_id,
            order_id        = "",            # unknown until ACK
            gateway         = self.gateway_name,
            strategy        = strategy,
            symbol          = order["symbol"],
            side            = order["side"],
            order_type      = order.get("order_type", "limit"),
            price           = float(order.get("price", 0)),
            quantity        = float(order["quantity"]),
            state           = OrderState.PENDING_SUBMIT,
            create_time_ms  = now_ms,
            update_time_ms  = now_ms,
            update_source   = "rest",
        )
        await self._process_order_update(pending_update)

        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            order_id = resp["data"][0]["ordId"]
            self._cl_to_ord[cl_ord_id] = order_id
            self._ord_to_cl[order_id]  = cl_ord_id
            self.logger.info(
                "Order placed: %s %s %s qty=%s @ %s -> clOrdId=%s ordId=%s",
                order["side"], order.get("order_type", "limit"),
                order["symbol"], order["quantity"],
                order.get("price", "MKT"), cl_ord_id, order_id,
            )
            # Patch order_id into the pending record
            open_update = OrderUpdate(
                client_order_id = cl_ord_id,
                order_id        = order_id,
                gateway         = self.gateway_name,
                strategy        = strategy,
                symbol          = order["symbol"],
                side            = order["side"],
                order_type      = order.get("order_type", "limit"),
                price           = float(order.get("price", 0)),
                quantity        = float(order["quantity"]),
                state           = OrderState.OPEN,
                create_time_ms  = now_ms,
                update_time_ms  = int(time.time() * 1000),
                update_source   = "rest",
            )
            await self._process_order_update(open_update)
            return order_id
        else:
            self.logger.error("Order failed: %s", resp)
            err_msg = ""
            if resp and resp.get("data"):
                err_msg = resp["data"][0].get("sMsg", "")
            rejected = OrderUpdate(
                client_order_id = cl_ord_id,
                order_id        = "",
                gateway         = self.gateway_name,
                strategy        = strategy,
                symbol          = order["symbol"],
                side            = order["side"],
                order_type      = order.get("order_type", "limit"),
                price           = float(order.get("price", 0)),
                quantity        = float(order["quantity"]),
                state           = OrderState.REJECTED,
                update_time_ms  = int(time.time() * 1000),
                update_source   = "rest",
            )
            await self._process_order_update(rejected)
            return ""

    async def cancel_order(self, order: dict) -> None:
        path = "/api/v5/trade/cancel-order"
        order_id  = order.get("order_id", "")
        cl_ord_id = order.get("client_order_id", "") or self._ord_to_cl.get(order_id, "")
        body: dict = {"instId": order.get("symbol", "")}
        # Prefer clOrdId if available (idempotent even if ordId changes on amend)
        if cl_ord_id:
            body["clOrdId"] = cl_ord_id
        else:
            body["ordId"] = order_id
        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            self.logger.info("Cancelled order clOrdId=%s ordId=%s", cl_ord_id, order_id)
            # REST ACK only tells us it was accepted; the definitive state arrives
            # via WS push.  We do a REST sync only as a fallback when WS is slow.
            await self._sync_single_order(order_id, order.get("symbol", ""),
                                          cl_ord_id=cl_ord_id)
        else:
            self.logger.error("Cancel failed: %s", resp)

    async def amend_order(self, order: dict) -> None:
        path = "/api/v5/trade/amend-order"
        order_id  = order.get("order_id", "")
        cl_ord_id = order.get("client_order_id", "") or self._ord_to_cl.get(order_id, "")
        body: dict = {"instId": order.get("symbol", "")}
        if cl_ord_id:
            body["clOrdId"] = cl_ord_id
        else:
            body["ordId"] = order_id
        if order.get("price"):
            body["newPx"] = str(order["price"])
        if order.get("quantity"):
            body["newSz"] = str(order["quantity"])
        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            self.logger.info("Amended order clOrdId=%s ordId=%s", cl_ord_id, order_id)
            await self._sync_single_order(order_id, order.get("symbol", ""),
                                          cl_ord_id=cl_ord_id)
        else:
            self.logger.error("Amend failed: %s", resp)

    async def _sync_single_order(self, order_id: str, symbol: str, *,
                                 cl_ord_id: str = "") -> None:
        """Query an order's latest state from OKX REST and update Redis.

        This is a fallback / reconciliation path.  The WS push is the primary
        source of truth.  Both paths share ``_process_order_update`` which
        deduplicates terminal writes via the was_pending guard.
        """
        try:
            path = f"/api/v5/trade/order?instId={symbol}&ordId={order_id}"
            resp = await self._rest_request("GET", path)
            if resp and resp.get("code") == "0" and resp.get("data"):
                record = resp["data"][0]
                update = self._build_order_update_from_rest(record, cl_ord_id=cl_ord_id)
                await self._process_order_update(update)
        except Exception as e:
            self.logger.error("Failed to sync order %s: %s", order_id, e)

    async def query_pending_orders(self) -> list[dict]:
        path = "/api/v5/trade/orders-pending"
        resp = await self._rest_request("GET", path)
        if resp and resp.get("code") == "0":
            return resp.get("data", [])
        return []

    async def query_positions(self) -> list[dict]:
        path = "/api/v5/account/positions"
        resp = await self._rest_request("GET", path)
        if resp and resp.get("code") == "0":
            return resp.get("data", [])
        return []

    async def _rest_request(self, method: str, path: str, body: dict = None) -> dict:
        timestamp = self._get_timestamp()
        body_str = json.dumps(body) if body else ""
        sign_str = timestamp + method + path + body_str
        signature = base64.b64encode(
            hmac.new(self.secret_key.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.simulated:
            headers["x-simulated-trading"] = "1"

        url = self.REST_BASE + path
        async with self._session.request(method, url, headers=headers,
                                         data=body_str if body else None) as resp:
            return await resp.json()

    def _get_timestamp(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

    async def _public_rest_request(self, path: str) -> dict:
        """GET a public (no-auth) OKX endpoint."""
        url = self.REST_BASE + path
        async with self._session.get(url) as resp:
            return await resp.json()

    # ── Instrument metadata ──────────────────────────────────────────────

    async def _fetch_and_store_instruments(self) -> None:
        try:
            r = self.redis.client
            instruments = {}
            seen_types = {"SWAP", "FUTURES"}
            for symbol in self.symbols:
                if not symbol.endswith("-SWAP") and not symbol.split("-")[-1].isdigit():
                    seen_types.add("SPOT")

            for inst_type in seen_types:
                resp = await self._public_rest_request(
                    f"/api/v5/public/instruments?instType={inst_type}"
                )
                if resp and resp.get("code") == "0":
                    for inst in resp.get("data", []):
                        instruments[inst["instId"]] = {
                            "instId":    inst.get("instId", ""),
                            "instType":  inst.get("instType", ""),
                            "expTime":   inst.get("expTime", ""),
                            "ctVal":     inst.get("ctVal", ""),
                            "tickSz":    inst.get("tickSz", ""),
                            "lotSz":     inst.get("lotSz", ""),
                            "listTime":  inst.get("listTime", ""),
                            "uly":       inst.get("uly", ""),
                            "settleCcy": inst.get("settleCcy", ""),
                        }
            await r.set(f"instruments:{self.gateway_name}",
                        json.dumps(instruments), ex=7200)
            self.logger.info("Fetched %d instruments", len(instruments))
        except Exception as e:
            self.logger.error("Failed to fetch instruments: %s", e)

    async def _refresh_instruments_loop(self) -> None:
        while self.running:
            await asyncio.sleep(3600)
            await self._fetch_and_store_instruments()

    # ── Fee rates ────────────────────────────────────────────────────────

    async def _fetch_fee_rates(self) -> None:
        try:
            r = self.redis.client
            fees = {}
            for inst_type in ("SWAP", "FUTURES", "SPOT"):
                resp = await self._rest_request(
                    "GET", f"/api/v5/account/trade-fee?instType={inst_type}"
                )
                if resp and resp.get("code") == "0" and resp.get("data"):
                    item = resp["data"][0]
                    fees[inst_type] = {
                        "maker":  item.get("maker", ""),
                        "taker":  item.get("taker", ""),
                        "makerU": item.get("makerU", ""),
                        "takerU": item.get("takerU", ""),
                    }
            await r.set(f"fees:{self.gateway_name}", json.dumps(fees), ex=86400)
            self.logger.info("Fetched fee rates: %s", list(fees.keys()))
        except Exception as e:
            self.logger.error("Failed to fetch fee rates: %s", e)

    # ── Cancel-on-disconnect ─────────────────────────────────────────────

    async def _cancel_all_after(self, timeout_seconds: int) -> None:
        path = "/api/v5/trade/cancel-all-after"
        body = {"timeOut": str(timeout_seconds)}
        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            self.logger.info("cancel-all-after set to %ds", timeout_seconds)
        else:
            self.logger.error("cancel-all-after failed: %s", resp)

    async def _cod_keepalive_loop(self) -> None:
        while self.running:
            try:
                await self._cancel_all_after(120)
            except Exception as e:
                self.logger.error("CoD keepalive error: %s", e)
            await asyncio.sleep(90)

    async def disconnect_exchange(self) -> None:
        if self._has_credentials() and self._session:
            try:
                await self._cancel_all_after(0)
            except Exception:
                pass
        if self._ws_public:
            await self._ws_public.close()
        if self._ws_private:
            await self._ws_private.close()
        if self._session:
            await self._session.close()
