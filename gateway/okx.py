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
from typing import Optional

import aiohttp

from gateway.base import BaseGateway


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
        # Local order book state per symbol for incremental update management
        self._orderbooks: dict = {}

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

        if self._has_credentials():
            # Connect and authenticate private websocket for order/fill streams
            self.logger.info("Connecting private WS: %s", self.WS_PRIVATE)
            self._ws_private = await self._session.ws_connect(self.WS_PRIVATE)
            self.logger.info("Private WS connected, authenticating...")
            await self._authenticate()
            self.logger.info("Authentication successful")
            await self._subscribe_private()
            await self._sync_pending_orders()
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
        """Subscribe to order and position updates."""
        args = []
        for symbol in self.symbols:
            args.append({"channel": "orders", "instId": symbol})
            args.append({"channel": "positions", "instId": symbol})
        await self._ws_private.send_json({"op": "subscribe", "args": args})

    async def _stream_market_data(self) -> None:
        """Subscribe to tickers and orderbook, then stream."""
        # Subscribe
        args = []
        for symbol in self.symbols:
            args.append({"channel": "tickers", "instId": symbol})
            args.append({"channel": "books", "instId": symbol})
        await self._ws_public.send_json({"op": "subscribe", "args": args})

        # Listen and publish (private task only when credentials were provided)
        if self._has_credentials():
            asyncio.create_task(self._listen_private())

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
                await self.publish_tick(symbol, {
                    "bid": float(record.get("bidPx", 0)),
                    "ask": float(record.get("askPx", 0)),
                    "bid_size": float(record.get("bidSz", 0)),
                    "ask_size": float(record.get("askSz", 0)),
                    "last": float(record.get("last", 0)),
                    "last_size": float(record.get("lastSz", 0)),
                    "timestamp": time.time(),
                })
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

    async def _handle_private_msg(self, data: dict) -> None:
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        records = data.get("data", [])

        for record in records:
            if channel == "orders":
                state = record.get("state", "")
                order_info = {
                    "order_id": record.get("ordId", ""),
                    "symbol": record.get("instId", ""),
                    "side": record.get("side", ""),
                    "order_type": record.get("ordType", ""),
                    "price": record.get("px", "0"),
                    "quantity": record.get("sz", "0"),
                    "filled_qty": record.get("accFillSz", "0"),
                    "avg_price": record.get("avgPx", "0"),
                    "state": state,
                    "timestamp": record.get("uTime", ""),
                }
                await self._update_pending_orders(order_info)

                if state == "filled":
                    await self.publish_fill({
                        "order_id": record.get("ordId", ""),
                        "symbol": record.get("instId", ""),
                        "side": "buy" if record.get("side") == "buy" else "sell",
                        "price": float(record.get("avgPx", 0)),
                        "quantity": float(record.get("fillSz", 0)),
                        "commission": float(record.get("fee", 0)),
                        "timestamp": time.time(),
                    })

    async def _sync_pending_orders(self) -> None:
        """On startup, reconcile Redis active orders with OKX actual state."""
        try:
            live_orders = await self.query_pending_orders()
            live_ids = {o.get("ordId", "") for o in live_orders}
            synced = []
            for o in live_orders:
                synced.append({
                    "order_id": o.get("ordId", ""),
                    "symbol": o.get("instId", ""),
                    "side": o.get("side", ""),
                    "order_type": o.get("ordType", ""),
                    "price": o.get("px", "0"),
                    "quantity": o.get("sz", "0"),
                    "filled_qty": o.get("accFillSz", "0"),
                    "avg_price": o.get("avgPx", "0"),
                    "state": o.get("state", "live"),
                    "timestamp": o.get("uTime", ""),
                })
            r = self.redis.client
            await r.set("pending_orders:okx", json.dumps(synced))
            self.logger.info("Synced active orders from OKX: %d live", len(synced))
        except Exception as e:
            self.logger.error("Failed to sync pending orders: %s", e)

    async def _update_pending_orders(self, order_info: dict) -> None:
        """Track active orders in Redis; log terminal orders to history stream."""
        try:
            key = "pending_orders:okx"
            r = self.redis.client
            raw = await r.get(key)
            orders = json.loads(raw) if raw else []
            oid = order_info["order_id"]
            terminal = {"filled", "canceled", "cancelled"}
            orders = [o for o in orders if o["order_id"] != oid]
            if order_info["state"] not in terminal:
                orders.append(order_info)
            else:
                await r.xadd("finished_orders", {
                    k: str(v) for k, v in order_info.items()
                }, maxlen=500)
            await r.set(key, json.dumps(orders))
        except Exception as e:
            self.logger.debug("Failed to update pending orders in Redis: %s", e)

    async def send_order(self, order: dict) -> str:
        """Place order via REST API."""
        path = "/api/v5/trade/order"
        body = {
            "instId": order["symbol"],
            "tdMode": "cross",  # cross margin
            "side": order["side"],
            "ordType": order.get("order_type", "limit"),
            "sz": str(order["quantity"]),
        }
        if order.get("order_type") == "limit":
            body["px"] = str(order["price"])

        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            order_id = resp["data"][0]["ordId"]
            self.logger.info("Order placed: %s %s %s qty=%s @ %s -> ordId=%s",
                             order["side"], order.get("order_type", "limit"),
                             order["symbol"], order["quantity"],
                             order.get("price", "MKT"), order_id)
            await self._update_pending_orders({
                "order_id": order_id,
                "symbol": order["symbol"],
                "side": order["side"],
                "order_type": order.get("order_type", "limit"),
                "price": str(order.get("price", 0)),
                "quantity": str(order["quantity"]),
                "filled_qty": "0",
                "state": "live",
                "timestamp": str(int(time.time() * 1000)),
            })
            return order_id
        else:
            self.logger.error("Order failed: %s", resp)
            return ""

    async def cancel_order(self, order: dict) -> None:
        path = "/api/v5/trade/cancel-order"
        body = {
            "instId": order.get("symbol", ""),
            "ordId": order.get("order_id", ""),
        }
        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            self.logger.info("Cancelled order %s", order.get("order_id"))
            await self._sync_single_order(order.get("order_id", ""), order.get("symbol", ""))
        else:
            self.logger.error("Cancel failed: %s", resp)

    async def amend_order(self, order: dict) -> None:
        path = "/api/v5/trade/amend-order"
        body = {
            "instId": order.get("symbol", ""),
            "ordId": order.get("order_id", ""),
        }
        if order.get("price"):
            body["newPx"] = str(order["price"])
        if order.get("quantity"):
            body["newSz"] = str(order["quantity"])
        resp = await self._rest_request("POST", path, body)
        if resp and resp.get("code") == "0":
            self.logger.info("Amended order %s", order.get("order_id"))
            await self._sync_single_order(order.get("order_id", ""), order.get("symbol", ""))
        else:
            self.logger.error("Amend failed: %s", resp)

    async def _sync_single_order(self, order_id: str, symbol: str) -> None:
        """Query an order's latest state from OKX and update Redis."""
        try:
            path = f"/api/v5/trade/order?instId={symbol}&ordId={order_id}"
            resp = await self._rest_request("GET", path)
            if resp and resp.get("code") == "0" and resp.get("data"):
                record = resp["data"][0]
                await self._update_pending_orders({
                    "order_id": record.get("ordId", ""),
                    "symbol": record.get("instId", ""),
                    "side": record.get("side", ""),
                    "order_type": record.get("ordType", ""),
                    "price": record.get("px", "0"),
                    "quantity": record.get("sz", "0"),
                    "filled_qty": record.get("accFillSz", "0"),
                    "avg_price": record.get("avgPx", "0"),
                    "state": record.get("state", ""),
                    "timestamp": record.get("uTime", ""),
                })
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

    async def disconnect_exchange(self) -> None:
        if self._ws_public:
            await self._ws_public.close()
        if self._ws_private:
            await self._ws_private.close()
        if self._session:
            await self._session.close()
