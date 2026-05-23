"""
Webull gateway: market data + trading for US Stocks/ETFs and HK Stocks.

- Auth: optional. With email/password/trade_pin set, the gateway logs in
  and unlocks order entry. Without credentials it runs market-data-only
  (anonymous L1 MQTT stream — same as the dashboard depth viewer).
- Market data: MQTT over WebSocket to wspush.webullbroker.com.
  Topic 102 = L1 quote snapshot, 105 = trade ticks, 104 = L2 depth
  (L2 requires login + paid subscription).
- Trading: webull library REST API. Market / Limit / Stop / Stop-Limit,
  with extended-hours support.
"""
import asyncio
import json
import os
import ssl
import time
import uuid
from typing import Optional

import paho.mqtt.client as mqtt

from gateway.base import BaseGateway


class WebullGateway(BaseGateway):
    """Webull stock/ETF gateway."""

    MQTT_HOST = "wspush.webullbroker.com"
    MQTT_PORT = 443
    REGION_CODES = {"US": 6, "HK": 2}

    # MQTT topic types
    TOPIC_L1     = "102"   # quote snapshot (best bid/ask, last, volume)
    TOPIC_TRADES = "105"   # trade ticks
    TOPIC_L2     = "104"   # 10-level depth (auth + subscription)

    # Order type mapping (generic -> webull)
    ORDER_TYPE_MAP = {
        "market":     "MKT",
        "limit":      "LMT",
        "stop":       "STP",
        "stop_limit": "STP LMT",
    }

    def __init__(
        self,
        email: str,
        password: str,
        trade_pin: str,
        mfa_seed: str,
        device_id: str,
        symbols: list,
        region: str = "US",
        paper: bool = True,
        extended_hours: bool = True,
        **kwargs,
    ):
        super().__init__(gateway_name="webull", **kwargs)
        self.email = email
        self.password = password
        self.trade_pin = trade_pin
        self.mfa_seed = mfa_seed
        self.symbols = symbols
        self.region = region.upper()
        self.paper = paper
        self.extended_hours = extended_hours
        self.device_id = device_id or self._make_did()

        self._wb = None            # webull client (lazy import)
        self._authenticated = False
        self._access_token: Optional[str] = None

        self._mqtt: Optional[mqtt.Client] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._mq_queue: Optional[asyncio.Queue] = None

        # Symbol <-> Webull tickerId
        self._symbol_to_tid: dict = {}
        self._tid_to_symbol: dict = {}

        # Local L2 book state (only used when L2 stream is available)
        self._orderbooks: dict = {}

        mode = "PAPER" if paper else "LIVE"
        self.logger.info("Initialized in %s mode (region=%s, ext_hours=%s)",
                         mode, self.region, self.extended_hours)

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────
    @staticmethod
    def _make_did() -> str:
        return uuid.uuid4().hex[:32]

    def _has_credentials(self) -> bool:
        def _set(val):
            return bool(val) and not str(val).startswith("${")
        return _set(self.email) and _set(self.password) and _set(self.trade_pin)

    def _build_client(self):
        from webull import webull, paper_webull
        return paper_webull() if self.paper else webull()

    # ──────────────────────────────────────────────────────────────────────
    # Connect / authenticate
    # ──────────────────────────────────────────────────────────────────────
    async def connect_exchange(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._mq_queue = asyncio.Queue()

        # 1) Login (best effort — without it we run market-data-only)
        self._wb = self._build_client()
        if self._has_credentials():
            try:
                await self._login()
                self._authenticated = True
                self.logger.info("Webull login successful")
            except Exception as exc:
                self.logger.warning("Webull login failed (%s) — running market-data-only", exc)
                self._authenticated = False
        else:
            self.logger.info("No Webull credentials — running market-data-only")

        # 2) Resolve tickerIds for configured symbols
        await self._resolve_tickers()

        # 3) Connect MQTT (uses access_token if authenticated, anonymous otherwise)
        self._connect_mqtt()

        # 4) Start the asyncio worker that drains MQTT->queue
        asyncio.create_task(self._process_mq_queue())

    async def _login(self) -> None:
        """Log into Webull. Uses TOTP seed for unattended MFA if available."""
        run = self._loop.run_in_executor

        if self.mfa_seed:
            import pyotp
            mfa = pyotp.TOTP(self.mfa_seed).now()
            self.logger.info("Using TOTP-derived MFA code")
        else:
            # No seed → must trigger SMS/email MFA; without operator input it will fail
            await run(None, self._wb.get_mfa, self.email)
            raise RuntimeError("WEBULL_MFA_SEED not set; cannot complete MFA unattended")

        result = await run(
            None,
            lambda: self._wb.login(
                username=self.email,
                password=self.password,
                device_name="claude-trading-bot",
                mfa=mfa,
            ),
        )
        if not result or not result.get("accessToken"):
            raise RuntimeError(f"login response missing accessToken: {result}")
        self._access_token = result["accessToken"]

        # Trade token unlocks order placement
        trade_ok = await run(None, lambda: self._wb.get_trade_token(self.trade_pin))
        if not trade_ok:
            raise RuntimeError("trade_pin rejected by Webull")

    async def _resolve_tickers(self) -> None:
        """Fetch tickerId for each configured symbol."""
        run = self._loop.run_in_executor
        for symbol in self.symbols:
            try:
                tid = await run(None, self._wb.get_ticker, symbol)
                if tid:
                    self._symbol_to_tid[symbol] = tid
                    self._tid_to_symbol[str(tid)] = symbol
                    self.logger.info("Resolved %s -> tickerId=%s", symbol, tid)
                else:
                    self.logger.warning("No tickerId for %s", symbol)
            except Exception as exc:
                self.logger.warning("get_ticker(%s) failed: %s", symbol, exc)

    # ──────────────────────────────────────────────────────────────────────
    # MQTT (market data)
    # ──────────────────────────────────────────────────────────────────────
    def _connect_mqtt(self) -> None:
        self._mqtt = mqtt.Client(client_id=self.device_id, transport="websockets")
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_message = self._on_mqtt_message
        self._mqtt.tls_set_context(ssl.create_default_context())
        self._mqtt.username_pw_set("test", password="test")
        self._mqtt.connect(self.MQTT_HOST, self.MQTT_PORT, keepalive=30)

        header = {"did": self.device_id, "hl": "en",
                  "app": "desktop", "os": "web", "osType": "windows"}
        if self._access_token:
            header["access_token"] = self._access_token
        say_hello = json.dumps({"header": header})
        self._mqtt.loop()
        self._mqtt.subscribe(say_hello)
        self._mqtt.loop()
        self._mqtt.loop_start()
        self.logger.info("MQTT connected to %s:%d (auth=%s)",
                         self.MQTT_HOST, self.MQTT_PORT, bool(self._access_token))

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self.logger.warning("MQTT connect rc=%d", rc)
            return
        # Subscribe to L1 + trades for each ticker. L2 only if authenticated.
        types = [self.TOPIC_L1, self.TOPIC_TRADES]
        if self._authenticated:
            types.append(self.TOPIC_L2)
        for symbol, tid in self._symbol_to_tid.items():
            for t in types:
                topic = '{' + f'"tickerIds":[{tid}],"type":"{t}"' + '}'
                client.subscribe(topic)
            self.logger.info("MQTT subscribed: %s (tId=%s, types=%s)", symbol, tid, types)

    def _on_mqtt_message(self, client, userdata, msg):
        """Runs in MQTT thread — bridge to asyncio queue."""
        try:
            topic = json.loads(msg.topic)
            data = json.loads(msg.payload)
            asyncio.run_coroutine_threadsafe(
                self._mq_queue.put((topic, data)), self._loop
            )
        except Exception as exc:
            self.logger.debug("MQTT parse error: %s", exc)

    async def _process_mq_queue(self) -> None:
        """Drain MQTT messages and publish to ZMQ."""
        while self.running:
            try:
                topic, data = await asyncio.wait_for(self._mq_queue.get(), timeout=1.0)
                tid = str(topic.get("tickerId", ""))
                symbol = self._tid_to_symbol.get(tid)
                if not symbol:
                    continue
                t = str(topic.get("type", ""))

                if t == self.TOPIC_L1:
                    await self._handle_l1(symbol, data)
                elif t == self.TOPIC_TRADES:
                    await self._handle_trade(symbol, data)
                elif t == self.TOPIC_L2:
                    await self._handle_l2(symbol, data)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                self.logger.warning("Queue processing error: %s", exc)

    async def _handle_l1(self, symbol: str, data: dict) -> None:
        """Webull anonymous MQTT delivers last/OHLC/volume but NOT bid/ask.
        If we are authenticated, bid/ask may show up — handle both shapes."""
        bid  = data.get("bid")
        ask  = data.get("ask")
        last = data.get("close") or data.get("pPrice") or data.get("price")
        if bid is None and ask is None and last is None:
            return
        last_f = float(last) if last is not None else 0.0
        await self.publish_tick(symbol, {
            "bid":        float(bid) if bid is not None else last_f,
            "ask":        float(ask) if ask is not None else last_f,
            "bid_size":   float(data.get("bidSize", 0) or 0),
            "ask_size":   float(data.get("askSize", 0) or 0),
            "last":       last_f,
            "last_size":  float(data.get("volume", 0) or 0),
            "open":       float(data.get("open", 0) or 0),
            "high":       float(data.get("high", 0) or 0),
            "low":        float(data.get("low", 0) or 0),
            "change":     float(data.get("change", 0) or 0),
            "timestamp":  time.time(),
        })

    async def _handle_trade(self, symbol: str, data: dict) -> None:
        price = data.get("price") or data.get("pPrice")
        size  = data.get("volume") or data.get("size") or 0
        if price is None:
            return
        # Publish as a tick with last-trade only
        await self.publish_tick(symbol, {
            "bid": 0.0, "ask": 0.0, "bid_size": 0.0, "ask_size": 0.0,
            "last":       float(price),
            "last_size":  float(size),
            "is_trade":   True,
            "timestamp":  time.time(),
        })

    async def _handle_l2(self, symbol: str, data: dict) -> None:
        raw_bids = data.get("bidList", []) or []
        raw_asks = data.get("askList", []) or []
        if not raw_bids and not raw_asks:
            return
        bids = sorted([[float(b["price"]), float(b["volume"])] for b in raw_bids], reverse=True)[:10]
        asks = sorted([[float(a["price"]), float(a["volume"])] for a in raw_asks])[:10]
        await self.publish_orderbook(symbol, {
            "bids": bids, "asks": asks, "timestamp": time.time(),
        })

    async def _stream_market_data(self) -> None:
        """BaseGateway expects this; the MQTT loop is already running in a thread
        and being drained by _process_mq_queue. Just idle here while the process
        is alive."""
        while self.running:
            await asyncio.sleep(1)

    # ──────────────────────────────────────────────────────────────────────
    # Orders
    # ──────────────────────────────────────────────────────────────────────
    def _require_auth(self) -> bool:
        if not self._authenticated:
            self.logger.error("Cannot send orders — Webull not authenticated")
            return False
        return True

    async def send_order(self, order: dict) -> str:
        if not self._require_auth():
            return ""
        symbol = order["symbol"]
        tid = self._symbol_to_tid.get(symbol)
        if not tid:
            try:
                tid = await self._loop.run_in_executor(None, self._wb.get_ticker, symbol)
            except Exception:
                tid = None
        if not tid:
            self.logger.error("Order rejected: no tickerId for %s", symbol)
            return ""

        side = "BUY" if str(order.get("side", "buy")).lower() == "buy" else "SELL"
        order_type_raw = str(order.get("order_type", "limit")).lower()
        order_type = self.ORDER_TYPE_MAP.get(order_type_raw, "LMT")
        quantity = int(order["quantity"])
        price = float(order.get("price", 0) or 0)
        stop_price = order.get("stop_price")
        enforce = str(order.get("time_in_force", "DAY")).upper()  # DAY/GTC

        kwargs = dict(
            tId=tid,
            price=price if order_type in ("LMT", "STP LMT") else 0,
            action=side,
            orderType=order_type,
            enforce=enforce,
            quant=quantity,
            outsideRegularTradingHour=self.extended_hours,
        )
        if order_type in ("STP", "STP LMT") and stop_price is not None:
            kwargs["stpPrice"] = float(stop_price)

        try:
            resp = await self._loop.run_in_executor(
                None, lambda: self._wb.place_order(**kwargs)
            )
        except Exception as exc:
            self.logger.error("place_order error: %s", exc)
            return ""

        if not isinstance(resp, dict):
            self.logger.error("place_order returned non-dict: %r", resp)
            return ""
        if not resp.get("success", True) and resp.get("code"):
            self.logger.error("Order rejected: %s", resp)
            return ""

        order_id = str(resp.get("orderId") or resp.get("data", {}).get("orderId", ""))
        self.logger.info("Order placed: %s %s %s @ %s -> id=%s",
                         side, quantity, symbol, price or "MKT", order_id)
        return order_id

    async def cancel_order(self, order: dict) -> None:
        if not self._require_auth():
            return
        order_id = order.get("order_id", "")
        try:
            await self._loop.run_in_executor(None, self._wb.cancel_order, order_id)
            self.logger.info("Cancelled order %s", order_id)
        except Exception as exc:
            self.logger.error("cancel_order(%s) error: %s", order_id, exc)

    async def amend_order(self, order: dict) -> None:
        if not self._require_auth():
            return
        order_id = order.get("order_id", "")
        try:
            await self._loop.run_in_executor(
                None, self._wb.modify_order, order_id,
                order.get("price"), order.get("quantity"),
            )
            self.logger.info("Amended order %s", order_id)
        except Exception as exc:
            self.logger.error("amend_order(%s) error: %s", order_id, exc)

    async def query_positions(self) -> list:
        if not self._require_auth():
            return []
        try:
            positions = await self._loop.run_in_executor(None, self._wb.get_positions)
            return positions or []
        except Exception as exc:
            self.logger.error("get_positions error: %s", exc)
            return []

    async def disconnect_exchange(self) -> None:
        if self._mqtt:
            try:
                self._mqtt.loop_stop()
                self._mqtt.disconnect()
            except Exception:
                pass
