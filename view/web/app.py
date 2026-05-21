"""
Web dashboard API server.
Serves REST endpoints and WebSocket for live updates.
"""
import asyncio
import json
import logging
import os
import ssl
import threading
import time
from pathlib import Path
from typing import Optional

import aiohttp
import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import redis.asyncio as aioredis

from core.config import load_env
from core.logger import get_logger
from core.zmq_channels import MultiSubscriber, GATEWAY_MARKET_DATA_PORTS

load_env()
logger = get_logger("view_web")

STATIC_DIR = Path(__file__).parent / "static"
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

app = FastAPI(title="Trading Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_redis = None

# Orderbook cache keyed by "{exchange}:{env}:{symbol}"
# e.g. "OKX:PAPER:BTC-USDT-SWAP", "WEBULL:LIVE:AAPL"
_orderbooks: dict = {}
_orderbook_seq: dict = {}


# ── OKX depth manager ────────────────────────────────────────────────────────

class OKXDepthManager:
    """OKX public WebSocket — one instance per env (PAPER / LIVE)."""

    WS_ENDPOINTS = {
        "PAPER": "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999",
        "LIVE":  "wss://ws.okx.com:8443/ws/v5/public",
    }
    OKX_REST = "https://www.okx.com"
    INSTRUMENTS_TTL = 300

    def __init__(self, env: str):
        self.env = env
        self._ws_url = self.WS_ENDPOINTS[env]
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws = None
        self._subscribed: set = set()
        self._local_books: dict = {}
        self._instruments: dict = {}    # instType -> list
        self._instruments_ts: dict = {} # instType -> float
        self._running = False

    def _key(self, symbol: str) -> str:
        return f"OKX:{self.env}:{symbol}"

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._running = True
        asyncio.create_task(self._ws_loop())

    async def stop(self):
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session:
            await self._session.close()

    async def subscribe(self, symbol: str):
        if symbol in self._subscribed:
            return
        self._subscribed.add(symbol)
        if self._ws and not self._ws.closed:
            await self._ws.send_json({
                "op": "subscribe",
                "args": [{"channel": "books", "instId": symbol}],
            })

    async def get_instruments(self, inst_type: str = "SWAP") -> list:
        now = time.time()
        if inst_type in self._instruments and now - self._instruments_ts.get(inst_type, 0) < self.INSTRUMENTS_TTL:
            return self._instruments[inst_type]

        url = f"{self.OKX_REST}/api/v5/public/instruments?instType={inst_type}"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                result = data.get("data", [])
                self._instruments[inst_type] = result
                self._instruments_ts[inst_type] = now
                return result
        except Exception as exc:
            logger.warning("OKX instruments fetch error (%s): %s", self.env, exc)
            return self._instruments.get(inst_type, [])

    async def _ws_loop(self):
        while self._running:
            try:
                self._ws = await self._session.ws_connect(self._ws_url)
                if self._subscribed:
                    args = [{"channel": "books", "instId": s} for s in self._subscribed]
                    await self._ws.send_json({"op": "subscribe", "args": args})
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_msg(json.loads(msg.data))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                logger.warning("OKX depth WS error [%s]: %s", self.env, exc)
            if self._running:
                await asyncio.sleep(5)

    async def _handle_msg(self, data: dict):
        arg = data.get("arg", {})
        if arg.get("channel") != "books":
            return
        action = data.get("action", "snapshot")
        symbol = arg.get("instId", "")
        records = data.get("data", [])
        if not symbol or not records:
            return

        if symbol not in self._local_books:
            self._local_books[symbol] = {"bids": {}, "asks": {}}
        book = self._local_books[symbol]

        for record in records:
            if action == "snapshot":
                book["bids"] = {float(e[0]): float(e[1]) for e in record.get("bids", [])}
                book["asks"] = {float(e[0]): float(e[1]) for e in record.get("asks", [])}
            else:
                for e in record.get("bids", []):
                    p, s = float(e[0]), float(e[1])
                    book["bids"].pop(p, None) if s == 0 else book["bids"].__setitem__(p, s)
                for e in record.get("asks", []):
                    p, s = float(e[0]), float(e[1])
                    book["asks"].pop(p, None) if s == 0 else book["asks"].__setitem__(p, s)

        top_bids = sorted(book["bids"].items(), reverse=True)[:10]
        top_asks = sorted(book["asks"].items())[:10]
        key = self._key(symbol)
        _orderbooks[key] = {
            "bids": [[p, s] for p, s in top_bids],
            "asks": [[p, s] for p, s in top_asks],
            "timestamp": time.time(),
        }
        _orderbook_seq[key] = _orderbook_seq.get(key, 0) + 1


_depth_managers: dict[str, OKXDepthManager] = {
    "OKX:PAPER": OKXDepthManager("PAPER"),
    "OKX:LIVE":  OKXDepthManager("LIVE"),
}


# ── Webull depth manager ──────────────────────────────────────────────────────

class WebullDepthManager:
    """
    Streams Webull L2 orderbook (topic 104) via MQTT over WebSocket.
    No authentication required for price streaming.
    MQTT runs in a background thread; data is bridged to asyncio via a queue.
    """
    MQTT_HOST = "wspush.webullbroker.com"
    MQTT_PORT = 443
    SEARCH_URL = "https://quotes-gw.webullbroker.com/api/search/pc/tickers"
    REGION_CODES = {"US": 6, "HK": 2}

    def __init__(self):
        self._did: Optional[str] = None
        self._mqtt: Optional[mqtt.Client] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._subscribed: dict = {}       # symbol -> tId
        self._tid_to_symbol: dict = {}    # str(tId) -> symbol
        self._queue: Optional[asyncio.Queue] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False

    def _make_did(self) -> str:
        import uuid
        return str(uuid.uuid4()).replace("-", "")[:32]

    async def start(self):
        self._loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue()
        self._session = aiohttp.ClientSession()
        self._did = self._make_did()
        self._running = True
        self._connect_mqtt()
        asyncio.create_task(self._process_queue())

    async def stop(self):
        self._running = False
        if self._mqtt:
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
        if self._session:
            await self._session.close()

    def _connect_mqtt(self):
        self._mqtt = mqtt.Client(client_id=self._did, transport='websockets')
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.tls_set_context(ssl.create_default_context())
        self._mqtt.username_pw_set('test', password='test')
        try:
            self._mqtt.connect(self.MQTT_HOST, self.MQTT_PORT, keepalive=30)
            # Send hello handshake
            say_hello = json.dumps({"header": {
                "did": self._did, "hl": "en",
                "app": "desktop", "os": "web", "osType": "windows",
            }})
            self._mqtt.loop()
            self._mqtt.subscribe(say_hello)
            self._mqtt.loop()
            self._mqtt.loop_start()   # background network thread
            logger.info("Webull MQTT connected to %s:%d", self.MQTT_HOST, self.MQTT_PORT)
        except Exception as exc:
            logger.warning("Webull MQTT connect error: %s", exc)

    # Anonymous Webull MQTT can only access L1 (topic 102 = quote snapshot).
    # L2 (topic 104) requires login + active L2 subscription.
    L1_TOPIC = "102"

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            # Resubscribe all symbols after reconnect
            for symbol, tId in self._subscribed.items():
                client.subscribe('{' + f'"tickerIds":[{tId}],"type":"{self.L1_TOPIC}"' + '}')

    def _on_message(self, client, userdata, msg):
        try:
            topic = json.loads(msg.topic)
            data = json.loads(msg.payload)
            if str(topic.get("type")) == self.L1_TOPIC:
                asyncio.run_coroutine_threadsafe(
                    self._queue.put((topic, data)), self._loop
                )
        except Exception as exc:
            logger.debug("Webull MQTT message parse error: %s", exc)

    async def _process_queue(self):
        while self._running:
            try:
                topic, data = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                tid = str(topic.get("tickerId", ""))
                symbol = self._tid_to_symbol.get(tid)
                if not symbol:
                    continue

                # Webull anonymous MQTT (topic 102) delivers last + OHLC + volume,
                # but NOT bid/ask (those are auth-gated). Build a synthetic single
                # row with bid=ask=last so the depth grid still shows the price.
                last_px = data.get("close") or data.get("pPrice") or data.get("price")
                if not last_px:
                    continue
                last_px = float(last_px)
                vol = float(data.get("volume", 0) or 0)
                bids = [[last_px, vol]]
                asks = [[last_px, vol]]

                key = f"WEBULL:LIVE:{symbol}"
                _orderbooks[key] = {
                    "bids": bids,
                    "asks": asks,
                    "last":         last_px,
                    "open":         float(data.get("open", 0) or 0),
                    "high":         float(data.get("high", 0) or 0),
                    "low":          float(data.get("low", 0) or 0),
                    "volume":       vol,
                    "change":       float(data.get("change", 0) or 0),
                    "change_ratio": float(data.get("changeRatio", 0) or 0),
                    "level":        "Last-only (anon)",
                    "timestamp":    time.time(),
                }
                _orderbook_seq[key] = _orderbook_seq.get(key, 0) + 1
            except asyncio.TimeoutError:
                pass
            except Exception as exc:
                logger.warning("Webull queue error: %s", exc)

    async def subscribe(self, symbol: str, region: str = "US"):
        if symbol in self._subscribed:
            return
        # Look up tickerId via REST
        try:
            primary, fallback = (region.upper(), "HK" if region.upper() == "US" else "US")
            results = await self.search(symbol, region=primary)
            match = next((r for r in results if r.get("disSymbol", "").upper() == symbol.upper()), None)
            if not match and results:
                match = results[0]
            if not match:
                results_alt = await self.search(symbol, region=fallback)
                match = next((r for r in results_alt if r.get("disSymbol", "").upper() == symbol.upper()), None)
                if not match and results_alt:
                    match = results_alt[0]

            if not match:
                logger.warning("Webull: no ticker found for %s", symbol)
                return

            tId = match.get("tickerId")
            if not tId:
                return

            self._subscribed[symbol] = tId
            self._tid_to_symbol[str(tId)] = symbol

            if self._mqtt:
                self._mqtt.subscribe('{' + f'"tickerIds":[{tId}],"type":"{self.L1_TOPIC}"' + '}')
            logger.info("Webull subscribed to %s (tId=%s, L1)", symbol, tId)
        except Exception as exc:
            logger.warning("Webull subscribe error for %s: %s", symbol, exc)

    def _api_headers(self) -> dict:
        return {
            "App": "global",
            "App-Group": "broker",
            "Appid": "wb_web_app",
            "Device-Type": "Web",
            "Did": self._did or "",
            "Hl": "en",
            "Locale": "eng",
            "Os": "web",
            "Osv": "i9zh",
            "Platform": "web",
            "Ph": "MacOS Firefox",
            "Tz": "America/New_York",
            "Ver": "3.39.18",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "*/*",
        }

    async def search(self, keyword: str, region: str = "US") -> list:
        region_code = self.REGION_CODES.get(region.upper(), 6)
        url = f"{self.SEARCH_URL}?keyword={keyword}&pageIndex=1&pageSize=20&regionId={region_code}"
        try:
            async with self._session.get(
                url,
                headers=self._api_headers(),
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Webull search HTTP %d for '%s'", resp.status, keyword)
                    return []
                data = await resp.json(content_type=None)
                if isinstance(data, list):
                    return data
                return data.get("data", []) or data.get("list", [])
        except Exception as exc:
            logger.warning("Webull search error (%s): %s", keyword, exc)
            return []


_webull_manager = WebullDepthManager()


# ── ZMQ subscriber (mirrors OKX gateway, always PAPER) ───────────────────────

async def _zmq_orderbook_subscriber():
    sub = MultiSubscriber(GATEWAY_MARKET_DATA_PORTS, topics=["orderbook."])
    try:
        while True:
            try:
                _topic, data = await sub.receive()
                symbol = data.get("symbol", "")
                gateway = (data.get("gateway") or "").upper()
                if not symbol or not gateway:
                    continue
                # OKX dashboard distinguishes PAPER/LIVE; everything else is LIVE.
                env = "PAPER" if gateway == "OKX" else "LIVE"
                key = f"{gateway}:{env}:{symbol}"
                _orderbooks[key] = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                    "timestamp": data.get("timestamp", 0),
                }
                _orderbook_seq[key] = _orderbook_seq.get(key, 0) + 1
            except Exception as exc:
                logger.warning("ZMQ orderbook recv error: %s", exc)
                await asyncio.sleep(1)
    finally:
        sub.close()


# ── App lifecycle ─────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(_zmq_orderbook_subscriber())
    for mgr in _depth_managers.values():
        await mgr.start()
    await _webull_manager.start()


@app.on_event("shutdown")
async def shutdown():
    global _redis
    for mgr in _depth_managers.values():
        await mgr.stop()
    await _webull_manager.stop()
    if _redis:
        await _redis.aclose()


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/strategies")
async def get_strategies():
    r = await get_redis()
    raw = await r.get("dashboard:strategies")
    return json.loads(raw) if raw else {}


@app.get("/api/prices")
async def get_prices():
    r = await get_redis()
    raw = await r.get("dashboard:prices")
    return json.loads(raw) if raw else {}


@app.get("/api/positions")
async def get_positions():
    r = await get_redis()
    keys = await r.keys("position:*")
    positions = []
    for key in keys:
        pos = await r.hgetall(key)
        pos["_key"] = key
        positions.append(pos)
    return positions


@app.get("/api/trades")
async def get_trades(count: int = 50):
    r = await get_redis()
    trades = await r.xrange("trades", "-", "+", count=count)
    return [{"id": t[0], **t[1]} for t in trades]


@app.get("/api/heartbeats")
async def get_heartbeats():
    r = await get_redis()
    keys = await r.keys("heartbeat:*")
    result = {}
    for key in keys:
        name = key.replace("heartbeat:", "")
        hb = await r.hgetall(key)
        ttl = await r.ttl(key)
        hb["ttl"] = ttl
        result[name] = hb
    return result


@app.get("/api/pnl/{strategy}")
async def get_pnl(strategy: str, count: int = 100):
    r = await get_redis()
    key = f"pnl:{strategy}"
    history = await r.xrange(key, "-", "+", count=count)
    return [{"id": h[0], **h[1]} for h in history]


@app.post("/api/strategy/{name}/params")
async def update_params(name: str, params: dict):
    r = await get_redis()
    cmd = json.dumps({"action": "update_params", "strategy": name, "params": params})
    await r.publish("strategy:command", cmd)
    return {"status": "ok", "message": f"Params update sent to {name}"}


@app.post("/api/strategy/{name}/enable")
async def enable_strategy(name: str):
    r = await get_redis()
    await r.publish("strategy:command", json.dumps({"action": "enable", "strategy": name}))
    return {"status": "ok", "message": f"{name} enabled"}


@app.post("/api/strategy/{name}/disable")
async def disable_strategy(name: str):
    r = await get_redis()
    await r.publish("strategy:command", json.dumps({"action": "disable", "strategy": name}))
    return {"status": "ok", "message": f"{name} disabled"}


@app.get("/api/symbols")
async def get_symbols(exchange: str = "OKX", env: str = "PAPER"):
    prefix = f"{exchange.upper()}:{env.upper()}:"
    return sorted(k[len(prefix):] for k in _orderbooks if k.startswith(prefix))


@app.get("/api/depth/{symbol:path}")
async def get_depth(symbol: str, exchange: str = "OKX", env: str = "PAPER"):
    key = f"{exchange.upper()}:{env.upper()}:{symbol}"
    return _orderbooks.get(key, {"bids": [], "asks": [], "timestamp": 0})


@app.get("/api/exchange/instruments")
async def get_exchange_instruments(
    exchange: str = "OKX",
    env: str = "PAPER",
    instType: str = "SWAP",
    keyword: str = "",
    region: str = "US",
):
    """
    OKX:    returns instruments list for the given instType.
    WEBULL: searches symbols by keyword + region.
    """
    if exchange.upper() == "WEBULL":
        if not keyword:
            return []
        results = await _webull_manager.search(keyword, region)
        return [
            {
                "instId":   r.get("disSymbol", r.get("symbol", "")),
                "name":     r.get("name", ""),
                "exchange": r.get("disExchangeCode", ""),
                "region":   region,
            }
            for r in results
            if r.get("disSymbol") or r.get("symbol")
        ]
    else:
        mgr_key = f"OKX:{env.upper()}"
        mgr = _depth_managers.get(mgr_key, _depth_managers["OKX:PAPER"])
        instruments = await mgr.get_instruments(instType)
        return [
            {
                "instId":   i["instId"],
                "baseCcy":  i.get("baseCcy", ""),
                "quoteCcy": i.get("quoteCcy", ""),
                "instType": i.get("instType", ""),
            }
            for i in instruments
        ]


# ── WebSocket endpoints ───────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    r = await get_redis()
    try:
        while True:
            strategies = await r.get("dashboard:strategies")
            prices = await r.get("dashboard:prices")
            heartbeat_keys = await r.keys("heartbeat:*")
            heartbeats = {}
            for key in heartbeat_keys:
                name = key.replace("heartbeat:", "")
                heartbeats[name] = await r.hgetall(key)

            await websocket.send_json({
                "strategies": json.loads(strategies) if strategies else {},
                "prices": json.loads(prices) if prices else {},
                "heartbeats": heartbeats,
                "timestamp": asyncio.get_event_loop().time(),
            })
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/depth")
async def depth_websocket(websocket: WebSocket):
    """
    Client sends {symbol, exchange, env} to select a feed.
    Server subscribes via the matching manager and streams live updates.
    """
    await websocket.accept()

    state = {"symbol": None, "exchange": "OKX", "env": "PAPER",
             "region": "US", "cache_key": None, "last_seq": -1}

    async def receive_messages():
        try:
            while True:
                msg = await websocket.receive_json()
                symbol   = msg.get("symbol",   state["symbol"])
                exchange = msg.get("exchange", state["exchange"]).upper()
                env      = msg.get("env",      state["env"]).upper()
                region   = msg.get("region",   state["region"]).upper()
                # Webull cache keys are always LIVE
                effective_env = "LIVE" if exchange == "WEBULL" else env
                new_key = f"{exchange}:{effective_env}:{symbol}" if symbol else None

                if new_key != state["cache_key"]:
                    state.update(symbol=symbol, exchange=exchange, env=env,
                                 region=region, cache_key=new_key, last_seq=-1)
                    if symbol:
                        if exchange == "WEBULL":
                            await _webull_manager.subscribe(symbol, region=region)
                        else:
                            mgr = _depth_managers.get(f"{exchange}:{env}")
                            if mgr:
                                await mgr.subscribe(symbol)
        except Exception:
            pass

    recv_task = asyncio.create_task(receive_messages())
    try:
        while True:
            key = state["cache_key"]
            if key and key in _orderbooks:
                seq = _orderbook_seq.get(key, 0)
                if seq != state["last_seq"]:
                    await websocket.send_json({
                        "symbol":   state["symbol"],
                        "exchange": state["exchange"],
                        "env":      state["env"],
                        **_orderbooks[key],
                    })
                    state["last_seq"] = seq
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        pass
    finally:
        recv_task.cancel()
