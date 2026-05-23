"""
Web dashboard API server.
Serves REST endpoints and WebSocket for live updates.
"""
import asyncio
import json
import logging
import os
import secrets
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import paho.mqtt.client as mqtt
import pyotp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.middleware.gzip import GZipMiddleware
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
import redis.asyncio as aioredis

from core.config import load_env
from core.logger import get_logger
from core.zmq_channels import MultiSubscriber, Pusher, GATEWAY_MARKET_DATA_PORTS, SIGNAL_PORT

load_env()
logger = get_logger("view_web")

STATIC_DIR = Path(__file__).parent / "static"
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

# ── Authentication ────────────────────────────────────────────────────────────

AUTH_USERNAME = os.environ.get("DASHBOARD_USER", "joshzhou")
INITIAL_PASSWORD = os.environ.get("DASHBOARD_INITIAL_PASSWORD", "changeme123")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
COOKIE_NAME = "session_token"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
_jwt_secret: str = ""


async def _ensure_auth_ready():
    """Initialize JWT secret and seed initial password on first run."""
    global _jwt_secret
    r = await get_redis()

    _jwt_secret = os.environ.get("JWT_SECRET", "")
    if not _jwt_secret:
        stored = await r.get("auth:jwt_secret")
        if stored:
            _jwt_secret = stored
        else:
            _jwt_secret = secrets.token_hex(64)
            await r.set("auth:jwt_secret", _jwt_secret)

    existing = await r.get(f"auth:user:{AUTH_USERNAME}:password_hash")
    if not existing:
        hashed = pwd_context.hash(INITIAL_PASSWORD)
        pipe = r.pipeline()
        pipe.set(f"auth:user:{AUTH_USERNAME}:password_hash", hashed)
        pipe.set(f"auth:user:{AUTH_USERNAME}:totp_enabled", "0")
        await pipe.execute()
        logger.info("Seeded initial password for user '%s'", AUTH_USERNAME)


def _create_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, _jwt_secret, algorithm=JWT_ALGORITHM)


def _verify_token(token: str) -> str:
    payload = jwt.decode(token, _jwt_secret, algorithms=[JWT_ALGORITHM])
    username = payload.get("sub")
    if username != AUTH_USERNAME:
        raise JWTError("wrong user")
    return username


async def _get_current_user(request: Request) -> str:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        return _verify_token(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Session expired")


async def _authenticate_ws(websocket: WebSocket) -> bool:
    """Accept the WebSocket, then verify auth. Close with 4001 if invalid."""
    token = websocket.cookies.get(COOKIE_NAME)
    await websocket.accept()
    if not token:
        await websocket.close(code=4001, reason="Not authenticated")
        return False
    try:
        _verify_token(token)
        return True
    except JWTError:
        await websocket.close(code=4001, reason="Session expired")
        return False


_PUBLIC_PATHS = {"/login", "/auth/login"}


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/static/") or path in _PUBLIC_PATHS:
            return await call_next(request)
        if path.startswith("/ws"):
            return await call_next(request)

        token = request.cookies.get(COOKIE_NAME)
        if not token:
            if path.startswith("/api/") or path.startswith("/auth/"):
                return JSONResponse({"detail": "Not authenticated"}, status_code=401)
            return RedirectResponse("/login", status_code=302)

        try:
            _verify_token(token)
        except JWTError:
            if path.startswith("/api/") or path.startswith("/auth/"):
                return JSONResponse({"detail": "Session expired"}, status_code=401)
            resp = RedirectResponse("/login", status_code=302)
            resp.delete_cookie(COOKIE_NAME)
            return resp

        return await call_next(request)


app = FastAPI(title="Trading Dashboard")
app.add_middleware(AuthMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=500)
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
        self._instruments: dict = {}
        self._instruments_ts: dict = {}
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

    async def _resubscribe(self, symbol: str):
        """Force resubscribe to get a fresh snapshot."""
        if self._ws and not self._ws.closed:
            await self._ws.send_json({
                "op": "unsubscribe",
                "args": [{"channel": "books", "instId": symbol}],
            })
            await asyncio.sleep(0.1)
            await self._ws.send_json({
                "op": "subscribe",
                "args": [{"channel": "books", "instId": symbol}],
            })

    async def get_instruments(self, inst_type: str = "SWAP") -> list:
        now = time.time()
        if inst_type in self._instruments and now - self._instruments_ts.get(inst_type, 0) < self.INSTRUMENTS_TTL:
            return self._instruments[inst_type]

        url = f"{self.OKX_REST}/api/v5/public/instruments?instType={inst_type}"
        headers = {"x-simulated-trading": "1"} if self.env == "PAPER" else {}
        try:
            async with self._session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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

    @staticmethod
    def _compute_checksum(bids: dict, asks: dict) -> int:
        """OKX checksum: CRC32 of top-25 bids/asks as price:size pairs."""
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
        # OKX uses signed 32-bit
        if crc >= 0x80000000:
            crc -= 0x100000000
        return crc

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
                    logger.warning("OKX checksum mismatch for %s [%s]: expected=%s got=%s, resubscribing",
                                   symbol, self.env, expected_crc, actual_crc)
                    self._local_books.pop(symbol, None)
                    asyncio.create_task(self._resubscribe(symbol))
                    return

        top_bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:25]
        top_asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:25]
        key = self._key(symbol)
        _orderbooks[key] = {
            "bids": [[float(p), float(s)] for p, s in top_bids],
            "asks": [[float(p), float(s)] for p, s in top_asks],
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
    await _ensure_auth_ready()
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


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.get("/login")
async def login_page(request: Request):
    token = request.cookies.get(COOKIE_NAME)
    if token:
        try:
            _verify_token(token)
            return RedirectResponse("/", status_code=302)
        except JWTError:
            pass
    return FileResponse(str(STATIC_DIR / "login.html"))


class LoginRequest(BaseModel):
    password: str
    totp_code: str = ""


@app.post("/auth/login")
async def auth_login(req: LoginRequest):
    r = await get_redis()
    stored_hash = await r.get(f"auth:user:{AUTH_USERNAME}:password_hash")
    if not stored_hash or not pwd_context.verify(req.password, stored_hash):
        raise HTTPException(401, "Invalid password")

    totp_enabled = await r.get(f"auth:user:{AUTH_USERNAME}:totp_enabled")
    if totp_enabled == "1":
        if not req.totp_code:
            return JSONResponse({"requires_2fa": True})
        secret = await r.get(f"auth:user:{AUTH_USERNAME}:totp_secret")
        totp = pyotp.TOTP(secret)
        if not totp.verify(req.totp_code, valid_window=1):
            raise HTTPException(401, "Invalid 2FA code")

    token = _create_token(AUTH_USERNAME)
    response = JSONResponse({"status": "ok"})
    response.set_cookie(
        COOKIE_NAME, token,
        httponly=True, samesite="strict",
        max_age=JWT_EXPIRY_HOURS * 3600,
    )
    return response


@app.post("/auth/logout")
async def auth_logout():
    response = JSONResponse({"status": "ok"})
    response.delete_cookie(COOKIE_NAME)
    return response


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


@app.post("/auth/change-password")
async def change_password(req: ChangePasswordRequest, user: str = Depends(_get_current_user)):
    r = await get_redis()
    stored_hash = await r.get(f"auth:user:{AUTH_USERNAME}:password_hash")
    if not pwd_context.verify(req.current_password, stored_hash):
        raise HTTPException(401, "Current password is incorrect")
    if len(req.new_password) < 8:
        raise HTTPException(400, "New password must be at least 8 characters")
    new_hash = pwd_context.hash(req.new_password)
    await r.set(f"auth:user:{AUTH_USERNAME}:password_hash", new_hash)
    return {"status": "ok", "message": "Password changed"}


@app.get("/auth/status")
async def auth_status(user: str = Depends(_get_current_user)):
    r = await get_redis()
    totp_enabled = await r.get(f"auth:user:{AUTH_USERNAME}:totp_enabled")
    return {"username": AUTH_USERNAME, "totp_enabled": totp_enabled == "1"}


@app.post("/auth/2fa/setup")
async def setup_2fa(user: str = Depends(_get_current_user)):
    r = await get_redis()
    totp_enabled = await r.get(f"auth:user:{AUTH_USERNAME}:totp_enabled")
    if totp_enabled == "1":
        raise HTTPException(400, "2FA is already enabled. Disable it first.")
    secret = pyotp.random_base32()
    await r.set(f"auth:user:{AUTH_USERNAME}:totp_secret", secret)
    totp = pyotp.TOTP(secret)
    uri = totp.provisioning_uri(name=AUTH_USERNAME, issuer_name="TradingDashboard")
    return {"secret": secret, "uri": uri}


class Verify2FARequest(BaseModel):
    code: str


@app.post("/auth/2fa/verify")
async def verify_2fa(req: Verify2FARequest, user: str = Depends(_get_current_user)):
    r = await get_redis()
    secret = await r.get(f"auth:user:{AUTH_USERNAME}:totp_secret")
    if not secret:
        raise HTTPException(400, "Run 2FA setup first")
    totp = pyotp.TOTP(secret)
    if not totp.verify(req.code, valid_window=1):
        raise HTTPException(400, "Invalid code. Try again.")
    await r.set(f"auth:user:{AUTH_USERNAME}:totp_enabled", "1")
    return {"status": "ok", "message": "2FA enabled"}


class Disable2FARequest(BaseModel):
    password: str


@app.post("/auth/2fa/disable")
async def disable_2fa(req: Disable2FARequest, user: str = Depends(_get_current_user)):
    r = await get_redis()
    stored_hash = await r.get(f"auth:user:{AUTH_USERNAME}:password_hash")
    if not pwd_context.verify(req.password, stored_hash):
        raise HTTPException(401, "Invalid password")
    await r.set(f"auth:user:{AUTH_USERNAME}:totp_enabled", "0")
    await r.delete(f"auth:user:{AUTH_USERNAME}:totp_secret")
    return {"status": "ok", "message": "2FA disabled"}


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


class ManualOrderRequest(BaseModel):
    gateway: str
    symbol: str
    side: str
    order_type: str = "limit"
    price: float = 0.0
    quantity: float = 1.0


class CancelOrderRequest(BaseModel):
    gateway: str
    symbol: str
    order_id: str


class AmendOrderRequest(BaseModel):
    gateway: str
    symbol: str
    order_id: str
    price: float = 0.0
    quantity: float = 0.0


_signal_pusher: Optional[Pusher] = None


def _get_signal_pusher() -> Pusher:
    global _signal_pusher
    if _signal_pusher is None:
        _signal_pusher = Pusher(SIGNAL_PORT, bind=False)
    return _signal_pusher


@app.post("/api/order")
async def place_manual_order(req: ManualOrderRequest):
    signal = {
        "event_type": "signal",
        "strategy": "_manual_",
        "gateway": req.gateway,
        "symbol": req.symbol,
        "side": req.side,
        "order_type": req.order_type,
        "price": req.price,
        "quantity": req.quantity,
        "reason": "manual order via dashboard",
        "timestamp": time.time(),
    }
    pusher = _get_signal_pusher()
    await pusher.push(signal)
    r = await get_redis()
    await r.xadd("manual_orders", {
        "gateway": req.gateway,
        "symbol": req.symbol,
        "side": req.side,
        "order_type": req.order_type,
        "price": str(req.price),
        "quantity": str(req.quantity),
        "timestamp": str(time.time()),
        "action": "new",
    }, maxlen=500)
    return {"status": "ok", "message": f"{req.side} {req.quantity} {req.symbol} sent to risk engine"}


@app.post("/api/order/cancel")
async def cancel_order(req: CancelOrderRequest):
    event = {
        "event_type": "order_cancel",
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "timestamp": time.time(),
    }
    pusher = _get_signal_pusher()
    await pusher.push(event)
    r = await get_redis()
    await r.xadd("manual_orders", {
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "timestamp": str(time.time()),
        "action": "cancel",
    }, maxlen=500)
    return {"status": "ok", "message": f"Cancel {req.order_id} sent to {req.gateway}"}


@app.post("/api/order/amend")
async def amend_order(req: AmendOrderRequest):
    event = {
        "event_type": "order_amend",
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "price": req.price,
        "quantity": req.quantity,
        "timestamp": time.time(),
    }
    pusher = _get_signal_pusher()
    await pusher.push(event)
    r = await get_redis()
    await r.xadd("manual_orders", {
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "price": str(req.price),
        "quantity": str(req.quantity),
        "timestamp": str(time.time()),
        "action": "amend",
    }, maxlen=500)
    return {"status": "ok", "message": f"Amend {req.order_id} sent to {req.gateway}"}


@app.get("/api/order/history")
async def get_order_history(count: int = 50):
    r = await get_redis()
    orders = await r.xrange("finished_orders", "-", "+", count=count)
    return [{"id": o[0], **o[1]} for o in orders]


@app.get("/api/order/pending")
async def get_pending_orders(gateway: str = "okx"):
    r = await get_redis()
    raw = await r.get(f"pending_orders:{gateway}")
    return json.loads(raw) if raw else []


@app.get("/api/accounts")
async def list_accounts():
    r = await get_redis()
    raw = await r.hgetall("accounts")
    return [json.loads(v) for v in raw.values()]


@app.get("/api/account/{acct_id}")
async def get_account_info(acct_id: str):
    r = await get_redis()
    config_raw = await r.get(f"account:{acct_id}:config")
    trading_raw = await r.get(f"account:{acct_id}:trading")
    funding_raw = await r.get(f"account:{acct_id}:funding")
    earning_raw = await r.get(f"account:{acct_id}:earning")
    positions_raw = await r.get(f"account:{acct_id}:positions")
    return {
        "config": json.loads(config_raw) if config_raw else None,
        "trading": json.loads(trading_raw) if trading_raw else None,
        "funding": json.loads(funding_raw) if funding_raw else [],
        "earning": json.loads(earning_raw) if earning_raw else [],
        "positions": json.loads(positions_raw) if positions_raw else [],
    }


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
    if not await _authenticate_ws(websocket):
        return
    # _authenticate_ws already accepted if valid
    r = await get_redis()
    try:
        while True:
            pipe = r.pipeline()
            pipe.get("dashboard:strategies")
            pipe.get("dashboard:prices")
            pipe.keys("heartbeat:*")
            strategies_raw, prices_raw, heartbeat_keys = await pipe.execute()

            heartbeats = {}
            if heartbeat_keys:
                pipe2 = r.pipeline()
                for key in heartbeat_keys:
                    pipe2.hgetall(key)
                    pipe2.ttl(key)
                results = await pipe2.execute()
                for i, key in enumerate(heartbeat_keys):
                    name = key.replace("heartbeat:", "")
                    hb = results[i * 2]
                    hb["ttl"] = results[i * 2 + 1]
                    heartbeats[name] = hb

            await websocket.send_json({
                "strategies": json.loads(strategies_raw) if strategies_raw else {},
                "prices": json.loads(prices_raw) if prices_raw else {},
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
    if not await _authenticate_ws(websocket):
        return

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
