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


@app.get("/api/risk/pnl")
async def get_risk_pnl():
    """
    Return live PnL and delta snapshots written by the risk engine.

    Response shape
    ──────────────
    strategies   Per-strategy snapshots keyed by strategy name.
    aggregate    Sum of all strategies: total PnL + positions merged by symbol.
    delta_by_symbol  Only symbols with an open position; quick view for the UI.
    """
    r = await get_redis()
    keys = await r.keys("pnl:*")

    strategies: dict = {}
    for key in keys:
        raw = await r.get(key)
        if not raw:
            continue
        try:
            data     = json.loads(raw)
            name     = data.get("strategy") or (
                key.decode() if isinstance(key, bytes) else key
            ).replace("pnl:", "")
            strategies[name] = data
        except Exception:
            pass

    # Build aggregate across all strategies
    agg_positions: dict = {}
    total_realized   = 0.0
    total_unrealized = 0.0
    total_fees       = 0.0

    for s_data in strategies.values():
        total_realized   += s_data.get("realized_pnl", 0)
        total_unrealized += s_data.get("unrealized_pnl", 0)
        total_fees       += s_data.get("total_fees", 0)
        for pk, pos in s_data.get("positions", {}).items():
            if pk not in agg_positions:
                agg_positions[pk] = {
                    "symbol":       pos["symbol"],
                    "gateway":      pos["gateway"],
                    "net_qty":      0.0,
                    "avg_entry_price": pos.get("avg_entry_price", 0),
                    "realized_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "total_fees":   0.0,
                    "dollar_delta": 0.0,
                    "last_price":   pos.get("last_price", 0),
                }
            ap = agg_positions[pk]
            ap["net_qty"]        += pos.get("net_qty", 0)
            ap["realized_pnl"]   += pos.get("realized_pnl", 0)
            ap["unrealized_pnl"] += pos.get("unrealized_pnl", 0)
            ap["total_fees"]     += pos.get("total_fees", 0)
            ap["dollar_delta"]    = ap["net_qty"] * pos.get("last_price", 0)
            ap["last_price"]      = pos.get("last_price", ap["last_price"])

    # Flatten to per-symbol delta summary (open positions only)
    delta_by_symbol = {
        ap["symbol"]: {
            "gateway":     ap["gateway"],
            "net_qty":     round(ap["net_qty"], 8),
            "dollar_delta": round(ap["dollar_delta"], 2),
            "last_price":  ap["last_price"],
        }
        for ap in agg_positions.values()
        if ap["net_qty"] != 0
    }

    return {
        "strategies": strategies,
        "aggregate": {
            "realized_pnl":   round(total_realized, 8),
            "unrealized_pnl": round(total_unrealized, 8),
            "total_pnl":      round(total_realized + total_unrealized, 8),
            "total_fees":     round(total_fees, 8),
            "positions":      agg_positions,
        },
        "delta_by_symbol": delta_by_symbol,
    }


# ── Loan / Baseline management ────────────────────────────────────────────────

_STABLES = frozenset({"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD", "USD"})


class LoanEntry(BaseModel):
    ccy: str
    amount: float


class LoanRequest(BaseModel):
    entries: list[LoanEntry]
    note: str = ""


@app.get("/api/loan/{gateway}")
async def get_loan(gateway: str):
    """Return the saved loan (baseline) snapshot for a gateway."""
    r = await get_redis()
    raw  = await r.get(f"loan:{gateway}")
    meta = await r.get(f"loan:{gateway}:meta")
    return {
        "gateway":  gateway,
        "entries":  json.loads(raw)  if raw  else {},
        "meta":     json.loads(meta) if meta else {},
    }


@app.post("/api/loan/{gateway}")
async def set_loan(gateway: str, req: LoanRequest):
    """Save a loan (baseline) snapshot for a gateway."""
    r = await get_redis()
    entries = {e.ccy.upper(): e.amount for e in req.entries if e.amount != 0}
    meta    = {"set_at_ms": int(time.time() * 1000), "note": req.note}
    await r.set(f"loan:{gateway}", json.dumps(entries))
    await r.set(f"loan:{gateway}:meta", json.dumps(meta))
    return {"status": "ok", "gateway": gateway, "entries": entries}


async def _get_px(ccy: str, r) -> float:
    """Look up a currency's USD price from the risk engine's price cache."""
    ccy = ccy.upper()
    if ccy in _STABLES:
        return 1.0
    raw = await r.get(f"px:{ccy}")
    return float(raw) if raw else 0.0


@app.get("/api/risk/exchange_pnl")
async def get_exchange_pnl(gateway: str = "okx"):
    """
    Exchange-based PnL and delta.

    Formula per currency C
    ──────────────────────
      cash_pnl_C   = (cashBal_C  − loan_C) × price_C_USD
      settle_upnl_C = upl_C  (unrealized PnL for instruments settling in C)
      pnl_C        = cash_pnl_C + settle_upnl_C

    total_exchange_pnl = Σ_C pnl_C

    Delta per currency C
    ─────────────────────
      delta_C = (cashBal_C + Σ open_instrument_qty_in_C) × price_C_USD
              ≈ eqUsd_C   (OKX already computes equity × price = eqUsd)

    Reconciliation
    ──────────────
      trade_pnl   from risk engine fill tracking
      exchange_pnl from this calculation
      diff        = trade_pnl − exchange_pnl  (should be near 0)

    Notes
    ─────
    - For OKX: eqUsd per currency = (cashBal + settled derivatives) × OKX spot price.
      We use eqUsd_C / cashBal_C as the implied price, avoiding a separate price feed.
    - For Webull/FUTU: balances are already in USD; price = 1.0 per USD unit.
    - Prices for the loan valuation come from the risk engine's px:{ccy} cache
      (populated from live tick data).  Missing prices show as 0 — fill them in
      via the loan page or wait for ticks to arrive.
    """
    r = await get_redis()

    # ── Load loan baseline ────────────────────────────────────────────────
    loan_raw  = await r.get(f"loan:{gateway}")
    loan_meta = await r.get(f"loan:{gateway}:meta")
    loan: dict[str, float] = json.loads(loan_raw)  if loan_raw  else {}
    meta: dict             = json.loads(loan_meta) if loan_meta else {}

    # ── Fetch trading account from Redis (written by gateway) ─────────────
    acct_keys = await r.hgetall("accounts")
    acct_id   = None
    for aid, raw_v in acct_keys.items():
        info = json.loads(raw_v)
        if info.get("gateway", "").lower() == gateway.lower():
            acct_id = aid.decode() if isinstance(aid, bytes) else aid
            break

    if not acct_id:
        return {"error": f"No account found for gateway '{gateway}'", "loan": loan}

    trading_raw = await r.get(f"account:{acct_id}:trading")
    trading     = json.loads(trading_raw) if trading_raw else {}
    summary     = trading.get("summary", {})
    details     = trading.get("details", [])    # list of per-ccy dicts

    total_eq_usd = float(summary.get("totalEq", 0) or 0)

    # ── Derivative positions (for delta calculation) ──────────────────────
    # Each position contributes directional delta to its base currency.
    # Example: BTC-USDT-SWAP pos=0.01 notionalUsd=7.67 → +7.67 to BTC delta
    #          short pos=-0.5 → negative delta
    positions_raw = await r.get(f"account:{acct_id}:positions")
    positions     = json.loads(positions_raw) if positions_raw else []

    # Aggregate derivative notional per base currency
    deriv_delta: dict[str, float] = {}   # ccy → signed USD notional
    for pos in positions:
        inst_id = pos.get("instId", "")
        base    = inst_id.split("-")[0].upper() if "-" in inst_id else ""
        if not base:
            continue
        pos_qty   = float(pos.get("pos", 0) or 0)
        notional  = float(pos.get("notionalUsd", 0) or 0)
        signed_n  = notional if pos_qty >= 0 else -notional
        deriv_delta[base] = deriv_delta.get(base, 0.0) + signed_n

    # ── Per-currency calculation ──────────────────────────────────────────
    per_ccy: dict = {}
    loan_total_usd     = 0.0
    exchange_delta_usd = 0.0

    for d in details:
        ccy       = d.get("ccy", "").upper()
        cash_bal  = float(d.get("cashBal", 0) or 0)
        eq        = float(d.get("eq",      cash_bal) or cash_bal)
        eq_usd    = float(d.get("eqUsd",   0) or 0)
        upl       = float(d.get("upl",     0) or 0)

        # Implied price from OKX's own conversion (most accurate for OKX)
        implied_px = (eq_usd / eq) if eq != 0 else await _get_px(ccy, r)
        if implied_px == 0:
            implied_px = await _get_px(ccy, r)

        loan_ccy     = loan.get(ccy, 0.0)
        loan_usd_val = loan_ccy * implied_px
        pnl_usd      = eq_usd - loan_usd_val

        loan_total_usd += loan_usd_val

        # Delta: stablecoins have zero market delta (no directional exposure).
        # Non-stable cash delta = eqUsd (you're long that crypto).
        # Derivative delta from positions is added on top.
        is_stable   = ccy in _STABLES
        cash_delta  = 0.0 if is_stable else eq_usd
        deriv_d     = deriv_delta.pop(ccy, 0.0)
        total_delta = cash_delta + deriv_d

        exchange_delta_usd += total_delta

        per_ccy[ccy] = {
            "ccy":          ccy,
            "cash_bal":     cash_bal,
            "eq":           eq,
            "eq_usd":       round(eq_usd, 4),
            "upl":          round(upl, 4),
            "price_usd":    round(implied_px, 6),
            "loan":         loan_ccy,
            "loan_usd":     round(loan_usd_val, 4),
            "pnl_usd":      round(pnl_usd, 4),
            "cash_delta":   round(cash_delta, 4),
            "deriv_delta":  round(deriv_d, 4),
            "delta_usd":    round(total_delta, 4),
        }

    # Derivative delta for currencies with no cash balance (shouldn't happen
    # normally, but included for completeness)
    for ccy, dd in deriv_delta.items():
        px = await _get_px(ccy, r)
        per_ccy[ccy] = {
            "ccy": ccy, "cash_bal": 0, "eq": 0, "eq_usd": 0, "upl": 0,
            "price_usd": round(px, 6), "loan": loan.get(ccy, 0),
            "loan_usd": round(loan.get(ccy, 0) * px, 4),
            "pnl_usd": round(-loan.get(ccy, 0) * px, 4),
            "cash_delta": 0, "deriv_delta": round(dd, 4),
            "delta_usd": round(dd, 4),
        }
        exchange_delta_usd += dd

    exchange_pnl = total_eq_usd - loan_total_usd

    # ── Reconciliation with trade-based PnL ──────────────────────────────
    trade_pnl = 0.0
    pnl_keys  = await r.keys("pnl:*")
    for pk in pnl_keys:
        pnl_raw = await r.get(pk)
        if pnl_raw:
            try:
                trade_pnl += json.loads(pnl_raw).get("total_pnl", 0)
            except Exception:
                pass

    return {
        "gateway":          gateway,
        "total_eq_usd":     round(total_eq_usd, 4),
        "loan_total_usd":   round(loan_total_usd, 4),
        "exchange_pnl":     round(exchange_pnl, 4),
        "exchange_delta":   round(exchange_delta_usd, 4),
        "trade_pnl":        round(trade_pnl, 8),
        "reconciliation":   round(trade_pnl - exchange_pnl, 4),
        "per_ccy":          per_ccy,
        "loan_meta":        meta,
        "has_loan":         bool(loan),
    }


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
    client_order_id: str = ""   # preferred over order_id on exchanges that support it


class AmendOrderRequest(BaseModel):
    gateway: str
    symbol: str
    order_id: str
    client_order_id: str = ""
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
    # request_id links the signal to its response and doubles as client_order_id
    # for rejected orders written to finished_orders by the risk engine.
    request_id = secrets.token_hex(8)
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
        "request_id": request_id,
    }
    pusher = _get_signal_pusher()
    await pusher.push(signal)

    # Wait up to 500 ms for the risk engine to write its verdict.
    # 10 × 50 ms is well within a single ZMQ round-trip on localhost.
    r = await get_redis()
    response_key = f"order_response:{request_id}"
    result = None
    for _ in range(10):
        await asyncio.sleep(0.05)
        raw = await r.get(response_key)
        if raw:
            result = json.loads(raw)
            await r.delete(response_key)
            break

    if result is None:
        # Risk engine didn't respond in time — treat as pending (rare).
        logger.warning("Risk engine did not respond to request_id %s within 500ms", request_id)
        await r.xadd("manual_orders", {
            "gateway": req.gateway, "symbol": req.symbol, "side": req.side,
            "order_type": req.order_type, "price": str(req.price),
            "quantity": str(req.quantity), "timestamp": str(time.time()),
            "action": "new", "status": "pending",
        }, maxlen=500)
        return {"status": "pending", "message": f"{req.side} {req.quantity} {req.symbol} sent (unconfirmed — risk engine timeout)"}

    if not result["approved"]:
        reason = result.get("reason", "Rejected by risk engine")
        return JSONResponse(
            status_code=422,
            content={"status": "rejected", "message": reason},
        )

    await r.xadd("manual_orders", {
        "gateway": req.gateway, "symbol": req.symbol, "side": req.side,
        "order_type": req.order_type, "price": str(req.price),
        "quantity": str(req.quantity), "timestamp": str(time.time()),
        "action": "new", "status": "approved",
    }, maxlen=500)
    return {"status": "ok", "message": f"{req.side} {req.quantity} {req.symbol} order placed"}


@app.post("/api/order/cancel")
async def cancel_order(req: CancelOrderRequest):
    event = {
        "event_type": "order_cancel",
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "client_order_id": req.client_order_id,
        "timestamp": time.time(),
    }
    pusher = _get_signal_pusher()
    await pusher.push(event)
    r = await get_redis()
    await r.xadd("manual_orders", {
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "client_order_id": req.client_order_id,
        "timestamp": str(time.time()),
        "action": "cancel",
    }, maxlen=500)
    label = req.client_order_id or req.order_id
    return {"status": "ok", "message": f"Cancel {label} sent to {req.gateway}"}


@app.post("/api/order/amend")
async def amend_order(req: AmendOrderRequest):
    event = {
        "event_type": "order_amend",
        "gateway": req.gateway,
        "symbol": req.symbol,
        "order_id": req.order_id,
        "client_order_id": req.client_order_id,
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
        "client_order_id": req.client_order_id,
        "price": str(req.price),
        "quantity": str(req.quantity),
        "timestamp": str(time.time()),
        "action": "amend",
    }, maxlen=500)
    label = req.client_order_id or req.order_id
    return {"status": "ok", "message": f"Amend {label} sent to {req.gateway}"}


@app.get("/api/order/updates")
async def get_order_updates(count: int = 200, gateway: str = "okx"):
    """Full audit log — every state change (pending_submit→open→partial→filled/cancelled)."""
    r = await get_redis()
    entries = await r.xrange(f"order_updates:{gateway}", "-", "+", count=count)
    return [{"_stream_id": e[0], **e[1]} for e in entries]


@app.get("/api/order/history")
async def get_order_history(count: int = 50, gateway: str = "okx"):
    """Terminal orders, one entry per order (deduped by client_order_id / order_id).

    finished_orders may contain duplicate entries for the same order if a REST
    sync and a WS push both arrived before the SET-NX lock was in place.
    We deduplicate here at read time, keeping the entry with the latest
    update_time_ms so the UI always shows the most complete snapshot.
    """
    r = await get_redis()
    # Fetch more than `count` so dedup doesn't shrink the result below what the
    # caller asked for (fetch 4× then trim to count after dedup).
    raw = await r.xrange("finished_orders", "-", "+", count=count * 4)

    # Deduplicate: key = client_order_id if set, else order_id.
    # Stream entries arrive oldest-first; iterating forward means later entries
    # (higher update_time_ms) naturally overwrite earlier ones in the dict.
    seen: dict[str, dict] = {}
    for stream_id, fields in raw:
        entry = {"_stream_id": stream_id, **fields}
        # order_id (exchange-assigned) is the canonical dedup key.
        # client_order_id is only a fallback for rejected orders that never
        # received an exchange ID.  Old entries written before client_order_id
        # was added to the schema will only have order_id, so checking
        # order_id first ensures they still deduplicate correctly.
        key = fields.get("order_id") or fields.get("client_order_id") or stream_id
        existing = seen.get(key)
        if existing is None:
            seen[key] = entry
        else:
            # Keep whichever has the later update_time_ms (more recent state).
            # Fall back to stream_id comparison (later stream_id = later write)
            # for old entries that predate the update_time_ms field.
            new_ts = int(fields.get("update_time_ms") or 0)
            old_ts = int(existing.get("update_time_ms") or 0)
            if new_ts > old_ts or (new_ts == old_ts and stream_id > existing["_stream_id"]):
                seen[key] = entry

    # Return newest-first, capped at count
    deduped = list(seen.values())
    deduped.sort(key=lambda e: int(e.get("update_time_ms", 0) or 0), reverse=True)
    return deduped[:count]


@app.get("/api/order/fills")
async def get_order_fills(count: int = 200, gateway: str = "okx"):
    """Individual fill executions, one entry per fill_id (deduped).

    Deduplicates by fill_id for the same reason as /api/order/history.
    """
    r = await get_redis()
    raw = await r.xrange(f"fills:{gateway}", "-", "+", count=count * 4)

    seen: dict[str, dict] = {}
    for stream_id, fields in raw:
        entry = {"_stream_id": stream_id, **fields}
        key = fields.get("fill_id") or stream_id
        existing = seen.get(key)
        if existing is None:
            seen[key] = entry
        else:
            if int(fields.get("fill_time_ms", 0)) >= int(existing.get("fill_time_ms", 0)):
                seen[key] = entry

    deduped = list(seen.values())
    deduped.sort(key=lambda e: int(e.get("fill_time_ms", 0) or 0), reverse=True)
    return deduped[:count]


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
