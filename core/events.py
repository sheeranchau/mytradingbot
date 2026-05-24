"""
Core event types for inter-process communication.
All events are serialized via msgpack over ZeroMQ.
"""
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
import time
import msgpack


class EventType(str, Enum):
    # Market data
    TICK = "tick"
    ORDERBOOK = "orderbook"
    KLINE = "kline"
    FUNDING_RATE = "funding_rate"

    # Trading
    SIGNAL = "signal"
    ORDER_NEW = "order_new"
    ORDER_UPDATE = "order_update"   # lifecycle change: open / partial / filled / cancelled
    ORDER_CANCEL = "order_cancel"
    ORDER_AMEND = "order_amend"
    FILL = "fill"                   # individual execution (fill_id populated)

    # Position & account
    POSITION = "position"
    ACCOUNT = "account"

    # Risk
    RISK_ALERT = "risk_alert"
    RISK_KILL = "risk_kill"

    # System
    HEARTBEAT = "heartbeat"
    STATUS = "status"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


@dataclass
class Event:
    event_type: str
    timestamp: float = field(default_factory=time.time)
    source: str = ""

    def pack(self) -> bytes:
        return msgpack.packb(asdict(self), use_bin_type=True)

    @classmethod
    def unpack(cls, data: bytes) -> dict:
        return msgpack.unpackb(data, raw=False)


@dataclass
class TickEvent(Event):
    event_type: str = EventType.TICK
    gateway: str = ""
    symbol: str = ""
    bid: float = 0.0
    ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    last: float = 0.0
    last_size: float = 0.0


@dataclass
class OrderBookEvent(Event):
    event_type: str = EventType.ORDERBOOK
    gateway: str = ""
    symbol: str = ""
    bids: list = field(default_factory=list)  # [[price, size], ...]
    asks: list = field(default_factory=list)


@dataclass
class KlineEvent(Event):
    event_type: str = EventType.KLINE
    gateway: str = ""
    symbol: str = ""
    interval: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


@dataclass
class SignalEvent(Event):
    event_type: str = EventType.SIGNAL
    strategy: str = ""
    gateway: str = ""
    symbol: str = ""
    side: str = ""
    order_type: str = OrderType.LIMIT
    price: float = 0.0
    quantity: float = 0.0
    reason: str = ""


@dataclass
class OrderEvent(Event):
    event_type: str = EventType.ORDER_NEW
    client_order_id: str = ""
    order_id: str = ""
    strategy: str = ""
    gateway: str = ""
    symbol: str = ""
    side: str = ""
    order_type: str = OrderType.LIMIT
    price: float = 0.0
    quantity: float = 0.0


@dataclass
class OrderUpdateEvent(Event):
    """Emitted whenever order state changes (open, partial, filled, cancelled…)."""
    event_type: str = EventType.ORDER_UPDATE
    client_order_id: str = ""
    order_id: str = ""
    strategy: str = ""
    gateway: str = ""
    symbol: str = ""
    state: str = ""             # OrderState constant
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    remaining_qty: float = 0.0
    update_time_ms: int = 0


@dataclass
class FillEvent(Event):
    """Emitted for each individual execution (fill_id is always populated)."""
    event_type: str = EventType.FILL
    # Identity
    client_order_id: str = ""
    order_id: str = ""
    fill_id: str = ""           # unique per execution
    strategy: str = ""
    gateway: str = ""
    symbol: str = ""
    side: str = ""
    # Fill details
    price: float = 0.0          # execution price of this fill
    quantity: float = 0.0       # quantity executed in this fill
    commission: float = 0.0     # fee (kept for backward compat; mirrors fill_fee)
    fill_fee: float = 0.0       # fee (negative = cost, positive = rebate)
    fill_fee_ccy: str = ""      # fee currency
    fill_pnl: float = 0.0       # realised PnL (reduce-only legs)
    fill_time_ms: int = 0       # fill timestamp ms
    # Running totals at time of this fill
    filled_qty: float = 0.0     # cumulative filled qty after this fill
    avg_fill_price: float = 0.0 # VWAP after this fill


@dataclass
class PositionEvent(Event):
    event_type: str = EventType.POSITION
    gateway: str = ""
    symbol: str = ""
    strategy: str = ""
    quantity: float = 0.0
    avg_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class RiskAlertEvent(Event):
    event_type: str = EventType.RISK_ALERT
    strategy: str = ""
    rule: str = ""
    message: str = ""
    level: str = "warning"  # warning, critical


@dataclass
class RiskKillEvent(Event):
    event_type: str = EventType.RISK_KILL
    strategy: str = ""  # empty means kill all
    reason: str = ""
    flatten: bool = False  # whether to flatten positions


@dataclass
class HeartbeatEvent(Event):
    event_type: str = EventType.HEARTBEAT
    process_name: str = ""
    pid: int = 0
    uptime: float = 0.0
