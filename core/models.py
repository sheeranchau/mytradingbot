"""
Unified data models shared across all components.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Order:
    order_id: str = ""
    client_order_id: str = ""
    strategy: str = ""
    gateway: str = ""
    symbol: str = ""
    side: str = ""
    order_type: str = "limit"
    price: float = 0.0
    quantity: float = 0.0
    filled_quantity: float = 0.0
    status: str = "pending"  # pending, submitted, partial, filled, cancelled, rejected
    create_time: float = 0.0
    update_time: float = 0.0


@dataclass
class Position:
    gateway: str = ""
    symbol: str = ""
    strategy: str = ""
    side: str = ""  # long, short, flat
    quantity: float = 0.0
    avg_price: float = 0.0
    market_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class Account:
    gateway: str = ""
    balance: float = 0.0
    available: float = 0.0
    margin_used: float = 0.0
    unrealized_pnl: float = 0.0


@dataclass
class RiskLimits:
    """Per-strategy risk configuration."""
    max_position_size: float = 0.0
    max_notional: float = 0.0
    max_daily_loss: float = 0.0
    max_daily_loss_pct: float = 0.0
    max_delta: float = 0.0
    max_order_rate: int = 100  # orders per minute
    max_drawdown_pct: float = 0.0
