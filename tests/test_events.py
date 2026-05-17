"""
Tests for event serialization/deserialization.
Events are the wire format between all processes — correctness here is critical.
"""
import time
import pytest
from core.events import (
    Event, TickEvent, OrderBookEvent, KlineEvent,
    SignalEvent, OrderEvent, FillEvent, PositionEvent,
    RiskKillEvent, HeartbeatEvent,
    EventType, Side, OrderType,
)


class TestEventSerialization:
    """Events must survive pack/unpack round-trip over ZMQ."""

    def test_tick_event_round_trip(self):
        tick = TickEvent(
            gateway="okx", symbol="BTC-USDT-SWAP",
            bid=50000.0, ask=50001.0,
            bid_size=1.5, ask_size=2.0,
            last=50000.5, last_size=0.1,
        )
        packed = tick.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["event_type"] == EventType.TICK
        assert unpacked["gateway"] == "okx"
        assert unpacked["symbol"] == "BTC-USDT-SWAP"
        assert unpacked["bid"] == 50000.0
        assert unpacked["ask"] == 50001.0
        assert unpacked["last"] == 50000.5

    def test_orderbook_event_round_trip(self):
        book = OrderBookEvent(
            gateway="okx", symbol="ETH-USDT-SWAP",
            bids=[[3000.0, 10.0], [2999.0, 20.0]],
            asks=[[3001.0, 5.0], [3002.0, 15.0]],
        )
        packed = book.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["bids"] == [[3000.0, 10.0], [2999.0, 20.0]]
        assert unpacked["asks"] == [[3001.0, 5.0], [3002.0, 15.0]]

    def test_signal_event_round_trip(self):
        signal = SignalEvent(
            strategy="momentum", gateway="okx", symbol="BTC-USDT-SWAP",
            side=Side.BUY, order_type=OrderType.LIMIT,
            price=50000.0, quantity=0.01,
            reason="momentum > threshold",
        )
        packed = signal.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["strategy"] == "momentum"
        assert unpacked["side"] == "buy"
        assert unpacked["price"] == 50000.0
        assert unpacked["reason"] == "momentum > threshold"

    def test_fill_event_round_trip(self):
        fill = FillEvent(
            order_id="abc123", strategy="momentum",
            gateway="okx", symbol="BTC-USDT-SWAP",
            side="buy", price=50000.0, quantity=0.01,
            commission=-0.005,
        )
        packed = fill.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["order_id"] == "abc123"
        assert unpacked["commission"] == -0.005

    def test_risk_kill_event_round_trip(self):
        kill = RiskKillEvent(
            strategy="momentum",
            reason="Max daily loss breached",
            flatten=True,
        )
        packed = kill.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["strategy"] == "momentum"
        assert unpacked["flatten"] is True
        assert unpacked["reason"] == "Max daily loss breached"

    def test_heartbeat_event_round_trip(self):
        hb = HeartbeatEvent(
            process_name="gateway_okx", pid=12345, uptime=3600.0,
        )
        packed = hb.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["process_name"] == "gateway_okx"
        assert unpacked["pid"] == 12345

    def test_event_timestamp_auto_set(self):
        before = time.time()
        event = TickEvent(gateway="okx", symbol="BTC-USDT-SWAP")
        after = time.time()

        assert before <= event.timestamp <= after

    def test_kline_event_round_trip(self):
        kline = KlineEvent(
            gateway="okx", symbol="BTC-USDT-SWAP", interval="1m",
            open=50000.0, high=50100.0, low=49900.0,
            close=50050.0, volume=100.0,
        )
        packed = kline.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["interval"] == "1m"
        assert unpacked["high"] == 50100.0
        assert unpacked["volume"] == 100.0

    def test_empty_event_defaults(self):
        tick = TickEvent()
        packed = tick.pack()
        unpacked = Event.unpack(packed)

        assert unpacked["gateway"] == ""
        assert unpacked["bid"] == 0.0
        assert unpacked["event_type"] == EventType.TICK
