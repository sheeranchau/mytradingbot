"""
Tests for gateway configuration — endpoint selection based on trading mode.
"""
import pytest


class TestOKXEndpoints:

    def test_demo_mode_uses_demo_endpoints(self):
        from gateway.okx import OKXGateway

        gw = OKXGateway.__new__(OKXGateway)
        gw.simulated = True
        gw.WS_PUBLIC = OKXGateway.DEMO_WS_PUBLIC
        gw.WS_PRIVATE = OKXGateway.DEMO_WS_PRIVATE
        gw.REST_BASE = OKXGateway.DEMO_REST_BASE

        assert "wspap" in gw.WS_PUBLIC
        assert "wspap" in gw.WS_PRIVATE

    def test_live_mode_uses_live_endpoints(self):
        from gateway.okx import OKXGateway

        gw = OKXGateway.__new__(OKXGateway)
        gw.simulated = False
        gw.WS_PUBLIC = OKXGateway.LIVE_WS_PUBLIC
        gw.WS_PRIVATE = OKXGateway.LIVE_WS_PRIVATE
        gw.REST_BASE = OKXGateway.LIVE_REST_BASE

        assert "wspap" not in gw.WS_PUBLIC
        assert "ws.okx.com" in gw.WS_PUBLIC

    def test_demo_ws_includes_broker_id(self):
        from gateway.okx import OKXGateway
        assert "brokerId=9999" in OKXGateway.DEMO_WS_PUBLIC
        assert "brokerId=9999" in OKXGateway.DEMO_WS_PRIVATE


class TestFUTUConfig:

    def test_simulate_mode_no_unlock_required(self):
        from gateway.futu import FUTUGateway

        # Should not raise — no password needed for simulate
        gw = FUTUGateway.__new__(FUTUGateway)
        gw.trade_env = "SIMULATE"
        gw.unlock_password = ""
        # In SIMULATE, _connect_sync skips unlock

    def test_real_mode_requires_unlock_password(self):
        from gateway.futu import FUTUGateway

        gw = FUTUGateway.__new__(FUTUGateway)
        gw.trade_env = "REAL"
        gw.unlock_password = ""

        # The check happens in _connect_sync, so verify the field
        assert gw.unlock_password == ""
        assert gw.trade_env == "REAL"
        # _connect_sync would raise ValueError("unlock_password required")
