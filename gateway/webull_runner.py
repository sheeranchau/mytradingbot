"""Entry point for Webull gateway process."""
import asyncio
import os

from core.config import load_settings
from gateway.webull import WebullGateway


def main():
    config = load_settings()
    wb_cfg = config["gateways"]["webull"]

    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    gateway = WebullGateway(
        email=os.environ.get("WEBULL_EMAIL", wb_cfg.get("email", "")),
        password=os.environ.get("WEBULL_PASSWORD", wb_cfg.get("password", "")),
        trade_pin=os.environ.get("WEBULL_TRADE_PIN", wb_cfg.get("trade_pin", "")),
        mfa_seed=os.environ.get("WEBULL_MFA_SEED", wb_cfg.get("mfa_seed", "")),
        device_id=os.environ.get("WEBULL_DID", wb_cfg.get("device_id", "")),
        symbols=wb_cfg["symbols"],
        region=wb_cfg.get("region", "US"),
        paper=wb_cfg.get("paper", True),
        extended_hours=wb_cfg.get("extended_hours", True),
        redis_url=redis_url,
    )
    asyncio.run(gateway.start())


if __name__ == "__main__":
    main()
