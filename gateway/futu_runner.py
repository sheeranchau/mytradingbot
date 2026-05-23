"""Entry point for FUTU gateway process."""
import asyncio
import os

from core.config import load_settings
from gateway.futu import FUTUGateway


def main():
    config = load_settings()

    futu_cfg = config["gateways"]["futu"]
    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    gateway = FUTUGateway(
        host=futu_cfg.get("host", "127.0.0.1"),
        port=futu_cfg.get("port", 11111),
        symbols=futu_cfg["symbols"],
        market=futu_cfg.get("market", "US"),
        trade_env=futu_cfg.get("trade_env", "SIMULATE"),
        unlock_password=os.environ.get("FUTU_UNLOCK_PWD", futu_cfg.get("unlock_password", "")),
        redis_url=redis_url,
    )
    asyncio.run(gateway.start())


if __name__ == "__main__":
    main()
