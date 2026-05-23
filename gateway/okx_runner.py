"""Entry point for OKX gateway process."""
import asyncio
import os

from core.config import load_settings
from gateway.okx import OKXGateway


def main():
    config = load_settings()

    okx_cfg = config["gateways"]["okx"]
    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    gateway = OKXGateway(
        api_key=os.environ.get("OKX_API_KEY", okx_cfg.get("api_key", "")),
        secret_key=os.environ.get("OKX_SECRET_KEY", okx_cfg.get("secret_key", "")),
        passphrase=os.environ.get("OKX_PASSPHRASE", okx_cfg.get("passphrase", "")),
        symbols=okx_cfg["symbols"],
        simulated=okx_cfg.get("simulated", True),
        redis_url=redis_url,
    )
    asyncio.run(gateway.start())


if __name__ == "__main__":
    main()
