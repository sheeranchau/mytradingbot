"""Entry point for FUTU gateway process."""
import asyncio
import os
import yaml

from gateway.futu import FUTUGateway


def main():
    with open("config/settings.yaml") as f:
        config = yaml.safe_load(f)

    futu_cfg = config["gateways"]["futu"]
    gateway = FUTUGateway(
        host=futu_cfg.get("host", "127.0.0.1"),
        port=futu_cfg.get("port", 11111),
        symbols=futu_cfg["symbols"],
        market=futu_cfg.get("market", "US"),
        trade_env=futu_cfg.get("trade_env", "SIMULATE"),
        unlock_password=os.environ.get("FUTU_UNLOCK_PWD", futu_cfg.get("unlock_password", "")),
    )
    asyncio.run(gateway.start())


if __name__ == "__main__":
    main()
