"""Entry point for OKX gateway process."""
import asyncio
import os
import yaml

from gateway.okx import OKXGateway


def main():
    with open("config/settings.yaml") as f:
        config = yaml.safe_load(f)

    okx_cfg = config["gateways"]["okx"]
    gateway = OKXGateway(
        api_key=os.environ.get("OKX_API_KEY", okx_cfg.get("api_key", "")),
        secret_key=os.environ.get("OKX_SECRET_KEY", okx_cfg.get("secret_key", "")),
        passphrase=os.environ.get("OKX_PASSPHRASE", okx_cfg.get("passphrase", "")),
        symbols=okx_cfg["symbols"],
        simulated=okx_cfg.get("simulated", True),
    )
    asyncio.run(gateway.start())


if __name__ == "__main__":
    main()
