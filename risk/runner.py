"""Entry point for risk engine process."""
import asyncio
from core.config import load_settings
from risk.engine import RiskEngine


def main():
    config = load_settings()
    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    engine = RiskEngine(redis_url=redis_url)
    asyncio.run(engine.start())


if __name__ == "__main__":
    main()
