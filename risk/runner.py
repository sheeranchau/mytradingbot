"""Entry point for risk engine process."""
import asyncio
from risk.engine import RiskEngine


def main():
    engine = RiskEngine()
    asyncio.run(engine.start())


if __name__ == "__main__":
    main()
