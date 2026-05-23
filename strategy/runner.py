"""Entry point for strategy container process."""
import asyncio
import importlib

from core.config import load_settings
from strategy.container import StrategyContainer


def load_strategies() -> list:
    """Load strategy instances from config."""
    config = load_settings()

    strategies = []
    for name, strat_config in config.get("strategies", {}).items():
        class_path = strat_config["class"]
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        instance = cls(
            name=name,
            gateway=strat_config["gateway"],
            symbols=strat_config["symbols"],
            **strat_config.get("params", {}),
        )
        strategies.append(instance)

    return strategies


def main():
    config = load_settings()
    strategies = load_strategies()
    if not strategies:
        print("[StrategyRunner] No strategies configured. Exiting.")
        return
    redis_url = config.get("redis", {}).get("url", "redis://127.0.0.1:6379")
    container = StrategyContainer(strategies=strategies, redis_url=redis_url)
    asyncio.run(container.start())


if __name__ == "__main__":
    main()
