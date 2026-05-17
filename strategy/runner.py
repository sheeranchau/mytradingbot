"""Entry point for strategy container process."""
import asyncio
import yaml
import importlib
from pathlib import Path

from strategy.container import StrategyContainer


def load_strategies(config_path: str = "config/settings.yaml") -> list:
    """Load strategy instances from config."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

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
    strategies = load_strategies()
    if not strategies:
        print("[StrategyRunner] No strategies configured. Exiting.")
        return
    container = StrategyContainer(strategies=strategies)
    asyncio.run(container.start())


if __name__ == "__main__":
    main()
