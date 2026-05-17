"""
Configuration loader.
Loads .env file and settings.yaml, resolving env var references.
"""
import os
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).parent.parent


def load_env(env_file: str = None) -> None:
    """Load .env file into os.environ. Does not override existing vars."""
    path = Path(env_file) if env_file else PROJECT_ROOT / ".env"
    if not path.exists():
        return

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            # Don't override existing env vars
            if key and key not in os.environ:
                os.environ[key] = value


def load_settings(config_file: str = None) -> dict:
    """Load settings.yaml, auto-loading .env first."""
    load_env()
    path = Path(config_file) if config_file else PROJECT_ROOT / "config" / "settings.yaml"
    with open(path) as f:
        return yaml.safe_load(f)
