"""
Configuration loader.
Loads .env file and settings.yaml, resolving env var references.
"""
import os
import re
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).parent.parent

_ENV_VAR_RE = re.compile(r"\$\{(\w+)\}")


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
            if key and key not in os.environ:
                os.environ[key] = value


def _resolve_env_vars(obj):
    """Recursively replace ${VAR} placeholders with os.environ values."""
    if isinstance(obj, str):
        return _ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), ""), obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(v) for v in obj]
    return obj


def load_settings(config_file: str = None) -> dict:
    """Load settings.yaml, auto-loading .env first and resolving ${VAR} refs."""
    load_env()
    path = Path(config_file) if config_file else PROJECT_ROOT / "config" / "settings.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)
