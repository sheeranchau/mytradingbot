"""
Centralized logging setup for all processes.

Log output:
  - Console: colored, human-readable
  - File: logs/<process_name>.log with daily rotation, 7-day retention

Usage in any module:
    from core.logger import get_logger
    logger = get_logger("okx_gateway")
    logger.info("Connected", symbol="BTC-USDT-SWAP", latency_ms=12.3)
"""
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Default format: timestamp | level | process | message
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(
    name: str,
    level: str = None,
    log_file: bool = True,
    retention_days: int = 7,
) -> logging.Logger:
    """
    Get a configured logger for a process/module.

    Args:
        name: Logger name (typically process name like "gateway_okx")
        level: Log level override. Default reads LOG_LEVEL env var, fallback INFO.
        log_file: Whether to write to file in addition to console.
        retention_days: How many days of log files to keep.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    resolved_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logger.setLevel(resolved_level)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(resolved_level)
    logger.addHandler(console)

    # File handler with daily rotation
    if log_file:
        log_path = LOG_DIR / f"{name}.log"
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=retention_days,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(resolved_level)
        file_handler.suffix = "%Y-%m-%d"
        logger.addHandler(file_handler)

    # Don't propagate to root logger
    logger.propagate = False

    return logger
