"""
Centralized logging configuration for Options CIO.

Two handlers:
  - File: options_cio.log, rotating (10MB max, 5 backups)
  - Console: stderr, INFO level

Every log entry: timestamp, module, level, message.
API call logs include endpoint, response time, token count, cost.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent
LOG_FILE = LOG_DIR / "options_cio.log"
LOG_FORMAT = "%(asctime)s  %(name)-30s  %(levelname)-8s  %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5


def setup_logging(level: int = logging.DEBUG) -> None:
    """Configure root logger with file and console handlers."""
    root = logging.getLogger()

    # Avoid duplicate handlers on repeated calls
    if any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        return

    root.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Rotating file handler — captures everything (DEBUG+)
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Console handler — INFO+ only (keeps terminal clean)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    # Quiet noisy third-party loggers
    for name in ("urllib3", "httpx", "httpcore", "yfinance", "peewee"):
        logging.getLogger(name).setLevel(logging.WARNING)
