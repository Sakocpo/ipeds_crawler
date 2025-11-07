# src/ipeds_crawler/logging.py
from __future__ import annotations

import logging
from pathlib import Path
from rich.logging import RichHandler

def setup_logging(level: str = "INFO") -> logging.Logger:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    console = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_time=True,
        show_path=False,
    )

    file_handler = logging.FileHandler(log_dir / "ipeds_crawler.log", encoding="utf-8")

    console_fmt = "%(message)s"
    file_fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    console.setFormatter(logging.Formatter(console_fmt))
    file_handler.setFormatter(logging.Formatter(file_fmt))

    logger = logging.getLogger("ipeds_crawler")
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.addHandler(console)
    logger.addHandler(file_handler)

    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    return logger
