"""日志配置，统一使用 loguru。"""
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

from .config import ROOT_DIR

_LOG_DIR = ROOT_DIR / "logs"
_LOG_DIR.mkdir(exist_ok=True)

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

# 移除默认 handler，自定义输出
logger.remove()
logger.add(sys.stdout, format=_FORMAT, level="INFO", enqueue=True)
logger.add(
    _LOG_DIR / "app_{time:YYYY-MM-DD}.log",
    format=_FORMAT,
    rotation="00:00",
    retention="30 days",
    compression="zip",
    level="DEBUG",
    enqueue=True,
)

__all__ = ["logger"]
