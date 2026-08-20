"""全局配置，从 .env 读取。"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# 项目根目录
ROOT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(ROOT_DIR / ".env")


def _get_env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"环境变量 {name} 未设置")
    return val


# ── 数据库 ────────────────────────────────────────────────────
DB_HOST = _get_env("DB_HOST", "localhost")
DB_PORT = int(_get_env("DB_PORT", "5432"))
DB_NAME = _get_env("DB_NAME", "china_stocks")
DB_USER = _get_env("DB_USER", "postgres")
DB_PASSWORD = _get_env("DB_PASSWORD", "postgres")

# ── 调度 ──────────────────────────────────────────────────────
SCHEDULER_ENABLED = _get_env("SCHEDULER_ENABLED", "true").lower() == "true"
TIMEZONE = _get_env("TIMEZONE", "Asia/Shanghai")

# ── 股票池 ────────────────────────────────────────────────────
def _parse_csv(val: str) -> list[str]:
    if not val:
        return []
    return [x.strip() for x in val.split(",") if x.strip()]


WATCHLIST_CODES: list[str] = _parse_csv(_get_env("WATCHLIST_CODES", ""))
PRIORITY_INDUSTRIES: list[str] = _parse_csv(_get_env("PRIORITY_INDUSTRIES", ""))
MAX_WORKERS = int(_get_env("MAX_WORKERS", "4"))

# ── 告警邮件（可选）──────────────────────────────────────────
SMTP_HOST = _get_env("SMTP_HOST", "")
SMTP_PORT = int(_get_env("SMTP_PORT", "465"))
SMTP_USER = _get_env("SMTP_USER", "")
SMTP_PASSWORD = _get_env("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = _get_env("ALERT_EMAIL_TO", "")

# ── Web 工作台 ───────────────────────────────────────────────
WEB_HOST = _get_env("WEB_HOST", "0.0.0.0")
WEB_PORT = int(_get_env("WEB_PORT", "8080"))


# ── 工具 ──────────────────────────────────────────────────────
def db_url() -> str:
    """返回 SQLAlchemy 用的连接字符串。"""
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
