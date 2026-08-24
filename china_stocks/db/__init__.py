"""数据库连接与 SQLAlchemy 引擎封装。"""
from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..config import db_url
from ..logging_setup import logger

_url = db_url()

# 启动时打印数据库名（脱敏密码），方便排查部署时库名配错问题
def _mask_db_url(url: str) -> str:
    """脱敏数据库连接串中的密码，仅保留 host:port/dbname 用于日志。"""
    m = re.search(r"://[^:]+:([^@]+)@", url)
    if m:
        return url.replace(m.group(1), "***")
    return url

logger.info(f"数据库连接: {_mask_db_url(_url)}")

_engine = create_engine(
    _url,
    pool_size=2,
    max_overflow=3,
    pool_pre_ping=True,
    pool_recycle=1800,  # 30 分钟回收旧连接，避免长时间空闲被服务端断开
    future=True,
)

SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def get_session() -> Iterator[Session]:
    """事务性会话上下文，异常自动回滚。"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_engine():
    return _engine
