"""数据库连接与 SQLAlchemy 引擎封装。"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..config import db_url

_engine = create_engine(
    db_url(),
    pool_size=5,
    max_overflow=10,
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
