"""采集运行记录（sys.ingest_run）的封装。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from ..db import get_session


@dataclass
class IngestRun:
    run_id: int
    platform_code: str
    phase: str
    target: Optional[str] = None


def start_run(platform_code: str, phase: str, target: str | None = None) -> IngestRun:
    """启动一次采集任务，返回 run 对象。"""
    with get_session() as sess:
        row = sess.execute(
            text("""
                INSERT INTO sys.ingest_run (platform_code, phase, target, status)
                VALUES (:pc, :phase, :target, 'running')
                RETURNING run_id
            """),
            {"pc": platform_code, "phase": phase, "target": target},
        ).fetchone()
        return IngestRun(run_id=row[0], platform_code=platform_code, phase=phase, target=target)


def finish_run(
    run: IngestRun,
    status: str = "success",
    rows_inserted: int = 0,
    rows_updated: int = 0,
    error_msg: str | None = None,
) -> None:
    """结束采集任务，记录结果。"""
    with get_session() as sess:
        sess.execute(
            text("""
                UPDATE sys.ingest_run
                SET status = :status,
                    rows_inserted = :ins,
                    rows_updated = :upd,
                    error_msg = :err,
                    finished_at = NOW(),
                    cost_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::NUMERIC(10,2)
                WHERE run_id = :rid
            """),
            {
                "rid": run.run_id,
                "status": status,
                "ins": rows_inserted,
                "upd": rows_updated,
                "err": error_msg,
            },
        )
