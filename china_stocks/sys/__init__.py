"""采集运行记录（sys.ingest_run）的封装。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger


@dataclass
class IngestRun:
    run_id: int
    platform_code: str
    phase: str
    target: Optional[str] = None


# 任务状态枚举（合法值）
VALID_STATUSES = {"running", "success", "failed", "skipped", "warning"}

# 任务最大执行时长（分钟），超时自动置为 failed
DEFAULT_TIMEOUT_MINUTES = 120


def start_run(platform_code: str, phase: str, target: str | None = None) -> IngestRun:
    """启动一次采集任务，返回 run 对象。

    启动前会先回收同 phase 的僵尸 running 任务（超时熔断），
    然后检查是否已有同 phase 的 running 任务（防并发重入），
    如有则直接 skipped 返回。
    """
    # 先回收同 phase 的僵尸任务
    reap_stale_runs(phase=phase)

    with get_session() as sess:
        # 检查是否已有同 phase 的 running 任务（防并发重入）
        existing = sess.execute(
            text("""
                SELECT 1 FROM sys.ingest_run
                WHERE phase = :phase AND status = 'running'
                LIMIT 1
            """),
            {"phase": phase},
        ).fetchone()
        if existing:
            logger.warning(f"phase={phase} 已有 running 任务，跳过本次启动（防并发重入）")
            # 插入一条 skipped 记录并返回
            row = sess.execute(
                text("""
                    INSERT INTO sys.ingest_run (platform_code, phase, target, status)
                    VALUES (:pc, :phase, :target, 'skipped')
                    RETURNING run_id
                """),
                {"pc": platform_code, "phase": phase, "target": target},
            ).fetchone()
            return IngestRun(run_id=row[0], platform_code=platform_code, phase=phase, target=target)

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
    """结束采集任务，记录结果。

    status 合法值：success / failed / skipped / warning
    - success：正常完成且写入了预期量级数据
    - warning：正常完成但写入行数为 0 或低于预期
    - skipped：上游依赖为空，主动跳过（需触发告警）
    - failed：执行异常
    """
    if status not in VALID_STATUSES:
        logger.warning(f"finish_run: 未知 status '{status}'，回退为 'failed'")
        status = "failed"

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


def reap_stale_runs(
    phase: str | None = None,
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES,
) -> int:
    """回收超时的 running 任务（僵尸任务熔断）。

    将超过 timeout_minutes 仍处于 running 状态的任务置为 failed，
    并记录超时错误信息。返回回收的任务数。
    """
    with get_session() as sess:
        result = sess.execute(
            text("""
                UPDATE sys.ingest_run
                SET status = 'failed',
                    error_msg = :err,
                    finished_at = NOW(),
                    cost_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::NUMERIC(10,2)
                WHERE status = 'running'
                  AND started_at < NOW() - (:mins || ' minutes')::INTERVAL
                  AND (:phase IS NULL OR phase = :phase)
            """),
            {
                "err": f"任务执行超时（>{timeout_minutes}分钟），已自动回收",
                "mins": timeout_minutes,
                "phase": phase,
            },
        )
        count = result.rowcount
        if count > 0:
            logger.warning(f"回收了 {count} 个超时僵尸任务（phase={phase or 'all'}）")
        return count


def reap_all_stale_runs(timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES) -> int:
    """回收所有 phase 的僵尸任务。服务启动时调用一次。"""
    return reap_stale_runs(phase=None, timeout_minutes=timeout_minutes)


def determine_status(
    rows_inserted: int = 0,
    rows_updated: int = 0,
    expected_min_rows: int = 1,
    blocked: bool = False,
    blocked_reason: str | None = None,
) -> tuple[str, str | None]:
    """根据写入行数和上游依赖状态判定任务最终状态。

    返回 (status, error_msg)：
    - blocked=True → ('skipped', blocked_reason)
    - 写入行数 < expected_min_rows → ('warning', '无数据写入')
    - 否则 → ('success', None)
    """
    if blocked:
        return "skipped", blocked_reason or "上游依赖为空，任务跳过"

    total = rows_inserted + rows_updated
    if total < expected_min_rows:
        return "warning", f"写入行数 {total} 低于预期最小值 {expected_min_rows}"

    return "success", None
