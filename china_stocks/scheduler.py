"""
APScheduler 定时调度器。

所有采集任务的统一调度入口，取代 n8n（参考 crypto 项目的演化路径）。
任务失败会自动记录到 sys.ingest_run，可选邮件告警。

定时任务设计：
  - 每日 08:30 — Phase A（股票池 + 行业刷新，开市前）
  - 每日 16:00 — Phase B 日线行情（收盘后 1 小时，数据稳定）
  - 每日 17:30 — Phase C stock_basic 估值画像
  - 每日 19:00 — Phase B2 公告入口采集（巨潮资讯网）
  - 每日 20:00 — Phase B3 券商研报入口（东财研报中心）
  - 每日 20:30 — Phase B3 机构调研纪要补充
  - 每日 21:00 — Phase C 资金面画像（北向 + 融资融券）
  - 每周一 20:00 — Phase C 财务指标刷新（周频足够）
  - 每周二至六 02:00 — 公告 PDF 下载（夜间闲时批量下载）
  - 每周二至六 03:00 — 文档解析切块（PDF → doc_chunk，RAG 用）
  - 每周三 03:00 — Phase D 公司事件采集（分红/解禁/业绩预告/回购/增减持）
  - 每周日 03:00 — Phase C 股东画像（十大股东 + 质押 + 股东户数）
  - 每周日 06:00 — Phase D 投研笔记本刷新（完整性清单更新）
"""

from __future__ import annotations

import smtplib
import traceback
from email.mime.text import MIMEText
from email.header import Header

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import (
    ALERT_EMAIL_TO,
    SCHEDULER_ENABLED,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
    TIMEZONE,
)
from .logging_setup import logger


def send_alert(subject: str, body: str) -> None:
    """失败邮件告警。SMTP 未配置时跳过。"""
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO]):
        logger.debug("SMTP 未配置，跳过邮件告警")
        return

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL_TO

        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, ALERT_EMAIL_TO.split(","), msg.as_string())
        logger.info(f"告警邮件已发送: {subject}")
    except Exception as e:
        logger.warning(f"发送告警邮件失败: {e}")


def run_with_alert(job_name: str, func) -> None:
    """包装任务函数，失败发邮件。"""
    try:
        logger.info(f"[调度] 开始执行: {job_name}")
        func()
        logger.info(f"[调度] 执行完成: {job_name}")
    except Exception as e:
        tb = traceback.format_exc()
        logger.exception(f"[调度] 任务失败: {job_name} - {e}")
        send_alert(f"[A股采集告警] {job_name} 失败", tb)


def build_scheduler() -> BlockingScheduler:
    """构建调度器并注册所有任务。"""
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    # 延迟导入，避免循环依赖
    from .src.phase_a_stock_pool import run_phase_a
    from .src.phase_b_daily import run_phase_daily
    from .src.phase_b2_announcements import run_phase_b2_announcements
    from .src.phase_b2_download import run_download_announcements
    from .src.phase_b3_research import run_phase_b3_research
    from .src.phase_b3_survey import run_phase_b3_survey
    from .biz.stock_basic import run_stock_basic
    from .biz.finance_snapshot import run_finance_snapshot
    from .biz.capital_snapshot import run_capital_snapshot
    from .biz.shareholder_snapshot import run_shareholder_snapshot
    from .biz.research_notebook import run_build_notebooks
    from .biz.doc_parser import run_parse_docs
    from .src.phase_d_events import run_corporate_events

    # 每日 08:30 — 股票池 + 行业刷新
    scheduler.add_job(
        lambda: run_with_alert("Phase A-股票池构建", run_phase_a),
        CronTrigger(hour=8, minute=30, timezone=TIMEZONE),
        id="phase_a",
        misfire_grace_time=3600,
        max_instances=1,
    )

    # 每日 16:00 — 日线行情（收盘后）
    scheduler.add_job(
        lambda: run_with_alert("Phase B-日线行情", run_phase_daily),
        CronTrigger(hour=16, minute=0, timezone=TIMEZONE),
        id="phase_b_daily",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每日 17:30 — 估值画像
    scheduler.add_job(
        lambda: run_with_alert("Phase C-估值画像", run_stock_basic),
        CronTrigger(hour=17, minute=30, timezone=TIMEZONE),
        id="phase_c_stock_basic",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每周一 20:00 — 财务指标
    scheduler.add_job(
        lambda: run_with_alert("Phase C-财务指标", run_finance_snapshot),
        CronTrigger(day_of_week="mon", hour=20, minute=0, timezone=TIMEZONE),
        id="phase_c_finance",
        misfire_grace_time=86400,
        max_instances=1,
    )

    # 每日 19:00 — 公告入口采集
    scheduler.add_job(
        lambda: run_with_alert("Phase B2-公告入口", run_phase_b2_announcements),
        CronTrigger(hour=19, minute=0, timezone=TIMEZONE),
        id="phase_b2_announcements",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每日 20:00 — 券商研报入口采集
    scheduler.add_job(
        lambda: run_with_alert("Phase B3-券商研报", run_phase_b3_research),
        CronTrigger(hour=20, minute=0, timezone=TIMEZONE),
        id="phase_b3_research",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每日 20:30 — 机构调研纪要补充
    scheduler.add_job(
        lambda: run_with_alert("Phase B3-调研纪要", run_phase_b3_survey),
        CronTrigger(hour=20, minute=30, timezone=TIMEZONE),
        id="phase_b3_survey",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每日 21:00 — 资金面画像
    scheduler.add_job(
        lambda: run_with_alert("Phase C-资金面画像", run_capital_snapshot),
        CronTrigger(hour=21, minute=0, timezone=TIMEZONE),
        id="phase_c_capital",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # 每周二至六 02:00 — 公告 PDF 下载（夜间闲时跑，避免高峰）
    scheduler.add_job(
        lambda: run_with_alert("Phase B2-公告下载", run_download_announcements),
        CronTrigger(day_of_week="tue,sat", hour=2, minute=0, timezone=TIMEZONE),
        id="phase_b2_download",
        misfire_grace_time=86400,
        max_instances=1,
    )

    # 每周二至六 03:00 — 文档解析切块（下载完之后马上解析，供 RAG 用）
    scheduler.add_job(
        lambda: run_with_alert("Phase B-文档解析", run_parse_docs),
        CronTrigger(day_of_week="tue,sat", hour=3, minute=0, timezone=TIMEZONE),
        id="phase_b_parse",
        misfire_grace_time=86400,
        max_instances=1,
    )

    # 每周日 03:00 — 股东画像（十大股东每季度更新，周频足够）
    scheduler.add_job(
        lambda: run_with_alert("Phase C-股东画像", run_shareholder_snapshot),
        CronTrigger(day_of_week="sun", hour=3, minute=0, timezone=TIMEZONE),
        id="phase_c_shareholder",
        misfire_grace_time=86400,
        max_instances=1,
    )

    # 每周三 03:00 — 公司事件采集
    scheduler.add_job(
        lambda: run_with_alert("Phase D-公司事件", run_corporate_events),
        CronTrigger(day_of_week="wed", hour=3, minute=0, timezone=TIMEZONE),
        id="phase_d_events",
        misfire_grace_time=86400,
        max_instances=1,
    )

    # 每周日 06:00 — 刷新所有投研笔记本（周末计算，避免工作日干扰）
    scheduler.add_job(
        lambda: run_with_alert("Phase D-笔记本刷新", run_build_notebooks),
        CronTrigger(day_of_week="sun", hour=6, minute=0, timezone=TIMEZONE),
        id="phase_d_notebook",
        misfire_grace_time=86400,
        max_instances=1,
    )

    return scheduler


def start_scheduler() -> None:
    """启动调度器（阻塞式）。"""
    if not SCHEDULER_ENABLED:
        logger.info("调度器已禁用 (SCHEDULER_ENABLED=false)")
        return

    # 启动 Web 工作台（包含健康检查端点 + 状态看板）
    from .web_app import start_web_server

    start_web_server(port=8080)

    scheduler = build_scheduler()
    logger.info(
        f"调度器启动，时区 {TIMEZONE}，已注册 {len(scheduler.get_jobs())} 个任务:"
    )
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.id}: {job.next_run_time}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已停止")
