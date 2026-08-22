"""
缺失资料一键补齐。

参照 crypto 项目的"缺失项一键补齐：按缺失类型映射动作串行执行"。

dispatching 表：completeness_key → 补齐动作函数

补齐范围：
  - 画像类（finance / capital / shareholder）：触发单股票采集
  - 事件类（dividend / unlock / buyback / profit_alert ...）：触发事件采集
  - 文档类（annual_report / research / survey ...）：扩日期范围回溯

文档类补齐较重，默认只补画像和事件类。
"""

from __future__ import annotations

from typing import Callable, Optional

from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src.phase_a_stock_pool import get_stock_codes
from .research_notebook import build_notebook_for_stock, list_missing_items


# ============================================================
# 补齐动作
# ============================================================


def _fill_finance(code: str) -> bool:
    from .finance_snapshot import fetch_and_save_finance

    try:
        fetch_and_save_finance(codes=[code])
        return True
    except Exception as e:
        logger.warning(f"{code} 财务补齐失败: {e}")
        return False


def _fill_stock_basic(code: str) -> bool:
    from .stock_basic import fetch_and_save_valuation

    try:
        fetch_and_save_valuation(codes=[code])
        return True
    except Exception as e:
        logger.warning(f"{code} 估值补齐失败: {e}")
        return False


def _fill_capital(code: str) -> bool:
    # 资金面是全市场数据（北向/两融），单只补齐效率太低，建议批量运行
    logger.info(f"{code} 资金面补齐：建议批量运行 python -m china_stocks capital")
    return False


def _fill_shareholder(code: str) -> bool:
    from .shareholder_snapshot import fetch_and_save_shareholders

    try:
        fetch_and_save_shareholders(codes=[code])
        return True
    except Exception as e:
        logger.warning(f"{code} 股东补齐失败: {e}")
        return False


def _fill_events(code: str) -> bool:
    from ..src.phase_d_events import fetch_all_events_for_stock

    try:
        fetch_all_events_for_stock(code)
        return True
    except Exception as e:
        logger.warning(f"{code} 事件补齐失败: {e}")
        return False


def _fill_docs(code: str) -> bool:
    """文档类补齐：提示用户运行全量公告/研报采集（单只回溯代价高）。"""
    logger.info(
        f"{code} 文档类补齐：请运行 python -m china_stocks announcements --start YYYYMMDD 全量回溯"
    )
    return False


# ============================================================
# 分发映射：completeness_key → (补齐函数, 权重分组)
# 同一补齐函数会被多个 key 共享，做去重
# ============================================================

FILL_DISPATCH: dict[str, Callable] = {
    # 画像类
    "finance_snapshot": _fill_finance,
    "stock_basic": _fill_stock_basic,
    "capital_snapshot": _fill_capital,
    "shareholder": _fill_shareholder,
    # 事件类（全部走事件采集）
    "dividend": _fill_events,
    "unlock": _fill_events,
    "buyback": _fill_events,
    "profit_alert": _fill_events,
    "share_change": _fill_events,
    "corporate_event": _fill_events,
    # 文档类（不自动补，提示用户）
    "annual_report": _fill_docs,
    "semi_annual_report": _fill_docs,
    "quarterly_report": _fill_docs,
    "prospectus": _fill_docs,
    "research_deep": _fill_docs,
    "research_comment": _fill_docs,
    "survey": _fill_docs,
    "announcement_other": _fill_docs,
    "industry_report": _fill_docs,
    "st_change": _fill_docs,
    "government_grant": _fill_docs,
    "related_party": _fill_docs,
}


# ============================================================
# 主流程
# ============================================================


def fill_missing(stock_code: str, item_keys: Optional[list[str]] = None) -> dict:
    """
    一键补齐缺失资料。

    Args:
        stock_code: 股票代码
        item_keys: 指定补齐哪些项，None = 补齐全部缺失项

    Returns:
        {"filled": [...], "failed": [...], "skipped": [...]}
    """
    missing = list_missing_items(stock_code)
    if not missing:
        return {"filled": [], "failed": [], "skipped": [], "message": "没有缺失项"}

    targets = [m["key"] for m in missing if (not item_keys or m["key"] in item_keys)]
    if not targets:
        return {
            "filled": [],
            "failed": [],
            "skipped": item_keys or [],
            "message": "指定项均已齐全",
        }

    filled = []
    failed = []
    skipped = []
    executed_actions = set()  # 函数 id 去重

    for key in targets:
        action = FILL_DISPATCH.get(key)
        if not action:
            skipped.append(key)
            continue

        action_id = id(action)

        if action_id in executed_actions:
            # 同组动作已经执行过，直接标记成功
            filled.append(key)
            _record_fill_task(stock_code, key, "skipped", "同组动作已执行")
            continue

        _record_fill_task(stock_code, key, "running")
        logger.info(f"补齐 {stock_code} - {key}")

        success = action(stock_code)
        if success:
            filled.append(key)
            _record_fill_task(stock_code, key, "success")
            executed_actions.add(action_id)
        else:
            failed.append(key)
            _record_fill_task(stock_code, key, "failed", "执行失败")

    # 刷新 notebook
    try:
        build_notebook_for_stock(stock_code)
    except Exception as e:
        logger.warning(f"补齐后刷新 notebook 失败: {e}")

    return {
        "filled": filled,
        "failed": failed,
        "skipped": skipped,
    }


def _record_fill_task(
    stock_code: str, fill_type: str, status: str, error: str | None = None
) -> None:
    """记录补齐任务。"""
    with get_session() as sess:
        if status == "running":
            sess.execute(
                text("""
                INSERT INTO biz.notebook_fill_task (stock_code, fill_type, status)
                VALUES (:code, :ft, :status)
            """),
                {"code": stock_code, "ft": fill_type, "status": status},
            )
        else:
            sess.execute(
                text("""
                UPDATE biz.notebook_fill_task SET
                    status = :status,
                    error_msg = :err,
                    finished_at = NOW()
                WHERE id = (
                    SELECT id FROM biz.notebook_fill_task
                    WHERE stock_code = :code AND fill_type = :ft AND status = 'running'
                    ORDER BY id DESC LIMIT 1
                )
            """),
                {"code": stock_code, "ft": fill_type, "status": status, "err": error},
            )


def run_fill_notebook(stock_code: str, items: Optional[list[str]] = None) -> None:
    """CLI 入口：执行补齐。"""
    run = start_run(platform_code="local", phase="phase_d_fill", target=stock_code)
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可执行补齐",
            )
            logger.warning("笔记本补齐跳过：core.stock 为空")
            return

        result = fill_missing(stock_code, item_keys=items)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=len(result["filled"]),
            expected_min_rows=1,
        )
        finish_run(
            run,
            status=status,
            rows_inserted=len(result["filled"]),
            rows_updated=len(result["failed"]),
            error_msg=err_msg,
        )
        if status != "success":
            logger.warning(f"笔记本补齐结束，状态: {status}，原因: {err_msg}")
        else:
            logger.info(
                f"补齐完成: 成功 {len(result['filled'])}, "
                f"失败 {len(result['failed'])}, 跳过 {len(result['skipped'])}"
            )
    except Exception as e:
        logger.exception(f"补齐失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise
