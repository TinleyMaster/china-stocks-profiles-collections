"""
biz 层：stock_basic 画像构建（最新行情 + 估值）

从 src_akshare.stock_daily 汇总最新交易日数据，加上估值指标（PE/PB 从 akshare 拉取），
写入 biz.stock_basic，供前端/投研查询使用。
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import MAX_WORKERS
from ..db import get_session
from ..logging_setup import logger
from ..sys import finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes
from concurrent.futures import ThreadPoolExecutor, as_completed


def _get_latest_trade_date() -> Optional[date]:
    """从日线表里找最新交易日。"""
    with get_session() as sess:
        row = sess.execute(text("SELECT MAX(trade_date) FROM src_akshare.stock_daily")).fetchone()
        return row[0] if row and row[0] else None


def build_stock_basic_from_daily() -> int:
    """
    从日线数据的最新交易日，构建 stock_basic 的基础行情字段。
    返回更新行数。
    """
    with get_session() as sess:
        result = sess.execute(text("""
            INSERT INTO biz.stock_basic
                (stock_code, stock_name, close, change_pct, volume, amount,
                 turnover_rate, as_of_date, updated_at)
            SELECT
                d.stock_code,
                s.stock_name,
                d.close,
                d.change_pct,
                d.volume,
                d.amount,
                d.turnover_rate,
                d.trade_date AS as_of_date,
                NOW()
            FROM src_akshare.stock_daily d
            JOIN core.stock s ON s.stock_code = d.stock_code
            WHERE d.trade_date = (SELECT MAX(trade_date) FROM src_akshare.stock_daily)
            ON CONFLICT (stock_code) DO UPDATE SET
                close = EXCLUDED.close,
                change_pct = EXCLUDED.change_pct,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                turnover_rate = EXCLUDED.turnover_rate,
                as_of_date = EXCLUDED.as_of_date,
                updated_at = NOW()
        """))
        # rowcount 可能不准（PG upsert 会返回所有命中行），用查询数兜底
        count = sess.execute(text("SELECT COUNT(*) FROM biz.stock_basic")).fetchone()[0]
    logger.info(f"biz.stock_basic 行情字段已刷新，共 {count} 只")
    return count


def _fetch_valuation_one(code: str) -> Optional[dict]:
    """从 akshare 获取单只股票的估值数据（PE/PB/市值等）。

    使用 stock_a_indicator_lg（乐咕乐股网的 A 股指标）或东财 F10 接口兜底。
    """
    try:
        # 尝试东财 F10 主要指标
        df = ak.call_api(
            "stock_a_indicator_lg",
            save_raw=False,
            symbol=code,
        )
        if df.empty:
            return None
        # 取最新一行
        latest = df.iloc[-1]
        return {
            "stock_code": code,
            "pe_ttm": _safe_float(latest.get("pe_ttm")),
            "pb": _safe_float(latest.get("pb")),
            "ps_ttm": _safe_float(latest.get("ps_ttm")),
            "dv_ttm": _safe_float(latest.get("dv_ttm")),
            "total_market_cap": _safe_float(latest.get("total_mv")),  # 万元
            "float_market_cap": _safe_float(latest.get("circ_mv")),
        }
    except Exception as e:
        logger.debug(f"{code} 估值获取失败: {e}")
        return None


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def fetch_and_save_valuation(codes: Optional[list[str]] = None, limit: int = 0) -> int:
    """
    批量采集估值数据并写入 biz.stock_basic。
    全量 5000+ 只约 5~10 分钟（4 并发）。
    """
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    results: list[dict] = []

    def _task(code: str) -> Optional[dict]:
        return _fetch_valuation_one(code)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_task, code): code for code in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                results.append(res)
            if i % 200 == 0:
                logger.info(f"估值采集进度: {i}/{len(codes)}, 成功 {len(results)}")

    if not results:
        logger.warning("估值数据全部获取失败")
        return 0

    with get_session() as sess:
        for r in results:
            total_mv = r.get("total_market_cap")
            # 单位转换：乐咕接口 total_mv 是万元 → 元
            if total_mv is not None:
                total_mv = total_mv * 10000
            float_mv = r.get("float_market_cap")
            if float_mv is not None:
                float_mv = float_mv * 10000

            sess.execute(text("""
                INSERT INTO biz.stock_basic
                    (stock_code, pe_ttm, pb, ps_ttm, dv_ttm,
                     total_market_cap, float_market_cap, updated_at)
                VALUES
                    (:code, :pe_ttm, :pb, :ps_ttm, :dv_ttm,
                     :total_mv, :float_mv, NOW())
                ON CONFLICT (stock_code) DO UPDATE SET
                    pe_ttm = EXCLUDED.pe_ttm,
                    pb = EXCLUDED.pb,
                    ps_ttm = EXCLUDED.ps_ttm,
                    dv_ttm = EXCLUDED.dv_ttm,
                    total_market_cap = EXCLUDED.total_market_cap,
                    float_market_cap = EXCLUDED.float_market_cap,
                    updated_at = NOW()
            """), {
                "code": r["stock_code"],
                "pe_ttm": r.get("pe_ttm"),
                "pb": r.get("pb"),
                "ps_ttm": r.get("ps_ttm"),
                "dv_ttm": r.get("dv_ttm"),
                "total_mv": total_mv,
                "float_mv": float_mv,
            })

    logger.info(f"估值数据写入完成，共 {len(results)} 只")
    return len(results)


def run_stock_basic() -> None:
    """刷新 biz.stock_basic（行情 + 估值）。"""
    run = start_run(platform_code="akshare", phase="phase_c_stock_basic")
    try:
        count = build_stock_basic_from_daily()
        val_count = fetch_and_save_valuation()
        finish_run(run, status="success", rows_inserted=count, rows_updated=val_count)
    except Exception as e:
        logger.exception(f"stock_basic 刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_stock_basic()
