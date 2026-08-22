"""
biz 层：财务指标画像 biz.finance_snapshot

从 akshare 拉取关键财务指标（ROE/毛利率/净利率/营收增速等），
结构化写入 biz.finance_snapshot，供投研快速查询。

数据来源：stock_financial_analysis_indicator（新浪财务分析指标）
"""
from __future__ import annotations

import time
from datetime import date as dt_date
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


def _fetch_finance_one(code: str) -> Optional[dict]:
    """获取单只股票的最新财务指标（最近约两年报告期，取最新一期）。"""
    try:
        df = ak.fetch_finance_indicator(
            symbol=code,
            start_year=str(dt_date.today().year - 2),
        )
        if df.empty or "日期" not in df.columns:
            return None

        # 返回行按报告期升序，最后一行才是最新报告期
        latest = df.sort_values("日期").iloc[-1]

        # akshare 返回的列名是中文，版本差异大，用模糊匹配
        def pick(keys: list[str]) -> Optional[float]:
            for k in keys:
                for col in df.columns:
                    if k in col:
                        try:
                            val = float(latest[col])
                        except (ValueError, TypeError):
                            continue
                        if val == val:  # 非 NaN 才返回，NaN 继续尝试备选列
                            return val
            return None

        report = latest["日期"]
        report_date = (
            report.isoformat() if hasattr(report, "isoformat") else str(report)
        )
        return {
            "stock_code": code,
            "report_date": report_date,
            "revenue": None,  # 这个接口主要是比率类指标，绝对值在三大表里
            "revenue_yoy": pick(["主营业务收入增长率", "营业总收入增长率"]),
            "net_profit": None,
            "net_profit_yoy": pick(["净利润增长率", "归母净利润增长率"]),
            "roe": pick(["加权净资产收益率", "净资产收益率"]),
            "roa": pick(["总资产净利润率", "总资产利润率"]),
            "gross_margin": pick(["销售毛利率", "主营业务利润率"]),
            "net_margin": pick(["销售净利率"]),
            "debt_ratio": pick(["资产负债率"]),
            "current_ratio": pick(["流动比率"]),
            "eps": pick(["摊薄每股收益"]),
            "bps": pick(["每股净资产_调整前"]),
        }
    except Exception as e:
        logger.debug(f"{code} 财务指标获取失败: {e}")
        return None


def fetch_and_save_finance(codes: Optional[list[str]] = None, limit: int = 0) -> int:
    """批量采集财务指标。串行拉取（新浪源，约 0.5s/只），批量写入。"""
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    results: list[dict] = []
    logger.info(f"开始采集财务指标: {len(codes)} 只股票")

    for i, code in enumerate(codes, 1):
        res = _fetch_finance_one(code)
        if res:
            results.append(res)
        if i % 200 == 0:
            logger.info(f"财务指标进度: {i}/{len(codes)}, 成功 {len(results)}")
        # 节流：共享 Session 虽复用连接，但新浪仍会按请求频率限流，逐只间隔 0.25s
        time.sleep(0.25)

    if not results:
        logger.warning("财务指标全部获取失败")
        return 0

    # 批量写入：临时表 + 一次性 upsert，替代逐条 INSERT
    df = pd.DataFrame(results)
    # report_date 统一为 YYYY-MM-DD 字符串，避免类型歧义
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    # 数值列强转 float（全 None 列会被 pandas 推断为 TEXT，导致入库类型不匹配）
    numeric_cols = [c for c in df.columns if c not in ("stock_code", "report_date")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    with get_session() as sess:
        conn = sess.connection()
        df.to_sql(
            "tmp_finance_snapshot", conn, schema="biz",
            if_exists="replace", index=False, method="multi", chunksize=1000,
        )
        sess.execute(text("""
            INSERT INTO biz.finance_snapshot
                (stock_code, report_date, revenue, revenue_yoy, net_profit,
                 net_profit_yoy, roe, roa, gross_margin, net_margin,
                 debt_ratio, current_ratio, eps, bps, updated_at)
            SELECT stock_code, CAST(report_date AS date), revenue, revenue_yoy, net_profit,
                   net_profit_yoy, roe, roa, gross_margin, net_margin,
                   debt_ratio, current_ratio, eps, bps, NOW()
            FROM biz.tmp_finance_snapshot
            ON CONFLICT (stock_code) DO UPDATE SET
                report_date = EXCLUDED.report_date,
                revenue = EXCLUDED.revenue,
                revenue_yoy = EXCLUDED.revenue_yoy,
                net_profit = EXCLUDED.net_profit,
                net_profit_yoy = EXCLUDED.net_profit_yoy,
                roe = EXCLUDED.roe,
                roa = EXCLUDED.roa,
                gross_margin = EXCLUDED.gross_margin,
                net_margin = EXCLUDED.net_margin,
                debt_ratio = EXCLUDED.debt_ratio,
                current_ratio = EXCLUDED.current_ratio,
                eps = EXCLUDED.eps,
                bps = EXCLUDED.bps,
                updated_at = NOW()
        """))
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_finance_snapshot"))

    logger.info(f"财务指标写入完成，共 {len(results)} 只")
    return len(results)


def run_finance_snapshot() -> None:
    run = start_run(platform_code="akshare", phase="phase_c_finance")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可刷新财务指标",
            )
            logger.warning("财务指标刷新跳过：core.stock 为空")
            return

        count = fetch_and_save_finance(codes=stock_codes)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=count,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=count, error_msg=err_msg)
        if status != "success":
            logger.warning(f"财务指标刷新结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"财务指标刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_finance_snapshot()
