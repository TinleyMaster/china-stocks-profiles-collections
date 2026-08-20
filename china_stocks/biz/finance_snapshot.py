"""
biz 层：财务指标画像 biz.finance_snapshot

从 akshare 拉取关键财务指标（ROE/毛利率/净利率/营收增速等），
结构化写入 biz.finance_snapshot，供投研快速查询。

数据来源：stock_financial_analysis_indicator（东方财富财务分析指标）
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import MAX_WORKERS
from ..db import get_session
from ..logging_setup import logger
from ..sys import finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


def _fetch_finance_one(code: str) -> Optional[dict]:
    """获取单只股票的最新财务指标。"""
    try:
        df = ak.call_api(
            "stock_financial_analysis_indicator",
            save_raw=False,
            symbol=code,
        )
        if df.empty:
            return None

        # akshare 返回的列名是中文，版本差异大，用模糊匹配
        latest = df.iloc[0]  # 第一行是最新报告期
        cols = {c: c for c in df.columns}

        def pick(keys: list[str]) -> Optional[float]:
            for k in keys:
                for col in cols:
                    if k in col:
                        try:
                            val = float(latest[col])
                            return val
                        except (ValueError, TypeError):
                            continue
            return None

        return {
            "stock_code": code,
            "report_date": str(latest.index[0]) if hasattr(latest, "index") else None,
            "revenue": None,  # 这个接口主要是比率类指标，绝对值在三大表里
            "revenue_yoy": pick(["主营业务收入增长率", "营业总收入增长率"]),
            "net_profit": None,
            "net_profit_yoy": pick(["净利润增长率", "归母净利润增长率"]),
            "roe": pick(["净资产收益率", "ROE"]),
            "roa": pick(["总资产报酬率", "ROA", "总资产净利率"]),
            "gross_margin": pick(["销售毛利率", "毛利率"]),
            "net_margin": pick(["销售净利率", "净利率"]),
            "debt_ratio": pick(["资产负债率"]),
            "current_ratio": pick(["流动比率"]),
            "eps": pick(["每股收益", "基本每股收益"]),
            "bps": pick(["每股净资产", "每股净资产BPS"]),
        }
    except Exception as e:
        logger.debug(f"{code} 财务指标获取失败: {e}")
        return None


def fetch_and_save_finance(codes: Optional[list[str]] = None, limit: int = 0) -> int:
    """批量采集财务指标。全量约 10~15 分钟（4 并发）。"""
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    results: list[dict] = []
    logger.info(f"开始采集财务指标: {len(codes)} 只股票")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_finance_one, code): code for code in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                results.append(res)
            if i % 200 == 0:
                logger.info(f"财务指标进度: {i}/{len(codes)}, 成功 {len(results)}")

    if not results:
        logger.warning("财务指标全部获取失败")
        return 0

    with get_session() as sess:
        for r in results:
            report_date = r.get("report_date")
            # 清洗日期格式
            if report_date and isinstance(report_date, str):
                try:
                    # 可能是 "20241231" 或 "2024-12-31" 或带时间
                    if len(report_date) == 8 and report_date.isdigit():
                        report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"
                except Exception:
                    report_date = None

            sess.execute(text("""
                INSERT INTO biz.finance_snapshot
                    (stock_code, report_date, revenue, revenue_yoy, net_profit,
                     net_profit_yoy, roe, roa, gross_margin, net_margin,
                     debt_ratio, current_ratio, eps, bps, updated_at)
                VALUES
                    (:code, :report_date, :revenue, :revenue_yoy, :net_profit,
                     :net_profit_yoy, :roe, :roa, :gross_margin, :net_margin,
                     :debt_ratio, :current_ratio, :eps, :bps, NOW())
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
            """), {
                "code": r["stock_code"],
                "report_date": report_date,
                "revenue": r.get("revenue"),
                "revenue_yoy": r.get("revenue_yoy"),
                "net_profit": r.get("net_profit"),
                "net_profit_yoy": r.get("net_profit_yoy"),
                "roe": r.get("roe"),
                "roa": r.get("roa"),
                "gross_margin": r.get("gross_margin"),
                "net_margin": r.get("net_margin"),
                "debt_ratio": r.get("debt_ratio"),
                "current_ratio": r.get("current_ratio"),
                "eps": r.get("eps"),
                "bps": r.get("bps"),
            })

    logger.info(f"财务指标写入完成，共 {len(results)} 只")
    return len(results)


def run_finance_snapshot() -> None:
    run = start_run(platform_code="akshare", phase="phase_c_finance")
    try:
        count = fetch_and_save_finance()
        finish_run(run, status="success", rows_inserted=count)
    except Exception as e:
        logger.exception(f"财务指标刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_finance_snapshot()
