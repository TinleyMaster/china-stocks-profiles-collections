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

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


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


def _fetch_valuation_snapshot() -> pd.DataFrame:
    """全市场一次拉取估值快照（腾讯源，含 PE_TTM/PB/市值）。

    替代原逐只调用的 stock_a_indicator_lg（该接口在新版 akshare 已移除）。
    腾讯接口约 15 秒返回全部 5500+ 只，远快于逐只采集。
    """
    df = ak.fetch_tx_spot_snapshot(save_raw=True)
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    # code 形如 sh688808 / sz000001，提取 6 位代码
    out["stock_code"] = df["code"].astype(str).str[-6:]
    out["pe_ttm"] = pd.to_numeric(df.get("pe_ttm"), errors="coerce")
    out["pb"] = pd.to_numeric(df.get("pn"), errors="coerce")  # pn = 市净率
    out["close"] = pd.to_numeric(df.get("zxj"), errors="coerce")
    out["change_pct"] = pd.to_numeric(df.get("zdf"), errors="coerce")
    out["turnover_rate"] = pd.to_numeric(df.get("hsl"), errors="coerce")
    # 市值单位：腾讯返回亿元 → 元
    out["total_market_cap"] = pd.to_numeric(df.get("zsz"), errors="coerce") * 1e8
    out["float_market_cap"] = pd.to_numeric(df.get("ltsz"), errors="coerce") * 1e8

    # 去重（以代码为准）
    out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)
    return out


def fetch_and_save_valuation(codes: Optional[list[str]] = None, limit: int = 0) -> int:
    """
    采集估值数据并写入 biz.stock_basic。

    全市场一次拉取（腾讯快照），批量 upsert，秒级完成。
    codes/limit 参数保留用于兼容，实际按全市场快照写入后过滤。
    """
    val_df = _fetch_valuation_snapshot()
    if val_df.empty:
        logger.warning("估值快照拉取为空")
        return 0

    # 如指定了 codes，只保留目标股票
    if codes:
        val_df = val_df[val_df["stock_code"].isin(set(codes))]
    if limit and limit > 0:
        val_df = val_df.head(limit)

    if val_df.empty:
        return 0

    with get_session() as sess:
        conn = sess.connection()
        # 临时表 + 批量 upsert
        val_df.to_sql(
            "tmp_valuation",
            conn,
            schema="biz",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=2000,
        )
        sess.execute(text("""
            INSERT INTO biz.stock_basic
                (stock_code, pe_ttm, pb, total_market_cap, float_market_cap, updated_at)
            SELECT stock_code, pe_ttm, pb, total_market_cap, float_market_cap, NOW()
            FROM biz.tmp_valuation
            ON CONFLICT (stock_code) DO UPDATE SET
                pe_ttm = EXCLUDED.pe_ttm,
                pb = EXCLUDED.pb,
                total_market_cap = EXCLUDED.total_market_cap,
                float_market_cap = EXCLUDED.float_market_cap,
                updated_at = NOW()
        """))
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_valuation"))

    logger.info(f"估值数据写入完成，共 {len(val_df)} 只")
    return len(val_df)


def run_stock_basic() -> None:
    """刷新 biz.stock_basic（行情 + 估值）。"""
    run = start_run(platform_code="akshare", phase="phase_c_stock_basic")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可刷新 stock_basic",
            )
            logger.warning("stock_basic 刷新跳过：core.stock 为空")
            return

        count = build_stock_basic_from_daily()
        val_count = fetch_and_save_valuation(codes=stock_codes)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=count,
            rows_updated=val_count,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=count, rows_updated=val_count, error_msg=err_msg)
        if status != "success":
            logger.warning(f"stock_basic 刷新结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"stock_basic 刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_stock_basic()
