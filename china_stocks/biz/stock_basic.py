"""
biz 层：stock_basic 画像构建（最新行情 + 估值）

从 src_akshare.stock_daily 汇总最新交易日数据，加上估值指标（PE/PB 从 akshare 拉取），
写入 biz.stock_basic，供前端/投研查询使用。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import MAX_WORKERS
from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


def _last_trading_day() -> date:
    """取最近交易日（北京时间，跳过周末）。

    腾讯快照在非交易日返回最近一个交易日的收盘价，若用 DB 的 CURRENT_DATE
    （且为 UTC）会把周六/周日甚至 UTC 凌晨前的时点标错一天。故 as_of_date
    不用 CURRENT_DATE，而用最近交易日：先转北京时间，再回退到最近工作日。
    """
    bj = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))
    d = bj.date()
    while d.weekday() >= 5:  # 5=周六 6=周日
        d -= timedelta(days=1)
    return d


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
        # 行情字段（close/change_pct/turnover_rate）已在腾讯快照中取到；
        # stock_name 来自 core.stock，as_of_date 取最近交易日（北京时间），
        # 解除对空 stock_daily 的依赖，且避免非交易日/UTC 时区把时点标错一天。
        as_of_date = _last_trading_day()
        sess.execute(text("""
            INSERT INTO biz.stock_basic
                (stock_code, stock_name, pe_ttm, pb, close, change_pct,
                 turnover_rate, total_market_cap, float_market_cap, as_of_date, updated_at)
            SELECT
                t.stock_code,
                s.stock_name,
                t.pe_ttm, t.pb, t.close, t.change_pct, t.turnover_rate,
                t.total_market_cap, t.float_market_cap,
                :as_of_date, NOW()
            FROM biz.tmp_valuation t
            LEFT JOIN core.stock s ON s.stock_code = t.stock_code
            ON CONFLICT (stock_code) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                pe_ttm = EXCLUDED.pe_ttm,
                pb = EXCLUDED.pb,
                close = EXCLUDED.close,
                change_pct = EXCLUDED.change_pct,
                turnover_rate = EXCLUDED.turnover_rate,
                total_market_cap = EXCLUDED.total_market_cap,
                float_market_cap = EXCLUDED.float_market_cap,
                as_of_date = EXCLUDED.as_of_date,
                updated_at = NOW()
        """), {"as_of_date": as_of_date})
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_valuation"))

    logger.info(f"估值数据写入完成，共 {len(val_df)} 只")
    return len(val_df)


def _load_close_prices() -> dict[str, float]:
    """一次性取出 biz.stock_basic 的最新收盘价（作为股息率分母）。"""
    price_map: dict[str, float] = {}
    with get_session() as sess:
        rows = sess.execute(text(
            "SELECT stock_code, close FROM biz.stock_basic "
            "WHERE close IS NOT NULL AND close > 0"
        )).fetchall()
    for r in rows:
        price_map[r[0]] = float(r[1])
    return price_map


def fetch_and_save_dividend(codes: list[str], flush_every: int = 200) -> int:
    """批量采集股息率 dv_ttm（并发优化版）。

    并发采集每只股票的最新已实施现金分红（每10股派息），
    结合 biz.stock_basic 的最新收盘价计算股息率：
        dv_ttm(%) = (每10股派息 / 10) / 收盘价 × 100
    写入 biz.stock_basic.dv_ttm。

    并发策略：ThreadPoolExecutor，默认 MAX_WORKERS 线程，
    5500 只从串行数小时降到分钟级，避免任务超时被截断。

    去静默：统计 failed（接口/解析异常）与 skipped（无分红/无价），
    并在日志中明确反映，不再用 blanket try/except 吞掉整个采集。
    """
    results: list[dict] = []
    total = 0
    failed = 0
    skipped = 0

    price_map = _load_close_prices()
    logger.info(
        f"开始采集股息率: {len(codes)} 只股票, "
        f"有收盘价 {len(price_map)} 只, 并发 {MAX_WORKERS} 线程"
    )

    def _fetch_one(code: str) -> tuple[str, float | None, str | None]:
        """单只股票股息率采集，返回 (code, dv_ttm_or_None, error_or_None)。"""
        try:
            cash_per_10 = ak.fetch_cash_dividend_per_10(symbol=code)
        except Exception as e:  # noqa: BLE001
            return code, None, str(e)

        if cash_per_10 is None:
            return code, None, None  # 无分红

        price = price_map.get(code)
        if not price or price <= 0:
            return code, None, None  # 无价跳过

        div_per_share = cash_per_10 / 10.0
        dv_ttm = round(div_per_share / price * 100, 4)
        return code, dv_ttm, None

    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, code): code for code in codes}
        for fut in as_completed(futures):
            code, dv_ttm, err = fut.result()
            completed += 1
            if err:
                failed += 1
                logger.debug(f"{code} 股息率采集异常: {err}")
            elif dv_ttm is None:
                skipped += 1
            else:
                results.append({"stock_code": code, "dv_ttm": dv_ttm})

            if completed % 500 == 0:
                logger.info(f"股息率进度: {completed}/{len(codes)}")

            if len(results) >= flush_every:
                total += _batch_update_dividend(results)
                results = []

    if results:
        total += _batch_update_dividend(results)

    # 去静默：明确汇报成功/跳过/异常
    level = logger.warning if failed > 0 else logger.info
    level(
        f"股息率采集完成: 写入 {total} 只, "
        f"无分红/无价跳过 {skipped} 只, 异常 {failed} 只"
    )
    return total


def _batch_update_dividend(results: list[dict]) -> int:
    """批量写入股息率到 biz.stock_basic。"""
    if not results:
        return 0
    df = pd.DataFrame(results)
    with get_session() as sess:
        conn = sess.connection()
        df.to_sql(
            "tmp_dv", conn, schema="biz",
            if_exists="replace", index=False, method="multi", chunksize=1000,
        )
        r = sess.execute(text("""
            UPDATE biz.stock_basic b
            SET dv_ttm = t.dv_ttm, updated_at = NOW()
            FROM biz.tmp_dv t
            WHERE b.stock_code = t.stock_code
              AND b.dv_ttm IS DISTINCT FROM t.dv_ttm
        """))
        updated = r.rowcount
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_dv"))
    return updated


def update_ps_ttm() -> int:
    """根据营收和市值计算市销率 ps_ttm（P2-4 修复）。

    ps_ttm = total_market_cap / revenue（最新报告期）
    需要 P0-2 跑通（revenue 有数据）后才能生效。
    """
    with get_session() as sess:
        r = sess.execute(text("""
            UPDATE biz.stock_basic b
            SET ps_ttm = CASE
                    WHEN f.revenue IS NOT NULL AND f.revenue > 0
                         AND b.total_market_cap IS NOT NULL AND b.total_market_cap > 0
                    THEN ROUND((b.total_market_cap / f.revenue)::numeric, 4)
                    ELSE NULL
                END,
                updated_at = NOW()
            FROM biz.finance_snapshot f
            WHERE b.stock_code = f.stock_code
              AND (
                  (f.revenue IS NOT NULL AND f.revenue > 0
                   AND b.total_market_cap IS NOT NULL AND b.total_market_cap > 0
                   AND b.ps_ttm IS DISTINCT FROM ROUND((b.total_market_cap / f.revenue)::numeric, 4))
                  OR
                  ((f.revenue IS NULL OR f.revenue <= 0
                    OR b.total_market_cap IS NULL OR b.total_market_cap <= 0)
                   AND b.ps_ttm IS NOT NULL)
              )
        """))
        updated = r.rowcount
    logger.info(f"市销率更新: {updated} 只")
    return updated


def run_stock_basic() -> None:
    """刷新 biz.stock_basic（行情 + 估值 + 股息率 + 市销率）。"""
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

        # P2-4 修复：更新股息率 dv_ttm（失败不影响主流程）
        try:
            dv_count = fetch_and_save_dividend(codes=stock_codes)
            logger.info(f"股息率更新: {dv_count} 只")
        except Exception as e:
            logger.warning(f"股息率更新失败（将跳过）: {e}")

        # P2-4 修复：更新市销率 ps_ttm（失败不影响主流程）
        try:
            ps_count = update_ps_ttm()
            logger.info(f"市销率更新: {ps_count} 只")
        except Exception as e:
            logger.warning(f"市销率更新失败（将跳过）: {e}")

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
