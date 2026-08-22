"""
日线行情采集 —— 增量追加到 src_akshare.stock_daily

策略：
- 单只股票用 ak.stock_zh_a_hist（前复权，日频），start_date 从库里已有最大日期+1天开始
- 批量并发采集，支持断点续跑
- 每日收盘后（16:00+）跑一次即可

注意：akshare 的 hist 接口是单股票调用，全 A 5000+ 只要约 10~15 分钟（4 并发）。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import MAX_WORKERS
from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from . import akshare_client as ak
from .phase_a_stock_pool import get_stock_codes


def _get_last_trade_date(stock_code: str) -> Optional[date]:
    """获取某股票在库里的最后交易日，用于增量采集。"""
    with get_session() as sess:
        row = sess.execute(
            text("SELECT MAX(trade_date) FROM src_akshare.stock_daily WHERE stock_code = :code"),
            {"code": stock_code},
        ).fetchone()
        return row[0] if row and row[0] else None


def _fetch_one_stock(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """拉取单只股票日 K 线。

    优先用东方财富 stock_zh_a_hist（字段全），失败则用新浪 stock_zh_a_daily 兜底。
    """
    # 尝试东方财富
    try:
        df = ak.call_api(
            "stock_zh_a_hist",
            save_raw=False,  # 单只股票不存 raw，避免表爆炸；整体在外部批量汇总再存
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",  # 前复权
        )
        if not df.empty:
            return _normalize_em_daily(df, stock_code)
    except Exception as e:
        logger.debug(f"{stock_code} 东方财富日线失败: {e}，尝试新浪兜底")

    # 新浪兜底：stock_zh_a_daily（symbol 需带市场前缀，如 sz000001 / sh600000）
    from .phase_a_stock_pool import _detect_market

    market = _detect_market(stock_code)
    if market in ("SH", "SZ"):
        symbol = f"{market.lower()}{stock_code}"
        try:
            df = ak.call_api(
                "stock_zh_a_daily",
                save_raw=False,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if not df.empty:
                return _normalize_sina_daily(df, stock_code)
        except Exception as e:
            logger.warning(f"{stock_code} 新浪日线也失败: {e}")

    return pd.DataFrame()


def _normalize_em_daily(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    """标准化东方财富日线数据。"""
    # 列名映射（akshare 返回中文）
    col_map = {
        "日期": "trade_date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "change_pct",
        "涨跌额": "change_amount",
        "换手率": "turnover_rate",
    }
    # 只保留存在的列
    existing = {c: col_map[c] for c in col_map if c in df.columns}
    df = df.rename(columns=existing)

    if "trade_date" not in df.columns:
        logger.warning(f"{stock_code} 返回字段无 '日期' 列，跳过")
        return pd.DataFrame()

    df["stock_code"] = stock_code
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # 数值列清洗
    for col in ["open", "close", "high", "low", "amplitude", "change_pct",
                "change_amount", "turnover_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    return df[["stock_code", "trade_date", "open", "high", "low", "close",
               "volume", "amount", "amplitude", "change_pct",
               "change_amount", "turnover_rate"]]


def _normalize_sina_daily(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    """标准化新浪日线数据（字段少，衍生指标需计算）。"""
    out = pd.DataFrame()
    out["trade_date"] = pd.to_datetime(df["date"]).dt.date
    out["stock_code"] = stock_code
    out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
    out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
    out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
    out["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").astype("Int64")
    out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")

    # 衍生指标（新浪接口不直接提供）
    # 涨跌幅 = (今收 - 昨收) / 昨收 * 100
    prev_close = out["close"].shift(1)
    out["change_pct"] = ((out["close"] - prev_close) / prev_close * 100).round(3)
    out["change_amount"] = (out["close"] - prev_close).round(3)
    # 振幅 = (最高 - 最低) / 昨收 * 100
    out["amplitude"] = ((out["high"] - out["low"]) / prev_close * 100).round(3)
    # 换手率 = 成交量 / 流通股本 * 100（新浪有 outstanding_share 但单位可能不一致，暂留空）
    if "outstanding_share" in df.columns:
        out_share = pd.to_numeric(df["outstanding_share"], errors="coerce")
        out["turnover_rate"] = (out["volume"] / out_share * 100).round(3)
    else:
        out["turnover_rate"] = None

    return out[["stock_code", "trade_date", "open", "high", "low", "close",
               "volume", "amount", "amplitude", "change_pct",
               "change_amount", "turnover_rate"]]


def _save_daily_batch(df: pd.DataFrame) -> int:
    """批量写入日线行情（临时表 + UPSERT，性能远优于逐条 executemany）。"""
    if df.empty:
        return 0

    with get_session() as sess:
        conn = sess.connection()
        # 1. 写入临时表（pandas to_sql + multi 批量）
        df.to_sql(
            "tmp_daily_batch",
            conn,
            schema="src_akshare",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=2000,
        )
        # 2. 从临时表 UPSERT 到正式表
        result = sess.execute(text("""
            INSERT INTO src_akshare.stock_daily
                (stock_code, trade_date, open, high, low, close,
                 volume, amount, amplitude, change_pct,
                 change_amount, turnover_rate, fetched_at)
            SELECT
                stock_code, trade_date, open, high, low, close,
                volume, amount, amplitude, change_pct,
                change_amount, turnover_rate, NOW()
            FROM src_akshare.tmp_daily_batch
            ON CONFLICT (stock_code, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                amplitude = EXCLUDED.amplitude,
                change_pct = EXCLUDED.change_pct,
                change_amount = EXCLUDED.change_amount,
                turnover_rate = EXCLUDED.turnover_rate,
                fetched_at = NOW()
        """))
        # 3. 清理临时表
        sess.execute(text("DROP TABLE IF EXISTS src_akshare.tmp_daily_batch"))

    return len(df)


def fetch_daily(
    stock_codes: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    incremental: bool = True,
) -> tuple[int, int]:
    """
    批量采集日线行情。

    Args:
        stock_codes: 股票代码列表，None 表示用 core.stock 全部
        start_date: 起始日期 YYYYMMDD，None 时根据 incremental 决定
        end_date: 结束日期 YYYYMMDD，None 表示今天
        incremental: 是否增量采集（从库里已有最后日期的下一天开始）

    Returns:
        (成功股票数, 写入行数)
    """
    if stock_codes is None:
        stock_codes = get_stock_codes()

    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    total_rows = 0
    success = 0
    failed = 0
    all_dfs: list[pd.DataFrame] = []

    logger.info(f"开始采集日线行情: {len(stock_codes)} 只股票, 截止 {end_date}")

    def _task(code: str) -> tuple[str, pd.DataFrame, Optional[str]]:
        try:
            if incremental and not start_date:
                last = _get_last_trade_date(code)
                if last:
                    s = (last + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    # 新股：从上市日期推，默认拉近 2 年数据兜底
                    s = (date.today() - timedelta(days=730)).strftime("%Y%m%d")
            else:
                s = start_date or (date.today() - timedelta(days=730)).strftime("%Y%m%d")

            # 如果开始日期 > 结束日期，跳过
            if s > end_date:
                return code, pd.DataFrame(), None

            df = _fetch_one_stock(code, s, end_date)
            return code, df, None
        except Exception as e:
            return code, pd.DataFrame(), str(e)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_task, code): code for code in stock_codes}
        for i, fut in enumerate(as_completed(futures), 1):
            code, df, err = fut.result()
            if err:
                failed += 1
                logger.warning(f"[{i}/{len(stock_codes)}] {code} 失败: {err}")
                continue

            if not df.empty:
                all_dfs.append(df)
                success += 1

            if i % 100 == 0:
                logger.info(f"进度: {i}/{len(stock_codes)}, 成功 {success}, 失败 {failed}")

    # 批量写入
    if all_dfs:
        big_df = pd.concat(all_dfs, ignore_index=True)
        total_rows = _save_daily_batch(big_df)

    logger.info(
        f"日线采集完成: 成功 {success}, 失败 {failed}, 写入 {total_rows} 行"
    )
    return success, total_rows


def run_phase_daily(
    stock_codes: Optional[list[str]] = None,
    incremental: bool = True,
) -> None:
    """执行日线行情采集 phase。"""
    target = ",".join(stock_codes[:5]) + ("..." if stock_codes and len(stock_codes) > 5 else "")
    run = start_run(platform_code="akshare", phase="phase_b_daily", target=target or "all")
    try:
        # 检查上游依赖：core.stock 是否有数据
        if stock_codes is None:
            stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可采集日线",
            )
            logger.warning("日线采集跳过：core.stock 为空")
            return

        success, rows = fetch_daily(stock_codes=stock_codes, incremental=incremental)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=rows,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=rows, error_msg=err_msg)
        if status != "success":
            logger.warning(f"日线采集结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"日线采集失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_phase_daily()
