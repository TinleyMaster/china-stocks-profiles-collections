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
from ..sys import finish_run, start_run
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
    """拉取单只股票日 K 线。"""
    df = ak.call_api(
        "stock_zh_a_hist",
        save_raw=False,  # 单只股票不存 raw，避免表爆炸；整体在外部批量汇总再存
        symbol=stock_code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",  # 前复权
    )

    if df.empty:
        return df

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


def _save_daily_batch(df: pd.DataFrame) -> int:
    """批量写入日线行情，冲突更新（其实应该是新增为主）。"""
    if df.empty:
        return 0

    rows = df.to_dict(orient="records")
    with get_session() as sess:
        sess.execute(
            text("""
                INSERT INTO src_akshare.stock_daily
                    (stock_code, trade_date, open, high, low, close,
                     volume, amount, amplitude, change_pct,
                     change_amount, turnover_rate, fetched_at)
                VALUES
                    (:stock_code, :trade_date, :open, :high, :low, :close,
                     :volume, :amount, :amplitude, :change_pct,
                     :change_amount, :turnover_rate, NOW())
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
            """),
            rows,
        )
    return len(rows)


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
        success, rows = fetch_daily(stock_codes=stock_codes, incremental=incremental)
        finish_run(run, status="success", rows_inserted=rows)
    except Exception as e:
        logger.exception(f"日线采集失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_phase_daily()
