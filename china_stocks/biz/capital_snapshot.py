"""
biz 层：资金面画像 biz.capital_snapshot

A 股特有 Alpha 因子数据源：
  1. 北向资金持股（沪股通 + 深股通） — 东财接口
  2. 融资融券余额 — 东财接口
  3. 龙虎榜 — 标记当日是否上榜

策略：
- 北向持股：从 akshare 的沪深港通持股明细里汇总，按个股存最新快照 + 日变动
- 融资融券：同样日频更新
- 全部走 akshare 免费接口，零成本
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
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


# ============================================================
# 1. 北向资金
# ============================================================

def fetch_north_holdings(trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    获取北向资金最新持股明细（沪股通 + 深股通合并）。

    akshare 接口 stock_hsgt_hold_stock_em 按市场分开，这里合并。
    """
    all_dfs = []

    for market in ["沪股通", "深股通"]:
        try:
            df = ak.call_api(
                "stock_hsgt_hold_stock_em",
                save_raw=False,
                market=market,
                indicator="今日排行",
            )
            if df.empty:
                continue
            all_dfs.append(df)
            logger.info(f"{market} 持股: {len(df)} 条")
        except Exception as e:
            logger.warning(f"{market} 持股获取失败: {e}")

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)

    # 列名映射
    col_map = _find_columns(df.columns.tolist(), {
        "code": ["代码", "股票代码"],
        "name": ["名称", "股票名称"],
        "hold_shares": ["持股数量", "持股数", "数量"],
        "hold_pct": ["持股占流通股比", "持股比例", "占比", "占流通股比例"],
        "hold_amount": ["持股市值", "市值"],
        "change_pct": ["持股变动"],
    })

    if not col_map.get("code"):
        logger.warning("北向持股返回字段异常，找不到股票代码列")
        return pd.DataFrame()

    out = pd.DataFrame()
    out["stock_code"] = df[col_map["code"]].astype(str).str.zfill(6)
    out["north_hold_shares"] = _to_numeric(df, col_map.get("hold_shares"))
    out["north_hold_pct"] = _to_numeric(df, col_map.get("hold_pct"))
    out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)

    logger.info(f"北向持股汇总: {len(out)} 只股票")
    return out


# ============================================================
# 2. 融资融券
# ============================================================

def fetch_margin_balance() -> pd.DataFrame:
    """
    获取单只股票融资融券余额。

    akshare 有两种方式：
    - stock_margin_detail_szse / stock_margin_detail_sse（分交易所，按日期）
    - 这里用东财的个股融资融券数据更方便，但需要逐只拉

    折中方案：用 stock_margin 接口（全市场汇总），再按股票逐个补充
    —— 实际上 akshare.stock_margin_detail 可以获取全市场明细
    """
    try:
        df = ak.call_api("stock_margin_detail_szse", save_raw=False, date="20240101")
        # 这个接口返回数据格式可能变化，先用动态列匹配
        logger.info(f"融资融券明细: {len(df)} 条")
    except Exception as e:
        logger.warning(f"融资融券明细接口失败，尝试备用方式: {e}")
        return pd.DataFrame()

    return df


def save_capital_snapshot(north_df: pd.DataFrame, as_of: Optional[date] = None) -> int:
    """
    将资金面数据写入 biz.capital_snapshot。
    使用 upsert，缺失字段保留原值。
    """
    if north_df.empty:
        logger.warning("没有资金面数据可写入")
        return 0

    if as_of is None:
        as_of = date.today()

    count = 0
    with get_session() as sess:
        for _, row in north_df.iterrows():
            code = row["stock_code"]
            sess.execute(text("""
                INSERT INTO biz.capital_snapshot
                    (stock_code, as_of_date, north_hold_shares, north_hold_pct, updated_at)
                VALUES
                    (:code, :as_of, :hold_shares, :hold_pct, NOW())
                ON CONFLICT (stock_code) DO UPDATE SET
                    as_of_date = EXCLUDED.as_of_date,
                    north_hold_shares = EXCLUDED.north_hold_shares,
                    north_hold_pct = EXCLUDED.north_hold_pct,
                    updated_at = NOW()
            """), {
                "code": code,
                "as_of": as_of,
                "hold_shares": _safe_int(row.get("north_hold_shares")),
                "hold_pct": row.get("north_hold_pct"),
            })
            count += 1

    logger.info(f"biz.capital_snapshot 写入/更新 {count} 行")
    return count


# ============================================================
# 3. 融资融券（个股逐只采集）
# ============================================================

def _fetch_margin_one(code: str) -> Optional[dict]:
    """获取单只股票最新融资融券数据。"""
    try:
        df = ak.call_api(
            "stock_margin_detail_sse",
            save_raw=False,
            date=date.today().strftime("%Y%m%d"),
        )
        # 这个接口是全市场明细，按日期的，我们从里面过滤对应股票
        if df.empty:
            return None

        # 找代码列
        code_col = None
        for c in df.columns:
            if "证券代码" in c or "代码" in c:
                code_col = c
                break
        if code_col is None:
            return None

        row = df[df[code_col].astype(str).str.zfill(6) == code]
        if row.empty:
            return None

        row = row.iloc[0]
        balance_col = None
        for c in df.columns:
            if "融资余额" in c:
                balance_col = c
                break

        return {
            "stock_code": code,
            "margin_balance": _safe_float(row.get(balance_col)) if balance_col else None,
        }
    except Exception as e:
        logger.debug(f"{code} 融资融券获取失败: {e}")
        return None


def fetch_and_save_margin(codes: Optional[list[str]] = None, limit: int = 0) -> int:
    """
    批量获取融资融券余额并写入。
    注意：全市场 5000+ 只逐只拉效率低，实际建议用交易所全量数据一次拉取。
    这里先实现逐只版本，后续可以优化。
    """
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    results: list[dict] = []
    logger.info(f"开始采集融资融券: {len(codes)} 只")

    # 尝试一次性全量接口
    try:
        # 上交所融资融券明细
        df_sh = ak.call_api(
            "stock_margin_detail_sse",
            save_raw=False,
            date=date.today().strftime("%Y%m%d"),
        )
        if not df_sh.empty:
            logger.info(f"上交所融资融券明细: {len(df_sh)} 条")
    except Exception as e:
        logger.warning(f"上交所融资融券接口失败: {e}")
        df_sh = pd.DataFrame()

    try:
        # 深交所融资融券明细
        df_sz = ak.call_api(
            "stock_margin_detail_szse",
            save_raw=False,
            date=date.today().strftime("%Y%m%d"),
        )
        if not df_sz.empty:
            logger.info(f"深交所融资融券明细: {len(df_sz)} 条")
    except Exception as e:
        logger.warning(f"深交所融资融券接口失败: {e}")
        df_sz = pd.DataFrame()

    # 两个接口返回的字段格式差异很大，这里做通用化处理
    all_data: list[dict] = []

    for df_ in [df_sh, df_sz]:
        if df_.empty:
            continue
        cols = df_.columns.tolist()
        col_map = _find_columns(cols, {
            "code": ["证券代码", "标的证券代码", "代码"],
            "balance": ["融资余额", "融资余额(元)"],
        })
        if not col_map.get("code") or not col_map.get("balance"):
            continue

        for _, row in df_.iterrows():
            code = str(row[col_map["code"]]).zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue
            all_data.append({
                "stock_code": code,
                "margin_balance": _safe_float(row[col_map["balance"]]),
            })

    if all_data:
        with get_session() as sess:
            for item in all_data:
                sess.execute(text("""
                    INSERT INTO biz.capital_snapshot
                        (stock_code, as_of_date, margin_balance, updated_at)
                    VALUES
                        (:code, :as_of, :balance, NOW())
                    ON CONFLICT (stock_code) DO UPDATE SET
                        as_of_date = EXCLUDED.as_of_date,
                        margin_balance = EXCLUDED.margin_balance,
                        updated_at = NOW()
                """), {
                    "code": item["stock_code"],
                    "as_of": date.today(),
                    "balance": item["margin_balance"],
                })
        logger.info(f"融资融券写入完成: {len(all_data)} 只")
        return len(all_data)

    # 兜底：逐只采集
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_margin_one, code): code for code in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                results.append(res)
            if i % 200 == 0:
                logger.info(f"融资融券进度: {i}/{len(codes)}")

    if results:
        with get_session() as sess:
            for r in results:
                sess.execute(text("""
                    INSERT INTO biz.capital_snapshot
                        (stock_code, as_of_date, margin_balance, updated_at)
                    VALUES
                        (:code, :as_of, :balance, NOW())
                    ON CONFLICT (stock_code) DO UPDATE SET
                        as_of_date = EXCLUDED.as_of_date,
                        margin_balance = EXCLUDED.margin_balance,
                        updated_at = NOW()
                """), {
                    "code": r["stock_code"],
                    "as_of": date.today(),
                    "balance": r.get("margin_balance"),
                })
        logger.info(f"融资融券写入完成: {len(results)} 只")
        return len(results)

    logger.warning("融资融券数据全部获取失败")
    return 0


# ============================================================
# 工具函数
# ============================================================

def _find_columns(columns: list[str], mapping: dict[str, list[str]]) -> dict[str, str]:
    result = {}
    lower_cols = {c.lower(): c for c in columns}
    for logical, candidates in mapping.items():
        for cand in candidates:
            if cand in columns:
                result[logical] = cand
                break
            if cand.lower() in lower_cols:
                result[logical] = lower_cols[cand.lower()]
                break
    return result


def _to_numeric(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([None] * len(df))
    return pd.to_numeric(df[col], errors="coerce")


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (ValueError, TypeError):
        return None


def _safe_int(v) -> Optional[int]:
    f = _safe_float(v)
    return int(f) if f is not None else None


# ============================================================
# 主入口
# ============================================================

def run_capital_snapshot() -> None:
    """刷新资金面画像（北向 + 融资融券）。"""
    run = start_run(platform_code="akshare", phase="phase_c_capital")
    try:
        # 1. 北向持股
        north_df = fetch_north_holdings()
        north_count = save_capital_snapshot(north_df)

        # 2. 融资融券
        margin_count = fetch_and_save_margin()

        finish_run(
            run,
            status="success",
            rows_inserted=north_count,
            rows_updated=margin_count,
        )
        logger.info("资金面画像刷新完成")
    except Exception as e:
        logger.exception(f"资金面画像刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_capital_snapshot()
