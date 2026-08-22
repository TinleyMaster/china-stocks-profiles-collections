"""
biz 层：资金面画像 biz.capital_snapshot

A 股特有的 Alpha 因子数据源：
  1. 北向资金持股（沪股通 + 深股通） — 东财接口（2024-08 后港交所停止实时披露，
     接口可能无数据，失败时跳过不阻塞）
  2. 融资融券余额 — 上交所/深交所官网源（注意：深交所比上交所晚一天披露）

策略：
- 北向持股：从 akshare 的沪深港通持股明细里汇总，按个股存最新快照 + 日变动
- 融资融券：按交易所回溯最近有数据的交易日，全量明细批量写入
- 全部走 akshare 免费接口，零成本
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
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

def _fetch_margin_latest(api_name: str, label: str, lookback: int = 10) -> pd.DataFrame:
    """从昨天起向前回溯，找最近一个有数据的交易日（交易所官网源）。

    深交所比上交所晚一天披露，周末/节假日也无数据，因此不能写死日期。
    """
    for back in range(1, lookback + 1):
        d = (date.today() - timedelta(days=back)).strftime("%Y%m%d")
        try:
            df = ak.call_api(api_name, save_raw=False, date=d)
        except Exception as e:
            logger.debug(f"{label}两融 {d} 拉取失败: {e}")
            continue
        if df is not None and not df.empty:
            logger.info(f"{label}两融明细 {d}: {len(df)} 条")
            return df
    logger.warning(f"{label}两融明细回溯 {lookback} 天均无数据")
    return pd.DataFrame()


def save_capital_snapshot(north_df: pd.DataFrame, as_of: Optional[date] = None) -> int:
    """
    将北向持股数据写入 biz.capital_snapshot。
    临时表 + 批量 upsert，缺失字段保留原值。
    """
    if north_df.empty:
        logger.warning("没有北向持股数据可写入")
        return 0

    if as_of is None:
        as_of = date.today()

    df = north_df.copy()
    df["as_of_date"] = as_of
    df["north_hold_shares"] = pd.to_numeric(df.get("north_hold_shares"), errors="coerce")
    df["north_hold_pct"] = pd.to_numeric(df.get("north_hold_pct"), errors="coerce")

    with get_session() as sess:
        conn = sess.connection()
        df.to_sql(
            "tmp_north_hold", conn, schema="biz",
            if_exists="replace", index=False, method="multi", chunksize=2000,
        )
        sess.execute(text("""
            INSERT INTO biz.capital_snapshot
                (stock_code, as_of_date, north_hold_shares, north_hold_pct, updated_at)
            SELECT stock_code, CAST(as_of_date AS date), north_hold_shares, north_hold_pct, NOW()
            FROM biz.tmp_north_hold
            ON CONFLICT (stock_code) DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                north_hold_shares = EXCLUDED.north_hold_shares,
                north_hold_pct = EXCLUDED.north_hold_pct,
                updated_at = NOW()
        """))
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_north_hold"))

    logger.info(f"biz.capital_snapshot 北向持股写入/更新 {len(df)} 行")
    return len(df)


# ============================================================
# 3. 融资融券（交易所全量明细，批量写入）
# ============================================================

def fetch_and_save_margin() -> int:
    """
    批量获取融资融券余额并写入。

    上交所/深交所官网源各一次全量拉取（各约 2000 条），
    汇总后临时表批量 upsert，秒级完成。
    """
    df_sh = _fetch_margin_latest("stock_margin_detail_sse", "上交所")
    df_sz = _fetch_margin_latest("stock_margin_detail_szse", "深交所")

    # 两个接口返回的字段格式差异很大，这里做通用化处理
    records: list[dict] = []
    for df_ in (df_sh, df_sz):
        if df_.empty:
            continue
        col_map = _find_columns(df_.columns.tolist(), {
            "code": ["标的证券代码", "证券代码", "代码"],
            "balance": ["融资余额(元)", "融资余额"],
        })
        if not col_map.get("code") or not col_map.get("balance"):
            logger.warning(f"两融明细字段异常: {df_.columns.tolist()}")
            continue

        part = pd.DataFrame()
        part["stock_code"] = df_[col_map["code"]].astype(str).str.zfill(6)
        part["margin_balance"] = pd.to_numeric(df_[col_map["balance"]], errors="coerce")
        # 过滤非 6 位数字代码
        part = part[part["stock_code"].str.fullmatch(r"\d{6}")]
        records.append(part)

    if not records:
        logger.warning("融资融券数据全部获取失败")
        return 0

    df = pd.concat(records, ignore_index=True).drop_duplicates(subset=["stock_code"])
    df["as_of_date"] = date.today()

    with get_session() as sess:
        conn = sess.connection()
        df.to_sql(
            "tmp_margin", conn, schema="biz",
            if_exists="replace", index=False, method="multi", chunksize=2000,
        )
        sess.execute(text("""
            INSERT INTO biz.capital_snapshot
                (stock_code, as_of_date, margin_balance, updated_at)
            SELECT stock_code, CAST(as_of_date AS date), margin_balance, NOW()
            FROM biz.tmp_margin
            ON CONFLICT (stock_code) DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                margin_balance = EXCLUDED.margin_balance,
                updated_at = NOW()
        """))
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_margin"))

    logger.info(f"融资融券写入完成: {len(df)} 只")
    return len(df)


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


# ============================================================
# 主入口
# ============================================================

def run_capital_snapshot() -> None:
    """刷新资金面画像（北向 + 融资融券）。"""
    run = start_run(platform_code="akshare", phase="phase_c_capital")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可刷新资金面画像",
            )
            logger.warning("资金面画像刷新跳过：core.stock 为空")
            return

        # 1. 北向持股
        north_df = fetch_north_holdings()
        north_count = save_capital_snapshot(north_df)

        # 2. 融资融券（交易所全量明细，无需按个股采集）
        margin_count = fetch_and_save_margin()

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=north_count,
            rows_updated=margin_count,
            expected_min_rows=1,
        )
        finish_run(
            run,
            status=status,
            rows_inserted=north_count,
            rows_updated=margin_count,
            error_msg=err_msg,
        )
        if status != "success":
            logger.warning(f"资金面画像刷新结束，状态: {status}，原因: {err_msg}")
        else:
            logger.info("资金面画像刷新完成")
    except Exception as e:
        logger.exception(f"资金面画像刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_capital_snapshot()
