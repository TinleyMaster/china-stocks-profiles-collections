"""
Phase A：构建股票统一实体

流程：
  1. 从 akshare 拉取全 A 股列表 → src_akshare.stock_list
  2. 拉取申万行业分类 → src_akshare.sw_industry
  3. 合并写入 core.stock（统一实体层）

全量跑一次约 5000+ 只股票，建议每日开盘前跑一次。
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import WATCHLIST_CODES
from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from . import akshare_client as ak


def _detect_market(code: str) -> str:
    """根据 6 位股票代码判断交易所。"""
    if code.startswith(("60", "68", "90")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("43", "83", "87", "88")):
        return "BJ"
    return "UNKNOWN"


def fetch_stock_list() -> pd.DataFrame:
    """获取全 A 股列表（带代码、名称、交易所）。

    优先用东方财富（最全），失败则用新浪接口兜底。
    """
    last_error = None
    # 数据源优先级：东方财富 > 新浪
    for source in ["eastmoney", "sina"]:
        try:
            if source == "eastmoney":
                df = ak.call_api("stock_zh_a_spot_em", save_raw=True)
                if "代码" not in df.columns or "名称" not in df.columns:
                    raise RuntimeError("stock_zh_a_spot_em 返回字段不符合预期")
                out = pd.DataFrame()
                out["stock_code"] = df["代码"].astype(str).str.zfill(6)
                out["stock_name"] = df["名称"].astype(str)
            else:  # sina
                logger.warning("东方财富接口不可用，改用新浪接口 stock_info_a_code_name")
                df = ak.call_api("stock_info_a_code_name", save_raw=True)
                out = pd.DataFrame()
                if "code" in df.columns and "name" in df.columns:
                    out["stock_code"] = df["code"].astype(str).str.zfill(6)
                    out["stock_name"] = df["name"].astype(str)
                else:
                    # 某些版本列名是中文
                    col_map = _find_columns(df.columns.tolist(), {
                        "code": ["代码", "code", "股票代码"],
                        "name": ["名称", "name", "股票名称"],
                    })
                    if not col_map.get("code") or not col_map.get("name"):
                        raise RuntimeError("stock_info_a_code_name 返回字段不符合预期")
                    out["stock_code"] = df[col_map["code"]].astype(str).str.zfill(6)
                    out["stock_name"] = df[col_map["name"]].astype(str)

            out["market"] = out["stock_code"].apply(_detect_market)
            out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)
            logger.info(f"获取到 {len(out)} 只 A 股（来源: {source}）")
            return out
        except Exception as e:
            last_error = e
            logger.warning(f"{source} 数据源获取股票列表失败: {e}")
            continue

    raise RuntimeError(f"所有数据源都获取股票列表失败: {last_error}")


def save_stock_list_to_src(df: pd.DataFrame) -> int:
    """写入 src_akshare.stock_list（先清空再写入快照）。"""
    with get_session() as sess:
        sess.execute(text("TRUNCATE TABLE src_akshare.stock_list"))
        # 用 pandas to_sql + multi 批量插入，比 executemany 快 10-100 倍
        # （远程数据库网络延迟下尤其明显）
        df[["stock_code", "stock_name", "market"]].to_sql(
            "stock_list",
            sess.connection(),
            schema="src_akshare",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    logger.info(f"src_akshare.stock_list 写入 {len(df)} 行")
    return len(df)


def fetch_sw_industry() -> pd.DataFrame:
    """获取申万行业分类（新版接口）。"""
    try:
        df = ak.call_api("sw_index_first_info", save_raw=True)
        # 尝试不同的 akshare 接口名（版本差异大，兼容）
    except Exception:
        # 兜底：从行业成分股接口逐个拼
        logger.warning("sw_index_first_info 调用失败，尝试用 stock_board_industry_name_em 兜底")
        df = ak.call_api("stock_board_industry_name_em", save_raw=False)

    # 字段名因 akshare 版本差异很大，尝试匹配常见命名
    col_map = _find_columns(df.columns.tolist(), {
        "code": ["代码", "股票代码", "stock_code"],
        "name": ["名称", "股票名称", "stock_name"],
        "l1": ["申万一级", "一级行业", "行业", "板块名称", "行业名称"],
        "l2": ["申万二级", "二级行业"],
        "l3": ["申万三级", "三级行业"],
    })

    out = pd.DataFrame()
    out["stock_code"] = df[col_map["code"]].astype(str).str.zfill(6) if col_map.get("code") else ""
    out["stock_name"] = df[col_map["name"]].astype(str) if col_map.get("name") else ""
    out["industry_l1"] = df[col_map["l1"]].astype(str) if col_map.get("l1") else ""
    out["industry_l2"] = df[col_map["l2"]].astype(str) if col_map.get("l2") else None
    out["industry_l3"] = df[col_map["l3"]].astype(str) if col_map.get("l3") else None
    return out


def _find_columns(columns: list[str], mapping: dict[str, list[str]]) -> dict[str, str]:
    """在 columns 中查找映射字段，返回 {逻辑字段: 实际列名}。"""
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


def save_sw_industry_to_src(df: pd.DataFrame) -> int:
    """写入申万行业快照。"""
    with get_session() as sess:
        sess.execute(text("TRUNCATE TABLE src_akshare.sw_industry"))
        rows = df.to_dict(orient="records")
        if not rows:
            return 0
        sess.execute(
            text("""
                INSERT INTO src_akshare.sw_industry
                    (stock_code, stock_name, industry_l1, industry_l2, industry_l3)
                VALUES
                    (:stock_code, :stock_name, :industry_l1, :industry_l2, :industry_l3)
            """),
            rows,
        )
    logger.info(f"src_akshare.sw_industry 写入 {len(rows)} 行")
    return len(rows)


def refresh_core_stock() -> tuple[int, int]:
    """
    以 src 层为数据源，刷新 core.stock 统一实体表。
    返回 (inserted, updated)。
    """
    with get_session() as sess:
        # 单条 SQL 批量 UPSERT，避免逐行循环的 N 次远程往返
        # is_st / full_code / 行业字段都在 SQL 内计算
        result = sess.execute(text("""
            INSERT INTO core.stock (
                stock_code, stock_name, market, full_code,
                primary_industry_l1, primary_industry_l2, primary_industry_l3,
                is_st, is_delisted
            )
            SELECT
                sl.stock_code,
                sl.stock_name,
                sl.market,
                sl.market || '.' || sl.stock_code AS full_code,
                sw.industry_l1,
                sw.industry_l2,
                sw.industry_l3,
                (sl.stock_name LIKE '%ST%') AS is_st,
                FALSE
            FROM src_akshare.stock_list sl
            LEFT JOIN src_akshare.sw_industry sw
                ON sl.stock_code = sw.stock_code
            ON CONFLICT (stock_code) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                market = EXCLUDED.market,
                full_code = EXCLUDED.full_code,
                primary_industry_l1 = COALESCE(EXCLUDED.primary_industry_l1, core.stock.primary_industry_l1),
                primary_industry_l2 = COALESCE(EXCLUDED.primary_industry_l2, core.stock.primary_industry_l2),
                primary_industry_l3 = COALESCE(EXCLUDED.primary_industry_l3, core.stock.primary_industry_l3),
                is_st = EXCLUDED.is_st,
                updated_at = NOW()
            RETURNING (xmax = 0) AS is_new
        """))
        rows = result.fetchall()
        inserted = sum(1 for r in rows if r.is_new)
        updated = len(rows) - inserted

        # 2. 同步 core.stock_source_map（akshare 平台）
        sess.execute(
            text("""
                INSERT INTO core.stock_source_map (stock_code, platform_code, source_id, source_name)
                SELECT stock_code, 'akshare', stock_code, stock_name
                FROM core.stock
                ON CONFLICT (stock_code, platform_code) DO UPDATE
                    SET source_id = EXCLUDED.source_id,
                        source_name = EXCLUDED.source_name
            """)
        )

    logger.info(f"core.stock 更新完成: 新增 {inserted}, 更新 {updated}")
    return inserted, updated


def get_stock_codes(limit: Optional[int] = None, only_watchlist: bool = False) -> list[str]:
    """
    获取需要采集的股票代码列表。

    - 如果配置了 WATCHLIST_CODES 且 only_watchlist=True，则只返回自选股
    - limit 可选限制数量（用于调试）
    """
    if only_watchlist and WATCHLIST_CODES:
        codes = [c.zfill(6) for c in WATCHLIST_CODES]
    else:
        with get_session() as sess:
            rows = sess.execute(
                text("SELECT stock_code FROM core.stock WHERE is_delisted = FALSE ORDER BY stock_code")
            ).fetchall()
            codes = [r[0] for r in rows]

    if limit and limit > 0:
        codes = codes[:limit]
    return codes


def run_phase_a() -> None:
    """完整执行 Phase A。"""
    run = start_run(platform_code="akshare", phase="phase_a", target="all")
    try:
        # 1. 股票列表
        list_df = fetch_stock_list()
        list_count = save_stock_list_to_src(list_df)

        # 2. 申万行业（可能接口不稳定，失败不影响主流程）
        sw_count = 0
        try:
            sw_df = fetch_sw_industry()
            if not sw_df.empty and "stock_code" in sw_df.columns:
                sw_count = save_sw_industry_to_src(sw_df)
        except Exception as e:
            logger.warning(f"申万行业采集失败（将跳过行业字段）: {e}")

        # 3. 刷新 core.stock
        inserted, updated = refresh_core_stock()

        # 三态判定：Phase A 是源头，写入行数为 0 说明异常
        total_rows = inserted + list_count + sw_count
        status, err_msg = determine_status(
            rows_inserted=total_rows,
            rows_updated=updated,
            expected_min_rows=100,  # A 股至少上百只才算正常
        )

        finish_run(
            run,
            status=status,
            rows_inserted=inserted + list_count + sw_count,
            rows_updated=updated,
            error_msg=err_msg,
        )
        if status == "success":
            logger.info("Phase A 执行成功")
        else:
            logger.warning(f"Phase A 执行结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"Phase A 执行失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_phase_a()
