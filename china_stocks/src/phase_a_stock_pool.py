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

    # 所有数据源都失败时，回落 raw.api_response 最近快照（降级可用）
    logger.warning("所有数据源实时拉取均失败，尝试从 raw.api_response 快照降级")
    try:
        with get_session() as sess:
            import json

            row = sess.execute(
                text("""
                    SELECT response, fetched_at
                    FROM raw.api_response
                    WHERE api_name IN ('stock_zh_a_spot_em', 'stock_info_a_code_name')
                    ORDER BY fetched_at DESC
                    LIMIT 1
                """),
            ).fetchone()
            if row is None:
                raise RuntimeError("raw.api_response 中也无可用快照")

            logger.warning(f"使用 raw 快照降级（时间: {row.fetched_at}）")
            records = row.response  # JSONB 自动解析为 list[dict]
            df = pd.DataFrame(records)

            # 兼容多种列名
            out = pd.DataFrame()
            col_code = next((c for c in df.columns if c in ("代码", "code", "股票代码")), None)
            col_name = next((c for c in df.columns if c in ("名称", "name", "股票名称")), None)
            if not col_code or not col_name:
                raise RuntimeError(f"raw 快照字段无法识别: {list(df.columns)[:10]}")
            out["stock_code"] = df[col_code].astype(str).str.zfill(6)
            out["stock_name"] = df[col_name].astype(str)
            out["market"] = out["stock_code"].apply(_detect_market)
            out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)
            logger.info(f"raw 快照降级成功: {len(out)} 只")
            return out
    except Exception as snapshot_err:
        raise RuntimeError(
            f"所有数据源都获取股票列表失败: {last_error}，raw 快照降级也失败: {snapshot_err}"
        )


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


# 申万一级行业（31 个，2021 版）——作为 legulegu 分类接口失败时的硬编码兜底
_SW_L1_INDUSTRIES: list[tuple[str, str]] = [
    ("801010", "农林牧渔"),
    ("801030", "基础化工"),
    ("801040", "钢铁"),
    ("801050", "有色金属"),
    ("801080", "电子"),
    ("801880", "汽车"),
    ("801110", "家用电器"),
    ("801120", "食品饮料"),
    ("801130", "纺织服饰"),
    ("801140", "轻工制造"),
    ("801150", "医药生物"),
    ("801160", "公用事业"),
    ("801170", "交通运输"),
    ("801180", "房地产"),
    ("801200", "商贸零售"),
    ("801210", "社会服务"),
    ("801780", "银行"),
    ("801790", "非银金融"),
    ("801230", "综合"),
    ("801710", "建筑材料"),
    ("801720", "建筑装饰"),
    ("801730", "电力设备"),
    ("801890", "机械设备"),
    ("801740", "国防军工"),
    ("801750", "计算机"),
    ("801760", "传媒"),
    ("801770", "通信"),
    ("801950", "煤炭"),
    ("801960", "石油石化"),
    ("801970", "环保"),
    ("801980", "美容护理"),
]


def _fetch_sw_l1_list() -> list[tuple[str, str]]:
    """获取申万一级行业列表 (代码, 名称)。legulegu 失败则硬编码兜底。"""
    try:
        df = ak.call_api("sw_index_first_info", save_raw=True)
        result: list[tuple[str, str]] = []
        for _, row in df.iterrows():
            code = str(row["行业代码"]).split(".")[0].strip()
            name = str(row["行业名称"]).strip()
            if code and name:
                result.append((code, name))
        if result:
            logger.info(f"申万一级行业分类获取成功: {len(result)} 个")
            return result
    except Exception as e:
        logger.warning(f"sw_index_first_info 失败，改用硬编码兜底: {e}")

    logger.info(f"使用硬编码申万一级行业: {len(_SW_L1_INDUSTRIES)} 个")
    return list(_SW_L1_INDUSTRIES)


def fetch_sw_industry() -> pd.DataFrame:
    """获取申万一级行业成分股 → 股票-行业映射。

    流程：先拿一级行业列表，再逐个行业拉成分股（index_component_sw，申万宏源源，稳定），
    汇总成 stock_code → industry_l1 映射。l2/l3 暂留空（后续可用三级行业接口补全）。
    """
    l1_list = _fetch_sw_l1_list()
    if not l1_list:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for code, name in l1_list:
        try:
            cons = ak.call_api("index_component_sw", save_raw=False, symbol=code)
            if cons.empty:
                logger.warning(f"申万一级行业 {code} {name} 无成分股")
                continue

            col_map = _find_columns(cons.columns.tolist(), {
                "code": ["证券代码", "股票代码", "代码", "code"],
                "name": ["证券名称", "股票名称", "名称", "name"],
            })
            if not col_map.get("code"):
                logger.warning(f"申万一级行业 {code} {name} 字段无法识别: {list(cons.columns)}")
                continue

            sub = pd.DataFrame()
            sub["stock_code"] = cons[col_map["code"]].astype(str).str.zfill(6)
            sub["stock_name"] = (
                cons[col_map["name"]].astype(str)
                if col_map.get("name")
                else cons[col_map["code"]].astype(str).str.zfill(6)
            )
            sub["industry_l1"] = name
            sub["industry_l2"] = None
            sub["industry_l3"] = None
            frames.append(sub)
        except Exception as e:
            logger.warning(f"申万一级行业 {code} {name} 成分股拉取失败: {e}")

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    # 申万一级行业互斥，但保险起见按股票代码去重（保留首个）
    out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)
    logger.info(f"申万行业成分股汇总: {len(out)} 只股票覆盖 {len(l1_list)} 个一级行业")
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
    """写入申万行业快照（pandas to_sql + multi 批量）。"""
    if df.empty:
        return 0
    with get_session() as sess:
        sess.execute(text("TRUNCATE TABLE src_akshare.sw_industry"))
        df[["stock_code", "stock_name", "industry_l1", "industry_l2", "industry_l3"]].to_sql(
            "sw_industry",
            sess.connection(),
            schema="src_akshare",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    logger.info(f"src_akshare.sw_industry 写入 {len(df)} 行")
    return len(df)


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


def update_list_date(codes: Optional[list[str]] = None, flush_every: int = 100) -> int:
    """批量更新 core.stock.list_date（上市日期）。

    使用 akshare 的 stock_individual_info_em 接口逐只拉取，
    避免 stock_zh_a_spot_em 无 list_date 字段的问题。
    返回更新行数。
    """
    if codes is None:
        codes = get_stock_codes()

    updated = 0
    results: list[dict] = []
    logger.info(f"开始更新上市日期: {len(codes)} 只股票")

    for i, code in enumerate(codes, 1):
        ld = ak.fetch_list_date(code)
        if ld:
            results.append({"stock_code": code, "list_date": ld})
        if i % 100 == 0:
            logger.info(f"上市日期进度: {i}/{len(codes)}, 已获取 {len(results)}")

        if len(results) >= flush_every:
            df = pd.DataFrame(results)
            with get_session() as sess:
                conn = sess.connection()
                df.to_sql(
                    "tmp_list_date", conn, schema="core",
                    if_exists="replace", index=False, method="multi", chunksize=1000,
                )
                r = sess.execute(text("""
                    UPDATE core.stock s
                    SET list_date = CAST(t.list_date AS date),
                        updated_at = NOW()
                    FROM core.tmp_list_date t
                    WHERE s.stock_code = t.stock_code
                      AND s.list_date IS DISTINCT FROM CAST(t.list_date AS date)
                """))
                updated += r.rowcount
                sess.execute(text("DROP TABLE IF EXISTS core.tmp_list_date"))
            results = []

    # 末尾剩余批次
    if results:
        df = pd.DataFrame(results)
        with get_session() as sess:
            conn = sess.connection()
            df.to_sql(
                "tmp_list_date", conn, schema="core",
                if_exists="replace", index=False, method="multi", chunksize=1000,
            )
            r = sess.execute(text("""
                UPDATE core.stock s
                SET list_date = CAST(t.list_date AS date),
                    updated_at = NOW()
                FROM core.tmp_list_date t
                WHERE s.stock_code = t.stock_code
                  AND s.list_date IS DISTINCT FROM CAST(t.list_date AS date)
            """))
            updated += r.rowcount
            sess.execute(text("DROP TABLE IF EXISTS core.tmp_list_date"))

    logger.info(f"上市日期更新完成: {updated} 只")
    return updated


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

        # 4. 更新上市日期（P2-1 修复，失败不影响主流程）
        try:
            list_date_count = update_list_date(codes=get_stock_codes())
            logger.info(f"上市日期更新: {list_date_count} 只")
        except Exception as e:
            logger.warning(f"上市日期更新失败（将跳过）: {e}")

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
