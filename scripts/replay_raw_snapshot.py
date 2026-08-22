"""
raw 层快照重放入库脚本

用途：当 akshare 实时接口网络不通时，从 raw.api_response 中读取历史快照，
      重放写入 src_akshare 和 core 层，打通入库链路用于调试/验证。

用法：
    python replay_raw_snapshot.py

只重放 Phase A（股票列表 + core.stock），这是整个流水线的源头。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# 数据库连接（直接从环境变量或命令行参数读取，避免依赖项目 config）
DB_URL = (
    "postgresql://root:iuU2F8Vx1aj7A6gw3Pd4bH9rG5eL0RyW"
    "@43.166.198.83:32405/china_stocks"
)


def _detect_market(code: str) -> str:
    """根据 6 位股票代码判断交易所。"""
    if code.startswith(("60", "68", "90")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("43", "83", "87", "88")):
        return "BJ"
    return "UNKNOWN"


def load_latest_snapshot(engine, api_name: str) -> pd.DataFrame:
    """从 raw.api_response 读取指定 api 的最新快照，转成 DataFrame。"""
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT id, response, fetched_at
                FROM raw.api_response
                WHERE api_name = :api
                ORDER BY fetched_at DESC
                LIMIT 1
            """),
            {"api": api_name},
        ).fetchone()

    if row is None:
        raise RuntimeError(f"raw.api_response 中找不到 {api_name} 的快照")

    print(f"[重放] 读取快照 id={row.id}, api={api_name}, 时间={row.fetched_at}")
    records = row.response  # 已经是 list[dict]（JSONB 自动解析）
    df = pd.DataFrame(records)
    print(f"[重放] 快照共 {len(df)} 行, 字段: {list(df.columns)[:10]}...")
    return df


def normalize_stock_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    将 raw 快照标准化为 stock_code / stock_name / market 三列。

    兼容多种接口格式：
    - stock_zh_a_spot_em: 列名 "代码", "名称"
    - stock_zh_a_spot: 列名 "代码", "名称"（代码带 bj/sh/sz 前缀）
    - stock_info_a_code_name: 列名 "code", "name"
    """
    out = pd.DataFrame()

    # 尝试各种列名组合
    col_code = None
    col_name = None
    for c in df.columns:
        if c in ("代码", "code", "股票代码") and col_code is None:
            col_code = c
        if c in ("名称", "name", "股票名称") and col_name is None:
            col_name = c

    if col_code is None or col_name is None:
        raise RuntimeError(f"无法识别股票代码/名称列，现有列: {list(df.columns)}")

    out["stock_code"] = df[col_code].astype(str)
    out["stock_name"] = df[col_name].astype(str)

    # 清理代码：去掉 bj/sh/sz 前缀（stock_zh_a_spot 接口会带）
    out["stock_code"] = out["stock_code"].str.replace(
        r"^(bj|sh|sz)", "", regex=True, case=False
    )
    # 补零到 6 位
    out["stock_code"] = out["stock_code"].str.zfill(6)

    # 过滤掉非 6 位数字的异常代码
    out = out[out["stock_code"].str.match(r"^\d{6}$")].reset_index(drop=True)

    # 去重
    out = out.drop_duplicates(subset=["stock_code"]).reset_index(drop=True)

    # 判断市场
    out["market"] = out["stock_code"].apply(_detect_market)

    print(f"[重放] 标准化后: {len(out)} 只股票（去重+过滤后）")
    return out


def save_stock_list_to_src(engine, df: pd.DataFrame) -> int:
    """写入 src_akshare.stock_list（先清空再写入快照）。"""
    # 重命名列以匹配表结构
    out_df = df.rename(columns={
        "stock_code": "stock_code",
        "stock_name": "stock_name",
        "market": "market",
    })[["stock_code", "stock_name", "market"]]

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE src_akshare.stock_list"))
        # 用 pandas to_sql + multi 批量插入，比逐条 executemany 快得多
        out_df.to_sql(
            "stock_list",
            conn,
            schema="src_akshare",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    print(f"[重放] src_akshare.stock_list 写入 {len(out_df)} 行")
    return len(out_df)


def refresh_core_stock(engine) -> tuple[int, int]:
    """
    从 src_akshare.stock_list 刷新 core.stock（统一实体层）。

    与 phase_a_stock_pool.py 中的逻辑一致：
    - 新股票 INSERT
    - 已有股票 UPDATE 名称
    - 同步 core.stock_source_map
    """
    with engine.begin() as conn:
        # 1. UPSERT core.stock
        # full_code = market + '.' + stock_code（非空列必须有值）
        result = conn.execute(text("""
            INSERT INTO core.stock (stock_code, stock_name, market, full_code, is_delisted)
            SELECT
                sl.stock_code,
                sl.stock_name,
                sl.market,
                sl.market || '.' || sl.stock_code,
                FALSE
            FROM src_akshare.stock_list sl
            ON CONFLICT (stock_code) DO UPDATE
                SET stock_name = EXCLUDED.stock_name,
                    market = EXCLUDED.market,
                    full_code = EXCLUDED.full_code,
                    is_delisted = FALSE,
                    updated_at = NOW()
            RETURNING (xmax = 0) AS is_new
        """))
        rows = result.fetchall()
        inserted = sum(1 for r in rows if r.is_new)
        updated = len(rows) - inserted

        # 2. 同步 core.stock_source_map（akshare 平台）
        conn.execute(text("""
            INSERT INTO core.stock_source_map (stock_code, platform_code, source_id, source_name)
            SELECT stock_code, 'akshare', stock_code, stock_name
            FROM core.stock
            ON CONFLICT (stock_code, platform_code) DO UPDATE
                SET source_id = EXCLUDED.source_id,
                    source_name = EXCLUDED.source_name
        """))

    print(f"[重放] core.stock 更新完成: 新增 {inserted}, 更新 {updated}")
    return inserted, updated


def main():
    engine = create_engine(DB_URL)

    print("=" * 60)
    print("Phase A 快照重放入库")
    print("=" * 60)

    # 1. 从 raw 层读取最新股票列表快照
    # 优先尝试 stock_zh_a_spot_em，其次 stock_zh_a_spot
    api_candidates = ["stock_zh_a_spot_em", "stock_zh_a_spot", "stock_info_a_code_name"]
    raw_df = None
    used_api = None
    for api in api_candidates:
        try:
            raw_df = load_latest_snapshot(engine, api)
            used_api = api
            break
        except RuntimeError:
            continue

    if raw_df is None:
        print("[错误] raw.api_response 中找不到任何股票列表快照")
        sys.exit(1)

    print(f"[重放] 使用快照接口: {used_api}")

    # 2. 标准化
    stock_df = normalize_stock_list(raw_df)

    # 3. 写入 src_akshare.stock_list
    list_count = save_stock_list_to_src(engine, stock_df)

    # 4. 刷新 core.stock
    inserted, updated = refresh_core_stock(engine)

    # 5. 验证
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM core.stock")).scalar()
        sh = conn.execute(
            text("SELECT COUNT(*) FROM core.stock WHERE market = 'SH'")
        ).scalar()
        sz = conn.execute(
            text("SELECT COUNT(*) FROM core.stock WHERE market = 'SZ'")
        ).scalar()
        bj = conn.execute(
            text("SELECT COUNT(*) FROM core.stock WHERE market = 'BJ'")
        ).scalar()

    print()
    print("=" * 60)
    print("重放完成！")
    print(f"  src_akshare.stock_list: {list_count} 行")
    print(f"  core.stock 总数: {total}")
    print(f"    SH: {sh}, SZ: {sz}, BJ: {bj}")
    print(f"  本次新增: {inserted}, 更新: {updated}")
    print("=" * 60)


if __name__ == "__main__":
    main()
