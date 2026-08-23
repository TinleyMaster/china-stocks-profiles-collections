"""
biz 层：股东画像 biz.shareholder_snapshot

A 股投研核心维度之一，覆盖：
  1. 十大股东（每季度更新，来自定期报告）
  2. 股东人数（反映筹码集中度）
  3. 机构持仓占比（根据前十大股东名称估算）

数据来源：新浪财经-主要股东（stock_main_stock_holder）。
  注：原东财接口（十大股东/质押/股东户数）已全线失效，改用新浪单接口，
  一次请求同时拿到十大股东 + 股东总数 + 平均持股数。股权质押暂无可用免费源，
  置空跳过。
"""
from __future__ import annotations

import json
import time
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


def _estimate_inst_hold_pct(holders: list[dict]) -> Optional[float]:
    """根据前十大股东名称估算机构持仓占比（粗略）。"""
    inst_keywords = [
        "基金", "社保", "保险", "QFII", "券商", "资产管理", "信托", "银行", "养老",
        "投资公司", "投资管理", "证券投资", "资本管理", "企业年金",
        "全国社保", "基本养老保险", "保险产品", "保险资金",
    ]
    total_pct = 0.0
    found = False
    for h in holders:
        name = h.get("name") or ""
        pct = h.get("hold_pct")
        if pct is None:
            continue
        if any(kw in name for kw in inst_keywords):
            try:
                total_pct += float(pct)
                found = True
            except (ValueError, TypeError):
                continue
    return round(total_pct, 2) if found else None


def _fetch_profile(code: str) -> Optional[dict]:
    """拉取单只股票主要股东，解析最新一期画像（十大股东 + 股东总数）。"""
    try:
        df = ak.fetch_main_stock_holder(symbol=code)
    except Exception as e:
        logger.debug(f"{code} 主要股东获取失败: {e}")
        return None

    if df.empty or "股东名称" not in df.columns:
        return None

    # 选最新报告期：优先取「股东总数」非空的最新一期（回购披露可能缺股东总数）
    latest_date = None
    shareholder_count = None
    for d in df["截至日期"].dropna().unique():
        sub = df[df["截至日期"] == d]
        cnt = sub["股东总数"].dropna()
        if len(cnt):
            latest_date = d
            shareholder_count = int(cnt.iloc[0])
            break
        if latest_date is None:
            latest_date = d
    if latest_date is None:
        latest_date = df["截至日期"].max()

    sub = df[df["截至日期"] == latest_date]
    holders = []
    for _, row in sub.iterrows():
        name = row.get("股东名称")
        if pd.isna(name):
            continue
        holders.append({
            "name": str(name),
            "hold_shares": (
                None if pd.isna(row["持股数量"]) else float(row["持股数量"])
            ),
            "hold_pct": (
                None if pd.isna(row["持股比例"]) else float(row["持股比例"])
            ),
        })
    holders = holders[:10]

    return {
        "stock_code": code,
        "report_date": latest_date,
        "holders": holders,
        "shareholder_count": shareholder_count,
        "inst_hold_pct": _estimate_inst_hold_pct(holders),
        "pledge_pct": ak.fetch_pledge_pct(symbol=code),
    }


def fetch_and_save_shareholders(
    codes: Optional[list[str]] = None,
    limit: int = 0,
    flush_every: int = 500,
) -> tuple[int, int]:
    """批量采集股东画像。串行拉取（新浪源），增量落盘。

    flush_every：每累积这么多只就先 upsert 一次，避免一次性写入失败导致整批丢失。
    """
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    results: list[dict] = []
    total_saved = 0
    total_failed = 0
    logger.info(f"开始采集股东画像: {len(codes)} 只（每 {flush_every} 只增量落盘）")

    for i, code in enumerate(codes, 1):
        profile = _fetch_profile(code)
        if profile:
            results.append(profile)
        else:
            total_failed += 1
        # 节流：共享 Session 虽复用连接，但新浪仍按请求频率限流，逐只间隔 0.25s
        time.sleep(0.25)

        if len(results) >= flush_every:
            saved = _flush_shareholders(results)
            total_saved += saved
            results = []
            logger.info(f"股东画像进度: {i}/{len(codes)}, 累计落盘 {total_saved}")

    # 末尾剩余批次
    if results:
        saved = _flush_shareholders(results)
        total_saved += saved
        logger.info(f"股东画像进度: {len(codes)}/{len(codes)}, 累计落盘 {total_saved}")

    if total_saved == 0:
        logger.warning("股东画像全部获取失败，未写入任何数据")
    else:
        logger.info(f"股东画像写入完成，共 {total_saved} 只")
    return total_saved, total_failed


def _flush_shareholders(results: list[dict]) -> int:
    """把一批股东画像写入 biz.shareholder_snapshot（临时表 + 幂等 upsert）。返回写入条数。"""
    if not results:
        return 0

    rows = []
    for p in results:
        extra = {}
        if p.get("shareholder_count") is not None:
            extra["shareholder_count"] = p["shareholder_count"]
        full_json = {"top10_holders": p["holders"], "extra": extra}
        rows.append({
            "stock_code": p["stock_code"],
            "report_date": p["report_date"],
            "top10_json": json.dumps(full_json, ensure_ascii=False),
            "inst_hold_pct": p["inst_hold_pct"],
            "pledge_pct": p.get("pledge_pct"),
        })

    df = pd.DataFrame(rows)
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    # report_date NOT NULL，过滤掉解析不出的记录
    df = df.dropna(subset=["report_date"])
    # 数值列强转 float（全 None 列会被 pandas 推断为 TEXT，导致入库类型不匹配）
    df["inst_hold_pct"] = pd.to_numeric(df["inst_hold_pct"], errors="coerce")
    df["pledge_pct"] = pd.to_numeric(df["pledge_pct"], errors="coerce")

    with get_session() as sess:
        conn = sess.connection()
        df.to_sql(
            "tmp_shareholder", conn, schema="biz",
            if_exists="replace", index=False, method="multi", chunksize=1000,
        )
        # 幂等：先删这些股票的旧记录，再插入最新一期快照
        sess.execute(text("""
            DELETE FROM biz.shareholder_snapshot
            WHERE stock_code IN (SELECT stock_code FROM biz.tmp_shareholder)
        """))
        sess.execute(text("""
            INSERT INTO biz.shareholder_snapshot
                (stock_code, report_date, top10_json, inst_hold_pct, pledge_pct, updated_at)
            SELECT stock_code, CAST(report_date AS date), CAST(top10_json AS jsonb),
                   inst_hold_pct, pledge_pct, NOW()
            FROM biz.tmp_shareholder
        """))
        sess.execute(text("DROP TABLE IF EXISTS biz.tmp_shareholder"))

    return len(df)


def run_shareholder_snapshot() -> None:
    """刷新股东画像。"""
    run = start_run(platform_code="akshare", phase="phase_c_shareholder")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可刷新股东画像",
            )
            logger.warning("股东画像刷新跳过：core.stock 为空")
            return

        success, failed = fetch_and_save_shareholders(codes=stock_codes)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=success,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=success, rows_updated=failed, error_msg=err_msg)
        if status != "success":
            logger.warning(f"股东画像刷新结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"股东画像刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_shareholder_snapshot()
