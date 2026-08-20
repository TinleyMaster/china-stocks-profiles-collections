"""
biz 层：股东画像 biz.shareholder_snapshot

A 股投研核心维度之一，覆盖三大块：
  1. 十大股东（每季度更新，来自定期报告）
  2. 股权质押（日频更新，东财数据）
  3. 股东人数（半月/月频，反映筹码集中度）

策略：
  - 十大股东：akshare.stock_gdfx_top_10_em（东财股东分析）
  - 质押：akshare.stock_pledge_detail（东财股权质押）
  - 股东人数：akshare.stock_zh_a_gdhs（东财股东户数）
  - 全部按报告期存快照，最新一期置顶
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
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
# 1. 十大股东
# ============================================================

def fetch_top10_holders(code: str) -> Optional[dict]:
    """
    获取最新一期十大流通股东。
    返回 {report_date, holders: [...], inst_hold_pct}
    """
    try:
        df = ak.call_api(
            "stock_gdfx_free_top_10_em",
            save_raw=False,
            symbol=code,
            date="20240630",  # 会自动取最新一期
        )
        if df.empty:
            return None
    except Exception as e:
        logger.debug(f"{code} 十大股东获取失败: {e}")
        return None

    col_map = _find_columns(df.columns.tolist(), {
        "name": ["股东名称", "名称"],
        "hold_shares": ["持有流通股数量", "持股数量", "持股数"],
        "hold_pct": ["占总股本比例", "占流通股比例", "持股比例", "占比"],
        "change": ["变动方向", "变动"],
        "change_amount": ["变动比例", "变动股数"],
    })

    if not col_map.get("name"):
        return None

    holders = []
    for _, row in df.iterrows():
        holder = {}
        for key, col in col_map.items():
            if col in df.columns:
                val = row[col]
                if pd.isna(val):
                    val = None
                holder[key] = str(val) if val is not None else None
        holders.append(holder)

    # 报告期：从数据里推断，取第一条的日期字段（如果有的话）
    report_date = None
    for col in df.columns:
        if "报告期" in col or "日期" in col:
            val = df.iloc[0][col]
            if pd.notna(val):
                try:
                    report_date = pd.to_datetime(str(val)).date()
                except Exception:
                    pass
            break

    # 估算机构持仓占比（前十大股东里带"基金/社保/保险/QFII/券商"等字样的）
    inst_pct = _estimate_inst_hold_pct(holders)

    return {
        "stock_code": code,
        "report_date": report_date,
        "holders": holders,
        "inst_hold_pct": inst_pct,
    }


def _estimate_inst_hold_pct(holders: list[dict]) -> Optional[float]:
    """根据前十大股东名称估算机构持仓占比（粗略）。"""
    inst_keywords = ["基金", "社保", "保险", "QFII", "券商", "资产管理", "信托", "银行", "养老"]
    total_pct = 0.0
    found = False
    for h in holders:
        name = h.get("name", "") or ""
        pct_str = h.get("hold_pct", "") or ""
        if any(kw in name for kw in inst_keywords):
            try:
                pct = float(str(pct_str).replace("%", "").strip())
                total_pct += pct
                found = True
            except (ValueError, TypeError):
                continue
    return round(total_pct, 2) if found else None


# ============================================================
# 2. 股权质押
# ============================================================

def fetch_pledge(code: str) -> Optional[float]:
    """获取最新股权质押比例（%）。"""
    try:
        df = ak.call_api(
            "stock_pledge_detail",
            save_raw=False,
            symbol=code,
        )
        if df.empty:
            return None
    except Exception as e:
        logger.debug(f"{code} 质押数据获取失败: {e}")
        return None

    # 找质押比例列
    for col in df.columns:
        if "质押比例" in col or "占总股本比例" in col:
            try:
                # 取最新一条
                val = df.iloc[-1][col]
                return float(str(val).replace("%", "").strip())
            except (ValueError, TypeError, IndexError):
                continue
    return None


# ============================================================
# 3. 股东人数（筹码集中度）
# ============================================================

def fetch_shareholder_count(code: str) -> Optional[dict]:
    """获取股东户数变化。"""
    try:
        df = ak.call_api(
            "stock_zh_a_gdhs",
            save_raw=False,
            symbol=code,
        )
        if df.empty or len(df) < 2:
            return None
    except Exception as e:
        logger.debug(f"{code} 股东户数获取失败: {e}")
        return None

    col_map = _find_columns(df.columns.tolist(), {
        "date": ["股东统计日期", "截止日期", "日期"],
        "count": ["股东户数", "总户数"],
        "change_pct": ["较上期变化", "增减比例", "变化比例"],
    })

    if not col_map.get("count"):
        return None

    latest = df.iloc[0]
    try:
        return {
            "date": str(latest[col_map["date"]]) if col_map.get("date") else None,
            "count": int(float(latest[col_map["count"]])),
            "change_pct": _safe_float(latest[col_map["change_pct"]]) if col_map.get("change_pct") else None,
        }
    except (ValueError, TypeError):
        return None


# ============================================================
# 4. 写入数据库
# ============================================================

def _save_one_shareholder(code: str, top10: Optional[dict], pledge_pct: Optional[float],
                          gdhs: Optional[dict]) -> bool:
    """写入单只股票的股东画像。"""
    report_date = None
    holders_json = None
    inst_hold_pct = None

    if top10:
        report_date = top10.get("report_date")
        holders_json = top10.get("holders")
        inst_hold_pct = top10.get("inst_hold_pct")

    # 如果报告日期解析不出来，用当前季度末兜底
    if report_date is None and gdhs and gdhs.get("date"):
        try:
            report_date = pd.to_datetime(gdhs["date"]).date()
        except Exception:
            pass

    # 附加字段存到 JSON 里
    extra = {}
    if gdhs:
        extra["shareholder_count"] = gdhs.get("count")
        extra["shareholder_change_pct"] = gdhs.get("change_pct")
    if pledge_pct:
        extra["pledge_pct"] = pledge_pct

    # 合并 holders + extra 到 top10_json
    full_json = {
        "top10_holders": holders_json or [],
        "extra": extra,
    }

    with get_session() as sess:
        # 检查是否已有相同报告期记录
        existing = None
        if report_date:
            existing = sess.execute(text("""
                SELECT id FROM biz.shareholder_snapshot
                WHERE stock_code = :code AND report_date = :rd
            """), {"code": code, "rd": report_date}).fetchone()

        if existing:
            sess.execute(text("""
                UPDATE biz.shareholder_snapshot SET
                    top10_json = :json::jsonb,
                    inst_hold_pct = COALESCE(:inst, inst_hold_pct),
                    pledge_pct = COALESCE(:pledge, pledge_pct),
                    updated_at = NOW()
                WHERE id = :id
            """), {
                "id": existing[0],
                "json": json.dumps(full_json, ensure_ascii=False),
                "inst": inst_hold_pct,
                "pledge": pledge_pct,
            })
        else:
            sess.execute(text("""
                INSERT INTO biz.shareholder_snapshot
                    (stock_code, report_date, top10_json, inst_hold_pct, pledge_pct)
                VALUES
                    (:code, :rd, :json::jsonb, :inst, :pledge)
            """), {
                "code": code,
                "rd": report_date,
                "json": json.dumps(full_json, ensure_ascii=False),
                "inst": inst_hold_pct,
                "pledge": pledge_pct,
            })

    return True


def fetch_and_save_shareholders(
    codes: Optional[list[str]] = None,
    limit: int = 0,
) -> tuple[int, int]:
    """批量采集股东画像。"""
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    logger.info(f"开始采集股东画像: {len(codes)} 只")

    def _task(code: str) -> tuple[str, bool, str]:
        try:
            top10 = fetch_top10_holders(code)
            pledge = fetch_pledge(code)
            gdhs = fetch_shareholder_count(code)
            _save_one_shareholder(code, top10, pledge, gdhs)
            return code, True, ""
        except Exception as e:
            return code, False, str(e)

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_task, code): code for code in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            code, ok, err = fut.result()
            if ok:
                success += 1
            else:
                failed += 1
            if i % 200 == 0:
                logger.info(f"股东画像进度: {i}/{len(codes)}, 成功 {success}, 失败 {failed}")

    logger.info(f"股东画像完成: 成功 {success}, 失败 {failed}")
    return success, failed


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


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace("%", "").strip()
        f = float(v)
        return f if f == f else None
    except (ValueError, TypeError):
        return None


# ============================================================
# 主入口
# ============================================================

def run_shareholder_snapshot() -> None:
    """刷新股东画像。"""
    run = start_run(platform_code="akshare", phase="phase_c_shareholder")
    try:
        success, failed = fetch_and_save_shareholders()
        finish_run(run, status="success", rows_inserted=success, rows_updated=failed)
    except Exception as e:
        logger.exception(f"股东画像刷新失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_shareholder_snapshot()
