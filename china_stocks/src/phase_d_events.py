"""
Phase D：公司事件结构化提取（biz.corporate_event）

A 股投研的"事件驱动"核心数据。

策略：
  - 优先用 akshare 结构化接口（免费），拉到直接入库
  - 结构化接口覆盖不到的，再从公告标题/正文中提取
  - 事件类型与 mapping.taxonomy 的 content_topics 对齐

目前覆盖的事件类型（全部有 akshare 结构化接口）：
  1. 分红送转（dividend）
  2. 限售股解禁（unlock）
  3. 业绩预告（profit_alert）
  4. 股份回购（buyback）
  5. 增减持（share_change）
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
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak
from ..src.phase_a_stock_pool import get_stock_codes


# ============================================================
# 1. 分红送转
# ============================================================

def fetch_dividend(code: str) -> list[dict]:
    """
    获取分红送转记录。
    akshare 接口：stock_fhps_em（东方财富-分红派息）
    """
    try:
        df = ak.call_api("stock_fhps_em", save_raw=False, symbol=code)
        if df.empty:
            return []
    except Exception as e:
        logger.debug(f"{code} 分红数据获取失败: {e}")
        return []

    col_map = _find_columns(df.columns.tolist(), {
        "report_date": ["报告期", "分红年度"],
        "dividend_date": ["除权除息日", "实施日期", "除息日"],
        "plan": ["分配方案", "分红方案"],
        "per_share": ["每股送转", "每股派息", "每10股派息", "每10股现金分红"],
        "progress": ["进度", "方案进度"],
    })

    events = []
    for _, row in df.iterrows():
        event_data = {}
        for k, col in col_map.items():
            if col in df.columns and pd.notna(row[col]):
                event_data[k] = str(row[col])

        if not event_data:
            continue

        div_date = event_data.get("dividend_date") or event_data.get("report_date")
        event_date = _parse_date(div_date)

        events.append({
            "event_type": "dividend",
            "event_date": event_date,
            "event_data": event_data,
        })

    return events


# ============================================================
# 2. 限售股解禁
# ============================================================

def fetch_unlock(code: str) -> list[dict]:
    """
    获取限售股解禁记录。
    akshare 接口：stock_restricted_release_queue（解禁队列）
    """
    try:
        df = ak.call_api("stock_restricted_release_queue", save_raw=False, symbol=code)
        if df.empty:
            return []
    except Exception as e:
        logger.debug(f"{code} 解禁数据获取失败: {e}")
        return []

    col_map = _find_columns(df.columns.tolist(), {
        "release_date": ["解禁日期", "上市日期"],
        "release_shares": ["解禁数量", "数量"],
        "release_amount": ["解禁市值", "市值"],
        "share_type": ["解禁类型", "限售股份类型"],
    })

    events = []
    for _, row in df.iterrows():
        event_data = {}
        for k, col in col_map.items():
            if col in df.columns and pd.notna(row[col]):
                event_data[k] = str(row[col])

        rel_date = event_data.get("release_date")
        event_date = _parse_date(rel_date)

        events.append({
            "event_type": "unlock",
            "event_date": event_date,
            "event_data": event_data,
        })

    return events


# ============================================================
# 3. 业绩预告
# ============================================================

def fetch_profit_alert(code: str) -> list[dict]:
    """
    获取业绩预告。
    akshare 接口：stock_yjyg_ths（同花顺-业绩预告）或 stock_profit_forecast
    """
    try:
        df = ak.call_api("stock_profit_forecast", save_raw=False, symbol=code)
        if df.empty:
            return []
    except Exception as e:
        logger.debug(f"{code} 业绩预告获取失败: {e}")
        return []

    col_map = _find_columns(df.columns.tolist(), {
        "report_date": ["报告期", "报告时间", "报告年度"],
        "announce_date": ["公告日期", "披露日期"],
        "type": ["业绩变动类型", "类型", "业绩类型"],
        "net_profit_min": ["预测净利润最小值", "净利润下限"],
        "net_profit_max": ["预测净利润最大值", "净利润上限"],
        "change_pct_min": ["预测净利润同比变动幅度下限", "同比增长下限"],
        "change_pct_max": ["预测净利润同比变动幅度上限", "同比增长上限"],
        "summary": ["业绩变动摘要", "摘要", "业绩变动原因"],
    })

    events = []
    for _, row in df.iterrows():
        event_data = {}
        for k, col in col_map.items():
            if col in df.columns and pd.notna(row[col]):
                event_data[k] = str(row[col])

        ann_date = event_data.get("announce_date") or event_data.get("report_date")
        event_date = _parse_date(ann_date)

        events.append({
            "event_type": "profit_alert",
            "event_date": event_date,
            "event_data": event_data,
        })

    return events


# ============================================================
# 4. 股份回购
# ============================================================

def fetch_buyback(code: str) -> list[dict]:
    """
    获取股份回购进展。
    akshare 接口：stock_repurchase_em（东财-回购）
    """
    try:
        df = ak.call_api("stock_repurchase_em", save_raw=False, symbol=code)
        if df.empty:
            return []
    except Exception as e:
        logger.debug(f"{code} 回购数据获取失败: {e}")
        return []

    col_map = _find_columns(df.columns.tolist(), {
        "progress": ["进度", "回购进度"],
        "plan_amount": ["计划回购金额", "计划金额"],
        "done_amount": ["已回购金额", "回购金额"],
        "done_shares": ["已回购数量", "回购数量"],
        "price_range": ["回购价格区间", "成交价区间"],
        "start_date": ["首次实施日期", "开始日期"],
        "end_date": ["最新实施日期", "截止日期"],
    })

    events = []
    for _, row in df.iterrows():
        event_data = {}
        for k, col in col_map.items():
            if col in df.columns and pd.notna(row[col]):
                event_data[k] = str(row[col])

        event_date = _parse_date(event_data.get("end_date") or event_data.get("start_date"))

        events.append({
            "event_type": "buyback",
            "event_date": event_date,
            "event_data": event_data,
        })

    return events


# ============================================================
# 5. 增减持
# ============================================================

def fetch_share_change(code: str) -> list[dict]:
    """
    获取重要股东增减持。
    akshare 接口：stock_hold_num_cninfo（增减持）
    """
    try:
        df = ak.call_api("stock_hold_num_cninfo", save_raw=False, date="20240101")
        if df.empty:
            return []
    except Exception as e:
        logger.debug(f"{code} 增减持数据获取失败: {e}")
        return []

    # 这个接口是全市场的，需要按代码过滤
    code_col = None
    for col in df.columns:
        if "证券代码" in col or "代码" in col:
            code_col = col
            break

    if code_col:
        df = df[df[code_col].astype(str).str.zfill(6) == code]
        if df.empty:
            return []

    col_map = _find_columns(df.columns.tolist(), {
        "holder_name": ["股东名称", "股东"],
        "type": ["变动方向", "类型", "增减"],
        "shares": ["变动数量", "数量"],
        "ratio": ["变动比例", "占总股本比例"],
        "price": ["变动均价", "均价"],
        "amount": ["变动金额", "金额"],
        "date": ["变动日期", "日期"],
    })

    events = []
    for _, row in df.iterrows():
        event_data = {}
        for k, col in col_map.items():
            if col in df.columns and pd.notna(row[col]):
                event_data[k] = str(row[col])

        event_date = _parse_date(event_data.get("date"))

        events.append({
            "event_type": "share_change",
            "event_date": event_date,
            "event_data": event_data,
        })

    return events


# ============================================================
# 写入数据库
# ============================================================

def _save_events(code: str, events: list[dict]) -> int:
    """保存事件列表，按 (stock_code, event_type, event_date, event_data hash) 去重。
    简单起见，用 stock_code + event_type + event_date + 数据 JSON 字符串做去重。
    """
    if not events:
        return 0

    count = 0
    with get_session() as sess:
        for ev in events:
            data_json = json.dumps(ev["event_data"], ensure_ascii=False, sort_keys=True)

            # 去重：同股票同类型同日期，且数据内容一致则跳过
            existing = sess.execute(text("""
                SELECT id FROM biz.corporate_event
                WHERE stock_code = :code
                  AND event_type = :etype
                  AND event_date = :edate
                ORDER BY id DESC
                LIMIT 1
            """), {
                "code": code,
                "etype": ev["event_type"],
                "edate": ev["event_date"],
            }).fetchone()

            if existing:
                # 更新数据（如果内容有变化）
                sess.execute(text("""
                    UPDATE biz.corporate_event SET
                        event_data = :data::jsonb,
                        fetched_at = NOW()
                    WHERE id = :id
                """), {"id": existing[0], "data": data_json})
            else:
                sess.execute(text("""
                    INSERT INTO biz.corporate_event
                        (stock_code, event_type, event_date, event_data)
                    VALUES
                        (:code, :etype, :edate, :data::jsonb)
                """), {
                    "code": code,
                    "etype": ev["event_type"],
                    "edate": ev["event_date"],
                    "data": data_json,
                })
                count += 1

    return count


def fetch_all_events_for_stock(code: str) -> tuple[int, int]:
    """采集单只股票的全部事件。返回 (新增数, 事件总数)。"""
    all_events = []

    # 各类事件采集
    fetchers = [
        fetch_dividend,
        fetch_unlock,
        fetch_profit_alert,
        fetch_buyback,
        fetch_share_change,
    ]

    for fetcher in fetchers:
        try:
            events = fetcher(code)
            all_events.extend(events)
        except Exception as e:
            logger.debug(f"{code} {fetcher.__name__} 失败: {e}")

    inserted = _save_events(code, all_events)
    return inserted, len(all_events)


def fetch_and_save_events(
    codes: Optional[list[str]] = None,
    limit: int = 0,
) -> tuple[int, int]:
    """批量采集公司事件。"""
    if codes is None:
        codes = get_stock_codes()
    if limit and limit > 0:
        codes = codes[:limit]

    logger.info(f"开始采集公司事件: {len(codes)} 只")

    total_inserted = 0
    total_events = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_all_events_for_stock, code): code for code in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                inserted, count = fut.result()
                total_inserted += inserted
                total_events += count
            except Exception as e:
                failed += 1
                logger.debug(f"事件采集异常: {e}")

            if i % 200 == 0:
                logger.info(f"事件采集进度: {i}/{len(codes)}, 新增 {total_inserted}, 总事件 {total_events}")

    logger.info(f"公司事件采集完成: 新增 {total_inserted}, 总事件 {total_events}, 失败 {failed}")
    return total_inserted, total_events


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


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """宽松解析日期字符串。"""
    if not date_str:
        return None
    date_str = str(date_str).strip()
    for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d", "%Y.%m.%d"]:
        try:
            return datetime.strptime(date_str[:10] if len(date_str) >= 10 else date_str, fmt).date()
        except ValueError:
            continue
    return None


# ============================================================
# 主入口
# ============================================================

def run_corporate_events() -> None:
    """执行公司事件采集。"""
    run = start_run(platform_code="akshare", phase="phase_d_events")
    try:
        # 检查上游依赖：core.stock 是否有数据
        stock_codes = get_stock_codes()
        if not stock_codes:
            finish_run(
                run,
                status="skipped",
                error_msg="core.stock 为空，无股票可采集事件",
            )
            logger.warning("公司事件采集跳过：core.stock 为空")
            return

        inserted, total = fetch_and_save_events(codes=stock_codes)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=inserted,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=inserted, rows_updated=total, error_msg=err_msg)
        if status != "success":
            logger.warning(f"公司事件采集结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"公司事件采集失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_corporate_events()
