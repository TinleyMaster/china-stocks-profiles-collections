"""
Phase B3：机构调研纪要采集

A 股投研中，调研纪要是最直接反映公司经营近况的一手资料（仅次于财报）。

来源：
  1. 巨潮资讯网的"投资者关系活动记录表"公告（已通过 announcements 采到 biz.doc_source_entry，doc_type='survey'）
  2. akshare.stock_jgdy_tj_em（东财机构调研统计）

策略：
  - 主来源：从已有的 doc_source_entry 中提取 survey 类型（已经在公告采集中覆盖）
  - 补充：东财机构调研接口（有参与机构数量、调研方式、接待人员等结构化字段）
  - 调研纪要 PDF 走通用下载器

本模块专注于第 2 点：补充结构化字段到 biz.doc_source_entry 的 sub_type/event_data 扩展。
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..config import MAX_WORKERS
from ..db import get_session
from ..logging_setup import logger
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak


def fetch_survey_stat_by_date(trade_date: str) -> pd.DataFrame:
    """
    获取指定日期的机构调研统计。

    akshare 接口：stock_jgdy_tj_em
    """
    try:
        df = ak.call_api(
            "stock_jgdy_tj_em",
            save_raw=False,
            date=trade_date,
        )
        if df.empty:
            return pd.DataFrame()
        logger.info(f"{trade_date} 调研: {len(df)} 条")
        return df
    except Exception as e:
        logger.debug(f"{trade_date} 调研获取失败: {e}")
        return pd.DataFrame()


def _enrich_survey_entries(df: pd.DataFrame) -> int:
    """
    用东财调研统计数据丰富已有的 survey 类公告条目。
    更新：sub_type（调研方式）、附加字段写入 content_topics。
    同时把结构化数据存到一个 JSON 辅助列（扩展用）。
    """
    if df.empty:
        return 0

    col_map = _find_columns(df.columns.tolist(), {
        "code": ["股票代码", "代码"],
        "name": ["股票简称", "名称"],
        "survey_date": ["公告日期", "调研日期", "日期"],
        "institution_count": ["接待机构数量", "机构数量", "机构数"],
        "survey_type": ["调研方式", "类型"],
        "contact_person": ["接待人员", "接待"],
    })

    if not col_map.get("code"):
        logger.warning(f"调研数据字段不匹配，列: {df.columns.tolist()}")
        return 0

    updated = 0
    with get_session() as sess:
        for _, row in df.iterrows():
            code = str(row[col_map["code"]]).strip().split(".")[0].zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue

            # 找同日期的调研公告
            survey_date = None
            if col_map.get("survey_date") and pd.notna(row[col_map["survey_date"]]):
                survey_date = _parse_date(str(row[col_map["survey_date"]]))

            if not survey_date:
                continue

            survey_type = ""
            if col_map.get("survey_type") and pd.notna(row[col_map["survey_type"]]):
                survey_type = str(row[col_map["survey_type"]]).strip()

            inst_count = None
            if col_map.get("institution_count") and pd.notna(row[col_map["institution_count"]]):
                try:
                    inst_count = int(float(row[col_map["institution_count"]]))
                except (ValueError, TypeError):
                    pass

            # 找匹配的 doc_source_entry
            existing = sess.execute(text("""
                SELECT id FROM biz.doc_source_entry
                WHERE stock_code = :code
                  AND doc_type = 'survey'
                  AND publish_date = :sd
                ORDER BY id DESC
                LIMIT 1
            """), {"code": code, "sd": survey_date}).fetchone()

            if existing:
                # 更新 sub_type 为调研方式
                sub_type = f"调研:{survey_type}" if survey_type else "调研"
                sess.execute(text("""
                    UPDATE biz.doc_source_entry SET
                        sub_type = :st
                    WHERE id = :id
                """), {"id": existing[0], "st": sub_type[:100]})
                updated += 1
            else:
                # 没有对应公告，直接创建一条只有元数据的 entry（URL 留空，等后续补充）
                sess.execute(text("""
                    INSERT INTO biz.doc_source_entry
                        (stock_code, source_platform, doc_type, sub_type, title,
                         publish_date, content_topics, classify_method, classify_confidence)
                    VALUES
                        (:code, 'eastmoney', 'survey', :st, :title,
                         :sd, ARRAY['survey']::text[], 'rule', 0.9)
                """), {
                    "code": code,
                    "st": f"调研:{survey_type}" if survey_type else "调研",
                    "title": f"机构调研（{inst_count or 'N/A'}家机构）",
                    "sd": survey_date,
                })
                updated += 1

    logger.info(f"调研纪要补充/更新: {updated} 条")
    return updated


def fetch_survey_range(
    start_date: str,
    end_date: Optional[str] = None,
) -> int:
    """按日期范围补充调研数据。"""
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    start_dt = datetime.strptime(start_date, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date, "%Y%m%d").date()

    total_updated = 0
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%Y%m%d")
        try:
            df = fetch_survey_stat_by_date(date_str)
            updated = _enrich_survey_entries(df)
            total_updated += updated
        except Exception as e:
            logger.warning(f"{date_str} 调研补充异常: {e}")

        current += timedelta(days=1)
        time.sleep(0.5)

    return total_updated


def run_phase_b3_survey(
    incremental: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """执行调研纪要补充采集。"""
    run = start_run(platform_code="eastmoney", phase="phase_b3_survey")
    try:
        if start_date:
            updated = fetch_survey_range(start_date, end_date)
        elif incremental:
            # 增量：从最近 7 天开始（避免周末无数据的问题）
            start = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
            updated = fetch_survey_range(start, end_date)
        else:
            today = date.today().strftime("%Y%m%d")
            updated = fetch_survey_range(today, end_date)

        # 三态判定
        status, err_msg = determine_status(
            rows_updated=updated,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_updated=updated, error_msg=err_msg)
        if status != "success":
            logger.warning(f"调研纪要采集结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"调研纪要采集失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


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


def _parse_date(date_str: str) -> Optional[date]:
    if not date_str:
        return None
    date_str = str(date_str).strip()
    for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d", "%Y.%m.%d"]:
        try:
            return datetime.strptime(date_str[:10] if len(date_str) >= 10 else date_str, fmt).date()
        except ValueError:
            continue
    return None


if __name__ == "__main__":
    run_phase_b3_survey()
