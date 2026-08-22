"""
Phase B3：券商研报入口采集（东方财富研报中心）

A 股投研最重要的第三方文本数据，覆盖：
  - 券商发布的个股深度报告 / 点评报告
  - 行业研究报告
  - 宏观 / 策略报告

策略：
  - 用 akshare.stock_research_report_em（东财研报中心接口）
  - 按日期范围拉取，增量更新
  - 写入 biz.doc_source_entry，doc_type = 'research'
  - content_topics 用关键词规则初分（行业/公司/宏观/策略...）
  - 研报 PDF 下载单独 Phase 处理

注意：东财研报接口可能有频率限制，注意限速。
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text

from ..db import get_session
from ..logging_setup import logger
from ..mapping import classify_announcement
from ..sys import determine_status, finish_run, start_run
from ..src import akshare_client as ak


def _get_last_research_date() -> Optional[date]:
    """查询已采集的最新研报日期，用于增量。"""
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT MAX(publish_date) FROM biz.doc_source_entry
            WHERE source_platform = 'eastmoney_research'
        """)).fetchone()
        return row[0] if row and row[0] else None


def fetch_research_by_date(trade_date: str) -> pd.DataFrame:
    """
    获取指定日期的研报列表。

    akshare 接口：stock_research_report_em
    """
    try:
        df = ak.call_api(
            "stock_research_report_em",
            save_raw=True,
            date=trade_date,
        )
        if df.empty:
            logger.info(f"{trade_date} 无研报数据")
            return pd.DataFrame()
        logger.info(f"{trade_date} 研报: {len(df)} 条")
        return df
    except Exception as e:
        logger.warning(f"获取 {trade_date} 研报失败: {e}")
        return pd.DataFrame()


def _parse_and_save(df: pd.DataFrame) -> tuple[int, int]:
    """
    解析研报 DataFrame 并存入 biz.doc_source_entry。
    返回 (新增数, 更新数)
    """
    if df.empty:
        return 0, 0

    col_map = _find_columns(df.columns.tolist(), {
        "code": ["股票代码", "代码"],
        "name": ["股票简称", "简称", "股票名称"],
        "title": ["报告标题", "标题", "研报名称"],
        "author": ["研究员", "作者"],
        "broker": ["券商", "机构名称", "研究所"],
        "publish_date": ["发布日期", "日期", "公布日期"],
        "rating": ["评级", "投资评级", "投资评级变动"],
        "url": ["研报链接", "链接"],
    })

    if not col_map.get("title"):
        logger.warning(f"研报数据字段不匹配，列: {df.columns.tolist()}")
        return 0, 0

    inserted = 0
    updated = 0

    with get_session() as sess:
        for _, row in df.iterrows():
            # 研报可能对应多只股票，也可能是行业/宏观报告
            code = ""
            if col_map.get("code") and pd.notna(row[col_map["code"]]):
                code = str(row[col_map["code"]]).strip().split(".")[0].zfill(6)
                if len(code) != 6 or not code.isdigit():
                    code = ""

            title = str(row[col_map["title"]]).strip()
            if not title:
                continue

            url = ""
            if col_map.get("url") and pd.notna(row[col_map["url"]]):
                url = str(row[col_map["url"]]).strip()

            # 发布日期
            pub_date = None
            if col_map.get("publish_date") and pd.notna(row[col_map["publish_date"]]):
                pub_date = _parse_date(str(row[col_map["publish_date"]]))

            # 用规则分类器初分（研报类型 + 主题标签）
            cls = classify_announcement(title)
            # 覆盖 doc_type 为 research
            content_topics = cls.content_topics

            # 附加行业/类型标签（从标题推断）
            extra_topics = _infer_research_topics(title)
            for t in extra_topics:
                if t not in content_topics:
                    content_topics.append(t)

            # 去重：按 URL，没有 URL 按标题+日期+代码
            existing = None
            if url:
                existing = sess.execute(
                    text("SELECT id FROM biz.doc_source_entry WHERE url = :url"),
                    {"url": url},
                ).fetchone()

            if existing:
                sess.execute(text("""
                    UPDATE biz.doc_source_entry SET
                        title = :title,
                        content_topics = :topics::text[],
                        classify_method = :method,
                        classify_confidence = :conf
                    WHERE id = :id
                """), {
                    "id": existing[0],
                    "title": title,
                    "topics": "{" + ",".join(content_topics) + "}",
                    "method": cls.method,
                    "conf": cls.confidence,
                })
                updated += 1
            else:
                broker = ""
                if col_map.get("broker") and pd.notna(row[col_map["broker"]]):
                    broker = str(row[col_map["broker"]]).strip()[:50]

                # 如果没有股票代码（行业研报/宏观研报），用一个特殊的 stock_code 占位
                stock_code = code if code else "000000"  # 用 000000 表示非个股研报

                sess.execute(text("""
                    INSERT INTO biz.doc_source_entry
                        (stock_code, source_platform, doc_type, sub_type, title,
                         publish_date, url, content_topics, classify_method,
                         classify_confidence)
                    VALUES
                        (:code, 'eastmoney_research', 'research', :broker, :title,
                         :pub_date, :url, :topics::text[], :method, :conf)
                """), {
                    "code": stock_code,
                    "broker": broker or None,
                    "title": title,
                    "pub_date": pub_date,
                    "url": url,
                    "topics": "{" + ",".join(content_topics) + "}",
                    "method": cls.method,
                    "conf": cls.confidence,
                })
                inserted += 1

    logger.info(f"研报入库: 新增 {inserted}, 更新 {updated}")
    return inserted, updated


def _infer_research_topics(title: str) -> list[str]:
    """根据研报标题补充标签。"""
    topics = []
    title_clean = title.lower()

    # 报告类型
    if "深度" in title or "深度报告" in title:
        topics.append("research_deep")
    if "点评" in title or "快评" in title:
        topics.append("research_comment")
    if "首次覆盖" in title or "首次" in title:
        topics.append("research_initiation")

    # 行业研报 vs 个股研报
    if "行业" in title or "板块" in title:
        topics.append("industry_research")
    else:
        topics.append("company_research")

    return topics


def fetch_research_range(
    start_date: str,
    end_date: Optional[str] = None,
) -> tuple[int, int]:
    """按日期范围拉取研报。"""
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    start_dt = datetime.strptime(start_date, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date, "%Y%m%d").date()

    total_inserted = 0
    total_updated = 0
    current = start_dt

    logger.info(f"开始拉取研报: {start_dt} ~ {end_dt}")

    while current <= end_dt:
        date_str = current.strftime("%Y%m%d")
        try:
            df = fetch_research_by_date(date_str)
            inserted, updated = _parse_and_save(df)
            total_inserted += inserted
            total_updated += updated
        except Exception as e:
            logger.warning(f"{date_str} 研报采集异常: {e}")

        current += timedelta(days=1)
        time.sleep(1)  # 研报接口限速严格一点

    logger.info(f"研报采集完成 ({start_dt} ~ {end_dt}): 新增 {total_inserted}, 更新 {total_updated}")
    return total_inserted, total_updated


def run_phase_b3_research(
    incremental: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """执行研报入口采集。"""
    run = start_run(platform_code="eastmoney_research", phase="phase_b3_research")
    try:
        if start_date:
            inserted, updated = fetch_research_range(start_date, end_date)
        elif incremental:
            last = _get_last_research_date()
            if last:
                start = (last + timedelta(days=1)).strftime("%Y%m%d")
            else:
                # 首次跑，拉近 30 天
                start = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
                logger.info(f"首次运行研报采集，从 {start} 开始")
            inserted, updated = fetch_research_range(start, end_date)
        else:
            today = date.today().strftime("%Y%m%d")
            inserted, updated = fetch_research_range(today, end_date)

        # 三态判定
        status, err_msg = determine_status(
            rows_inserted=inserted,
            rows_updated=updated,
            expected_min_rows=1,
        )
        finish_run(run, status=status, rows_inserted=inserted, rows_updated=updated, error_msg=err_msg)
        if status != "success":
            logger.warning(f"研报采集结束，状态: {status}，原因: {err_msg}")
    except Exception as e:
        logger.exception(f"研报采集失败: {e}")
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
    run_phase_b3_research()
