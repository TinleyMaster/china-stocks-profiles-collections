"""
Phase B2：公告入口发现 —— 巨潮资讯网

A 股最核心的文本数据源，证监会指定信息披露平台。

策略：
  1. 全市场公告按日期拉取（akshare.stock_notice_cninfo）
  2. 写入 biz.doc_source_entry（公告 URL / 标题 / 分类）
  3. 按 doc_id 去重，支持增量

巨潮公告分类（L1 规则）：
  - 规则分类在 china_stocks.mapping.classify_announcement
  - 22 类 content_topics 多标签
  - 后续可加 AI L2 精分

注意：akshare 的 cninfo 接口返回巨潮数据，免费但有频率限制。
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
from ..sys import finish_run, start_run
from ..src import akshare_client as ak


def fetch_announcements_by_date(
    trade_date: str,
    category: str = "全部",
) -> pd.DataFrame:
    """
    拉取指定日期的全部公告。

    Args:
        trade_date: 日期 YYYYMMDD
        category: 公告类别，默认全部
                  可选：年度报告、半年度报告、一季度报告、三季度报告、
                        股权变动、重大事项、融资公告、风险提示...
    """
    try:
        df = ak.call_api(
            "stock_notice_cninfo",
            save_raw=True,
            date=trade_date,
        )
        if df.empty:
            logger.info(f"{trade_date} 无公告数据")
            return pd.DataFrame()
        logger.info(f"{trade_date} 公告: {len(df)} 条")
        return df
    except Exception as e:
        logger.warning(f"获取 {trade_date} 公告失败: {e}")
        return pd.DataFrame()


def _get_last_announcement_date() -> Optional[date]:
    """查询已采集的最新公告日期，用于增量。"""
    with get_session() as sess:
        row = sess.execute(text("""
            SELECT MAX(publish_date) FROM biz.doc_source_entry
            WHERE source_platform = 'cninfo' AND doc_type != 'research'
        """)).fetchone()
        return row[0] if row and row[0] else None


def _parse_and_save(df: pd.DataFrame) -> tuple[int, int]:
    """
    解析巨潮公告 DataFrame 并存入 biz.doc_source_entry。
    返回 (新增数, 更新数)
    """
    if df.empty:
        return 0, 0

    # 列名模糊匹配
    col_map = _find_columns(df.columns.tolist(), {
        "code": ["股票代码", "代码"],
        "name": ["股票简称", "简称"],
        "title": ["公告标题", "标题", "公告名称"],
        "publish_date": ["公告时间", "发布日期", "公告日期"],
        "url": ["公告链接", "链接", "PDF链接", "查看PDF链接"],
        "notice_type": ["公告类型", "类型"],
    })

    if not col_map.get("code") or not col_map.get("title"):
        logger.warning(f"公告数据字段不匹配，列: {df.columns.tolist()}")
        return 0, 0

    inserted = 0
    updated = 0

    with get_session() as sess:
        for _, row in df.iterrows():
            code = str(row[col_map["code"]]).strip()
            # 代码清洗：有些带市场后缀如 "600519.SH"
            code = code.split(".")[0].zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue

            title = str(row[col_map["title"]]).strip()
            if not title:
                continue

            url = ""
            if col_map.get("url") and pd.notna(row[col_map["url"]]):
                url = str(row[col_map["url"]]).strip()

            # 发布日期
            pub_date = None
            if col_map.get("publish_date") and pd.notna(row[col_map["publish_date"]]):
                date_str = str(row[col_map["publish_date"]]).strip()
                try:
                    # 尝试多种格式
                    if len(date_str) >= 10:
                        pub_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
                    elif len(date_str) == 8 and date_str.isdigit():
                        pub_date = datetime.strptime(date_str, "%Y%m%d").date()
                except ValueError:
                    pass

            # 公告分类
            cls = classify_announcement(title)
            notice_type_from_source = ""
            if col_map.get("notice_type") and pd.notna(row[col_map["notice_type"]]):
                notice_type_from_source = str(row[col_map["notice_type"]]).strip()

            # 去重检查（按 URL 或 标题+日期+代码）
            existing = None
            if url:
                existing = sess.execute(
                    text("SELECT id FROM biz.doc_source_entry WHERE url = :url"),
                    {"url": url},
                ).fetchone()

            if existing:
                # 更新分类和标题
                sess.execute(text("""
                    UPDATE biz.doc_source_entry SET
                        title = :title,
                        doc_type = :doc_type,
                        sub_type = :sub_type,
                        content_topics = :topics::text[],
                        classify_method = :method,
                        classify_confidence = :conf
                    WHERE id = :id
                """), {
                    "id": existing[0],
                    "title": title,
                    "doc_type": cls.doc_type,
                    "sub_type": notice_type_from_source[:100] if notice_type_from_source else None,
                    "topics": "{" + ",".join(cls.content_topics) + "}",
                    "method": cls.method,
                    "conf": cls.confidence,
                })
                updated += 1
            else:
                sess.execute(text("""
                    INSERT INTO biz.doc_source_entry
                        (stock_code, source_platform, doc_type, sub_type, title,
                         publish_date, url, content_topics, classify_method,
                         classify_confidence)
                    VALUES
                        (:code, 'cninfo', :doc_type, :sub_type, :title,
                         :pub_date, :url, :topics::text[], :method, :conf)
                """), {
                    "code": code,
                    "doc_type": cls.doc_type,
                    "sub_type": notice_type_from_source[:100] if notice_type_from_source else None,
                    "title": title,
                    "pub_date": pub_date,
                    "url": url,
                    "topics": "{" + ",".join(cls.content_topics) + "}",
                    "method": cls.method,
                    "conf": cls.confidence,
                })
                inserted += 1

    logger.info(f"公告入库: 新增 {inserted}, 更新 {updated}")
    return inserted, updated


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


def fetch_announcements_range(
    start_date: str,
    end_date: Optional[str] = None,
) -> tuple[int, int]:
    """
    按日期范围拉取公告（全量/批量回溯用）。
    """
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    start_dt = datetime.strptime(start_date, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date, "%Y%m%d").date()

    total_inserted = 0
    total_updated = 0
    current = start_dt

    logger.info(f"开始拉取公告: {start_dt} ~ {end_dt}")

    while current <= end_dt:
        date_str = current.strftime("%Y%m%d")
        try:
            df = fetch_announcements_by_date(date_str)
            inserted, updated = _parse_and_save(df)
            total_inserted += inserted
            total_updated += updated
        except Exception as e:
            logger.warning(f"{date_str} 公告采集异常: {e}")

        current += timedelta(days=1)
        # 轻微限速，避免触发反爬
        time.sleep(0.5)

    logger.info(f"公告采集完成 ({start_dt} ~ {end_dt}): 新增 {total_inserted}, 更新 {total_updated}")
    return total_inserted, total_updated


def run_phase_b2_announcements(
    incremental: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """执行公告入口采集。"""
    run = start_run(platform_code="cninfo", phase="phase_b2_announcements")
    try:
        if start_date:
            inserted, updated = fetch_announcements_range(start_date, end_date)
        elif incremental:
            last = _get_last_announcement_date()
            if last:
                start = (last + timedelta(days=1)).strftime("%Y%m%d")
            else:
                # 首次跑，默认拉近 30 天
                start = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
                logger.info(f"首次运行公告采集，从 {start} 开始")
            inserted, updated = fetch_announcements_range(start, end_date)
        else:
            # 非增量且没指定日期 = 今天
            today = date.today().strftime("%Y%m%d")
            inserted, updated = fetch_announcements_range(today, end_date)

        finish_run(run, status="success", rows_inserted=inserted, rows_updated=updated)
    except Exception as e:
        logger.exception(f"公告采集失败: {e}")
        finish_run(run, status="failed", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_phase_b2_announcements()
